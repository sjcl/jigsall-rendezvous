use puzzella_rendezvous::{serve, Limits, Server};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Restrict logging to our own target. Dependency trace logs can include
    // WebSocket frame contents; RUST_LOG must not enable those in this server.
    let level = std::env::var("PUZZELLA_RENDEZVOUS_LOG")
        .unwrap_or_else(|_| "info".into())
        .parse::<tracing_subscriber::filter::LevelFilter>()?;
    use tracing_subscriber::prelude::*;
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(
            tracing_subscriber::filter::Targets::new().with_target("puzzella_rendezvous", level),
        ))
        .init();
    let address = std::env::var("PUZZELLA_RENDEZVOUS_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:8080".into())
        .parse::<std::net::SocketAddr>()?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!(listen = %listener.local_addr()?, "rendezvous v1 listening");
    serve(listener, Server::new(Limits::default()), async {
        #[cfg(unix)]
        {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("SIGTERM handler");
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
    tracing::info!("rendezvous stopped");
    Ok(())
}
