use puzzella_rendezvous::{serve, Config, Server};
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
    let config = Config::from_env()?;
    let mut server = Server::with_trusted_proxies(config.limits, config.trusted_proxies);
    if let Some(turn) = config.turn {
        match puzzella_rendezvous::turn::CloudflareTurnProvider::new(&turn) {
            Ok(provider) => {
                server = server.with_turn(puzzella_rendezvous::turn::TurnService::new(
                    std::sync::Arc::new(provider),
                    &turn,
                ))
            }
            Err(_) => tracing::warn!("TURN unavailable; continuing with direct ICE"),
        }
    }
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    tracing::info!(listen = %listener.local_addr()?, "rendezvous v1 listening");
    serve(listener, server, async {
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
