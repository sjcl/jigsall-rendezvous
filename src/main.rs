use jigsall_rendezvous::{serve, Config, Server};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Restrict logging to our own target. Dependency trace logs can include
    // WebSocket frame contents; RUST_LOG must not enable those in this server.
    let level = std::env::var("JIGSALL_RENDEZVOUS_LOG")
        .unwrap_or_else(|_| "info".into())
        .parse::<tracing_subscriber::filter::LevelFilter>()?;
    use tracing_subscriber::prelude::*;
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(
            tracing_subscriber::filter::Targets::new().with_target("jigsall_rendezvous", level),
        ))
        .init();
    let config = Config::from_env()?;
    let mut server = Server::with_trusted_proxies(config.limits, config.trusted_proxies);
    if let Some(turn) = config.turn {
        match jigsall_rendezvous::turn::CloudflareTurnProvider::new(&turn) {
            Ok(provider) => {
                let mut service = jigsall_rendezvous::turn::TurnService::new(
                    std::sync::Arc::new(provider),
                    &turn,
                );
                if let Some(budget) = config.turn_budget {
                    let analytics = jigsall_rendezvous::turn::budget::CloudflareAnalytics::new(
                        &budget,
                        &turn.key_id,
                    )
                    .map_err(|_| std::io::Error::other("TURN Analytics client unavailable"))?;
                    service = service.with_budget(budget, turn.key_id, std::sync::Arc::new(analytics)).await
                        .map_err(|_| std::io::Error::other("TURN budget storage initialization failed; check registry path, configuration and single-writer lock"))?;
                }
                server = server.with_turn(service);
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
