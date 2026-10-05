//! Manual cross-repository harness; never loads or requests production secrets.
use puzzella_rendezvous::{
    protocol::TurnServer,
    serve,
    turn::{TurnConfig, TurnError, TurnProvider, TurnService},
    Limits, Server,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
struct Provider {
    address: String,
    count: AtomicUsize,
    mode: String,
    recovered_at: std::time::Instant,
}
impl TurnProvider for Provider {
    fn issue<'a>(
        &'a self,
        _: &'a str,
        _: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<TurnServer>, TurnError>> + Send + 'a>> {
        Box::pin(async move {
            if self.mode == "unavailable"
                || (self.mode == "recovering" && std::time::Instant::now() < self.recovered_at)
            {
                return Err(TurnError::Unavailable);
            }
            let version = if self.count.fetch_add(1, Ordering::SeqCst) < 2 {
                "A"
            } else {
                "B"
            };
            Ok(vec![TurnServer {
                address: self.address.clone(),
                username: format!("user-{version}"),
                password: format!("password-{version}"),
            }])
        })
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::args()
        .nth(1)
        .expect("pass the local TURN fixture host:port");
    let config = TurnConfig {
        key_id: String::new(),
        api_token: String::new(),
        ttl: Duration::from_secs(4),
        concurrency: 4,
        requests_per_minute: 600,
    };
    let server = Server::new(Limits::default()).with_turn(TurnService::new(
        Arc::new(Provider {
            address,
            count: AtomicUsize::new(0),
            mode: std::env::args().nth(2).unwrap_or_default(),
            recovered_at: std::time::Instant::now() + Duration::from_secs(2),
        }),
        &config,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    println!("ws://{}/v1/ws", listener.local_addr()?);
    serve(listener, server, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
    Ok(())
}
