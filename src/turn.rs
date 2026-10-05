//! Optional async credential service; no room/member state or gameplay authority.
use crate::{
    outbound,
    protocol::{ServerMessage, TurnCredentials, TurnServer},
    state::Window,
};
use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct TurnConfig {
    pub key_id: String,
    pub api_token: String,
    pub ttl: Duration,
    pub concurrency: usize,
    pub requests_per_minute: u32,
}
impl fmt::Debug for TurnConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TurnConfig")
            .field("key_id", &"[redacted]")
            .field("api_token", &"[redacted]")
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TurnError {
    Unavailable,
    Limited,
    InvalidResponse,
}
/// Errors are deliberately fixed codes: HTTP bodies/headers and serde errors are secret-bearing.
pub trait TurnProvider: Send + Sync {
    fn issue<'a>(
        &'a self,
        identifier: &'a str,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<TurnServer>, TurnError>> + Send + 'a>>;
}
pub struct CloudflareTurnProvider {
    client: reqwest::Client,
    endpoint: String,
    token: String,
}
impl CloudflareTurnProvider {
    pub fn new(config: &TurnConfig) -> Result<Self, TurnError> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(3))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|_| TurnError::Unavailable)?,
            endpoint: format!(
                "https://rtc.live.cloudflare.com/v1/turn/keys/{}/credentials/generate-ice-servers",
                config.key_id
            ),
            token: config.api_token.clone(),
        })
    }
}
impl TurnProvider for CloudflareTurnProvider {
    fn issue<'a>(
        &'a self,
        identifier: &'a str,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<TurnServer>, TurnError>> + Send + 'a>> {
        Box::pin(async move {
            let mut response = self
                .client
                .post(&self.endpoint)
                .bearer_auth(&self.token)
                .json(&serde_json::json!({"ttl": ttl.as_secs(), "customIdentifier": identifier}))
                .send()
                .await
                .map_err(|_| TurnError::Unavailable)?;
            if response.status() != reqwest::StatusCode::CREATED {
                return Err(TurnError::Unavailable);
            }
            let mut body = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(|_| TurnError::Unavailable)? {
                if body.len() + chunk.len() > 16 * 1024 {
                    return Err(TurnError::InvalidResponse);
                }
                body.extend_from_slice(&chunk);
            }
            parse_response(&body)
        })
    }
}
fn parse_response(body: &[u8]) -> Result<Vec<TurnServer>, TurnError> {
    #[derive(serde::Deserialize)]
    struct Response {
        #[serde(rename = "iceServers")]
        servers: Vec<IceServer>,
    }
    #[derive(serde::Deserialize)]
    struct IceServer {
        urls: Vec<String>,
        username: Option<String>,
        credential: Option<String>,
    }
    let response: Response =
        serde_json::from_slice(body).map_err(|_| TurnError::InvalidResponse)?;
    let mut servers = Vec::new();
    for server in response.servers {
        for url in server.urls {
            // Cloudflare returns STUN/TCP/TLS too. Keep the primary/alternate UDP ports only.
            let Some(address) = url
                .strip_prefix("turn:")
                .and_then(|u| u.strip_suffix("?transport=udp"))
            else {
                continue;
            };
            let (Some(username), Some(password)) = (&server.username, &server.credential) else {
                return Err(TurnError::InvalidResponse);
            };
            servers.push(TurnServer {
                address: address.into(),
                username: username.clone(),
                password: password.clone(),
            });
        }
    }
    TurnCredentials {
        expires_at_unix: 1,
        servers: servers.clone(),
    }
    .validate()
    .map_err(|_| TurnError::InvalidResponse)?;
    Ok(servers)
}
#[derive(Clone)]
pub struct TurnService {
    provider: Arc<dyn TurnProvider>,
    ttl: Duration,
    permits: Arc<Semaphore>,
    rate: Arc<Mutex<Window>>,
}
impl TurnService {
    pub fn new(provider: Arc<dyn TurnProvider>, config: &TurnConfig) -> Self {
        Self {
            provider,
            ttl: config.ttl,
            permits: Arc::new(Semaphore::new(config.concurrency)),
            rate: Arc::new(Mutex::new(Window::new(
                config.requests_per_minute,
                Duration::from_secs(60),
                Instant::now(),
            ))),
        }
    }
    pub(crate) async fn issue(&self, identifier: &str) -> Result<TurnCredentials, TurnError> {
        // One deadline covers both admission waiting and external HTTP I/O.
        tokio::time::timeout(Duration::from_secs(3), self.issue_admitted(identifier))
            .await
            .map_err(|_| TurnError::Unavailable)?
    }
    async fn issue_admitted(&self, identifier: &str) -> Result<TurnCredentials, TurnError> {
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| TurnError::Unavailable)?;
        if !self.rate.lock().unwrap().take(Instant::now()) {
            return Err(TurnError::Limited);
        }
        // Start the TTL clock before HTTP I/O so latency cannot extend validity locally.
        let issued = unix_now();
        let servers = self.provider.issue(identifier, self.ttl).await?;
        let turn = TurnCredentials {
            expires_at_unix: issued + self.ttl.as_secs(),
            servers,
        };
        turn.validate().map_err(|_| TurnError::InvalidResponse)?;
        if turn.expires_at_unix <= unix_now() {
            return Err(TurnError::InvalidResponse);
        }
        Ok(turn)
    }
    pub(crate) async fn rotate(
        &self,
        identifier: String,
        tx: outbound::Sender,
        initial: TurnCredentials,
    ) {
        let mut current = initial;
        let mut delay = self.ttl / 2;
        let mut backoff = Duration::from_secs(1);
        let mut expiry_reported = false;
        loop {
            tokio::time::sleep(delay).await;
            let update = self.issue(&identifier).await.and_then(|turn| {
                if current.same_server_set(&turn) {
                    Ok(turn)
                } else {
                    Err(TurnError::InvalidResponse)
                }
            });
            match update {
                Ok(turn) => {
                    // Keep one newly issued value while a bounded outbound queue is full.
                    // Do not mint another credential or close a room because of TURN pressure.
                    while tx
                        .try_send(ServerMessage::TurnCredentials { turn: turn.clone() })
                        .is_err()
                    {
                        if unix_now() >= turn.expires_at_unix {
                            break;
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    if unix_now() < turn.expires_at_unix {
                        current = turn;
                        expiry_reported = false;
                        backoff = Duration::from_secs(1);
                        delay = self.ttl / 2;
                    } else {
                        delay = backoff;
                    }
                }
                Err(_) => {
                    // An update failure never replaces the client's valid credential.
                    if unix_now() >= current.expires_at_unix && !expiry_reported {
                        expiry_reported = tx.try_send(ServerMessage::TurnUnavailable {}).is_ok();
                    }
                    delay = backoff;
                    if let Some(remaining) = current.expires_at_unix.checked_sub(unix_now()) {
                        delay = delay.min(Duration::from_secs(remaining.max(1)));
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                }
            }
        }
    }
}
fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
/// Aborting the session-owned task cancels HTTP I/O and releases its permit.
pub(crate) struct Rotation(pub tokio::task::JoinHandle<()>);
impl Drop for Rotation {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests;
