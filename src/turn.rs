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
pub mod budget;

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
    fn revoke<'a>(
        &'a self,
        _username: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), TurnError>> + Send + 'a>> {
        Box::pin(async { Err(TurnError::Unavailable) })
    }
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
    fn revoke<'a>(
        &'a self,
        username: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), TurnError>> + Send + 'a>> {
        Box::pin(async move {
            let base = self
                .endpoint
                .strip_suffix("/generate-ice-servers")
                .ok_or(TurnError::Unavailable)?;
            let mut url = reqwest::Url::parse(base).map_err(|_| TurnError::Unavailable)?;
            url.path_segments_mut()
                .map_err(|_| TurnError::Unavailable)?
                .push(username)
                .push("revoke");
            let response = self
                .client
                .post(url)
                .bearer_auth(&self.token)
                .send()
                .await
                .map_err(|_| TurnError::Unavailable)?;
            if response.status() == reqwest::StatusCode::NO_CONTENT {
                Ok(())
            } else {
                Err(TurnError::Unavailable)
            }
        })
    }
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
    budget: Option<Arc<budget::BudgetGuard>>,
    _monitor: Option<Arc<budget::MonitorTasks>>,
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
            budget: None,
            _monitor: None,
        }
    }
    pub async fn with_budget(
        mut self,
        config: budget::BudgetConfig,
        key: String,
        backend: Arc<dyn budget::AnalyticsBackend>,
    ) -> std::io::Result<Self> {
        let (guard, monitor) = budget::start(config, key, backend, self.provider.clone()).await?;
        self.budget = Some(guard);
        self._monitor = Some(monitor);
        Ok(self)
    }
    pub fn budget(&self) -> Option<Arc<budget::BudgetGuard>> {
        self.budget.clone()
    }
    pub(crate) async fn publish<T>(
        &self,
        turn: TurnCredentials,
        publish: impl FnOnce(Option<TurnCredentials>) -> T,
    ) -> T {
        if let Some(budget) = &self.budget {
            budget.publish(turn, publish).await
        } else {
            publish(Some(turn))
        }
    }
    pub(crate) async fn issue(&self, identifier: &str) -> Result<TurnCredentials, TurnError> {
        // One deadline covers both admission waiting and external HTTP I/O.
        tokio::time::timeout(Duration::from_secs(3), self.issue_admitted(identifier))
            .await
            .map_err(|_| TurnError::Unavailable)?
    }
    async fn issue_admitted(&self, identifier: &str) -> Result<TurnCredentials, TurnError> {
        if self.budget.as_ref().is_some_and(|b| !b.can_issue()) {
            return Err(TurnError::Unavailable);
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let _permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| TurnError::Unavailable)?;
        if self.budget.as_ref().is_some_and(|b| !b.can_issue()) {
            return Err(TurnError::Unavailable);
        }
        if !self.rate.lock().unwrap().take(Instant::now()) {
            return Err(TurnError::Limited);
        }
        // Start the TTL clock before HTTP I/O so latency cannot extend validity locally.
        let issued = unix_now();
        let servers = if let Some(budget) = &self.budget {
            // Once HTTP starts, a bounded task owns the permit and commits any
            // successful response even if a socket/outer deadline cancels its wait.
            let provider = self.provider.clone();
            let budget = budget.clone();
            let identifier = identifier.to_owned();
            let ttl = self.ttl;
            return tokio::spawn(async move {
                let _permit = _permit;
                if !budget.can_issue() {
                    return Err(TurnError::Unavailable);
                }
                let servers = tokio::time::timeout_at(deadline, provider.issue(&identifier, ttl))
                    .await
                    .map_err(|_| TurnError::Unavailable)??;
                let turn = TurnCredentials {
                    expires_at_unix: issued + ttl.as_secs(),
                    servers,
                };
                turn.validate().map_err(|_| TurnError::InvalidResponse)?;
                // Registry expiry conservatively covers provider processing latency.
                let registered = budget.register(&turn, unix_now() + ttl.as_secs() + 1).await;
                if !registered || turn.expires_at_unix <= unix_now() {
                    budget.discard(&turn).await;
                    return Err(TurnError::Unavailable);
                }
                Ok(turn)
            })
            .await
            .map_err(|_| TurnError::Unavailable)?;
        } else {
            self.provider.issue(identifier, self.ttl).await?
        };
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
    // Publish future-connection defaults, never replace active allocation credentials.
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
        let mut changes = self.budget.as_ref().map(|b| b.subscribe());
        let mut hard_generation = changes.as_ref().map_or(0, |c| c.borrow().hard_generation);
        if self
            .budget
            .as_ref()
            .is_some_and(|b| b.status().state == budget::BudgetState::HardLimited)
        {
            delay = Duration::ZERO;
        }
        loop {
            tokio::select! {
                _ = tokio::time::sleep(delay) => {},
                _ = async { match &mut changes { Some(c) => { let _ = c.changed().await; }, None => std::future::pending().await } } => {}
            }
            let hard = self.budget.as_ref().is_some_and(|b| {
                let status = b.status();
                let hard = status.state == budget::BudgetState::HardLimited
                    || status.hard_generation != hard_generation;
                hard_generation = status.hard_generation;
                hard
            });
            if hard && !expiry_reported {
                expiry_reported = tx.try_send(ServerMessage::TurnUnavailable {}).is_ok();
            }
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
                    let mut published = false;
                    while !published {
                        let result = self
                            .publish(turn.clone(), |value| {
                                value.map(|turn| {
                                    tx.try_send(ServerMessage::TurnCredentials { turn }).is_ok()
                                })
                            })
                            .await;
                        let Some(sent) = result else {
                            break;
                        };
                        published = sent;
                        if published {
                            break;
                        }
                        if unix_now() >= turn.expires_at_unix {
                            break;
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    if published && unix_now() < turn.expires_at_unix {
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
/// Aborting rotation cancels its wait. With budgets, already-started issuance
/// finishes registry bookkeeping in its permit-owning task.
pub(crate) struct Rotation(pub tokio::task::JoinHandle<()>);
impl Drop for Rotation {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests;
