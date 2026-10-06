//! Account/key-scoped TURN budget. Room and gameplay state never enter this module.
use super::{unix_now, TurnError, TurnProvider};
use crate::protocol::TurnCredentials;
use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc};
use futures_util::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt,
    fs::{File, OpenOptions},
    future::Future,
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{watch, Notify};

#[derive(Clone)]
pub struct BudgetConfig {
    pub account_id: String,
    pub analytics_token: String,
    pub soft_limit_bytes: u64,
    pub hard_limit_bytes: u64,
    pub polling_interval: Duration,
    pub stale_timeout: Duration,
    pub billing_anchor: DateTime<Utc>,
    /// None means calendar months, preserving the anchor's day and UTC time.
    pub period_seconds: Option<u64>,
    pub registry_path: PathBuf,
}
impl BudgetConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.account_id.len() != 32 || !self.account_id.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err("invalid TURN budget account ID");
        }
        if self.analytics_token.is_empty()
            || self.analytics_token.len() > 4096
            || !self.analytics_token.bytes().all(|b| b.is_ascii_graphic())
        {
            return Err("invalid TURN Analytics token");
        }
        if self.soft_limit_bytes == 0 || self.soft_limit_bytes >= self.hard_limit_bytes {
            return Err("TURN budget requires 0 < soft limit < hard limit");
        }
        if self.polling_interval.is_zero()
            || self.polling_interval > Duration::from_secs(3600)
            || self.stale_timeout <= self.polling_interval
            || self.stale_timeout > Duration::from_secs(86400)
        {
            return Err("invalid TURN budget polling/stale interval");
        }
        if self.billing_anchor.timestamp_subsec_nanos() != 0
            || !(1970..=9998).contains(&self.billing_anchor.year())
            || self
                .period_seconds
                .is_some_and(|s| !(60..=366 * 86400).contains(&s))
        {
            return Err("invalid TURN billing cycle");
        }
        if self.registry_path.as_os_str().is_empty() {
            return Err("TURN registry path is required");
        }
        Ok(())
    }
}
impl fmt::Debug for BudgetConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BudgetConfig")
            .field("analytics_token", &"[redacted]")
            .field("soft_limit_bytes", &self.soft_limit_bytes)
            .field("hard_limit_bytes", &self.hard_limit_bytes)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BudgetState {
    Normal,
    SoftLimited,
    HardLimited,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BillingPeriod {
    pub start: i64,
    pub end: i64,
}
/// Pure boundary calculation, including leap years and short-month clamping.
pub fn billing_period(
    anchor: DateTime<Utc>,
    period_seconds: Option<u64>,
    now: DateTime<Utc>,
) -> BillingPeriod {
    if let Some(seconds) = period_seconds {
        let seconds = seconds as i64;
        let start = anchor.timestamp()
            + (now.timestamp() - anchor.timestamp()).div_euclid(seconds) * seconds;
        return BillingPeriod {
            start,
            end: start + seconds,
        };
    }
    let boundary = |month: i32| {
        let year = month.div_euclid(12);
        let month = month.rem_euclid(12) as u32 + 1;
        let next = if month == 12 {
            (year + 1, 1)
        } else {
            (year, month + 1)
        };
        let last_day =
            Utc.with_ymd_and_hms(next.0, next.1, 1, 0, 0, 0).unwrap() - chrono::Duration::days(1);
        Utc.with_ymd_and_hms(
            year,
            month,
            anchor.day().min(last_day.day()),
            anchor.hour(),
            anchor.minute(),
            anchor.second(),
        )
        .unwrap()
        .timestamp()
    };
    let mut month = now.year() * 12 + now.month0() as i32;
    if boundary(month) > now.timestamp() {
        month -= 1;
    }
    BillingPeriod {
        start: boundary(month),
        end: boundary(month + 1),
    }
}
pub trait AnalyticsBackend: Send + Sync {
    fn usage<'a>(
        &'a self,
        period: BillingPeriod,
        until: i64,
    ) -> Pin<Box<dyn Future<Output = Result<u64, TurnError>> + Send + 'a>>;
}
pub struct CloudflareAnalytics {
    client: reqwest::Client,
    endpoint: String,
    account: String,
    key: String,
    token: String,
}
// No dimensions: Cloudflare aggregates the entire key/period into one group.
const QUERY: &str = r#"query TurnBudget($account: String!, $key: String!, $start: Time!, $end: Time!) {
  viewer { accounts(filter: {accountTag: $account}) {
    callsTurnUsageAdaptiveGroups(limit: 1, filter: {keyId: $key, datetime_geq: $start, datetime_lt: $end}) {
      sum { egressBytes }
    }
  } }
}"#;
impl CloudflareAnalytics {
    pub fn new(config: &BudgetConfig, key: &str) -> Result<Self, TurnError> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|_| TurnError::Unavailable)?,
            endpoint: "https://api.cloudflare.com/client/v4/graphql".into(),
            account: config.account_id.clone(),
            key: key.into(),
            token: config.analytics_token.clone(),
        })
    }
}
impl AnalyticsBackend for CloudflareAnalytics {
    fn usage<'a>(
        &'a self,
        period: BillingPeriod,
        until: i64,
    ) -> Pin<Box<dyn Future<Output = Result<u64, TurnError>> + Send + 'a>> {
        Box::pin(async move {
            let format = |t| DateTime::<Utc>::from_timestamp(t, 0).unwrap().to_rfc3339();
            let mut response = self
                .client
                .post(&self.endpoint)
                .bearer_auth(&self.token)
                .json(&serde_json::json!({"query": QUERY, "variables": {
                    "account": self.account, "key": self.key,
                    "start": format(period.start), "end": format(until.min(period.end))
                }}))
                .send()
                .await
                .map_err(|_| TurnError::Unavailable)?;
            if !response.status().is_success() {
                return Err(TurnError::Unavailable);
            }
            let mut body = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(|_| TurnError::Unavailable)? {
                if body.len() + chunk.len() > 16 * 1024 {
                    return Err(TurnError::InvalidResponse);
                }
                body.extend_from_slice(&chunk);
            }
            parse_usage(&body)
        })
    }
}
fn parse_usage(body: &[u8]) -> Result<u64, TurnError> {
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| TurnError::InvalidResponse)?;
    if value
        .get("errors")
        .is_some_and(|e| !e.is_null() && e.as_array().is_none_or(|e| !e.is_empty()))
    {
        return Err(TurnError::Unavailable);
    }
    let accounts = value
        .pointer("/data/viewer/accounts")
        .and_then(|v| v.as_array())
        .filter(|a| a.len() == 1)
        .ok_or(TurnError::InvalidResponse)?;
    let groups = accounts[0]
        .get("callsTurnUsageAdaptiveGroups")
        .and_then(|v| v.as_array())
        .filter(|g| g.len() <= 1)
        .ok_or(TurnError::InvalidResponse)?;
    let Some(group) = groups.first() else {
        return Ok(0);
    };
    // Round sampled fractional bytes up. Reject negatives/NaN/overflow, never assume zero.
    let bytes = group
        .pointer("/sum/egressBytes")
        .ok_or(TurnError::InvalidResponse)?;
    if let Some(n) = bytes.as_u64() {
        return Ok(n);
    }
    let n = bytes
        .as_f64()
        .filter(|n| n.is_finite() && *n >= 0.0 && *n < u64::MAX as f64)
        .ok_or(TurnError::InvalidResponse)?;
    Ok(n.ceil() as u64)
}

#[derive(Clone, Serialize, Deserialize)]
struct Credential {
    expires_at: u64,
    pending_revoke: bool,
}
#[derive(Clone, Serialize, Deserialize)]
struct Snapshot {
    version: u32,
    revision: u64,
    account: String,
    key: String,
    anchor: i64,
    period_seconds: Option<u64>,
    period: BillingPeriod,
    usage: u64,
    state: BudgetState,
    credentials: BTreeMap<String, Credential>,
}
#[derive(Serialize, Deserialize)]
enum Event {
    Checkpoint(Box<Snapshot>),
    Budget {
        period: BillingPeriod,
        usage: u64,
        state: BudgetState,
    },
    Register(BTreeMap<String, Credential>),
    Remove(Vec<String>),
}
#[derive(Serialize, Deserialize)]
struct Record {
    revision: u64,
    event: Event,
}
impl Event {
    fn apply(self, data: &mut Snapshot) {
        match self {
            Self::Checkpoint(snapshot) => *data = *snapshot,
            Self::Budget {
                period,
                usage,
                state,
            } => {
                data.period = period;
                data.usage = usage;
                data.state = state;
            }
            Self::Register(entries) => data.credentials.extend(entries),
            Self::Remove(names) => {
                for name in names {
                    data.credentials.remove(&name);
                }
            }
        }
    }
}
/// A synced append journal with atomic checkpoint replacement. The sidecar lock
/// stays open across replacement and forbids multiple writers/processes.
struct Journal {
    path: PathBuf,
    _lock: File,
    serial: Mutex<JournalState>,
}
struct JournalState {
    revision: u64,
    broken: bool,
}
const MAX_REGISTRY: usize = 250_000;
const MAX_JOURNAL_BYTES: u64 = 256 * 1024 * 1024;
impl Journal {
    fn open(config: &BudgetConfig, data: &mut Snapshot) -> std::io::Result<Self> {
        let path = config.registry_path.clone();
        let mut lock_path = path.as_os_str().to_owned();
        lock_path.push(".lock");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(PathBuf::from(lock_path))?;
        lock.try_lock().map_err(std::io::Error::other)?;
        let journal = Self {
            path,
            _lock: lock,
            serial: Mutex::new(JournalState {
                revision: 0,
                broken: false,
            }),
        };
        let expected_key = data.key.clone();
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&journal.path)?;
        if file.metadata()?.len() > MAX_JOURNAL_BYTES {
            return Err(std::io::Error::other("registry size limit"));
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let complete = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
        // Only an unfinished final record may be discarded. Complete malformed
        // records fail startup instead of silently losing revocable credentials.
        for line in bytes[..complete]
            .split(|b| *b == b'\n')
            .filter(|l| !l.is_empty())
        {
            let record: Record = serde_json::from_slice(line)
                .map_err(|_| std::io::Error::other("invalid registry"))?;
            if record.revision >= data.revision {
                record.event.apply(data);
                data.revision = record.revision;
            }
            if data.credentials.len() > MAX_REGISTRY {
                return Err(std::io::Error::other("registry entry limit"));
            }
        }
        if complete < bytes.len() {
            file.set_len(complete as u64)?;
            file.sync_all()?;
        }
        if data.version != 1
            || data.account != config.account_id
            || data.anchor != config.billing_anchor.timestamp()
            || data.period_seconds != config.period_seconds
            || data.key != expected_key
        {
            return Err(std::io::Error::other("registry configuration mismatch"));
        }
        drop(file);
        journal.write(&Record {
            revision: data.revision,
            event: Event::Checkpoint(Box::new(data.clone())),
        })?;
        Ok(journal)
    }
    fn write(&self, record: &Record) -> std::io::Result<()> {
        let mut serial = self.serial.lock().unwrap();
        // Cancellation may leave an older blocking job alive. It cannot append
        // stale Normal/budget data after a newer checkpoint or hard transition.
        if record.revision < serial.revision {
            return Ok(());
        }
        let checkpoint = matches!(record.event, Event::Checkpoint(_));
        if !checkpoint && record.revision != serial.revision + 1 {
            serial.broken = true;
            return Err(std::io::Error::other(
                "registry requires ordered checkpoint",
            ));
        }
        if serial.broken && !checkpoint {
            return Err(std::io::Error::other("registry requires checkpoint"));
        }
        let result = self.write_record(record);
        if result.is_ok() {
            serial.revision = record.revision;
            serial.broken = false;
        } else {
            serial.broken = true;
        }
        result
    }
    fn write_record(&self, record: &Record) -> std::io::Result<()> {
        let mut bytes = serde_json::to_vec(record)
            .map_err(|_| std::io::Error::other("registry serialization"))?;
        bytes.push(b'\n');
        if matches!(record.event, Event::Checkpoint(_)) {
            let parent = self
                .path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(std::path::Path::new("."));
            let mut temp = tempfile::NamedTempFile::new_in(parent)?;
            temp.write_all(&bytes)?;
            temp.as_file().sync_all()?;
            temp.persist(&self.path).map_err(|error| error.error)?;
            #[cfg(unix)]
            File::open(parent)?.sync_all()?;
        } else {
            let mut file = OpenOptions::new().append(true).open(&self.path)?;
            if file.metadata()?.len() + bytes.len() as u64 > MAX_JOURNAL_BYTES {
                return Err(std::io::Error::other("registry size limit"));
            }
            file.seek(SeekFrom::End(0))?;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BudgetStatus {
    pub state: BudgetState,
    pub stale: bool,
    pub usage_bytes: u64,
    pub hard_generation: u64,
}
struct Gate {
    status: BudgetStatus,
    last_success: Option<Instant>,
    period_end: i64,
    period_start: i64,
    storage_ok: bool,
    capacity_ok: bool,
}
struct Retry {
    next: Instant,
    failures: u32,
}
struct Inner {
    data: Snapshot,
    writes: usize,
    dirty: bool,
    retries: BTreeMap<String, Retry>,
}
pub struct BudgetGuard {
    config: BudgetConfig,
    journal: Arc<Journal>,
    inner: tokio::sync::Mutex<Inner>,
    gate: Mutex<Gate>,
    changes: watch::Sender<BudgetStatus>,
    revoke_wakeup: Notify,
    sweep_gate: tokio::sync::Mutex<()>,
}
impl BudgetGuard {
    async fn open(config: BudgetConfig, key: String) -> std::io::Result<Arc<Self>> {
        config.validate().map_err(std::io::Error::other)?;
        let period = billing_period(config.billing_anchor, config.period_seconds, now());
        let mut data = Snapshot {
            version: 1,
            revision: 0,
            account: config.account_id.clone(),
            key: key.clone(),
            anchor: config.billing_anchor.timestamp(),
            period_seconds: config.period_seconds,
            period,
            usage: 0,
            state: BudgetState::Normal,
            credentials: BTreeMap::new(),
        };
        let c = config.clone();
        let (journal, mut data) = tokio::task::spawn_blocking(move || {
            let journal = Journal::open(&c, &mut data)?;
            if data.key != key {
                return Err(std::io::Error::other("registry key mismatch"));
            }
            data.credentials.retain(|_, c| c.expires_at > unix_now());
            journal.write(&Record {
                revision: data.revision,
                event: Event::Checkpoint(Box::new(data.clone())),
            })?;
            Ok::<_, std::io::Error>((journal, data))
        })
        .await
        .map_err(|_| std::io::Error::other("registry worker"))??;
        if data.period.start < period.start {
            if data.state == BudgetState::HardLimited {
                for credential in data.credentials.values_mut() {
                    credential.pending_revoke = true;
                }
            }
            data.period = period;
            data.usage = 0;
            data.state = BudgetState::Normal;
            tracing::info!(
                start = period.start,
                end = period.end,
                "TURN billing period changed at startup"
            );
        }
        let status = BudgetStatus {
            state: data.state,
            stale: true,
            usage_bytes: data.usage,
            hard_generation: 0,
        };
        let loaded_period = data.period;
        let (changes, _) = watch::channel(status);
        let guard = Arc::new(Self {
            config,
            journal: Arc::new(journal),
            inner: tokio::sync::Mutex::new(Inner {
                data,
                writes: 0,
                dirty: true,
                retries: BTreeMap::new(),
            }),
            gate: Mutex::new(Gate {
                status,
                last_success: None,
                period_end: loaded_period.end,
                period_start: loaded_period.start,
                storage_ok: true,
                capacity_ok: true,
            }),
            changes,
            revoke_wakeup: Notify::new(),
            sweep_gate: tokio::sync::Mutex::new(()),
        });
        Ok(guard)
    }
    fn allowed(&self, gate: &Gate) -> bool {
        gate.storage_ok
            && gate.capacity_ok
            && gate.status.state == BudgetState::Normal
            && gate
                .last_success
                .is_some_and(|t| t.elapsed() < self.config.stale_timeout)
            && (unix_now() as i64) < gate.period_end
            && (unix_now() as i64) >= gate.period_start
    }
    pub fn can_issue(&self) -> bool {
        self.allowed(&self.gate.lock().unwrap())
    }
    pub fn status(&self) -> BudgetStatus {
        let gate = self.gate.lock().unwrap();
        BudgetStatus {
            stale: gate
                .last_success
                .is_none_or(|t| t.elapsed() >= self.config.stale_timeout),
            ..gate.status
        }
    }
    pub(crate) fn subscribe(&self) -> watch::Receiver<BudgetStatus> {
        self.changes.subscribe()
    }
    async fn persist(&self, inner: &mut Inner, event: Event) -> bool {
        inner.data.revision += 1;
        let event = if inner.dirty || inner.writes >= 512 {
            Event::Checkpoint(Box::new(inner.data.clone()))
        } else {
            event
        };
        let checkpoint = matches!(event, Event::Checkpoint(_));
        let record = Record {
            revision: inner.data.revision,
            event,
        };
        let journal = self.journal.clone();
        let ok = tokio::task::spawn_blocking(move || journal.write(&record))
            .await
            .is_ok_and(|r| r.is_ok());
        inner.dirty = !ok;
        inner.writes = if checkpoint && ok {
            0
        } else {
            inner.writes + 1
        };
        self.gate.lock().unwrap().storage_ok = ok;
        if !ok {
            tracing::warn!("TURN registry write failed; issuance stopped");
        }
        ok
    }
    pub(super) async fn register(&self, turn: &TurnCredentials, expires_at: u64) -> bool {
        let mut inner = self.inner.lock().await;
        let mut entries = BTreeMap::new();
        for server in &turn.servers {
            let expiry = inner
                .data
                .credentials
                .get(&server.username)
                .map_or(expires_at, |c| c.expires_at.max(expires_at));
            let pending_revoke = inner
                .data
                .credentials
                .get(&server.username)
                .is_some_and(|c| c.pending_revoke);
            entries.insert(
                server.username.clone(),
                Credential {
                    expires_at: expiry,
                    pending_revoke,
                },
            );
        }
        if inner.data.credentials.len()
            + entries
                .keys()
                .filter(|n| !inner.data.credentials.contains_key(*n))
                .count()
            > MAX_REGISTRY
        {
            self.gate.lock().unwrap().capacity_ok = false;
            tracing::warn!("TURN registry capacity reached; issuance stopped");
            // Admission checks reserve headroom for at most 32 in-flight issues.
        }
        inner.data.credentials.extend(entries.clone());
        self.gate.lock().unwrap().capacity_ok = inner.data.credentials.len() < MAX_REGISTRY - 128;
        let pending = entries.values().any(|c| c.pending_revoke);
        self.persist(&mut inner, Event::Register(entries)).await && !pending && self.can_issue()
    }
    pub(crate) async fn discard(&self, turn: &TurnCredentials) {
        let mut inner = self.inner.lock().await;
        let mut entries = BTreeMap::new();
        for server in &turn.servers {
            if let Some(entry) = inner.data.credentials.get_mut(&server.username) {
                entry.pending_revoke = true;
                entries.insert(server.username.clone(), entry.clone());
            }
        }
        self.persist(&mut inner, Event::Register(entries)).await;
        self.revoke_wakeup.notify_one();
    }
    /// Final enqueue and budget transition are serialized. The callback performs
    /// only nonblocking publication; it must never await or do network I/O.
    pub(crate) async fn publish<T>(
        &self,
        turn: TurnCredentials,
        publish: impl FnOnce(Option<TurnCredentials>) -> T,
    ) -> T {
        let inner = self.inner.lock().await;
        {
            let gate = self.gate.lock().unwrap();
            if self.allowed(&gate) {
                return publish(Some(turn));
            }
        }
        drop(inner);
        self.discard(&turn).await;
        publish(None)
    }
    pub async fn refresh(&self, backend: &dyn AnalyticsBackend) {
        self.maintain().await;
        let period = self.inner.lock().await.data.period;
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            backend.usage(period, unix_now() as i64),
        )
        .await;
        let Ok(Ok(usage)) = result else {
            tracing::warn!("TURN Analytics query failure");
            self.maintain().await;
            return;
        };
        let mut inner = self.inner.lock().await;
        if inner.data.period != period
            || billing_period(
                self.config.billing_anchor,
                self.config.period_seconds,
                now(),
            ) != period
        {
            return; // A response for the previous period cannot unlock the next one.
        }
        inner.data.usage = inner.data.usage.max(usage);
        let previous = inner.data.state;
        if inner.data.usage >= self.config.hard_limit_bytes {
            inner.data.state = BudgetState::HardLimited;
        } else if inner.data.usage >= self.config.soft_limit_bytes
            && previous == BudgetState::Normal
        {
            inner.data.state = BudgetState::SoftLimited;
        }
        self.update_gate(&inner.data, Some(Instant::now())); // Close issuance BEFORE persisting/revoking.
        if previous != inner.data.state {
            tracing::info!(from = ?previous, to = ?inner.data.state, usage_bytes = inner.data.usage, "TURN budget transition");
            if inner.data.state == BudgetState::HardLimited {
                tracing::info!(
                    count = inner.data.credentials.len(),
                    "TURN hard limit revoke targets"
                );
                self.revoke_wakeup.notify_one();
            }
        }
        let event = Event::Budget {
            period,
            usage: inner.data.usage,
            state: inner.data.state,
        };
        self.persist(&mut inner, event).await;
    }
    fn update_gate(&self, data: &Snapshot, success: Option<Instant>) {
        let mut gate = self.gate.lock().unwrap();
        let old = gate.status;
        let period_changed = gate.period_end != data.period.end;
        if period_changed {
            gate.last_success = None;
        }
        if let Some(success) = success {
            gate.last_success = Some(success);
        }
        let stale = gate
            .last_success
            .is_none_or(|t| t.elapsed() >= self.config.stale_timeout);
        if old.stale != stale {
            if stale {
                tracing::warn!("TURN Analytics stale started; issuance stopped");
            } else {
                tracing::info!("TURN Analytics stale recovered");
            }
        }
        gate.status = BudgetStatus {
            state: data.state,
            stale,
            usage_bytes: data.usage,
            hard_generation: old.hard_generation
                + u64::from(
                    data.state == BudgetState::HardLimited && old.state != BudgetState::HardLimited,
                ),
        };
        gate.period_end = data.period.end;
        gate.period_start = data.period.start;
        gate.capacity_ok = data.credentials.len() < MAX_REGISTRY - 128;
        let notify =
            old.state != gate.status.state || old.stale != gate.status.stale || period_changed;
        // Usage-only samples must not wake rotation and mint credentials every poll.
        self.changes.send_if_modified(|status| {
            *status = gate.status;
            notify
        });
    }
    async fn maintain(&self) -> bool {
        let mut inner = self.inner.lock().await;
        let period = billing_period(
            self.config.billing_anchor,
            self.config.period_seconds,
            now(),
        );
        let changed = inner.data.period.start < period.start;
        if changed {
            // Keep revoke obligations for old-period credentials across reset.
            if inner.data.state == BudgetState::HardLimited {
                for credential in inner.data.credentials.values_mut() {
                    credential.pending_revoke = true;
                }
            }
            inner.data.period = period;
            inner.data.usage = 0;
            inner.data.state = BudgetState::Normal;
            self.gate.lock().unwrap().last_success = None;
            inner.dirty = true;
            tracing::info!(
                start = period.start,
                end = period.end,
                "TURN billing period changed"
            );
        }
        let expired: Vec<_> = inner
            .data
            .credentials
            .iter()
            .filter(|(_, c)| c.expires_at <= unix_now())
            .map(|(n, _)| n.clone())
            .collect();
        for name in &expired {
            inner.data.credentials.remove(name);
            inner.retries.remove(name);
        }
        self.update_gate(&inner.data, None);
        if inner.dirty || !expired.is_empty() {
            self.persist(&mut inner, Event::Remove(expired)).await;
        }
        changed
    }
    async fn revoke_batch(&self, provider: &dyn TurnProvider) {
        let _sweep = self.sweep_gate.lock().await;
        let names: Vec<_> = {
            let inner = self.inner.lock().await;
            inner
                .data
                .credentials
                .iter()
                .filter(|(name, c)| {
                    c.expires_at > unix_now()
                        && (c.pending_revoke || inner.data.state == BudgetState::HardLimited)
                        && inner
                            .retries
                            .get(*name)
                            .is_none_or(|r| r.next <= Instant::now())
                })
                .take(32)
                .map(|(n, _)| n.clone())
                .collect()
        };
        if names.is_empty() {
            return;
        }
        let results: Vec<_> = stream::iter(names.into_iter().map(|name| async move {
            let result = tokio::time::timeout(Duration::from_secs(3), provider.revoke(&name)).await;
            (name, matches!(result, Ok(Ok(()))))
        }))
        .buffer_unordered(4)
        .collect()
        .await;
        let mut inner = self.inner.lock().await;
        let mut removed = Vec::new();
        let mut failed = 0;
        for (name, ok) in results {
            if ok {
                inner.data.credentials.remove(&name);
                inner.retries.remove(&name);
                removed.push(name);
            } else {
                failed += 1;
                let retry = inner.retries.entry(name).or_insert(Retry {
                    next: Instant::now(),
                    failures: 0,
                });
                retry.failures = retry.failures.saturating_add(1);
                retry.next =
                    Instant::now() + Duration::from_secs((1u64 << retry.failures.min(9)).min(300));
            }
        }
        tracing::info!(
            success = removed.len(),
            failed,
            "TURN revoke batch completed"
        );
        if failed > 0 {
            tracing::warn!(failed, "TURN revoke failures retained for bounded retry");
        }
        if !removed.is_empty() {
            self.persist(&mut inner, Event::Remove(removed)).await;
        }
    }
}
fn now() -> DateTime<Utc> {
    DateTime::from_timestamp(unix_now() as i64, 0).unwrap()
}
pub(crate) struct MonitorTasks(Vec<tokio::task::JoinHandle<()>>);
impl Drop for MonitorTasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}
pub(crate) async fn start(
    config: BudgetConfig,
    key: String,
    backend: Arc<dyn AnalyticsBackend>,
    provider: Arc<dyn TurnProvider>,
) -> std::io::Result<(Arc<BudgetGuard>, Arc<MonitorTasks>)> {
    let guard = BudgetGuard::open(config, key).await?;
    let restored_hard = guard.status().state == BudgetState::HardLimited;
    tracing::info!("TURN budget monitor started; awaiting initial Analytics query");
    tracing::warn!("TURN Analytics stale started; awaiting initial observation");
    guard.refresh(backend.as_ref()).await;
    if restored_hard {
        tracing::info!(
            count = guard.inner.lock().await.data.credentials.len(),
            "TURN restored hard limit revoke targets"
        );
    }
    let polling = guard.clone();
    let monitor = tokio::spawn(async move {
        let mut next = Instant::now() + polling.config.polling_interval;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if polling.maintain().await || Instant::now() >= next {
                polling.refresh(backend.as_ref()).await;
                next = Instant::now() + polling.config.polling_interval;
            }
        }
    });
    let revoking = guard.clone();
    let revoker = tokio::spawn(async move {
        loop {
            revoking.revoke_batch(provider.as_ref()).await;
            tokio::select! { _ = tokio::time::sleep(Duration::from_secs(1)) => {}, _ = revoking.revoke_wakeup.notified() => {} }
        }
    });
    Ok((guard, Arc::new(MonitorTasks(vec![monitor, revoker]))))
}

#[cfg(test)]
mod tests;
