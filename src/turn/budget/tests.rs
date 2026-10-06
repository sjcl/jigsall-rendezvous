use super::*;
use crate::{
    protocol::TurnServer,
    turn::{TurnConfig, TurnService},
};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

fn config(path: PathBuf) -> BudgetConfig {
    BudgetConfig {
        account_id: "a".repeat(32),
        analytics_token: "secret-analytics-token".into(),
        soft_limit_bytes: 100,
        hard_limit_bytes: 200,
        polling_interval: Duration::from_secs(30),
        stale_timeout: Duration::from_secs(300),
        billing_anchor: "2020-01-17T12:34:56Z".parse().unwrap(),
        period_seconds: None,
        registry_path: path,
    }
}
fn turn() -> TurnCredentials {
    TurnCredentials {
        expires_at_unix: unix_now() + 600,
        servers: vec![
            TurnServer {
                address: "turn.cloudflare.com:3478".into(),
                username: "secret-user".into(),
                password: "secret-password".into(),
            },
            TurnServer {
                address: "turn.cloudflare.com:443".into(),
                username: "secret-user".into(),
                password: "secret-password".into(),
            },
        ],
    }
}
fn turn_config() -> TurnConfig {
    TurnConfig {
        key_id: "key".into(),
        api_token: "secret-token".into(),
        ttl: Duration::from_secs(600),
        concurrency: 4,
        requests_per_minute: 100,
    }
}
struct Analytics {
    bytes: AtomicU64,
    failed: AtomicBool,
}
impl Analytics {
    fn new(bytes: u64) -> Self {
        Self {
            bytes: AtomicU64::new(bytes),
            failed: AtomicBool::new(false),
        }
    }
}
impl AnalyticsBackend for Analytics {
    fn usage<'a>(
        &'a self,
        _: BillingPeriod,
        _: i64,
    ) -> Pin<Box<dyn Future<Output = Result<u64, TurnError>> + Send + 'a>> {
        Box::pin(async move {
            if self.failed.load(Ordering::SeqCst) {
                Err(TurnError::Unavailable)
            } else {
                Ok(self.bytes.load(Ordering::SeqCst))
            }
        })
    }
}
#[derive(Default)]
struct Provider {
    issues: AtomicUsize,
    revokes: AtomicUsize,
    fail_revoke: AtomicBool,
}
impl TurnProvider for Provider {
    fn issue<'a>(
        &'a self,
        _: &'a str,
        _: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<TurnServer>, TurnError>> + Send + 'a>> {
        Box::pin(async move {
            self.issues.fetch_add(1, Ordering::SeqCst);
            Ok(turn().servers)
        })
    }
    fn revoke<'a>(
        &'a self,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), TurnError>> + Send + 'a>> {
        Box::pin(async move {
            self.revokes.fetch_add(1, Ordering::SeqCst);
            if self.fail_revoke.load(Ordering::SeqCst) {
                Err(TurnError::Unavailable)
            } else {
                Ok(())
            }
        })
    }
}
async fn guard(dir: &tempfile::TempDir) -> Arc<BudgetGuard> {
    BudgetGuard::open(config(dir.path().join("registry.jsonl")), "key".into())
        .await
        .unwrap()
}
async fn service(
    dir: &tempfile::TempDir,
    provider: Arc<dyn TurnProvider>,
    analytics: Arc<Analytics>,
) -> TurnService {
    TurnService::new(provider, &turn_config())
        .with_budget(
            config(dir.path().join("registry.jsonl")),
            "key".into(),
            analytics,
        )
        .await
        .unwrap()
}
#[test]
fn calendar_anchor_leap_year_short_month_and_exact_boundary() {
    let a = "2024-01-31T13:14:15Z".parse().unwrap();
    let cases = [
        (
            "2024-02-29T13:14:14Z",
            "2024-01-31T13:14:15Z",
            "2024-02-29T13:14:15Z",
        ),
        (
            "2024-02-29T13:14:15Z",
            "2024-02-29T13:14:15Z",
            "2024-03-31T13:14:15Z",
        ),
        (
            "2025-02-28T13:14:15Z",
            "2025-02-28T13:14:15Z",
            "2025-03-31T13:14:15Z",
        ),
        (
            "2023-12-31T13:14:14Z",
            "2023-11-30T13:14:15Z",
            "2023-12-31T13:14:15Z",
        ),
    ];
    for (now, start, end) in cases {
        assert_eq!(
            billing_period(a, None, now.parse().unwrap()),
            BillingPeriod {
                start: start.parse::<DateTime<Utc>>().unwrap().timestamp(),
                end: end.parse::<DateTime<Utc>>().unwrap().timestamp()
            }
        );
    }
    assert_eq!(
        billing_period(a, Some(30 * 86400), a - chrono::Duration::seconds(1)).end,
        a.timestamp()
    );
}
#[tokio::test]
async fn normal_soft_hard_and_decreasing_usage_are_monotonic() {
    let dir = tempfile::tempdir().unwrap();
    let g = guard(&dir).await;
    let a = Analytics::new(0);
    let p = Provider::default();
    assert!(!g.can_issue());
    g.refresh(&a).await;
    assert!(g.register(&turn(), unix_now() + 600).await);
    assert_eq!(g.inner.lock().await.data.credentials.len(), 1);
    a.bytes.store(100, Ordering::SeqCst);
    g.refresh(&a).await;
    assert_eq!(g.status().state, BudgetState::SoftLimited);
    assert!(!g.can_issue());
    g.revoke_batch(&p).await;
    assert_eq!(p.revokes.load(Ordering::SeqCst), 0);
    a.bytes.store(20, Ordering::SeqCst);
    g.refresh(&a).await;
    assert_eq!(g.status().usage_bytes, 100);
    assert_eq!(g.status().state, BudgetState::SoftLimited);
    a.bytes.store(200, Ordering::SeqCst);
    g.refresh(&a).await;
    assert_eq!(g.status().state, BudgetState::HardLimited);
    g.revoke_batch(&p).await;
    assert_eq!(p.revokes.load(Ordering::SeqCst), 1);
    assert!(g.inner.lock().await.data.credentials.is_empty());
    a.bytes.store(0, Ordering::SeqCst);
    g.refresh(&a).await;
    assert_eq!(g.status().state, BudgetState::HardLimited);
    assert_eq!(g.status().usage_bytes, 200);
}
#[tokio::test]
async fn stale_failure_and_recovery_do_not_revoke_or_unlatch_limits() {
    let dir = tempfile::tempdir().unwrap();
    let g = guard(&dir).await;
    let a = Analytics::new(0);
    a.failed.store(true, Ordering::SeqCst);
    g.refresh(&a).await;
    assert!(!g.can_issue());
    a.failed.store(false, Ordering::SeqCst);
    g.refresh(&a).await;
    assert!(g.can_issue());
    g.register(&turn(), unix_now() + 600).await;
    a.failed.store(true, Ordering::SeqCst);
    g.refresh(&a).await;
    assert!(g.can_issue()); // A transient failure alone is not stale.
    g.gate.lock().unwrap().last_success = Some(Instant::now() - Duration::from_secs(301));
    assert!(!g.can_issue()); // Checks enforce timeout even before maintenance runs.
    g.maintain().await;
    assert!(g.status().stale);
    let p = Provider::default();
    g.revoke_batch(&p).await;
    assert_eq!(p.revokes.load(Ordering::SeqCst), 0);
    a.failed.store(false, Ordering::SeqCst);
    g.refresh(&a).await;
    assert!(g.can_issue());
    a.bytes.store(100, Ordering::SeqCst);
    g.refresh(&a).await;
    g.gate.lock().unwrap().last_success = None;
    a.bytes.store(0, Ordering::SeqCst);
    g.refresh(&a).await;
    assert!(!g.status().stale);
    assert_eq!(g.status().state, BudgetState::SoftLimited);
    assert!(!g.can_issue());
}
#[tokio::test]
async fn bounded_retry_keeps_failure_and_continues_other_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let g = guard(&dir).await;
    let a = Analytics::new(0);
    g.refresh(&a).await;
    g.register(&turn(), unix_now() + 600).await;
    a.bytes.store(200, Ordering::SeqCst);
    g.refresh(&a).await;
    let p = Provider::default();
    p.fail_revoke.store(true, Ordering::SeqCst);
    g.revoke_batch(&p).await;
    assert_eq!(g.inner.lock().await.data.credentials.len(), 1);
    g.revoke_batch(&p).await;
    assert_eq!(p.revokes.load(Ordering::SeqCst), 1);
    g.inner
        .lock()
        .await
        .retries
        .get_mut("secret-user")
        .unwrap()
        .next = Instant::now();
    p.fail_revoke.store(false, Ordering::SeqCst);
    g.revoke_batch(&p).await;
    assert_eq!(p.revokes.load(Ordering::SeqCst), 2);
    assert!(g.inner.lock().await.data.credentials.is_empty());
}

#[tokio::test]
async fn revoke_sweeps_bound_batch_concurrency_and_continue_after_individual_failure() {
    struct Concurrent {
        active: AtomicUsize,
        peak: AtomicUsize,
        calls: AtomicUsize,
    }
    impl TurnProvider for Concurrent {
        fn issue<'a>(
            &'a self,
            _: &'a str,
            _: Duration,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<TurnServer>, TurnError>> + Send + 'a>> {
            Box::pin(async { Err(TurnError::Unavailable) })
        }
        fn revoke<'a>(
            &'a self,
            username: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<(), TurnError>> + Send + 'a>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(active, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(5)).await;
                self.active.fetch_sub(1, Ordering::SeqCst);
                if username == "user-000" {
                    Err(TurnError::Unavailable)
                } else {
                    Ok(())
                }
            })
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let g = guard(&dir).await;
    g.refresh(&Analytics::new(0)).await;
    for i in 0..70 {
        let mut t = turn();
        for s in &mut t.servers {
            s.username = format!("user-{i:03}");
        }
        assert!(g.register(&t, unix_now() + 600).await);
    }
    g.refresh(&Analytics::new(200)).await;
    let p = Concurrent {
        active: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
        calls: AtomicUsize::new(0),
    };
    g.revoke_batch(&p).await;
    assert_eq!(p.calls.load(Ordering::SeqCst), 32);
    assert_eq!(p.peak.load(Ordering::SeqCst), 4);
    assert_eq!(g.inner.lock().await.data.credentials.len(), 39);
    g.revoke_batch(&p).await;
    assert_eq!(p.calls.load(Ordering::SeqCst), 64);
    assert_eq!(g.inner.lock().await.data.credentials.len(), 7);
    g.revoke_batch(&p).await;
    assert_eq!(g.inner.lock().await.data.credentials.len(), 1);
    assert!(g
        .inner
        .lock()
        .await
        .data
        .credentials
        .contains_key("user-000"));
}

#[tokio::test]
async fn obsolete_blocking_write_cannot_restore_normal_after_hard_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let g = guard(&dir).await;
    g.refresh(&Analytics::new(200)).await;
    let inner = g.inner.lock().await;
    let revision = inner.data.revision;
    g.journal
        .write(&Record {
            revision: revision - 1,
            event: Event::Budget {
                period: inner.data.period,
                usage: 0,
                state: BudgetState::Normal,
            },
        })
        .unwrap();
    drop(inner);
    drop(g);
    let restored = guard(&dir).await;
    assert_eq!(restored.status().state, BudgetState::HardLimited);
    assert_eq!(restored.status().usage_bytes, 200);
}

#[tokio::test]
async fn maintenance_cleans_expired_registry_entries() {
    let dir = tempfile::tempdir().unwrap();
    let g = guard(&dir).await;
    g.refresh(&Analytics::new(0)).await;
    g.register(&turn(), unix_now() - 1).await;
    g.maintain().await;
    assert!(g.inner.lock().await.data.credentials.is_empty());
}

#[tokio::test]
async fn usage_only_samples_do_not_trigger_extra_rotations() {
    let dir = tempfile::tempdir().unwrap();
    let g = guard(&dir).await;
    g.refresh(&Analytics::new(0)).await;
    let changes = g.subscribe();
    g.refresh(&Analytics::new(10)).await;
    assert_eq!(g.status().usage_bytes, 10);
    assert!(!changes.has_changed().unwrap());
    g.refresh(&Analytics::new(100)).await;
    assert!(changes.has_changed().unwrap());
}
#[tokio::test]
async fn restart_restores_registry_latches_and_cleans_expired_and_torn_tail() {
    let dir = tempfile::tempdir().unwrap();
    let g = guard(&dir).await;
    g.refresh(&Analytics::new(0)).await;
    g.register(&turn(), unix_now() + 600).await;
    let mut expired = turn();
    for s in &mut expired.servers {
        s.username = "expired".into();
    }
    g.register(&expired, unix_now() - 1).await;
    g.refresh(&Analytics::new(200)).await;
    let path = g.config.registry_path.clone();
    drop(g);
    OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"{partial")
        .unwrap();
    let g = guard(&dir).await;
    assert_eq!(g.status().state, BudgetState::HardLimited);
    assert_eq!(g.inner.lock().await.data.credentials.len(), 1);
    let bytes = std::fs::read_to_string(path).unwrap();
    assert!(!bytes.contains("password"));
    assert!(!bytes.contains("token"));
    assert!(!bytes.contains("partial"));
    g.refresh(&Analytics::new(0)).await;
    assert_eq!(g.status().usage_bytes, 200);
    let p = Provider::default();
    g.revoke_batch(&p).await;
    assert_eq!(p.revokes.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn new_period_resets_usage_state_and_stale_without_losing_revoke_debt() {
    let dir = tempfile::tempdir().unwrap();
    let g = guard(&dir).await;
    g.refresh(&Analytics::new(0)).await;
    g.register(&turn(), unix_now() + 600).await;
    g.refresh(&Analytics::new(200)).await;
    // Simulate an old persisted period. The pure boundary calculator has separate tests.
    g.inner.lock().await.data.period.start -= 86400;
    assert!(g.maintain().await);
    assert_eq!(g.status().state, BudgetState::Normal);
    assert_eq!(g.status().usage_bytes, 0);
    assert!(g.status().stale);
    assert!(!g.can_issue());
    g.refresh(&Analytics::new(0)).await;
    assert!(g.can_issue());
    g.revoke_batch(&Provider::default()).await;
    assert!(g.inner.lock().await.data.credentials.is_empty());
}
#[tokio::test]
async fn single_writer_and_corrupt_storage_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let g = guard(&dir).await;
    assert!(BudgetGuard::open(g.config.clone(), "key".into())
        .await
        .is_err());
    let path = g.config.registry_path.clone();
    drop(g);
    let mut c = config(path.clone());
    c.account_id = "b".repeat(32);
    assert!(BudgetGuard::open(c, "key".into()).await.is_err());
    assert!(
        BudgetGuard::open(config(path.clone()), "different-key".into())
            .await
            .is_err()
    );
    OpenOptions::new()
        .append(true)
        .open(path)
        .unwrap()
        .write_all(b"bad-record\n")
        .unwrap();
    assert!(
        BudgetGuard::open(config(dir.path().join("registry.jsonl")), "key".into())
            .await
            .is_err()
    );
}
#[tokio::test]
async fn service_blocks_soft_hard_stale_before_provider_and_disabled_is_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let p = Arc::new(Provider::default());
    let a = Arc::new(Analytics::new(0));
    let s = service(&dir, p.clone(), a.clone()).await;
    assert!(s.issue("one").await.is_ok());
    a.bytes.store(100, Ordering::SeqCst);
    s.budget().unwrap().refresh(a.as_ref()).await;
    assert_eq!(s.issue("two").await, Err(TurnError::Unavailable));
    a.bytes.store(200, Ordering::SeqCst);
    s.budget().unwrap().refresh(a.as_ref()).await;
    assert_eq!(s.issue("three").await, Err(TurnError::Unavailable));
    assert_eq!(p.issues.load(Ordering::SeqCst), 1);
    assert!(TurnService::new(p.clone(), &turn_config())
        .issue("disabled")
        .await
        .is_ok());
    assert_eq!(p.issues.load(Ordering::SeqCst), 2);
    let dir2 = tempfile::tempdir().unwrap();
    a.failed.store(true, Ordering::SeqCst);
    let stale = service(&dir2, p.clone(), a.clone()).await;
    assert_eq!(
        stale.issue("startup-failed").await,
        Err(TurnError::Unavailable)
    );
    assert_eq!(p.issues.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn in_flight_issue_is_registered_revoked_and_never_published_on_hard_or_soft() {
    for usage in [100, 200] {
        struct Paused {
            started: Notify,
            release: Notify,
            revokes: AtomicUsize,
        }
        impl TurnProvider for Paused {
            fn issue<'a>(
                &'a self,
                _: &'a str,
                _: Duration,
            ) -> Pin<Box<dyn Future<Output = Result<Vec<TurnServer>, TurnError>> + Send + 'a>>
            {
                Box::pin(async move {
                    self.started.notify_one();
                    self.release.notified().await;
                    Ok(turn().servers)
                })
            }
            fn revoke<'a>(
                &'a self,
                _: &'a str,
            ) -> Pin<Box<dyn Future<Output = Result<(), TurnError>> + Send + 'a>> {
                Box::pin(async move {
                    self.revokes.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let p = Arc::new(Paused {
            started: Notify::new(),
            release: Notify::new(),
            revokes: AtomicUsize::new(0),
        });
        let a = Arc::new(Analytics::new(0));
        let s = service(&dir, p.clone(), a.clone()).await;
        let copy = s.clone();
        let task = tokio::spawn(async move { copy.issue("race").await });
        p.started.notified().await;
        a.bytes.store(usage, Ordering::SeqCst);
        s.budget().unwrap().refresh(a.as_ref()).await;
        s.budget().unwrap().revoke_batch(p.as_ref()).await; // Sweep before HTTP completes.
        p.release.notify_one();
        assert_eq!(task.await.unwrap(), Err(TurnError::Unavailable));
        s.budget().unwrap().revoke_batch(p.as_ref()).await;
        assert_eq!(p.revokes.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn transition_after_issue_before_enqueue_cannot_publish_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let p = Arc::new(Provider::default());
    let a = Arc::new(Analytics::new(0));
    let s = service(&dir, p, a.clone()).await;
    let value = s.issue("race").await.unwrap();
    a.bytes.store(200, Ordering::SeqCst);
    s.budget().unwrap().refresh(a.as_ref()).await;
    assert!(s.publish(value, |value| value).await.is_none());
}

#[tokio::test]
async fn cancelled_session_wait_does_not_drop_successful_issuance_bookkeeping() {
    struct Paused {
        started: Notify,
        release: Notify,
        revokes: AtomicUsize,
    }
    impl TurnProvider for Paused {
        fn issue<'a>(
            &'a self,
            _: &'a str,
            _: Duration,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<TurnServer>, TurnError>> + Send + 'a>> {
            Box::pin(async move {
                self.started.notify_one();
                self.release.notified().await;
                Ok(turn().servers)
            })
        }
        fn revoke<'a>(
            &'a self,
            _: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<(), TurnError>> + Send + 'a>> {
            Box::pin(async move {
                self.revokes.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let p = Arc::new(Paused {
        started: Notify::new(),
        release: Notify::new(),
        revokes: AtomicUsize::new(0),
    });
    let a = Arc::new(Analytics::new(0));
    let s = service(&dir, p.clone(), a.clone()).await;
    let copy = s.clone();
    let task = tokio::spawn(async move { copy.issue("cancelled").await });
    p.started.notified().await;
    task.abort();
    let _ = task.await;
    a.bytes.store(200, Ordering::SeqCst);
    s.budget().unwrap().refresh(a.as_ref()).await;
    p.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while p.revokes.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(p.revokes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn old_period_analytics_response_cannot_unlock_reset_period() {
    struct Paused {
        started: Notify,
        release: Notify,
    }
    impl AnalyticsBackend for Paused {
        fn usage<'a>(
            &'a self,
            _: BillingPeriod,
            _: i64,
        ) -> Pin<Box<dyn Future<Output = Result<u64, TurnError>> + Send + 'a>> {
            Box::pin(async move {
                self.started.notify_one();
                self.release.notified().await;
                Ok(0)
            })
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let g = guard(&dir).await;
    let a = Arc::new(Paused {
        started: Notify::new(),
        release: Notify::new(),
    });
    let copy = g.clone();
    let backend = a.clone();
    let task = tokio::spawn(async move { copy.refresh(backend.as_ref()).await });
    a.started.notified().await;
    // Simulate a boundary occurring while this old-period request is in flight.
    g.inner.lock().await.data.period.start -= 86400;
    a.release.notify_one();
    task.await.unwrap();
    assert!(!g.can_issue());
    assert!(g.status().stale);
}
#[tokio::test]
async fn analytics_http_aggregate_filter_token_and_response_validation() {
    use axum::{routing::post, Json};
    let app = axum::Router::new().route("/graphql", post(|headers: axum::http::HeaderMap, Json(body): Json<serde_json::Value>| async move {
        assert_eq!(headers["authorization"], "Bearer secret-analytics-token");
        assert_eq!(body["query"], QUERY);
        assert!(!QUERY.contains("dimensions"));
        assert!(QUERY.contains("limit: 1"));
        assert_eq!(body["variables"]["key"], "key");
        assert_eq!(body["variables"]["start"], "2026-09-17T12:34:56+00:00");
        assert_eq!(body["variables"]["end"], "2026-10-01T00:00:00+00:00");
        Json(serde_json::json!({"data":{"viewer":{"accounts":[{"callsTurnUsageAdaptiveGroups":[{"sum":{"egressBytes":123.4}}]}]}},"errors":null}))
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let a = CloudflareAnalytics {
        client: reqwest::Client::new(),
        endpoint: format!("http://{address}/graphql"),
        account: "a".repeat(32),
        key: "key".into(),
        token: "secret-analytics-token".into(),
    };
    let date = "2026-10-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
    let period = billing_period("2020-01-17T12:34:56Z".parse().unwrap(), None, date);
    assert_eq!(a.usage(period, date.timestamp()).await, Ok(124));
    task.abort();
    for body in [br#"{"errors":[{"message":"secret"}]}"#.as_slice(), b"{}",
        br#"{"data":{"viewer":{"accounts":[{"callsTurnUsageAdaptiveGroups":[{"sum":{"egressBytes":-1}}]}]}}}"#] {
        assert!(parse_usage(body).is_err());
    }
    assert_eq!(
        parse_usage(br#"{"data":{"viewer":{"accounts":[{"callsTurnUsageAdaptiveGroups":[]}]}}}"#),
        Ok(0)
    );
}
