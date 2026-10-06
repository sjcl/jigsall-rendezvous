use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
struct Provider(AtomicUsize);
impl TurnProvider for Provider {
    fn issue<'a>(
        &'a self,
        _: &'a str,
        _: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<TurnServer>, TurnError>> + Send + 'a>> {
        Box::pin(async {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(vec![TurnServer {
                address: "turn.cloudflare.com:3478".into(),
                username: "secret-user".into(),
                password: "secret-password".into(),
            }])
        })
    }
}
fn config() -> TurnConfig {
    TurnConfig {
        key_id: "secret-key".into(),
        api_token: "secret-token".into(),
        ttl: Duration::from_secs(86400),
        concurrency: 1,
        requests_per_minute: 1,
    }
}
#[test]
fn udp_urls_remain_paired_with_their_own_credentials() {
    let body = br#"{"iceServers":[{"urls":["stun:stun.cloudflare.com:3478"]},{"urls":["turn:turn.cloudflare.com:3478?transport=udp","turn:turn.cloudflare.com:443?transport=udp","turn:turn.cloudflare.com:3478?transport=tcp","turns:turn.cloudflare.com:5349?transport=tcp"],"username":"a","credential":"b"},{"urls":["turn:other.example:3478?transport=udp"],"username":"c","credential":"d"}]}"#;
    let servers = parse_response(body).unwrap();
    assert_eq!(servers.len(), 3);
    assert_eq!(servers[0].username, "a");
    assert_eq!(servers[1].password, "b");
    assert_eq!(servers[2].password, "d");
    assert!(parse_response(br#"{"iceServers":[{"urls":["turn:x:3478?transport=udp"]}]}"#).is_err());
    assert!(parse_response(br#"{"iceServers":[]}"#).is_err());
}
#[tokio::test]
async fn global_rate_limit_and_debug_redaction() {
    let p = Arc::new(Provider(AtomicUsize::new(0)));
    let c = config();
    let service = TurnService::new(p.clone(), &c);
    let value = service.issue("random-connection-id").await.unwrap();
    assert_eq!(
        service.issue("other-connection-id").await,
        Err(TurnError::Limited)
    );
    assert_eq!(p.0.load(Ordering::SeqCst), 1);
    let debug = format!(
        "{c:?} {value:?} {:?}",
        ServerMessage::TurnCredentials {
            turn: value.clone()
        }
    );
    for secret in [
        "secret-key",
        "secret-token",
        "secret-user",
        "secret-password",
    ] {
        assert!(!debug.contains(secret));
    }
}
#[tokio::test]
async fn concurrency_waits_for_a_permit_and_cancellation_releases_it() {
    let mut c = config();
    c.requests_per_minute = 100;
    let provider = Arc::new(Provider(AtomicUsize::new(0)));
    let service = TurnService::new(provider.clone(), &c);
    let held = service.permits.acquire().await.unwrap();
    let copy = service.clone();
    let waiting = tokio::spawn(async move { copy.issue("waiting").await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!waiting.is_finished());
    assert_eq!(provider.0.load(Ordering::SeqCst), 0);
    drop(held);
    assert!(waiting.await.unwrap().is_ok());
    assert_eq!(provider.0.load(Ordering::SeqCst), 1);

    let held = service.permits.acquire().await.unwrap();
    let copy = service.clone();
    let waiting = tokio::spawn(async move { copy.issue("cancelled").await });
    tokio::task::yield_now().await;
    waiting.abort();
    let _ = waiting.await;
    drop(held);
    assert_eq!(service.permits.available_permits(), 1);
    assert_eq!(provider.0.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn issuance_deadline_includes_permit_wait_and_http() {
    struct Slow;
    impl TurnProvider for Slow {
        fn issue<'a>(
            &'a self,
            _: &'a str,
            _: Duration,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<TurnServer>, TurnError>> + Send + 'a>> {
            Box::pin(async {
                tokio::time::sleep(Duration::from_secs(2)).await;
                Err(TurnError::InvalidResponse)
            })
        }
    }
    let service = TurnService::new(Arc::new(Slow), &config());
    let held = service.permits.clone().acquire_owned().await.unwrap();
    let releasing = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        drop(held);
    });
    let start = Instant::now();
    assert_eq!(service.issue("one").await, Err(TurnError::Unavailable));
    assert!(start.elapsed() < Duration::from_millis(3400));
    releasing.await.unwrap();
    assert_eq!(service.permits.available_permits(), 1);
}
#[test]
fn rotation_endpoint_set_is_order_independent_and_exact() {
    let initial = TurnCredentials {
        expires_at_unix: 1,
        servers: vec![
            TurnServer {
                address: "one.example:3478".into(),
                username: "A".into(),
                password: "A".into(),
            },
            TurnServer {
                address: "two.example:443".into(),
                username: "A".into(),
                password: "A".into(),
            },
        ],
    };
    let mut next = initial.clone();
    next.servers.reverse();
    next.servers[0].password = "B".into();
    assert!(initial.same_server_set(&next));
    next.servers[1].address = "changed.example:3478".into();
    assert!(!initial.same_server_set(&next));
    next.servers.pop();
    assert!(!initial.same_server_set(&next));
}

#[tokio::test]
async fn http_provider_posts_only_ttl_and_random_identifier_and_sanitizes_failures() {
    use axum::{routing::post, Json};
    let app = axum::Router::new().route("/credentials", post(|headers: axum::http::HeaderMap, Json(body): Json<serde_json::Value>| async move {
        assert_eq!(headers["authorization"], "Bearer fixture-token");
        assert_eq!(body, serde_json::json!({"ttl":86400,"customIdentifier":"random-id"}));
        (axum::http::StatusCode::CREATED, Json(serde_json::json!({"iceServers":[{"urls":["turn:turn.cloudflare.com:3478?transport=udp"],"username":"short-user","credential":"short-password"}]})))
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let provider = CloudflareTurnProvider {
        client: reqwest::Client::new(),
        endpoint: format!("http://{address}/credentials"),
        token: "fixture-token".into(),
    };
    let result = provider
        .issue("random-id", Duration::from_secs(86400))
        .await
        .unwrap();
    assert_eq!(result[0].address, "turn.cloudflare.com:3478");
    let bad = CloudflareTurnProvider {
        client: reqwest::Client::new(),
        endpoint: format!("http://{address}/missing"),
        token: "fixture-token".into(),
    };
    assert_eq!(
        bad.issue("random-id", Duration::from_secs(86400))
            .await
            .err(),
        Some(TurnError::Unavailable)
    );
    task.abort();
}

#[tokio::test]
async fn http_revoke_uses_encoded_username_path_turn_token_and_no_secrets_in_errors() {
    use axum::{extract::Path, routing::post};
    let app = axum::Router::new().route(
        "/keys/key/credentials/{username}/revoke",
        post(
            |Path(username): Path<String>,
             headers: axum::http::HeaderMap,
             body: axum::body::Bytes| async move {
                assert_eq!(headers["authorization"], "Bearer fixture-turn-token");
                assert!(body.is_empty());
                if username == "user/with?reserved#chars" {
                    axum::http::StatusCode::NO_CONTENT
                } else {
                    axum::http::StatusCode::SERVICE_UNAVAILABLE
                }
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let provider = CloudflareTurnProvider {
        client: reqwest::Client::new(),
        endpoint: format!("http://{address}/keys/key/credentials/generate-ice-servers"),
        token: "fixture-turn-token".into(),
    };
    assert_eq!(provider.revoke("user/with?reserved#chars").await, Ok(()));
    assert_eq!(
        provider.revoke("failure").await,
        Err(TurnError::Unavailable)
    );
    task.abort();
}
#[tokio::test]
#[ignore = "manual: requires explicitly configured Cloudflare TURN secrets"]
async fn cloudflare_manual_credential_issue() {
    let Some(config) = crate::Config::from_env().unwrap().turn else {
        return;
    };
    let provider = CloudflareTurnProvider::new(&config).unwrap();
    let value = provider
        .issue(&uuid::Uuid::new_v4().simple().to_string(), config.ttl)
        .await
        .unwrap();
    assert!(!value.is_empty());
}
