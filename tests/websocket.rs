use futures_util::{SinkExt, StreamExt};
use puzzella_rendezvous::{protocol::*, serve, Limits, Server};
use std::time::Duration;
use tokio::{net::TcpListener, sync::oneshot, time::timeout};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Error, Message},
};
type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
struct Fixture {
    url: String,
    stop: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}
impl Fixture {
    async fn new(limits: Limits) -> Self {
        Self::with_server(Server::new(limits)).await
    }
    async fn with_server(server: Server) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(serve(listener, server, async {
            let _ = stopped.await;
        }));
        Self {
            url: format!("ws://{address}/v1/ws"),
            stop: Some(stop),
            task,
        }
    }
    async fn connect(&self) -> Socket {
        self.connect_from(None).await.unwrap()
    }
    async fn connect_from(&self, forwarded: Option<&str>) -> Result<Socket, Error> {
        let mut request = self.url.clone().into_client_request().unwrap();
        if let Some(ip) = forwarded {
            request
                .headers_mut()
                .insert("x-forwarded-for", ip.parse().unwrap());
        }
        let (mut socket, _) = connect_async(request).await?;
        assert!(matches!(
            receive(&mut socket).await,
            ServerMessage::Welcome { .. }
        ));
        Ok(socket)
    }
    async fn shutdown(mut self) {
        let _ = self.stop.take().unwrap().send(());
        timeout(Duration::from_secs(5), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
async fn receive(socket: &mut Socket) -> ServerMessage {
    timeout(Duration::from_secs(3), async {
        loop {
            match socket.next().await.unwrap().unwrap() {
                Message::Text(text) => break parse_server(&text).unwrap(),
                Message::Ping(_) => socket.flush().await.unwrap(),
                other => panic!("unexpected {other:?}"),
            }
        }
    })
    .await
    .unwrap()
}
async fn send(socket: &mut Socket, message: ClientMessage) {
    socket
        .send(Message::Text(
            serde_json::to_string(&Frame::new(message)).unwrap().into(),
        ))
        .await
        .unwrap();
}
#[tokio::test]
async fn rejected_pending_join_releases_slot_and_host_accepts_same_peer_again() {
    let f = Fixture::new(Limits {
        participants: 1,
        ..Limits::default()
    })
    .await;
    let mut host = f.connect().await;
    send(
        &mut host,
        ClientMessage::CreateRoom {
            peer_id: PeerId([1; 16]),
        },
    )
    .await;
    let ServerMessage::RoomCreated { room_code, .. } = receive(&mut host).await else {
        panic!()
    };
    let mut old_member = None;
    for reject in [true, false] {
        let mut joiner = f.connect().await;
        send(
            &mut joiner,
            ClientMessage::JoinRoom {
                room_code: room_code.clone(),
                peer_id: PeerId([2; 16]),
            },
        )
        .await;
        let ServerMessage::AuthorizePeer {
            join_id, member_id, ..
        } = receive(&mut host).await
        else {
            panic!()
        };
        if reject {
            old_member = Some(member_id);
            send(&mut host, ClientMessage::AuthorizeReject { join_id }).await;
            assert_eq!(
                receive(&mut joiner).await,
                ServerMessage::Error {
                    code: ErrorCode::Capacity
                }
            );
            assert_eq!(
                receive(&mut host).await,
                ServerMessage::PeerUnavailable {
                    peer_id: PeerId([2; 16])
                }
            );
        } else {
            assert_ne!(old_member, Some(member_id));
            send(&mut host, ClientMessage::AuthorizeAck { join_id }).await;
            assert_eq!(
                receive(&mut host).await,
                ServerMessage::PeerJoined {
                    peer_id: PeerId([2; 16]),
                    member_id
                }
            );
            assert!(matches!(
                receive(&mut joiner).await,
                ServerMessage::RoomJoined { .. }
            ));
            send(
                &mut host,
                ClientMessage::ConfirmPeer {
                    peer_id: PeerId([2; 16]),
                    member_id,
                },
            )
            .await;
            send(
                &mut joiner,
                ClientMessage::Signal {
                    to_peer_id: PeerId([1; 16]),
                    payload_base64: "AP8H".into(),
                },
            )
            .await;
            assert_eq!(
                receive(&mut host).await,
                ServerMessage::Signal {
                    from_peer_id: PeerId([2; 16]),
                    payload_base64: "AP8H".into()
                }
            );
        }
    }
    f.shutdown().await;
}
#[tokio::test]
async fn ponging_routed_member_expires_but_confirmed_member_keeps_routing() {
    for confirmed in [false, true] {
        let f = Fixture::new(Limits {
            participants: 1,
            game_auth_timeout: Duration::from_millis(350),
            heartbeat_interval: Duration::from_millis(40),
            heartbeat_timeout: Duration::from_millis(200),
            ..Limits::default()
        })
        .await;
        let mut host = f.connect().await;
        let mut joiner = f.connect().await;
        send(
            &mut host,
            ClientMessage::CreateRoom {
                peer_id: PeerId([1; 16]),
            },
        )
        .await;
        let ServerMessage::RoomCreated { room_code, .. } = receive(&mut host).await else {
            panic!()
        };
        send(
            &mut joiner,
            ClientMessage::JoinRoom {
                room_code: room_code.clone(),
                peer_id: PeerId([2; 16]),
            },
        )
        .await;
        let ServerMessage::AuthorizePeer {
            join_id, member_id, ..
        } = receive(&mut host).await
        else {
            panic!()
        };
        send(&mut host, ClientMessage::AuthorizeAck { join_id }).await;
        assert!(matches!(
            receive(&mut host).await,
            ServerMessage::PeerJoined { .. }
        ));
        assert!(matches!(
            receive(&mut joiner).await,
            ServerMessage::RoomJoined { .. }
        ));
        if confirmed {
            send(
                &mut host,
                ClientMessage::ConfirmPeer {
                    peer_id: PeerId([2; 16]),
                    member_id,
                },
            )
            .await;
        }
        // receive() flushes automatic Pongs; both sockets keep their heartbeat.
        let (host_message, joiner_message) = tokio::join!(
            timeout(Duration::from_millis(550), receive(&mut host)),
            timeout(Duration::from_millis(550), receive(&mut joiner)),
        );
        if confirmed {
            assert!(host_message.is_err() && joiner_message.is_err());
            send(
                &mut joiner,
                ClientMessage::Signal {
                    to_peer_id: PeerId([1; 16]),
                    payload_base64: "AP8H".into(),
                },
            )
            .await;
            assert!(matches!(
                receive(&mut host).await,
                ServerMessage::Signal { .. }
            ));
        } else {
            assert_eq!(
                host_message.unwrap(),
                ServerMessage::PeerUnavailable {
                    peer_id: PeerId([2; 16])
                }
            );
            assert_eq!(
                joiner_message.unwrap(),
                ServerMessage::Error {
                    code: ErrorCode::JoinTimeout
                }
            );
            let mut replacement = f.connect().await;
            send(
                &mut replacement,
                ClientMessage::JoinRoom {
                    room_code,
                    peer_id: PeerId([3; 16]),
                },
            )
            .await;
            assert!(matches!(
                receive(&mut host).await,
                ServerMessage::AuthorizePeer { .. }
            ));
        }
        f.shutdown().await;
    }
}
#[tokio::test]
async fn real_websocket_health_join_ack_opaque_relay_and_shutdown() {
    let f = Fixture::new(Limits::default()).await;
    let health = f
        .url
        .replace("ws://", "http://")
        .replace("/v1/ws", "/healthz");
    assert_eq!(reqwest::get(health).await.unwrap().status(), 200);
    let mut host = f.connect().await;
    let mut joiner = f.connect().await;
    send(
        &mut host,
        ClientMessage::CreateRoom {
            peer_id: PeerId([1; 16]),
        },
    )
    .await;
    let ServerMessage::RoomCreated { room_code, .. } = receive(&mut host).await else {
        panic!()
    };
    send(
        &mut joiner,
        ClientMessage::JoinRoom {
            room_code,
            peer_id: PeerId([2; 16]),
        },
    )
    .await;
    let ServerMessage::AuthorizePeer { join_id, .. } = receive(&mut host).await else {
        panic!()
    };
    assert!(timeout(Duration::from_millis(50), joiner.next())
        .await
        .is_err());
    send(&mut host, ClientMessage::AuthorizeAck { join_id }).await;
    assert!(matches!(
        receive(&mut joiner).await,
        ServerMessage::RoomJoined { .. }
    ));
    assert!(matches!(
        receive(&mut host).await,
        ServerMessage::PeerJoined { .. }
    ));
    send(
        &mut joiner,
        ClientMessage::Signal {
            to_peer_id: PeerId([1; 16]),
            payload_base64: "AP8H".into(),
        },
    )
    .await;
    assert_eq!(
        receive(&mut host).await,
        ServerMessage::Signal {
            from_peer_id: PeerId([2; 16]),
            payload_base64: "AP8H".into()
        }
    );
    f.stop.as_ref().unwrap();
    f.shutdown().await;
    assert!(matches!(
        receive(&mut joiner).await,
        ServerMessage::RoomClosed {}
    ));
}
#[tokio::test]
async fn oversized_frame_and_unknown_version_fail_closed() {
    let f = Fixture::new(Limits::default()).await;
    let mut socket = f.connect().await;
    socket
        .send(Message::Text(r#"{"v":2,"type":"leave_room"}"#.into()))
        .await
        .unwrap();
    assert_eq!(
        receive(&mut socket).await,
        ServerMessage::Error {
            code: ErrorCode::UnsupportedVersion
        }
    );
    assert!(matches!(socket.next().await, Some(Ok(Message::Close(_)))));
    let mut socket = f.connect().await;
    socket
        .send(Message::Text(" ".repeat(MAX_WS_BYTES + 1).into()))
        .await
        .unwrap();
    assert!(timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .is_none_or(|m| !matches!(m, Ok(Message::Text(_)))));
    f.shutdown().await;
}
#[tokio::test]
async fn heartbeat_requires_matching_pong_even_with_incoming_text() {
    let f = Fixture::new(Limits {
        heartbeat_interval: Duration::from_millis(50),
        heartbeat_timeout: Duration::from_millis(50),
        ..Limits::default()
    })
    .await;
    let (mut socket, _) = connect_async(&f.url).await.unwrap();
    // Never read (and therefore never auto-pong); application text cannot renew
    // the server's fixed outstanding-ping deadline.
    for _ in 0..5 {
        let _ = socket.send(Message::Text(r#"{"v":1,"type":"signal","to_peer_id":"11111111-1111-4111-8111-111111111111","payload_base64":"AP8H"}"#.into())).await;
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    let closed = timeout(Duration::from_secs(2), async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                _ => {}
            }
        }
    })
    .await;
    assert!(closed.is_ok());
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn immediate_joiner_and_host_signals_follow_their_activation_notifications() {
    let f = Fixture::new(Limits::default()).await;
    let mut host = f.connect().await;
    send(
        &mut host,
        ClientMessage::CreateRoom {
            peer_id: PeerId([1; 16]),
        },
    )
    .await;
    let ServerMessage::RoomCreated { room_code, .. } = receive(&mut host).await else {
        panic!()
    };
    for n in 2..18 {
        let mut joiner = f.connect().await;
        send(
            &mut joiner,
            ClientMessage::JoinRoom {
                room_code: room_code.clone(),
                peer_id: PeerId([n; 16]),
            },
        )
        .await;
        let ServerMessage::AuthorizePeer { join_id, .. } = receive(&mut host).await else {
            panic!()
        };
        send(&mut host, ClientMessage::AuthorizeAck { join_id }).await;
        // Host does not consume PeerJoined until the joiner has signaled.
        assert!(matches!(
            receive(&mut joiner).await,
            ServerMessage::RoomJoined { .. }
        ));
        send(
            &mut joiner,
            ClientMessage::Signal {
                to_peer_id: PeerId([1; 16]),
                payload_base64: "AP8H".into(),
            },
        )
        .await;
        assert!(
            matches!(receive(&mut host).await, ServerMessage::PeerJoined { peer_id, .. } if peer_id == PeerId([n; 16]))
        );
        assert!(
            matches!(receive(&mut host).await, ServerMessage::Signal { from_peer_id, .. } if from_peer_id == PeerId([n; 16]))
        );
        send(&mut joiner, ClientMessage::LeaveRoom {}).await;
        assert!(matches!(
            receive(&mut host).await,
            ServerMessage::PeerUnavailable { .. }
        ));
    }
    // Also force the symmetric case: host signals immediately after PeerJoined,
    // before the joiner reads its RoomJoined frame.
    let mut joiner = f.connect().await;
    send(
        &mut joiner,
        ClientMessage::JoinRoom {
            room_code,
            peer_id: PeerId([20; 16]),
        },
    )
    .await;
    let ServerMessage::AuthorizePeer { join_id, .. } = receive(&mut host).await else {
        panic!()
    };
    send(&mut host, ClientMessage::AuthorizeAck { join_id }).await;
    assert!(matches!(
        receive(&mut host).await,
        ServerMessage::PeerJoined { .. }
    ));
    send(
        &mut host,
        ClientMessage::Signal {
            to_peer_id: PeerId([20; 16]),
            payload_base64: "AP8H".into(),
        },
    )
    .await;
    assert!(matches!(
        receive(&mut joiner).await,
        ServerMessage::RoomJoined { .. }
    ));
    assert!(matches!(
        receive(&mut joiner).await,
        ServerMessage::Signal { .. }
    ));
    f.shutdown().await;
}

fn rejected_status(error: Error) -> u16 {
    let Error::Http(response) = error else {
        panic!("expected HTTP rejection: {error}")
    };
    response.status().as_u16()
}
#[tokio::test]
async fn only_trusted_proxy_headers_select_the_ip_guard() {
    let limits = Limits {
        connections_per_ip: 1,
        ..Limits::default()
    };
    let f = Fixture::with_server(Server::with_trusted_proxies(
        limits.clone(),
        "127.0.0.1/32".parse().unwrap(),
    ))
    .await;
    assert_eq!(
        rejected_status(f.connect_from(None).await.unwrap_err()),
        400
    );
    assert_eq!(
        rejected_status(f.connect_from(Some("spoofed")).await.unwrap_err()),
        400
    );
    let _a = f.connect_from(Some("198.51.100.1")).await.unwrap();
    // A spoofed left prefix cannot get around the nearest untrusted client's cap.
    assert_eq!(
        rejected_status(
            f.connect_from(Some("203.0.113.9, 198.51.100.1"))
                .await
                .unwrap_err()
        ),
        429
    );
    let _b = f.connect_from(Some("198.51.100.2")).await.unwrap();
    f.shutdown().await;
    let f = Fixture::new(limits).await;
    let _a = f.connect_from(Some("198.51.100.1")).await.unwrap();
    assert_eq!(
        rejected_status(f.connect_from(Some("198.51.100.2")).await.unwrap_err()),
        429
    );
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_trusted_proxy_can_admit_a_full_64_remote_room() {
    let f = Fixture::with_server(Server::with_trusted_proxies(
        Limits::default(),
        "127.0.0.1/32".parse().unwrap(),
    ))
    .await;
    let mut host = f.connect_from(Some("198.51.100.1")).await.unwrap();
    send(
        &mut host,
        ClientMessage::CreateRoom {
            peer_id: PeerId([1; 16]),
        },
    )
    .await;
    let ServerMessage::RoomCreated { room_code, .. } = receive(&mut host).await else {
        panic!()
    };
    let mut participants = Vec::new();
    for n in 2..=65 {
        let mut joiner = f
            .connect_from(Some(&format!("198.51.100.{n}")))
            .await
            .unwrap();
        send(
            &mut joiner,
            ClientMessage::JoinRoom {
                room_code: room_code.clone(),
                peer_id: PeerId([n; 16]),
            },
        )
        .await;
        let ServerMessage::AuthorizePeer { join_id, .. } = receive(&mut host).await else {
            panic!()
        };
        send(&mut host, ClientMessage::AuthorizeAck { join_id }).await;
        assert!(matches!(
            receive(&mut host).await,
            ServerMessage::PeerJoined { .. }
        ));
        assert!(matches!(
            receive(&mut joiner).await,
            ServerMessage::RoomJoined { .. }
        ));
        participants.push(joiner);
    }
    let mut excess = f.connect_from(Some("198.51.100.66")).await.unwrap();
    send(
        &mut excess,
        ClientMessage::JoinRoom {
            room_code,
            peer_id: PeerId([66; 16]),
        },
    )
    .await;
    assert_eq!(
        receive(&mut excess).await,
        ServerMessage::Error {
            code: ErrorCode::RoomFull
        }
    );
    assert_eq!(participants.len(), 64);
    f.shutdown().await;
}

#[tokio::test]
#[ignore = "requires real Caddy sample + backend with trusted loopback and per-IP cap 2"]
async fn caddy_edge_replaces_spoofed_ip_and_relays_ordered_activation() {
    let url = std::env::var("PUZZELLA_CADDY_SMOKE_URL").expect("loopback sample URL");
    // This fixture deliberately uses HTTP/WS only on local loopback. Production
    // runs the unmodified sample with a public domain and automatic HTTPS.
    assert!(url.starts_with("ws://127.0.0.1:"));
    let mut request = url.clone().into_client_request().unwrap();
    request
        .headers_mut()
        .insert("x-forwarded-for", "198.51.100.1".parse().unwrap());
    let (mut host, _) = connect_async(request).await.unwrap();
    assert!(matches!(
        receive(&mut host).await,
        ServerMessage::Welcome { .. }
    ));
    let mut request = url.clone().into_client_request().unwrap();
    request
        .headers_mut()
        .insert("x-forwarded-for", "203.0.113.2".parse().unwrap());
    request
        .headers_mut()
        .insert("forwarded", "for=203.0.113.3".parse().unwrap());
    let (mut joiner, _) = connect_async(request).await.unwrap();
    assert!(matches!(
        receive(&mut joiner).await,
        ServerMessage::Welcome { .. }
    ));
    let mut request = url.clone().into_client_request().unwrap();
    request
        .headers_mut()
        .insert("x-forwarded-for", "192.0.2.3".parse().unwrap());
    assert_eq!(
        rejected_status(connect_async(request).await.unwrap_err()),
        429
    );
    let base = url.replace("ws://", "http://").replace("/v1/ws", "");
    assert_eq!(
        reqwest::get(format!("{base}/healthz"))
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        reqwest::get(format!("{base}/not-a-route"))
            .await
            .unwrap()
            .status(),
        404
    );
    send(
        &mut host,
        ClientMessage::CreateRoom {
            peer_id: PeerId([1; 16]),
        },
    )
    .await;
    let ServerMessage::RoomCreated { room_code, .. } = receive(&mut host).await else {
        panic!()
    };
    send(
        &mut joiner,
        ClientMessage::JoinRoom {
            room_code,
            peer_id: PeerId([2; 16]),
        },
    )
    .await;
    let ServerMessage::AuthorizePeer { join_id, .. } = receive(&mut host).await else {
        panic!()
    };
    send(&mut host, ClientMessage::AuthorizeAck { join_id }).await;
    assert!(matches!(
        receive(&mut joiner).await,
        ServerMessage::RoomJoined { .. }
    ));
    send(
        &mut joiner,
        ClientMessage::Signal {
            to_peer_id: PeerId([1; 16]),
            payload_base64: "AP8H".into(),
        },
    )
    .await;
    assert!(matches!(
        receive(&mut host).await,
        ServerMessage::PeerJoined { .. }
    ));
    assert!(matches!(
        receive(&mut host).await,
        ServerMessage::Signal { .. }
    ));
    host.close(None).await.unwrap();
    joiner.close(None).await.unwrap();
}

struct RotatingProvider(std::sync::atomic::AtomicUsize);
impl puzzella_rendezvous::turn::TurnProvider for RotatingProvider {
    fn issue<'a>(
        &'a self,
        identifier: &'a str,
        _: Duration,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Vec<TurnServer>, puzzella_rendezvous::turn::TurnError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            assert_eq!(identifier.len(), 32);
            let n = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n == 1 {
                return Err(puzzella_rendezvous::turn::TurnError::Unavailable);
            }
            Ok(vec![TurnServer {
                address: "turn.cloudflare.com:3478".into(),
                username: format!("user-{n}"),
                password: "short-lived-password".into(),
            }])
        })
    }
}
fn turn_config() -> puzzella_rendezvous::turn::TurnConfig {
    puzzella_rendezvous::turn::TurnConfig {
        key_id: "server-key".into(),
        api_token: "server-token".into(),
        ttl: Duration::from_secs(4),
        concurrency: 1,
        requests_per_minute: 60,
    }
}
#[tokio::test]
async fn welcome_precedes_room_creation_and_rotation_retries_without_changing_room() {
    use puzzella_rendezvous::turn::TurnService;
    let p = std::sync::Arc::new(RotatingProvider(std::sync::atomic::AtomicUsize::new(0)));
    let f = Fixture::with_server(
        Server::new(Limits::default()).with_turn(TurnService::new(p.clone(), &turn_config())),
    )
    .await;
    let (mut socket, _) = connect_async(&f.url).await.unwrap();
    let ServerMessage::Welcome {
        turn: Some(initial),
        ..
    } = receive(&mut socket).await
    else {
        panic!("missing initial TURN");
    };
    assert_eq!(initial.servers[0].username, "user-0");
    send(
        &mut socket,
        ClientMessage::CreateRoom {
            peer_id: PeerId([7; 16]),
        },
    )
    .await;
    assert!(matches!(
        receive(&mut socket).await,
        ServerMessage::RoomCreated { .. }
    ));
    let ServerMessage::TurnCredentials { turn } = timeout(Duration::from_secs(5), async {
        loop {
            match socket.next().await.unwrap().unwrap() {
                Message::Text(text) => break parse_server(&text).unwrap(),
                Message::Ping(_) => socket.flush().await.unwrap(),
                _ => panic!(),
            }
        }
    })
    .await
    .unwrap() else {
        panic!("rotation missing");
    };
    assert_eq!(turn.servers[0].username, "user-2");
    assert!(turn.expires_at_unix > initial.expires_at_unix);
    send(&mut socket, ClientMessage::LeaveRoom {}).await;
    assert!(matches!(
        receive(&mut socket).await,
        ServerMessage::RoomClosed {}
    ));
    f.shutdown().await;
}
#[tokio::test]
async fn unavailable_provider_still_allows_direct_room_creation() {
    use puzzella_rendezvous::turn::{TurnError, TurnProvider, TurnService};
    struct Unavailable;
    impl TurnProvider for Unavailable {
        fn issue<'a>(
            &'a self,
            _: &'a str,
            _: Duration,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Vec<TurnServer>, TurnError>> + Send + 'a>,
        > {
            Box::pin(async { Err(TurnError::Unavailable) })
        }
    }
    let f = Fixture::with_server(Server::new(Limits::default()).with_turn(TurnService::new(
        std::sync::Arc::new(Unavailable),
        &turn_config(),
    )))
    .await;
    let (mut socket, _) = connect_async(&f.url).await.unwrap();
    assert!(matches!(
        receive(&mut socket).await,
        ServerMessage::Welcome { turn: None, .. }
    ));
    send(
        &mut socket,
        ClientMessage::CreateRoom {
            peer_id: PeerId([8; 16]),
        },
    )
    .await;
    assert!(matches!(
        receive(&mut socket).await,
        ServerMessage::RoomCreated { .. }
    ));
    f.shutdown().await;
}
