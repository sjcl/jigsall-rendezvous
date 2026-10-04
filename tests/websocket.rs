use futures_util::{SinkExt, StreamExt};
use puzzella_rendezvous::{protocol::*, serve, Limits, Server};
use std::time::Duration;
use tokio::{net::TcpListener, sync::oneshot, time::timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};
type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
struct Fixture {
    url: String,
    stop: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}
impl Fixture {
    async fn new(limits: Limits) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(serve(listener, Server::new(limits), async {
            let _ = stopped.await;
        }));
        Self {
            url: format!("ws://{address}/v1/ws"),
            stop: Some(stop),
            task,
        }
    }
    async fn connect(&self) -> Socket {
        let (mut socket, _) = connect_async(&self.url).await.unwrap();
        assert!(matches!(
            receive(&mut socket).await,
            ServerMessage::Welcome { .. }
        ));
        socket
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
