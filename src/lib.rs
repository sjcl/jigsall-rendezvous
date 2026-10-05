//! Anonymous control-plane routing only. Gameplay authentication stays in SPAKE2.
mod config;
mod outbound;
pub mod protocol;
mod proxy;
mod state;
pub mod turn;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        ConnectInfo, State,
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
pub use config::{Config, ConfigError};
use futures_util::{SinkExt, StreamExt};
use protocol::{ErrorCode, Frame, ServerMessage, MAX_WS_BYTES};
pub use proxy::TrustedProxies;
use state::Effects;
pub use state::Limits;
use std::{
    collections::HashSet,
    future::Future,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore},
    time::timeout,
};

#[derive(Clone)]
pub struct Server {
    state: Arc<Mutex<state::State>>,
    turn: Option<turn::TurnService>,
    permits: Arc<Semaphore>,
    limits: Limits,
    trusted_proxies: TrustedProxies,
    outbound_bytes: Arc<Semaphore>,
    // State changes and their nonblocking enqueues form one ordered transition.
    // This gate is never held during socket I/O or any await.
    dispatch_gate: Arc<Mutex<()>>,
}
impl Server {
    pub fn new(limits: Limits) -> Self {
        Self::with_trusted_proxies(limits, TrustedProxies::default())
    }
    pub fn with_trusted_proxies(limits: Limits, trusted_proxies: TrustedProxies) -> Self {
        Self {
            state: Arc::new(Mutex::new(state::State::new(limits.clone()))),
            permits: Arc::new(Semaphore::new(limits.connections)),
            outbound_bytes: Arc::new(Semaphore::new(limits.outbound_bytes_global)),
            dispatch_gate: Arc::new(Mutex::new(())),
            turn: None,
            trusted_proxies,
            limits,
        }
    }
    pub fn with_turn(mut self, service: turn::TurnService) -> Self {
        self.turn = Some(service);
        self
    }
    pub fn router(&self) -> Router {
        Router::new()
            .route("/healthz", get(|| async { StatusCode::OK }))
            .route("/v1/ws", get(upgrade))
            .with_state(self.clone())
    }
    fn transition(&self, update: impl FnOnce(&mut state::State) -> Effects) {
        let _gate = self.dispatch_gate.lock().unwrap();
        let effects = update(&mut self.state.lock().unwrap());
        // Drop the state lock before serializing/enqueueing. The dispatch gate
        // keeps later transitions from overtaking activation or cleanup frames.
        self.dispatch(effects);
    }
    // Called only within transition. No network send or await occurs here.
    fn dispatch(&self, mut e: Effects) {
        loop {
            let mut failed = HashSet::new();
            for delivery in e.deliveries.drain(..) {
                if delivery.requires.is_some_and(|id| failed.contains(&id)) {
                    continue;
                }
                if delivery.tx.try_send(delivery.message).is_err() {
                    if let Some(source) = delivery.source {
                        let reply = self
                            .state
                            .lock()
                            .unwrap()
                            .error(source, ErrorCode::Backpressure);
                        for d in reply.deliveries {
                            if d.tx.try_send(d.message).is_err() {
                                failed.insert(source);
                            }
                        }
                    } else {
                        failed.insert(delivery.target);
                    }
                }
            }
            for cancel in e.cancel.drain(..) {
                let _ = cancel.send(true);
            }
            if failed.is_empty() {
                break;
            }
            let mut state = self.state.lock().unwrap();
            for id in failed {
                state.disconnect(id, &mut e, Instant::now());
            }
        }
    }
    pub fn shutdown(&self) {
        self.transition(|state| state.shutdown(Instant::now()));
    }
}
struct Lease {
    server: Server,
    id: u64,
    _permit: OwnedSemaphorePermit,
    turn_tx: outbound::Sender,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut e = Effects::default();
        self.server.transition(|state| {
            state.disconnect(self.id, &mut e, Instant::now());
            e
        });
    }
}
async fn upgrade(
    State(server): State<Server>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let Ok(ip) = server.trusted_proxies.source(peer.ip(), &headers) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Ok(permit) = server.permits.clone().try_acquire_owned() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let (tx, rx) = outbound::channel(
        server.limits.outbound,
        server.limits.outbound_bytes_per_connection,
        server.outbound_bytes.clone(),
    );
    let (cancel, cancelled) = watch::channel(false);
    let admitted = server
        .state
        .lock()
        .unwrap()
        .admit(ip, tx.clone(), cancel, Instant::now());
    let Ok(id) = admitted else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    let lease = Lease {
        server,
        id,
        _permit: permit,
        turn_tx: tx,
    };
    // Lease is dropped on failed upgrade too. Reservation lifetime is also
    // bounded by the fixed pre-room admission deadline.
    ws.read_buffer_size(MAX_WS_BYTES)
        .write_buffer_size(0)
        .max_write_buffer_size(MAX_WS_BYTES * 2)
        .max_frame_size(MAX_WS_BYTES)
        .max_message_size(MAX_WS_BYTES)
        .on_upgrade(move |socket| session(socket, lease, rx, cancelled))
}
async fn session(
    socket: WebSocket,
    lease: Lease,
    mut outbound: mpsc::Receiver<outbound::Packet>,
    mut cancelled: watch::Receiver<bool>,
) {
    let server = &lease.server;
    let identifier = uuid::Uuid::new_v4().simple().to_string();
    let initial = if let Some(service) = &server.turn {
        tokio::select! {
            biased;
            _ = cancelled.changed() => return,
            value = service.issue(&identifier) => value.ok(),
        }
    } else {
        None
    };
    server.transition(|state| {
        let mut effects = state.welcome(lease.id);
        for delivery in &mut effects.deliveries {
            if let ServerMessage::Welcome { turn, .. } = &mut delivery.message {
                *turn = initial.clone();
            }
        }
        effects
    });
    let _rotation = server.turn.clone().map(|service| {
        let tx = lease.turn_tx.clone();
        turn::Rotation(tokio::spawn(async move {
            service.rotate(identifier, tx, initial).await
        }))
    });
    let (mut sink, mut stream) = socket.split();
    let limits = &server.limits;
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + limits.heartbeat_interval,
        Duration::from_millis(50),
    );
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut next_ping = Instant::now() + limits.heartbeat_interval;
    let mut outstanding: Option<([u8; 8], Instant)> = None;
    let mut sequence = 0u64;
    let mut frames = state::Window::new(512, Duration::from_secs(1), Instant::now());
    loop {
        // The watch value may already be true before changed() is first polled.
        if *cancelled.borrow() {
            break;
        }
        tokio::select! {
            biased;
            _ = cancelled.changed() => break,
            _ = heartbeat.tick() => {
                let now = Instant::now();
                if outstanding.is_some_and(|(_, deadline)| now >= deadline) { break; }
                if outstanding.is_none() && now >= next_ping {
                    sequence += 1;
                    let nonce = sequence.to_be_bytes();
                    outstanding = Some((nonce, now + limits.heartbeat_timeout));
                    if !matches!(timeout(limits.write_timeout, sink.send(Message::Ping(nonce.to_vec().into()))).await, Ok(Ok(()))) { break; }
                }
            }
            message = stream.next() => {
                if !frames.take(Instant::now()) { break; }
                match message {
                    Some(Ok(Message::Text(text))) => {
                        match protocol::parse_client(&text) {
                            Ok(message) => {
                                server.transition(|state| state.handle(lease.id, message, Instant::now()));
                            }
                            Err(code) => {
                                let json = serde_json::to_string(&Frame::new(ServerMessage::Error { code })).unwrap();
                                let _ = timeout(limits.write_timeout, sink.send(Message::Text(json.into()))).await;
                                break;
                            }
                        }
                    }
                    Some(Ok(Message::Pong(bytes))) => {
                        if outstanding.is_some_and(|(nonce, _)| bytes.as_ref() == nonce) {
                            outstanding = None;
                            next_ping = Instant::now() + limits.heartbeat_interval;
                        }
                    }
                    Some(Ok(Message::Ping(_))) => {
                        // Tungstenite queued the automatic pong; flush it with a deadline.
                        if !matches!(timeout(limits.write_timeout, sink.flush()).await, Ok(Ok(()))) { break; }
                    }
                    _ => break,
                }
            }
            Some(message) = outbound.recv() => {
                let outbound::Packet { text, reservation } = message;
                let sent = timeout(limits.write_timeout, sink.send(Message::Text(text))).await;
                drop(reservation);
                if !matches!(sent, Ok(Ok(()))) { break; }
            }
        }
    }
    // A bounded grace budget for RoomClosed/errors already queued by cleanup.
    let _ = timeout(limits.write_timeout, async {
        while let Ok(message) = outbound.try_recv() {
            let outbound::Packet { text, reservation } = message;
            let sent = sink.send(Message::Text(text)).await;
            drop(reservation);
            if sent.is_err() {
                break;
            }
        }
        let _ = sink.send(Message::Close(None)).await;
    })
    .await;
}
pub async fn serve(
    listener: TcpListener,
    server: Server,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let maintenance = server.clone();
    let (stop, mut stopped) = watch::channel(false);
    let sweeper = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        loop {
            tokio::select! {
                _ = stopped.changed() => break,
                _ = tick.tick() => {
                    maintenance.transition(|state| state.sweep(Instant::now()));
                }
            }
        }
    });
    let cleanup = server.clone();
    let result = axum::serve(
        listener,
        server
            .router()
            .into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown.await;
        cleanup.shutdown();
    })
    .await;
    server.shutdown();
    let _ = stop.send(true);
    let _ = sweeper.await;
    // Upgrade tasks are detached from HTTP serving; wait for their leases too.
    let _ = timeout(
        server.limits.write_timeout * 2,
        server
            .permits
            .clone()
            .acquire_many_owned(server.limits.connections as u32),
    )
    .await;
    result
}
