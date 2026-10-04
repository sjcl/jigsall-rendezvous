use super::*;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
mod auth;
mod caps;
mod ordering;
mod reject;
#[test]
fn silent_routed_members_cannot_permanently_fill_room() {
    let mut h = Harness::new(Limits::default());
    let host = h.connection(1);
    let code = h.create(host, 1);
    for peer in 2..=65 {
        let client = h.connection(peer);
        let join = h.pending(client, code.clone(), peer);
        h.send(host, ClientMessage::AuthorizeAck { join_id: join });
    }
    let client = h.connection(66);
    Harness::error(
        h.send(
            client,
            ClientMessage::JoinRoom {
                room_code: code.clone(),
                peer_id: PeerId([66; 16]),
            },
        ),
        ErrorCode::RoomFull,
    );
    h.now += Duration::from_secs(31);
    h.state.sweep(h.now);
    assert_eq!(
        h.state.connections.len(),
        1,
        "all unauthenticated slots must expire"
    );
    let client = h.connection(67);
    h.pending(client, code, 67);
}
struct Harness {
    state: State,
    now: Instant,
    receivers: HashMap<u64, mpsc::Receiver<outbound::Packet>>,
    outbound_bytes: Arc<Semaphore>,
}
impl Harness {
    fn new(limits: Limits) -> Self {
        Self {
            outbound_bytes: Arc::new(Semaphore::new(limits.outbound_bytes_global)),
            state: State::new(limits),
            now: Instant::now(),
            receivers: HashMap::new(),
        }
    }
    fn connection(&mut self, ip: u8) -> u64 {
        let (tx, rx) = self.queue();
        let (cancel, _) = watch::channel(false);
        let id = self
            .state
            .admit(IpAddr::from([127, 0, 0, ip]), tx, cancel, self.now)
            .unwrap();
        self.receivers.insert(id, rx);
        id
    }
    fn queue(&self) -> (outbound::Sender, mpsc::Receiver<outbound::Packet>) {
        outbound::channel(
            self.state.limits.outbound,
            self.state.limits.outbound_bytes_per_connection,
            self.outbound_bytes.clone(),
        )
    }
    fn send(&mut self, id: u64, message: ClientMessage) -> Effects {
        self.state.handle(id, message, self.now)
    }
    fn create(&mut self, id: u64, peer: u8) -> RoomCode {
        let e = self.send(
            id,
            ClientMessage::CreateRoom {
                peer_id: PeerId([peer; 16]),
            },
        );
        let ServerMessage::RoomCreated { room_code, .. } = &e.deliveries[0].message else {
            panic!()
        };
        room_code.clone()
    }
    fn pending(&mut self, id: u64, code: RoomCode, peer: u8) -> JoinId {
        let e = self.send(
            id,
            ClientMessage::JoinRoom {
                room_code: code,
                peer_id: PeerId([peer; 16]),
            },
        );
        let ServerMessage::AuthorizePeer { join_id, .. } = e.deliveries[0].message else {
            panic!()
        };
        join_id
    }
    fn error(e: Effects, code: ErrorCode) {
        assert_eq!(
            e.deliveries.last().unwrap().message,
            ServerMessage::Error { code }
        );
    }
    fn signal(to: u8) -> ClientMessage {
        ClientMessage::Signal {
            to_peer_id: PeerId([to; 16]),
            payload_base64: encode_signal(&[0, 255, 7]),
        }
    }
}
#[test]
fn host_ack_gates_activation_and_authoritative_star_relay() {
    let mut h = Harness::new(Limits::default());
    let host = h.connection(1);
    let a = h.connection(2);
    let b = h.connection(3);
    let code = h.create(host, 1);
    let join = h.pending(a, code.clone(), 2);
    Harness::error(h.send(a, Harness::signal(1)), ErrorCode::NotInRoom);
    Harness::error(
        h.send(a, ClientMessage::AuthorizeAck { join_id: join }),
        ErrorCode::ProtocolViolation,
    );
    let e = h.send(host, ClientMessage::AuthorizeAck { join_id: join });
    assert!(matches!(
        e.deliveries[0].message,
        ServerMessage::PeerJoined { .. }
    ));
    assert_eq!(e.deliveries[0].target, host);
    assert!(matches!(
        e.deliveries[1].message,
        ServerMessage::RoomJoined { .. }
    ));
    assert_eq!(e.deliveries[1].target, a);
    assert_eq!(e.deliveries[1].requires, Some(host));
    let join = h.pending(b, code, 3);
    h.send(host, ClientMessage::AuthorizeAck { join_id: join });
    for (from, to, sender_peer) in [(host, 2, 1), (a, 1, 2)] {
        let e = h.send(from, Harness::signal(to));
        assert_eq!(
            e.deliveries[0].message,
            ServerMessage::Signal {
                from_peer_id: PeerId([sender_peer; 16]),
                payload_base64: "AP8H".into()
            }
        );
    }
    Harness::error(h.send(a, Harness::signal(3)), ErrorCode::ProtocolViolation);
    Harness::error(h.send(a, Harness::signal(99)), ErrorCode::UnknownTarget);
    assert!(parse_client(r#"{"v":1,"type":"signal","to_peer_id":"11111111-1111-4111-8111-111111111111","from_peer_id":"22222222-2222-4222-8222-222222222222","payload_base64":"AP8H"}"#).is_err());
}
#[test]
fn membership_capacity_unknown_room_and_duplicate_peer() {
    let mut h = Harness::new(Limits {
        participants: 1,
        ..Limits::default()
    });
    let host = h.connection(1);
    let a = h.connection(2);
    let b = h.connection(3);
    let code = h.create(host, 1);
    Harness::error(
        h.send(
            a,
            ClientMessage::JoinRoom {
                room_code: "0000000000".parse().unwrap(),
                peer_id: PeerId([2; 16]),
            },
        ),
        ErrorCode::UnknownRoom,
    );
    Harness::error(
        h.send(
            a,
            ClientMessage::JoinRoom {
                room_code: code.clone(),
                peer_id: PeerId([1; 16]),
            },
        ),
        ErrorCode::DuplicatePeer,
    );
    h.pending(a, code.clone(), 2);
    Harness::error(
        h.send(
            b,
            ClientMessage::JoinRoom {
                room_code: code,
                peer_id: PeerId([3; 16]),
            },
        ),
        ErrorCode::RoomFull,
    );
    Harness::error(
        h.send(
            host,
            ClientMessage::CreateRoom {
                peer_id: PeerId([4; 16]),
            },
        ),
        ErrorCode::ProtocolViolation,
    );
}
#[test]
fn malformed_and_oversized_signals_are_rejected() {
    let mut h = Harness::new(Limits::default());
    let host = h.connection(1);
    let a = h.connection(2);
    let code = h.create(host, 1);
    let join = h.pending(a, code, 2);
    h.send(host, ClientMessage::AuthorizeAck { join_id: join });
    for (payload, error) in [
        ("*".into(), ErrorCode::InvalidSignal),
        (
            encode_signal(&vec![1; MAX_SIGNAL_BYTES + 1]),
            ErrorCode::SignalTooLarge,
        ),
    ] {
        Harness::error(
            h.send(
                a,
                ClientMessage::Signal {
                    to_peer_id: PeerId([1; 16]),
                    payload_base64: payload,
                },
            ),
            error,
        );
    }
}
#[test]
fn fixed_pending_deadline_and_disconnect_cleanup() {
    let mut h = Harness::new(Limits::default());
    let host = h.connection(1);
    let a = h.connection(2);
    let code = h.create(host, 1);
    let join = h.pending(a, code.clone(), 2);
    h.now += h.state.limits.pending_timeout;
    Harness::error(
        h.send(host, ClientMessage::AuthorizeAck { join_id: join }),
        ErrorCode::JoinTimeout,
    );
    let e = h.state.sweep(h.now);
    assert!(e.deliveries.iter().any(|d| d.message
        == ServerMessage::Error {
            code: ErrorCode::JoinTimeout
        }));
    assert!(e.deliveries.iter().any(|d| d.message
        == ServerMessage::PeerUnavailable {
            peer_id: PeerId([2; 16])
        }));
    assert!(!h.state.connections.contains_key(&a));
    assert_eq!(h.state.pending, 0);
    let a = h.connection(2);
    let join = h.pending(a, code, 2);
    h.send(host, ClientMessage::AuthorizeAck { join_id: join });
    let mut e = Effects::default();
    h.state.disconnect(a, &mut e, h.now);
    assert_eq!(h.state.peers.len(), 1);
    assert_eq!(h.state.rooms.values().next().unwrap().members.len(), 0);
    assert_eq!(
        e.deliveries[0].message,
        ServerMessage::PeerUnavailable {
            peer_id: PeerId([2; 16])
        }
    );
    let a = h.connection(2);
    let code = h.state.rooms.values().next().unwrap().code.clone();
    h.pending(a, code, 2);
    let mut e = Effects::default();
    h.state.disconnect(host, &mut e, h.now);
    assert!(h.state.rooms.is_empty() && h.state.codes.is_empty() && h.state.peers.is_empty());
    assert_eq!(h.state.pending, 0);
    assert!(matches!(
        e.deliveries[0].message,
        ServerMessage::RoomClosed {}
    ));
}
#[test]
fn rates_global_caps_and_bounded_ip_history() {
    let mut h = Harness::new(Limits {
        connections: 2,
        rooms: 1,
        pending: 1,
        ip_history: 2,
        connections_per_ip: 1,
        ..Limits::default()
    });
    let host = h.connection(1);
    let a = h.connection(2);
    h.create(host, 1);
    Harness::error(
        h.send(
            a,
            ClientMessage::CreateRoom {
                peer_id: PeerId([2; 16]),
            },
        ),
        ErrorCode::Capacity,
    );
    let mut e = Effects::default();
    h.state.disconnect(a, &mut e, h.now);
    let (tx, _) = h.queue();
    let (cancel, _) = watch::channel(false);
    assert_eq!(
        h.state.admit(
            IpAddr::from([127, 0, 0, 3]),
            tx.clone(),
            cancel.clone(),
            h.now
        ),
        Err(ErrorCode::Capacity)
    );
    assert_eq!(
        h.state
            .admit(IpAddr::from([127, 0, 0, 1]), tx, cancel, h.now),
        Err(ErrorCode::RateLimited)
    );
    let a = h.connection(2);
    for _ in 0..4 {
        h.send(
            a,
            ClientMessage::JoinRoom {
                room_code: "0000000000".parse().unwrap(),
                peer_id: PeerId([2; 16]),
            },
        );
    }
    Harness::error(
        h.send(
            a,
            ClientMessage::JoinRoom {
                room_code: "0000000000".parse().unwrap(),
                peer_id: PeerId([2; 16]),
            },
        ),
        ErrorCode::RateLimited,
    );
    let code = h.state.rooms.values().next().unwrap().code.clone();
    h.now += Duration::from_secs(10);
    let join = h.pending(a, code, 2);
    h.send(host, ClientMessage::AuthorizeAck { join_id: join });
    for _ in 0..32 {
        assert!(matches!(
            h.send(a, Harness::signal(1)).deliveries[0].message,
            ServerMessage::Signal { .. }
        ));
    }
    Harness::error(h.send(a, Harness::signal(1)), ErrorCode::RateLimited);
    let e = h.state.shutdown(h.now);
    assert!(!e.cancel.is_empty());
    assert!(h.state.connections.is_empty() && h.state.rooms.is_empty() && h.state.peers.is_empty());
    h.state.sweep(h.now + Duration::from_secs(301));
    assert!(h.state.ips.is_empty());
}
#[test]
fn room_code_collision_retry_is_finite() {
    let mut h = Harness::new(Limits::default());
    let host = h.connection(1);
    let code = h.create(host, 1);
    let mut count = 0;
    let next = h
        .state
        .code_with(|| {
            count += 1;
            Ok(if count == 1 {
                code.clone()
            } else {
                "0000000000".parse().unwrap()
            })
        })
        .unwrap();
    assert_ne!(next, code);
    assert_eq!(count, 2);
    count = 0;
    assert_eq!(
        h.state.code_with(|| {
            count += 1;
            Ok(code.clone())
        }),
        Err(ErrorCode::Capacity)
    );
    assert_eq!(count, 32);
}
#[test]
fn slow_signal_consumer_keeps_host_alive_and_cleanup_controls_are_bounded() {
    let mut h = Harness::new(Limits {
        outbound: 1,
        ..Limits::default()
    });
    let host = h.connection(1);
    let a = h.connection(2);
    let code = h.create(host, 1);
    let join = h.pending(a, code, 2);
    h.send(host, ClientMessage::AuthorizeAck { join_id: join });
    let server = crate::Server::new(h.state.limits.clone());
    *server.state.lock().unwrap() = h.state;
    server.transition(|state| state.welcome(host));
    server.transition(|state| state.handle(a, Harness::signal(1), h.now));
    assert!(server.state.lock().unwrap().connections.contains_key(&host));
    assert_eq!(
        parse_server(&h.receivers.get_mut(&a).unwrap().try_recv().unwrap().text).unwrap(),
        ServerMessage::Error {
            code: ErrorCode::Backpressure
        }
    );
    server.transition(|state| state.welcome(host));
    assert!(!server.state.lock().unwrap().connections.contains_key(&host));
    assert!(server.state.lock().unwrap().rooms.is_empty());
}
#[test]
fn pre_room_idle_deadline_does_not_move_with_traffic() {
    let mut h = Harness::new(Limits::default());
    let a = h.connection(1);
    for _ in 0..3 {
        h.now += Duration::from_secs(9);
        h.send(a, Harness::signal(99));
    }
    h.now += Duration::from_secs(3);
    h.state.sweep(h.now);
    assert!(h.state.connections.is_empty());
}
