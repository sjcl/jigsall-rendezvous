use super::*;
use std::{sync::mpsc as blocking, thread};

fn promoted() -> (Harness, crate::Server, u64, u64, JoinId) {
    let mut h = Harness::new(Limits::default());
    let host = h.connection(1);
    let joiner = h.connection(2);
    let code = h.create(host, 1);
    let join = h.pending(joiner, code, 2);
    let server = crate::Server::new(h.state.limits.clone());
    std::mem::swap(&mut *server.state.lock().unwrap(), &mut h.state);
    (h, server, host, joiner, join)
}
#[test]
fn committed_activation_cannot_be_overtaken_before_either_enqueue() {
    let (mut h, server, host, joiner, join) = promoted();
    // Force the exact gap between the state commit and dispatch, leaving the
    // state lock free. Competing transitions must wait for the dispatch gate.
    let gate = server.dispatch_gate.lock().unwrap();
    let effects = server.state.lock().unwrap().handle(
        host,
        ClientMessage::AuthorizeAck { join_id: join },
        h.now,
    );
    thread::scope(|scope| {
        let (started, starting) = blocking::channel();
        let (entered, entering) = blocking::channel();
        for (sender, target) in [(joiner, 1), (host, 2)] {
            let server = &server;
            let started = started.clone();
            let entered = entered.clone();
            scope.spawn(move || {
                started.send(()).unwrap();
                server.transition(|state| {
                    entered.send(()).unwrap();
                    state.handle(sender, Harness::signal(target), Instant::now())
                });
            });
        }
        starting.recv_timeout(Duration::from_secs(2)).unwrap();
        starting.recv_timeout(Duration::from_secs(2)).unwrap();
        let blocked = entering.recv_timeout(Duration::from_millis(50)).is_err();
        server.dispatch(effects);
        drop(gate);
        entering.recv_timeout(Duration::from_secs(2)).unwrap();
        entering.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(blocked, "signal transition overtook activation dispatch");
    });
    let host_rx = h.receivers.get_mut(&host).unwrap();
    assert!(matches!(
        parse_server(&host_rx.try_recv().unwrap().text).unwrap(),
        ServerMessage::PeerJoined { .. }
    ));
    assert!(matches!(
        parse_server(&host_rx.try_recv().unwrap().text).unwrap(),
        ServerMessage::Signal { .. }
    ));
    let joiner_rx = h.receivers.get_mut(&joiner).unwrap();
    assert!(matches!(
        parse_server(&joiner_rx.try_recv().unwrap().text).unwrap(),
        ServerMessage::RoomJoined { .. }
    ));
    assert!(matches!(
        parse_server(&joiner_rx.try_recv().unwrap().text).unwrap(),
        ServerMessage::Signal { .. }
    ));
}
#[test]
fn host_activation_enqueue_failure_never_releases_joiner() {
    let mut h = Harness::new(Limits {
        outbound: 1,
        ..Limits::default()
    });
    let host = h.connection(1);
    let joiner = h.connection(2);
    let code = h.create(host, 1);
    let join = h.pending(joiner, code, 2);
    let server = crate::Server::new(h.state.limits.clone());
    *server.state.lock().unwrap() = h.state;
    server.transition(|state| state.welcome(host)); // fill host queue
    server.transition(|state| {
        state.handle(
            host,
            ClientMessage::AuthorizeAck { join_id: join },
            Instant::now(),
        )
    });
    let rx = h.receivers.get_mut(&joiner).unwrap();
    assert_eq!(
        parse_server(&rx.try_recv().unwrap().text).unwrap(),
        ServerMessage::RoomClosed {}
    );
    assert!(rx.try_recv().is_err()); // no RoomJoined was enqueued
    assert!(server.state.lock().unwrap().rooms.is_empty());
}
#[test]
fn byte_congestion_returns_backpressure_without_removing_target() {
    let cost = serde_json::to_string(&Frame::new(ServerMessage::Signal {
        from_peer_id: PeerId([2; 16]),
        payload_base64: encode_signal(&vec![7; MAX_SIGNAL_BYTES]),
    }))
    .unwrap()
    .len();
    for global in [false, true] {
        let mut h = Harness::new(Limits {
            outbound_bytes_per_connection: if global { cost * 2 } else { cost },
            outbound_bytes_global: if global { cost + 256 } else { cost * 10 },
            ..Limits::default()
        });
        let host = h.connection(1);
        let joiner = h.connection(2);
        let code = h.create(host, 1);
        let join = h.pending(joiner, code, 2);
        h.send(host, ClientMessage::AuthorizeAck { join_id: join });
        let server = crate::Server::new(h.state.limits.clone());
        *server.state.lock().unwrap() = h.state;
        let signal = || ClientMessage::Signal {
            to_peer_id: PeerId([1; 16]),
            payload_base64: encode_signal(&vec![7; MAX_SIGNAL_BYTES]),
        };
        server.transition(|state| state.handle(joiner, signal(), Instant::now()));
        server.transition(|state| state.handle(joiner, signal(), Instant::now()));
        let feedback = h.receivers.get_mut(&joiner).unwrap().try_recv().unwrap();
        assert_eq!(
            parse_server(&feedback.text).unwrap(),
            ServerMessage::Error {
                code: ErrorCode::Backpressure
            }
        );
        drop(feedback);
        assert_eq!(server.state.lock().unwrap().connections.len(), 2);
        drop(h.receivers.get_mut(&host).unwrap().try_recv().unwrap());
        server.transition(|state| state.handle(joiner, signal(), Instant::now()));
        assert!(matches!(
            parse_server(&h.receivers.get_mut(&host).unwrap().try_recv().unwrap().text).unwrap(),
            ServerMessage::Signal { .. }
        ));
        assert_eq!(
            h.outbound_bytes.available_permits(),
            server.limits.outbound_bytes_global
        );
    }
}
