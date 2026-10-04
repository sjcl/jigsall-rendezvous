use super::*;
#[test]
fn global_pending_connection_and_cross_room_caps() {
    let mut h = Harness::new(Limits {
        pending: 1,
        connections: 5,
        ..Limits::default()
    });
    let host = h.connection(1);
    let other_host = h.connection(2);
    let a = h.connection(3);
    let b = h.connection(4);
    let idle = h.connection(5);
    let (tx, _) = h.queue();
    let (cancel, _) = watch::channel(false);
    assert_eq!(
        h.state
            .admit(IpAddr::from([127, 0, 0, 6]), tx, cancel, h.now),
        Err(ErrorCode::Capacity)
    );
    let code = h.create(host, 1);
    let other = h.create(other_host, 2);
    let join = h.pending(a, code, 3);
    Harness::error(
        h.send(
            b,
            ClientMessage::JoinRoom {
                room_code: other.clone(),
                peer_id: PeerId([4; 16]),
            },
        ),
        ErrorCode::Capacity,
    );
    h.send(host, ClientMessage::AuthorizeAck { join_id: join });
    let join = h.pending(b, other, 4);
    h.send(other_host, ClientMessage::AuthorizeAck { join_id: join });
    Harness::error(h.send(a, Harness::signal(4)), ErrorCode::UnknownTarget);
    Harness::error(h.send(host, Harness::signal(4)), ErrorCode::UnknownTarget);
    for _ in 0..256 {
        h.send(idle, Harness::signal(99));
    }
    h.send(idle, Harness::signal(99));
    assert!(!h.state.connections.contains_key(&idle));
    let e = h.state.shutdown(h.now);
    assert_eq!(
        e.deliveries
            .iter()
            .filter(|d| matches!(d.message, ServerMessage::RoomClosed {}))
            .count(),
        4
    );
    assert_eq!(e.deliveries.len(), 4);
}
#[test]
fn ip_attempt_history_survives_membership_churn() {
    let mut h = Harness::new(Limits::default());
    for round in 0..31 {
        let connection = h.connection(1);
        for _ in 0..4 {
            let e = h.send(
                connection,
                ClientMessage::JoinRoom {
                    room_code: "0000000000".parse().unwrap(),
                    peer_id: PeerId([2; 16]),
                },
            );
            Harness::error(
                e,
                if round == 30 {
                    ErrorCode::RateLimited
                } else {
                    ErrorCode::UnknownRoom
                },
            );
        }
        h.state
            .disconnect(connection, &mut Effects::default(), h.now);
    }
    assert_eq!(h.state.ips.len(), 1);
    for _ in 31..60 {
        let c = h.connection(1);
        h.state.disconnect(c, &mut Effects::default(), h.now);
    }
    let (tx, _) = h.queue();
    let (cancel, _) = watch::channel(false);
    assert_eq!(
        h.state
            .admit(IpAddr::from([127, 0, 0, 1]), tx, cancel, h.now),
        Err(ErrorCode::RateLimited)
    );
}
