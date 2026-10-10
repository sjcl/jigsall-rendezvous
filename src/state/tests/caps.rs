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

#[test]
fn prefix_history_churn_preserves_limits_and_active_connection_counts() {
    let mut h = Harness::new(Limits {
        ip_history: 2,
        admissions_per_ip_per_minute: 2,
        room_attempts_per_ip_per_minute: 2,
        ..Limits::default()
    });
    let active = h.connection(1);
    let bad = h.connection(2);
    for _ in 0..2 {
        Harness::error(
            h.send(
                bad,
                ClientMessage::JoinRoom {
                    room_code: "0000000000".parse().unwrap(),
                    peer_id: PeerId([2; 16]),
                },
            ),
            ErrorCode::UnknownRoom,
        );
    }
    h.state.disconnect(bad, &mut Effects::default(), h.now);
    for n in 3..=30 {
        // Choose distinct overflow slots for this deterministic bounded test.
        let ip = IpAddr::from([127, 0, 0, n]);
        if h.state.history_slot(ip) == h.state.history_slot(IpAddr::from([127, 0, 0, 2])) {
            continue;
        }
        let (tx, rx) = h.queue();
        let (cancel, _) = watch::channel(false);
        let id = h.state.admit(ip, tx, cancel, h.now).unwrap();
        drop(rx);
        h.state.disconnect(id, &mut Effects::default(), h.now);
    }
    assert!(h.state.connections.contains_key(&active));
    assert_eq!(h.state.ips[&IpAddr::from([127, 0, 0, 1])].connections, 1);
    let again = h.connection(2);
    Harness::error(
        h.send(
            again,
            ClientMessage::JoinRoom {
                room_code: "0000000000".parse().unwrap(),
                peer_id: PeerId([2; 16]),
            },
        ),
        ErrorCode::RateLimited,
    );
    h.state.disconnect(again, &mut Effects::default(), h.now);
    let (tx, _) = h.queue();
    let (cancel, _) = watch::channel(false);
    assert_eq!(
        h.state
            .admit(IpAddr::from([127, 0, 0, 2]), tx, cancel, h.now),
        Err(ErrorCode::RateLimited)
    );
    assert_eq!(h.state.ips.len(), 2);
    assert_eq!(h.state.overflow.len(), 512);
}

#[test]
fn abuse_keys_survive_member_reissue_and_are_room_and_prefix_scoped() {
    let mut h = Harness::new(Limits::default());
    let host = h.connection(1);
    let code = h.create(host, 1);
    let join = |h: &mut Harness, ip: &str, peer: u8| {
        let (tx, rx) = h.queue();
        let (cancel, _) = watch::channel(false);
        let id = h
            .state
            .admit(ip.parse().unwrap(), tx, cancel, h.now)
            .unwrap();
        h.receivers.insert(id, rx);
        let e = h.send(
            id,
            ClientMessage::JoinRoom {
                room_code: code.clone(),
                peer_id: PeerId([peer; 16]),
            },
        );
        let ServerMessage::AuthorizePeer {
            member_id,
            abuse_key,
            ..
        } = e.deliveries[0].message
        else {
            panic!()
        };
        h.state.disconnect(id, &mut Effects::default(), h.now);
        (member_id, abuse_key)
    };
    let a = join(&mut h, "2001:db8:1:2::1", 2);
    let b = join(&mut h, "2001:db8:1:2::ffff", 3);
    let c = join(&mut h, "2001:db8:1:3::1", 4);
    assert_ne!(a.0, b.0);
    assert_eq!(a.1, b.1);
    assert_ne!(a.1, c.1);
    let room = *h.state.codes.get(&code).unwrap();
    assert_eq!(
        h.state.abuse_key(room, "192.0.2.1".parse().unwrap()),
        h.state.abuse_key(room, "::ffff:192.0.2.1".parse().unwrap())
    );
    assert_ne!(
        a.1,
        h.state
            .abuse_key(RoomId([9; 16]), "2001:db8:1:2::1".parse().unwrap())
    );
    assert_ne!(
        a.1,
        State::new(Limits::default()).abuse_key(room, "2001:db8:1:2::1".parse().unwrap())
    );
}

#[test]
fn ipv6_and_mapped_addresses_cannot_reset_connection_admission() {
    for (a, b) in [
        ("2001:db8:1:2::1", "2001:db8:1:2::2"),
        ("192.0.2.1", "::ffff:192.0.2.1"),
    ] {
        let mut h = Harness::new(Limits {
            connections_per_ip: 1,
            ..Limits::default()
        });
        let (tx, _) = h.queue();
        let (cancel, _) = watch::channel(false);
        h.state
            .admit(a.parse().unwrap(), tx.clone(), cancel.clone(), h.now)
            .unwrap();
        assert_eq!(
            h.state.admit(b.parse().unwrap(), tx, cancel, h.now),
            Err(ErrorCode::RateLimited)
        );
        assert_eq!(h.state.ips.len(), 1);
    }
}
