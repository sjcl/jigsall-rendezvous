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
        let ip = IpAddr::from([127, 0, 0, n]);
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
    assert!(h.state.retired.len() <= RETIRED_IPS);
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

fn admit_numbered(h: &mut Harness, n: u32) -> Result<u64, ErrorCode> {
    let (tx, _rx) = h.queue();
    let (cancel, _) = watch::channel(false);
    h.state
        .admit(std::net::Ipv4Addr::from(n).into(), tx, cancel, h.now)
}
#[test]
fn forced_history_collisions_do_not_transfer_admission_or_attempt_penalties() {
    let mut h = Harness::new(Limits {
        ip_history: 1,
        admissions_per_ip_per_minute: 2,
        room_attempts_per_ip_per_minute: 1,
        ..Limits::default()
    });
    h.state.collide = true;
    let a = h.connection(1);
    let probe = ClientMessage::JoinRoom {
        room_code: "0000000000".parse().unwrap(),
        peer_id: PeerId([1; 16]),
    };
    Harness::error(h.send(a, probe.clone()), ErrorCode::UnknownRoom);
    h.state.disconnect(a, &mut Effects::default(), h.now);
    let b = h.connection(2); // A moved into the exact-owner retired cache.
    Harness::error(h.send(b, probe.clone()), ErrorCode::UnknownRoom);
    assert_eq!(
        h.state.retired[0].fingerprint,
        h.state.fingerprint(IpAddr::from([127, 0, 0, 2]))
    );
    let again = h.connection(1);
    Harness::error(h.send(again, probe), ErrorCode::RateLimited);
    h.state.disconnect(again, &mut Effects::default(), h.now);
    let (tx, _) = h.queue();
    let (cancel, _) = watch::channel(false);
    assert_eq!(
        h.state
            .admit(IpAddr::from([127, 0, 0, 1]), tx, cancel, h.now),
        Err(ErrorCode::RateLimited)
    );
    assert_eq!(
        h.state
            .history_mut(IpAddr::from([127, 0, 0, 2]))
            .unwrap()
            .connections,
        1
    );
}
#[test]
fn saturated_history_preserves_known_limits_and_newcomer_opportunities_with_bounded_memory() {
    let mut h = Harness::new(Limits {
        ip_history: 2,
        admissions_per_ip_per_minute: 2,
        ..Limits::default()
    });
    h.state.collide = true;
    for n in 1..=2 + RETIRED_IPS as u32 {
        let id = admit_numbered(&mut h, n).unwrap();
        h.state.disconnect(id, &mut Effects::default(), h.now);
    }
    for n in 10000..20000 {
        let result = admit_numbered(&mut h, n);
        if n < 10032 {
            assert!(result.is_ok());
        } else {
            assert_eq!(result, Err(ErrorCode::RateLimited));
        }
        if let Ok(id) = result {
            h.state.disconnect(id, &mut Effects::default(), h.now);
        }
        assert!(h.state.ips.len() <= 2);
        assert!(h.state.retired.len() <= RETIRED_IPS);
        assert!(h.state.connections.is_empty());
    }
    // Exhausted fallback cannot affect a remembered prefix's remaining token.
    let a = admit_numbered(&mut h, 1).unwrap();
    h.state.disconnect(a, &mut Effects::default(), h.now);
    assert_eq!(admit_numbered(&mut h, 1), Err(ErrorCode::RateLimited));
    h.now += Duration::from_secs(1);
    let b = admit_numbered(&mut h, 30000).unwrap();
    h.state.disconnect(b, &mut Effects::default(), h.now);
    assert_eq!(admit_numbered(&mut h, 1), Err(ErrorCode::RateLimited));
    h.now += IP_HISTORY_TTL;
    let a = admit_numbered(&mut h, 1).unwrap();
    assert!(h.state.retired.is_empty());
    assert_eq!(h.state.ips.len(), 1);
    h.state.disconnect(a, &mut Effects::default(), h.now);
}
#[test]
fn active_overflow_and_unrecorded_connections_preserve_counts_and_attempt_limits() {
    let mut h = Harness::new(Limits {
        ip_history: 1,
        connections_per_ip: 2,
        room_attempts_per_ip_per_minute: 1,
        ..Limits::default()
    });
    let pinned = admit_numbered(&mut h, 1).unwrap();
    for n in 2..=1 + RETIRED_IPS as u32 {
        admit_numbered(&mut h, n).unwrap();
    }
    let source = 10000;
    let a = admit_numbered(&mut h, source).unwrap();
    let b = admit_numbered(&mut h, source).unwrap();
    assert_eq!(admit_numbered(&mut h, source), Err(ErrorCode::RateLimited));
    h.state.disconnect(a, &mut Effects::default(), h.now);
    let c = admit_numbered(&mut h, source).unwrap();
    // No cached IP is required for room requests or cleanup to work safely.
    let probe = ClientMessage::JoinRoom {
        room_code: "0000000000".parse().unwrap(),
        peer_id: PeerId([1; 16]),
    };
    for _ in 0..4 {
        Harness::error(h.send(b, probe.clone()), ErrorCode::UnknownRoom);
    }
    Harness::error(h.send(b, probe), ErrorCode::RateLimited);
    // An active retired prefix remains pinned beyond the normal TTL.
    h.now += IP_HISTORY_TTL;
    h.state.prune_ips(h.now);
    assert_eq!(h.state.ips.len(), 1);
    assert_eq!(h.state.retired.len(), RETIRED_IPS);
    let old = h
        .state
        .connections
        .keys()
        .copied()
        .find(|&id| id != pinned && id != b && id != c)
        .unwrap();
    h.state.disconnect(old, &mut Effects::default(), h.now);
    // This now-reclaimable slot can record the previously untracked prefix;
    // its connection count must include both existing connections.
    assert_eq!(admit_numbered(&mut h, source), Err(ErrorCode::RateLimited));
    assert_eq!(
        h.state
            .history_mut(std::net::Ipv4Addr::from(source).into())
            .unwrap()
            .connections,
        2
    );
    h.state.disconnect(b, &mut Effects::default(), h.now);
    h.state.disconnect(c, &mut Effects::default(), h.now);
    assert_eq!(
        h.state
            .history_mut(std::net::Ipv4Addr::from(source).into())
            .unwrap()
            .connections,
        0
    );
    h.state.disconnect(pinned, &mut Effects::default(), h.now);
    assert_eq!(h.state.ips.values().next().unwrap().connections, 0);
}
#[test]
fn exact_retired_history_fits_previous_overflow_memory_budget() {
    #[allow(dead_code)]
    struct OldIpHistory {
        connections: usize,
        touched: Instant,
        admission: Window,
        attempts: Window,
    }
    assert!(std::mem::size_of::<RetiredIp>() <= std::mem::size_of::<Option<OldIpHistory>>());
    assert!(std::mem::size_of::<IpHistory>() <= std::mem::size_of::<OldIpHistory>());
}

#[test]
fn repeated_minute_rollover_expiry_and_disconnect_keep_all_history_structures_bounded() {
    let mut h = Harness::new(Limits {
        ip_history: 2,
        ..Limits::default()
    });
    for round in 0..1000u32 {
        h.now += Duration::from_secs(1);
        for n in 0..10 {
            let result = admit_numbered(&mut h, round * 10 + n + 1);
            if let Ok(id) = result {
                h.state.disconnect(id, &mut Effects::default(), h.now);
            }
        }
        assert!(h.state.ips.len() <= 2);
        assert!(h.state.retired.len() <= RETIRED_IPS);
        assert!(h.state.connections.is_empty());
        assert!(h.state.ips.values().all(|ip| ip.connections == 0));
        assert!(h.state.retired.iter().all(|r| r.history.connections == 0));
    }
}
