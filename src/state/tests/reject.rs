use super::*;

#[test]
fn rejection_releases_pending_slot_and_same_peer_can_join_again() {
    let mut h = Harness::new(Limits {
        participants: 1,
        admissions_per_ip_per_minute: 512,
        room_attempts_per_ip_per_minute: 512,
        ..Limits::default()
    });
    let host = h.connection(1);
    let code = h.create(host, 1);
    let room = h.state.codes[&code];
    for _ in 0..130 {
        let client = h.connection(2);
        let join = h.pending(client, code.clone(), 2);
        let e = h.send(host, ClientMessage::AuthorizeReject { join_id: join });
        assert_eq!(e.deliveries.len(), 2);
        assert_eq!(e.deliveries[0].target, client);
        assert_eq!(
            e.deliveries[0].message,
            ServerMessage::Error {
                code: ErrorCode::Capacity
            }
        );
        assert_eq!(e.deliveries[1].target, host);
        assert_eq!(
            e.deliveries[1].message,
            ServerMessage::PeerUnavailable {
                peer_id: PeerId([2; 16])
            }
        );
        assert_eq!(h.state.pending, 0);
        assert!(!h.state.peers.contains_key(&PeerId([2; 16])));
        assert!(h.state.rooms[&room].pending.is_empty());
        assert!(h.state.rooms[&room].members.is_empty());
        assert_eq!(h.state.connections.len(), 1);
    }
    let client = h.connection(2);
    let join = h.pending(client, code, 2);
    let e = h.send(host, ClientMessage::AuthorizeAck { join_id: join });
    assert!(matches!(
        e.deliveries[0].message,
        ServerMessage::PeerJoined { .. }
    ));
    assert!(matches!(
        e.deliveries[1].message,
        ServerMessage::RoomJoined { .. }
    ));
}

#[test]
fn only_own_host_can_reject_a_pending_join() {
    let mut h = Harness::new(Limits::default());
    let host = h.connection(1);
    let other_host = h.connection(3);
    h.create(other_host, 3);
    let code = h.create(host, 1);
    let client = h.connection(2);
    let join = h.pending(client, code, 2);
    Harness::error(
        h.send(client, ClientMessage::AuthorizeReject { join_id: join }),
        ErrorCode::ProtocolViolation,
    );
    Harness::error(
        h.send(other_host, ClientMessage::AuthorizeReject { join_id: join }),
        ErrorCode::UnknownTarget,
    );
    assert_eq!(h.state.pending, 1);
    h.send(host, ClientMessage::AuthorizeAck { join_id: join });
    Harness::error(
        h.send(host, ClientMessage::AuthorizeReject { join_id: join }),
        ErrorCode::UnknownTarget,
    );
    assert!(h.state.connections.contains_key(&client));
    assert!(h.state.connections.contains_key(&host));
}

#[test]
fn cancelled_or_expired_join_does_not_make_late_ack_or_reject_fatal() {
    for expiry in [false, true] {
        let mut h = Harness::new(Limits::default());
        let host = h.connection(1);
        let code = h.create(host, 1);
        let client = h.connection(2);
        let join = h.pending(client, code.clone(), 2);
        if expiry {
            h.now += Duration::from_secs(12);
            let e = h.send(host, ClientMessage::AuthorizeReject { join_id: join });
            assert_eq!(
                e.deliveries[0].message,
                ServerMessage::Error {
                    code: ErrorCode::JoinTimeout
                }
            );
        } else {
            h.state.disconnect(client, &mut Effects::default(), h.now);
        }
        for command in [
            ClientMessage::AuthorizeAck { join_id: join },
            ClientMessage::AuthorizeReject { join_id: join },
        ] {
            Harness::error(h.send(host, command), ErrorCode::UnknownTarget);
        }
        assert_eq!(h.state.pending, 0);
        let client = h.connection(2);
        let fresh = h.pending(client, code, 2);
        assert_ne!(fresh, join);
        assert!(matches!(
            h.send(host, ClientMessage::AuthorizeAck { join_id: fresh })
                .deliveries[1]
                .message,
            ServerMessage::RoomJoined { .. }
        ));
    }
}
