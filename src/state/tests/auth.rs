use super::*;

fn routed(h: &mut Harness, host: u64, code: RoomCode, peer: u8) -> (u64, MemberId) {
    let client = h.connection(peer);
    let join = h.pending(client, code, peer);
    let e = h.send(host, ClientMessage::AuthorizeAck { join_id: join });
    let ServerMessage::PeerJoined { member_id, .. } = e.deliveries[0].message else {
        panic!()
    };
    (client, member_id)
}
fn confirm(peer: u8, member_id: MemberId) -> ClientMessage {
    ClientMessage::ConfirmPeer {
        peer_id: PeerId([peer; 16]),
        member_id,
    }
}
fn revoke(peer: u8, member_id: MemberId) -> ClientMessage {
    ClientMessage::RevokePeer {
        peer_id: PeerId([peer; 16]),
        member_id,
    }
}
#[test]
fn authenticated_member_survives_deadline_and_host_can_revoke_immediately() {
    let mut h = Harness::new(Limits::default());
    let host = h.connection(1);
    let code = h.create(host, 1);
    let (client, member) = routed(&mut h, host, code.clone(), 2);
    h.now += Duration::from_secs(29);
    assert!(h.send(host, confirm(2, member)).deliveries.is_empty());
    assert!(h.send(host, confirm(2, member)).deliveries.is_empty());
    h.now += Duration::from_secs(100);
    h.state.sweep(h.now);
    assert!(h.state.connections.contains_key(&client));
    assert!(matches!(
        h.send(client, Harness::signal(1)).deliveries[0].message,
        ServerMessage::Signal { .. }
    ));
    let e = h.send(host, revoke(2, member));
    assert_eq!(
        e.deliveries[0].message,
        ServerMessage::PeerUnavailable {
            peer_id: PeerId([2; 16])
        }
    );
    assert!(!h.state.connections.contains_key(&client));
    assert_eq!(h.state.pending, 0);
    assert!(!h.state.peers.contains_key(&PeerId([2; 16])));
    routed(&mut h, host, code, 2);
}
#[test]
fn only_own_host_and_exact_member_incarnation_can_confirm_or_revoke() {
    let mut h = Harness::new(Limits::default());
    let host = h.connection(1);
    let other = h.connection(3);
    h.create(other, 3);
    let code = h.create(host, 1);
    let (client, member) = routed(&mut h, host, code.clone(), 2);
    for command in [confirm(2, member), revoke(2, member)] {
        Harness::error(
            h.send(client, command.clone()),
            ErrorCode::ProtocolViolation,
        );
        Harness::error(h.send(other, command), ErrorCode::UnknownTarget);
    }
    Harness::error(h.send(host, confirm(1, member)), ErrorCode::UnknownTarget);
    Harness::error(
        h.send(host, confirm(2, MemberId([99; 16]))),
        ErrorCode::UnknownTarget,
    );
    h.send(host, revoke(2, member));
    let (new_client, new_member) = routed(&mut h, host, code, 2);
    assert_ne!(member, new_member);
    Harness::error(h.send(host, confirm(2, member)), ErrorCode::UnknownTarget);
    Harness::error(h.send(host, revoke(2, member)), ErrorCode::UnknownTarget);
    assert!(h.state.connections.contains_key(&new_client));
    h.now += Duration::from_secs(31);
    h.state.sweep(h.now);
    assert!(!h.state.connections.contains_key(&new_client));
}
#[test]
fn signaling_and_late_confirmation_cannot_extend_or_resurrect_deadline() {
    for late_signal in [false, true] {
        let mut h = Harness::new(Limits::default());
        let host = h.connection(1);
        let code = h.create(host, 1);
        let (client, member) = routed(&mut h, host, code, 2);
        h.now += Duration::from_secs(29);
        h.send(client, Harness::signal(1));
        h.send(host, Harness::signal(2));
        h.now += Duration::from_secs(1);
        // No sweep: the command itself enforces the deadline.
        let e = h.send(
            if late_signal { client } else { host },
            if late_signal {
                Harness::signal(1)
            } else {
                confirm(2, member)
            },
        );
        assert!(e.deliveries.iter().any(|d| d.message
            == ServerMessage::Error {
                code: ErrorCode::JoinTimeout
            }));
        assert!(!h.state.connections.contains_key(&client));
        assert!(h.state.connections.contains_key(&host));
        assert_eq!(h.state.pending, 0);
    }
}
#[test]
fn pending_members_cannot_skip_routing_authorization() {
    let mut h = Harness::new(Limits::default());
    let host = h.connection(1);
    let client = h.connection(2);
    let code = h.create(host, 1);
    let join = h.pending(client, code, 2);
    let room_id = h.state.codes.values().next().copied().unwrap();
    let member = h.state.rooms[&room_id].pending[&join].member.member;
    Harness::error(h.send(host, confirm(2, member)), ErrorCode::UnknownTarget);
    assert_eq!(h.state.pending, 1);
    h.now += Duration::from_secs(13);
    h.state.sweep(h.now);
    assert_eq!(h.state.pending, 0);
}
