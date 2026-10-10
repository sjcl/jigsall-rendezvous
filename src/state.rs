use crate::outbound;
use crate::protocol::*;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::{
    collections::{hash_map::RandomState, HashMap},
    hash::BuildHasher,
    net::IpAddr,
    time::{Duration, Instant},
};
use tokio::sync::watch;
use zeroize::Zeroizing;

#[derive(Clone, Debug)]
pub struct Limits {
    pub connections: usize,
    pub rooms: usize,
    pub participants: usize,
    pub pending: usize,
    pub outbound: usize,
    pub outbound_bytes_per_connection: usize,
    pub outbound_bytes_global: usize,
    pub ip_history: usize,
    pub connections_per_ip: usize,
    pub admissions_per_ip_per_minute: u32,
    pub room_attempts_per_ip_per_minute: u32,
    pub pending_timeout: Duration,
    pub game_auth_timeout: Duration,
    pub idle_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub write_timeout: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            connections: 1024,
            rooms: 256,
            participants: 64,
            pending: 512,
            outbound: 128,
            outbound_bytes_per_connection: 256 * 1024,
            outbound_bytes_global: 32 * 1024 * 1024,
            ip_history: 4096,
            connections_per_ip: 64,
            admissions_per_ip_per_minute: 60,
            room_attempts_per_ip_per_minute: 120,
            pending_timeout: Duration::from_secs(12),
            game_auth_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(15),
            heartbeat_timeout: Duration::from_secs(15),
            write_timeout: Duration::from_secs(2),
        }
    }
}
impl Limits {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        for (name, value, maximum) in [
            ("MAX_CONNECTIONS", self.connections, 1024),
            ("MAX_ROOMS", self.rooms, 256),
            ("MAX_PARTICIPANTS", self.participants, 64),
            ("MAX_PENDING_JOINS", self.pending, 512),
            ("MAX_OUTBOUND_MESSAGES", self.outbound, 128),
            ("ip_history", self.ip_history, 4096),
            ("MAX_CONNECTIONS_PER_IP", self.connections_per_ip, 1024),
            (
                "MAX_ADMISSIONS_PER_IP_PER_MINUTE",
                self.admissions_per_ip_per_minute as usize,
                65536,
            ),
            (
                "MAX_ROOM_ATTEMPTS_PER_IP_PER_MINUTE",
                self.room_attempts_per_ip_per_minute as usize,
                65536,
            ),
            (
                "MAX_OUTBOUND_BYTES_PER_CONNECTION",
                self.outbound_bytes_per_connection,
                1024 * 1024,
            ),
            (
                "MAX_OUTBOUND_BYTES_GLOBAL",
                self.outbound_bytes_global,
                64 * 1024 * 1024,
            ),
        ] {
            if !(1..=maximum).contains(&value) {
                return Err(name);
            }
        }
        for d in [
            self.pending_timeout,
            self.game_auth_timeout,
            self.idle_timeout,
            self.heartbeat_interval,
            self.heartbeat_timeout,
            self.write_timeout,
        ] {
            if d.is_zero() || d > Duration::from_secs(60) {
                return Err("timeout must be within 0..=60 seconds and nonzero");
            }
        }
        Ok(())
    }
}
#[derive(Clone)]
pub(crate) struct Window {
    started: Instant,
    count: u32,
    limit: u32,
    period: Duration,
}
impl Window {
    pub(crate) fn new(limit: u32, period: Duration, now: Instant) -> Self {
        Self {
            started: now,
            count: 0,
            limit,
            period,
        }
    }
    pub(crate) fn take(&mut self, now: Instant) -> bool {
        if now.saturating_duration_since(self.started) >= self.period {
            self.started = now;
            self.count = 0;
        }
        if self.count >= self.limit {
            return false;
        }
        self.count += 1;
        true
    }
}
const RETIRED_IPS: usize = 512;
const IP_WINDOW: Duration = Duration::from_secs(60);
const IP_HISTORY_TTL: Duration = Duration::from_secs(300);
// The policy is common to all IPs; omit duplicated limit/period fields so
// exact ownership fits within the previous compressed-history allocation.
#[derive(Clone)]
struct HistoryWindow {
    started: Instant,
    count: u32,
}
impl HistoryWindow {
    fn take(&mut self, limit: u32, now: Instant) -> bool {
        if now.saturating_duration_since(self.started) >= IP_WINDOW {
            self.started = now;
            self.count = 0;
        }
        if self.count >= limit {
            return false;
        }
        self.count += 1;
        true
    }
    fn spent(&self, now: Instant) -> bool {
        self.count != 0 && now.saturating_duration_since(self.started) < IP_WINDOW
    }
}
#[derive(Clone)]
struct IpHistory {
    connections: usize,
    touched: Instant,
    admission: HistoryWindow,
    attempts: HistoryWindow,
}
impl IpHistory {
    fn reclaimable(&self, now: Instant) -> bool {
        self.connections == 0 && !self.admission.spent(now) && !self.attempts.spent(now)
    }
}
struct RetiredIp {
    owner: IpAddr,
    fingerprint: u64,
    history: IpHistory,
}
#[derive(Clone, Copy)]
enum Membership {
    Host(RoomId),
    Pending(RoomId, JoinId),
    Routed(RoomId),
    Established(RoomId),
}
pub(crate) struct Connection {
    tx: outbound::Sender,
    cancel: watch::Sender<bool>,
    ip: IpAddr,
    created: Instant,
    membership: Option<Membership>,
    peer: Option<PeerId>,
    messages: Window,
    signals: Window,
    attempts: Window,
}
#[derive(Clone, Copy)]
struct Member {
    connection: u64,
    peer: PeerId,
    member: MemberId,
    auth_deadline: Option<Instant>,
}
struct Pending {
    member: Member,
    deadline: Instant,
}
struct Room {
    code: RoomCode,
    host: Member,
    members: HashMap<PeerId, Member>,
    pending: HashMap<JoinId, Pending>,
}
pub(crate) struct Delivery {
    pub target: u64,
    pub tx: outbound::Sender,
    pub message: ServerMessage,
    // Signal congestion reports backpressure to its source; it must not let a
    // flooding participant tear down the host's control plane.
    pub source: Option<u64>,
    // Activation may release the joiner only if its host's notification was
    // enqueued successfully in this same ordered batch.
    pub requires: Option<u64>,
}
#[derive(Default)]
pub(crate) struct Effects {
    pub deliveries: Vec<Delivery>,
    pub cancel: Vec<watch::Sender<bool>>,
}
pub(crate) struct State {
    pub authority: AuthorityId,
    pub limits: Limits,
    pub stopping: bool,
    next: u64,
    pub connections: HashMap<u64, Connection>,
    rooms: HashMap<RoomId, Room>,
    codes: HashMap<RoomCode, RoomId>,
    peers: HashMap<PeerId, u64>,
    ips: HashMap<IpAddr, IpHistory>,
    retired: Vec<RetiredIp>,
    untracked_admissions: Window,
    untracked_attempts: Window,
    #[cfg(test)]
    collide: bool,
    history_hash: RandomState,
    abuse_secret: Zeroizing<[u8; 32]>,
    pending: usize,
}
fn random() -> [u8; 16] {
    *uuid::Uuid::new_v4().as_bytes()
}
impl State {
    pub fn new(limits: Limits) -> Self {
        limits.validate().expect("invalid rendezvous limits");
        let mut abuse_secret = Zeroizing::new([0; 32]);
        getrandom::fill(&mut *abuse_secret).expect("OS random source unavailable");
        Self {
            authority: AuthorityId(random()),
            limits,
            stopping: false,
            next: 1,
            connections: HashMap::new(),
            rooms: HashMap::new(),
            codes: HashMap::new(),
            peers: HashMap::new(),
            ips: HashMap::new(),
            retired: Vec::with_capacity(RETIRED_IPS),
            untracked_admissions: Window::new(32, Duration::from_secs(1), Instant::now()),
            untracked_attempts: Window::new(32, Duration::from_secs(1), Instant::now()),
            #[cfg(test)]
            collide: false,
            history_hash: RandomState::new(),
            abuse_secret,
            pending: 0,
        }
    }
    pub fn admit(
        &mut self,
        ip: IpAddr,
        tx: outbound::Sender,
        cancel: watch::Sender<bool>,
        now: Instant,
    ) -> Result<u64, ErrorCode> {
        // The caller has resolved TrustedProxies against the full source IP.
        let ip = crate::proxy::prefix(ip);
        self.prune_ips(now);
        if self.stopping || self.connections.len() >= self.limits.connections {
            return Err(ErrorCode::Capacity);
        }
        self.ensure_history(ip, now);
        let fingerprint = self.fingerprint(ip);
        let history = self.ips.get_mut(&ip).or_else(|| {
            self.retired
                .iter_mut()
                .find(|r| r.fingerprint == fingerprint && r.owner == ip)
                .map(|r| &mut r.history)
        });
        if let Some(history) = history {
            history.touched = now;
            if history.connections >= self.limits.connections_per_ip
                || !history
                    .admission
                    .take(self.limits.admissions_per_ip_per_minute, now)
            {
                return Err(ErrorCode::RateLimited);
            }
        } else {
            // Only unrecorded prefixes share this budget. Connection ownership
            // remains in the already bounded connection table, even without a
            // retained rate window (including custom all-active small tables).
            if self.connections.values().filter(|c| c.ip == ip).count()
                >= self.limits.connections_per_ip
                || !self.untracked_admissions.take(now)
            {
                return Err(ErrorCode::RateLimited);
            }
        }
        let id = self.next;
        self.next = self.next.checked_add(1).ok_or(ErrorCode::Capacity)?;
        if let Some(history) = self.history_mut(ip) {
            history.connections += 1;
        }
        self.connections.insert(
            id,
            Connection {
                tx,
                cancel,
                ip,
                created: now,
                membership: None,
                peer: None,
                messages: Window::new(256, Duration::from_secs(1), now),
                signals: Window::new(32, Duration::from_secs(1), now),
                attempts: Window::new(4, Duration::from_secs(10), now),
            },
        );
        Ok(id)
    }
    fn fingerprint(&self, ip: IpAddr) -> u64 {
        #[cfg(test)]
        if self.collide {
            return 0;
        }
        self.history_hash.hash_one(ip)
    }
    fn history_mut(&mut self, ip: IpAddr) -> Option<&mut IpHistory> {
        let fingerprint = self.fingerprint(ip);
        self.ips.get_mut(&ip).or_else(|| {
            self.retired
                .iter_mut()
                .find(|r| r.fingerprint == fingerprint && r.owner == ip)
                .map(|r| &mut r.history)
        })
    }
    fn remember(&mut self, ip: IpAddr, history: IpHistory, now: Instant) -> Result<(), IpHistory> {
        if self.retired.len() == RETIRED_IPS {
            let victim = self
                .retired
                .iter()
                .enumerate()
                .filter(|(_, r)| r.history.reclaimable(now))
                .min_by_key(|(_, r)| r.history.touched)
                .map(|(i, _)| i);
            let Some(victim) = victim else {
                return if history.reclaimable(now) {
                    Ok(())
                } else {
                    Err(history)
                };
            };
            self.retired.swap_remove(victim);
        }
        let fingerprint = self.fingerprint(ip);
        self.retired.push(RetiredIp {
            owner: ip,
            fingerprint,
            history,
        });
        Ok(())
    }
    fn ensure_history(&mut self, ip: IpAddr, now: Instant) {
        if self.history_mut(ip).is_some() {
            return;
        }
        let history = IpHistory {
            connections: self.connections.values().filter(|c| c.ip == ip).count(),
            touched: now,
            admission: HistoryWindow {
                started: now,
                count: 0,
            },
            attempts: HistoryWindow {
                started: now,
                count: 0,
            },
        };
        if self.ips.len() >= self.limits.ip_history {
            let victim = self
                .ips
                .iter()
                .filter(|(_, h)| h.connections == 0)
                .min_by_key(|(_, h)| h.touched)
                .map(|(&ip, _)| ip);
            let Some(victim) = victim else {
                // Keep active primary histories pinned; bounded exact overflow
                // can also hold active prefixes when ip_history is configured small.
                let _ = self.remember(ip, history, now);
                return;
            };
            let old = self.ips.remove(&victim).unwrap();
            if let Err(old) = self.remember(victim, old, now) {
                self.ips.insert(victim, old);
                return;
            }
        }
        self.ips.insert(ip, history);
    }
    fn abuse_key(&self, room: RoomId, ip: IpAddr) -> AbuseKey {
        let mut mac = Hmac::<Sha256>::new_from_slice(&*self.abuse_secret).expect("HMAC key length");
        mac.update(b"jigsall-rendezvous-abuse-v1\0");
        mac.update(&room.0);
        match crate::proxy::prefix(ip) {
            IpAddr::V4(ip) => {
                mac.update(&[4]);
                mac.update(&ip.octets());
            }
            IpAddr::V6(ip) => {
                mac.update(&[6]);
                mac.update(&ip.octets()[..8]);
            }
        }
        AbuseKey(
            mac.finalize().into_bytes()[..16]
                .try_into()
                .expect("128-bit key"),
        )
    }
    fn emit(&self, effects: &mut Effects, target: u64, message: ServerMessage) {
        if let Some(c) = self.connections.get(&target) {
            effects.deliveries.push(Delivery {
                target,
                tx: c.tx.clone(),
                message,
                source: None,
                requires: None,
            });
        }
    }
    pub fn welcome(&self, id: u64) -> Effects {
        let mut e = Effects::default();
        self.emit(
            &mut e,
            id,
            ServerMessage::Welcome {
                authority_id: self.authority,
                turn: None,
            },
        );
        e
    }
    pub fn error(&self, id: u64, code: ErrorCode) -> Effects {
        let mut e = Effects::default();
        self.emit(&mut e, id, ServerMessage::Error { code });
        e
    }
    pub fn handle(&mut self, id: u64, message: ClientMessage, now: Instant) -> Effects {
        let Some(c) = self.connections.get_mut(&id) else {
            return Effects::default();
        };
        if !c.messages.take(now) {
            let mut e = self.error(id, ErrorCode::RateLimited);
            self.disconnect(id, &mut e, now);
            return e;
        }
        if matches!(
            message,
            ClientMessage::CreateRoom { .. } | ClientMessage::JoinRoom { .. }
        ) {
            let source = c.ip;
            if !c.attempts.take(now) {
                return self.error(id, ErrorCode::RateLimited);
            }
            let limit = self.limits.room_attempts_per_ip_per_minute;
            let allowed = if let Some(ip) = self.history_mut(source) {
                ip.touched = now;
                ip.attempts.take(limit, now)
            } else {
                self.untracked_attempts.take(now)
            };
            if !allowed {
                return self.error(id, ErrorCode::RateLimited);
            }
        }
        let result = match message {
            ClientMessage::CreateRoom { peer_id } => self.create(id, peer_id),
            ClientMessage::JoinRoom { room_code, peer_id } => {
                self.join(id, room_code, peer_id, now)
            }
            ClientMessage::AuthorizeAck { join_id } => self.ack(id, join_id, now),
            ClientMessage::AuthorizeReject { join_id } => self.reject(id, join_id, now),
            ClientMessage::ConfirmPeer { peer_id, member_id } => {
                self.lifecycle(id, peer_id, member_id, false, now)
            }
            ClientMessage::RevokePeer { peer_id, member_id } => {
                self.lifecycle(id, peer_id, member_id, true, now)
            }
            ClientMessage::Signal {
                to_peer_id,
                payload_base64,
            } => self.signal(id, to_peer_id, payload_base64, now),
            ClientMessage::LeaveRoom {} => {
                let mut e = Effects::default();
                self.emit(&mut e, id, ServerMessage::RoomClosed {});
                self.disconnect(id, &mut e, now);
                Ok(e)
            }
        };
        result.unwrap_or_else(|code| self.error(id, code))
    }
    fn vacant(&self, id: u64, peer: PeerId) -> Result<(), ErrorCode> {
        if self.connections[&id].membership.is_some() {
            return Err(ErrorCode::ProtocolViolation);
        }
        if self.peers.contains_key(&peer) {
            return Err(ErrorCode::DuplicatePeer);
        }
        Ok(())
    }
    fn code_with(
        &self,
        mut generate: impl FnMut() -> Result<RoomCode, ErrorCode>,
    ) -> Result<RoomCode, ErrorCode> {
        for _ in 0..32 {
            let code = generate()?;
            if !self.codes.contains_key(&code) {
                return Ok(code);
            }
        }
        Err(ErrorCode::Capacity)
    }
    fn create(&mut self, id: u64, peer: PeerId) -> Result<Effects, ErrorCode> {
        self.vacant(id, peer)?;
        if self.rooms.len() >= self.limits.rooms {
            return Err(ErrorCode::Capacity);
        }
        let code = self.code_with(|| {
            let mut bytes = [0; 10];
            getrandom::fill(&mut bytes).map_err(|_| ErrorCode::Capacity)?;
            bytes
                .iter()
                .map(|b| CODE_ALPHABET[(b & 31) as usize] as char)
                .collect::<String>()
                .parse()
                .map_err(|_| ErrorCode::Capacity)
        })?;
        let room = RoomId(random());
        let host = Member {
            connection: id,
            peer,
            member: MemberId(random()),
            auth_deadline: None,
        };
        self.rooms.insert(
            room,
            Room {
                code: code.clone(),
                host,
                members: HashMap::new(),
                pending: HashMap::new(),
            },
        );
        self.codes.insert(code.clone(), room);
        self.peers.insert(peer, id);
        let c = self.connections.get_mut(&id).unwrap();
        c.membership = Some(Membership::Host(room));
        c.peer = Some(peer);
        c.signals.limit = 128;
        let mut e = Effects::default();
        self.emit(
            &mut e,
            id,
            ServerMessage::RoomCreated {
                room_id: room,
                room_code: code,
                self_member_id: host.member,
            },
        );
        Ok(e)
    }
    fn join(
        &mut self,
        id: u64,
        code: RoomCode,
        peer: PeerId,
        now: Instant,
    ) -> Result<Effects, ErrorCode> {
        self.vacant(id, peer)?;
        let room_id = *self.codes.get(&code).ok_or(ErrorCode::UnknownRoom)?;
        let abuse_key = self.abuse_key(room_id, self.connections[&id].ip);
        let room = self.rooms.get_mut(&room_id).unwrap();
        if room.members.len() + room.pending.len() >= self.limits.participants {
            return Err(ErrorCode::RoomFull);
        }
        if self.pending >= self.limits.pending {
            return Err(ErrorCode::Capacity);
        }
        let join = JoinId(random());
        let member = Member {
            connection: id,
            peer,
            member: MemberId(random()),
            auth_deadline: None,
        };
        room.pending.insert(
            join,
            Pending {
                member,
                deadline: now + self.limits.pending_timeout,
            },
        );
        let host = room.host.connection;
        self.pending += 1;
        self.peers.insert(peer, id);
        let c = self.connections.get_mut(&id).unwrap();
        c.membership = Some(Membership::Pending(room_id, join));
        c.peer = Some(peer);
        let mut e = Effects::default();
        self.emit(
            &mut e,
            host,
            ServerMessage::AuthorizePeer {
                join_id: join,
                peer_id: peer,
                member_id: member.member,
                abuse_key,
            },
        );
        Ok(e)
    }
    fn ack(&mut self, id: u64, join: JoinId, now: Instant) -> Result<Effects, ErrorCode> {
        let Some(Membership::Host(room_id)) = self.connections[&id].membership else {
            return Err(ErrorCode::ProtocolViolation);
        };
        let room = self.rooms.get_mut(&room_id).unwrap();
        let pending = room.pending.get(&join).ok_or(ErrorCode::UnknownTarget)?;
        if now >= pending.deadline {
            return Err(ErrorCode::JoinTimeout);
        }
        let mut member = room.pending.remove(&join).unwrap().member;
        member.auth_deadline = Some(now + self.limits.game_auth_timeout);
        let host = room.host;
        room.members.insert(member.peer, member);
        self.pending -= 1;
        self.connections
            .get_mut(&member.connection)
            .unwrap()
            .membership = Some(Membership::Routed(room_id));
        let host_abuse_key = self.abuse_key(room_id, self.connections[&host.connection].ip);
        let mut e = Effects::default();
        // This ordering is a protocol contract. Server::transition serializes
        // the state commit and both enqueues against every competing relay.
        self.emit(
            &mut e,
            id,
            ServerMessage::PeerJoined {
                peer_id: member.peer,
                member_id: member.member,
            },
        );
        self.emit(
            &mut e,
            member.connection,
            ServerMessage::RoomJoined {
                room_id,
                self_member_id: member.member,
                host_peer_id: host.peer,
                host_member_id: host.member,
                host_abuse_key,
            },
        );
        e.deliveries.last_mut().unwrap().requires = Some(id);
        Ok(e)
    }
    fn reject(&mut self, id: u64, join: JoinId, now: Instant) -> Result<Effects, ErrorCode> {
        let Some(Membership::Host(room_id)) = self.connections[&id].membership else {
            return Err(ErrorCode::ProtocolViolation);
        };
        let pending = self.rooms[&room_id]
            .pending
            .get(&join)
            .ok_or(ErrorCode::UnknownTarget)?;
        let target = pending.member.connection;
        let code = if now >= pending.deadline {
            ErrorCode::JoinTimeout
        } else {
            ErrorCode::Capacity
        };
        let mut e = Effects::default();
        self.emit(&mut e, target, ServerMessage::Error { code });
        // PeerUnavailable completes the host's rejection transaction, even
        // though it installed no route for this pending membership.
        self.disconnect(target, &mut e, now);
        Ok(e)
    }
    fn lifecycle(
        &mut self,
        id: u64,
        peer: PeerId,
        member_id: MemberId,
        revoke: bool,
        now: Instant,
    ) -> Result<Effects, ErrorCode> {
        let Some(Membership::Host(room_id)) = self.connections[&id].membership else {
            return Err(ErrorCode::ProtocolViolation);
        };
        let member = *self.rooms[&room_id]
            .members
            .get(&peer)
            .ok_or(ErrorCode::UnknownTarget)?;
        if member.member != member_id {
            return Err(ErrorCode::UnknownTarget);
        }
        let mut e = Effects::default();
        if revoke || member.auth_deadline.is_some_and(|deadline| now >= deadline) {
            if !revoke {
                self.emit(
                    &mut e,
                    member.connection,
                    ServerMessage::Error {
                        code: ErrorCode::JoinTimeout,
                    },
                );
            }
            self.disconnect(member.connection, &mut e, now);
        } else {
            self.rooms
                .get_mut(&room_id)
                .unwrap()
                .members
                .get_mut(&peer)
                .unwrap()
                .auth_deadline = None;
            self.connections
                .get_mut(&member.connection)
                .unwrap()
                .membership = Some(Membership::Established(room_id));
        }
        Ok(e)
    }
    fn signal(
        &mut self,
        id: u64,
        to: PeerId,
        payload: String,
        now: Instant,
    ) -> Result<Effects, ErrorCode> {
        let c = self.connections.get_mut(&id).unwrap();
        if !c.signals.take(now) {
            return Err(ErrorCode::RateLimited);
        }
        let (room_id, host) = match c.membership {
            Some(Membership::Host(room)) => (room, true),
            Some(Membership::Routed(room) | Membership::Established(room)) => (room, false),
            _ => return Err(ErrorCode::NotInRoom),
        };
        let from = c.peer.unwrap();
        let room = &self.rooms[&room_id];
        // Check the fixed authentication deadline even between sweeper ticks.
        let participant = if host { to } else { from };
        if let Some(member) = room.members.get(&participant).copied() {
            if member.auth_deadline.is_some_and(|deadline| now >= deadline) {
                let mut e = Effects::default();
                self.emit(
                    &mut e,
                    member.connection,
                    ServerMessage::Error {
                        code: ErrorCode::JoinTimeout,
                    },
                );
                self.disconnect(member.connection, &mut e, now);
                return Ok(e);
            }
        }
        let target = if host {
            room.members.get(&to).ok_or(ErrorCode::UnknownTarget)?
        } else if to == room.host.peer {
            &room.host
        } else if room.members.contains_key(&to) {
            return Err(ErrorCode::ProtocolViolation);
        } else {
            return Err(ErrorCode::UnknownTarget);
        };
        // Opaque GNS bytes are decoded only to validate size/canonical Base64.
        decode_signal(&payload)?;
        let mut e = Effects::default();
        self.emit(
            &mut e,
            target.connection,
            ServerMessage::Signal {
                from_peer_id: from,
                payload_base64: payload,
            },
        );
        e.deliveries[0].source = Some(id);
        Ok(e)
    }
    pub fn disconnect(&mut self, id: u64, e: &mut Effects, now: Instant) {
        let Some(c) = self.connections.remove(&id) else {
            return;
        };
        e.cancel.push(c.cancel);
        if let Some(ip) = self.history_mut(c.ip) {
            ip.connections -= 1;
            ip.touched = now;
        }
        if let Some(peer) = c.peer {
            self.peers.remove(&peer);
        }
        match c.membership {
            Some(Membership::Host(room_id)) => {
                if let Some(room) = self.rooms.remove(&room_id) {
                    self.codes.remove(&room.code);
                    self.pending -= room.pending.len();
                    let others = room
                        .members
                        .values()
                        .copied()
                        .chain(room.pending.values().map(|p| p.member))
                        .collect::<Vec<_>>();
                    for member in others {
                        self.peers.remove(&member.peer);
                        if let Some(other) = self.connections.get_mut(&member.connection) {
                            other.peer = None;
                            other.membership = None;
                            // Fixed new admission deadline; arbitrary traffic cannot renew it.
                            other.created = now;
                        }
                        self.emit(e, member.connection, ServerMessage::RoomClosed {});
                    }
                }
            }
            Some(Membership::Pending(room_id, join)) => {
                if let Some(room) = self.rooms.get_mut(&room_id) {
                    if room.pending.remove(&join).is_some() {
                        self.pending -= 1;
                    }
                    let host = room.host.connection;
                    self.emit(
                        e,
                        host,
                        ServerMessage::PeerUnavailable {
                            peer_id: c.peer.unwrap(),
                        },
                    );
                }
            }
            Some(Membership::Routed(room_id) | Membership::Established(room_id)) => {
                if let Some(room) = self.rooms.get_mut(&room_id) {
                    room.members.remove(&c.peer.unwrap());
                    let host = room.host.connection;
                    self.emit(
                        e,
                        host,
                        ServerMessage::PeerUnavailable {
                            peer_id: c.peer.unwrap(),
                        },
                    );
                }
            }
            None => {}
        }
    }
    fn prune_ips(&mut self, now: Instant) {
        self.ips.retain(|_, ip| {
            ip.connections != 0 || now.saturating_duration_since(ip.touched) < IP_HISTORY_TTL
        });
        self.retired.retain(|r| {
            r.history.connections != 0
                || now.saturating_duration_since(r.history.touched) < IP_HISTORY_TTL
        });
    }
    pub fn sweep(&mut self, now: Instant) -> Effects {
        let mut expired = Vec::new();
        for room in self.rooms.values() {
            for member in room
                .members
                .values()
                .filter(|m| m.auth_deadline.is_some_and(|deadline| now >= deadline))
            {
                expired.push((member.connection, ErrorCode::JoinTimeout));
            }
            for p in room.pending.values().filter(|p| now >= p.deadline) {
                expired.push((p.member.connection, ErrorCode::JoinTimeout));
            }
        }
        for (&id, c) in &self.connections {
            if c.membership.is_none()
                && now.saturating_duration_since(c.created) >= self.limits.idle_timeout
            {
                expired.push((id, ErrorCode::NotInRoom));
            }
        }
        let mut e = Effects::default();
        for (id, code) in expired {
            self.emit(&mut e, id, ServerMessage::Error { code });
            self.disconnect(id, &mut e, now);
        }
        self.prune_ips(now);
        e
    }
    pub fn shutdown(&mut self, now: Instant) -> Effects {
        self.stopping = true;
        let mut e = Effects::default();
        let ids = self.connections.keys().copied().collect::<Vec<_>>();
        for &id in &ids {
            self.emit(&mut e, id, ServerMessage::RoomClosed {});
        }
        self.rooms.clear();
        self.codes.clear();
        self.peers.clear();
        self.pending = 0;
        for connection in self.connections.values_mut() {
            connection.membership = None;
            connection.peer = None;
        }
        for id in ids {
            self.disconnect(id, &mut e, now);
        }
        e
    }
}

#[cfg(test)]
mod tests;
