# Rendezvous/signaling protocol v1

Endpoint: `GET /v1/ws`. Every application message is one UTF-8 JSON text message
with numeric `"v":1` and a lower snake_case `"type"`. Unknown versions return
`unsupported_version` and close the control connection. Unknown message types,
fields, duplicate fields, malformed IDs/JSON and oversized messages fail closed.
Binary application messages close the connection. WS frames and reassembled
messages are each bounded at 24 KiB. WS Ping/Pong is outside the JSON schema.

## Identities

| Wire field/type | Meaning |
| --- | --- |
| AuthorityId | CSPRNG 128-bit server process ID; changes on restart |
| RoomId | CSPRNG 128-bit room identity; issued only by server |
| RoomCode | Ten Crockford Base32 characters; CSPRNG 50 bits, ASCII case normalized to upper, no I/L/O/U aliases |
| MemberId | CSPRNG 128-bit anonymous member identity; issued only by server |
| PeerId | Existing GNS/Puzzella 128-bit process routing label; bound to the current WebSocket membership |
| JoinId | CSPRNG 128-bit pending authorization transaction; host ACK/Reject only |

IDs are non-nil lowercase hyphenated UUID-shaped strings; noncanonical spellings
are rejected. Random server IDs use UUID v4. Client PeerIds need not carry UUID
version/variant bits; all nonzero 16-byte values have the same canonical encoding.
Clients never mint RoomId, MemberId, AuthorityId or a signal sender identity.
Only the host echoes a server-issued MemberId in ConfirmPeer/RevokePeer.
Room code is not a game authentication credential. MemberId has no account/Sybil
guarantee; future account authentication can replace its use as RouteOrigin.account
in an explicitly versioned extension. Current routing key is
`RouteOrigin(authority_id, room_id, remote_member_id)`.

## Handshake and topology

1. Server sends Welcome on an admitted WebSocket.
2. Host sends CreateRoom(peer_id); server binds it and returns RoomCreated.
3. Joiner sends JoinRoom(room_code, peer_id). The server reserves a participant
   slot and emits AuthorizePeer(join_id, peer_id, member_id) **only to the host**.
4. Host adapter verifies its current room/authority, calls authorize_peer, then
   sends AuthorizeAck(join_id). ACK must originate from this room's host before
   the fixed 12-second deadline. A duplicate/stale/foreign ACK returns
   unknown_target; an expired pending ACK returns join_timeout. These host-side
   cancellation/expiry races are nonfatal and do not activate any member.
   If a retained unavailable/revoking PeerId or local route/native capacity
   prevents safe admission, the host instead sends AuthorizeReject(join_id).
   Only this room's host may reject its pending transaction. The server sends
   capacity to the joiner (join_timeout if expired), closes that connection,
   immediately frees all pending/peer/IP/participant resources and emits
   PeerUnavailable to the host as rejection completion. The room continues.
   A stale/duplicate/foreign Reject returns unknown_target; a participant
   sender returns protocol_violation.
5. Server commits the participant's Routed state with a fixed 30-second game-auth
   deadline, **enqueues PeerJoined to the
   host first**, then **enqueues RoomJoined to the joiner**. The state commit and
   both nonblocking enqueues are one serialized transition: concurrent message
   handling, relays and cleanup cannot overtake them. Failed host enqueue closes
   that control plane/room and never releases the joiner with RoomJoined.
   Joiner installs the host's route and only then emits
   HostReady to its caller. The caller may connect_peer; the adapter starts no
   password bootstrap or Sync/Ready.
6. Signals travel only between this host and a Routed/Established participant.
   Participant-to-participant relay is protocol_violation. Targets outside the
   sender's room (including pending targets) are unknown_target. A pending sender
   is not_in_room. Sender PeerId is taken from server connection state.
7. After SPAKE2 reaches Authenticated, the host sends ConfirmPeer(peer_id,
   member_id). The exact current member becomes Established and loses its auth
   deadline. Confirmation must precede expiry; it never resurrects a member.
   Duplicate confirmation of the same Established member is idempotent.
   Do not wait for Ready, image transfer, baseline or catch-up.
8. On definitive ICE/authentication/bootstrap failure, the host sends
   RevokePeer(peer_id, member_id), closing that member's control connection and
   freeing its slot immediately. Both commands require the current room host
   and exact member incarnation. Stale/foreign targets return unknown_target;
   participant senders return protocol_violation. Hosts treat unknown_target
   and join_timeout as nonfatal lifecycle races. No password, PAKE transcript
   or game player identity is sent to this server.

Pending has a fixed 12-second ACK deadline; Routed has a fixed 30-second game-auth
deadline measured from ACK; Established has neither deadline. Ping/Pong, signals
and repeated commands do not extend either deadline. Expired Routed sockets get
join_timeout and close, and their host gets PeerUnavailable. Signaling and Confirm
also check expiry between sweeper ticks. Global pending limits count pre-ACK joins;
all three participant states count toward the room limit.

The per-WebSocket FIFO contract is `PeerJoined → Signal` at the host and
`RoomJoined → Signal` at the joiner, including when either peer signals immediately
after its notification. Cross-socket network delivery order is not assumed.
This ordering prevents signaling before route authorization **and activation**.
Clients may fail closed if an authenticated server violates this contract;
pending routes must not be activated implicitly by a Signal.
Cleanup and new authorization share that serialized FIFO: an old member's
PeerUnavailable precedes AuthorizePeer for a reused PeerId; rejected pending
membership completion precedes any further authorization reusing it. The host
tracks rejected transactions separately and consumes their PeerUnavailable
without changing an older retained native route for that same PeerId, even if
that route has already retired. Clients keep this history bounded by the 64-slot
room limit and do not evict it before completion.
Host adapter does not advertise a peer to its caller until PeerJoined confirms
activation. The client uses new_routed and never derives identities from opaque
GNS payloads. Subsequent Connected still requires the existing SecureTransport /
SPAKE2 password protocol before any player or game state transition.

## Messages

Client: create_room(peer_id), join_room(room_code, peer_id), authorize_ack(join_id),
authorize_reject(join_id),
confirm_peer(peer_id, member_id), revoke_peer(peer_id, member_id),
signal(to_peer_id, payload_base64), leave_room(). A socket belongs to at most one
room. Duplicate PeerIds are rejected across live/pending memberships. LeaveRoom
ends this control connection; create/join again requires a new connection.

Server: welcome(authority_id), room_created(room_id, room_code, self_member_id),
authorize_peer(join_id, peer_id, member_id),
room_joined(room_id, self_member_id, host_peer_id, host_member_id),
peer_joined(peer_id, member_id), peer_unavailable(peer_id),
signal(from_peer_id, payload_base64), room_closed(), error(code).

Signal payloads use **canonical padded RFC 4648 standard Base64**, decoded length
**1..=16,384 bytes**, exactly matching Puzzella MAX_SIGNAL_BYTES. Maximum Base64
length is 21,848 bytes; decoded validation also rejects a 16,385-byte value that
has the same encoded length. Server decodes only to validate size/encoding, relays
the original canonical string, and never parses or logs GNS payloads.

Error codes: protocol_violation, unsupported_version, invalid_message,
not_in_room, unknown_room, room_full, capacity, duplicate_peer, unknown_target,
invalid_signal, signal_too_large, rate_limited, join_timeout, backpressure.
Schema/framing violations close; operation rejection normally leaves the socket
open for bounded retries. AuthorizeReject sends capacity and closes only its
pending joiner. Pending/Routed expiry sends join_timeout and closes its
socket. Global admission rejection uses HTTP 429/503 before upgrade. A control
queue overflow, write failure, frame-rate exhaustion or heartbeat expiry closes
and cleans the connection. ConfirmPeer is a host-reported lifecycle milestone;
the server remains outside game authentication.

## Lifecycle, TLS and future extensions

PeerUnavailable means further routing to this peer is unavailable. RoomClosed
means this room no longer admits/routes members. Neither event terminates GNS game
connections. The adapter preserves active bindings and exposes control events;
explicit release_route/revoke_peer belongs to the connection owner. Unactivated
pending routes can be revoked safely on timeout/control loss. Host loss deletes
the room/code; member loss removes its binding. No resume or restart persistence
is defined. Future extensions may add authenticated accounts, resume tokens and
room recovery with fresh authorization; v1 clients must reject unknown versions.

Production uses server-authenticated WSS with certificate validation. A reverse
proxy may terminate TLS in front of the localhost WS server. Abuse source identity
defaults to the TCP peer IP. Only TCP peers in explicitly
configured trusted-proxy CIDRs may supply one bounded `X-Forwarded-For` header.
The chain is walked right-to-left from the trusted TCP peer and stops at the
nearest untrusted hop; malformed, duplicate, missing or oversized trusted-proxy
headers fail closed. IPv4-mapped IPv6 addresses are normalized. Untrusted peers
cannot change source identity with headers; `Forwarded` and `X-Real-IP` are
always ignored. See README for finite resource/rate/deadline limits. STUN/TURN is separate from this protocol.

## Canonical golden JSON

These values are fixed test vectors, not real identities. `C` and `S` distinguish
direction in `tests/fixtures/protocol_v1.jsonl`; the prefix is not on the wire.
Both repositories parse and round-trip these exact examples. The mirrored Rust
schema and fixtures must be updated together. Field order has no semantic meaning.

```json
{"v":1,"type":"welcome","authority_id":"11111111-1111-4111-8111-111111111111"}
```

```json
{"v":1,"type":"create_room","peer_id":"22222222-2222-4222-8222-222222222222"}
```

```json
{"v":1,"type":"room_created","room_id":"33333333-3333-4333-8333-333333333333","room_code":"ABCDEFGHJK","self_member_id":"44444444-4444-4444-8444-444444444444"}
```

```json
{"v":1,"type":"join_room","room_code":"ABCDEFGHJK","peer_id":"55555555-5555-4555-8555-555555555555"}
```

```json
{"v":1,"type":"authorize_peer","join_id":"66666666-6666-4666-8666-666666666666","peer_id":"55555555-5555-4555-8555-555555555555","member_id":"77777777-7777-4777-8777-777777777777"}
```

```json
{"v":1,"type":"authorize_ack","join_id":"66666666-6666-4666-8666-666666666666"}
```

```json
{"v":1,"type":"authorize_reject","join_id":"66666666-6666-4666-8666-666666666666"}
```

```json
{"v":1,"type":"room_joined","room_id":"33333333-3333-4333-8333-333333333333","self_member_id":"77777777-7777-4777-8777-777777777777","host_peer_id":"22222222-2222-4222-8222-222222222222","host_member_id":"44444444-4444-4444-8444-444444444444"}
```

```json
{"v":1,"type":"peer_joined","peer_id":"55555555-5555-4555-8555-555555555555","member_id":"77777777-7777-4777-8777-777777777777"}
```

```json
{"v":1,"type":"signal","to_peer_id":"22222222-2222-4222-8222-222222222222","payload_base64":"AP8H"}
```

```json
{"v":1,"type":"signal","from_peer_id":"55555555-5555-4555-8555-555555555555","payload_base64":"AP8H"}
```

```json
{"v":1,"type":"peer_unavailable","peer_id":"55555555-5555-4555-8555-555555555555"}
```

```json
{"v":1,"type":"leave_room"}
```

```json
{"v":1,"type":"room_closed"}
```

```json
{"v":1,"type":"error","code":"join_timeout"}
```
