# puzzella-rendezvous

Async Rust (axum + Tokio) rendezvous/signaling v1 for Puzzella's custom GNS
signaling. Hosts create a room; participants join with a ten-character room code.
Only host–participant signaling is relayed. GNS native ICE establishes game P2P
connections separately. Optional Cloudflare Realtime TURN supplies UDP relay
fallback and refreshes credentials for future peer connections through WSS.
Existing TURN allocations keep their initial credentials.
Direct ICE remains available when TURN is disabled or unavailable. See
[TURN configuration and lifetime](docs/DEPLOYMENT.md#optional-cloudflare-realtime-turn).
An optional [TURN usage budget](docs/DEPLOYMENT.md#turn-usage-budget) monitors
Cloudflare egress for a configured billing cycle. Soft limits stop issuance and
rotation; hard limits also revoke tracked credentials and notify TURN-enabled
control sessions. Budget monitoring requires a small persistent registry volume.

This server carries **no gameplay traffic** and accepts **no puzzle password,
password hash, PAKE scalar, image hash, snapshot, or gameplay authentication**.
A room code is a routing locator, not an authentication secret. Existing Puzzella
SPAKE2 + SecureTransport remains responsible for password authentication and
subsequent player assignment, Sync and Ready.

## Run

Rust 1.95 or newer:

```sh
cargo run --locked --release
```

The default listener is `127.0.0.1:8080`. Set `PUZZELLA_RENDEZVOUS_LISTEN` to a
numeric socket address (`127.0.0.1:9000`, `[::1]:9000`, etc.).
`GET /healthz` returns HTTP 200 without external dependencies.
`GET /v1/ws` upgrades to the JSON WebSocket protocol described in
[PROTOCOL_V1.md](docs/PROTOCOL_V1.md).

## Docker

```sh
docker build --pull -t puzzella-rendezvous:local .
```

The multi-stage [Dockerfile](Dockerfile) builds with Rust 1.95 and runs only the
release server binary on Debian slim as UID/GID 10001. It listens on
`0.0.0.0:8080` inside the container; configure the existing environment variables
with `docker run --env` or `--env-file`. Proxy trust defaults to empty.
Caddy remains a separate service. See [Docker deployment](docs/DEPLOYMENT.md#docker-behind-caddy)
for private networking and trusted-proxy settings.

## Internet deployment and trust

```text
Client -- WSS --> reverse proxy -- localhost WS --> puzzella-rendezvous
```

Terminate TLS with a correctly configured reverse proxy and a valid certificate;
keep the backend listener private. The Puzzella production adapter accepts only
`wss://`, verifies certificates with WebPKI roots, and has no verification bypass.
Its explicit local test constructor accepts `ws://` only for literal loopback IPs.
Route bindings are trusted only because they arrive on this authenticated server
channel. They certify anonymous membership, never the game password.

By default the server uses the **TCP peer IP** and ignores forwarding headers.
For a reverse proxy, set `PUZZELLA_RENDEZVOUS_TRUSTED_PROXIES` to a comma-separated
list of explicit proxy CIDRs, e.g. `127.0.0.1/32,::1/128`. Only connections from
those addresses use `X-Forwarded-For` for all IP connection/admission/room-attempt
guards. Read [deployment and configuration](docs/DEPLOYMENT.md) for validation,
proxy chains and the trust boundary. `Forwarded` and `X-Real-IP` are never used.

[deploy/Caddyfile](deploy/Caddyfile) and
[deploy/rendezvous.env.example](deploy/rendezvous.env.example) provide a direct
Internet-edge WSS configuration with a private localhost backend. In this setup
the 64-per-IP guard applies to **each client IP**, so the proxy no longer imposes
an aggregate 64-client ceiling. HTTP sockets before an accepted upgrade remain
outside the application's WebSocket cap; the deployment guide covers edge limits.

## Room lifecycle

AuthorityId changes at process startup. RoomId, MemberId and JoinId are generated
with OS-backed CSPRNG UUID v4. Room codes use 50 random bits (ten Crockford Base32
characters), normalize ASCII case, exclude I/L/O/U and retry collisions at most
32 times. There is no room listing API.

Joining reserves one slot and has a fixed 12-second deadline. The host receives
AuthorizePeer, installs RouteOrigin/authorize_peer locally, then sends
AuthorizeAck. This ACK permits routing with a fixed 30-second game-auth deadline.
If retained native routes or local capacity prevent admission, the host sends
AuthorizeReject. The server frees the pending slot immediately, returns Capacity
to the joiner and leaves the host room open.
The server enqueues PeerJoined
to the host **before** RoomJoined to the joiner, within the same ordered
transition. Both activation notifications precede subsequent signals to their
socket. If the host notification cannot be queued, the joiner is not released. A
pending participant cannot signal. Expiry/cancellation releases its identity and
slot and informs the host with PeerUnavailable.

After SPAKE2 reaches Authenticated, the host sends ConfirmPeer with the current
PeerId and server-issued MemberId to establish the member and remove that deadline.
Ready/image/baseline/catch-up are not prerequisites. Host RevokePeer releases a
failed native/auth/bootstrap member immediately. Ping/Pong and signals do not
renew the deadline. The server receives no password or PAKE data. This unreleased
v1 extension requires updating the game and server together.

MemberId is a **server-issued anonymous room membership**, not a Steam account
or Sybil-proof user identity. New connections can obtain new memberships;
server-side connection, IP, attempt and message limits complement client route
limits. A future authenticated-account extension must explicitly change the
binding contract.

Member control disconnection removes its routing membership and sends
PeerUnavailable to the host. Host control disconnection deletes the room/code and
sends RoomClosed to active and pending members. These events indicate **control
plane unavailability**, not a request to close established GNS gameplay channels.
The connection manager decides any explicit security revocation or pending route
cleanup. Room state is memory-only; restart clears all rooms. v1 has no resume tokens,
host reconnect or persistence.

## Resource and rate limits

All application state and channels are bounded. The binary accepts validated
environment settings for capacities, IP guards and outbound byte budgets;
[DEPLOYMENT.md](docs/DEPLOYMENT.md) lists defaults and hard ceilings. Defaults:

| Resource | Limit |
| --- | --- |
| WebSocket reservations/connections | 1,024 global, 64 per resolved source IP |
| Rooms | 256 |
| Remote participants, including pending joins | 64 per room |
| Pending joins before host ACK | 512 global, within room participant limit |
| WebSocket text message and individual frame | 24 KiB each; binary application frames rejected |
| Decoded opaque signal | 1..=16 KiB; canonical padded standard Base64 |
| Per-connection application messages | 256 per fixed 1-second window |
| Signals | 32/second per participant, 128/second per host |
| All incoming frames (including Ping/Pong) | 512 per fixed 1-second window |
| Create/join attempts | 4/10 seconds per connection, 120/minute per IP |
| New connection attempts | 60/minute per IP |
| Outbound WebSocket queue | 128 messages and 256 KiB per connection; 32 MiB global |
| WebSocket read / write buffers | 24 KiB / 48 KiB maximum write buffer |
| IP history | 4,096 entries, retained 5 minutes after use; never evict active IPs |
| Pre-room idle / pending join | Fixed 30 / 12 seconds |
| Routed member awaiting host game-auth confirmation | Fixed 30 seconds after ACK |
| Ping interval / matching-Pong deadline | 15 / 15 seconds |
| Network write / shutdown drain budget | 2 seconds per write / 2 seconds total drain per socket |
| Room code allocation | At most 32 collision retries |

An arbitrary incoming packet cannot renew a pending/admission/heartbeat deadline.
Only a matching Pong satisfies an outstanding heartbeat. Fixed rate windows can
permit a burst across a window boundary; queues and byte limits remain finite.
Signal congestion returns Backpressure to the source without kicking the host
for a participant flood. A full control queue or expired writer closes that
control connection. Byte reservations cover serialized UTF-8 for **all** queued
application frames and remain held during socket writes. Failed enqueue, write
completion/cancellation and queue destruction return both per-socket and global
bytes automatically. These caps bound the outbound allocations, not the entire
process: WebSocket/TCP buffers, incoming frames and bounded state are additional.
State changes and nonblocking enqueues are serialized, with no lock over network
I/O. Payloads are decoded only
for validation and are never interpreted as GNS/game messages.

## Logging and shutdown

`PUZZELLA_RENDEZVOUS_LOG` sets the application's logging level (default `info`).
Only this server's target is enabled; `RUST_LOG` does not enable dependency frame
logs. Logs contain startup/listener/shutdown events, never signal payloads,
secrets, room codes or member identities.

Ctrl+C (also SIGTERM on Unix) stops admission, clears room/binding/pending state,
queues RoomClosed once to each socket, and cancels socket tasks with a bounded
drain. Shutdown waits for upgrade-task leases after HTTP serving ends. RoomClosed
delivery is best effort when a peer is slow or already gone. Clients retain
established GNS routes independently.

This server is **not STUN/TURN**. STUN endpoints and public candidates are supplied
through Puzzella `IceConfig`; no third-party STUN endpoint is embedded. TURN relay
hosting, accounts, matchmaking, public browsing, room persistence and UI/runtime room
integration remain outside v1.

## Validate

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo build --locked
```

Tests use only local state and loopback sockets: host/ACK/join/opaque relay,
star topology, spoofing, unknown room/target, duplicate peers, capacity, malformed
and oversized signals, frame/version rejection, fixed deadlines, disconnect and
shutdown cleanup, bounded slow consumers, rate/IP history and code collisions.
Regression tests force the activation commit/enqueue gap with competing threads,
immediate signals on both sockets, failed host activation enqueue, local/global
byte saturation and reservation release. Real WebSocket tests check trusted and
untrusted headers and admit a full 64-remote room through one loopback proxy IP.
Both repositories parse and round-trip the same
[golden messages](tests/fixtures/protocol_v1.jsonl). Update schema, these fixtures
and the Puzzella copy together.

The Puzzella repository's `docs/RENDEZVOUS_V1.md` documents a two-process test
against this real binary, including native ICE, SPAKE2, encrypted lanes, and
continued gameplay transport after control WebSocket shutdown. No other checkout
or external network is required by this repository's CI.
