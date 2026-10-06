# Deployment and configuration

## Direct Internet edge with Caddy

Use [deploy/Caddyfile](../deploy/Caddyfile) with stock Caddy 2. The backend and
Caddy run on the same host; the backend listens only on `127.0.0.1:8080`. Point
your domain's DNS at the host and permit HTTPS/certificate issuance traffic to
Caddy. Set the backend environment before starting it:

```sh
export PUZZELLA_RENDEZVOUS_LISTEN=127.0.0.1:8080
export PUZZELLA_RENDEZVOUS_TRUSTED_PROXIES=127.0.0.1/32
cargo run --locked --release
```

In a separate shell/service:

```sh
export RENDEZVOUS_DOMAIN=rendezvous.your-domain.example
caddy validate --config deploy/Caddyfile --adapter caddyfile
caddy run --config deploy/Caddyfile --adapter caddyfile
```

PowerShell uses `$env:NAME = 'value'` instead of `export`. A service manager may
load [deploy/rendezvous.env.example](../deploy/rendezvous.env.example) as an
environment file; the binary itself does not read `.env` files. The application
URL is `wss://<domain>/v1/ws`; `https://<domain>/healthz` checks process health.

Caddy obtains/renews certificates and proxies WebSocket upgrades automatically.
The sample only forwards GET `/v1/ws` and `/healthz`, overwrites X-Forwarded-For
with its TCP client's address, strips competing identity headers and sets finite
HTTP header/body/idle and upstream dial/response-header deadlines. It does not
buffer application streams or impose a short WebSocket lifetime. Access/frame
debug logging is not enabled. See Caddy's official
[reverse proxy documentation](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy)
and [server options](https://caddyserver.com/docs/caddyfile/options).

This sample assumes clients connect **directly to Caddy**. Adding a CDN or
another proxy requires an explicit trust policy at every hop; blindly preserving
incoming forwarding headers would allow IP-limit spoofing. A Caddy configuration
reload can close WebSockets/rooms, so plan reloads as control-plane interruptions;
existing GNS gameplay channels have independent lifetime.

Keep the backend inaccessible from the Internet. A trusted CIDR authorizes its
machines/processes to assert source identity; local processes are trusted when
loopback is trusted. For separate hosts/containers, use a private listener and
the exact proxy IPs/subnets instead of trusting all private networks. The stock
Caddy sample bounds HTTP parsing time and header size; it does not add a
connection/rate-limit plugin. Apply edge/firewall connection and request limits
for HTTP sockets that have not upgraded. Application caps cover admitted WS
reservations/connections and their state, not every pre-upgrade TCP socket.

## Docker behind Caddy

Build from the repository root:

```sh
docker build --pull -t puzzella-rendezvous:local .
```

The image contains the release binary and Debian runtime libraries, runs as
UID/GID 10001, and supports a read-only filesystem. It serves plain HTTP/WS on
`0.0.0.0:8080` by default. Caddy handles TLS separately; no Caddy binary or
certificates are included. A persistent volume is needed only when the optional
TURN usage budget is enabled. Restarting the container clears all rooms, as with
a native process. Runtime settings below
remain available through `--env` or `--env-file`.

### Caddy on the Linux host

Use host networking and override the listener to loopback. This preserves the
existing [deploy/Caddyfile](../deploy/Caddyfile) upstream and lets the backend
trust exactly the local Caddy connection:

```sh
docker run -d --name puzzella-rendezvous --restart unless-stopped \
  --network host --read-only --cap-drop ALL \
  --security-opt no-new-privileges=true --stop-timeout 10 \
  --env PUZZELLA_RENDEZVOUS_LISTEN=127.0.0.1:8080 \
  --env PUZZELLA_RENDEZVOUS_TRUSTED_PROXIES=127.0.0.1/32 \
  puzzella-rendezvous:local
curl --fail http://127.0.0.1:8080/healthz
```

This example requires Docker Engine on Linux. Do not use `--publish` with host
networking. Keep the loopback listener override when using this mode.

### Caddy in an existing container

Connect the existing Caddy container to a dedicated backend network, assigning
it a stable IP. The example assumes its container name is `caddy`; choose an
unused subnet for your deployment:

```sh
docker network create --subnet 172.30.0.0/24 rendezvous-backend
docker network connect --ip 172.30.0.2 rendezvous-backend caddy
docker run -d --name puzzella-rendezvous --restart unless-stopped \
  --network rendezvous-backend --read-only --cap-drop ALL \
  --security-opt no-new-privileges=true --stop-timeout 10 \
  --env PUZZELLA_RENDEZVOUS_TRUSTED_PROXIES=172.30.0.2/32 \
  puzzella-rendezvous:local
```

In the existing Caddy configuration, use `reverse_proxy puzzella-rendezvous:8080`
instead of `reverse_proxy 127.0.0.1:8080`, retaining the sample's route/header
policy and transport deadlines. Preserve the backend-network attachment and
static Caddy IP in your container manager when recreating it. Publish only
Caddy's public ports; the backend needs no published port. Trust Caddy's exact
IP, rather than the entire Docker/private address range. The native
`deploy/rendezvous.env.example` sets a loopback listener/proxy and therefore
needs those values changed before use with this bridge-network example.

Use `docker logs puzzella-rendezvous` for server logs.
`docker stop --time 10 puzzella-rendezvous` delivers SIGTERM directly to the
server's PID 1 and allows its bounded shutdown drain to finish.

## Source-IP trust policy

`PUZZELLA_RENDEZVOUS_TRUSTED_PROXIES` defaults to empty (trust no proxy). Configure
at most 32 comma-separated canonical IPv4/IPv6 CIDRs with explicit prefixes.
`0.0.0.0/0`, `::/0`, noncanonical networks and IPv4-mapped IPv6 CIDRs are rejected.
Use IPv4 CIDRs for IPv4 clients; mapped IPv4 socket/header addresses are normalized
so IPv4 and mapped-IPv4 forms share one admission identity.

- An untrusted TCP peer's headers are ignored, even if malformed.
- A trusted TCP peer must send exactly one X-Forwarded-For header, at most 1,024
  bytes and 16 comma-separated IP literals. Missing, duplicate, malformed,
  port-bearing, quoted or oversized values return HTTP 400 before admission.
- The server walks the chain from right to left, starting at the trusted TCP
  peer, and stops at the nearest untrusted hop. A caller's spoofed left prefix
  cannot replace that source. If every hop is trusted, the leftmost IP is used
  (this permits a real loopback client behind local Caddy).
- `Forwarded`, `X-Real-IP` and other headers never influence admission.

The resolved source is used consistently for concurrent connections, new
connection attempts, create/join attempts and bounded IP history. With the
sample's trust configuration, 64 connections **per client IP** and 1,024 global
connections remain the defaults. A room of host + 64 remote participants can
therefore fit through one proxy. Clients sharing one NAT still share its IP
limits; raise the per-IP settings deliberately if that deployment needs it.

If source forwarding is unavailable, leave trusted proxies empty and configure
the proxy-IP aggregate limits explicitly. For example, set MAX_CONNECTIONS_PER_IP
to 1,024, MAX_ADMISSIONS_PER_IP_PER_MINUTE to 2,048 and
MAX_ROOM_ATTEMPTS_PER_IP_PER_MINUTE to 4,096, **and enforce equivalent client-IP
guards at the edge**. Raising only the connection cap leaves the default
60-admissions/minute and 120-room-attempts/minute aggregate bottlenecks intact.

## Validated runtime limits

All names below have the prefix `PUZZELLA_RENDEZVOUS_`. Values are positive integer
counts or bytes, not `KiB`/`MiB` strings. Invalid, zero, overflowing and above-ceiling
values fail startup **before binding the listener**. The lower of local and global
caps always applies; IP caps may exceed a smaller global connection cap.

| Suffix | Default | Hard ceiling |
| --- | ---: | ---: |
| MAX_CONNECTIONS | 1,024 | 1,024 |
| MAX_CONNECTIONS_PER_IP | 64 | 1,024 |
| MAX_ADMISSIONS_PER_IP_PER_MINUTE | 60 | 65,536 |
| MAX_ROOM_ATTEMPTS_PER_IP_PER_MINUTE | 120 | 65,536 |
| MAX_ROOMS | 256 | 256 |
| MAX_PARTICIPANTS (remote, including pending) | 64 | 64 |
| MAX_PENDING_JOINS | 512 | 512 |
| MAX_OUTBOUND_MESSAGES (per socket) | 128 | 128 |
| MAX_OUTBOUND_BYTES_PER_CONNECTION | 262,144 (256 KiB) | 1,048,576 (1 MiB) |
| MAX_OUTBOUND_BYTES_GLOBAL | 33,554,432 (32 MiB) | 67,108,864 (64 MiB) |

Frame/signal limits and other fixed rate/deadline/history guards remain in
[README.md](../README.md). Library users can construct `Limits` and opt in with
`Server::with_trusted_proxies`; `Server::new` trusts no proxy. Environment loading
is provided by `Config::from_env` and used by the binary.

## Outbound memory accounting

The count cap and both byte caps must all permit an enqueue. Every serialized
application frame (signals **and** controls) holds per-socket and global byte
reservations equal to its retained UTF-8 allocation, with serializer spare
capacity removed. The writer retains these reservations after dequeue until
the write completes or is cancelled. Failed reservation/enqueue, receiver drop,
disconnect and shutdown release ownership automatically; no manual counters
can leak across membership churn. A disconnected writer may retain its bytes
until its bounded write/drain finishes, preventing churn from evading the global
cap.

Signal byte/count congestion sends Backpressure to its source and preserves the
target connection. The caller can retry or shed signaling according to GNS's own
limits. If a control/error frame cannot fit, its target control connection closes,
as for a full message queue. Byte saturation is not an authorization failure.
The global default limits retained outbound frame data to 32 MiB instead of
allowing every 128-message queue to accumulate maximum-sized signals. Channel
slots, Rust objects, current incoming/serialized messages, WebSocket/TCP buffers,
room state and the proxy's memory are additional; this is not a total-RSS limit.

## Verification

Run the repository's fmt, all-target Clippy, tests and build commands in README.
Tests exercise the fixed commit/enqueue ordering with competing threads, immediate
signals in both directions on a multithread Tokio runtime, failed host activation
enqueue, local/global byte exhaustion, in-flight reservations and all drop paths,
spoofed/untrusted forwarding headers, malformed chains and a full 65-socket room
through one trusted loopback proxy address. Shared v1 golden fixtures validate the canonical room messages and optional TURN extension.

The optional real-Caddy test is
`caddy_edge_replaces_spoofed_ip_and_relays_ordered_activation`. Start the backend
with trusted loopback and `PUZZELLA_RENDEZVOUS_MAX_CONNECTIONS_PER_IP=2`. For this
local test only, copy the sample to an ignored file under `target/`, replace its
site address with `http://127.0.0.1:18081`, and add `bind 127.0.0.1` in the site.
Disable the admin endpoint/config persistence in that copy and place test
`storage file_system` under `target/` too. Keep its proxy/header/routing options
unchanged. Then run:

```sh
PUZZELLA_CADDY_SMOKE_URL=ws://127.0.0.1:18081/v1/ws \
  cargo test --locked caddy_edge_replaces_spoofed_ip_and_relays_ordered_activation -- --ignored --nocapture
```

This test uses deliberately different spoofed headers on three requests and
requires the third to hit the same real source-IP cap. It also checks health,
unknown-route rejection, room activation and immediate signaling through Caddy.
Stop both local test processes afterward; use the original HTTPS sample for
production and restore production limit settings.

Local verification on 2026-10-05 (Windows x86_64, Rust 1.97.0, Caddy 2.11.6):
server fmt/all-target Clippy/build and all 30 automated tests passed, as did
sample adaptation/provision validation and the separate real-Caddy loopback
test. This verifies the configuration and proxy behavior; it does not claim a
public certificate deployment or Internet NAT traversal result.

## TURN usage budget

The budget is disabled by default. With `PUZZELLA_RENDEZVOUS_TURN_BUDGET_ENABLED=true`,
configure all required settings below in addition to the existing TURN key/token.
The Analytics token is independent of the credential-generation/revoke token.
Give it **Account / Account Analytics / Read**, scoped to the configured account,
as required by [Cloudflare TURN Analytics](https://developers.cloudflare.com/realtime/turn/analytics/).
Keep both tokens on the server. Credential revocation uses the TURN key API token
and the documented [revoke endpoint](https://developers.cloudflare.com/realtime/turn/generate-credentials/#revoke-credentials).

All suffixes below have the prefix `PUZZELLA_RENDEZVOUS_`:

| Suffix | Default / requirement |
| --- | --- |
| `TURN_BUDGET_ENABLED` | `false`; accepts `true`/`false` or `1`/`0` |
| `TURN_BUDGET_ACCOUNT_ID` | Required: 32 hexadecimal characters |
| `TURN_BUDGET_ANALYTICS_TOKEN` | Required: account-scoped Analytics read token |
| `TURN_BUDGET_SOFT_LIMIT_BYTES` | Required: positive integer bytes |
| `TURN_BUDGET_HARD_LIMIT_BYTES` | Required: positive integer, strictly greater than soft limit |
| `TURN_BUDGET_POLL_SECONDS` | 30; range 1–3600 |
| `TURN_BUDGET_STALE_SECONDS` | 300; range 1–86400, greater than polling interval |
| `TURN_BUDGET_BILLING_ANCHOR` | Required: RFC3339 timestamp with whole seconds |
| `TURN_BUDGET_PERIOD_SECONDS` | Unset: calendar months; optional fixed cycle length 60–31622400 seconds |
| `TURN_BUDGET_REGISTRY_PATH` | Required: writable durable file, e.g. `/var/lib/puzzella-turn/registry.jsonl` |

Missing/invalid settings reject startup before the listener binds. Budget settings
without an enable flag also reject startup; explicit `false` ignores retained
budget settings and restores the existing TURN behavior without storage or
Analytics access. Disabling the guard deliberately removes budget protection.

Normal issues and rotates credentials as before. At `usage >= soft limit`,
SoftLimited stops new credentials and rotation, retaining previously distributed
valid credentials and allocations. At `usage >= hard limit`, HardLimited closes
issuance first, then revokes every tracked unexpired username. Existing sessions
whose Welcome carried TURN receive `turn_unavailable`; rooms and signaling stay
open. Direct ICE/STUN configuration is unchanged. Revoking a relay credential can
interrupt a route using that relay; this server never explicitly disconnects a
gameplay connection. Revoke failures retain registry entries and retry; they do
not prevent other credentials from being revoked.

The query filters `callsTurnUsageAdaptiveGroups` by the configured key and the
half-open UTC range `[period start, observation time)`, requesting only
`sum { egressBytes }` and no dimensions. Cloudflare aggregates the entire range
into one group (`limit: 1`); the server does not download/sum a timeseries.
Usage is `max(previous, latest)` within a period. Soft/Hard states and the observed
high-water usage persist across restart and never decrease within that period.
Analytics can lag and use adaptive sampling; polling and revocation are not an
exact spending cap. Leave headroom below your actual cost ceiling. See
[Cloudflare's sampling guidance](https://developers.cloudflare.com/realtime/turn/replacing-existing/#how-to-bill-end-users-for-their-turn-usage).

Before admission starts, the server performs one Analytics query (five-second
deadline). A failed initial query leaves issuance stopped. Later transient query
failures preserve the last valid observation; after the stale timeout, issuance
and rotation stop without revoking previously distributed credentials. Staleness
is separate from the latched Soft/Hard state. A successful observation clears
staleness, allowing issuance only if the period is still Normal. HTTP/query errors
and malformed/partial responses do not become zero usage or force HardLimited.

Set the anchor to your account's actual billing date/time. For example,
`2026-01-17T12:00:00Z` uses the 17th at 12:00 UTC each calendar month, rather than
the first. Offsets are normalized to UTC. An anchor on the 31st clamps to February's
last day, then uses the 31st again in March. Set `TURN_BUDGET_PERIOD_SECONDS=2592000`
only if your billing cycle actually uses fixed 30-day intervals from the anchor.
Confirm the schedule against your account's invoices;
[Cloudflare billing cycles](https://developers.cloudflare.com/billing/understand/how-billing-works/#billing-cycles)
use UTC. Boundary calculation is isolated and tested. When the next period starts,
usage/state/staleness reset; issuance remains stopped until a successful query
for that new period. Responses for the previous period are discarded. Clock
rollback does not reset a previously recorded newer period.

The registry stores deduplicated usernames, conservative expiry timestamps and
pending revoke flags, never passwords. It also stores budget/period metadata.
Writes are appended and synced before credentials become publishable. Every
512 writes a synced temporary checkpoint replaces the journal atomically in the
same directory. Startup restores it, removes expired entries and truncates only
an unfinished final journal record; complete malformed records reject startup.
A sidecar `.lock` file holds an OS file lock across replacements: use one server
process per registry. Account/key/anchor/cycle changes reject an existing registry;
drain/revoke old credentials before switching configuration or storage. Keep the
registry directory private and on a local filesystem with working file locking,
atomic replacement and flush semantics. Do not delete it between restarts.

The journal has a 256 MiB startup/append bound and a 250,000-entry bound with
headroom for admitted in-flight requests. Capacity/storage errors stop issuance
and leave Direct ICE/signaling available; successful storage recovery resumes
issuance only when Analytics and budget state permit it. Expired entries are
removed at startup and during maintenance. Revoke work uses one worker, batches
of at most 32, up to four parallel HTTP calls, three-second deadlines and bounded
exponential retry delays (2–300 seconds). Old-period revoke obligations survive
a billing-period reset. No credential username, password or token is logged.

For Docker, mount a writable persistent directory at the registry path and make
it owned by UID/GID 10001; the rest of the filesystem may remain read-only. Add
`--mount type=bind,src=/srv/puzzella-turn,dst=/var/lib/puzzella-turn` and set the
registry path to `/var/lib/puzzella-turn/registry.jsonl` in the environment file.
Back up this directory along with its configuration. A process crash/network
timeout after Cloudflare minted a credential but before its response was received
cannot reveal that unknown username locally; no such credential is sent to the
client, and it expires under the configured TTL.

Normal tests use mock Analytics/providers and loopback HTTP/WSS. They cover limit
latching, stale recovery, anchored boundaries, restart/cleanup/deduplication,
issuance and publication races, bounded revoke retry/concurrency and continued
room/signaling operation after hard unavailability. Live Analytics/revoke and
real relay disruption require separate account/network validation.

Local budget verification on 2026-10-06 (Windows): fmt, all-target Clippy with
warnings denied, locked build and 76 automated tests passed. The existing live
Cloudflare issuance and real-Caddy tests remained ignored. No production Analytics
or revoke API was called by this verification.

## Optional Cloudflare Realtime TURN

Create a TURN key and API token using Cloudflare's
[credential guide](https://developers.cloudflare.com/realtime/turn/generate-credentials/).
Keep both in the Rendezvous service's environment/secret store:

```sh
export PUZZELLA_RENDEZVOUS_TURN_KEY_ID=your-turn-key-id
export PUZZELLA_RENDEZVOUS_TURN_API_TOKEN=your-server-only-api-token
export PUZZELLA_RENDEZVOUS_TURN_TTL_SECONDS=86400
```

Never put these secrets in Puzzella's environment, `internet-defaults.env`,
source, release binaries or WSS protocol. WSS distributes only short-lived
username/password pairs and UDP server addresses. SPAKE2 remains the gameplay
password authentication; TURN issuance confers no player identity or membership.
The service must reach `rtc.live.cloudflare.com` over HTTPS. Docker includes CA
certificates for that outbound connection; Caddy still terminates inbound WSS.

| Setting | Default | Accepted range |
| --- | --- | --- |
| `PUZZELLA_RENDEZVOUS_TURN_TTL_SECONDS` | 86400 (24 hours) | 600–172800 seconds |
| `PUZZELLA_RENDEZVOUS_TURN_MAX_CONCURRENCY` | 4 | 1–32 |
| `PUZZELLA_RENDEZVOUS_TURN_REQUESTS_PER_MINUTE` | 120 | 1–4096 |

Set both key variables or neither. Invalid/partial settings fail before binding,
with secret-free diagnostics. TURN disabled or provider/network/rate-limit failure
still produces Welcome and permits STUN/direct ICE. One three-second deadline
covers both waiting for the shared concurrency permit and the HTTP call; a busy
permit waits within this budget instead of immediately disabling TURN. A global
issuance-rate rejection or deadline/provider failure still permits direct ICE.
Welcome without TURN fixes that entire WSS session (including future peers) to
direct-only: no background retries or later TURN push start. A newly opened control
session can receive initial credentials after the provider recovers. Only admitted
WebSockets can cause issuance; there is no client refresh command or publicly exposed credential HTTP endpoint. Existing
IP/admission/resource limits apply in addition to the shared issuance quota and
concurrency bound. Each socket has a random 128-bit `customIdentifier`, unrelated
to IP, room, password or player identity, retained across its credential updates.

Only sessions whose Welcome contains usable TURN start rotation. The endpoint
address set must remain equal to Welcome's set (ordering may change). An endpoint
addition/removal/change is an invalid rotation response; it is never pushed or
partially applied, and the current credentials are retained during retry.
Pushed credentials update host listener / future outgoing connection defaults
only. Existing ICE connections keep the credentials with which they initialized:
Refresh, CreatePermission and reallocation continue with A after defaults become B.
This follows [RFC 8656 sections 5/6](https://www.rfc-editor.org/rfc/rfc8656.html#section-5);
a different valid username on an existing allocation receives 441 Wrong Credentials.
No native API replaces active allocation credentials.

Default refresh starts at half the TTL (about 12 hours for the default). Failures retain
the old credential and retry with 1–60 second bounded exponential backoff, capped
by the remaining validity. Retries remain bounded after expiry so recovery can
serve future peers on an initially TURN-enabled control session too. A new
credential waits in a single retained slot if the outbound byte/message budget is
full; it does not create an unbounded queue or
close the room. Expiry can produce `turn_unavailable`; the client checks the latest
default expiry and disables TURN in listener/future outgoing defaults so a new
peer does not attempt an allocation with expired credentials. Existing routes
retain their original configuration. The initial endpoint set stays fixed, and
recovery re-enables future TURN when a fresh credential with that set arrives.
Budget-aware clients must honor `turn_unavailable` even before the latest default
expires: a hard budget can revoke it early. Older Puzzella clients that ignore
early unavailability retain unusable cached TURN defaults until expiry; update
those clients before enabling the hard budget. The event never requests a
gameplay disconnect or removal of Direct ICE/STUN configuration.

Only `turn:host:port?transport=udp` entries are distributed (normally UDP ports 3478
and 443). TCP/TLS entries are filtered out and credentials stay paired with their
own server entries. Native ICE retains its host/reflexive preference over relay.
TURN allocations may be gathered alongside direct candidates, but gameplay uses
relay only when the selected route requires it.

WSS control loss cancels rotation for that socket. When a budget is enabled,
already-started issuance finishes registry bookkeeping in a task holding the
original concurrency permit, so cancellation cannot lose known credentials.
Existing GNS,
SecureTransport, SPAKE2, Sync/Ready and gameplay state continue independently.
An existing TURN-only route can fail when its original credential expires, even
if newer defaults keep arriving over WSS. Choose a TTL for the expected peer
connection duration (default 24 hours, configurable up to 48 hours); a connection
inherits the credential's remaining validity from issuance, not a fresh TTL.
ICE restart /
allocation migration and Rendezvous reconnect/resume are outside this implementation.
Cloudflare's [TURN FAQ](https://developers.cloudflare.com/realtime/turn/faq/)
documents the maximum TTL, expiry behavior and ICE restart recommendation.

Normal CI uses mock HTTP/providers and a local TURN fixture, never production
Cloudflare. To check real issuance manually, configure the secrets explicitly and
run `cargo test --locked cloudflare_manual_credential_issue -- --ignored`.
That test verifies credential issuance only; it does not establish or bill relay
traffic. Real Cloudflare allocation and credential expiry require separate network validation.


TURN verification on 2026-10-05 (Windows x86_64): 42 unit and 14 WebSocket tests
passed; the production Cloudflare and real-Caddy checks remain opt-in. Fmt,
all-target Clippy and the server/example builds passed. Mock HTTP verifies the
API request and UDP credential pairing; WebSocket tests cover initial ordering,
transient update failure, successful rotation, provider recovery, fixed initial
availability, endpoint-set rejection, queued concurrent welcomes, and expired
defaults becoming unavailable followed by provider recovery without room closure. The
server's `turn_fixture_server` example also passed six local cross-repository
smoke runs using Puzzella's `run_turn_smoke.py`, including fixed allocation
credentials during pushed-default updates, forced-relay Sync/Ready
and encrypted traffic after control shutdown, plus initially unavailable sessions
remaining direct-only after mock API recovery. This uses loopback WS and a local
UDP relay, with no production API requests. Fixture keys are static and provider
metadata TTLs are accelerated; this does not validate actual credential expiry,
48-hour sessions or ICE restart recovery.
