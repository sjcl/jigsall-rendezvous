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
through one trusted loopback proxy address. Golden wire schemas remain unchanged.

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
