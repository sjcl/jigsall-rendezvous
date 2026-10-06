FROM rust:1.95.0-slim-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release --bin jigsall-rendezvous

FROM debian:bookworm-slim

# Outbound Cloudflare HTTPS requires a root certificate store.
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/jigsall-rendezvous /usr/local/bin/jigsall-rendezvous

# TLS terminates at the external reverse proxy. Trust its IP explicitly at runtime.
ENV JIGSALL_RENDEZVOUS_LISTEN=0.0.0.0:8080
USER 10001:10001
EXPOSE 8080/tcp
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/jigsall-rendezvous"]
