FROM rust:1.95.0-slim-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release --bin puzzella-rendezvous

FROM debian:bookworm-slim

COPY --from=builder /app/target/release/puzzella-rendezvous /usr/local/bin/puzzella-rendezvous

# TLS terminates at the external reverse proxy. Trust its IP explicitly at runtime.
ENV PUZZELLA_RENDEZVOUS_LISTEN=0.0.0.0:8080
USER 10001:10001
EXPOSE 8080/tcp
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/puzzella-rendezvous"]
