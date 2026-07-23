FROM rust:1.96.0-bookworm@sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc AS builder
WORKDIR /workspace
COPY . .
RUN cargo build --locked --release -p identity-policy-worker --bin identity-policy-worker

FROM debian:bookworm-slim@sha256:60eac759739651111db372c07be67863818726f754804b8707c90979bda511df AS runtime
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /workspace/target/release/identity-policy-worker /usr/local/bin/identity-policy-worker
USER 10001:10001
HEALTHCHECK --interval=10s --timeout=5s --retries=5 --start-period=5s \
    CMD ["/usr/local/bin/identity-policy-worker", "--healthcheck"]
ENTRYPOINT ["/usr/local/bin/identity-policy-worker"]
