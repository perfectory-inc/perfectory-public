FROM rust:1.96.0-bookworm@sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc AS builder
WORKDIR /workspace
# Toolchain pin is the base image tag (rust:1.96.0, enforced by
# scripts/guard/toolchain-consistency.sh); the root rust-toolchain.toml is
# outside this build context, so cargo uses the image default = 1.96.0.
COPY Cargo.toml Cargo.lock /workspace/
COPY crates /workspace/crates
COPY services /workspace/services
COPY migrations /workspace/migrations
RUN cargo build --locked --release -p foundation-api \
    --bin foundation-api --bin foundation-migrate

FROM debian:bookworm-slim@sha256:60eac759739651111db372c07be67863818726f754804b8707c90979bda511df AS runtime
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /workspace/target/release/foundation-api /usr/local/bin/foundation-api
COPY --from=builder /workspace/target/release/foundation-migrate /usr/local/bin/foundation-migrate
USER 10001:10001
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=5s --retries=5 \
    CMD ["/usr/local/bin/foundation-api", "--healthcheck"]
ENTRYPOINT ["/usr/local/bin/foundation-api"]
