FROM rust:1.99.1-bookworm@sha256:665b6e0a9290a7298236c08c6e4f1e754e3f0d9b70421c434daa3dfba888d63e AS builder
WORKDIR /workspace
COPY . .
RUN cargo build --locked --release -p identity-api --bin identity-api \
    -p identity-database-migrator --bin identity-database-migrator \
    -p identity-service-provisioner --bin identity-service-provisioner

FROM debian:bookworm-slim@sha256:60eac759739651111db372c07be67863818726f754804b8707c90979bda511df AS runtime
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /workspace/target/release/identity-api /usr/local/bin/identity-api
COPY --from=builder /workspace/target/release/identity-database-migrator /usr/local/bin/identity-database-migrator
COPY --from=builder /workspace/target/release/identity-service-provisioner /usr/local/bin/identity-service-provisioner
USER 10001:10001
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=5s --retries=5 \
    CMD ["/usr/local/bin/identity-api", "--healthcheck"]
ENTRYPOINT ["/usr/local/bin/identity-api"]
