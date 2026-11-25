FROM rust:1.99.1-bookworm@sha256:665b6e0a9290a7298236c08c6e4f1e754e3f0d9b70421c434daa3dfba888d63e AS builder
WORKDIR /workspace
COPY . .
RUN cargo build --locked --release -p identity-policy-worker --bin identity-policy-worker

FROM debian:bookworm-slim@sha256:e67eeead736ff7bbe258ef088bff683635c561b1fcfefad0806c1df4ab49ce0e AS runtime
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /workspace/target/release/identity-policy-worker /usr/local/bin/identity-policy-worker
USER 10001:10001
HEALTHCHECK --interval=10s --timeout=5s --retries=5 --start-period=5s \
    CMD ["/usr/local/bin/identity-policy-worker", "--healthcheck"]
ENTRYPOINT ["/usr/local/bin/identity-policy-worker"]
