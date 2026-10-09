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
# GET /catalog/v1/pipeline-graph reads the registry at run time. Its default path is resolved against
# the build tree, which the runtime image does not have, so the endpoint answered 500 in production
# until the registry was carried here and named by its environment variable (2026-09-30).
COPY docs/catalog/pipeline-graph.v1.json /usr/local/share/foundation-platform/pipeline-graph.v1.json
ENV FOUNDATION_PLATFORM_PIPELINE_GRAPH_REGISTRY_PATH=/usr/local/share/foundation-platform/pipeline-graph.v1.json
# GET /data-catalog/v1/scheduled-jobs lists the jobs of this job list (root ADR-0165), carried the
# same way and for the same reason.
COPY orchestration/jobs.v1.json /usr/local/share/foundation-platform/jobs.v1.json
ENV FOUNDATION_PLATFORM_SCHEDULED_JOBS_PATH=/usr/local/share/foundation-platform/jobs.v1.json
USER 10001:10001
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=5s --retries=5 \
    CMD ["/usr/local/bin/foundation-api", "--healthcheck"]
ENTRYPOINT ["/usr/local/bin/foundation-api"]
