# The Dawneer console as the data host runs it (root ADR-0434 §1). CI builds it into the signed
# release with the other compose images (root ADR-0276); the host never builds it.
FROM node:24.20.0-bookworm@sha256:be23f54a88d34e8824c741b19b91064094f92c1c97b194144bfc8b50d67258e2 AS web
WORKDIR /web
# pnpm's version is package.json `packageManager`; corepack reads it.
COPY web/package.json web/pnpm-lock.yaml /web/
RUN corepack enable && pnpm install --frozen-lockfile
COPY web /web
RUN pnpm build

FROM rust:1.99.1-bookworm@sha256:665b6e0a9290a7298236c08c6e4f1e754e3f0d9b70421c434daa3dfba888d63e AS builder
WORKDIR /workspace
COPY Cargo.toml Cargo.lock /workspace/
COPY server /workspace/server
RUN cargo build --locked --release -p dawneer-server

FROM debian:bookworm-slim@sha256:60eac759739651111db372c07be67863818726f754804b8707c90979bda511df AS runtime
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /workspace/target/release/dawneer-server /usr/local/bin/dawneer-server
COPY --from=web /web/dist /usr/local/share/dawneer/web
ENV DAWNEER_WEB_DIR=/usr/local/share/dawneer/web
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/dawneer-server"]
