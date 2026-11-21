# The image `scripts/verify/cargo-verify.sh` verifies inside, and the one test image on the data host
# (root ADR-0601): the signed release carries it (compose.verify-image.yml, build-only) and the deploy
# tags it perfectory-verify:local there, so a crate test on the host runs in exactly this environment.
# On 2026-11-21 the host had a second, hand-written definition (perfectory-rust-test:20261121b, its
# own apt list and its own copy of the sccache pin); every value below comes from a file CI reads.
#
# It exists so verification can run with no network. Every area's apt dependencies are baked here
# instead of installed per run, which removes the only step that needed the internet: `xtask`'s
# `ensure_apt` finds them present and returns without touching apt. Cargo is already offline against
# the mounted registry, and `--network none` then makes "offline verification" a property of the
# sandbox rather than a claim about what the tests happen to reach.
#
# The base is written out rather than taken as a build argument, because
# `scripts/guard/check-container-runtime-policy.sh` requires every `FROM` to name a digest it can
# read — a variable defeats that, and the rule is right: a reference nobody can audit is the thing it
# exists to stop. Every other Dockerfile here does the same.
#
# That makes this the second copy of a value `tools/container-images.env` also holds, so
# `technology-version-consistency` asserts the two are equal. This image layers onto the harness's
# toolchain; a drift between them would verify against something other than what CI runs.
FROM node:24.20.0-bookworm@sha256:be23f54a88d34e8824c741b19b91064094f92c1c97b194144bfc8b50d67258e2 AS node-tools

ENV COREPACK_HOME=/opt/corepack \
    PNPM_HOME=/opt/pnpm \
    PATH=/opt/pnpm:$PATH
RUN mkdir -p "$COREPACK_HOME" "$PNPM_HOME" /pnpm/store \
  && corepack enable --install-directory "$PNPM_HOME" \
  && corepack prepare pnpm@12.21.0 --activate
WORKDIR /warm/profile-gateway
COPY platforms/foundation-platform/services/foundation-profile-gateway/package.json ./package.json
COPY platforms/foundation-platform/services/foundation-profile-gateway/pnpm-lock.yaml ./pnpm-lock.yaml
# The overrides and allowBuilds (esbuild, workerd) the install needs; pnpm 11+ reads them only here.
COPY platforms/foundation-platform/services/foundation-profile-gateway/pnpm-workspace.yaml ./pnpm-workspace.yaml
# `pnpm fetch` omitted platform-specific optional packages (pnpm 9) even when the
# lockfile contains them.  workerd and esbuild resolve their Linux binaries
# through optionalDependencies, so perform the exact Linux install while the
# image still has network access and retain its content-addressed store only.
RUN pnpm install --frozen-lockfile --store-dir /pnpm/store \
  && rm -rf node_modules

FROM rust:1.99.1-bookworm@sha256:665b6e0a9290a7298236c08c6e4f1e754e3f0d9b70421c434daa3dfba888d63e

# The union of `Area::apt_deps` in tools/xtask/src/main.rs, the list CI installs through
# scripts/ci/apt-install.sh. `technology-version-consistency` fails when the two sets differ: the
# host's hand-built test image of 2026-11-21 carried its own list (clang added, python3-pytest
# missing) and so tested in an environment CI never ran.
#
# The slow, rarely changing layers come first and the Node store last: the store changes with the
# profile gateway's lockfile, and before it sat above apt, so a lockfile bump rebuilt every layer.
#
# cmake drives the vendored librdkafka build; libssl/libsasl2/libcurl/zlib are the transports it
# links (SSL, GSSAPI/SASL, OAUTHBEARER/OIDC, gzip). python3 and python3-pytest run the Foundation
# python suites. libclang-dev: bindgen generates the BoringSSL bindings of the court auction browser
# sessions (wreq, root ADR-0289 §1).
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    cmake \
    libclang-dev \
    python3 \
    python3-pytest \
    libssl-dev \
    libsasl2-dev \
    libcurl4-openssl-dev \
    zlib1g-dev \
  && rm -rf /var/lib/apt/lists/*

# The toolchain, resolved from the repository's own pin rather than from a list repeated here.
# `rust-toolchain.toml` asks for components the base image does not ship (`rust-analyzer`,
# `rust-src`), and rustup installs the missing ones the first time it runs inside the tree — over the
# network, which a `--network none` run cannot do. Installing at build time is what makes the run
# offline.
#
# `rustup toolchain install` with no arguments reads the file and installs exactly what it names, so
# the pin keeps one home. Copying the list into this Dockerfile would make two, and they would drift.
# The context is the repository root, narrowed by the root .dockerignore to the files this image
# copies.
COPY rust-toolchain.toml /toolchain/rust-toolchain.toml
WORKDIR /toolchain
RUN rustup toolchain install
WORKDIR /

# Warm the pinned release cache with actionlint. `scripts/guard/check-workflow-policy.sh` runs
# `scripts/ci/actionlint.sh`, which fetches a checksum-pinned release on first use — the last thing
# in the guard chain that reached the network, and the reason a network-less run died there without
# printing a verdict.
#
# The fetch runs through that script and the one pinned fetcher rather than a copy of their logic,
# so the version and checksum keep their single home in `tools/actionlint.env` and the URL in
# `tools/pinned-artifacts.json` (root ADR-0197). The cache sits at a fixed path rather than under a
# home directory, so whichever user the container runs as finds it. It needs a workflow to lint
# before it will exit zero, so it is pointed at a stub: the subject here is the cache, not this
# file. The fetcher verifies the checksum again on every read, so a corrupted cache is caught
# rather than trusted.
#
# sccache comes the same way: the version and SHA-256 CI's scripts/ci/sccache-setup.sh fetches live
# only in tools/sccache.env. The image carries the binary and a cache directory; it does not set
# RUSTC_WRAPPER, so cargo-verify.sh builds as before and a host test run opts in with
# `-e RUSTC_WRAPPER=sccache -e CARGO_INCREMENTAL=0 -v perfectory-sccache:/sccache` (runbook:
# host-crate-tests.md).
ENV PERFECTORY_PINNED_CACHE=/opt/perfectory-pinned
COPY tools/actionlint.env tools/sccache.env tools/pinned-artifacts.json /warm/tools/
COPY scripts/ci/actionlint.sh scripts/ci/fetch_pinned.py /warm/scripts/ci/
RUN set -eu; \
  mkdir -p /warm/.github/workflows; \
  printf 'name: warm\non:\n  push:\njobs:\n  warm:\n    runs-on: ubuntu-24.04\n    steps:\n      - run: "true"\n' \
    > /warm/.github/workflows/warm.yml; \
  cd /warm && PYTHONDONTWRITEBYTECODE=1 bash scripts/ci/actionlint.sh .github/workflows; \
  PYTHONDONTWRITEBYTECODE=1 python3 scripts/ci/fetch_pinned.py tool sccache --output /warm/sccache.tar.gz; \
  tar -xzf /warm/sccache.tar.gz -C /usr/local/bin --strip-components=1 --wildcards '*/sccache'; \
  chmod 0755 /usr/local/bin/sccache; \
  /usr/local/bin/sccache --version; \
  chmod -R a+rX "$PERFECTORY_PINNED_CACHE"; \
  rm -rf /warm
# The host runs mount one shared volume here; 20G is what fits beside the per-run target volumes on
# the data host's Docker disk (405 GB free, measured 2026-11-21).
ENV SCCACHE_DIR=/sccache \
    SCCACHE_CACHE_SIZE=20G

COPY --from=node-tools /usr/local/bin/node /usr/local/bin/node
COPY --from=node-tools /usr/local/lib/node_modules/corepack /usr/local/lib/node_modules/corepack
COPY --from=node-tools /opt/corepack /opt/corepack
COPY --from=node-tools /opt/pnpm /opt/pnpm
COPY --from=node-tools /pnpm/store /pnpm/store
ENV COREPACK_HOME=/opt/corepack \
    PNPM_HOME=/opt/pnpm \
    PATH=/opt/pnpm:$PATH

# Docker COPY dereferences `/usr/local/bin/corepack` from the Node image. Recreate
# the relative symlink after copying the module so its internal imports resolve.
RUN ln -s ../lib/node_modules/corepack/dist/corepack.js /usr/local/bin/corepack
