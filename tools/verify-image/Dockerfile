# The image `scripts/verify/cargo-verify.sh` verifies inside.
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
FROM node:20.19.0-bookworm@sha256:a5fb035ac1dff34a4ecaea85f90f7321185695d3fd22c12ba12f4535a4647cc5 AS node-tools

ENV COREPACK_HOME=/opt/corepack \
    PNPM_HOME=/opt/pnpm \
    PATH=/opt/pnpm:$PATH
RUN mkdir -p "$COREPACK_HOME" "$PNPM_HOME" /pnpm/store \
  && corepack enable --install-directory "$PNPM_HOME" \
  && corepack prepare pnpm@9.12.0 --activate
WORKDIR /warm/profile-gateway
COPY platforms/foundation-platform/services/foundation-profile-gateway/package.json ./package.json
COPY platforms/foundation-platform/services/foundation-profile-gateway/pnpm-lock.yaml ./pnpm-lock.yaml
# `pnpm fetch` omits platform-specific optional packages in pnpm 9 even when the
# lockfile contains them.  workerd and esbuild resolve their Linux binaries
# through optionalDependencies, so perform the exact Linux install while the
# image still has network access and retain its content-addressed store only.
RUN pnpm install --frozen-lockfile --store-dir /pnpm/store \
  && rm -rf node_modules

FROM rust:1.96.0-bookworm@sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc

COPY --from=node-tools /usr/local/bin/node /usr/local/bin/node
COPY --from=node-tools /usr/local/lib/node_modules/corepack /usr/local/lib/node_modules/corepack
COPY --from=node-tools /opt/corepack /opt/corepack
COPY --from=node-tools /opt/pnpm /opt/pnpm
COPY --from=node-tools /pnpm/store /pnpm/store
ENV COREPACK_HOME=/opt/corepack \
    PNPM_HOME=/opt/pnpm \
    PATH=/opt/pnpm:$PATH

# The union of `Area::apt_deps` in tools/xtask/src/main.rs. Keep the two in step: a dependency listed
# there and missing here makes `ensure_apt` reach for apt, which a network-less run cannot do — the
# failure is loud, not silent.
#
# cmake drives the vendored librdkafka build; libssl/libsasl2/libcurl/zlib are the transports it
# links (SSL, GSSAPI/SASL, OAUTHBEARER/OIDC, gzip). python3 and python3-pytest run the Foundation
# python suites.
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    cmake \
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
# The caller assembles the build context so this file can be copied without widening it to the
# repository root.
COPY rust-toolchain.toml /toolchain/rust-toolchain.toml
WORKDIR /toolchain
RUN rustup toolchain install
WORKDIR /

# Warm the actionlint download cache. `scripts/guard/check-workflow-policy.sh` runs
# `scripts/ci/actionlint.sh`, which fetches a checksum-pinned release on first use — the last thing
# in the guard chain that reached the network, and the reason a network-less run died there without
# printing a verdict.
#
# The fetch runs through that script rather than a copy of its logic, so the version and checksum
# keep their single home in `tools/actionlint.env`. It needs a workflow to lint before it will exit
# zero, so it is pointed at a stub: the subject here is the cache, not this file. The script verifies
# the checksum again at run time, so a corrupted cache is caught rather than trusted.
COPY tools/actionlint.env /warm/tools/actionlint.env
COPY scripts/ci/actionlint.sh /warm/scripts/ci/actionlint.sh
RUN set -eu; \
  mkdir -p /warm/.github/workflows; \
  printf 'name: warm\non:\n  push:\njobs:\n  warm:\n    runs-on: ubuntu-24.04\n    steps:\n      - run: "true"\n' \
    > /warm/.github/workflows/warm.yml; \
  cd /warm && bash scripts/ci/actionlint.sh .github/workflows; \
  rm -rf /warm

# Docker COPY dereferences `/usr/local/bin/corepack` from the Node image. Recreate
# the relative symlink after copying the module so its internal imports resolve.
RUN ln -s ../lib/node_modules/corepack/dist/corepack.js /usr/local/bin/corepack
