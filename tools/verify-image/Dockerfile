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
FROM rust:1.96.0-bookworm@sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc

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
