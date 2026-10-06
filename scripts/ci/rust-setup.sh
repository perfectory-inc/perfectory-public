#!/usr/bin/env bash
# The one Rust setup every CI job that runs cargo goes through (root ADR-0155).
#
# It installs the toolchain the root rust-toolchain.toml pins and exports, for
# every later step of the job, the registry settings that make a cold crates.io
# download survive a flaky network. Before it, five copies of the setup lived in
# the workflows: the jobs that used dtolnay/rust-toolchain silently got
# CARGO_HTTP_MULTIPLEXING=false from that action, the jobs that called rustup
# directly did not, and on 2026-10-06 two of the latter failed whole pull
# requests with "curl failed [16] Error in the HTTP2 framing layer".
#
# The cache step that follows this one (Swatinem/rust-cache) hashes CARGO_* and
# RUST* into its key, so these values must be exported before it runs.
# scripts/guard/rust-ci-setup.sh enforces the order and that no Rust job
# installs a toolchain any other way.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"

fail() {
  echo "FAIL rust-setup: $*" >&2
  exit 1
}

[ -n "${GITHUB_ENV:-}" ] || fail "GITHUB_ENV is unset; this script configures a GitHub Actions job"

channel="$(sed -n 's/^channel[[:space:]]*=[[:space:]]*"\([^"]*\)"[[:space:]]*$/\1/p' "$root/rust-toolchain.toml")"
case "$channel" in
  [0-9]*.[0-9]*.[0-9]*) ;;
  *) fail "rust-toolchain.toml must pin an exact channel, found '${channel}'" ;;
esac

# Registry robustness. HTTP/2 multiplexing is what the curl framing error comes
# from (rust-lang/cargo#12202 is the same failure); HTTP/1.1 with ten retries
# and the sparse index is what cargo itself recommends for CI.
settings=(
  "CARGO_NET_RETRY=10"
  "CARGO_HTTP_MULTIPLEXING=false"
  "CARGO_HTTP_TIMEOUT=60"
  "CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse"
  "RUSTUP_MAX_RETRIES=10"
  # CI builds once from clean sources; incremental state only bloats the cache.
  "CARGO_INCREMENTAL=0"
  "CARGO_TERM_COLOR=always"
  # The marker xtask checks before it runs in CI (scripts/guard/rust-ci-setup.sh).
  "PERFECTORY_RUST_SETUP=$channel"
)
for setting in "${settings[@]}"; do
  export "${setting?}"
  printf '%s\n' "$setting" >>"$GITHUB_ENV"
done

rustup toolchain install "$channel" --profile minimal \
  --component rustfmt --component clippy --no-self-update
rustup default "$channel"
rustc --version
cargo --version
