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
  # The nextest profile every workspace's .config/nextest.toml defines for CI (root ADR-0269).
  "NEXTEST_PROFILE=ci"
  # The marker xtask checks before it runs in CI (scripts/guard/rust-ci-setup.sh).
  "PERFECTORY_RUST_SETUP=$channel"
)
for setting in "${settings[@]}"; do
  export "${setting?}"
  printf '%s\n' "$setting" >>"$GITHUB_ENV"
done

# RUSTUP_MAX_RETRIES retries at once, so an outage longer than a few seconds
# still fails the job: on 2026-10-10 a merge-queue run lost both identity jobs
# when every retry of the channel manifest download ran out within 30 s. Wait
# longer between whole attempts; a partial install is resumed by the next one.
waits=(15 30 60 120)
for attempt in 1 2 3 4 5; do
  if rustup toolchain install "$channel" --profile minimal \
    --component rustfmt --component clippy --no-self-update; then
    break
  fi
  [ "$attempt" -lt 5 ] || fail "rustup could not install ${channel} after ${attempt} attempts"
  wait_seconds="${waits[$((attempt - 1))]}"
  echo "rust-setup: toolchain install attempt ${attempt} failed; retrying in ${wait_seconds}s" >&2
  sleep "$wait_seconds"
done
rustup default "$channel"
rustc --version
cargo --version

# sccache: a shared compiler cache on R2, so a toolchain bump or a one-line change recompiles only
# the units that changed, not the whole target (root ADR-0293). Only when the R2 credentials are in
# the environment: a pull request from a fork gets none and builds as before, without sccache.
if [ -n "${SCCACHE_R2_ENDPOINT:-}" ] && [ -n "${AWS_ACCESS_KEY_ID:-}" ] && [ -n "${AWS_SECRET_ACCESS_KEY:-}" ]; then
  sccache_version="v0.18.0" # root ADR-0293; bump with the publisher's checksum, which this verifies
  sccache_dir="sccache-${sccache_version}-x86_64-unknown-linux-musl"
  base="https://github.com/mozilla/sccache/releases/download/${sccache_version}"
  tmp="$(mktemp -d)"
  curl -sSfL --retry 5 --retry-connrefused -o "$tmp/sccache.tar.gz" "$base/${sccache_dir}.tar.gz"
  curl -sSfL --retry 5 --retry-connrefused -o "$tmp/sccache.sha256" "$base/${sccache_dir}.tar.gz.sha256"
  expected="$(cut -d' ' -f1 <"$tmp/sccache.sha256")"
  actual="$(sha256sum "$tmp/sccache.tar.gz" | cut -d' ' -f1)"
  { [ -n "$expected" ] && [ "$expected" = "$actual" ]; } \
    || fail "sccache ${sccache_version} download did not match its published checksum ($actual != $expected)"
  tar -xzf "$tmp/sccache.tar.gz" -C "$tmp"
  install -d "$HOME/.local/bin"
  install -m 0755 "$tmp/${sccache_dir}/sccache" "$HOME/.local/bin/sccache"
  printf '%s\n' "$HOME/.local/bin" >>"$GITHUB_PATH"
  rm -rf "$tmp"
  # SCCACHE_* configure the S3 backend; the AWS_* credentials come from the job env (repo secrets).
  # A per-toolchain key prefix keeps a channel's objects apart, so a bump never serves a stale unit.
  sccache_settings=(
    "RUSTC_WRAPPER=sccache"
    "SCCACHE_BUCKET=sccache"
    "SCCACHE_ENDPOINT=${SCCACHE_R2_ENDPOINT}"
    "SCCACHE_REGION=auto"
    "SCCACHE_S3_KEY_PREFIX=${channel}"
  )
  for setting in "${sccache_settings[@]}"; do
    export "${setting?}"
    printf '%s\n' "$setting" >>"$GITHUB_ENV"
  done
  echo "rust-setup: sccache ${sccache_version} on, caching to R2 (bucket sccache, key prefix ${channel})"
else
  echo "rust-setup: sccache off (no R2 credentials in env); building without the shared compiler cache"
fi
