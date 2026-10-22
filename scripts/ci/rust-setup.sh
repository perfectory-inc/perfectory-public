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

# sccache: a shared Rust compiler cache on R2 (root ADR-0294), so a change recompiles only the units
# whose inputs changed, not the whole target. R2's S3 API refuses anonymous requests, so sccache runs
# only when the R2 key is in the environment (repo secrets, read-write — root ADR-0294 §A): a fork
# pull request gets none and builds as before. A bad or unreachable backend never breaks a build —
# sccache compiles directly on any backend error, and if the wrapper cannot run a probe the job
# builds without it. The binary comes through the one pinned downloader (root ADR-0197), published for
# Linux only; other runners (the Windows toolchain job) build as before. The endpoint is public (it
# names the bucket's account, not a key), so it travels as a plain env value.
if [ -n "${SCCACHE_R2_ENDPOINT:-}" ] && [ -n "${AWS_ACCESS_KEY_ID:-}" ] && [ -n "${AWS_SECRET_ACCESS_KEY:-}" ] && [ "$(uname -s)" = Linux ]; then
  install -d "$HOME/.local/bin"
  tmp="$(mktemp -d)"
  if python3 "$root/scripts/ci/fetch_pinned.py" tool sccache --output "$tmp/sccache.tar.gz" \
    && tar -xzf "$tmp/sccache.tar.gz" -C "$HOME/.local/bin" --strip-components=1 --wildcards '*/sccache'; then
    chmod 0755 "$HOME/.local/bin/sccache"
    export PATH="$HOME/.local/bin:$PATH"
    printf '%s\n' "$HOME/.local/bin" >>"$GITHUB_PATH"
    # SCCACHE_* configure the S3 backend (the R2 key arrives as the client's own AWS_* names); a
    # per-toolchain key prefix keeps a channel's objects apart, so a bump never serves a stale unit.
    sccache_settings=(
      "SCCACHE_BUCKET=perfectory-sccache"
      "SCCACHE_ENDPOINT=${SCCACHE_R2_ENDPOINT}"
      "SCCACHE_REGION=auto"
      "SCCACHE_S3_KEY_PREFIX=${channel}"
    )
    for setting in "${sccache_settings[@]}"; do export "${setting?}"; done
    # Prove the wrapper reaches R2; a cache miss is fine, but a bad endpoint or key degrades to a
    # normal build rather than carrying a broken wrapper into every compile.
    printf 'pub fn probe() {}\n' >"$tmp/probe.rs"
    if sccache rustc --crate-name sccache_probe --edition 2021 --emit=metadata \
        -o "$tmp/probe.rmeta" "$tmp/probe.rs" >"$tmp/sccache-probe.log" 2>&1; then
      sccache_settings+=("RUSTC_WRAPPER=sccache")
      for setting in "${sccache_settings[@]}"; do printf '%s\n' "$setting" >>"$GITHUB_ENV"; done
      echo "rust-setup: sccache on (read-write), bucket perfectory-sccache, key prefix ${channel}"
      sccache --show-stats 2>&1 | sed 's/^/  /' | head -n 20 || true
    else
      echo "rust-setup: sccache could not run a probe compile; building without the shared cache. Reason:" >&2
      sed 's/^/  sccache: /' "$tmp/sccache-probe.log" >&2 || true
      sccache --stop-server >/dev/null 2>&1 || true
    fi
  else
    echo "rust-setup: could not fetch the pinned sccache; building without the shared compiler cache" >&2
  fi
  rm -rf "$tmp"
else
  echo "rust-setup: sccache off (no SCCACHE_R2_ENDPOINT/key, or not Linux); building without the shared compiler cache"
fi
