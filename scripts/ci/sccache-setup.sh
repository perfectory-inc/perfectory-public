#!/usr/bin/env bash
# sccache: a shared Rust compiler cache on R2 (root ADR-0294), run as its own CI step AFTER
# scripts/ci/rust-setup.sh. Keeping it out of rust-setup.sh is deliberate: rust-setup is the one
# setup every Rust CI job goes through (root ADR-0155), and if it reached the pinned downloader every
# such job would have to restore the pinned cache first (scripts/guard/check-pinned-downloads.py). So
# only the jobs that opt into caching run this step, and only they carry the restore. A change then
# recompiles only the units whose inputs changed, not the whole target.
#
# R2's S3 API refuses anonymous requests, so sccache runs only when the R2 key is in the environment
# (repo secrets, read-write — root ADR-0294 §A): a fork pull request gets none and builds as before.
# A bad or unreachable backend never breaks a build — sccache compiles directly on any backend error,
# and if the wrapper cannot run a probe the job builds without it. The binary comes through the one
# pinned downloader (root ADR-0197), published for Linux only. The endpoint is public (it names the
# bucket's account, not a key), so it travels as a plain env value.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"

[ -n "${GITHUB_ENV:-}" ] || {
  echo "sccache-setup: GITHUB_ENV is unset; this configures a GitHub Actions job step" >&2
  exit 1
}

# The per-toolchain key prefix keeps a channel's objects apart, so a bump never serves a stale unit.
channel="$(sed -n 's/^channel[[:space:]]*=[[:space:]]*"\([^"]*\)"[[:space:]]*$/\1/p' "$root/rust-toolchain.toml")"

if [ -n "${SCCACHE_R2_ENDPOINT:-}" ] && [ -n "${AWS_ACCESS_KEY_ID:-}" ] && [ -n "${AWS_SECRET_ACCESS_KEY:-}" ] && [ "$(uname -s)" = Linux ]; then
  install -d "$HOME/.local/bin"
  tmp="$(mktemp -d)"
  if python3 "$root/scripts/ci/fetch_pinned.py" tool sccache --output "$tmp/sccache.tar.gz" \
    && tar -xzf "$tmp/sccache.tar.gz" -C "$HOME/.local/bin" --strip-components=1 --wildcards '*/sccache'; then
    chmod 0755 "$HOME/.local/bin/sccache"
    export PATH="$HOME/.local/bin:$PATH"
    printf '%s\n' "$HOME/.local/bin" >>"$GITHUB_PATH"
    # SCCACHE_* configure the S3 backend; the R2 key arrives as the client's own AWS_* names.
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
    if sccache rustc --crate-name sccache_probe --crate-type lib --edition 2021 --emit=metadata \
        -o "$tmp/probe.rmeta" "$tmp/probe.rs" >"$tmp/sccache-probe.log" 2>&1; then
      sccache_settings+=("RUSTC_WRAPPER=sccache")
      for setting in "${sccache_settings[@]}"; do printf '%s\n' "$setting" >>"$GITHUB_ENV"; done
      echo "sccache-setup: on (read-write), bucket perfectory-sccache, key prefix ${channel}"
      sccache --show-stats 2>&1 | sed 's/^/  /' | head -n 20 || true
    else
      echo "sccache-setup: could not run a probe compile; building without the shared cache. Reason:" >&2
      sed 's/^/  sccache: /' "$tmp/sccache-probe.log" >&2 || true
      sccache --stop-server >/dev/null 2>&1 || true
    fi
  else
    echo "sccache-setup: could not fetch the pinned sccache; building without the shared compiler cache" >&2
  fi
  rm -rf "$tmp"
else
  echo "sccache-setup: off (no SCCACHE_R2_ENDPOINT/key, or not Linux); building without the shared compiler cache"
fi
