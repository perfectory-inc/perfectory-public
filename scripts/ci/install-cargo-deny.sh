#!/usr/bin/env bash
# Installs the cargo-deny release pinned in tools/cargo-deny.env onto the job's
# PATH (root ADR-0155). The archive is checked against the pinned SHA-256 before
# anything is extracted from it.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
policy="$root/tools/cargo-deny.env"

fail() {
  echo "FAIL install-cargo-deny: $*" >&2
  exit 1
}

[ -n "${GITHUB_PATH:-}" ] || fail "GITHUB_PATH is unset; this script configures a GitHub Actions job"
[ -n "${RUNNER_TEMP:-}" ] || fail "RUNNER_TEMP is unset"
version="$(sed -n 's/^CARGO_DENY_VERSION=//p' "$policy" | tr -d '\r')"
printf '%s\n' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' || fail "unreadable version in $policy"

name="cargo-deny-${version}-x86_64-unknown-linux-musl"
archive="$RUNNER_TEMP/$name.tar.gz"
tool_bin="$RUNNER_TEMP/cargo-deny/bin"
mkdir -p "$tool_bin"
# Downloaded, checksum-verified and cached by the one pinned fetcher (root ADR-0197).
python3 "$root/scripts/ci/fetch_pinned.py" tool cargo-deny --output "$archive"
tar xzf "$archive" --strip-components=1 -C "$tool_bin" "$name/cargo-deny"
printf '%s\n' "$tool_bin" >>"$GITHUB_PATH"
"$tool_bin/cargo-deny" --version
