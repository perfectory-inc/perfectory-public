#!/usr/bin/env bash
# Installs the cargo-deny release pinned in tools/cargo-deny.env onto the job's
# PATH (root ADR-0152). The archive is checked against the pinned SHA-256 before
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
sha256="$(sed -n 's/^CARGO_DENY_LINUX_AMD64_SHA256=//p' "$policy" | tr -d '\r')"
printf '%s\n' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' || fail "unreadable version in $policy"
printf '%s\n' "$sha256" | grep -Eq '^[0-9a-f]{64}$' || fail "unreadable SHA-256 in $policy"

name="cargo-deny-${version}-x86_64-unknown-linux-musl"
archive="$RUNNER_TEMP/$name.tar.gz"
tool_bin="$RUNNER_TEMP/cargo-deny/bin"
mkdir -p "$tool_bin"
curl --silent --show-error --fail --location --retry 5 --retry-all-errors --output "$archive" \
  "https://github.com/EmbarkStudios/cargo-deny/releases/download/${version}/${name}.tar.gz"
printf '%s  %s\n' "$sha256" "$archive" | sha256sum --check --strict -
tar xzf "$archive" --strip-components=1 -C "$tool_bin" "$name/cargo-deny"
printf '%s\n' "$tool_bin" >>"$GITHUB_PATH"
"$tool_bin/cargo-deny" --version
