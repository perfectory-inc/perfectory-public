#!/usr/bin/env bash
# Threat model: honest-mistake detection.
# Prevents: a package.json pnpm.overrides list edited by hand, a lockfile not regenerated from
# the rendered overrides, or a lock still resolving a version below a recorded advisory floor
# (root ADR-0158). The trees are every tracked pnpm-lock.yaml; the list lives in
# tools/npm/security-overrides.contract.json and nowhere else.
# Does not prevent: an advisory nobody has recorded yet (the OSV ratchet and its daily run do).
set -euo pipefail
command -v node >/dev/null || {
  echo "FAIL npm-security-overrides: missing command 'node'" >&2
  exit 1
}
root="$(cd "$(dirname "$0")/../.." && pwd -P)"
node "$root/tools/npm/security-overrides.mjs" check --root "${1:-$root}"
