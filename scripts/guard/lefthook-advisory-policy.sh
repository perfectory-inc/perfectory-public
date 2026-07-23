#!/usr/bin/env bash
# Keep ADR-0005's advisory-hook promise true even when a launcher exists but
# its concrete package binary or optional Cargo component does not.
set -euo pipefail
cd "$(dirname "$0")/../.."

config="${1:-lefthook.yml}"
checker="scripts/guard/check-lefthook-advisory-policy.py"

if command -v python3 >/dev/null 2>&1; then
  exec python3 "$checker" "$config"
fi
exec python "$checker" "$config"
