#!/usr/bin/env bash
# Local hooks stay a fast pre-filter and every hook step also runs in CI (ADR-0149).
# Prevents: a hook that builds Rust or starts Docker (20-25 minute pushes killed on a
# 16 GB laptop), and a hook-only check that nobody enforces (the dead SP10 panel guards).
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="scripts/guard/check-lefthook-time-budget.py"
if command -v python3 >/dev/null 2>&1; then
  exec python3 "$checker" "$@"
fi
exec python "$checker" "$@"
