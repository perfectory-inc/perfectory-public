#!/usr/bin/env bash
# Every migration approval gate is declared with the day production got its setting (root ADR-0259
# §4). The rule lives in the Python checker and its contract; this wrapper only finds Python.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"
checker="$(cd "$(dirname "$0")" && pwd)/migration-approval-gates.py"

if command -v python3 >/dev/null 2>&1; then
  exec python3 "$checker" "$root"
fi
exec python "$checker" "$root"
