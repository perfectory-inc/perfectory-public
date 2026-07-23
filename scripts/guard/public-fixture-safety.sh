#!/usr/bin/env bash
# The Python checker parses JSON/JSONL structurally. This wrapper keeps the
# monorepo guard entry portable across Linux CI and Git for Windows.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"
checker="$(cd "$(dirname "$0")" && pwd)/public-fixture-safety.py"

if command -v python3 >/dev/null 2>&1; then
  exec python3 "$checker" "$root"
fi
exec python "$checker" "$root"
