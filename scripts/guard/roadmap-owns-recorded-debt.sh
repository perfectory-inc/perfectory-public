#!/usr/bin/env bash
# The Python checker walks the documentation tree in one pass, matching the portability the other
# document guards settled on: a per-file `grep` loop exceeded five minutes on Git for Windows.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"
checker="$(cd "$(dirname "$0")" && pwd)/roadmap-owns-recorded-debt.py"

if command -v python3 >/dev/null 2>&1; then
  exec python3 "$checker" "$root"
fi
exec python "$checker" "$root"
