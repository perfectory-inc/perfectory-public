#!/usr/bin/env bash
# The Python checker walks the tracked shell scripts and workflows in one pass, matching the
# portability the other document and source guards settled on.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"
checker="$(cd "$(dirname "$0")" && pwd)/judgment-position-exit-codes.py"

if command -v python3 >/dev/null 2>&1; then
  exec python3 "$checker" "$root"
fi
exec python "$checker" "$root"
