#!/usr/bin/env bash
# Every scheduled job says what one run processes, and `full` only shrinks (root ADR-0314). The
# rule, the incident it prevents and the ratchet live in the Python checker; this wrapper only
# finds Python.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"
checker="$(cd "$(dirname "$0")" && pwd)/every-job-declares-its-scope.py"

if command -v python3 >/dev/null 2>&1; then
  exec python3 "$checker" "$root"
fi
exec python "$checker" "$root"
