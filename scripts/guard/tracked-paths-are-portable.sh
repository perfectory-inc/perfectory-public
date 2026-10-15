#!/usr/bin/env bash
# Every tracked path can be checked out on every OS; see the Python checker for
# the incident. This wrapper keeps the entry portable across Linux CI and Git for
# Windows.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"
checker="$(cd "$(dirname "$0")" && pwd)/tracked-paths-are-portable.py"

if command -v python3 >/dev/null 2>&1; then
  exec python3 "$checker" "$root"
fi
exec python "$checker" "$root"
