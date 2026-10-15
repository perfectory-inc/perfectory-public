#!/usr/bin/env bash
# The area list has one home, tools/areas.contract.json. The rule lives in the Python checker; this
# wrapper only finds Python.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"
checker="$(cd "$(dirname "$0")" && pwd)/area-registry.py"

if command -v python3 >/dev/null 2>&1; then
  exec python3 "$checker" "$root"
fi
exec python "$checker" "$root"
