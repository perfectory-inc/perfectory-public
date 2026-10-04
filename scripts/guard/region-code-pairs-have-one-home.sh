#!/usr/bin/env bash
# Region code pairs have one home, parcel pairs another (root ADR-0145 §4). The rule and every name
# it checks live in the definition file the Python checker reads; this wrapper only finds Python.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"
checker="$(cd "$(dirname "$0")" && pwd)/region-code-pairs-have-one-home.py"

if command -v python3 >/dev/null 2>&1; then
  exec python3 "$checker" "$root"
fi
exec python "$checker" "$root"
