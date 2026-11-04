#!/usr/bin/env bash
# No Spark job takes the current rows of silver.parcel_boundaries but through the served reader
# (root ADR-0291 §5). The rule and the incident it prevents live in the Python checker; this wrapper
# only finds Python.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"
checker="$(cd "$(dirname "$0")" && pwd)/parcel-reads-the-served-edition.py"

if command -v python3 >/dev/null 2>&1; then
  exec python3 "$checker" "$root"
fi
exec python "$checker" "$root"
