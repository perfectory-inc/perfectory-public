#!/usr/bin/env bash
# No tracked source writes a 시도 code table by hand (root ADR-0403). The rule, the incident it
# prevents and the shrink-only baseline live in the Python checker; this wrapper only finds Python.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"
checker="$(cd "$(dirname "$0")" && pwd)/sido-codes-come-from-the-legal-dong-table.py"

if command -v python3 >/dev/null 2>&1; then
  exec python3 "$checker" "$root"
fi
exec python "$checker" "$root"
