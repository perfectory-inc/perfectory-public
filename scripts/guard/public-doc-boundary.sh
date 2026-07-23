#!/usr/bin/env bash
# Stable shell entry point for the structural public-document boundary guard.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"
checker="$(cd "$(dirname "$0")" && pwd)/public-doc-boundary.py"
command -v python3 >/dev/null || {
  echo "FAIL public-doc-boundary: missing command 'python3'" >&2
  exit 1
}
exec python3 "$checker" "$root"
