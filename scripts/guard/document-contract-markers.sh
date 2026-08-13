#!/usr/bin/env bash
# The Python checker walks the documentation and source trees in one pass. A per-file `grep` loop
# was the first shape and exceeded five minutes on Git for Windows; this wrapper keeps the monorepo
# guard entry portable across Linux CI and Git for Windows.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"
checker="$(cd "$(dirname "$0")" && pwd)/document-contract-markers.py"

if command -v python3 >/dev/null 2>&1; then
  exec python3 "$checker" "$root"
fi
exec python "$checker" "$root"
