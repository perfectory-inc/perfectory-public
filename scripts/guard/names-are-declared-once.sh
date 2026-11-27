#!/usr/bin/env bash
# A table name written in code is one the contracts or the naming registry's rename work list declare
# (root ADR-0626 §7, G4). The rule and the incident it prevents live in the Python checker; this wrapper
# only finds Python.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"
checker="$(cd "$(dirname "$0")" && pwd)/names-are-declared-once.py"

if command -v python3 >/dev/null 2>&1; then
  exec python3 "$checker" "$root"
fi
exec python "$checker" "$root"
