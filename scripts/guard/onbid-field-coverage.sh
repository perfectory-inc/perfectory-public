#!/usr/bin/env bash
# Every Onbid API field is carried to the screen or says why not (root ADR-0340). The rule and the
# incident it prevents live in the Python checker; this wrapper only finds Python.
set -euo pipefail

checker="$(cd "$(dirname "$0")" && pwd)/onbid-field-coverage.py"

if command -v python3 >/dev/null 2>&1; then
  exec python3 "$checker" "$@"
fi
exec python "$checker" "$@"
