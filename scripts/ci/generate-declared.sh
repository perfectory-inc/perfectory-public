#!/usr/bin/env bash
# Writes the artifacts the declared definitions generate into this checkout (root ADR-0418): the job
# list, the pipeline graph, the by-PNU lane lines, the host memory entries, the schema subjects and
# what is rendered from them. They are not committed; every CI job that checks out the whole tree
# runs this right after its checkout, so every reader finds them where it always has.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
generator="${root}/scripts/catalog/declared_definitions.py"
if command -v python3 >/dev/null 2>&1 && python3 -c '' >/dev/null 2>&1; then
  exec python3 "${generator}" generate --root "${root}"
fi
exec python "${generator}" generate --root "${root}"
