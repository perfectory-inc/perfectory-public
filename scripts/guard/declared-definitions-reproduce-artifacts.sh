#!/usr/bin/env bash
# The declared definitions are the one source of the job list, the pipeline graph, the by-PNU lane
# lines, the host memory entries and the schema subjects, and of what is rendered from them (root
# ADR-0415, ADR-0418): they generate, git tracks none of the artifacts, and a copy in the tree is what
# the definitions generate now. The incident it prevents: 2026-10-24 a new own-producer kind was
# written into some of the ~14 hand copies a lane needed and not into the host enumerations (#814); a
# committed copy would bring the copies, and their conflicts, back. The rule lives in the Python
# module; this wrapper only finds Python.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"
checker="$(cd "$(dirname "$0")/../.." && pwd)/scripts/catalog/declared_definitions.py"

if command -v python3 >/dev/null 2>&1 && python3 -c '' >/dev/null 2>&1; then
  exec python3 "$checker" check --root "$root"
fi
exec python "$checker" check --root "$root"
