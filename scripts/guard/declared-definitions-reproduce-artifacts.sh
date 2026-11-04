#!/usr/bin/env bash
# The declared definitions generate every artifact they will replace (root ADR-0415 §5 phase 1):
# the committed job list, pipeline graph, by-PNU lane lines, host memory entries and schema
# subjects split into one definition per dataset group, and generated back, equal in items,
# fields and order, with job-dependencies.v1.json and docs/data-pipeline-map.md byte for byte.
# The incident it prevents: 2026-10-24 a new own-producer kind was added to some of the ~14 hand
# copies a lane needs and not to the host enumerations (#814); the flip that ends those copies is
# safe only while this holds. The rule lives in the Python module; this wrapper only finds Python.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"
checker="$(cd "$(dirname "$0")/../.." && pwd)/scripts/catalog/declared_definitions.py"

if command -v python3 >/dev/null 2>&1; then
  exec python3 "$checker" check --root "$root"
fi
exec python "$checker" check --root "$root"
