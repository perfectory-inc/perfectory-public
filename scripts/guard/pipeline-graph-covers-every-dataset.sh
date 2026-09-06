#!/usr/bin/env bash
# ADR-0086: the graph silently omitted ten lakehouse tables and three source groups.
# Compare with the owners, never a second inventory written into this guard.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd -P)"
exec python3 "$root/scripts/catalog/pipeline_graph.py" "${1:-$root}"
