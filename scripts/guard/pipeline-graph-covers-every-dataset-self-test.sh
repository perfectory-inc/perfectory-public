#!/usr/bin/env bash
# Mutate independent owners as well as the graph, proving this is reconciliation.
# The connectivity injections prove the guard rejects producer-less and consumer-less nodes.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd -P)"
python3 "$root/scripts/catalog/test_pipeline_graph.py"
exec python3 "$root/scripts/catalog/test_pipeline_graph_connectivity.py"
