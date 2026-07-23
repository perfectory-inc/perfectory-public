#!/usr/bin/env bash
# Prevents: deploy-probe drift (observed 4 health styles across 4 areas; probes/monitors
# must not need per-area knowledge). Convention: /healthz + /readyz (docs/adr/0001 §5).
set -euo pipefail
cd "$(dirname "$0")/../.."
bad=$(grep -rnE '\.route\("(/health"|/health/live"|/health/ready"|/ready"|/healthz/ready")' \
  --include='*.rs' products/*/services platforms/*/services || true)
if [ -n "$bad" ]; then
  echo "FAIL health-route-conformance: use /healthz and /readyz:" >&2
  echo "$bad" >&2
  exit 1
fi
echo "OK health-route-conformance"
