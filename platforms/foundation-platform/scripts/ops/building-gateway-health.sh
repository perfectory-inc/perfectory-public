#!/usr/bin/env bash
# One canary step's verdict for the building gateway (root ADR-0151 Revision):
#
#   building-gateway-health.sh <new-version-id> [<old-version-id>]
#
# Run as root on the host that holds the Cloudflare analytics token: the publisher reads it in a
# transient unit from the file the contract names (by_pnu_section_packs.cloudflare_analytics.
# env_file, root:root 0600), never from this shell. scripts/ops/building-gateway-canary.sh calls it
# through CANARY_HEALTH_COMMAND. Read-only; exits non-zero on a breach.
set -euo pipefail
NEW="${1:?usage: building-gateway-health.sh <new-version-id> [<old-version-id>]}"
OLD="${2:-}"
PLATFORM_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
ENV_FILE="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["by_pnu_section_packs"]["cloudflare_analytics"]["env_file"])' \
  "${PLATFORM_ROOT}/config/r2-connections.contract.json")"
source "${PLATFORM_ROOT}/scripts/ops/admitted-writer-runtime.sh" --current
args=(-E "FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_CANARY_NEW_VERSION=${NEW}")
[[ -z "${OLD}" ]] || args+=(-E "FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_CANARY_OLD_VERSION=${OLD}")
exec systemd-run --wait --collect --pipe --quiet -p User=foundation-platform \
  -p "EnvironmentFile=${ENV_FILE}" "${args[@]}" \
  "${PUBLISHER_BIN}" check-building-gateway-version-health
