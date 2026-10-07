#!/usr/bin/env bash
# One canary step's verdict for a by-PNU gateway (root ADR-0151 Revision, ADR-0160):
#
#   by-pnu-gateway-health.sh <building|parcel> <new-version-id> [<old-version-id>]
#
# Run as root on the host that holds the Cloudflare analytics token: the publisher reads it in a
# transient unit from the file the contract names (by_pnu_section_packs.cloudflare_analytics.
# env_file, root:root 0600), never from this shell, and the gate sample from the serving monitor's
# file (infra/systemd/foundation-by-pnu-serving-monitor@.service). It first reads the contract's
# canary.synthetic_load PNUs from the live hostname pinned to each version, then judges from
# Cloudflare analytics. scripts/ops/by-pnu-gateway-canary.sh calls it through
# CANARY_HEALTH_COMMAND. Exits non-zero on a breach.
#
#   by-pnu-gateway-health.sh <building|parcel> --preflight
#
# runs both analytics queries the verdict needs (Workers invocations, and the zone's HTTP
# responses) once, so the canary refuses before it uploads anything when the token lacks either
# permission or the zone id is missing.
set -euo pipefail
USAGE="usage: by-pnu-gateway-health.sh <building|parcel> <new-version-id> [<old-version-id>] | --preflight"
LANE="${1:-}"
[[ "${LANE}" == building || "${LANE}" == parcel ]] || { echo "${USAGE}" >&2; exit 64; }
NEW="${2:?${USAGE}}"
OLD="${3:-}"
# The publisher's command and environment names carry the lane (section_packs.rs).
COMMAND="check-${LANE}-gateway-version-health"
PREFIX="FOUNDATION_PLATFORM_${LANE^^}_BY_PNU_SERVING"
PLATFORM_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
# The file is named once, by the runtime-secrets contract (root ADR-0153), group cloudflare-analytics.
ENV_FILE="$(python3 -c 'import json,sys; print(next(g["path"] for g in json.load(open(sys.argv[1]))["groups"] if g["name"] == "cloudflare-analytics"))' \
  "${PLATFORM_ROOT}/config/runtime-secrets.contract.json")"
# The monitor's own file, likewise named by the runtime-secrets contract (group <lane>-serving-monitor).
MONITOR_ENV_FILE="${FOUNDATION_BY_PNU_MONITOR_ENV_FILE:-$(python3 -c 'import json,sys; print(next(g["path"] for g in json.load(open(sys.argv[1]))["groups"] if g["name"] == sys.argv[2] + "-serving-monitor"))' "${PLATFORM_ROOT}/config/runtime-secrets.contract.json" "${LANE}")}"
# systemd-run with a missing EnvironmentFile fails with a bare "Failed to load environment files";
# say what is missing instead.
if [[ ! -f "${ENV_FILE}" ]]; then
  echo "by-pnu-gateway-health ${LANE}: refused: ${ENV_FILE} does not exist. Create it root:root 0600 with" \
    "the Cloudflare account id, the live hostname's zone id and a token scoped as the contract's" \
    "by_pnu_section_packs.cloudflare_analytics.token_scope says, under the variable names it names;" \
    "the canary cannot judge a step without it" >&2
  exit 78
fi
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
unit=(systemd-run --wait --collect --pipe --quiet -p User=foundation-platform -p "EnvironmentFile=${ENV_FILE}")
if [[ "${NEW}" == "--preflight" ]]; then
  exec "${unit[@]}" -E "${PREFIX}_CANARY_PREFLIGHT=true" "${PUBLISHER_BIN}" "${COMMAND}"
fi
if [[ ! -f "${MONITOR_ENV_FILE}" ]]; then
  echo "by-pnu-gateway-health ${LANE}: refused: ${MONITOR_ENV_FILE} does not exist; it names the gate sample" \
    "(${PREFIX}_MONITOR_SAMPLE_PATH) the pinned reads draw from" >&2
  exit 78
fi
args=(-p "EnvironmentFile=${MONITOR_ENV_FILE}" -E "${PREFIX}_CANARY_NEW_VERSION=${NEW}")
[[ -z "${OLD}" ]] || args+=(-E "${PREFIX}_CANARY_OLD_VERSION=${OLD}")
exec "${unit[@]}" "${args[@]}" "${PUBLISHER_BIN}" "${COMMAND}"
