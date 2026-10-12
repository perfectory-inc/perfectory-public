#!/usr/bin/env bash
# The hourly synthetic read of a live by-PNU hostname (root ADR-0151 Revision, ADR-0160, contract
# <lane>_by_pnu_gateway.section_packs.monitor); run by foundation-by-pnu-serving-monitor@<lane>.service.
#
#   by-pnu-serving-monitor.sh <building|parcel|a by_pnu_lanes lane>
#
# Reads the first monitor.pnus PNUs of the lane's gate sample (FOUNDATION_PLATFORM_<LANE>_BY_PNU_
# SERVING_MONITOR_SAMPLE_PATH, the equality evidence on this host) from the live hostname and holds
# each to the document the lane serves and, while they exist, to the object document (a pack-only
# lane of the contract's by_pnu_lanes has none, root ADR-0190). Read-only. A breach exits non-zero,
# and the unit's OnFailure reports it to Slack.
set -euo pipefail
LANE="${1:-}"
python3 -I -c 'import json, sys; sys.exit(sys.argv[2] not in ["parcel", "building", *json.load(open(sys.argv[1], encoding="utf-8")).get("by_pnu_lanes", {})])' \
  "$(dirname "${BASH_SOURCE[0]}")/../../config/r2-connections.contract.json" "${LANE}" \
  || { echo "usage: by-pnu-serving-monitor.sh <building|parcel|a by_pnu_lanes lane>" >&2; exit 64; }
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
export "FOUNDATION_PLATFORM_$(tr 'a-z-' 'A-Z_' <<<"${LANE}")_BY_PNU_SERVING_OUTPUT_STORAGE_DRIVER=r2"
exec "${PUBLISHER_BIN}" "monitor-${LANE}-by-pnu-serving"
