#!/usr/bin/env bash
# The hourly synthetic read of a live by-PNU hostname (root ADR-0151 Revision, ADR-0160, contract
# <lane>_by_pnu_gateway.section_packs.monitor); run by foundation-by-pnu-serving-monitor@<lane>.service.
#
#   by-pnu-serving-monitor.sh <building|parcel>
#
# Reads the first monitor.pnus PNUs of the lane's gate sample (FOUNDATION_PLATFORM_<LANE>_BY_PNU_
# SERVING_MONITOR_SAMPLE_PATH, the equality evidence on this host) from the live hostname and holds
# each to the document the lane serves and, while they exist, to the object document. Read-only. A
# breach exits non-zero, and the unit's OnFailure reports it to Slack.
set -euo pipefail
LANE="${1:-}"
[[ "${LANE}" == building || "${LANE}" == parcel ]] \
  || { echo "usage: by-pnu-serving-monitor.sh <building|parcel>" >&2; exit 64; }
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
export "FOUNDATION_PLATFORM_${LANE^^}_BY_PNU_SERVING_OUTPUT_STORAGE_DRIVER=r2"
exec "${PUBLISHER_BIN}" "monitor-${LANE}-by-pnu-serving"
