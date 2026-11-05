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
source "$(dirname "${BASH_SOURCE[0]}")/by-pnu-lane-index.sh"
by_pnu_lane_known "${LANE}" \
  || { echo "usage: by-pnu-serving-monitor.sh <building|parcel|a by_pnu_lanes lane>" >&2; exit 64; }
PREFIX="$(by_pnu_lane_env_prefix "${LANE}")"
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
export "${PREFIX}_OUTPUT_STORAGE_DRIVER=r2"

# Root ADR-0306: the served manifest's age and the budget it is held to, as node-exporter textfile
# series, so FoundationByPnuServingStale says in Slack that users see old data, lane by lane, and
# reminds every four hours. A failed unit alone said only "serving-monitor 실패": 2026-10-28..11-05
# the 공매 lane served a pre-photo manifest for 191 hours among some thirty open notices.
METRICS_DIR="${FOUNDATION_BY_PNU_SERVING_MONITOR_METRICS_DIR:-/var/lib/foundation-jobs/metrics}"
report="$(mktemp)"
trap 'rm -f "${report}"' EXIT
export "${PREFIX}_MONITOR_REPORT_PATH=${report}"
rc=0
"${PUBLISHER_BIN}" "monitor-${LANE}-by-pnu-serving" || rc=$?
python3 -I -B "$(dirname "${BASH_SOURCE[0]}")/serving_freshness_metrics.py" "${report}" "${LANE}" "${METRICS_DIR}" \
  || echo "by-pnu-serving-monitor ${LANE}: freshness metrics not written" >&2 # never fails the monitor
exit "${rc}"
