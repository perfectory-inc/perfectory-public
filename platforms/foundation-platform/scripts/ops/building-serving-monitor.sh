#!/usr/bin/env bash
# The hourly synthetic read of the live building hostname (root ADR-0151 Revision, contract
# building_by_pnu_gateway.section_packs.monitor); run by foundation-building-serving-monitor.service.
#
# Reads the first monitor.pnus PNUs of the gate sample (FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_
# MONITOR_SAMPLE_PATH, the equality evidence on this host) from the live hostname and holds each to
# the document the lane serves and, while they exist, to the object document. Read-only. A breach
# exits non-zero, and the unit's OnFailure reports it to Slack.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
export FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_OUTPUT_STORAGE_DRIVER=r2
exec "${PUBLISHER_BIN}" monitor-building-by-pnu-serving
