#!/usr/bin/env bash
# The photo ladder backlog (root ADR-0429): the display ladders a collector run left, made from the
# Silver photo indexes the shared media store's contract names. The registered job
# media_ladder_backlog runs it hourly. It writes only write-once rendition objects; the pending
# sources and the marks of the indexes read are kept under its state directory.
set -Eeuo pipefail
shopt -s inherit_errexit
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

required_env=(
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET
  FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY
)
missing=()
for name in "${required_env[@]}"; do
  [ -n "${!name:-}" ] || missing+=("${name}")
done
if [ "${#missing[@]}" -gt 0 ]; then
  echo "media ladder backlog: refused before any side effect: missing ${missing[*]}" >&2
  exit 78 # EX_CONFIG
fi

STATE_ROOT="${FOUNDATION_MEDIA_LADDER_BACKLOG_STATE_ROOT:-/var/lib/foundation-platform/media-ladder-backlog}"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}" "${STATE_ROOT}/state"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "media ladder backlog FAILED at line ${LINENO} run=${run_id}" >&2; job_run_log_tail "${run_log}"' ERR

env FOUNDATION_PLATFORM_MEDIA_LADDER_BACKLOG_STATE_DIR="${STATE_ROOT}/state" \
  "${PUBLISHER_BIN}" media-ladder-backlog >> "${run_log}" 2>&1
summary="$(grep '^media-ladder-backlog-summary-json ' "${run_log}" | tail -1)"
job_journal "${journal}" "media ladder backlog ok run=${run_id} ${summary#media-ladder-backlog-summary-json }"
outcome="$(grep '^foundation-job-outcome ' "${run_log}" | tail -1)"
printf '%s\n' "${outcome:-foundation-job-outcome unchanged}"
