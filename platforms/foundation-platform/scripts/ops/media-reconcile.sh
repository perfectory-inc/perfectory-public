#!/usr/bin/env bash
# The media reconciler (root ADR-0387): the onbid media in R2 against the Silver index that names it.
# The registered job media_reconcile runs it daily. Read-only: it writes no object and no row.
#
# The index keys of the data files added since the last run are read first (the publisher's
# media-index-delta, from the lakehouse itself, not Trino) and the prefix listed after, so a key the
# index names that the listing lacks is missing for certain (the collector puts a blob before the line
# that names it). The kept reference set, the comparison, the report and the node-exporter gauges are
# media_reconcile.py's.
set -Eeuo pipefail
shopt -s inherit_errexit
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

STATE_ROOT="${FOUNDATION_MEDIA_RECONCILE_STATE_ROOT:-/var/lib/foundation-platform/media-reconcile}"
METRICS="/var/lib/foundation-jobs/metrics/media-reconcile.prom"
RECONCILE="${RELEASE_ROOT}/scripts/ops/media_reconcile.py"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "media reconcile FAILED at line ${LINENO} run=${run_id}" >&2; job_run_log_tail "${run_log}"' ERR
# The index delta and the listing are inputs of this run only; the report under reports/ and the
# reference set (referenced.json) are what is kept.
trap 'rm -f "${work}/index.json" "${work}/listing.jsonl"' EXIT

env FOUNDATION_PLATFORM_MEDIA_INDEX_DELTA_STATE="${STATE_ROOT}/referenced.json" \
  FOUNDATION_PLATFORM_MEDIA_INDEX_DELTA_OUTPUT="${work}/index.json" \
  "${PUBLISHER_BIN}" media-index-delta >> "${run_log}" 2>&1
prefix="$(python3 -I -c 'import sys; sys.path.insert(0, sys.argv[1]); import media_reconcile; print(media_reconcile.settings()["prefix"])' "${RELEASE_ROOT}/scripts/ops")"
env FOUNDATION_PLATFORM_LAKEHOUSE_OBJECT_LISTING_PREFIX="${prefix}" \
  FOUNDATION_PLATFORM_LAKEHOUSE_OBJECT_LISTING_OUTPUT="${work}/listing.jsonl" \
  "${PUBLISHER_BIN}" list-lakehouse-objects >> "${run_log}" 2>&1
python3 -I "${RECONCILE}" compare "${STATE_ROOT}" "${work}/index.json" "${work}/listing.jsonl" "${STATE_ROOT}/reports" "${METRICS}" \
  > "${work}/summary.txt" 2>> "${run_log}"
job_journal "${journal}" "$(cat "${work}/summary.txt") run=${run_id}"
printf 'foundation-job-outcome unchanged\n'
