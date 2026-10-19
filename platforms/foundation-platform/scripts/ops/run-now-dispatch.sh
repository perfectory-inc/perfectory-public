#!/usr/bin/env bash
# Starts each operator's `run_now` request once (root ADR-0282 §4): foundation-api writes
# <switches>/run-now.d/<request id> (job, who, why, expires_at); foundation-run-now.path starts this
# when the directory changes.
#
# For an enabled job the job's DAG is triggered with conf {"operator_request": "<id>"}: the run goes
# through Airflow like a scheduled one, so what it changes starts what reads it (root ADR-0171), and
# the gate checks and consumes the request when the run starts (stage c). A request is triggered at
# most once (a mark here, never in the switch directory, which only foundation-api writes) and never
# after it expired. A disabled job's request waits for stage c, which starts it through the gate.
set -euo pipefail

# Fixed paths, no environment override: a caller cannot appoint what is started.
switches=/var/lib/foundation-jobs/switches
marks=/var/lib/foundation-run-now
release_root=/opt/foundation-platform
host_conf=/etc/foundation-platform/release-deploy.conf

log() { printf 'run-now: %s\n' "$*"; }
UUID='^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

deployer="$(sed -n 's/^FOUNDATION_DEPLOYER=\([a-z_][a-z0-9_-]*\)$/\1/p' "${host_conf}" | tail -n 1)"
[[ -n "${deployer}" ]] || { log "${host_conf} names no FOUNDATION_DEPLOYER"; exit 78; }
jobs_file="${release_root}/current/orchestration/jobs.v1.json"
mkdir -p "${marks}/triggered" "${marks}/expired"

shopt -s nullglob
for request in "${switches}/run-now.d"/*; do
  id="${request##*/}"
  [[ "${id}" =~ ${UUID} ]] || continue # a partial write or anything else
  [[ ! -e "${marks}/triggered/${id}" && ! -e "${marks}/expired/${id}" ]] || continue
  job="$(sed -n 's/^job=\([a-z][a-z0-9_]*\)$/\1/p' "${request}" | head -n 1)"
  expires_at="$(sed -n 's/^expires_at=//p' "${request}" | head -n 1)"
  [[ -n "${job}" && -n "${expires_at}" ]] || { log "${id}: no job or expiry; skipped"; continue; }
  expires="$(date -d "${expires_at}" +%s 2>/dev/null)" || { log "${id}: expiry ${expires_at} unreadable; skipped"; continue; }
  if (($(date +%s) >= expires)); then
    : >"${marks}/expired/${id}"
    log "${id}: ${job} expired at ${expires_at}; not started"
    continue
  fi
  enabled="$(python3 -I - "${jobs_file}" "${job}" <<'PY'
import json, sys
for job in json.load(open(sys.argv[1], encoding="utf-8"))["jobs"]:
    if job["id"] == sys.argv[2]:
        print("yes" if job["enabled"] else "no")
PY
)"
  case "${enabled}" in
    yes)
      conf="{\"operator_request\": \"${id}\"}"
      if sudo -u "${deployer}" -H bash "${release_root}/current/scripts/deploy/airflow-runtime.sh" \
        exec airflow-scheduler airflow dags trigger "foundation_${job}" --conf "${conf}" >/dev/null; then
        : >"${marks}/triggered/${id}"
        log "${id}: triggered foundation_${job}"
      else
        log "${id}: foundation_${job} could not be triggered; tried again on the next change"
      fi
      ;;
    no) log "${id}: ${job} is off; its run-now starts through the gate (ADR-0282 stage c)" ;;
    *) log "${id}: ${job} is not in the job list; skipped" ;;
  esac
done
