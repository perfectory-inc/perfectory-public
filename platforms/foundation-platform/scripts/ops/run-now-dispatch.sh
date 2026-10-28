#!/usr/bin/env bash
# Starts each operator's `run_now` request once (root ADR-0282 §4): foundation-api writes
# <switches>/run-now.d/<request id> (job, who, why, expires_at); foundation-run-now.path starts this
# when the directory changes.
#
# For an enabled job the job's DAG is triggered with conf {"operator_request": "<id>"}: the run goes
# through Airflow like a scheduled one, so what it changes starts what reads it (root ADR-0171), and
# the gate checks and consumes the request when the run starts (stage c). A request is triggered at
# most once (a mark here, never in the switch directory, which only foundation-api writes) and never
# after it expired. A disabled job (its DAG paused) starts through the gate directly. It also ends
# a run an operator stopped (below).
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
mkdir -p "${marks}/triggered" "${marks}/expired" "${marks}/stopped"

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
    no)
      # A job that is off has its DAG paused, so it starts through the gate itself, as the scheduler
      # does over SSH (the same checks, waits and single use), in a unit of its own: not named
      # foundation-*, which the autodeploy waits for (foundation-autodeploy.sh).
      if systemd-run --quiet --collect --unit "operator-run-now-${id}" -p User=foundation-scheduler \
        -E SSH_ORIGINAL_COMMAND="${job} requested ${id}" \
        "${release_root}/current/scripts/ops/start-scheduled-job.sh"; then
        : >"${marks}/triggered/${id}"
        log "${id}: ${job} is off; started through the gate (operator-run-now-${id})"
      else
        log "${id}: ${job} is off and its gate could not be started; tried again on the next change"
      fi
      ;;
    *) log "${id}: ${job} is not in the job list; skipped" ;;
  esac
done

# An operator's stop (root ADR-0282 §5): <switches>/<job>.stop ends the job's run in progress, once,
# and only within ten minutes of the request, so a stop handled late never ends a later run. The
# mark names the invocation it stopped and who asked, before the unit is stopped: the gate reports
# that run as `failed stopped by <who>`. A stop when nothing runs is marked and does nothing.
for request in "${switches}"/*.stop; do
  job="${request##*/}" job="${job%.stop}"
  [[ "${job}" =~ ^[a-z][a-z0-9_]*$ ]] || continue
  id="$(sed -n 's/^request=//p' "${request}" | head -n 1)"
  [[ "${id}" =~ ${UUID} && ! -e "${marks}/stopped/${id}" ]] || continue
  actor="$(sed -n 's/^actor=//p' "${request}" | head -n 1)"
  stamp="$(sed -n '/^requested_at=/{s///p;q}' "${request}")"
  asked="$(date -d "${stamp}" +%s 2>/dev/null)" || asked=0
  if (($(date +%s) - asked > 600)); then
    printf 'invocation=\nactor=%s\nlate\n' "${actor}" >"${marks}/stopped/${id}"
    log "${id}: the stop of ${job} was asked more than 10 minutes ago; nothing stopped"
    continue
  fi
  service="$(python3 -I - "${jobs_file}" "${job}" <<'PY'
import json, sys
for job in json.load(open(sys.argv[1], encoding="utf-8"))["jobs"]:
    if job["id"] == sys.argv[2]:
        print(job["systemd_service"])
PY
)"
  [[ -n "${service}" ]] || { log "${id}: ${job} is not in the job list; skipped"; continue; }
  running="$(python3 -I -B "${release_root}/current/scripts/deploy/job_units.py" running "${service}")" \
    || { log "${id}: systemd cannot tell whether ${service} runs; tried again on the next change"; continue; }
  if [[ -z "${running}" ]]; then
    printf 'invocation=\nactor=%s\n' "${actor}" >"${marks}/stopped/${id}"
    log "${id}: ${job} is not running; nothing stopped"
    continue
  fi
  invocation="$(systemctl show -p InvocationID --value "${service}")"
  printf 'invocation=%s\nactor=%s\n' "${invocation}" "${actor}" >"${marks}/stopped/${id}"
  systemctl stop --no-block "${service}"
  log "${id}: stopping ${service} (invocation ${invocation}) for ${actor}"
done
