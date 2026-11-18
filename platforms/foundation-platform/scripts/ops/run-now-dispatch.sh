#!/usr/bin/env bash
# Starts each operator's `run_now` request once (root ADR-0282 §4): foundation-api writes
# <switches>/run-now.d/<request id> (job, who, why, expires_at); foundation-run-now.path starts this
# when the directory changes.
#
# For an enabled job the job's DAG is triggered with conf {"operator_request": "<id>"}: the run goes
# through Airflow like a scheduled one, so what it changes starts what reads it (root ADR-0171), and
# the gate checks and consumes the request when the run starts (stage c). A request is triggered at
# most once (a mark here, never in the switch directory, which only foundation-api writes) and never
# after it expired. A job Dagster decides has no DAG (`dagster_stage: dagster`, root ADR-0565): its Dagster
# job is launched with the same request (scripts/deploy/job_orchestrator.py), and the run reaches the
# same gate with `requested <id>`. A disabled job (its DAG paused) starts through the gate directly. It also starts
# an approved by-PNU cut-over (root ADR-0433, ADR-0435) and ends a run an operator stopped (below).
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
  job="$(sed -n 's/^job=\([a-z][a-z0-9_]*\)$/\1/p' "${request}" | sed -n 1p)"
  expires_at="$(sed -n 's/^expires_at=//p' "${request}" | sed -n 1p)"
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
  # Which jobs Dagster decides is job_orchestrator.py's answer (root ADR-0565).
  orchestrator="${release_root}/current/scripts/deploy/job_orchestrator.py"
  if [[ "${enabled}" == yes && -f "${orchestrator}" ]] \
    && grep -qx "${job}" < <(python3 -I -B "${orchestrator}" decided "${jobs_file}"); then
    enabled=dagster
  fi
  case "${enabled}" in
    dagster)
      if python3 -I -B "${release_root}/current/scripts/deploy/job_orchestrator.py" start "${jobs_file}" \
        "${job}" requested "${id}" >/dev/null; then
        : >"${marks}/triggered/${id}"
        log "${id}: started ${job} in Dagster"
      else
        log "${id}: ${job} could not be started in Dagster; tried again on the next change"
      fi
      ;;
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

# An approved by-PNU cut-over (root ADR-0433 §2, ADR-0435): foundation-api writes <switches>/cutover.d/<id>
# (lanes, generation, who, why, expires_at). A lane's first publish needs no approval any more (the
# auto-promotion starts it, by-pnu-auto-promote.sh); an approval is the exceptional act, a lane cut over
# again at a new generation. Each is started once, at once, through the operator script's
# `cutover <generation> --approved <id>`, which checks it again and marks it used. It waits for nothing
# here and for no time of day: the run claims the spark pool and waits for capacity (root ADR-0435;
# 2026-11-06 03:46Z the operator window refused one). While another cut-over runs it is left for the next
# change, so the operator script's one-at-a-time refusal does not use it up. Each answer is a line of
# the approval's progress (release-progress.py), which the console shows.
used="${marks}/cutover-used"
mkdir -p "${marks}/cutover" "${used}"
ops="${release_root}/current/scripts/ops"
progress() { python3 -I -B "${ops}/release-progress.py" "$@" || log "$1: could not append to its progress"; }

# An operator's stop (root ADR-0282 §5): <switches>/<job>.stop ends the job's run in progress, once,
# and only within ten minutes of the request, so a stop handled late never ends a later run. The
# mark names the invocation it stopped and who asked, before the unit is stopped: the gate reports
# that run as `failed stopped by <who>`. A stop when nothing runs is marked and does nothing.
for request in "${switches}"/*.stop; do
  job="${request##*/}" job="${job%.stop}"
  [[ "${job}" =~ ^[a-z][a-z0-9_]*$ ]] || continue
  id="$(sed -n 's/^request=//p' "${request}" | sed -n 1p)"
  [[ "${id}" =~ ${UUID} && ! -e "${marks}/stopped/${id}" ]] || continue
  actor="$(sed -n 's/^actor=//p' "${request}" | sed -n 1p)"
  stamp="$(sed -n '/^requested_at=/{s///p;q}' "${request}")"
  asked="$(date -d "${stamp}" +%s 2>/dev/null)" || asked=0
  if (($(date +%s) - asked > 600)); then
    printf 'invocation=\nactor=%s\nlate\n' "${actor}" >"${marks}/stopped/${id}"
    log "${id}: the stop of ${job} was asked more than 10 minutes ago; nothing stopped"
    continue
  fi
  if [[ "${job}" == by_pnu_lane_cutover ]]; then
    # The cut-over runner is not a job (root ADR-0433 §1): its stop cancels every approval asked
    # before it that has not started, and ends a cut-over run in progress. The step that run is in
    # finishes in its own unit; nothing after it starts (the runner says so in its progress).
    printf 'invocation=\nactor=%s\n' "${actor}" >"${marks}/stopped/${id}"
    why="$(sed -n 's/^reason=//p' "${request}" | sed -n 1p)"
    for approval in "${switches}/cutover.d"/*; do
      name="${approval##*/}"
      [[ "${name}" =~ ${UUID} && ! -e "${marks}/cutover/${name}" && ! -e "${used}/${name}" ]] || continue
      approved="$(date -d "$(sed -n '/^requested_at=/{s///p;q}' "${approval}")" +%s 2>/dev/null)" || approved=0
      ((approved <= asked)) || continue
      : >"${marks}/cutover/${name}"
      log "${name}: cancelled by ${actor}'s stop ${id}"
      progress "${name}" cancelled "reason=stopped by ${actor}: ${why}"
    done
    running="$(python3 -I -B "${release_root}/current/scripts/deploy/job_units.py" running 'foundation-by-pnu-lane-cutover-g*.service')" \
      || { log "${id}: systemd cannot tell whether a cut-over runs; tried again on the next change"; rm -f "${marks}/stopped/${id}"; continue; }
    while read -r unit _; do
      [[ -n "${unit}" ]] || continue
      systemctl stop --no-block "${unit}"
      log "${id}: stopping ${unit} for ${actor}"
    done <<<"${running}"
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

# The approvals last, after the stops: a stop cancels an approval before it could start.
for request in "${switches}/cutover.d"/*; do
  id="${request##*/}"
  [[ "${id}" =~ ${UUID} ]] || continue
  [[ ! -e "${marks}/cutover/${id}" ]] || continue # started, refused or cancelled
  unit="operator-cutover-${id}"
  lanes="$(sed -n 's/^lanes=\([a-z0-9-]\{1,\}\(,[a-z0-9-]\{1,\}\)*\)$/\1/p' "${request}" | sed -n 1p)"
  generation="$(sed -n 's/^generation=\([1-9][0-9]\{0,3\}\)$/\1/p' "${request}" | sed -n 1p)"
  expires_at="$(sed -n 's/^expires_at=//p' "${request}" | sed -n 1p)"
  refused() {
    : >"${marks}/cutover/${id}"
    log "${id}: ${lanes:-?} generation ${generation:-?} not started: $1"
    progress "${id}" refused "reason=$1"
  }
  expires="$(date -d "${expires_at}" +%s 2>/dev/null)" || expires=0
  if [[ -z "${lanes}" || -z "${generation}" || "$(sed -n 's/^job=//p' "${request}" | sed -n 1p)" != by_pnu_lane_cutover ]]; then
    refused "the request names no cut-over lanes and generation"
    continue
  fi
  (($(date +%s) < expires)) || { refused "the approval expired at ${expires_at}"; continue; }
  busy="$(python3 -I -B "${release_root}/current/scripts/deploy/job_units.py" running \
    'foundation-by-pnu-lane-cutover-g*.service' "operator-cutover-*.service")" \
    || { log "${id}: systemd cannot tell whether a cut-over runs; tried again on the next change"; continue; }
  if [[ -n "${busy}" ]]; then
    log "${id}: a cut-over runs ($(cut -d' ' -f1 <<<"${busy}" | paste -sd' ')); started on the next change"
    continue
  fi
  # Not named foundation-*, which the autodeploy waits for (foundation-autodeploy.sh); the cut-over
  # unit the operator script starts is.
  if systemd-run --quiet --collect --unit "${unit}" \
    "${ops}/by-pnu-pack-operator.sh" "${lanes}" cutover "${generation}" --approved "${id}"; then
    : >"${marks}/cutover/${id}"
    log "${id}: ${lanes} generation ${generation}: started (${unit}); it waits for the spark pool by its claim"
  else
    log "${id}: ${unit} could not be started; tried again on the next change"
  fi
done
