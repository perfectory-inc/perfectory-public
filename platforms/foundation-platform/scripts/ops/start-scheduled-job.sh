#!/usr/bin/env bash
# The only command Airflow's SSH key can run on this host (root ADR-0122 §2).
#
# sshd runs it as `foundation-scheduler` whatever the client asked for; the client's words arrive
# in SSH_ORIGINAL_COMMAND and are read as one job id, nothing more. A job that is not in the job
# list, or not switched on there, is refused. The job's systemd service is started, its journal
# for this run is streamed back, and the exit status says whether systemd recorded success.
#
# The run belongs to systemd, not to this connection (root ADR-0270 §3, deploy D2): a dropped
# connection (a deploy restarted Airflow, or its task timed out) leaves the service running, and
# the next try of the same job joins that run instead of starting a second one. Stopping a run is
# an operator's act, never a side effect of a connection.
#
# Every start is recorded under the scheduler's home. A `takes_turns` job (the multi-day by-PNU
# bake) is deferred — reported, exit 0, nothing started — when a job in its pool that cannot run
# beside it has not started since the deferred job last did. Airflow alone would let a light job
# take the free slot again and again while a heavier one waits for all of them (root ADR-0138).
#
# The service's journal ends with its `foundation-job-outcome` line: `changed`, `unchanged`,
# `waiting <reason>` or `failed <reason>`. The DAG reads it to decide whether the run's outputs
# start the jobs that read them (root ADR-0171); a run without it, or that says `failed`, is
# failed here even when systemd counted it a success (root ADR-0260).
set -uo pipefail

jobs_file="${FOUNDATION_SCHEDULED_JOBS_FILE:-/opt/foundation-platform/current/orchestration/jobs.v1.json}"
starts="${FOUNDATION_SCHEDULED_STARTS_DIR:-/var/lib/foundation-scheduler/starts}"
request="${SSH_ORIGINAL_COMMAND:-}"
# `<job> check` starts the job's check service instead (jobs.v1.json `check_service`, root
# ADR-0277): it only decides whether the run has work, so it is never deferred, never recorded as
# the job's start, and its last `foundation-job-check` line is repeated for the DAG.
checking=false
[[ "${request}" == *" check" ]] && checking=true && request="${request% check}"
# `<job>` from a clock start, `<job> triggered` from a run its inputs or an operator started
# (root ADR-0179): only a clock start can be deferred by takes_turns.
job="${request% triggered}"
triggered=false
[[ "${request}" == "${job} triggered" ]] && triggered=true

refuse() {
  printf 'start-scheduled-job: refused: %s\n' "$*" >&2
  exit 64
}

[[ "${checking}" == true && "${triggered}" == true ]] && refuse "a check is not triggered"
[[ "${job}" =~ ^[a-z][a-z0-9_]*$ ]] || refuse "expected one job id, got '${job:0:40}'"
service="$(python3 - "${jobs_file}" "${job}" "${checking}" <<'PY'
import json, sys
for entry in json.load(open(sys.argv[1]))["jobs"]:
    if entry["id"] == sys.argv[2] and entry["enabled"]:
        print((entry.get("check_service") or "") if sys.argv[3] == "true" else entry["systemd_service"])
PY
)" || refuse "cannot read ${jobs_file}"
[[ -n "${service}" ]] || refuse "${job} is not an enabled job in ${jobs_file}$([[ "${checking}" == true ]] && echo ' with a check_service')"

mkdir -p "${starts}" || refuse "cannot keep the start record ${starts}"
# A check is not a turn: it is passed as `triggered`, which is never deferred.
turn="$(python3 - "${jobs_file}" "${job}" "${starts}" "$([[ "${checking}" == true ]] && echo true || echo "${triggered}")" <<'PY'
import json, pathlib, sys
listing, job_id, starts = json.load(open(sys.argv[1])), sys.argv[2], pathlib.Path(sys.argv[3])
jobs = {job["id"]: job for job in listing["jobs"]}
job = jobs[job_id]
if not job.get("takes_turns") or sys.argv[4] == "true":
    sys.exit(0)  # not a turn-taker, or started by its inputs or an operator: never deferred
def started(job_id):
    path = starts / job_id
    return int(path.read_text()) if path.is_file() else None
mine = started(job_id)
if mine is None:
    sys.exit(0)  # never started here: its first turn
slots = listing["pools"][job["pool"]]["slots"]
waiting = sorted(
    other["id"] for other in jobs.values()
    if other["id"] != job_id and other["enabled"] and other["pool"] == job["pool"]
    and other.get("pool_slots", 1) + job.get("pool_slots", 1) > slots
    and (started(other["id"]) or 0) < mine
)
if waiting:
    print(f"deferred: {', '.join(waiting)} cannot run beside {job_id} and have not started since its last start")
PY
)" || refuse "cannot read the start record ${starts}"
if [[ -n "${turn}" ]]; then
  printf 'start-scheduled-job: %s %s\n' "${job}" "${turn}"
  # Nothing ran, so nothing changed: no output events, nothing downstream starts (root ADR-0171).
  # The job's own fallback schedule starts it again.
  printf 'foundation-job-outcome unchanged\n'
  exit 0
fi

# A release build runs alone (root ADR-0137): every job's ExecStartPre refuses while it holds its
# lock, and a refused unit is a failed unit with a failure alert. A job that comes due during a
# deploy's build is a designed wait, not a failure (root ADR-0260): it is not started, its run says
# so, and the deploy that holds the build starts it once the schedule is back
# (deploy-start-once.sh). Admission owns the lock; it answers 75 while a build holds it. Any other
# answer starts the job, whose ExecStartPre still refuses a build this question missed.
admission="${FOUNDATION_RELEASE_ADMISSION:-/opt/perfectory-control/current/scripts/deploy/foundation-release-admission.py}"
# Which units run, and where a deferral is recorded, are job_units.py's answers, from this script's
# release (root ADR-0184, ADR-0260).
job_units="$(dirname "${BASH_SOURCE[0]}")/../deploy/job_units.py"
deferred="$(python3 -I -B "${job_units}" deferred-dir)" || refuse "cannot read where deferrals are recorded"
build=0
"${admission}" release-build-running || build=$?
if ((build == 75)) && [[ "${checking}" == true ]]; then
  # The check's own ExecStartPre would refuse and alert. No answer lets the run start, and the run
  # is the one deferred and recorded.
  printf 'start-scheduled-job: %s check not started: a release build is running\n' "${job}"
  exit 0
fi
if ((build == 75)); then
  mkdir -p "${deferred}" && date +%s >"${deferred}/${job}" ||
    printf 'start-scheduled-job: cannot record the deferral of %s under %s\n' "${job}" "${deferred}" >&2
  printf 'start-scheduled-job: %s deferred: a release build is running\n' "${job}"
  printf 'foundation-job-outcome waiting a release build is running; this job starts after it finishes\n'
  exit 0
fi

follower=""
# A lost connection ends only this follower; the service keeps running (see the header).
detach() {
  [[ -n "${follower}" ]] && kill "${follower}" 2>/dev/null
  exit 143
}
trap detach HUP INT TERM

state() { systemctl show -p "$1" --value "${service}"; }
# A systemd that does not answer whether the service runs is not a run that ended: ask again.
runs() {
  local answer
  answer="$(python3 -I -B "${job_units}" running "${service}")" || return 0
  [[ -n "${answer}" ]]
}

# A run already in progress (started by hand, or a retry racing the last one) is joined, not doubled.
before="$(state InvocationID)"
sudo -n /usr/bin/systemctl start --no-block "${service}" || refuse "systemd would not start ${service}"
invocation=""
for _ in $(seq 1 60); do
  invocation="$(state InvocationID)"
  [[ -n "${invocation}" && "${invocation}" != "${before}" ]] && break
  # The same invocation still running is the run in progress, joined.
  [[ -n "${invocation}" ]] && runs && break
  sleep 1
done
[[ -n "${invocation}" ]] || refuse "${service} reported no invocation"
if [[ "${checking}" == false ]]; then
  date +%s >"${starts}/${job}.next" && mv "${starts}/${job}.next" "${starts}/${job}"
  rm -f "${deferred}/${job}" # it runs now: nothing is owed to it
fi
printf 'start-scheduled-job: %s invocation=%s\n' "${service}" "${invocation}"

# A check's journal is not streamed: the DAG reads its answer only from the line repeated below,
# after systemd said the check succeeded.
if [[ "${checking}" == false ]]; then
  journalctl --no-pager -o cat -f "_SYSTEMD_INVOCATION_ID=${invocation}" &
  follower=$!
fi
while runs && [[ "$(state InvocationID)" == "${invocation}" ]]; do
  sleep "$([[ "${checking}" == true ]] && echo 1 || echo 5)"
done
if [[ -n "${follower}" ]]; then
  sleep 2 # let the journal follower print the last lines
  kill "${follower}" 2>/dev/null
  wait "${follower}" 2>/dev/null
fi

result="$(state Result)"
if [[ "${checking}" == true ]]; then
  # The check's answer, read from its own journal; a check that failed gives none.
  [[ "${result}" == success ]] && journalctl --no-pager -o cat -n 200 "_SYSTEMD_INVOCATION_ID=${invocation}" \
    | grep -E '^foundation-job-check [^ ]+$' | tail -n 1
  printf 'start-scheduled-job: %s result=%s\n' "${service}" "${result}"
  exit 0
fi
# The run says how it ended (root ADR-0260 §2): its last outcome line, read from this invocation's
# own journal, not from what the follower happened to print. A run systemd counts as a success
# that said nothing, or said `failed`, is a failed run; a run systemd counts as failed ends with a
# `failed` line of this script's whatever it said. The line decided on is repeated before the
# result line, inside the frame the data flow map reads a failure's cause from.
verdict=0
job_specs="$(dirname "${BASH_SOURCE[0]}")/../../orchestration/dags/job_specs.py"
outcome="$(journalctl --no-pager -o cat -n 2000 "_SYSTEMD_INVOCATION_ID=${invocation}" \
  | python3 -I -B "${job_specs}" --outcome-line)" || outcome=""
if [[ "${result}" != success ]]; then
  # A failed unit's last word is `failed`, whatever the run printed before it ended (2026-10-16:
  # the by-PNU bake said `changed` for the lane that published and exited 1 for the one that did
  # not). A run that claimed another outcome is named, so the contradiction is not lost.
  claimed="${outcome#foundation-job-outcome }"
  if [[ -n "${outcome}" && "${claimed}" != failed* ]]; then
    printf 'foundation-job-outcome failed systemd result=%s after the run said %s\n' "${result}" "${claimed}"
  else
    printf 'foundation-job-outcome failed systemd result=%s\n' "${result}"
  fi
  verdict=1
else
  if [[ -z "${outcome}" ]]; then
    outcome="foundation-job-outcome failed no outcome reported"
    verdict=1
  elif [[ "${outcome}" == "foundation-job-outcome failed "* ]]; then
    verdict=1
  fi
  printf '%s\n' "${outcome}"
fi
printf 'start-scheduled-job: %s result=%s\n' "${service}" "${result}"
exit "${verdict}"
