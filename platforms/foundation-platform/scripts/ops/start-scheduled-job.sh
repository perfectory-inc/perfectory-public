#!/usr/bin/env bash
# The only command Airflow's SSH key can run on this host (root ADR-0122 §2).
#
# sshd runs it as `foundation-scheduler` whatever the client asked for; the client's words arrive
# in SSH_ORIGINAL_COMMAND and are read as one job id, nothing more. A job that is not in the job
# list, or not switched on there, is refused. The job's systemd service is started, its journal
# for this run is streamed back, and the exit status says whether systemd recorded success. If the
# connection drops (Airflow timed out or was stopped), the service is stopped with it.
#
# Every start is recorded under the scheduler's home. A `takes_turns` job (the multi-day by-PNU
# bake) is deferred — reported, exit 0, nothing started — when a job in its pool that cannot run
# beside it has not started since the deferred job last did. Airflow alone would let a light job
# take the free slot again and again while a heavier one waits for all of them (root ADR-0138).
set -uo pipefail

jobs_file="${FOUNDATION_SCHEDULED_JOBS_FILE:-/opt/foundation-platform/current/orchestration/jobs.v1.json}"
starts="${FOUNDATION_SCHEDULED_STARTS_DIR:-/var/lib/foundation-scheduler/starts}"
job="${SSH_ORIGINAL_COMMAND:-}"

refuse() {
  printf 'start-scheduled-job: refused: %s\n' "$*" >&2
  exit 64
}

[[ "${job}" =~ ^[a-z][a-z0-9_]*$ ]] || refuse "expected one job id, got '${job:0:40}'"
service="$(python3 - "${jobs_file}" "${job}" <<'PY'
import json, sys
for entry in json.load(open(sys.argv[1]))["jobs"]:
    if entry["id"] == sys.argv[2] and entry["enabled"]:
        print(entry["systemd_service"])
PY
)" || refuse "cannot read ${jobs_file}"
[[ -n "${service}" ]] || refuse "${job} is not an enabled job in ${jobs_file}"

mkdir -p "${starts}" || refuse "cannot keep the start record ${starts}"
turn="$(python3 - "${jobs_file}" "${job}" "${starts}" <<'PY'
import json, pathlib, sys
listing, job_id, starts = json.load(open(sys.argv[1])), sys.argv[2], pathlib.Path(sys.argv[3])
jobs = {job["id"]: job for job in listing["jobs"]}
job = jobs[job_id]
if not job.get("takes_turns"):
    sys.exit(0)
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
  exit 0
fi

follower=""
stop_job() {
  sudo -n /usr/bin/systemctl stop "${service}" || true
  [[ -n "${follower}" ]] && kill "${follower}" 2>/dev/null
  exit 143
}
trap stop_job HUP INT TERM

state() { systemctl show -p "$1" --value "${service}"; }

# A run already in progress (started by hand, or a retry racing the last one) is joined, not doubled.
before="$(state InvocationID)"
sudo -n /usr/bin/systemctl start --no-block "${service}" || refuse "systemd would not start ${service}"
invocation=""
for _ in $(seq 1 60); do
  invocation="$(state InvocationID)"
  [[ -n "${invocation}" && ( "${invocation}" != "${before}" || "$(state ActiveState)" == activating ) ]] && break
  sleep 1
done
[[ -n "${invocation}" ]] || refuse "${service} reported no invocation"
date +%s >"${starts}/${job}.next" && mv "${starts}/${job}.next" "${starts}/${job}"
printf 'start-scheduled-job: %s invocation=%s\n' "${service}" "${invocation}"

journalctl --no-pager -o cat -f "_SYSTEMD_INVOCATION_ID=${invocation}" &
follower=$!
while [[ "$(state ActiveState)" =~ ^(activating|active|deactivating)$ && "$(state InvocationID)" == "${invocation}" ]]; do
  sleep 5
done
sleep 2 # let the journal follower print the last lines
kill "${follower}" 2>/dev/null
wait "${follower}" 2>/dev/null

result="$(state Result)"
printf 'start-scheduled-job: %s result=%s\n' "${service}" "${result}"
[[ "${result}" == success ]]
