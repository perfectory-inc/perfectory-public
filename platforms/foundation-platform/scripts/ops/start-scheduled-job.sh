#!/usr/bin/env bash
# The only command Airflow's SSH key can run on this host (root ADR-0122 §2).
#
# sshd runs it as `foundation-scheduler` whatever the client asked for; the client's words arrive
# in SSH_ORIGINAL_COMMAND and are read as one job id, nothing more. A job that is not in the job
# list, or not switched on there, is refused. The job's systemd service is started, its journal
# for this run is streamed back, and the exit status says whether systemd recorded success. If the
# connection drops (Airflow timed out or was stopped), the service is stopped with it.
set -uo pipefail

jobs_file="${FOUNDATION_SCHEDULED_JOBS_FILE:-/opt/foundation-platform/current/orchestration/jobs.v1.json}"
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
