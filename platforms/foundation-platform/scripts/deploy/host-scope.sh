#!/usr/bin/env bash
# Sourced by foundation-release.sh (it provides release_root). What the release does under the host's
# host_scope (orchestration/jobs.v1.json, root ADR-0625): `all` runs every job; `collection` runs only
# collection and what it needs, as orchestration/dags/job_specs.py runs_on_host computes it. The forced
# command, the scheduler's sudoers, Dagster's sensors and the deploy's lists read the same answer.
# shellcheck disable=SC2154 # release_root is set by foundation-release.sh, which sources this file

# What this host runs (jobs.v1.json `host_scope`, root ADR-0625), as the release's job_specs.py reads it.
host_scope_of_release() {
  python3 -I -B -c 'import json, sys
sys.path.insert(0, sys.argv[2])
from job_specs import host_scope
print(host_scope(json.load(open(sys.argv[1], encoding="utf-8"))))' \
    "${release_root}/current/orchestration/jobs.v1.json" "${release_root}/current/orchestration/dags"
}

# The services (and check services) of every registered job this host does not run under its host_scope
# (job_specs.on_host, root ADR-0625), one per line: an enabled job its scope leaves out, and a job that is off.
off_host_services() {
  python3 -I -B -c 'import sys
sys.path.insert(0, sys.argv[2])
from job_specs import on_host_file
for job in on_host_file(sys.argv[1])["jobs"]:
    if job.get("host_scope_off"):
        for service in filter(None, (job["systemd_service"], job.get("check_service"))):
            print(service)' "${release_root}/current/orchestration/jobs.v1.json" "${release_root}/current/orchestration/dags"
}

# The services of every job the host_scope leaves out (off_host_services): a run of one is stopped, its
# failed state cleared, and `off_host` names them for the retries `timers` would start.
declare -gA off_host=()
stop_off_host_jobs() {
  local service
  # A job the host_scope leaves out runs no more here (root ADR-0625): a run of it is stopped (a run keeps
  # nothing it has not committed, and Bronze lets any host make its output again) and its failed state is
  # cleared, since nothing here runs it again. Its unit file stays: `all` brings it back in one release.
  while read -r service; do
    [[ -n "${service}" ]] || continue
    off_host["${service}"]=1
    if [[ -n "$(python3 -I -B "${release_root}/current/scripts/deploy/job_units.py" running "${service}")" ]]; then
      systemctl stop "${service}" || true
      printf 'foundation-release: stopped %s; host_scope %s does not run it (root ADR-0625)\n' \
        "${service}" "$(host_scope_of_release)"
    fi
    systemctl reset-failed "${service}" 2>/dev/null || true
  done < <(off_host_services)
}

# The by-PNU controller, probes and serving monitors run where derived compute does.
derived_host_timers() {
  local timer
  local -a derived_timers monitors
  # What serves and probes the derived by-PNU data runs where derived compute does (root ADR-0625): the
  # edge probe (ADR-0327), the weekly synthetic screen (ADR-0480), the controller that publishes a lane
  # whose Gold is ready (ADR-0435) and each lane's hourly serving monitor (worker_autodeploy.py). Under
  # host_scope collection they are switched off, their unit files kept.
  derived_timers=(foundation-by-pnu-edge-probe.timer foundation-by-pnu-screen-probe.timer foundation-by-pnu-auto-promote.timer)
  if [[ "$(host_scope_of_release)" == all ]]; then
    systemctl enable --now "${derived_timers[@]}"
  else
    # Each declared lane's monitor (config/by-pnu-lane-index.v1.json), named, not asked of systemd.
    mapfile -t monitors < <(python3 -I -c 'import json, sys
for lane in json.load(open(sys.argv[1], encoding="utf-8"))["lanes"]:
  print("foundation-by-pnu-serving-monitor@" + lane["lane"] + ".timer")' \
      "${release_root}/current/config/by-pnu-lane-index.v1.json")
    for timer in "${derived_timers[@]}" "${monitors[@]}"; do
      systemctl disable --now "${timer}" 2>/dev/null || true
    done
    printf 'foundation-release: host_scope %s: the by-PNU controller, probes and %s serving monitors are off (root ADR-0625)\n' \
      "$(host_scope_of_release)" "${#monitors[@]}"
  fi
}

# DataHub: true when the host_scope runs no derived compute and its running containers were stopped.
datahub_stopped_by_scope() {
  local -a running
  # Nothing reads the catalog any more (root ADR-0609), and a host that runs no derived compute keeps no
  # catalog of it (root ADR-0625): its containers are stopped, never removed; removing them is ADR-0609's.
  [[ "$(host_scope_of_release)" != all ]] || return 1
  mapfile -t running < <(docker ps -q --filter label=com.docker.compose.project=datahub)
  ((${#running[@]} == 0)) || docker stop "${running[@]}" >/dev/null
  printf 'datahub: host_scope %s: %s running containers stopped, none removed (root ADR-0625)\n' \
    "$(host_scope_of_release)" "${#running[@]}"
  return 0
}
