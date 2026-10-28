#!/usr/bin/env bash
# Deploys one commit of main to this host, end to end (root ADR-0159): the steps an operator ran by
# hand from a copy outside the repository until 2026-10-07. foundation-autodeploy.sh runs it for
# each new main commit whose checks passed; an operator may run it the same way.
#
#   foundation-deploy.sh <40-hex commit on main>
#
# Root only. Every job list comes from orchestration/jobs.v1.json of the release being replaced:
# the DAGs paused and the services waited for are those of its jobs the new release holds
# (release_changes.py, root ADR-0308), and the jobs started once under the new
# release are those it marks `started_once_after_deploy`. Before the switch the new release runs the
# staging smoke (root ADR-0177); a smoke that fails refuses the release like any step before the
# switch. Stops at the first failure; every step is
# safe to re-run. A job that does not succeed after the switch does not fail the deploy or keep the
# DAGs paused (root ADR-0173): its unit's OnFailure alerts, the next scheduled run tries again, and
# the deploy's log names it. A deploy that stops before the release switch unpauses the DAGs again;
# one that stops during the switch leaves them paused and says so in capitals.
#
# The account that owns the Airflow runtime state (airflow-runtime.sh, $HOME/airflow-state) is the
# host's deployer; it is named in /etc/foundation-platform/release-deploy.conf as
# FOUNDATION_DEPLOYER=<account>, because it is a fact about the host, not about a release.
set -euo pipefail
# Every Python this deploy runs reads the release it installed, and some import from it (root
# ADR-0179's unpause order imports orchestration/dags). Run as root, Python would write __pycache__
# into that release, and release admission then refuses every job on it as "writable" (2026-10-10).
export PYTHONDONTWRITEBYTECODE=1
[[ ${EUID} == 0 ]] || { echo "foundation-deploy: run as root" >&2; exit 64; }
# Read now, before step 1 moves the control checkout this script runs from to the new commit.
source "$(dirname "${BASH_SOURCE[0]}")/deploy-start-once.sh"
sha="${1:-}"
[[ "${sha}" =~ ^[0-9a-f]{40}$ ]] || { echo "usage: foundation-deploy.sh <40-hex commit>" >&2; exit 64; }

host_conf=/etc/foundation-platform/release-deploy.conf
[[ -r "${host_conf}" ]] || { echo "foundation-deploy: ${host_conf} is missing (FOUNDATION_DEPLOYER=<account>)" >&2; exit 78; }
deployer="$(sed -n 's/^FOUNDATION_DEPLOYER=\([a-z_][a-z0-9_-]*\)$/\1/p' "${host_conf}" | tail -1)"
[[ -n "${deployer}" ]] && id "${deployer}" >/dev/null 2>&1 \
  || { echo "foundation-deploy: ${host_conf} names no existing FOUNDATION_DEPLOYER" >&2; exit 78; }
deployer_home="$(getent passwd "${deployer}" | cut -d: -f6)"

release_root=/opt/foundation-platform
control_root=/opt/perfectory-control
mirror=/var/lib/perfectory/control-source.git
# What this deploy holds, for the operator's starts (by-pnu-pack-operator.sh): its pid, then `*`
# from step 0 until the build is done, then the jobs its release holds until it is live (D5).
deploy_hold=/run/foundation-deploy-hold
# Where a deploy step records the inputs of its last successful run here (deploy_steps.py, root ADR-0308).
deploy_steps_state=/var/lib/foundation-platform/deploy-steps
identity="${control_root}/current/tools/github/repository-identity.json"
repository="https://github.com/$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["full_name"])' "${identity}").git"

log() { printf '\n==== %s\n' "$*"; }
as_deployer() { sudo -u "${deployer}" -H "$@"; }
airflow() { as_deployer bash "${release_root}/current/scripts/deploy/airflow-runtime.sh" exec airflow-scheduler airflow "$@"; }

# The lists, from the job registry of the release now running.
jobs_of() {
  python3 - "$1" "${release_root}/current/orchestration/jobs.v1.json" <<'PY'
import json, sys
which, path = sys.argv[1], sys.argv[2]
for job in json.load(open(path, encoding="utf-8"))["jobs"]:
    if which == "enabled" and job["enabled"]:
        print(job["id"])
    elif which == "disabled" and not job["enabled"]:
        print(job["id"])
    elif which == "units":
        # What step 0 stops or waits for (root ADR-0301, ADR-0308); a registry before ADR-0301 says
        # nothing, and a job that says nothing is waited for.
        print(job["id"], job["systemd_service"], "yes" if job.get("interruptible") is True else "no")
    elif which == "started-once" and job["enabled"] and job.get("started_once_after_deploy"):
        # One run's limit (root ADR-0270 §1), the sum job_specs.run_limit_minutes makes: the deploy
        # starts the unit itself, so it never waits for the host;
        # test_foundation_autodeploy holds the two to the same answer on the real list. one-off: the
        # list read is the release being replaced, and the one this change replaces still states
        # timeout_minutes. Drop that branch once the host runs a release with stop_grace_minutes.
        if "timeout_minutes" in job:
            print(job["systemd_service"], job["timeout_minutes"], job["id"])
        else:
            print(job["systemd_service"], job["time_limit_minutes"] + job["stop_grace_minutes"], job["id"])
if which == "enabled-consumers-first":
    # Airflow drops an output event for a DAG that is still paused (root ADR-0179): the order is the
    # job registry's own dependency graph, consumers first (orchestration/dags/job_specs.py).
    import os
    dags = os.path.join(os.path.dirname(path), "dags")
    if os.path.isdir(dags):
        sys.path.insert(0, dags)
        from job_specs import unpause_order
        print("\n".join(unpause_order()))
    else:
        print("\n".join(job["id"] for job in json.load(open(path, encoding="utf-8"))["jobs"] if job["enabled"]))
if which == "deferred":
    # The enabled jobs the scheduler's runner did not start because a release build was running
    # (root ADR-0260); job_units.py of the running release says where it recorded them.
    import os
    deploy = os.path.join(os.path.dirname(path), "..", "scripts", "deploy")
    if os.path.isfile(os.path.join(deploy, "job_units.py")):
        sys.path.insert(0, deploy)
        from job_units import deferred
        enabled = {job["id"] for job in json.load(open(path, encoding="utf-8"))["jobs"] if job["enabled"]}
        print("\n".join(job for job in deferred() if job in enabled))
PY
}
mapfile -t jobs < <(jobs_of enabled)
mapfile -t disabled < <(jobs_of disabled)
# Which registered jobs run is release admission's answer, the one its build refuses on (root
# ADR-0184): asked here before step 1 moves the control checkout, so the deploy waits on the list the
# checkout it came from admits by. 0 nothing runs, 75 something does (listed), anything else: no answer.
admission="${control_root}/current/scripts/deploy/foundation-release-admission.py"
old="$(readlink -f "${release_root}/current")"; old="${old##*/}"

log "pre-flight: the approval settings ${sha}'s pending migrations wait for are set (root ADR-0259 §4)"
# Before step 0: a migration that would refuse in step 4 stops the deploy here, with no DAG paused
# and no container recreated. On 2026-10-14 #465's refused after activate had recreated the API,
# which stayed down about 30 minutes. The checker and its contract are the commit's own.
/bin/bash "${control_root}/current/scripts/github/safe-git-transport.sh" --anonymous --repository "${mirror}" \
  fetch -q --no-tags "${repository}" +refs/heads/main:refs/heads/main
git --git-dir="${mirror}" merge-base --is-ancestor "${sha}" refs/heads/main
preflight="$(mktemp -d /var/lib/perfectory/preflight-XXXXXX)"
git --git-dir="${mirror}" archive "${sha}" scripts/guard/migration-approval-gates.py \
  tools/ci/migration-approval-gates.contract.json platforms/foundation-platform/migrations \
  platforms/foundation-platform/scripts/deploy/migration-approval-preflight.py | tar -x -C "${preflight}"
preflight_status=0
python3 "${preflight}/platforms/foundation-platform/scripts/deploy/migration-approval-preflight.py" "${preflight}" -- \
  "${release_root}/current/scripts/deploy/foundation-runtime.sh" exec -T postgres \
  sh -c 'psql -X -q -A -t -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' || preflight_status=$?
rm -rf "${preflight}"
((preflight_status == 0)) || exit "${preflight_status}"

log "pre-flight: what ${sha} changes and which jobs write it (root ADR-0270 §3 deploys D3, D5)"
# The answer names the jobs held until the release is live. A deploy that cannot tell holds every
# job, as before D5.
changes="$(mktemp -d /var/lib/perfectory/changes-XXXXXX)"
hold=("*")
git --git-dir="${mirror}" archive "${sha}" platforms/foundation-platform/scripts/deploy/release_changes.py \
  platforms/foundation-platform/orchestration/jobs.v1.json platforms/foundation-platform/docs/catalog/pipeline-graph.v1.json \
  platforms/foundation-platform/infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json \
  platforms/foundation-platform/migrations tools/release-build.contract.json | tar -x -C "${changes}" \
  && python3 -I -B "${changes}/platforms/foundation-platform/scripts/deploy/release_changes.py" \
    "${release_root}/current" "${changes}/platforms/foundation-platform" --hold-file "${changes}/hold" \
  && mapfile -t hold <"${changes}/hold" \
  || { hold=("*"); echo "foundation-deploy: cannot tell what ${sha} changes; every job is held until the release is live" >&2; }
# What step 0 drains (root ADR-0308): the held jobs and nothing else, since a release from the registry
# is pulled, not built, and a run keeps the release it started with (D1). A release the host builds
# (artifacts_from: server-build, the rollback of ADR-0276) needs the 22g builder, and by the memory
# budget nothing else fits beside it (ADR-0137 §3-4): every job, as before.
drain=("${hold[@]}")
artifacts_from="$(python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1]))["artifacts_from"])' \
  "${changes}/tools/release-build.contract.json" 2>/dev/null)" || artifacts_from=""
[[ "${artifacts_from}" == registry ]] || drain=("*")
: >"${changes}.hold"
((${#hold[@]} == 0)) || printf '%s\n' "${hold[@]}" >"${changes}.hold"
rm -rf "${changes}"
drains_all=no
[[ " ${drain[*]-} " != *" * "* ]] || drains_all=yes

log "0. pause and drain what ${sha} holds (root ADR-0308): ${drain[*]:-nothing}"
# From here until step 7 the drained DAGs are paused; every other job keeps its schedule and its run.
# A deploy that stops before the release switch gives the running release its schedule back; one that
# stops during the switch says loudly that the held ones stay paused (deploy-start-once.sh, root
# ADR-0173). A signal (the unit's time limit) ends it the same way.
phase=before-switch
trap 'status=$?; rm -f "${deploy_hold}"; deploy_paused_dags_on_exit "${status}" "${phase}" "${sha}"' EXIT
trap 'exit 143' TERM INT HUP
{ printf 'pid %s\n' "$$"; ((${#drain[@]} == 0)) || printf '%s\n' "${drain[@]}"; } >"${deploy_hold}"
if [[ "${drains_all}" == yes ]]; then
  for id in "${jobs[@]}"; do airflow dags pause "foundation_${id}" >/dev/null; done
  echo "paused ${#jobs[@]} DAGs"
  # Disabled jobs are paused too (a no-op when already paused); nothing here unpauses them.
  for id in "${disabled[@]}"; do airflow dags pause "foundation_${id}" >/dev/null 2>&1 || true; done
else
  count=0
  for id in "${drain[@]}"; do
    [[ " ${jobs[*]-} " == *" ${id} "* ]] || continue
    airflow dags pause "foundation_${id}" >/dev/null
    count=$((count + 1))
  done
  echo "paused ${count} DAGs; every other job keeps its schedule and its run"
fi
# The drained jobs' units, and those a deploy stops instead of waiting for (root ADR-0301 §2): an
# interruptible job is append-only and resumable, so a held one is stopped and runs again afterwards.
held_units=() stoppable=()
while read -r id service interruptible; do
  [[ "${drains_all}" == yes || " ${drain[*]-} " == *" ${id} "* ]] || continue
  held_units+=("${service}")
  [[ "${interruptible}" != yes ]] || stoppable+=("${service}")
done < <(jobs_of units)
running=0
if ((${#drain[@]} > 0)); then
  stopped=" "
  running=75
  for _ in $(seq 1 1800); do
    running=0
    busy="$("${admission}" running-jobs)" || running=$?
    ((running == 75)) || break
    # Only a drained job is waited for; any other registered job runs on through the release switch.
    waiting=()
    while read -r unit _; do
      [[ -n "${unit}" && " ${held_units[*]-} " == *" ${unit} "* ]] && waiting+=("${unit}")
    done <<<"${busy}"
    if ((${#waiting[@]} == 0)); then running=0; break; fi
    for unit in "${waiting[@]}"; do
      [[ " ${stoppable[*]-} " == *" ${unit} "* && "${stopped}" != *" ${unit} "* ]] || continue
      systemctl stop --no-block "${unit}"
      stopped+="${unit} "
      echo "stopped ${unit}: ${sha} holds it and it is interruptible; it runs again after the deploy (root ADR-0301)"
    done
    busy="$(printf '%s\n' "${waiting[@]}")"
    sleep 10
  done
fi
case "${running}" in
  0) if ((${#drain[@]} == 0)); then echo "${sha} holds no job: nothing is paused, waited for or stopped"
     elif [[ "${drains_all}" == yes ]]; then echo "no registered job is running"
     else echo "no held job is running"; fi ;;
  75) echo "held jobs still running after 5h: $(tr '\n' ' ' <<<"${busy}")"; exit 1 ;;
  *) echo "foundation-deploy: release admission cannot tell which registered jobs run (exit ${running})" >&2; exit 1 ;;
esac

log "1. control checkout at ${sha} (fetched by the pre-flight)"
target="${control_root}/releases/${sha}"
if [[ ! -e "${target}" ]]; then
  staging="$(mktemp -d "${control_root}/releases/.staging.XXXXXX")"
  git --git-dir="${mirror}" archive "${sha}" | tar -x --no-same-owner -C "${staging}"
  printf '%s\n' "${sha}" >"${staging}/.perfectory-control-commit"
  chown -R root:root "${staging}"; chmod -R u+rwX,go+rX,go-w "${staging}"; chmod 0755 "${staging}"
  mv -T "${staging}" "${target}"
fi
ln -sfn "releases/${sha}" "${control_root}/current.next"
mv -T "${control_root}/current.next" "${control_root}/current"
release="${control_root}/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh"

log "2. prepare (release admission: the signed release from the registry, or a build under the build lock)"
archive="$(mktemp /var/lib/perfectory/foundation-XXXXXX.tar.gz)"
git --git-dir="${mirror}" archive --format=tar.gz -o "${archive}" "${sha}:platforms/foundation-platform"
"${release}" prepare "${sha}" "${archive}"
rm -f "${archive}"

log "2b. the schedule comes back for every job ${sha} does not hold (root ADR-0270 §3, deploy D5)"
# A job ${sha} does not hold writes no table it changes and runs no migration, so it may run while the
# release is switched: a run keeps the release it started with (D1) and outlives the Airflow restart
# in step 5 (D2), and a run step 6 starts is joined, not doubled. The held jobs stay paused until
# step 7. Only a deploy that drained every job (a release built here, which ran alone, ADR-0137) has
# anything to give back now; otherwise no unheld DAG was paused.
if [[ "${drains_all}" == yes ]]; then
  deploy_unpause_unheld "${changes}.hold"
else
  echo "no unheld DAG was paused"
fi
{ printf 'pid %s\n' "$$"; cat "${changes}.hold"; } >"${deploy_hold}.next" && mv -f "${deploy_hold}.next" "${deploy_hold}"
rm -f "${changes}.hold"

log "3. FLOOR config for ${sha} (from ${old})"
floor="$(mktemp /var/lib/perfectory/floor-XXXXXX.env)"
cp "${release_root}/config/${old}/building-register-floor.env" "${floor}"
image="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["publisher_image"])' "${release_root}/artifacts/${sha}/build.json")"
sed -i -e "s|^FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT=.*|FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT=${release_root}/releases/${sha}|" \
  -e "s|^FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE=.*|FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE=${image}|" "${floor}"
chown root:root "${floor}"; chmod 0644 "${floor}"
"${release}" floor-config "${sha}" "${floor}"
rm -f "${floor}"

log "3b. staging smoke of ${sha}: a few real inputs under staging/ and foundation_staging (root ADR-0177)"
# Still before the switch: a smoke that fails stops the deploy here, nothing of the new release is
# active, and the EXIT trap gives the running release its schedule back.
"${release}" staging-smoke "${sha}"

log "4. activate, migrate, timers"
phase=switching
"${release}" activate "${sha}"
"${release}" migrate
"${release}" timers "${deployer_home}/airflow-state/scheduler_ed25519.pub"
"${release}" status | tail -3

log "5. Airflow picks up the release's DAGs and pools"
as_deployer bash "${release_root}/current/scripts/deploy/airflow-runtime.sh" up -d

log "6. start once, under the new release, each job its registry marks started_once_after_deploy"
# Then 7: unpause the enabled DAGs, whatever those runs did (deploy-start-once.sh, root ADR-0173).
phase=after-switch
start_once_then_unpause "${sha}"
rm -f "${deploy_hold}"
phase=done

# The check every job's ExecStartPre runs: a release this deploy left unadmissible (2026-10-10, root
# imports wrote bytecode into it) fails the deploy here, not every job at its next start.
if ! "${control_root}/current/scripts/deploy/foundation-release-admission.py" verify-current; then
  echo "foundation-deploy: RELEASE ADMISSION REFUSES THE RELEASE JUST INSTALLED; every job on it will be refused" >&2
  exit 1
fi

log "8. the data catalog's containers follow the release (only services whose config changed)"
# After the DAGs are back: a catalog that fails to come up fails the deploy and alerts, and no job
# waits on it.
"${release}" datahub

log "DONE: production runs ${sha}"
readlink "${release_root}/current"
