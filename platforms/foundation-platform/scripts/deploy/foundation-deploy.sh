#!/usr/bin/env bash
# Deploys one commit of main to this host, end to end (root ADR-0159): the steps an operator ran by
# hand from a copy outside the repository until 2026-10-07. foundation-autodeploy.sh runs it for
# each new main commit whose checks passed; an operator may run it the same way.
#
#   foundation-deploy.sh <40-hex commit on main>
#
# Root only. Every job list comes from orchestration/jobs.v1.json of the release being replaced:
# the DAGs paused and the services waited for are its jobs, and the jobs started once under the new
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
    elif which == "services" and job["enabled"]:
        print(job["systemd_service"])
    elif which == "started-once" and job["enabled"] and job.get("started_once_after_deploy"):
        print(job["systemd_service"], job["timeout_minutes"])
PY
}
mapfile -t jobs < <(jobs_of enabled)
mapfile -t disabled < <(jobs_of disabled)
mapfile -t units < <(jobs_of services)
old="$(readlink -f "${release_root}/current")"; old="${old##*/}"

log "0. pause the DAGs and wait until no registered job runs (oneshot jobs show 'activating')"
# From here until step 7 the DAGs are paused. A deploy that stops before the release switch gives the
# running release its schedule back; one that stops during the switch says loudly that they stay
# paused (deploy-start-once.sh, root ADR-0173). A signal (the unit's time limit) ends it the same way.
phase=before-switch
trap 'deploy_paused_dags_on_exit "$?" "${phase}" "${sha}"' EXIT
trap 'exit 143' TERM INT HUP
for id in "${jobs[@]}"; do airflow dags pause "foundation_${id}" >/dev/null; done
echo "paused ${#jobs[@]} DAGs"
# Disabled jobs are paused too (a no-op when already paused); nothing here unpauses them.
for id in "${disabled[@]}"; do airflow dags pause "foundation_${id}" >/dev/null 2>&1 || true; done
busy=()
for _ in $(seq 1 1800); do
  busy=()
  for unit in "${units[@]}"; do
    state="$(systemctl show -p ActiveState --value "${unit}")"
    [[ "${state}" == inactive || "${state}" == failed ]] || busy+=("${unit}=${state}")
  done
  ((${#busy[@]} == 0)) && break
  sleep 10
done
((${#busy[@]} == 0)) || { echo "jobs still running after 5h: ${busy[*]}"; exit 1; }
echo "no registered job is running"

log "1. control checkout at ${sha}"
/bin/bash "${control_root}/current/scripts/github/safe-git-transport.sh" --anonymous --repository "${mirror}" \
  fetch -q --no-tags "${repository}" +refs/heads/main:refs/heads/main
git --git-dir="${mirror}" merge-base --is-ancestor "${sha}" refs/heads/main
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

log "2. prepare (admission build under the build lock)"
archive="$(mktemp /var/lib/perfectory/foundation-XXXXXX.tar.gz)"
git --git-dir="${mirror}" archive --format=tar.gz -o "${archive}" "${sha}:platforms/foundation-platform"
"${release}" prepare "${sha}" "${archive}"
rm -f "${archive}"

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
phase=done

log "DONE: production runs ${sha}"
readlink "${release_root}/current"
