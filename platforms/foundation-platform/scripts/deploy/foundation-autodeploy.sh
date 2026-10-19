#!/usr/bin/env bash
# The host deploys main by itself (root ADR-0159). foundation-autodeploy.timer runs this every minute
# as root (ADR-0187); each run either does nothing or deploys exactly one commit:
#
# - main's head is read from GitHub anonymously (the repository is public, ADR-0136);
# - nothing happens when the host already runs it, when it was refused or failed before (a newer
#   commit on main is the way past it), or while /etc/foundation-platform/autodeploy.off exists;
# - its check runs decide (release_checks.py): all passed, or all the merge queue ran on it passed
#   (ADR-0167), deploys it; any still running waits for the next tick, any failed refuses it for
#   good;
# - a unit an operator started by hand (a transient foundation-* unit: a pack bake, a gate, an
#   unconditional Gold rebuild) is not in the job registry the deploy waits for, so while one runs
#   the deploy waits for the next tick (ADR-0167);
# - the deploy is foundation-deploy.sh from the control checkout the host trusts now, not from the
#   commit being deployed;
# - once the host runs main's head, it starts foundation-worker-autodeploy.service, which deploys the
#   Cloudflare Workers whose sources changed (root ADR-0175), and a new deploy waits while that runs.
#
# A refused or failed commit exits 1 once, so the unit's OnFailure reports it; later ticks skip it
# quietly. The outcome of every commit is kept under the state directory.
set -euo pipefail

# Fixed paths, no environment override: a caller cannot appoint what decides or what deploys.
control_root=/opt/perfectory-control/current
release_root=/opt/foundation-platform
state=/var/lib/perfectory/autodeploy
off_switch=/etc/foundation-platform/autodeploy.off
api=https://api.github.com
platform="${control_root}/platforms/foundation-platform"

log() { printf 'foundation-autodeploy: %s\n' "$*"; }
# The Cloudflare Workers' deploy (root ADR-0175) is its own unit, started without waiting: its
# failures reach Slack through its own OnFailure and never change this deploy's outcome. A host
# whose release does not carry the unit yet just does not start it.
worker_unit=foundation-worker-autodeploy.service
start_worker_deploy() { systemctl start --no-block "${worker_unit}" >/dev/null 2>&1 || true; }

if [[ -e "${off_switch}" ]]; then
  log "off (${off_switch} exists)"
  exit 0
fi
mkdir -p "${state}"
exec 9>"${state}/lock"
flock -n 9 || { log "a deploy is already running"; exit 0; }

repository="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["full_name"])' \
  "${control_root}/tools/github/repository-identity.json")"
head="$(/bin/bash "${control_root}/scripts/github/safe-git-transport.sh" --anonymous --no-repository \
  ls-remote "https://github.com/${repository}.git" refs/heads/main | cut -f1)" || head=""
[[ "${head}" =~ ^[0-9a-f]{40}$ ]] || { log "cannot read main's head; asking again next time"; exit 0; }
current="$(readlink -f "${release_root}/current")"; current="${current##*/}"
if [[ "${head}" == "${current}" ]]; then
  # The host runs main: the Workers follow it (root ADR-0175). The unit deploys only a Worker whose
  # sources changed or whose last attempt failed and may be retried, and does nothing until the
  # owner placed the account token's file. It runs on its own, so its outcome is its own.
  start_worker_deploy
  exit 0
fi
for outcome in refused failed; do
  if [[ -e "${state}/${outcome}/${head}" ]]; then
    log "${head} was ${outcome} before; waiting for a newer commit on main"
    exit 0
  fi
done

# A tick runs every minute (ADR-0187), but the check runs come from GitHub's REST API read
# anonymously, which allows a host 60 requests an hour, and one answer takes release_checks.py at
# least two. So a commit is asked about once per `recheck_seconds` at most while its checks give no
# verdict (still running, or unreadable), and never again once they passed: the verdict is kept
# while the deploy waits for an operator's unit or the Worker deploy. A new head is asked at once.
recheck_seconds=300
asked="${state}/asked/${head}"
passed="${state}/passed/${head}"
mkdir -p "${state}/asked" "${state}/passed"
find "${state}/asked" "${state}/passed" -type f ! -name "${head}" -delete
if [[ -e "${passed}" ]]; then
  checks="$(cat "${passed}")"
else
  if [[ -e "${asked}" ]] && (( $(date +%s) - $(stat -c %Y "${asked}") < recheck_seconds )); then
    exit 0
  fi
  touch "${asked}"
  if ! checks="$(python3 "${platform}/scripts/deploy/release_checks.py" "${api}" "${repository}" "${head}")"; then
    log "cannot read the checks of ${head}; asking again in ${recheck_seconds}s"
    exit 0
  fi
fi
case "${checks%% *}" in
  wait)
    log "${head}: ${checks#* }; asking again in ${recheck_seconds}s"
    exit 0
    ;;
  refuse)
    mkdir -p "${state}/refused"
    printf '%s\n' "${checks#* }" >"${state}/refused/${head}"
    log "${head} is not deployed: ${checks#* }"
    exit 1
    ;;
  deploy) [[ -e "${passed}" ]] || printf '%s\n' "${checks}" >"${passed}" ;;
  *)
    log "unexpected answer from release_checks.py: ${checks}"
    exit 1
    ;;
esac

# A quiet window after each deploy (config/autodeploy.contract.json): the next deploy waits until
# quiet_minutes_after_a_deploy have passed since the last one was deployed, then ships main's head
# with every commit merged meanwhile. Operators and scheduled jobs get the host between. A failed
# deploy left the running release as it was, so the commit that fixes it does not wait.
quiet="$(python3 -I -c 'import json, sys; print(int(json.load(open(sys.argv[1]))["quiet_minutes_after_a_deploy"]))' \
  "${platform}/config/autodeploy.contract.json" 2>/dev/null)" \
  || { quiet=0; log "no quiet window: ${platform}/config/autodeploy.contract.json cannot be read"; }
# Before the first deploy the directory does not exist: nothing to wait for.
last="$({ find "${state}/deployed" -type f -printf '%T@\n' 2>/dev/null || true; } | sort -n | tail -n 1)"
last="${last%.*}"
if [[ -n "${last}" ]] && (($(date +%s) - last < quiet * 60)); then
  log "${head}: waiting for the quiet window after the last deploy (until $(date -u -d "@$((last + quiet * 60))" +%H:%MZ))"
  exit 0
fi

# Whether a unit runs is job_units.py's answer (root ADR-0184). A systemd that cannot answer is not
# "nothing runs": the tick fails, and its OnFailure says so. Assign its output before testing it;
# a failure inside `[[ -n "$(...)" ]]` would not stop the script.
running_units() {
  python3 -I -B "${platform}/scripts/deploy/job_units.py" running "$@" \
    || { log "systemd cannot tell which units run; asking again next time" >&2; exit 1; }
}
running="$(running_units 'foundation-*')"
operator_units=()
while read -r unit _; do
  if [[ -n "${unit}" && "$(systemctl show -p Transient --value "${unit}")" == yes ]]; then
    operator_units+=("${unit}")
  fi
done <<<"${running}"
if ((${#operator_units[@]} > 0)); then
  log "${head}: waiting for the operator's ${operator_units[*]}"
  exit 0
fi
# A Worker deploy reads the control checkout and the release this deploy is about to replace.
running="$(running_units "${worker_unit}")"
if [[ -n "${running}" ]]; then
  log "${head}: waiting for the Worker deploy (${worker_unit})"
  exit 0
fi

log "deploying ${head} over ${current} (${checks#* })"
mkdir -p "${state}/failed" "${state}/deployed"
status=0
/bin/bash "${platform}/scripts/deploy/foundation-deploy.sh" "${head}" || status=$?
if [[ "${status}" == 0 ]]; then
  date -u +%FT%TZ >"${state}/deployed/${head}"
  log "${head} deployed"
  start_worker_deploy
else
  date -u +%FT%TZ >"${state}/failed/${head}"
  log "the deploy of ${head} failed (exit ${status}); see the log above"
  exit 1
fi
