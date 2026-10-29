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
mirror=/var/lib/perfectory/control-source.git
off_switch=/etc/foundation-platform/autodeploy.off
api=https://api.github.com
# What each tick decided, for node-exporter's textfile collector and the FoundationRelease* /
# FoundationHost* alerts (root ADR-0345), beside the deploy's own deploy.prom (root ADR-0321 §5).
metrics=/var/lib/perfectory/metrics/autodeploy.prom
platform="${control_root}/platforms/foundation-platform"

log() { printf 'foundation-autodeploy: %s\n' "$*"; }

# 2026-10-29: from #939 on every release build on main failed; each tick logged "no commit … has a
# published release yet", the host stayed on a879a889 for about 11 hours and about 25 merged pull
# requests (security fixes among them) never reached it, and nothing told anyone. Each tick that holds
# the lock now replaces ${metrics} whole with:
#   foundation_autodeploy_tick_timestamp_seconds{reason}   when, and why the host is not on main
#     (reason "" when it is, or the tick deployed what was there to deploy);
#   foundation_autodeploy_behind_since_timestamp_seconds{reason,commit}   while main has a commit in
#     the release's scope the host does not run: the merge time of the oldest one;
#   foundation_autodeploy_release_missing_since_timestamp_seconds{commit}   while a commit in the
#     scope newer than the newest released one has no release: the merge time of the oldest such.
# A merge commit's committer time is when it reached main (the merge queue writes it).
reason="" target="" released_known=0 host_commit=""
in_scope="" # "<sha> <merge time>" lines, newest first: main's commits after the host in the scope
write_metrics() {
  ( set -e
    local behind="" behind_at="" missing="" missing_at="" newer=1 sha at
    while read -r sha at; do
      [[ -n "${sha}" ]] || continue
      [[ "${sha}" != "${host_commit}" ]] || break
      behind="${sha}" behind_at="${at}"
      [[ "${sha}" != "${target}" ]] || newer=0
      if ((released_known && newer)) && [[ ! -e "${state}/refused/${sha}" && ! -e "${state}/failed/${sha}" ]]; then
        missing="${sha}" missing_at="${at}"
      fi
    done <<<"${in_scope}"
    mkdir -p "$(dirname "${metrics}")"
    {
      echo "# HELP foundation_autodeploy_tick_timestamp_seconds When the autodeploy last decided, and why the host is not on main (root ADR-0345)."
      echo "# TYPE foundation_autodeploy_tick_timestamp_seconds gauge"
      printf 'foundation_autodeploy_tick_timestamp_seconds{reason="%s"} %s\n' "${reason}" "$(date +%s)"
      echo "# HELP foundation_autodeploy_behind_since_timestamp_seconds When the oldest commit in the release's scope that the host does not run reached main."
      echo "# TYPE foundation_autodeploy_behind_since_timestamp_seconds gauge"
      [[ -z "${behind}" ]] \
        || printf 'foundation_autodeploy_behind_since_timestamp_seconds{reason="%s",commit="%s"} %s\n' "${reason:-no_release}" "${behind:0:12}" "${behind_at}"
      echo "# HELP foundation_autodeploy_release_missing_since_timestamp_seconds When the oldest in-scope commit newer than the newest release reached main."
      echo "# TYPE foundation_autodeploy_release_missing_since_timestamp_seconds gauge"
      [[ -z "${missing}" ]] \
        || printf 'foundation_autodeploy_release_missing_since_timestamp_seconds{commit="%s"} %s\n' "${missing:0:12}" "${missing_at}"
    } >"${metrics}.next"
    chmod 0644 "${metrics}.next"
    mv -f "${metrics}.next" "${metrics}"
  ) || log "could not write ${metrics}" >&2
}
# The Cloudflare Workers' deploy (root ADR-0175) is its own unit, started without waiting: its
# failures reach Slack through its own OnFailure and never change this deploy's outcome. A host
# whose release does not carry the unit yet just does not start it.
worker_unit=foundation-worker-autodeploy.service
start_worker_deploy() { systemctl start --no-block "${worker_unit}" >/dev/null 2>&1 || true; }

mkdir -p "${state}"
exec 9>"${state}/lock"
flock -n 9 || { log "a deploy is already running"; exit 0; }
# Only the tick that holds the lock writes the metrics; the deploying tick keeps its own word.
trap write_metrics EXIT
if [[ -e "${off_switch}" ]]; then
  reason=off
  log "off (${off_switch} exists)"
  exit 0
fi

repository="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["full_name"])' \
  "${control_root}/tools/github/repository-identity.json")"
head="$(/bin/bash "${control_root}/scripts/github/safe-git-transport.sh" --anonymous --no-repository \
  ls-remote "https://github.com/${repository}.git" refs/heads/main | cut -f1)" || head=""
[[ "${head}" =~ ^[0-9a-f]{40}$ ]] || { reason=main_unreadable; log "cannot read main's head; asking again next time"; exit 0; }
current="$(readlink -f "${release_root}/current")"; current="${current##*/}"
host_commit="${current}"
if [[ "${head}" == "${current}" ]]; then
  # The host runs main: the Workers follow it (root ADR-0175). The unit deploys only a Worker whose
  # sources changed or whose last attempt failed and may be retried, and does nothing until the
  # owner placed the account token's file. It runs on its own, so its outcome is its own.
  start_worker_deploy
  exit 0
fi
# The commit to deploy: the newest on main the host does not run whose release is published (root
# ADR-0276 §13). Its signed release is built after the merge (~31 min); while merges come faster,
# waiting for main's head would never deploy (2026-10-19: 13e6d650 was released while the tick
# waited on b375edaa). A refused or failed commit is passed over. Main's history comes from the
# deploy's mirror over git, not the rate-limited API; without it only the head is a candidate.
if [[ "$(git --git-dir="${mirror}" rev-parse -q --verify refs/heads/main 2>/dev/null)" != "${head}" ]]; then
  /bin/bash "${control_root}/scripts/github/safe-git-transport.sh" --anonymous --repository "${mirror}" \
    fetch -q --no-tags "https://github.com/${repository}.git" +refs/heads/main:refs/heads/main >/dev/null 2>&1 || true
fi
# Main's commits after the running release that change nothing the release is made of or the host
# reads get no release (root ADR-0309: the release build's scope in tools/ci/change-scope.json, read
# by change_scope.py of the control checkout): the host already runs what they would deploy, so it
# is on main as far as a release goes. Any answer but "outside the scope" goes on as before.
# It also lists the commits after the host that are in the scope one by one, newest first with the
# time each reached main: what the metrics above count (root ADR-0345).
scope=0
rm -f "${state}/in-scope"
python3 -I -B - "${control_root}" "${mirror}" "${current}" "${head}" "${state}/in-scope" <<'PY' || scope=$?
import subprocess, sys
from pathlib import Path
root, mirror, base, head, listed = sys.argv[1:]
try:
    sys.path.insert(0, f"{root}/scripts/ci")
    import change_scope
    contract = change_scope.load_contract(Path(root))
    git = ["git", "--git-dir", mirror]
    diff = [*git, "diff", "--name-only", "-z", "--no-renames", f"{base}...{head}"]
    changed = {p for p in subprocess.run(diff, check=True, capture_output=True).stdout.decode().split("\0") if p}
    run, reason = change_scope.decide(contract, "foundation-release-build.yml", "push", changed, Path(root))
    commits = subprocess.run([*git, "log", "--first-parent", "--format=%H %ct", f"{base}..{head}"],
                             check=True, capture_output=True, text=True).stdout.split()
    lines = []
    for commit, merged in zip(commits[::2], commits[1::2]):
        own = [*git, "diff", "--name-only", "-z", "--no-renames", f"{commit}^", commit]
        paths = {p for p in subprocess.run(own, check=True, capture_output=True).stdout.decode().split("\0") if p}
        if change_scope.decide(contract, "foundation-release-build.yml", "push", paths, Path(root))[0]:
            lines.append(f"{commit} {merged}\n")
    Path(listed).write_text("".join(lines), encoding="utf-8")
except Exception as error:  # no answer is not "outside the scope"
    print(f"foundation-autodeploy: cannot tell whether {head[:8]} is in the release's scope: {error!r}", file=sys.stderr)
    sys.exit(1)
if not run:
    print(f"foundation-autodeploy: nothing to deploy after {base[:8]} up to {head[:8]}: {reason}")
sys.exit(0 if run else 3)
PY
if ((scope == 3)); then
  start_worker_deploy
  exit 0
fi
in_scope="$(cat "${state}/in-scope" 2>/dev/null || true)"
candidates=()
while read -r commit; do
  [[ -e "${state}/refused/${commit}" || -e "${state}/failed/${commit}" ]] || candidates+=("${commit}")
done < <(git --git-dir="${mirror}" rev-list --first-parent "${current}..${head}" 2>/dev/null || printf '%s\n' "${head}")
if ((${#candidates[@]} == 0)); then
  reason=every_newer_commit_refused_or_failed
  log "every commit after ${current} up to ${head} was refused or failed before; waiting for a newer commit on main"
  exit 0
fi
if ! target="$(python3 "${platform}/scripts/deploy/release_checks.py" newest-released "${repository}" "${candidates[@]}")"; then
  reason=releases_unreadable
  log "cannot read which releases are published; asking again next time"
  exit 0
fi
released_known=1
if [[ -z "${target}" ]]; then
  reason=no_release
  log "no commit after ${current} up to ${head} has a published release yet"
  exit 0
fi

# A tick runs every minute (ADR-0187), but the check runs come from GitHub's REST API read
# anonymously, which allows a host 60 requests an hour, and one answer takes release_checks.py at
# least two. So a commit is asked about once per `recheck_seconds` at most while its checks give no
# verdict (still running, or unreadable), and never again once they passed: the verdict is kept
# while the deploy waits for an operator's unit or the Worker deploy. A new head is asked at once.
recheck_seconds=300
asked="${state}/asked/${target}"
passed="${state}/passed/${target}"
mkdir -p "${state}/asked" "${state}/passed"
find "${state}/asked" "${state}/passed" -type f ! -name "${target}" -delete
if [[ -e "${passed}" ]]; then
  checks="$(cat "${passed}")"
else
  if [[ -e "${asked}" ]] && (( $(date +%s) - $(stat -c %Y "${asked}") < recheck_seconds )); then
    reason=checks_pending
    exit 0
  fi
  touch "${asked}"
  if ! checks="$(python3 "${platform}/scripts/deploy/release_checks.py" "${api}" "${repository}" "${target}")"; then
    reason=checks_unreadable
    log "cannot read the checks of ${target}; asking again in ${recheck_seconds}s"
    exit 0
  fi
fi
case "${checks%% *}" in
  wait)
    reason=checks_pending
    log "${target}: ${checks#* }; asking again in ${recheck_seconds}s"
    exit 0
    ;;
  refuse)
    mkdir -p "${state}/refused"
    printf '%s\n' "${checks#* }" >"${state}/refused/${target}"
    reason=checks_failed
    log "${target} is not deployed: ${checks#* }"
    exit 1
    ;;
  deploy) [[ -e "${passed}" ]] || printf '%s\n' "${checks}" >"${passed}" ;;
  *)
    reason=checks_unreadable
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
  reason=quiet_window
  log "${target}: waiting for the quiet window after the last deploy (until $(date -u -d "@$((last + quiet * 60))" +%H:%MZ))"
  exit 0
fi

# Whether a unit runs is job_units.py's answer (root ADR-0184). A systemd that cannot answer is not
# "nothing runs": the tick fails, and its OnFailure says so. Assign its output before testing it;
# a failure inside `[[ -n "$(...)" ]]` would not stop the script.
running_units() {
  python3 -I -B "${platform}/scripts/deploy/job_units.py" running "$@" \
    || { log "systemd cannot tell which units run; asking again next time" >&2; exit 1; }
}
reason=systemd_unreadable # until it answers
running="$(running_units 'foundation-*')"
operator_units=()
while read -r unit _; do
  if [[ -n "${unit}" && "$(systemctl show -p Transient --value "${unit}")" == yes ]]; then
    operator_units+=("${unit}")
  fi
done <<<"${running}"
if ((${#operator_units[@]} > 0)); then
  reason=operator_unit
  log "${target}: waiting for the operator's ${operator_units[*]}"
  exit 0
fi
# A Worker deploy reads the control checkout and the release this deploy is about to replace.
running="$(running_units "${worker_unit}")"
if [[ -n "${running}" ]]; then
  reason=worker_deploy
  log "${target}: waiting for the Worker deploy (${worker_unit})"
  exit 0
fi

reason=deploying
write_metrics
log "deploying ${target} over ${current} (${checks#* })"
mkdir -p "${state}/failed" "${state}/deployed"
status=0
/bin/bash "${platform}/scripts/deploy/foundation-deploy.sh" "${target}" || status=$?
if [[ "${status}" == 0 ]]; then
  date -u +%FT%TZ >"${state}/deployed/${target}"
  reason="" host_commit="${target}"
  log "${target} deployed"
  start_worker_deploy
else
  date -u +%FT%TZ >"${state}/failed/${target}"
  reason=deploy_failed
  log "the deploy of ${target} failed (exit ${status}); see the log above"
  exit 1
fi
