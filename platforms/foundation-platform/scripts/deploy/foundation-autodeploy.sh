#!/usr/bin/env bash
# The host deploys main by itself (root ADR-0159). foundation-autodeploy.timer runs this every few
# minutes as root; each run either does nothing or deploys exactly one commit:
#
# - main's head is read from GitHub anonymously (the repository is public, ADR-0136);
# - nothing happens when the host already runs it, when it was refused or failed before (a newer
#   commit on main is the way past it), or while /etc/foundation-platform/autodeploy.off exists;
# - its check runs decide (release_checks.py): all passed deploys it, any still running waits for
#   the next tick, any failed refuses it for good;
# - the deploy is foundation-deploy.sh from the control checkout the host trusts now, not from the
#   commit being deployed.
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
  exit 0
fi
for outcome in refused failed; do
  if [[ -e "${state}/${outcome}/${head}" ]]; then
    log "${head} was ${outcome} before; waiting for a newer commit on main"
    exit 0
  fi
done

if ! checks="$(python3 "${platform}/scripts/deploy/release_checks.py" "${api}" "${repository}" "${head}")"; then
  log "cannot read the checks of ${head}; asking again next time"
  exit 0
fi
case "${checks%% *}" in
  wait)
    log "${head}: ${checks#* }"
    exit 0
    ;;
  refuse)
    mkdir -p "${state}/refused"
    printf '%s\n' "${checks#* }" >"${state}/refused/${head}"
    log "${head} is not deployed: ${checks#* }"
    exit 1
    ;;
  deploy) ;;
  *)
    log "unexpected answer from release_checks.py: ${checks}"
    exit 1
    ;;
esac

log "deploying ${head} over ${current} (${checks#* })"
mkdir -p "${state}/failed" "${state}/deployed"
status=0
/bin/bash "${platform}/scripts/deploy/foundation-deploy.sh" "${head}" || status=$?
if [[ "${status}" == 0 ]]; then
  date -u +%FT%TZ >"${state}/deployed/${head}"
  log "${head} deployed"
else
  date -u +%FT%TZ >"${state}/failed/${head}"
  log "the deploy of ${head} failed (exit ${status}); see the log above"
  exit 1
fi
