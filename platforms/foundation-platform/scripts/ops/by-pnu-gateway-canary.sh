#!/usr/bin/env bash
# Gradual rollout of a by-PNU gateway's pack path, by Worker version percentage
# (root ADR-0151 Revision, contract <lane>_by_pnu_gateway.section_packs.canary). One Worker source
# serves both lanes (root ADR-0160); the lane picks its contract block and Wrangler config.
#
#   by-pnu-gateway-canary.sh <building|parcel> [--execute] code     # new code, packs held off
#   by-pnu-gateway-canary.sh <building|parcel> [--execute] packs <off-version-id>
#   by-pnu-gateway-canary.sh <building|parcel> [--execute] rollback <version-id>
#   by-pnu-gateway-canary.sh <building|parcel> status
#
# Dry run by default: every command is printed, nothing is uploaded or deployed. --execute runs
# them. Run it from a checkout of the merged release, where wrangler is logged in to the account;
# it never touches the manifest.
#
# The two phases:
#   code   uploads this checkout with the lane's serving binding off (it serves
#          objects even when the manifest names packs) and moves traffic from the version now at
#          100% to it, step by step. After it, `_capabilities` answers [1, 2, 3] and the first
#          pack publish (runbook §5) can run; nothing a user sees changes.
#   packs  uploads the same code with the binding on and moves traffic from the off version to
#          it, step by step. A user reads packs only from here on.
# Every step holds canary.hold_seconds, then the health command judges the new version against
# the old one (check-<lane>-gateway-version-health: reads pinned to each version, then Cloudflare
# analytics). A breach rolls all traffic back to the old version at once and stops with exit 1.
# `rollback` does the same by hand; the manifest revert (runbook §6) is the second line, never
# needed for a version problem.
#
# A deployment that fails leaves traffic where Cloudflare left it, possibly split: the script then
# prints the deployment as it stands, tries to put every request back on the old version, and
# stops with exit 2 (rolled back) or 3 (the rollback failed too: the split is printed and the
# command that finishes it by hand is named).
#
# Environment:
#   CORS_ALLOWED_ORIGINS   the live Worker's FOUNDATION_PLATFORM_CORS_ALLOWED_ORIGINS value
#   CANARY_HEALTH_COMMAND  how a step is judged; @NEW@ and @OLD@ are replaced by version ids.
#                          Default: the release's publisher on this host, which reads the analytics
#                          token from the environment the contract names. On a workstation, point it
#                          at the host that holds the token, e.g.
#                          'ssh ai-server sudo /opt/foundation-platform/current/scripts/ops/by-pnu-gateway-health.sh <lane> @NEW@ @OLD@'
set -euo pipefail

# Logs go to stderr: the functions that answer a version id answer it on stdout.
log() { printf '%s by-pnu-gateway-canary %s: %s\n' "$(date -u +%FT%TZ)" "${LANE:-}" "$*" >&2; }
refuse() { log "refused: $1"; exit "${2:-64}"; }
USAGE="usage: by-pnu-gateway-canary.sh <building|parcel> [--execute] code|packs <off-version>|rollback <version>|status"

LANE="${1:-}"
[[ "${LANE}" == building || "${LANE}" == parcel ]] || refuse "${USAGE}"
shift
EXECUTE=no
if [[ "${1:-}" == "--execute" ]]; then
  EXECUTE=yes
  shift
fi
PHASE="${1:-}"
PLATFORM_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
# One Worker source for both by-PNU lanes (root ADR-0160); this lane's Wrangler config.
GATEWAY_DIR="${PLATFORM_ROOT}/services/foundation-by-pnu-gateway"
WRANGLER_CONFIG="wrangler.${LANE}.jsonc"
GATEWAY="${LANE}_by_pnu_gateway"
CONTRACT="${PLATFORM_ROOT}/config/r2-connections.contract.json"

contract() {
  python3 - "${CONTRACT}" "$1" <<'PY'
import json, sys
value = json.load(open(sys.argv[1], encoding="utf-8"))
for part in sys.argv[2].split("."):
    value = value[part]
print(" ".join(str(item) for item in value) if isinstance(value, list) else value)
PY
}

WORKER="$(contract "${GATEWAY}.worker_name")"
BINDING="$(contract "${GATEWAY}.section_packs.serving_binding")"
STEPS="$(contract "${GATEWAY}.section_packs.canary.steps_percent")"
HOLD="${CANARY_HOLD_SECONDS:-$(contract "${GATEWAY}.section_packs.canary.hold_seconds")}"
HEALTH="${CANARY_HEALTH_COMMAND:-${PLATFORM_ROOT}/scripts/ops/by-pnu-gateway-health.sh ${LANE} @NEW@ @OLD@}"

run() {
  log "+ $*"
  if [[ "${EXECUTE}" == yes ]]; then
    (cd "${GATEWAY_DIR}" && "$@")
  fi
}

wrangler() { run npx wrangler "$@" --config "${WRANGLER_CONFIG}"; }

# The version at 100% now; a rollout starts only from a single version.
current_version() {
  if [[ "${EXECUTE}" != yes ]]; then
    echo "<version-now-at-100%>"
    return
  fi
  (cd "${GATEWAY_DIR}" && npx wrangler deployments status --name "${WORKER}" --json --config "${WRANGLER_CONFIG}") | python3 -c '
import json, sys
versions = json.load(sys.stdin)["versions"]
full = [v["version_id"] for v in versions if v.get("percentage") == 100]
if len(full) != 1:
    sys.exit("a rollout starts from one version at 100%%; found %r" % versions)
print(full[0])'
}

upload() {
  local serving="$1" message="$2"
  if [[ "${EXECUTE}" != yes ]]; then
    log "+ npx wrangler versions upload --var ${BINDING}:${serving} --var FOUNDATION_PLATFORM_CORS_ALLOWED_ORIGINS:<CORS_ALLOWED_ORIGINS> --message '${message}'"
    echo "<uploaded-version-${serving}>"
    return
  fi
  [[ -n "${CORS_ALLOWED_ORIGINS:-}" ]] || refuse "CORS_ALLOWED_ORIGINS is required (the live Worker's value)"
  local out id
  out="$(cd "${GATEWAY_DIR}" && npx wrangler versions upload \
    --config "${WRANGLER_CONFIG}" \
    --var "${BINDING}:${serving}" \
    --var "FOUNDATION_PLATFORM_CORS_ALLOWED_ORIGINS:${CORS_ALLOWED_ORIGINS}" \
    --message "${message}" 2>&1)" || { printf '%s\n' "${out}" >&2; refuse "the upload failed" 70; }
  printf '%s\n' "${out}" >&2
  id="$(grep -oE 'Version ID: [0-9a-f-]{36}' <<<"${out}" | tail -1 | cut -d' ' -f3 || true)"
  # Without an id there is nothing to roll out to; going on would deploy a step of nothing.
  [[ "${id}" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]] \
    || refuse "the upload printed no version id (wrangler's output is above); nothing was deployed" 70
  echo "${id}"
}

judge() {
  local command="${HEALTH//@NEW@/$1}"
  command="${command//@OLD@/$2}"
  log "+ ${command}"
  [[ "${EXECUTE}" == yes ]] || return 0
  bash -c "${command}"
}

roll_back() {
  log "rolling every request back to $1"
  wrangler versions deploy "$1@100%" --name "${WORKER}" --yes --message "canary rollback"
}

# The deployment as Cloudflare holds it now, shouted: what a person must see when a step failed.
show_split() {
  log "!!! the live Worker's deployment is now:"
  (cd "${GATEWAY_DIR}" && npx wrangler deployments status --name "${WORKER}" --config "${WRANGLER_CONFIG}") >&2 \
    || log "!!! and it cannot be read; check it in the dashboard before anything else"
}

# A deployment that did not complete: show the split, try to put every request back on <old>.
deploy_failed() {
  local old="$1" what="$2"
  log "!!! ${what} failed; traffic may be split between versions"
  show_split
  if roll_back "${old}"; then
    show_split
    refuse "${what} failed; every request is back on ${old}" 2
  fi
  log "!!! THE ROLLBACK FAILED TOO: traffic is split as shown above. Finish it by hand:"
  log "!!!   by-pnu-gateway-canary.sh ${LANE} --execute rollback ${old}"
  exit 3
}

# Moves traffic from <old> to <new> along the contract's steps, judging each.
roll_out() {
  local old="$1" new="$2" percent compared
  for percent in ${STEPS}; do
    if (( percent >= 100 )); then
      wrangler versions deploy "${new}@100%" --name "${WORKER}" --yes --message "canary 100%" \
        || deploy_failed "${old}" "the deployment to 100%"
    else
      wrangler versions deploy "${old}@$((100 - percent))%" "${new}@${percent}%" \
        --name "${WORKER}" --yes --message "canary ${percent}%" \
        || deploy_failed "${old}" "the deployment of the ${percent}% step"
    fi
    log "holding ${HOLD}s at ${percent}%"
    [[ "${EXECUTE}" != yes ]] || sleep "${HOLD}"
    # At 100% the old version is no longer deployed: it serves nothing, a pin to it is not
    # honoured, and there is nothing to compare. The comparison was judged at every split step;
    # the last step is judged on the absolute bounds alone.
    compared="${old}"
    (( percent < 100 )) || compared=""
    if ! judge "${new}" "${compared}"; then
      roll_back "${old}" || deploy_failed "${old}" "the rollback after a breach at ${percent}%"
      refuse "the new version breached at ${percent}%; all traffic is back on ${old}" 1
    fi
  done
  log "${new} serves 100%; ${old} is the rollback target"
}

# A step that cannot be judged must not be taken: the health command is asked first whether it
# can read analytics at all (`--preflight`), before anything is uploaded.
preflight() {
  local command="${HEALTH//@NEW@/--preflight}"
  command="${command//@OLD@/}"
  log "+ ${command}"
  [[ "${EXECUTE}" == yes ]] || return 0
  bash -c "${command}" || refuse "the health check cannot read Cloudflare analytics; nothing was uploaded" 78
}

case "${PHASE}" in
  code)
    preflight
    old="$(current_version)"
    new="$(upload off "pack path held off (ADR-0151)")"
    roll_out "${old}" "${new}"
    ;;
  packs)
    old="${2:-}"
    [[ -n "${old}" ]] || refuse "usage: by-pnu-gateway-canary.sh ${LANE} [--execute] packs <off-version-id>"
    preflight
    # The packs phase starts where the code phase ended, the off version alone at 100%: any other
    # id would roll packs out against a version that is not serving, and roll back onto it.
    serving="$(current_version)"
    [[ "${EXECUTE}" != yes || "${serving}" == "${old}" ]] \
      || refuse "${old} is not the version at 100% (${serving} is); name the version the code phase left serving" 65
    new="$(upload on "pack path on (ADR-0151)")"
    roll_out "${old}" "${new}"
    ;;
  rollback)
    [[ -n "${2:-}" ]] || refuse "usage: by-pnu-gateway-canary.sh ${LANE} [--execute] rollback <version-id>"
    roll_back "$2" || { show_split; refuse "the rollback to $2 failed; the deployment is shown above" 3; }
    ;;
  status)
    EXECUTE=yes wrangler deployments status --name "${WORKER}"
    ;;
  *)
    refuse "${USAGE}"
    ;;
esac
