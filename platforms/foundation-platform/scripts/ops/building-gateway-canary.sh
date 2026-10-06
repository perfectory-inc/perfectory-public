#!/usr/bin/env bash
# Gradual rollout of the building gateway's pack path, by Worker version percentage
# (root ADR-0151 Revision, contract building_by_pnu_gateway.section_packs.canary).
#
#   building-gateway-canary.sh [--execute] code            # new code, packs held off
#   building-gateway-canary.sh [--execute] packs <off-version-id>
#   building-gateway-canary.sh [--execute] rollback <version-id>
#   building-gateway-canary.sh status
#
# Dry run by default: every command is printed, nothing is uploaded or deployed. --execute runs
# them. Run it from a checkout of the merged release, where wrangler is logged in to the account;
# it never touches the manifest.
#
# The two phases:
#   code   uploads this checkout with FOUNDATION_PLATFORM_BUILDING_PACK_SERVING=off (it serves
#          objects even when the manifest names packs) and moves traffic from the version now at
#          100% to it, step by step. After it, `_capabilities` answers [1, 2, 3] and the first
#          pack publish (runbook §5) can run; nothing a user sees changes.
#   packs  uploads the same code with the binding on and moves traffic from the off version to
#          it, step by step. A user reads packs only from here on.
# Every step holds canary.hold_seconds, then the health command judges the new version against
# the old one (check-building-gateway-version-health: reads pinned to each version, then Cloudflare
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
#                          'ssh ai-server sudo /opt/foundation-platform/current/scripts/ops/building-gateway-health.sh @NEW@ @OLD@'
set -euo pipefail

# Logs go to stderr: the functions that answer a version id answer it on stdout.
log() { printf '%s building-gateway-canary: %s\n' "$(date -u +%FT%TZ)" "$*" >&2; }
refuse() { log "refused: $1"; exit "${2:-64}"; }

EXECUTE=no
if [[ "${1:-}" == "--execute" ]]; then
  EXECUTE=yes
  shift
fi
PHASE="${1:-}"
PLATFORM_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
GATEWAY_DIR="${PLATFORM_ROOT}/services/foundation-building-gateway"
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

WORKER="$(contract building_by_pnu_gateway.worker_name)"
BINDING="$(contract building_by_pnu_gateway.section_packs.serving_binding)"
STEPS="$(contract building_by_pnu_gateway.section_packs.canary.steps_percent)"
HOLD="${CANARY_HOLD_SECONDS:-$(contract building_by_pnu_gateway.section_packs.canary.hold_seconds)}"
HEALTH="${CANARY_HEALTH_COMMAND:-${PLATFORM_ROOT}/scripts/ops/building-gateway-health.sh @NEW@ @OLD@}"

run() {
  log "+ $*"
  if [[ "${EXECUTE}" == yes ]]; then
    (cd "${GATEWAY_DIR}" && "$@")
  fi
}

wrangler() { run npx wrangler "$@"; }

# The version at 100% now; a rollout starts only from a single version.
current_version() {
  if [[ "${EXECUTE}" != yes ]]; then
    echo "<version-now-at-100%>"
    return
  fi
  (cd "${GATEWAY_DIR}" && npx wrangler deployments status --name "${WORKER}" --json) | python3 -c '
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
  (cd "${GATEWAY_DIR}" && npx wrangler deployments status --name "${WORKER}") >&2 \
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
  log "!!!   building-gateway-canary.sh --execute rollback ${old}"
  exit 3
}

# Moves traffic from <old> to <new> along the contract's steps, judging each.
roll_out() {
  local old="$1" new="$2" percent
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
    if ! judge "${new}" "${old}"; then
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
    [[ -n "${old}" ]] || refuse "usage: building-gateway-canary.sh [--execute] packs <off-version-id>"
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
    [[ -n "${2:-}" ]] || refuse "usage: building-gateway-canary.sh [--execute] rollback <version-id>"
    roll_back "$2" || { show_split; refuse "the rollback to $2 failed; the deployment is shown above" 3; }
    ;;
  status)
    EXECUTE=yes wrangler deployments status --name "${WORKER}"
    ;;
  *)
    refuse "usage: building-gateway-canary.sh [--execute] code|packs <off-version>|rollback <version>|status"
    ;;
esac
