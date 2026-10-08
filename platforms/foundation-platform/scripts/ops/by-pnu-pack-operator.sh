#!/usr/bin/env bash
# The operator's root steps of a by-PNU section pack cut-over (root ADR-0147, ADR-0160, ADR-0161),
# one fixed action at a time:
#
#   by-pnu-pack-operator.sh <building|parcel> equality <generation>
#   by-pnu-pack-operator.sh <building|parcel> latency <generation>
#   by-pnu-pack-operator.sh <building|parcel> publish <generation>
#   by-pnu-pack-operator.sh <building|parcel> monitor-sample <generation>
#   by-pnu-pack-operator.sh <building|parcel> health <new-version-id> [<old-version-id>] | --preflight
#   by-pnu-pack-operator.sh <building|parcel> status <generation>
#
# sudo grants this script, by its control-checkout path and nothing else, to the operator account
# (`foundation-release.sh operator-access`, root ADR-0161): it is how a cut-over's gates, publish
# and canary verdicts run without a password and without a root shell. So it takes no paths and no
# commands, only a lane, an action and a generation or version id, each checked before anything
# runs. Every publisher run is the admitted release's binary as the service user in a transient
# unit, with the environment files config/runtime-secrets.contract.json names for it.
#
# Paths are fixed by lane and generation: the bake's work directory
# /data/foundation-platform/by-pnu-bake/<lane>-pack-g<generation> holds summaries/, equality.json,
# latency.json and logs/. `equality`, `latency` and `publish` start a detached unit
# (foundation-<lane>-pack-<action>-g<generation>) whose output goes to logs/<action>.log; `status`
# shows them. Evidence is never overwritten: a latency run first keeps the earlier latency.json
# under its time.
set -euo pipefail
log() { printf '%s by-pnu-pack-operator: %s\n' "$(date -u +%FT%TZ)" "$*" >&2; }
refuse() { log "refused: $1"; exit "${2:-64}"; }
USAGE="usage: by-pnu-pack-operator.sh <building|parcel> equality|latency|publish|monitor-sample|status <generation> | health <new-version> [<old-version>] | health --preflight"

LANE="${1:-}" ACTION="${2:-}"
[[ "${LANE}" == building || "${LANE}" == parcel ]] || refuse "${USAGE}"
shift 2 || refuse "${USAGE}"
PLATFORM_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
# The release the host serves, not this checkout: the publisher and its scripts are the admitted
# release's (the control checkout only names this script to sudo).
RELEASE="${FOUNDATION_OPERATOR_RELEASE_ROOT:-/opt/foundation-platform/current}"
SYSTEMD_RUN="${FOUNDATION_OPERATOR_SYSTEMD_RUN:-systemd-run}"
BAKE_ROOT="${FOUNDATION_OPERATOR_BAKE_ROOT:-/data/foundation-platform/by-pnu-bake}"
ETC="${FOUNDATION_OPERATOR_ETC:-/etc/foundation-platform}"
# Owners of what this writes; the FOUNDATION_OPERATOR_* overrides exist for the tests, and sudo's
# env_reset removes them from a granted run.
SERVICE_OWNER="${FOUNDATION_OPERATOR_SERVICE_OWNER:-foundation-platform}"
SERVICE_GROUP="${FOUNDATION_OPERATOR_SERVICE_GROUP:-foundation-platform}"
ROOT_OWNER="${FOUNDATION_OPERATOR_ROOT_OWNER:-root}"
P="FOUNDATION_PLATFORM_${LANE^^}_BY_PNU_SERVING"
# sudo runs the control checkout's copy; the work is done by the current release's own copy, which
# binds its publisher from its own directory like every job (root ADR-0134 §3).
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
release_ops="$(cd "${RELEASE}/scripts/ops" 2>/dev/null && pwd -P)" || refuse "no current release at ${RELEASE}" 65
if [[ "${here}" != "${release_ops}" ]]; then
  [[ -x "${release_ops}/by-pnu-pack-operator.sh" ]] || refuse "the current release has no ${0##*/}; it is older than this script" 65
  exec "${release_ops}/by-pnu-pack-operator.sh" "${LANE}" "${ACTION}" "$@"
fi
UUID='^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

contract() {
  python3 -I - "${RELEASE}/config/r2-connections.contract.json" "$1" <<'PY'
import json, sys
value = json.load(open(sys.argv[1], encoding="utf-8"))
for part in sys.argv[2].split("."):
    value = value[part]
print(value)
PY
}

if [[ "${ACTION}" == health ]]; then
  if [[ "${1:-}" == --preflight ]]; then
    exec "${RELEASE}/scripts/ops/by-pnu-gateway-health.sh" "${LANE}" --preflight
  fi
  [[ "${1:-}" =~ ${UUID} ]] || refuse "health takes the new version id"
  [[ -z "${2:-}" || "${2}" =~ ${UUID} ]] || refuse "the old version id is not a version id"
  exec "${RELEASE}/scripts/ops/by-pnu-gateway-health.sh" "${LANE}" "$1" ${2:+"$2"}
fi

GENERATION="${1:-}"
[[ "${GENERATION}" =~ ^[1-9][0-9]{0,3}$ ]] || refuse "${ACTION} takes a generation number"
WORK="${BAKE_ROOT}/${LANE}-pack-g${GENERATION}"
UNIT="foundation-${LANE}-pack-${ACTION}-g${GENERATION}"

case "${ACTION}" in
  status)
    for action in equality latency publish; do
      unit="foundation-${LANE}-pack-${action}-g${GENERATION}"
      printf '%s: %s\n' "${unit}" "$(systemctl is-active "${unit}" 2>/dev/null || true)"
      [[ ! -f "${WORK}/logs/${action}.log" ]] || tail -3 "${WORK}/logs/${action}.log" | cut -c1-300
    done
    exit 0
    ;;
  monitor-sample)
    # The hourly monitor and the canary read the gate sample from the lane's file (runtime-secrets
    # group <lane>-serving-monitor); it names this generation's equality evidence.
    [[ -s "${WORK}/equality.json" ]] || refuse "${WORK}/equality.json does not exist; run equality first" 65
    file="$(python3 -I -c 'import json,sys; print(next(g["path"] for g in json.load(open(sys.argv[1]))["groups"] if g["name"] == sys.argv[2] + "-serving-monitor"))' \
      "${RELEASE}/config/runtime-secrets.contract.json" "${LANE}")"
    file="${ETC}/$(basename "${file}")"
    printf '%s_MONITOR_SAMPLE_PATH=%s\n' "${P}" "${WORK}/equality.json" |
      install -m 0640 -o "${ROOT_OWNER}" -g "${SERVICE_GROUP}" /dev/stdin "${file}"
    log "${file} names ${WORK}/equality.json"
    exit 0
    ;;
  equality | latency | publish) ;;
  *) refuse "${USAGE}" ;;
esac

[[ -d "${WORK}/summaries" ]] || refuse "${WORK}/summaries does not exist; bake generation ${GENERATION} first" 65
if systemctl is-active --quiet "${UNIT}"; then refuse "${UNIT} is already running" 75; fi
install -d -o "${SERVICE_OWNER}" -g "${SERVICE_GROUP}" "${WORK}/logs"
# The admitted binary of the current release, as every job binds it (root ADR-0134 §3).
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
release_id="${RELEASE_ID}"
props=() env=()
case "${ACTION}" in
  equality)
    read -ra props <<<"$(cd "${RELEASE}" && python3 scripts/deploy/runtime_secrets.py properties section-pack-operator)"
    env=("${P}_PACK_SUMMARY_DIR=${WORK}/summaries" "${P}_PACK_EQUALITY_EVIDENCE_PATH=${WORK}/equality.json")
    command="verify-${LANE}-by-pnu-section-pack-equality"
    ;;
  latency)
    [[ -s "${WORK}/equality.json" ]] || refuse "gate (나) draws its sample from ${WORK}/equality.json; run equality first" 65
    read -ra props <<<"$(cd "${RELEASE}" && python3 scripts/deploy/runtime_secrets.py properties section-pack-latency-probe)"
    preview="https://$(contract "${LANE}_by_pnu_gateway.section_packs.preview_worker.public_hostname")"
    if [[ -f "${WORK}/latency.json" ]]; then
      cp -p "${WORK}/latency.json" "${WORK}/latency-$(date -u -r "${WORK}/latency.json" +%Y%m%dT%H%M%SZ).json"
    fi
    env=("${P}_PACK_GENERATION=${GENERATION}" "${P}_PACK_PREVIEW_BASE_URL=${preview}"
      "${P}_PACK_EQUALITY_EVIDENCE_PATH=${WORK}/equality.json" "${P}_PACK_LATENCY_EVIDENCE_PATH=${WORK}/latency.json")
    command="probe-${LANE}-by-pnu-section-pack-latency"
    ;;
  publish)
    for evidence in equality latency; do
      python3 -I -c 'import json,sys; sys.exit(0 if json.load(open(sys.argv[1])).get("passed") else 1)' \
        "${WORK}/${evidence}.json" 2>/dev/null || refuse "${WORK}/${evidence}.json did not pass" 65
    done
    read -ra props <<<"$(cd "${RELEASE}" && python3 scripts/deploy/runtime_secrets.py properties section-pack-operator)"
    snapshot="$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["gold_iceberg_snapshot_id"])' \
      "$(ls "${WORK}"/summaries/*.json | head -1)")"
    env=("${P}_OUTPUT_STORAGE_DRIVER=r2" "${P}_CONFIRM_PACK_PUBLISH=true" "${P}_PACK_SUMMARY_DIR=${WORK}/summaries"
      "${P}_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID=${snapshot}"
      "${P}_PACK_EQUALITY_EVIDENCE_PATH=${WORK}/equality.json" "${P}_PACK_LATENCY_EVIDENCE_PATH=${WORK}/latency.json")
    command="publish-${LANE}-by-pnu-section-packs"
    ;;
esac
setenv=()
for value in "${env[@]}"; do setenv+=(-E "${value}"); done
log "starting ${UNIT}: ${command} (release ${release_id}); output ${WORK}/logs/${ACTION}.log"
"${SYSTEMD_RUN}" --collect --unit="${UNIT}" -p User=foundation-platform -p MemoryMax=8G \
  -p WorkingDirectory="${RELEASE}" "${props[@]}" "${setenv[@]}" \
  -p StandardOutput=append:"${WORK}/logs/${ACTION}.log" -p StandardError=append:"${WORK}/logs/${ACTION}.log" \
  "${PUBLISHER_BIN}" "${command}"
