#!/usr/bin/env bash
# The operator's root steps of a by-PNU section pack cut-over (root ADR-0147, ADR-0160, ADR-0161,
# ADR-0166), one fixed action at a time:
#
#   by-pnu-pack-operator.sh <building|parcel> bake <generation>
#   by-pnu-pack-operator.sh <building|parcel> equality <generation>
#   by-pnu-pack-operator.sh <building|parcel> latency <generation>
#   by-pnu-pack-operator.sh <building|parcel> publish <generation>
#   by-pnu-pack-operator.sh <building|parcel> monitor-sample <generation>
#   by-pnu-pack-operator.sh <building|parcel> health <new-version-id> [<old-version-id>] | --preflight
#   by-pnu-pack-operator.sh <building|parcel> status <generation>
#   by-pnu-pack-operator.sh <building|parcel> gold-rebuild
#   by-pnu-pack-operator.sh silver plan|start|status <silver-lane>
#
# `silver` drives one Silver refresh lane (root ADR-0169, ADR-0178): `plan` runs its
# `silver-refresh.sh <lane> --plan` (reads the ledger and the catalog, writes nothing) and prints
# the outcome; `start` starts its scheduled unit foundation-silver-refresh@<lane>.service once, the
# supervised first run before the job is enabled; `status` shows the unit and the end of its journal.
# The lanes are the jobs.v1.json jobs whose service is that template; nothing else is accepted.
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
# latency.json and logs/. `bake`, `equality`, `latency` and `publish` start a detached unit
# (foundation-<lane>-pack-<action>-g<generation>) whose output goes to logs/<action>.log; `status`
# shows them. Evidence is never overwritten: a latency run first keeps the earlier latency.json
# under its time.
#
# `bake` runs the release's by-pnu-pack-bake.sh: the whole generation under the lane lock, never
# published. `gold-rebuild` rebuilds both panel Gold tables unconditionally, once, the way the
# scheduled foundation-gold-panel-rebuild.service runs (its unit file is the definition), with a
# fixed reason; it takes no argument beyond the lane. Neither starts while the other, the scheduled
# Gold rebuild or a by-PNU bake runs: a Gold that moves under a bake stops the bake.
set -euo pipefail
log() { printf '%s by-pnu-pack-operator: %s\n' "$(date -u +%FT%TZ)" "$*" >&2; }
refuse() { log "refused: $1"; exit "${2:-64}"; }
USAGE="usage: by-pnu-pack-operator.sh <building|parcel> bake|equality|latency|publish|monitor-sample|status <generation> | gold-rebuild | health <new-version> [<old-version>] | health --preflight | silver plan|start|status <silver-lane>"

LANE="${1:-}" ACTION="${2:-}"
[[ "${LANE}" == building || "${LANE}" == parcel || "${LANE}" == silver ]] || refuse "${USAGE}"
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
# Units that move the panel Gold, and the bakes that read it (root ADR-0166).
GOLD_SCHEDULED=foundation-gold-panel-rebuild.service
GOLD_UNIT=foundation-gold-panel-rebuild-unconditional
BAKE_SCHEDULED=foundation-by-pnu-serving-bake.service
PACK_BAKES='foundation-*-pack-bake-g*.service'
# The rebuild's one reason, so the plan and the Gold history name where it came from.
GOLD_REASON="root ADR-0166: by-pnu-pack-operator gold-rebuild, the Gold a section pack generation is baked from"
GOLD_LOG="${BAKE_ROOT}/gold-rebuild/logs/gold-rebuild.log"

contract() {
  python3 -I - "${RELEASE}/config/r2-connections.contract.json" "$1" <<'PY'
import json, sys
value = json.load(open(sys.argv[1], encoding="utf-8"))
for part in sys.argv[2].split("."):
    value = value[part]
print(value)
PY
}

# Refuses (75) while a unit in "$@" runs.
refuse_while_active() {
  local unit
  for unit in "$@"; do
    if systemctl is-active --quiet "${unit}"; then refuse "${unit} is running" 75; fi
  done
}
# Refuses (75) while a by-PNU bake runs: the scheduled one, a pack generation bake, or anything else
# holding a lane lock (a hand-run publish takes it too). The lock is only tested, never created.
refuse_while_baking() {
  local lane lock
  refuse_while_active "${BAKE_SCHEDULED}"
  if [[ -n "$(systemctl list-units --plain --no-legend --state=active,activating "${PACK_BAKES}" 2>/dev/null)" ]]; then
    refuse "a section pack bake is running ($(systemctl list-units --plain --no-legend --state=active,activating "${PACK_BAKES}" | cut -d' ' -f1 | tr '\n' ' '))" 75
  fi
  for lane in building parcel; do
    lock="${BAKE_ROOT}/${lane}/lane.lock"
    if [[ -e "${lock}" ]] && ! flock -n "${lock}" true; then refuse "a run of the ${lane} lane holds ${lock}" 75; fi
  done
}

if [[ "${LANE}" == silver ]]; then
  SILVER_LANE="${1:-}"
  (($# == 1)) || refuse "silver ${ACTION} takes one Silver refresh lane"
  # The lanes are the jobs that run the template unit (orchestration/jobs.v1.json, root ADR-0169).
  mapfile -t silver_lanes < <(python3 -I - "${RELEASE}/orchestration/jobs.v1.json" <<'PY'
import json, re, sys
for job in json.load(open(sys.argv[1], encoding="utf-8"))["jobs"]:
    match = re.fullmatch(r"foundation-silver-refresh@([a-z0-9-]+)\.service", job["systemd_service"])
    if match:
        print(match.group(1))
PY
  )
  printf '%s\n' "${silver_lanes[@]}" | grep -qxF -- "${SILVER_LANE}" \
    || refuse "${SILVER_LANE:-<none>} is not a Silver refresh lane (${silver_lanes[*]})"
  SILVER_UNIT="foundation-silver-refresh@${SILVER_LANE}.service"
  case "${ACTION}" in
    plan)
      mapfile -t plan_props < <(python3 -I "${RELEASE}/scripts/deploy/runtime_secrets.py" properties silver-refresh-plan | tr ' ' '\n' | grep -v '^$')
      ((${#plan_props[@]} > 0)) || refuse "the runtime-secrets contract names no environment for silver-refresh-plan" 65
      log "planning ${SILVER_LANE} (writes nothing)"
      exec "${SYSTEMD_RUN}" --wait --pipe --collect --quiet -p User="${SERVICE_OWNER}" -p Group="${SERVICE_GROUP}" \
        "${plan_props[@]}" "${RELEASE}/scripts/ops/silver-refresh.sh" "${SILVER_LANE}" --plan
      ;;
    start)
      # One Spark lane at a time beside the Gold rebuild and the bakes, like the scheduled pool.
      refuse_while_active "${SILVER_UNIT}" "${GOLD_UNIT}" "${GOLD_SCHEDULED}"
      if [[ -n "$(systemctl list-units --plain --no-legend --state=active,activating 'foundation-silver-refresh@*.service' 2>/dev/null)" ]]; then
        refuse "another Silver refresh lane is running" 75
      fi
      log "starting ${SILVER_UNIT}; follow it with: silver status ${SILVER_LANE}"
      exec systemctl start --no-block "${SILVER_UNIT}"
      ;;
    status)
      systemctl status --no-pager --lines=0 "${SILVER_UNIT}" || true
      exec journalctl -u "${SILVER_UNIT}" --no-pager -o short-iso -n 40
      ;;
    *) refuse "${USAGE}" ;;
  esac
fi

if [[ "${ACTION}" == gold-rebuild ]]; then
  (($# == 0)) || refuse "gold-rebuild takes nothing after it: it rebuilds both panel Gold tables, and its reason is fixed"
  refuse_while_active "${GOLD_UNIT}" "${GOLD_SCHEDULED}"
  refuse_while_baking
  # The scheduled unit is the one definition of how a rebuild runs: account, environment files,
  # time bound, the Spark cleanup after every exit, sandbox and state directory. This starts a
  # transient copy of it under its own name with the unconditional reason added to its command.
  mapfile -t unit < <(python3 -I - "${RELEASE}/infra/systemd/${GOLD_SCHEDULED}" "${GOLD_UNIT}.service" <<'PY'
import shlex, sys
path, name = sys.argv[1], sys.argv[2]
section, command, properties = None, None, []
for raw in open(path, encoding="utf-8"):
    line = raw.strip()
    if not line or line.startswith(("#", ";")):
        continue
    if line.startswith("["):
        section = line
        if section not in ("[Unit]", "[Service]"):
            sys.exit(f"{path}: section {section} has no transient equivalent")
        continue
    key, _, value = line.partition("=")
    if key == "Description":
        continue
    if key == "ExecStart":
        argv = shlex.split(value)
        if len(argv) != 2 or not argv[0].endswith("/scripts/ops/gold-panel-rebuild.sh") or argv[1] != "all":
            sys.exit(f"{path}: ExecStart is not gold-panel-rebuild.sh all: {value}")
        command = argv[0]
        continue
    value = value.replace("%n", name)
    if "%" in value:
        sys.exit(f"{path}: {key}={value} holds a specifier a transient unit does not expand")
    properties.append(f"{key}={value}")
if command is None:
    sys.exit(f"{path}: no ExecStart")
print(command)
print("\n".join(properties))
PY
  )
  ((${#unit[@]} > 1)) || refuse "cannot read ${RELEASE}/infra/systemd/${GOLD_SCHEDULED}" 65
  props=()
  for property in "${unit[@]:1}"; do props+=(-p "${property}"); done
  install -d -o "${SERVICE_OWNER}" -g "${SERVICE_GROUP}" "${GOLD_LOG%/logs/*}" "${GOLD_LOG%/*}"
  log "starting ${GOLD_UNIT}: gold-panel-rebuild.sh all --unconditional; output ${GOLD_LOG}"
  # A oneshot unit: --no-block returns once it is queued, not when the rebuild ends.
  "${SYSTEMD_RUN}" --no-block --collect --unit="${GOLD_UNIT}" \
    --description="Foundation Platform panel Gold rebuild, unconditional, for a section pack generation (root ADR-0166)" \
    "${props[@]}" -p StandardOutput=append:"${GOLD_LOG}" -p StandardError=append:"${GOLD_LOG}" \
    "${unit[0]}" all --unconditional "${GOLD_REASON}"
  exit 0
fi

if [[ "${ACTION}" == health ]]; then
  if [[ "${1:-}" == --preflight ]]; then
    exec "${RELEASE}/scripts/ops/by-pnu-gateway-health.sh" "${LANE}" --preflight
  fi
  [[ "${1:-}" =~ ${UUID} ]] || refuse "health takes the new version id"
  [[ -z "${2:-}" || "${2}" =~ ${UUID} ]] || refuse "the old version id is not a version id"
  exec "${RELEASE}/scripts/ops/by-pnu-gateway-health.sh" "${LANE}" "$1" ${2:+"$2"}
fi

GENERATION="${1:-}"
[[ "$#" == 1 && "${GENERATION}" =~ ^[1-9][0-9]{0,3}$ ]] || refuse "${ACTION} takes a generation number"
WORK="${BAKE_ROOT}/${LANE}-pack-g${GENERATION}"
UNIT="foundation-${LANE}-pack-${ACTION}-g${GENERATION}"

case "${ACTION}" in
  status)
    for action in bake equality latency publish; do
      unit="foundation-${LANE}-pack-${action}-g${GENERATION}"
      printf '%s: %s\n' "${unit}" "$(systemctl is-active "${unit}" 2>/dev/null || true)"
      [[ ! -f "${WORK}/logs/${action}.log" ]] || tail -3 "${WORK}/logs/${action}.log" | cut -c1-300
    done
    for unit in "${GOLD_UNIT}" "${GOLD_SCHEDULED}" "${BAKE_SCHEDULED}"; do
      printf '%s: %s\n' "${unit}" "$(systemctl is-active "${unit}" 2>/dev/null || true)"
    done
    [[ ! -f "${GOLD_LOG}" ]] || tail -3 "${GOLD_LOG}" | cut -c1-300
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
  bake | equality | latency | publish) ;;
  *) refuse "${USAGE}" ;;
esac

[[ "${ACTION}" == bake || -d "${WORK}/summaries" ]] || refuse "${WORK}/summaries does not exist; bake generation ${GENERATION} first" 65
if systemctl is-active --quiet "${UNIT}"; then refuse "${UNIT} is already running" 75; fi
install -d -o "${SERVICE_OWNER}" -g "${SERVICE_GROUP}" "${WORK}" "${WORK}/logs"
# The admitted binary of the current release, as every job binds it (root ADR-0134 §3).
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
release_id="${RELEASE_ID}"
props=() env=() memory=8G
case "${ACTION}" in
  bake)
    # A Gold that moves under the bake stops it; the lane lock (the bake takes it) keeps it apart
    # from the scheduled bake.
    refuse_while_active "${GOLD_UNIT}" "${GOLD_SCHEDULED}"
    read -ra props <<<"$(cd "${RELEASE}" && python3 scripts/deploy/runtime_secrets.py properties section-pack-bake)"
    # Each worker exports one shard; the scheduled bake's MemoryMax is the measured bound of one
    # shard's export (its unit file says how it was measured), so the bake gets one per worker.
    source "$(dirname "${BASH_SOURCE[0]}")/by-pnu-bake-shards.sh"
    shard_memory="$(sed -n 's/^MemoryMax=\([0-9]\+\)G$/\1/p' "${RELEASE}/infra/systemd/${BAKE_SCHEDULED}")"
    [[ "${shard_memory}" =~ ^[0-9]+$ ]] || refuse "${BAKE_SCHEDULED} states no MemoryMax=<n>G" 65
    memory="$((shard_memory * BY_PNU_PACK_BAKE_WORKERS))G"
    props+=(-p MemorySwapMax=0 -p OOMPolicy=continue)
    run=("${here}/by-pnu-pack-bake.sh" "${LANE}" "${GENERATION}")
    command="by-pnu-pack-bake.sh ${LANE} ${GENERATION}"
    ;;
  equality)
    read -ra props <<<"$(cd "${RELEASE}" && python3 scripts/deploy/runtime_secrets.py properties section-pack-operator)"
    # The gate writes the generation's parts index too (root ADR-0163), to the lane's R2 output.
    env=("${P}_OUTPUT_STORAGE_DRIVER=r2" "${P}_PACK_SUMMARY_DIR=${WORK}/summaries"
      "${P}_PACK_EQUALITY_EVIDENCE_PATH=${WORK}/equality.json")
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
    # Gate (가) must have passed. Gate (나) must exist; whether it opens the publish is the
    # publisher's to judge, since only it applies the contract's latency waivers (root ADR-0162).
    python3 -I -c 'import json,sys; sys.exit(0 if json.load(open(sys.argv[1])).get("passed") else 1)' \
      "${WORK}/equality.json" 2>/dev/null || refuse "${WORK}/equality.json did not pass" 65
    [[ -s "${WORK}/latency.json" ]] || refuse "${WORK}/latency.json does not exist; run latency first" 65
    read -ra props <<<"$(cd "${RELEASE}" && python3 scripts/deploy/runtime_secrets.py properties section-pack-operator)"
    snapshot="$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["gold_iceberg_snapshot_id"])' \
      "$(ls "${WORK}"/summaries/*.json | head -1)")"
    env=("${P}_OUTPUT_STORAGE_DRIVER=r2" "${P}_CONFIRM_PACK_PUBLISH=true" "${P}_PACK_SUMMARY_DIR=${WORK}/summaries"
      "${P}_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID=${snapshot}"
      "${P}_PACK_EQUALITY_EVIDENCE_PATH=${WORK}/equality.json" "${P}_PACK_LATENCY_EVIDENCE_PATH=${WORK}/latency.json")
    command="publish-${LANE}-by-pnu-section-packs"
    ;;
esac
[[ "${ACTION}" == bake ]] || run=("${PUBLISHER_BIN}" "${command}")
setenv=()
for value in ${env[@]+"${env[@]}"}; do setenv+=(-E "${value}"); done
log "starting ${UNIT}: ${command} (release ${release_id}); output ${WORK}/logs/${ACTION}.log"
"${SYSTEMD_RUN}" --collect --unit="${UNIT}" -p User=foundation-platform -p MemoryMax="${memory}" \
  -p WorkingDirectory="${RELEASE}" "${props[@]}" ${setenv[@]+"${setenv[@]}"} \
  -p StandardOutput=append:"${WORK}/logs/${ACTION}.log" -p StandardError=append:"${WORK}/logs/${ACTION}.log" \
  "${run[@]}"
