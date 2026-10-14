#!/usr/bin/env bash
# The operator's root steps of a by-PNU section pack cut-over (root ADR-0147, ADR-0160, ADR-0161,
# ADR-0166), one fixed action at a time:
#
#   by-pnu-pack-operator.sh <building|parcel|<a by_pnu_lanes lane>> bake <generation>
#   by-pnu-pack-operator.sh <building|parcel> equality <generation>
#   by-pnu-pack-operator.sh <building|parcel> latency <generation>
#   by-pnu-pack-operator.sh <building|parcel> publish <generation>
#   by-pnu-pack-operator.sh <building|parcel|<a by_pnu_lanes lane>> monitor-sample <generation>
#   by-pnu-pack-operator.sh <building|parcel|<a by_pnu_lanes lane>> health <new-version-id> [<old-version-id>] | --preflight
#   by-pnu-pack-operator.sh <building|parcel> status <generation>
#   by-pnu-pack-operator.sh <building|parcel> gold-rebuild
#   by-pnu-pack-operator.sh <a by_pnu_lanes lane> gold-rebuild
#   by-pnu-pack-operator.sh <a by_pnu_lanes lane|lanes> cutover <generation> [--dry-run]
#   by-pnu-pack-operator.sh gold rebuild <a table's unit of gold-panel-rebuild.contract.json> [--dry-run]
#   by-pnu-pack-operator.sh <building|parcel> kind-sizes
#   by-pnu-pack-operator.sh silver plan|start|status <silver-lane>
#   by-pnu-pack-operator.sh job start|status <disabled job id>
#   by-pnu-pack-operator.sh raon build|plan|status
#   by-pnu-pack-operator.sh raon run <max-files|all> <new-bytes-budget>
#
# `cutover` is the supervised first cut-over of pack-only lanes (root ADR-0190, ADR-0193 §6): the
# release's by-pnu-lane-cutover.sh runs, lane after lane, its Gold, the parity with the bundle, then
# bake, equality, latency, publish and monitor-sample for <generation>, each through this script,
# stopping at the first failure. It runs as root in foundation-by-pnu-lane-cutover-g<generation>
# (output <bake root>/cutover/logs/cutover-g<generation>.log); --dry-run runs it here and starts
# nothing. `lanes` is every lane not cut over yet. The steps it calls wait for their unit
# (FOUNDATION_OPERATOR_WAIT, which sudo's env_reset removes from a granted run: only the root
# runner sets it).
#
# `gold rebuild <unit>` rebuilds that one Gold table the way `gold-rebuild` does, and only when an
# input moved: a table's first full build is run and timed outside the scheduled job this way (root
# ADR-0193 §7), so it does not first meet the job's time bound.
# `gold rebuild <unit> --dry-run` runs it with gold-panel-rebuild.sh --dry-run: the whole build and
# every check, nothing committed, and an input listed under unmeasured_inputs may be read. That is
# the measured run an input must pass before it leaves that list.
#
# `raon` drives the RAON large-file lane's supervised runs (root ADR-0170, runbook
# provider-acquisition-fargate.md, first supervised run): `build` and `plan` run
# raon-large-files.sh as the service user and wait; `run` starts it detached as
# foundation-raon-large-files-operator.service against the daily sweep's evidence, with that run's
# file count and byte budget; `status` shows that unit. None starts while the daily sweep runs.
#
# `silver` drives one Silver refresh lane (root ADR-0169, ADR-0178): `plan` runs its
# `silver-refresh.sh <lane> --plan` (reads the ledger and the catalog, writes nothing) and prints
# the outcome; `start` starts its scheduled unit foundation-silver-refresh@<lane>.service once, the
# supervised first run before the job is enabled; `status` shows the unit and the end of its journal.
# The lanes are the jobs.v1.json jobs whose service is that template; nothing else is accepted.
#
# `job` is the same supervised first run for any other job that jobs.v1.json lists as off
# (`enabled: false`, ADR-0122 §4): `start` starts its systemd service once, never while the
# autodeploy runs, and a spark-pool job never beside the Gold rebuilds or a Silver lane; `status`
# shows the unit and the end of its journal. An enabled job is Airflow's to start
# (start-scheduled-job.sh), and a Silver lane takes `silver`.
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
# published. `gold-rebuild` rebuilds every panel Gold table unconditionally, once, the way the
# scheduled foundation-gold-panel-rebuild.service runs (its unit file is the definition), with a
# fixed reason; it takes no argument beyond the lane. For a pack-only lane it rebuilds that lane's
# Gold only (gold-panel-rebuild.sh <its table's unit>), and only when an input moved: a lane's
# first Gold has no snapshot, so it is always built. Neither starts while the other, the scheduled
# Gold rebuild or a by-PNU bake runs: a Gold that moves under a bake stops the bake.
set -euo pipefail
log() { printf '%s by-pnu-pack-operator: %s\n' "$(date -u +%FT%TZ)" "$*" >&2; }
refuse() { log "refused: $1"; exit "${2:-64}"; }
USAGE="usage: by-pnu-pack-operator.sh <building|parcel> bake|equality|latency|publish|monitor-sample|status <generation> | gold-rebuild | kind-sizes | <lane|lanes> cutover <generation> [--dry-run] | gold rebuild <unit> [--dry-run] | health <new-version> [<old-version>] | health --preflight | silver plan|start|status <silver-lane> | job start|status <disabled job id> | raon build|plan|status | raon run <max-files|all> <new-bytes-budget>"

# The pack-only lanes the contract this script ships with names (root ADR-0190).
mapfile -t PACK_LANES < <(python3 -I - "$(dirname "${BASH_SOURCE[0]}")/../../config/r2-connections.contract.json" <<'PY'
import json, sys
print("\n".join(json.load(open(sys.argv[1], encoding="utf-8")).get("by_pnu_lanes", {})))
PY
)
is_pack_lane() { [[ -n "$1" && " ${PACK_LANES[*]-} " == *" $1 "* ]]; }
LANE="${1:-}" ACTION="${2:-}"
# A pack-only lane of the contract (root ADR-0190) takes bake, equality, latency, publish, status,
# and after its first publish monitor-sample and health: its cut-over is the pack-only gate, and from
# then on it is watched and canaried like the other lanes. Its gateway block is `by_pnu_lanes.<lane>`,
# or, for a lane written as a host and a kind (root ADR-0193 §4), its host's: the same Worker and
# preview serve it by path.
[[ "${LANE}" == building || "${LANE}" == parcel || "${LANE}" == silver || "${LANE}" == job || "${LANE}" == raon ]] || is_pack_lane "${LANE}" \
  || [[ "${LANE}" == lanes && "${ACTION}" == cutover ]] || [[ "${LANE}" == gold && "${ACTION}" == rebuild ]] \
  || refuse "${USAGE}"
if [[ "${LANE}" == lanes || "${LANE}" == gold ]]; then
  GATEWAY_BLOCK=""
elif is_pack_lane "${LANE}"; then
  [[ " bake equality latency publish status monitor-sample health gold-rebuild cutover " == *" ${ACTION} "* ]] || refuse "a pack-only lane takes bake, equality, latency, publish, status, monitor-sample, health, gold-rebuild or cutover"
  GATEWAY_BLOCK="$(python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["by_pnu_lanes"][sys.argv[2]].get("host") or "by_pnu_lanes." + sys.argv[2])'     "$(dirname "${BASH_SOURCE[0]}")/../../config/r2-connections.contract.json" "${LANE}")"
else
  GATEWAY_BLOCK="${LANE}_by_pnu_gateway"
fi
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
P="FOUNDATION_PLATFORM_$(tr 'a-z-' 'A-Z_' <<<"${LANE}")_BY_PNU_SERVING"
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
# The data kinds' rebuild (root ADR-0193): it moves the kind Golds a kind lane bakes.
GOLD_KINDS=foundation-gold-kind-rebuild.service
GOLD_UNIT=foundation-gold-panel-rebuild-unconditional
BAKE_SCHEDULED=foundation-by-pnu-serving-bake.service
PACK_BAKES='foundation-*-pack-bake-g*.service'
# The rebuild's one reason, so the plan and the Gold history name where it came from.
GOLD_REASON="root ADR-0166: by-pnu-pack-operator gold-rebuild, the Gold a section pack generation is baked from"
GOLD_LOG="${BAKE_ROOT}/gold-rebuild/logs/gold-rebuild.log"
# Set only by by-pnu-lane-cutover.sh (root, not through sudo): a started step waits for its unit and
# exits with its result, so the runner can stop at the first failure.
WAIT=()
[[ "${FOUNDATION_OPERATOR_WAIT:-}" != 1 ]] || WAIT=(--wait)

contract() {
  python3 -I - "${RELEASE}/config/r2-connections.contract.json" "$1" <<'PY'
import json, sys
value = json.load(open(sys.argv[1], encoding="utf-8"))
for part in sys.argv[2].split("."):
    value = value[part]
print(value)
PY
}

# "<unit> <state>" for each service in "$@" (names or patterns) that runs, by job_units.py: the one
# definition the deploy and release admission use too (root ADR-0184). A oneshot job is `activating`
# for its whole run and never `active`, so `systemctl is-active` called a running Gold rebuild
# stopped (2026-10-10). Refuses (70) when systemd cannot say; that is never "nothing runs".
running_units() {
  python3 -I -B "${RELEASE}/scripts/deploy/job_units.py" running "$@" \
    || refuse "systemd cannot tell whether $* run" 70
}
# Refuses (75) while a unit in "$@" runs. Assigned before it is tested: a refusal inside
# `[[ -n "$(...)" ]]` would not stop the script.
refuse_while_active() {
  local unit running
  local -a units=()
  for unit in "$@"; do
    [[ "${unit}" == *.service ]] || unit="${unit}.service"
    units+=("${unit}")
  done
  running="$(running_units "${units[@]}")"
  [[ -z "${running}" ]] || refuse "$(cut -d' ' -f1 <<<"${running}" | paste -sd' ') is running" 75
}
# Refuses (75) while a by-PNU bake runs: the scheduled one, a pack generation bake, or anything else
# holding a lane lock (a hand-run publish takes it too). The lock is only tested, never created.
refuse_while_baking() {
  local lane lock running
  refuse_while_active "${BAKE_SCHEDULED}"
  running="$(running_units "${PACK_BAKES}")"
  [[ -z "${running}" ]] || refuse "a section pack bake is running ($(cut -d' ' -f1 <<<"${running}" | paste -sd' '))" 75
  for lane in building parcel ${PACK_LANES[@]+"${PACK_LANES[@]}"}; do
    [[ -n "${lane}" ]] || continue
    lock="${BAKE_ROOT}/${lane}/lane.lock"
    if [[ -e "${lock}" ]] && ! flock -n "${lock}" true; then refuse "a run of the ${lane} lane holds ${lock}" 75; fi
  done
}

if [[ "${LANE}" == raon ]]; then
  RAON_UNIT=foundation-raon-large-files-operator.service
  # The daily sweep writes this evidence and runs the lane itself once it has a budget; a
  # supervised run never overlaps it (runbook provider-acquisition-fargate.md).
  RAON_EVIDENCE=/var/lib/foundation-platform/source-sweep/vworld-evidence.json
  raon_props() {
    mapfile -t props < <(python3 -I "${RELEASE}/scripts/deploy/runtime_secrets.py" properties raon-large-files | tr ' ' '\n' | grep -v '^$')
    ((${#props[@]} > 0)) || refuse "the runtime-secrets contract names no environment for raon-large-files" 65
  }
  case "${ACTION}" in
    build|plan)
      (($# == 0)) || refuse "raon ${ACTION} takes nothing after it"
      refuse_while_active foundation-source-sweep.service "${RAON_UNIT}"
      raon_props
      args=("${ACTION}")
      [[ "${ACTION}" == plan ]] && args+=("${RAON_EVIDENCE}")
      log "raon ${ACTION} (waits; nothing is collected)"
      exec "${SYSTEMD_RUN}" --wait --pipe --collect --quiet -p User="${SERVICE_OWNER}" -p Group="${SERVICE_GROUP}" \
        "${props[@]}" "${RELEASE}/scripts/ops/raon-large-files.sh" "${args[@]}"
      ;;
    run)
      (($# == 2)) || refuse "raon run takes <max-files|all> <new-bytes-budget>"
      [[ "$1" == all || "$1" =~ ^[1-9][0-9]{0,3}$ ]] || refuse "max-files must be all or 1-9999"
      [[ "$2" =~ ^[1-9][0-9]{0,13}$ ]] || refuse "new-bytes-budget must be a positive byte count"
      refuse_while_active foundation-source-sweep.service "${RAON_UNIT}"
      raon_props
      limits=(-E "FOUNDATION_RAON_LARGE_FILES_NEW_BYTES_BUDGET=$2")
      [[ "$1" == all ]] || limits+=(-E "FOUNDATION_RAON_LARGE_FILES_MAX_FILES=$1")
      log "starting ${RAON_UNIT} (max files $1, budget $2 bytes); follow it with: raon status"
      exec "${SYSTEMD_RUN}" --unit="${RAON_UNIT%.service}" --collect --quiet -p User="${SERVICE_OWNER}" \
        -p Group="${SERVICE_GROUP}" "${props[@]}" "${limits[@]}" \
        "${RELEASE}/scripts/ops/raon-large-files.sh" run "${RAON_EVIDENCE}"
      ;;
    status)
      (($# == 0)) || refuse "raon status takes nothing after it"
      systemctl status --no-pager --lines=0 "${RAON_UNIT}" || true
      exec journalctl -u "${RAON_UNIT}" --no-pager -o short-iso -n 40
      ;;
    *) refuse "${USAGE}" ;;
  esac
fi

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
      refuse_while_active "${SILVER_UNIT}" "${GOLD_UNIT}" "${GOLD_SCHEDULED}" "${GOLD_KINDS}"
      running="$(running_units 'foundation-silver-refresh@*.service')"
      [[ -z "${running}" ]] || refuse "another Silver refresh lane is running" 75
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

if [[ "${LANE}" == job ]]; then
  JOB_ID="${1:-}"
  (($# == 1)) || refuse "job ${ACTION} takes one job id"
  # The job's service and pool, only when the release lists it as off and it is not a Silver lane.
  read -r JOB_UNIT JOB_POOL < <(python3 -I - "${RELEASE}/orchestration/jobs.v1.json" "${JOB_ID}" <<'PY'
import json, sys
for job in json.load(open(sys.argv[1], encoding="utf-8"))["jobs"]:
    if job["id"] == sys.argv[2] and job["enabled"] is False             and not job["systemd_service"].startswith("foundation-silver-refresh@"):
        print(job["systemd_service"], job["pool"])
PY
  ) || true
  [[ -n "${JOB_UNIT:-}" ]] || refuse "${JOB_ID:-<none>} is not a job jobs.v1.json lists as off (an enabled job is Airflow's; a Silver lane takes 'silver')"
  case "${ACTION}" in
    start)
      refuse_while_active "${JOB_UNIT}" foundation-autodeploy.service
      if [[ "${JOB_POOL}" == spark ]]; then
        refuse_while_active "${GOLD_UNIT}" "${GOLD_SCHEDULED}" "${GOLD_KINDS}"
        running="$(running_units 'foundation-silver-refresh@*.service')"
        [[ -z "${running}" ]] || refuse "a Silver refresh lane is running" 75
      fi
      log "starting ${JOB_UNIT} once (supervised first run); follow it with: job status ${JOB_ID}"
      exec systemctl start --no-block "${JOB_UNIT}"
      ;;
    status)
      systemctl status --no-pager --lines=0 "${JOB_UNIT}" || true
      exec journalctl -u "${JOB_UNIT}" --no-pager -o short-iso -n 40
      ;;
    *) refuse "${USAGE}" ;;
  esac
fi

if [[ "${ACTION}" == kind-sizes ]]; then
  # Root ADR-0193 §6-1: what each data kind of the lane's panel weighs as its own lane. Read-only,
  # in the small Spark, never beside a Gold rebuild or a bake (the host's memory budget).
  (($# == 0)) || refuse "kind-sizes takes nothing after it"
  [[ "${LANE}" == parcel || "${LANE}" == building ]] || refuse "kind-sizes measures the parcel or the building panel"
  refuse_while_active "${GOLD_UNIT}" "${GOLD_SCHEDULED}" "foundation-${LANE}-kind-sizes"
  refuse_while_baking
  read -ra props <<<"$(cd "${RELEASE}" && python3 scripts/deploy/runtime_secrets.py properties section-pack-operator)"
  install -d -o "${SERVICE_OWNER}" -g "${SERVICE_GROUP}" "${BAKE_ROOT}/kind-sizes"
  log "starting foundation-${LANE}-kind-sizes (read-only); its summary path is the last line of ${BAKE_ROOT}/kind-sizes/${LANE}.log"
  exec "${SYSTEMD_RUN}" --no-block --collect --unit="foundation-${LANE}-kind-sizes" -p User="${SERVICE_OWNER}" \
    -p Group="${SERVICE_GROUP}" "${props[@]}" -p StandardOutput=append:"${BAKE_ROOT}/kind-sizes/${LANE}.log" \
    -p StandardError=append:"${BAKE_ROOT}/kind-sizes/${LANE}.log" "${RELEASE}/scripts/ops/by-pnu-kind-sizes.sh" "${LANE}"
fi

if [[ "${ACTION}" == cutover ]]; then
  is_pack_lane "${LANE}" || [[ "${LANE}" == lanes ]] || refuse "cutover is the first cut-over of a pack-only lane, or of lanes"
  [[ "$#" == 1 || ( "$#" == 2 && "${2}" == --dry-run ) ]] || refuse "cutover takes <generation> [--dry-run]"
  [[ "${1}" =~ ^[1-9][0-9]{0,3}$ ]] || refuse "cutover takes a generation number"
  if [[ "$#" == 2 ]]; then
    exec "${here}/by-pnu-lane-cutover.sh" "${LANE}" "$1" --dry-run
  fi
  CUTOVER_UNIT="foundation-by-pnu-lane-cutover-g$1"
  running="$(running_units 'foundation-by-pnu-lane-cutover-g*.service')"
  [[ -z "${running}" ]] || refuse "a lane cut-over is running ($(cut -d' ' -f1 <<<"${running}" | paste -sd' '))" 75
  install -d -o "${SERVICE_OWNER}" -g "${SERVICE_GROUP}" "${BAKE_ROOT}/cutover" "${BAKE_ROOT}/cutover/logs"
  log="${BAKE_ROOT}/cutover/logs/cutover-g$1.log"
  log "starting ${CUTOVER_UNIT}: by-pnu-lane-cutover.sh ${LANE} $1; output ${log}"
  # Root, like this script: each step it calls starts its own unit as the service user.
  exec "${SYSTEMD_RUN}" --collect --unit="${CUTOVER_UNIT}" \
    --description="Foundation Platform pack-only lane cut-over, generation $1 (root ADR-0190, ADR-0193)" \
    -p StandardOutput=append:"${log}" -p StandardError=append:"${log}" \
    "${here}/by-pnu-lane-cutover.sh" "${LANE}" "$1"
fi

TABLE_UNIT=""
TABLE_DRY_RUN=()
if [[ "${LANE}" == gold ]]; then
  (($# == 1)) || [[ $# == 2 && "$2" == --dry-run ]]     || refuse "gold rebuild takes one table's unit of gold-panel-rebuild.contract.json [--dry-run]"
  # Every table's unit, the kind lines' too (root ADR-0193 §9; gold_rebuild.load_contract).
  mapfile -t table_units < <(python3 -I -B -c 'import sys; sys.path.insert(0, sys.argv[1]); import gold_rebuild; print("\n".join(t["unit"] for t in gold_rebuild.load_contract()["tables"].values()))' \
    "${RELEASE}/infra/lakehouse/spark/jobs")
  printf '%s\n' "${table_units[@]}" | grep -qxF -- "$1" || refuse "${1:-<none>} is not a table's unit of gold-panel-rebuild.contract.json (${table_units[*]})"
  TABLE_UNIT="$1"
  # A dry run commits nothing and may read an input the contract lists as unmeasured: how it is measured.
  [[ "${2:-}" != --dry-run ]] || TABLE_DRY_RUN=(--dry-run)
  shift "$#"
  ACTION=gold-rebuild
fi

if [[ "${ACTION}" == gold-rebuild ]]; then
  (($# == 0)) || refuse "gold-rebuild takes nothing after it: it rebuilds the panel Gold tables, and its reason is fixed"
  refuse_while_active "${GOLD_UNIT}" "${GOLD_SCHEDULED}" "${GOLD_KINDS}"
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
  if [[ -n "${TABLE_UNIT}" ]]; then
    command=("${unit[0]}" "${TABLE_UNIT}" ${TABLE_DRY_RUN[@]+"${TABLE_DRY_RUN[@]}"})
  elif is_pack_lane "${LANE}"; then
    # The lane's own Gold, by the rebuild contract's unit for its table (root ADR-0193 §3).
    table_unit="$(python3 -I -B -c 'import sys; sys.path.insert(0, sys.argv[1]); import by_pnu_kinds, gold_rebuild; print(gold_rebuild.load_contract()["tables"][by_pnu_kinds.lane_fields(sys.argv[2])["gold_table"]]["unit"])' \
      "${RELEASE}/infra/lakehouse/spark/jobs" "${LANE}")" \
      || refuse "the rebuild contract has no table for the ${LANE} lane's Gold" 65
    command=("${unit[0]}" "${table_unit}")
  else
    command=("${unit[0]}" all --unconditional "${GOLD_REASON}")
  fi
  log "starting ${GOLD_UNIT}: gold-panel-rebuild.sh ${command[*]:1:2}; output ${GOLD_LOG}"
  # A oneshot unit: --no-block returns once it is queued, not when the rebuild ends; --wait (the
  # cut-over runner) returns with its result.
  ((${#WAIT[@]})) || WAIT=(--no-block)
  exec "${SYSTEMD_RUN}" "${WAIT[@]}" --collect --unit="${GOLD_UNIT}" \
    --description="Foundation Platform panel Gold rebuild, unconditional, for a section pack generation (root ADR-0166)" \
    "${props[@]}" -p StandardOutput=append:"${GOLD_LOG}" -p StandardError=append:"${GOLD_LOG}" \
    "${command[@]}"
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
    for unit in "${GOLD_UNIT}" "${GOLD_SCHEDULED}" "${GOLD_KINDS}" "${BAKE_SCHEDULED}"; do
      printf '%s: %s\n' "${unit}" "$(systemctl is-active "${unit}" 2>/dev/null || true)"
    done
    [[ ! -f "${GOLD_LOG}" ]] || tail -3 "${GOLD_LOG}" | cut -c1-300
    exit 0
    ;;
  monitor-sample)
    # The hourly monitor and the canary read the gate sample from the lane's file (runtime-secrets
    # group <lane>-serving-monitor); it names this generation's equality evidence. Then the lane's
    # hourly monitor is switched on (already on for a lane cut over before): for a pack-only lane this
    # step right after its first publish is what starts the watching, and the Worker autodeploy smoke
    # runs the monitor from the moment this file exists (root ADR-0190).
    # A data kind lane (root ADR-0193) has no group of its own: every kind's line is in the kinds' one
    # file, and this step replaces its own line there.
    [[ -s "${WORK}/equality.json" ]] || refuse "${WORK}/equality.json does not exist; run equality first" 65
    group="${LANE}-serving-monitor"
    ! is_pack_lane "${LANE}" || group="$(python3 -I "${RELEASE}/infra/lakehouse/spark/jobs/by_pnu_kinds.py" "${LANE}" monitor_group)"
    file="$(python3 -I -c 'import json,sys; print(next(g["path"] for g in json.load(open(sys.argv[1]))["groups"] if g["name"] == sys.argv[2]))' \
      "${RELEASE}/config/runtime-secrets.contract.json" "${group}")"
    file="${ETC}/$(basename "${file}")"
    # Read the other lanes' lines before install truncates the file: a grep piped into it raced it.
    kept="$([[ ! -f "${file}" ]] || grep -v "^${P}_MONITOR_SAMPLE_PATH=" "${file}" || true)"
    { [[ -z "${kept}" ]] || printf '%s\n' "${kept}"
      printf '%s_MONITOR_SAMPLE_PATH=%s\n' "${P}" "${WORK}/equality.json"; } |
      install -m 0640 -o "${ROOT_OWNER}" -g "${SERVICE_GROUP}" /dev/stdin "${file}"
    log "${file} names ${WORK}/equality.json"
    systemctl enable --now "foundation-by-pnu-serving-monitor@${LANE}.timer"
    log "foundation-by-pnu-serving-monitor@${LANE}.timer is on"
    exit 0
    ;;
  bake | equality | latency | publish) ;;
  *) refuse "${USAGE}" ;;
esac

[[ "${ACTION}" == bake || -d "${WORK}/summaries" ]] || refuse "${WORK}/summaries does not exist; bake generation ${GENERATION} first" 65
running="$(running_units "${UNIT}.service")"
[[ -z "${running}" ]] || refuse "${UNIT} is already running" 75
install -d -o "${SERVICE_OWNER}" -g "${SERVICE_GROUP}" "${WORK}" "${WORK}/logs"
# The admitted binary of the current release, as every job binds it (root ADR-0134 §3).
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
release_id="${RELEASE_ID}"
props=() env=() memory=8G
case "${ACTION}" in
  bake)
    # A Gold that moves under the bake stops it; the lane lock (the bake takes it) keeps it apart
    # from the scheduled bake.
    refuse_while_active "${GOLD_UNIT}" "${GOLD_SCHEDULED}" "${GOLD_KINDS}"
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
    # The generation is this action's, never an environment file's: a lane's first cut-over
    # (2026-10-13, the parcel kind lanes) has no file naming it, and the gate refuses without it.
    env=("${P}_OUTPUT_STORAGE_DRIVER=r2" "${P}_PACK_GENERATION=${GENERATION}" "${P}_PACK_SUMMARY_DIR=${WORK}/summaries"
      "${P}_PACK_EQUALITY_EVIDENCE_PATH=${WORK}/equality.json")
    command="verify-${LANE}-by-pnu-section-pack-equality"
    ;;
  latency)
    [[ -s "${WORK}/equality.json" ]] || refuse "gate (나) draws its sample from ${WORK}/equality.json; run equality first" 65
    read -ra props <<<"$(cd "${RELEASE}" && python3 scripts/deploy/runtime_secrets.py properties section-pack-latency-probe)"
    preview="https://$(contract "${GATEWAY_BLOCK}.section_packs.preview_worker.public_hostname")"
    if [[ -f "${WORK}/latency.json" ]]; then
      cp -p "${WORK}/latency.json" "${WORK}/latency-$(date -u -r "${WORK}/latency.json" +%Y%m%dT%H%M%SZ).json"
    fi
    # A pack-only lane's probe reads its unpublished packs back from R2 to know each answer's content.
    env=("${P}_OUTPUT_STORAGE_DRIVER=r2" "${P}_PACK_GENERATION=${GENERATION}" "${P}_PACK_PREVIEW_BASE_URL=${preview}"
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
"${SYSTEMD_RUN}" ${WAIT[@]+"${WAIT[@]}"} --collect --unit="${UNIT}" -p User=foundation-platform -p MemoryMax="${memory}" \
  -p WorkingDirectory="${RELEASE}" "${props[@]}" ${setenv[@]+"${setenv[@]}"} \
  -p StandardOutput=append:"${WORK}/logs/${ACTION}.log" -p StandardError=append:"${WORK}/logs/${ACTION}.log" \
  "${run[@]}"
