#!/usr/bin/env bash
# The scheduled panel Gold rebuild (root ADR-0139, ADR-0122).
#
#   gold-panel-rebuild.sh parcel|building|all [--dry-run] [--unconditional <reason>]
#   gold-panel-rebuild.sh cleanup        (the unit's ExecStopPost)
#
# `all` (the registered job) rebuilds gold.parcel_panel, then gold.building_panel, in the order of
# infra/lakehouse/contracts/gold-panel-rebuild.contract.json. One Spark run at a time: each takes
# the whole compose `spark` cap (root ADR-0138). A failed table does not stop the other; `all`
# fails if either did. One run of a table:
#
# 1. plans (infra/lakehouse/spark/jobs/gold_rebuild.py): reads the Gold table's and its Silver
#    inputs' snapshot histories from the catalog. No input changed rows since the Silver snapshots
#    the current Gold was built from: "nothing to do", exit 0. Otherwise the plan pins every input
#    to its current snapshot and states the fewest rows the new Gold may have.
# 2. runs the table's producer with those pins and that floor. The producer refuses, before it
#    writes, a Gold that fails its quality gates or has fewer rows than the floor; a refused run
#    commits nothing. A passing run commits one new Gold snapshot (the old ones stay in the
#    table's history) that records the pins in its summary, so the next plan starts from it.
#    When the plan says `incremental` (root ADR-0180) the producer recomputes only the PNUs whose
#    inputs changed since the current Gold's pins and merges them; past the contract's
#    max_changed_key_fraction, or when its parity sample disagrees, it rebuilds whole instead. An
#    incremental run whose recomputed rows all came out unchanged records the new pins only and
#    does not count as a change for the bake.
#
# `all` ends with `foundation-job-outcome changed` when a table committed a snapshot, `unchanged`
# otherwise (root ADR-0171): Airflow starts the by-PNU bake only on `changed`.
#
# --dry-run runs the producer with --validate-only: the whole transform and every check, no
# commit. The by-PNU bake (by-pnu-serving-bake.sh) bakes a Gold snapshot its published generation
# does not serve, so a committed rebuild is baked on the bake's next turn.
#
# --unconditional <reason> rebuilds whatever the Silver history says, still under the row floor and
# every check; the plan logs the reason. It is the supervised first run's (runbook 4): a Gold made
# before root ADR-0139 records no pins, and the planner's only other evidence for it, the rows'
# published_at_utc, is stamped when the producer started. Once the job is enabled
# (orchestration/jobs.v1.json) the planner refuses to plan from that time; a Gold without pins then
# needs an unconditional run.
#
# Spark runs in the Compose project foundation-gold-rebuild-<INVOCATION_ID>. A timeout kills this
# script and the compose client but not the daemon-owned container, which would keep its 20g while
# Airflow frees the pool. The unit's ExecStopPost (`cleanup`) has the release's publisher remove
# that project's one-off containers and networks, as FLOOR does (root ADR-0128), and nothing else.
set -euo pipefail

UNIT="${1:-}"
if [[ "${UNIT}" == cleanup ]]; then
  [[ "$#" == 1 ]] || { echo "gold-panel-rebuild: cleanup takes no arguments" >&2; exit 64; }
  [[ "${INVOCATION_ID:-}" =~ ^[0-9a-f]{32}$ ]] || { echo "gold-panel-rebuild: cleanup needs the systemd INVOCATION_ID" >&2; exit 64; }
  # Any installed release: `current` may have moved since this invocation started.
  source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --installed
  exec "${PUBLISHER_BIN}" stop-gold-panel-rebuild
fi
shift || true
OPTIONS=("$@")
VALIDATE=()
UNCONDITIONAL=""
while (($#)); do
  case "$1" in
    --dry-run) VALIDATE=(--validate-only); shift ;;
    --unconditional)
      [[ -n "${2:-}" && "${2}" != --* ]] || { echo "gold-panel-rebuild: --unconditional needs a reason" >&2; exit 64; }
      UNCONDITIONAL="$2"; shift 2 ;;
    *) echo "gold-panel-rebuild: expected --dry-run or --unconditional <reason>, got '$1'" >&2; exit 64 ;;
  esac
done
case "${UNIT}" in
  parcel | building) ;;
  all) ;;
  *) echo "gold-panel-rebuild: expected parcel, building, all or cleanup, got '${UNIT}'" >&2; exit 64 ;;
esac
# A run outside systemd (a supervised manual run) names its own project; ExecStopPost is not there
# to clean it, so the operator is told how.
if [[ -z "${INVOCATION_ID:-}" ]]; then
  INVOCATION_ID="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
  echo "gold-panel-rebuild: no systemd invocation; this run's Compose project is foundation-gold-rebuild-${INVOCATION_ID} (if interrupted: INVOCATION_ID=${INVOCATION_ID} $0 cleanup)" >&2
fi
[[ "${INVOCATION_ID}" =~ ^[0-9a-f]{32}$ ]] || { echo "gold-panel-rebuild: INVOCATION_ID is not a systemd invocation id" >&2; exit 64; }
export INVOCATION_ID
# One rebuild at a time, whoever started it: the scheduled unit, the operator's unconditional copy
# of it (by-pnu-pack-operator.sh gold-rebuild, root ADR-0166) or a supervised run. Two would each
# take the whole compose `spark` cap. Taken without waiting, like the bake's lane lock; `all` holds
# it for both tables. Opened read-only, so a file a root run created stays usable by the service user.
if [[ -z "${GOLD_PANEL_REBUILD_LOCK_HELD:-}" ]]; then
  lock="${FOUNDATION_GOLD_REBUILD_STATE_ROOT:-/var/lib/foundation-gold-panel-rebuild}/rebuild.lock"
  mkdir -p "${lock%/*}"
  [[ -e "${lock}" ]] || : >>"${lock}"
  exec 8<"${lock}"
  if ! flock -n 8; then
    echo "gold-panel-rebuild: refused: another rebuild holds ${lock}; this run starts after it finishes" >&2
    exit 75
  fi
  export GOLD_PANEL_REBUILD_LOCK_HELD=1
fi
if [[ "${UNIT}" == all ]]; then
  status=0
  # Each table that commits a snapshot names itself here; the job's last line says whether any did,
  # which is what starts the by-PNU bake (root ADR-0171).
  GOLD_PANEL_REBUILD_COMMITTED="$(mktemp)"
  export GOLD_PANEL_REBUILD_COMMITTED
  "${BASH_SOURCE[0]}" parcel ${OPTIONS[@]+"${OPTIONS[@]}"} || status=$?
  "${BASH_SOURCE[0]}" building ${OPTIONS[@]+"${OPTIONS[@]}"} || { table_status=$?; ((status)) || status=${table_status}; }
  if [[ -s "${GOLD_PANEL_REBUILD_COMMITTED}" ]]; then
    echo "foundation-job-outcome changed"
  else
    echo "foundation-job-outcome unchanged"
  fi
  rm -f "${GOLD_PANEL_REBUILD_COMMITTED}"
  exit "${status}"
fi
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
STATE_ROOT="${FOUNDATION_GOLD_REBUILD_STATE_ROOT:-/var/lib/foundation-gold-panel-rebuild}/${UNIT}"
SCRATCH="${FOUNDATION_GOLD_REBUILD_SCRATCH:-/data/foundation-platform/lakehouse/spark-scratch}"
CONTRACT="${RELEASE_ROOT}/infra/lakehouse/contracts/gold-panel-rebuild.contract.json"
JOBS="${RELEASE_ROOT}/orchestration/jobs.v1.json"
log() { printf 'gold-panel-rebuild %s: %s\n' "${UNIT}" "$*"; }

read -r gold_table producer master driver_memory < <(python3 -I - "${CONTRACT}" "${UNIT}" <<'PY'
import json, sys
contract, unit = json.load(open(sys.argv[1])), sys.argv[2]
(name, entry), = [(n, t) for n, t in contract["tables"].items() if t["unit"] == unit]
print(name, entry["producer"], entry["spark"]["master"], entry["spark"]["driver_memory"])
PY
)
[[ -n "${driver_memory:-}" ]] || { log "refused: ${CONTRACT} names no table for ${UNIT}"; exit 65; }

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
container_work="/workspace/target/lakehouse/runs/${run_id}"
mkdir -p "${work}"
# The Spark container runs as uid 185: its init step checks it can write the state root it mounts.
chmod 0777 "${STATE_ROOT}" "${work}"

spark() {
  docker rm foundation-platform-lakehouse-target-init >/dev/null 2>&1 || true
  FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${STATE_ROOT}" \
  FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
  docker compose --project-directory "${RELEASE_ROOT}" -f "${RELEASE_ROOT}/compose.lakehouse.yml" \
    -p "foundation-gold-rebuild-${INVOCATION_ID}" --profile lakehouse-batch run --rm -v "${SCRATCH}:/scratch" \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
    spark spark-submit --master "${master}" --driver-memory "${driver_memory}" \
    --conf spark.local.dir=/scratch --jars "${SPARK_RELEASE_JARS}" \
    "/workspace/infra/lakehouse/spark/jobs/$1" "${@:2}" >>"${work}/$1.log" 2>&1
}

# 1. Plan. An enabled job may not plan from a Gold's publish time (see --unconditional above); a
# missing or unreadable job list counts as enabled.
plan_options=()
if python3 -I - "${JOBS}" <<'PY'
import json, sys
try:
    jobs = json.load(open(sys.argv[1], encoding="utf-8"))["jobs"]
    enabled = next(job for job in jobs if job["id"] == "gold_panel_rebuild")["enabled"] is not False
except (OSError, ValueError, KeyError, StopIteration):
    enabled = True
sys.exit(0 if enabled else 1)
PY
then
  plan_options+=(--no-time-fallback)
fi
if [[ -n "${UNCONDITIONAL}" ]]; then
  plan_options+=(--unconditional-reason "${UNCONDITIONAL}")
fi
# A dry run may read an input the contract lists as unmeasured: it is how that input gets measured.
if ((${#VALIDATE[@]})); then
  plan_options+=(--measuring)
fi
if ! spark gold_rebuild.py --gold-table "${gold_table}" "${plan_options[@]}" \
    --plan-output "${container_work}/plan.json" --pins-output "${container_work}/pins.json"; then
  grep -a -E 'Error|Exception' "${work}/gold_rebuild.py.log" | grep -v '^\s*at ' | tail -5 >&2 || true
  log "FAILED: could not plan ${gold_table} (log ${work}/gold_rebuild.py.log)"
  exit 1
fi
mapfile -t plan < <(python3 -I - "${work}/plan.json" "${work}/previous-pins.json" <<'PY'
import json, sys
plan = json.load(open(sys.argv[1]))
print(plan["action"])
print(plan["source_snapshots"][plan["anchor_input"]])
print("" if plan["minimum_row_count"] is None else plan["minimum_row_count"])
print("; ".join(plan["reasons"]))
print(" ".join(plan["producer_arguments"]))
# Root ADR-0180: an incremental rebuild compares against the pins the current Gold was built from.
mode = plan.get("mode", "full")
print(mode)
print("; ".join(plan.get("mode_reasons") or []))
if mode == "incremental":
    with open(sys.argv[2], "w", encoding="utf-8") as previous:
        json.dump(plan["previous_source_snapshots"], previous, sort_keys=True)
    print(f'{plan["max_changed_key_fraction"]} {plan["parity_sample_keys"]}')
else:
    print("")
PY
)
action="${plan[0]}"; anchor="${plan[1]}"; minimum="${plan[2]}"; reasons="${plan[3]}"
read -r -a producer_arguments <<<"${plan[4]}"
mode="${plan[5]:-full}"; mode_reasons="${plan[6]:-}"
incremental=()
if [[ "${mode}" == incremental ]]; then
  read -r max_fraction sample_keys <<<"${plan[7]}"
  incremental=(--incremental-from-snapshots "${container_work}/previous-pins.json"
               --max-changed-key-fraction "${max_fraction}" --parity-sample-keys "${sample_keys}")
fi
if [[ "${action}" == nothing_to_do ]]; then
  log "nothing to do: no Silver input of ${gold_table} changed rows since the snapshots its current Gold was built from"
  exit 0
fi
[[ "${action}" == rebuild ]] || { log "refused: the plan says '${action}'"; exit 65; }
log "rebuilding ${gold_table}${VALIDATE[0]:+ (dry run)}: ${reasons}"
if [[ "${mode}" == incremental ]]; then
  log "incremental: only the PNUs whose inputs changed are recomputed and merged; the producer falls back to the full rebuild past ${max_fraction} of the Gold or on a parity disagreement"
else
  log "full rebuild: ${mode_reasons:-the plan names no reason}"
fi

# 2. Build, check, commit.
started=${SECONDS}
if ! spark "${producer}.py" --input-mode iceberg --write-mode iceberg \
    --iceberg-snapshot-id "${anchor}" --source-snapshots-path "${container_work}/pins.json" \
    ${minimum:+--minimum-count "${minimum}"} --allow-non-smoke-overwrite \
    --summary-output "${container_work}/summary.json" --lineage-output "${container_work}/lineage.json" \
    "${producer_arguments[@]}" ${incremental[@]+"${incremental[@]}"} "${VALIDATE[@]}"; then
  grep -a -E 'Error|Exception' "${work}/${producer}.py.log" | grep -v '^\s*at ' | tail -5 >&2 || true
  log "FAILED: ${producer} refused or crashed; nothing was committed (log ${work}/${producer}.py.log)"
  exit 1
fi
read -r rows built changed_rows < <(python3 -I - "${work}/summary.json" <<'PY'
import json, sys
summary = json.load(open(sys.argv[1]))
rebuild = summary.get("rebuild") or {}
# An incremental run whose recomputed PNUs all came out unchanged commits only the new pins.
print(summary["row_count"], rebuild.get("mode", "full"), "no" if rebuild.get("merged") is False else "yes")
PY
)
if ((${#VALIDATE[@]})); then
  log "dry run passed: ${rows} rows (floor ${minimum:-none}) in $((SECONDS - started))s by a ${built} rebuild; nothing was committed"
elif [[ "${changed_rows}" == no ]]; then
  log "recorded the new pins on ${gold_table}: no PNU's row changed; ${rows} rows in $((SECONDS - started))s by a ${built} rebuild"
else
  [[ -z "${GOLD_PANEL_REBUILD_COMMITTED:-}" ]] || echo "${gold_table}" >>"${GOLD_PANEL_REBUILD_COMMITTED}"
  log "committed ${gold_table}: ${rows} rows (floor ${minimum:-none}) in $((SECONDS - started))s by a ${built} rebuild; the by-PNU bake serves it on its next turn"
fi
