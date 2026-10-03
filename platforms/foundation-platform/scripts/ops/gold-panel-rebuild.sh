#!/usr/bin/env bash
# The scheduled panel Gold rebuild (root ADR-0139, ADR-0122).
#
#   gold-panel-rebuild.sh parcel|building|all [--dry-run]
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
#
# --dry-run runs the producer with --validate-only: the whole transform and every check, no
# commit. The by-PNU bake (by-pnu-serving-bake.sh) bakes a Gold snapshot its published generation
# does not serve, so a committed rebuild is baked on the bake's next turn.
set -euo pipefail

UNIT="${1:-}"
MODE="${2:-}"
case "${MODE}" in
  "") VALIDATE=() ;;
  --dry-run) VALIDATE=(--validate-only) ;;
  *) echo "gold-panel-rebuild: expected no option or --dry-run, got '${MODE}'" >&2; exit 64 ;;
esac
case "${UNIT}" in
  parcel | building) ;;
  all)
    status=0
    "${BASH_SOURCE[0]}" parcel ${MODE:+"${MODE}"} || status=$?
    "${BASH_SOURCE[0]}" building ${MODE:+"${MODE}"} || { table_status=$?; ((status)) || status=${table_status}; }
    exit "${status}"
    ;;
  *) echo "gold-panel-rebuild: expected parcel, building or all, got '${UNIT}'" >&2; exit 64 ;;
esac
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
STATE_ROOT="${FOUNDATION_GOLD_REBUILD_STATE_ROOT:-/var/lib/foundation-gold-panel-rebuild}/${UNIT}"
SCRATCH="${FOUNDATION_GOLD_REBUILD_SCRATCH:-/data/foundation-platform/lakehouse/spark-scratch}"
CONTRACT="${RELEASE_ROOT}/infra/lakehouse/contracts/gold-panel-rebuild.contract.json"
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
    -p foundation-platform-compute --profile lakehouse-batch run --rm -v "${SCRATCH}:/scratch" \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
    spark spark-submit --master "${master}" --driver-memory "${driver_memory}" \
    --conf spark.local.dir=/scratch --jars "${SPARK_RELEASE_JARS}" \
    "/workspace/infra/lakehouse/spark/jobs/$1" "${@:2}" >>"${work}/$1.log" 2>&1
}

# 1. Plan.
if ! spark gold_rebuild.py --gold-table "${gold_table}" \
    --plan-output "${container_work}/plan.json" --pins-output "${container_work}/pins.json"; then
  grep -a -E 'Error|Exception' "${work}/gold_rebuild.py.log" | grep -v '^\s*at ' | tail -5 >&2 || true
  log "FAILED: could not plan ${gold_table} (log ${work}/gold_rebuild.py.log)"
  exit 1
fi
mapfile -t plan < <(python3 -I - "${work}/plan.json" <<'PY'
import json, sys
plan = json.load(open(sys.argv[1]))
print(plan["action"])
print(plan["source_snapshots"][plan["anchor_input"]])
print("" if plan["minimum_row_count"] is None else plan["minimum_row_count"])
print("; ".join(plan["reasons"]))
print(" ".join(plan["producer_arguments"]))
PY
)
action="${plan[0]}"; anchor="${plan[1]}"; minimum="${plan[2]}"; reasons="${plan[3]}"
read -r -a producer_arguments <<<"${plan[4]}"
if [[ "${action}" == nothing_to_do ]]; then
  log "nothing to do: no Silver input of ${gold_table} changed rows since the snapshots its current Gold was built from"
  exit 0
fi
[[ "${action}" == rebuild ]] || { log "refused: the plan says '${action}'"; exit 65; }
log "rebuilding ${gold_table}${VALIDATE[0]:+ (dry run)}: ${reasons}"

# 2. Build, check, commit.
started=${SECONDS}
if ! spark "${producer}.py" --input-mode iceberg --write-mode iceberg \
    --iceberg-snapshot-id "${anchor}" --source-snapshots-path "${container_work}/pins.json" \
    ${minimum:+--minimum-count "${minimum}"} --allow-non-smoke-overwrite \
    --summary-output "${container_work}/summary.json" --lineage-output "${container_work}/lineage.json" \
    "${producer_arguments[@]}" "${VALIDATE[@]}"; then
  grep -a -E 'Error|Exception' "${work}/${producer}.py.log" | grep -v '^\s*at ' | tail -5 >&2 || true
  log "FAILED: ${producer} refused or crashed; nothing was committed (log ${work}/${producer}.py.log)"
  exit 1
fi
rows="$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["row_count"])' "${work}/summary.json")"
if ((${#VALIDATE[@]})); then
  log "dry run passed: ${rows} rows (floor ${minimum:-none}) in $((SECONDS - started))s; nothing was committed"
else
  log "committed ${gold_table}: ${rows} rows (floor ${minimum:-none}) in $((SECONDS - started))s; the by-PNU bake serves it on its next turn"
fi
