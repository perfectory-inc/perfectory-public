#!/usr/bin/env bash
# The national parcel tile bake from the lakehouse (root ADR-0133 §7, ADR-0194 5).
#
#   parcel-tile-bake.sh [--force <reason>]
#   parcel-tile-bake.sh cleanup        (the unit's ExecStopPost)
#
# Every parcel is baked again, nationally, from the edition the map serves; nothing reads PostGIS.
#
# 1. The edition is the source contract's `served_edition`
#    (infra/lakehouse/contracts/vworld-parcel-source-objects.json, root ADR-0148 §1). When the last
#    run of this job that finished baked that edition: "nothing to do" (ADR-0133 §7). --force
#    bakes it again and logs the reason.
# 2. The matching gate (parcel_matching_gate.py, root ADR-0113 §7) over every 시도 of the edition.
#    The bake binds its release to the edition only with this passing verdict (ADR-0133 §5); a
#    refusal stops the run before anything is written.
# 3. The served Gold (parcel_boundary_served_gold.py, ADR-0133 §2): the edition with every ledgered
#    `parcels` edit applied, committed to gold.parcel_boundary_served, and the v2 handoff parts.
# 4. bake-lakehouse-tiles: GDAL and tippecanoe in their capped containers, the archive gates, the
#    first-release equivalence gate against the active release (ADR-0133 §4, ADR-0135), the
#    create-only upload and the promotion. A failure before the promotion changes nothing served.
# 5. The edition is recorded as baked. The run directory stays as evidence (the handoff parts were
#    written by Spark's uid and are kept like every served handoff); a new run happens only for a
#    new edition or a forced bake, so the data disk gains one handoff per edition.
#
# Every byte of work lives under the tile-bake contract's work_disk.production_root, on the data
# disk (config/tile-bake-containers.contract.json); the root disk only holds the journal.
#
# The last line is `foundation-job-outcome changed` after a promotion, `unchanged` otherwise.
#
# Spark runs in the Compose project foundation-parcel-tile-bake-<INVOCATION_ID>; the unit's
# ExecStopPost (`cleanup`) removes that project's one-off containers after a timeout (root
# ADR-0128), as it does for the panel Gold rebuild.
set -euo pipefail

if [[ "${1:-}" == cleanup ]]; then
  [[ "$#" == 1 ]] || { echo "parcel-tile-bake: cleanup takes no arguments" >&2; exit 64; }
  [[ "${INVOCATION_ID:-}" =~ ^[0-9a-f]{32}$ ]] || { echo "parcel-tile-bake: cleanup needs the systemd INVOCATION_ID" >&2; exit 64; }
  source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --installed
  exec "${PUBLISHER_BIN}" stop-parcel-tile-bake
fi
FORCE=""
while (($#)); do
  case "$1" in
    --force)
      [[ -n "${2:-}" && "${2}" != --* ]] || { echo "parcel-tile-bake: --force needs a reason" >&2; exit 64; }
      FORCE="$2"; shift 2 ;;
    *) echo "parcel-tile-bake: expected --force <reason> or cleanup, got '$1'" >&2; exit 64 ;;
  esac
done

source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

STATE_ROOT="${FOUNDATION_PARCEL_TILE_BAKE_STATE_ROOT:-/var/lib/foundation-parcel-tile-bake}"
journal="${STATE_ROOT}/journal.log"
baked_file="${STATE_ROOT}/baked-edition"
mkdir -p "${STATE_ROOT}"

# A run outside systemd (the supervised first run) names its own project; ExecStopPost is not there
# to clean it, so the operator is told how.
if [[ -z "${INVOCATION_ID:-}" ]]; then
  INVOCATION_ID="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
  echo "parcel-tile-bake: no systemd invocation; this run's Compose project is foundation-parcel-tile-bake-${INVOCATION_ID} (if interrupted: INVOCATION_ID=${INVOCATION_ID} $0 cleanup)" >&2
fi
[[ "${INVOCATION_ID}" =~ ^[0-9a-f]{32}$ ]] || { echo "parcel-tile-bake: INVOCATION_ID is not a systemd invocation id" >&2; exit 64; }
export INVOCATION_ID

# One bake at a time, whoever started it. Taken without waiting.
lock="${STATE_ROOT}/bake.lock"
[[ -e "${lock}" ]] || : >>"${lock}"
exec 8<"${lock}"
flock -n 8 || { echo "parcel-tile-bake: refused: another bake holds ${lock}" >&2; exit 75; }

# The edition, its snapshot id, its 시도 and the data-disk work root, from the contracts this
# release ships: none of them is written down here.
mapfile -t plan < <(python3 -I - "${RELEASE_ROOT}" <<'PY'
import json, pathlib, sys
sys.dont_write_bytecode = True
release = pathlib.Path(sys.argv[1])
sys.path.insert(0, str(release / "infra/lakehouse/spark/jobs"))
import vworld_parcel_editions as editions
contract = editions.load()
served = editions.served(contract)
bake = json.loads((release / "config/tile-bake-containers.contract.json").read_text(encoding="utf-8"))
print(served)
print(editions.snapshot_id(contract, served))
print(",".join(editions.cadastral_sido(contract)))
print(bake["work_disk"]["production_root"])
PY
)
edition="${plan[0]}"; snapshot="${plan[1]}"; sido="${plan[2]}"; work_root="${plan[3]}"
[[ -n "${work_root:-}" && -n "${sido}" ]] || { echo "parcel-tile-bake: refused: the contracts name no edition, 시도 or work root" >&2; exit 65; }

if [[ -z "${FORCE}" && -s "${baked_file}" && "$(cat "${baked_file}")" == "${snapshot}" ]]; then
  job_journal "${journal}" "nothing to do: the parcel tiles already serve ${snapshot}"
  echo "foundation-job-outcome unchanged"
  exit 0
fi
[[ -z "${FORCE}" ]] || job_journal "${journal}" "forced: ${FORCE}"

if [[ -z "${DATABASE_URL:-}" ]]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
lakehouse_root="${work_root}/lakehouse"
work="${lakehouse_root}/parcel-tile-bake/${run_id}"
container_work="/workspace/target/lakehouse/parcel-tile-bake/${run_id}"
scratch="${work_root}/spark-scratch"
mkdir -p "${work}" "${scratch}"
# The Spark container writes as uid 185.
chmod 0777 "${lakehouse_root}" "${work}" "${scratch}"
run_log="${work}/run.log"

on_error() {
  job_journal "${journal}" "bake ${snapshot} FAILED at line $1 (work ${work})" >&2
  job_run_log_tail "${run_log}"
}
trap 'on_error ${LINENO}' ERR

spark() {
  docker rm foundation-platform-lakehouse-target-init >/dev/null 2>&1 || true
  FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${lakehouse_root}" \
  FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
  docker compose --project-directory "${RELEASE_ROOT}" -f "${RELEASE_ROOT}/compose.lakehouse.yml" \
    -p "foundation-parcel-tile-bake-${INVOCATION_ID}" --profile lakehouse-batch run --rm -v "${scratch}:/scratch" \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
    spark spark-submit --master 'local[8]' --driver-memory 16g \
    --conf spark.local.dir=/scratch --jars "${SPARK_RELEASE_JARS}" \
    "/workspace/infra/lakehouse/spark/jobs/$1" "${@:2}" >>"${run_log}" 2>&1
}

started=${SECONDS}
# 2. The matching gate: exit 1 is a refusal with its verdict written.
step=${SECONDS}
spark parcel_matching_gate.py --snapshot-id "${snapshot}" --sido "${sido}" \
  --summary-output "${container_work}/matching-verdict.json"
job_journal "${journal}" "matching gate passed for ${snapshot} in $((SECONDS - step))s"

# 3. The served Gold and its handoff parts.
step=${SECONDS}
spark parcel_boundary_served_gold.py --source-snapshot-id "${snapshot}" \
  --output-dir "${container_work}/served" --summary-output "${container_work}/served-summary.json" \
  --allow-non-smoke-write
served_bytes="$(du -sb "${work}/served" | cut -f1)"
job_journal "${journal}" "served Gold of ${snapshot} in $((SECONDS - step))s, handoff ${served_bytes} bytes"

# 4. Bake, gate, upload, promote. The edit store is not configured: parcels have no edits yet
# (ADR-0133 §6) and the oven refuses a snapshot that folds edits without one.
step=${SECONDS}
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_CONFIRM=1 \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_UNIT=parcels \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_SERVED_HANDOFF="${work}/served" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_SERVED_SUMMARY="${work}/served-summary.json" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_MATCHING_VERDICT="${work}/matching-verdict.json" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_WORK_ROOT="${work_root}/bake" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_BUILD_IDEMPOTENCY_KEY="parcel-tile-bake-${run_id}-build" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_PROMOTE_IDEMPOTENCY_KEY="parcel-tile-bake-${run_id}-promote" \
  "${PUBLISHER_BIN}" bake-lakehouse-tiles >>"${run_log}" 2>&1
result="$(grep -o 'lakehouse-tile-bake-ok.*' "${run_log}" | tail -1)"

# 5. Record.
printf '%s\n' "${snapshot}" >"${baked_file}.tmp"
mv "${baked_file}.tmp" "${baked_file}"
job_journal "${journal}" "baked ${snapshot}: bake $((SECONDS - step))s, total $((SECONDS - started))s, ${result}"
echo "foundation-job-outcome changed"
