#!/usr/bin/env bash
# The national parcel tile bake from the lakehouse (root ADR-0133 §7, ADR-0194 5, ADR-0253), in two
# halves that each fit the spark pool's starvation bound:
#
#   parcel-tile-bake.sh gold [--force <reason>]   job parcel_served_gold
#   parcel-tile-bake.sh bake                      job parcel_tile_bake (started when gold changed)
#   parcel-tile-bake.sh rehearsal-gold-<YYYYMM>   operator: that edition, nothing served
#   parcel-tile-bake.sh rehearsal-bake            operator: bake it, no catalog or R2 write
#   parcel-tile-bake.sh cleanup                   the unit's ExecStopPost
#
# Every parcel is baked again, nationally; nothing reads PostGIS.
#
# gold:
# 1. The edition is the source contract's `served_edition`
#    (infra/lakehouse/contracts/vworld-parcel-source-objects.json, root ADR-0148 §1). When it is the
#    one last baked, or the one whose Gold already waits for the bake: "nothing to do" (ADR-0133 §7).
#    --force runs it again and logs the reason.
# 2. The matching gate (parcel_matching_gate.py, root ADR-0113 §7) over every 시도 of the edition.
#    A refusal stops the run before anything is written: a build binds only to a passing verdict
#    (ADR-0133 §5). Its input silver.parcel_registry is the parcel_registry job's (ADR-0243); until
#    that table exists the gate exits 3 and this half (a rehearsal's too) waits: `unchanged`,
#    nothing written (root ADR-0265).
# 3. The served Gold (parcel_boundary_served_gold.py, ADR-0133 §2): the edition with every ledgered
#    `parcels` edit applied, committed to gold.parcel_boundary_served, and the v2 handoff parts.
#    The run directory is recorded as ready; the last line is `changed`, which starts the bake.
# bake:
# 4. bake-lakehouse-tiles over the ready run. Each 시도 is baked into the partition cache first; a
#    run that reaches the contract's partition_run_budget_seconds ends `waiting` with the Gold still
#    ready, and the next start continues (root ADR-0493). Then GDAL and tippecanoe in their capped containers, the
#    archive gates, the first-release equivalence gate against the active release (ADR-0133 §4,
#    ADR-0135, ADR-0251), the create-only upload and the promotion. A failure before the promotion
#    changes nothing served. The edition is then recorded as baked.
# rehearsal-gold-<YYYYMM> / rehearsal-bake: the same two halves for an edition the contract holds,
#    run by a person before the contract serves it (runbook parcel-tile-bake.md §2); the edition is
#    named, because the contract can hold one Silver has not loaded yet. The gate's refusal is
#    recorded and does not stop it; without silver.parcel_registry it waits like the job (ADR-0265); the Gold is written as for any edition (it serves
#    nothing until a bake promotes it); the bake runs every gate and stops before the catalog and R2
#    (FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_REHEARSAL). Neither touches the served state.
#
# Every byte of work lives under the tile-bake contract's work_disk.production_root, on the data
# disk (config/tile-bake-containers.contract.json); the root disk only holds the journal and the
# state files. Run directories stay as evidence.
#
# The last line is `foundation-job-outcome changed` when gold made a run ready or bake promoted,
# `unchanged` otherwise.
#
# Spark runs in the Compose project foundation-parcel-tile-bake-<INVOCATION_ID>, and the oven's
# GDAL and tippecanoe containers carry that project's one-off labels (CONTAINER_PROJECT): a timeout
# kills the publisher, not the tool it started. The unit's ExecStopPost (`cleanup`) removes every
# one-off container of the project (root ADR-0128).
set -Eeuo pipefail

MODE="${1:-}"
REHEARSED=""
case "${MODE}" in
  gold|bake|rehearsal-bake|cleanup) shift ;;
  rehearsal-gold-[0-9][0-9][0-9][0-9][0-9][0-9]) REHEARSED="${MODE#rehearsal-gold-}"; MODE=rehearsal-gold; shift ;;
  *) echo "parcel-tile-bake: expected gold, bake, rehearsal-gold-<YYYYMM>, rehearsal-bake or cleanup, got '${MODE}'" >&2; exit 64 ;;
esac

if [[ "${MODE}" == cleanup ]]; then
  [[ "$#" == 0 ]] || { echo "parcel-tile-bake: cleanup takes no arguments" >&2; exit 64; }
  [[ "${INVOCATION_ID:-}" =~ ^[0-9a-f]{32}$ ]] || { echo "parcel-tile-bake: cleanup needs the systemd INVOCATION_ID" >&2; exit 64; }
  source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --installed
  exec "${PUBLISHER_BIN}" stop-parcel-tile-bake
fi
FORCE=""
while (($#)); do
  case "$1" in
    --force)
      [[ "${MODE}" == gold ]] || { echo "parcel-tile-bake: only gold takes --force" >&2; exit 64; }
      [[ -n "${2:-}" && "${2}" != --* ]] || { echo "parcel-tile-bake: --force needs a reason" >&2; exit 64; }
      FORCE="$2"; shift 2 ;;
    *) echo "parcel-tile-bake: unexpected argument '$1'" >&2; exit 64 ;;
  esac
done

source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

STATE_ROOT="${FOUNDATION_PARCEL_TILE_BAKE_STATE_ROOT:-/var/lib/foundation-parcel-tile-bake}"
journal="${STATE_ROOT}/journal.log"
baked_file="${STATE_ROOT}/baked-edition"
# `<snapshot id> <run directory>` of the Gold the next bake takes; the rehearsal keeps its own.
if [[ "${MODE}" == rehearsal-* ]]; then
  ready_file="${STATE_ROOT}/rehearsal-ready"
else
  ready_file="${STATE_ROOT}/ready"
fi
mkdir -p "${STATE_ROOT}"

# A run outside systemd names its own project; ExecStopPost is not there to clean it, so the
# operator is told how.
if [[ -z "${INVOCATION_ID:-}" ]]; then
  INVOCATION_ID="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
  echo "parcel-tile-bake: no systemd invocation; this run's Compose project is foundation-parcel-tile-bake-${INVOCATION_ID} (if interrupted: INVOCATION_ID=${INVOCATION_ID} $0 cleanup)" >&2
fi
[[ "${INVOCATION_ID}" =~ ^[0-9a-f]{32}$ ]] || { echo "parcel-tile-bake: INVOCATION_ID is not a systemd invocation id" >&2; exit 64; }
export INVOCATION_ID

# One half at a time, whoever started it. Taken without waiting.
lock="${STATE_ROOT}/bake.lock"
[[ -e "${lock}" ]] || : >>"${lock}"
exec 8<"${lock}"
if ! flock -n 8; then
  echo "parcel-tile-bake: refused: another bake holds ${lock}" >&2
  source "$(dirname "${BASH_SOURCE[0]}")/held-lock.sh"
  lock_held_by_another_run "the parcel tile bake"
fi

# The edition, its snapshot id, its 시도 and the data-disk work root, from the contracts this
# release ships: none of them is written down here. A rehearsal's Gold half bakes the edition it
# names, which the contract must hold. `served` also answers whether the served edition is the
# ledger's first, served by the release from before the lakehouse bake (root ADR-0490 §4), and the
# candidate the readiness judgement names: the newest edition newer than the served one whose source
# and Silver passed and whose rehearsal has not (READINESS, written by parcel-edition-readiness.sh).
READINESS="${FOUNDATION_PARCEL_EDITION_READINESS_PATH:-/var/lib/foundation-platform/parcel-editions/readiness.json}"
edition_plan() {
  python3 -I - "${RELEASE_ROOT}" "$1" "${READINESS}" <<'PY'
import json, pathlib, sys
sys.dont_write_bytecode = True
release = pathlib.Path(sys.argv[1])
sys.path.insert(0, str(release / "infra/lakehouse/spark/jobs"))
import vworld_parcel_editions as editions
contract = editions.load()
chosen = editions.served(contract) if sys.argv[2] == "served" else sys.argv[2]
if chosen not in contract["editions"]:
    sys.exit(f"parcel-tile-bake: refused: the source contract holds no edition {chosen}")
bake = json.loads((release / "config/tile-bake-containers.contract.json").read_text(encoding="utf-8"))
ledger = contract["promotions"]
bootstrap = len(ledger) == 1 and ledger[0]["readiness"] is None and ledger[0]["edition"] == chosen
candidate = ""
report = pathlib.Path(sys.argv[3])
if report.is_file():
    rows = json.loads(report.read_text(encoding="utf-8")).get("editions") or []
    waiting = [row["edition"] for row in rows if row["edition"] > editions.served(contract)
               and row["edition"] in contract["editions"] and row.get("first_failing_stage") == "rehearsal"]
    candidate = max(waiting, default="")
print(chosen)
print(editions.snapshot_id(contract, chosen))
print(",".join(sorted({obj["region_code"][:2] for obj in editions.edition(contract, chosen)["objects"]
                       if obj["granularity"] == "sido"})))
print(bake["work_disk"]["production_root"])
print("bootstrap" if bootstrap else "promoted")
print(candidate or "-")
PY
}
mapfile -t plan < <(edition_plan "${REHEARSED:-served}")
edition="${plan[0]}"; snapshot="${plan[1]}"; sido="${plan[2]}"; work_root="${plan[3]}"
served_kind="${plan[4]:-}"; candidate="${plan[5]:-}"
[[ -n "${work_root:-}" && -n "${sido}" ]] || { echo "parcel-tile-bake: refused: the contracts name no edition, 시도 or work root" >&2; exit 65; }

if [[ -z "${DATABASE_URL:-}" ]]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi
lakehouse_root="${work_root}/lakehouse"
container_root="/workspace/target/lakehouse"

run_log=/dev/null
on_error() {
  job_journal "${journal}" "${MODE} ${snapshot} FAILED at line $1 (log ${run_log})" >&2
  job_run_log_tail "${run_log}"
  # A rehearsal bake that failed is not baked again from the same Gold (root ADR-0490 §4); one that only
  # deferred partitions to its next run (ADR-0493) did not fail and continues.
  if [[ "${MODE}" == rehearsal-bake && -s "${STATE_ROOT}/rehearsal-ready" ]]; then
    cp -- "${STATE_ROOT}/rehearsal-ready" "${STATE_ROOT}/rehearsal-bake-failed" || true
  fi
}
trap 'on_error ${LINENO}' ERR

spark() {
  FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${lakehouse_root}" \
  FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
  lakehouse_spark_run "foundation-parcel-tile-bake-${INVOCATION_ID}" -v "${scratch}:/scratch" \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
    spark spark-submit --master 'local[8,8]' --driver-memory 16g \
    --conf spark.local.dir=/scratch --jars "${SPARK_RELEASE_JARS}" \
    "/workspace/infra/lakehouse/spark/jobs/$1" "${@:2}" >>"${run_log}" 2>&1
}

if [[ "${MODE}" == gold || "${MODE}" == rehearsal-gold ]]; then
  if [[ "${MODE}" == gold && -z "${FORCE}" ]]; then
    served_done=""
    if [[ "${served_kind}" == bootstrap ]]; then
      served_done="${snapshot} is the ledger's first edition, served by the release from before the lakehouse bake"
    elif [[ -s "${baked_file}" && "$(cat "${baked_file}")" == "${snapshot}" ]]; then
      served_done="the parcel tiles already serve ${snapshot}"
    elif [[ -s "${ready_file}" && "$(cut -d' ' -f1 "${ready_file}")" == "${snapshot}" ]]; then
      served_done="the Gold of ${snapshot} waits for the bake"
    fi
    if [[ -n "${served_done}" ]]; then
      # Root ADR-0490 §4: with nothing to do for the served edition, the half rehearses the candidate.
      # A rehearsal that did not pass is tried again after REHEARSAL_RETRY_DAYS, not on every release (the
      # host deploys several a day, and a rehearsal is about 100 minutes of the spark pool). 2026-10-15..
      # 11-11 the one rehearsal a person ran failed and nobody ran another.
      attempted_file="${STATE_ROOT}/rehearsal-attempted"
      retry_seconds=$(( ${REHEARSAL_RETRY_DAYS:-7} * 86400 ))
      if [[ "${candidate}" == "-" || -z "${candidate}" ]]; then
        job_journal "${journal}" "nothing to do: ${served_done}; no edition waits for its rehearsal"
        echo "foundation-job-outcome unchanged"; exit 0
      fi
      mapfile -t plan < <(edition_plan "${candidate}")
      edition="${plan[0]}"; snapshot="${plan[1]}"; sido="${plan[2]}"
      if [[ -s "${attempted_file}" ]]; then
        read -r tried tried_at <"${attempted_file}"
        if [[ "${tried}" == "${snapshot}" ]] && (( $(date -u +%s) - tried_at < retry_seconds )); then
          job_journal "${journal}" "nothing to do: ${served_done}; ${snapshot} was rehearsed $(date -u -d "@${tried_at}" +%FT%TZ), again after ${REHEARSAL_RETRY_DAYS:-7} days"
          echo "foundation-job-outcome unchanged"; exit 0
        fi
      fi
      printf '%s %s\n' "${snapshot}" "$(date -u +%s)" >"${attempted_file}.tmp"
      mv "${attempted_file}.tmp" "${attempted_file}"
      job_journal "${journal}" "rehearsing ${snapshot}: ${served_done}"
      MODE=rehearsal-gold
      ready_file="${STATE_ROOT}/rehearsal-ready"
      REHEARSED_BY_JOB=1
    fi
  fi
  [[ -z "${FORCE}" ]] || job_journal "${journal}" "forced: ${FORCE}"
  run_id="$(date -u +%Y%m%dT%H%M%SZ)"
  work="${lakehouse_root}/parcel-tile-bake/${run_id}"
  container_work="${container_root}/parcel-tile-bake/${run_id}"
  scratch="${work_root}/spark-scratch"
  mkdir -p "${work}" "${scratch}"
  # The Spark container writes as uid 185.
  chmod 0777 "${lakehouse_root}" "${work}" "${scratch}"
  run_log="${work}/run.log"
  started=${SECONDS}

  # A rehearsal reads an edition the contract does not serve yet; both Spark jobs refuse that unless told.
  allow=()
  [[ "${MODE}" == rehearsal-gold ]] && allow=(--allow-non-served-edition)

  # 2. The matching gate: a refusal is exit 1 with a verdict that did not pass. Any other failure —
  # a crash exits 1 too, with no verdict — stops the run, a rehearsal's included.
  gate_status=0
  verdict="${work}/matching-verdict.json"
  spark parcel_matching_gate.py --snapshot-id "${snapshot}" --sido "${sido}" "${allow[@]}" \
    --summary-output "${container_work}/matching-verdict.json" || gate_status=$?
  if ((gate_status == 3)); then
    # ADR-0265: the gate's input does not exist yet; nothing is written and the next run retries.
    job_journal "${journal}" "waiting: the matching gate's input silver.parcel_registry does not exist yet (job parcel_registry, root ADR-0243)"
    echo "foundation-job-outcome unchanged"; exit 0
  fi
  if ((gate_status != 0)); then
    refused=no
    [[ "${gate_status}" == 1 && -s "${verdict}" ]] \
      && python3 -I -c 'import json, sys; sys.exit(json.load(open(sys.argv[1]))["passed"] is not False)' "${verdict}" \
      && refused=yes
    [[ "${MODE}" == rehearsal-gold && "${refused}" == yes ]] || { on_error "${LINENO}"; exit "${gate_status}"; }
    job_journal "${journal}" "rehearsal: the matching gate refused ${snapshot} in $((SECONDS - started))s (verdict ${verdict}); continuing"
  else
    job_journal "${journal}" "matching gate passed for ${snapshot} in $((SECONDS - started))s"
  fi

  # 3. The served Gold and its handoff parts.
  step=${SECONDS}
  spark parcel_boundary_served_gold.py --source-snapshot-id "${snapshot}" "${allow[@]}" \
    --output-dir "${container_work}/served" --summary-output "${container_work}/served-summary.json" \
    --allow-non-smoke-write
  job_journal "${journal}" "${MODE}: served Gold of ${snapshot} in $((SECONDS - step))s, handoff $(du -sb "${work}/served" | cut -f1) bytes, total $((SECONDS - started))s"
  printf '%s %s\n' "${snapshot}" "${work}" >"${ready_file}.tmp"
  mv "${ready_file}.tmp" "${ready_file}"
  # A rehearsal the job started hands its Gold to the bake half like a served one (root ADR-0490 §4).
  if [[ "${MODE}" == gold || -n "${REHEARSED_BY_JOB:-}" ]]; then echo "foundation-job-outcome changed"; else echo "foundation-job-outcome unchanged"; fi
  exit 0
fi

# bake / rehearsal-bake
# Root ADR-0490 §4: the bake half bakes the served Gold first; with none waiting, the candidate's rehearsal.
# A rehearsal bake that failed waits for the next rehearsal Gold, not the next day.
if [[ "${MODE}" == bake && ! -s "${ready_file}" && -s "${STATE_ROOT}/rehearsal-ready" ]]; then
  if [[ -s "${STATE_ROOT}/rehearsal-bake-failed" ]] \
    && cmp -s "${STATE_ROOT}/rehearsal-ready" "${STATE_ROOT}/rehearsal-bake-failed"; then
    job_journal "${journal}" "nothing to do: the bake of the rehearsal Gold $(cut -d' ' -f1 "${STATE_ROOT}/rehearsal-ready") failed; the next rehearsal makes another"
    echo "foundation-job-outcome unchanged"; exit 0
  fi
  MODE=rehearsal-bake
  ready_file="${STATE_ROOT}/rehearsal-ready"
fi
if [[ ! -s "${ready_file}" ]]; then
  job_journal "${journal}" "nothing to do: no served Gold waits for the bake"
  echo "foundation-job-outcome unchanged"; exit 0
fi
read -r ready_snapshot work <"${ready_file}"
# A rehearsal bakes the Gold its own Gold half made ready, whatever edition it named.
[[ "${MODE}" == rehearsal-bake ]] && snapshot="${ready_snapshot}"
if [[ "${ready_snapshot}" != "${snapshot}" ]]; then
  job_journal "${journal}" "nothing to do: the waiting Gold is ${ready_snapshot}, the contract's edition ${snapshot}"
  echo "foundation-job-outcome unchanged"; exit 0
fi
# Root ADR-0490 §6: the tiles of a promoted edition are released only once every Gold of the lockstep
# barrier is built on it (scripts/ops/edition_lockstep_gate.py tiles), so the map and the by-PNU panels
# move together. The Gold stays ready; the next start asks again. On a day without a promotion it is `ok`.
if [[ "${MODE}" == bake ]]; then
  lockstep="$(python3 -I -B "$(dirname "${BASH_SOURCE[0]}")/edition_lockstep_gate.py" tiles)" \
    || { job_journal "${journal}" "refused: cannot tell whether every served-universe Gold is on ${snapshot}"; exit 65; }
  if [[ "${lockstep}" == waiting* ]]; then
    job_journal "${journal}" "${lockstep}"
    echo "foundation-job-outcome ${lockstep}"; exit 0
  fi
fi
run_log="${work}/bake.log"
run_id="$(basename "${work}")"
step=${SECONDS}
rehearsal=0
[[ "${MODE}" == rehearsal-bake ]] && rehearsal=1
# The edit store is not configured: parcels have no edits yet (ADR-0133 §6) and the oven refuses
# a snapshot that folds edits without one.
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_CONFIRM=1 \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_REHEARSAL="${rehearsal}" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_UNIT=parcels \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_SERVED_HANDOFF="${work}/served" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_SERVED_SUMMARY="${work}/served-summary.json" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_MATCHING_VERDICT="${work}/matching-verdict.json" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_WORK_ROOT="${work_root}/bake" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_CONTAINER_PROJECT="foundation-parcel-tile-bake-${INVOCATION_ID}" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_BUILD_IDEMPOTENCY_KEY="parcel-tile-bake-${run_id}-build" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_PROMOTE_IDEMPOTENCY_KEY="parcel-tile-bake-${run_id}-promote" \
  "${PUBLISHER_BIN}" bake-lakehouse-tiles >>"${run_log}" 2>&1
# Root ADR-0493: the run reached its partition budget before any Catalog or R2 write. The baked
# partitions are in the cache; the Gold stays ready and the next start continues.
deferred="$(tail -n 5 "${run_log}" | grep -oE 'lakehouse-tile-bake-deferred .*' | tail -1 || true)"
if [[ -n "${deferred}" ]]; then
  job_journal "${journal}" "deferred ${snapshot} after $((SECONDS - step))s: ${deferred}"
  echo "foundation-job-outcome waiting ${deferred#lakehouse-tile-bake-deferred }"; exit 0
fi
result="$(grep -oE 'lakehouse-tile-bake-(ok|rehearsal).*' "${run_log}" | tail -1)"
rm -f "${ready_file}"
if ((rehearsal)); then
  job_journal "${journal}" "rehearsal baked ${snapshot} in $((SECONDS - step))s: ${result}"
  echo "foundation-job-outcome unchanged"; exit 0
fi
printf '%s\n' "${snapshot}" >"${baked_file}.tmp"
mv "${baked_file}.tmp" "${baked_file}"
job_journal "${journal}" "baked ${snapshot} in $((SECONDS - step))s: ${result}"
echo "foundation-job-outcome changed"
