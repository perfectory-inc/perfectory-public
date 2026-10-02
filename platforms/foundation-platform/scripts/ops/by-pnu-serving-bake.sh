#!/usr/bin/env bash
# The scheduled by-PNU serving bake (root ADR-0122, ADR-0096 parcels, ADR-0100 buildings).
#
#   by-pnu-serving-bake.sh parcel|building
#
# Replaces the hand-kept server script that baked both lanes before 2026-10. One run:
#
# 1. asks the publisher for the lane's state: the Gold table's current snapshot and the generation
#    the manifest serves. Same snapshot (or no snapshot yet): "nothing to do", exit 0.
# 2. picks the target generation: the published one + 1, or the generation an earlier run of the
#    same snapshot left half-written (a rerun resumes it; objects are create-only and the export
#    skips keys the generation listing already holds).
# 3. bakes PNU-prefix shards. The export refuses a shard that keeps more than its row cap in
#    memory; that shard is split into ten longer prefixes and the split is remembered for the next
#    run. A crash (an R2 429 storm, a dropped connection) is retried; the retry resumes.
# 4. publishes the manifest only when every shard baked the same Gold snapshot into the same
#    generation and the shards' exported_row_count adds up to the Gold row count. On 2026-09-10 a
#    national bake finished 4.95M objects short and nothing said so; this is that check.
#
# Never overwrites an object and never repoints a published generation: those publisher flags are
# cleared here, whatever the environment holds. Work files live under the state root on /data.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current

UNIT="${1:-}"
case "${UNIT}" in
  parcel) LANE=PARCEL ;;
  building) LANE=BUILDING ;;
  *) echo "by-pnu-serving-bake: expected parcel or building, got '${UNIT}'" >&2; exit 64 ;;
esac
ENV_PREFIX="FOUNDATION_PLATFORM_${LANE}_BY_PNU_SERVING"
STATE_ROOT="${FOUNDATION_BY_PNU_BAKE_STATE_ROOT:-/data/foundation-platform/by-pnu-bake}/${UNIT}"
MAX_CONCURRENCY="${FOUNDATION_BY_PNU_BAKE_MAX_CONCURRENCY:-128}"
ATTEMPTS="${FOUNDATION_BY_PNU_BAKE_ATTEMPTS:-6}"
RETRY_SECONDS="${FOUNDATION_BY_PNU_BAKE_RETRY_SECONDS:-45}"
log() { printf 'by-pnu-serving-bake %s: %s\n' "${UNIT}" "$*"; }

# Create-only and forward-only are not the caller's to switch off.
unset "${ENV_PREFIX}_ALLOW_OVERWRITE" "${ENV_PREFIX}_ALLOW_REPOINT" "${ENV_PREFIX}_FIRST_PUBLICATION" \
  "${ENV_PREFIX}_PNU_ALLOWLIST_PATH" "${ENV_PREFIX}_EXPORT_SUMMARY_PATH" "${ENV_PREFIX}_OUTPUT_ROOT"
export "${ENV_PREFIX}_OUTPUT_STORAGE_DRIVER=${FOUNDATION_BY_PNU_BAKE_STORAGE_DRIVER:-r2}"
export "${ENV_PREFIX}_RESUME_FROM_LISTING=true"

mkdir -p "${STATE_ROOT}"

# 1. What is there to do?
state="${STATE_ROOT}/state.json"
rm -f "${state}"
FOUNDATION_PLATFORM_BY_PNU_SERVING_STATE_PATH="${state}" \
  "${PUBLISHER_BIN}" "show-${UNIT}-by-pnu-serving-state"
read -r gold published_generation published_snapshot < <(python3 -I - "${state}" <<'PY'
import json, sys
state = json.load(open(sys.argv[1]))
published = state["published"]
print(state["gold_iceberg_snapshot_id"] or "-", published["current_generation"], published["gold_iceberg_snapshot_id"])
PY
)
[[ -n "${published_snapshot:-}" ]] || { log "refused: cannot read the lane state ${state}"; exit 65; }
if [[ "${gold}" == - ]]; then
  log "nothing to do: the Gold table has no snapshot"
  exit 0
fi
if [[ "${gold}" == "${published_snapshot}" ]]; then
  log "nothing to do: generation ${published_generation} already serves Gold snapshot ${gold}"
  exit 0
fi

# 2. Which generation?
progress="${STATE_ROOT}/in-progress.json"
target="$(python3 -I - "${progress}" "${gold}" "${published_generation}" <<'PY'
import json, pathlib, sys
progress, gold, published = pathlib.Path(sys.argv[1]), sys.argv[2], int(sys.argv[3])
earlier = json.loads(progress.read_text()) if progress.exists() else None
if earlier and earlier["gold_iceberg_snapshot_id"] == gold and earlier["target_generation"] > published:
    print(earlier["target_generation"])  # resume the half-written generation of this snapshot
else:
    # A generation half-written for another snapshot is never reused: its objects carry the other
    # snapshot and create-only would refuse to replace them. It is left in place, unserved.
    print(max(published, earlier["target_generation"] if earlier else 0) + 1)
PY
)"
printf '{"gold_iceberg_snapshot_id": "%s", "target_generation": %s}\n' "${gold}" "${target}" >"${progress}"
run="${STATE_ROOT}/runs/${gold}-g${target}"
mkdir -p "${run}"
log "baking Gold snapshot ${gold} into generation ${target} (published: generation ${published_generation} of ${published_snapshot})"

# 3. Bake every shard.
plan="${STATE_ROOT}/shard-plan.txt"
if [[ -s "${plan}" ]]; then mapfile -t queue <"${plan}"; else queue=(1 2 3 4 5 6 7 8 9); fi
done_shards=()
while ((${#queue[@]})); do
  prefix="${queue[0]}"; queue=("${queue[@]:1}")
  [[ "${prefix}" =~ ^[0-9]{1,10}$ ]] || { log "refused: shard plan holds '${prefix}'"; exit 65; }
  summary="${run}/shard-${prefix}.json"
  if [[ -s "${summary}" ]]; then done_shards+=("${prefix}"); continue; fi
  baked=""
  for attempt in $(seq 1 "${ATTEMPTS}"); do
    attempt_log="${run}/shard-${prefix}.attempt-${attempt}.log"
    if env "${ENV_PREFIX}_CONFIRM_EXPORT=true" "${ENV_PREFIX}_TARGET_GENERATION=${target}" \
        "${ENV_PREFIX}_PNU_PREFIX=${prefix}" "${ENV_PREFIX}_MAX_CONCURRENCY=${MAX_CONCURRENCY}" \
        "${ENV_PREFIX}_SUMMARY_PATH=${summary}.partial" \
        "${PUBLISHER_BIN}" "export-${UNIT}-by-pnu-serving" >"${attempt_log}" 2>&1; then
      mv "${summary}.partial" "${summary}"
      baked=yes
      break
    fi
    rm -f "${summary}.partial"
    if grep -q 'shard the run with' "${attempt_log}"; then
      ((${#prefix} < 10)) || { log "refused: shard ${prefix} is at full PNU length and still too large"; exit 65; }
      log "shard ${prefix} holds more rows than one run may keep; splitting it into ${prefix}0..${prefix}9"
      for digit in 0 1 2 3 4 5 6 7 8 9; do queue+=("${prefix}${digit}"); done
      baked=split
      break
    fi
    tail -n 5 "${attempt_log}" >&2
    log "shard ${prefix} attempt ${attempt}/${ATTEMPTS} failed; resuming in ${RETRY_SECONDS}s"
    sleep "${RETRY_SECONDS}"
  done
  case "${baked}" in
    yes) done_shards+=("${prefix}"); log "shard ${prefix} baked" ;;
    split) ;;
    *) log "FAILED: shard ${prefix} did not bake in ${ATTEMPTS} attempts; nothing was published"; exit 1 ;;
  esac
done
printf '%s\n' "${done_shards[@]}" >"${plan}.next" && mv "${plan}.next" "${plan}"

# 4. Complete? Then publish.
expected="$(python3 -I - "${run}" "${gold}" "${target}" "${done_shards[@]}" <<'PY'
import json, pathlib, sys
run, gold, target, shards = pathlib.Path(sys.argv[1]), sys.argv[2], int(sys.argv[3]), sys.argv[4:]
def refuse(reason):
    sys.exit("by-pnu-serving-bake: refused to publish: " + reason)
gold_rows, exported = set(), 0
nested = sorted(f"{a} inside {b}" for a in shards for b in shards if a != b and a.startswith(b))
if nested or len(set(shards)) != len(shards):
    refuse(f"shards overlap ({', '.join(nested) or 'repeated prefix'}); their rows would count twice")
for prefix in shards:
    summary = json.loads((run / f"shard-{prefix}.json").read_text())
    if summary["gold_iceberg_snapshot_id"] != gold:
        refuse(f"shard {prefix} baked Gold snapshot {summary['gold_iceberg_snapshot_id']}, not {gold}; "
               "the table moved during the bake, the next run starts a new generation")
    if summary["target_generation"] != target or summary.get("pnu_prefix") != prefix:
        refuse(f"shard {prefix}'s summary is for generation {summary['target_generation']} "
               f"prefix {summary.get('pnu_prefix')}")
    if summary["overwritten_object_count"]:
        refuse(f"shard {prefix} overwrote objects")
    gold_rows.add(summary["scanned_row_count"])
    exported += summary["exported_row_count"]
if len(gold_rows) != 1:
    refuse(f"shards scanned different Gold row counts {sorted(gold_rows)}")
(total,) = gold_rows
if exported != total:
    refuse(f"incomplete bake: shards exported {exported} of the Gold snapshot's {total} rows "
           f"({total - exported} missing); the manifest stays on the published generation")
print(total)
PY
)"
log "complete: ${#done_shards[@]} shards exported all ${expected} rows; publishing generation ${target}"
env "${ENV_PREFIX}_CONFIRM_PUBLISH=true" "${ENV_PREFIX}_PUBLISH_FROM_LISTING=true" \
  "${ENV_PREFIX}_TARGET_GENERATION=${target}" \
  "${ENV_PREFIX}_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID=${gold}" \
  "${ENV_PREFIX}_EXPECTED_OBJECT_COUNT=${expected}" \
  "${PUBLISHER_BIN}" "publish-${UNIT}-by-pnu-serving-manifest"
rm -f "${progress}"
# The shard summaries list every object (hundreds of MB for a national run). Keep their counts.
python3 -I - "${run}" <<'PY'
import json, pathlib, sys
for path in pathlib.Path(sys.argv[1]).glob("shard-*.json"):
    summary = json.loads(path.read_text())
    summary.pop("artifacts", None)
    path.write_text(json.dumps(summary, sort_keys=True) + "\n")
PY
log "published generation ${target}: ${expected} objects from Gold snapshot ${gold}"
