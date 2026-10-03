#!/usr/bin/env bash
# The scheduled by-PNU serving bake (root ADR-0122, ADR-0141; ADR-0096 parcels, ADR-0100 buildings).
#
#   by-pnu-serving-bake.sh parcel|building|all
#
# `all` (the registered job) bakes the parcel lane, then the building lane, in this one process
# tree: the host memory budget fits one lane's export at a time (root ADR-0138). A failed lane
# does not stop the other; `all` fails if either did.
#
# One run of a lane:
#
# 1. asks the publisher for the lane's state: the Gold table's current snapshot, the snapshot the
#    served state reflects, the patches the base carries, the document schema the base was baked
#    with, the contract's patch bounds, and every generation and patch that holds any object. Gold
#    already reflected (or no snapshot yet): "nothing to do", exit 0.
# 2. chooses how to reflect the new snapshot (root ADR-0141 §5, §8), and records the choice:
#    - full: a new base generation, when the operator forces it (FOUNDATION_BY_PNU_BAKE_FORCE_FULL
#      with a FOUNDATION_BY_PNU_BAKE_FORCE_FULL_REASON), when the export now bakes another document
#      schema than the base holds, when the base already carries max_patches patches (or more,
#      after max_patches was lowered), when its patches are listed by another prefix length than
#      the contract's pnu_prefix_length, or when the change set would take the patches past
#      max_cumulative_change_ratio of the base. A full bake is also the compaction.
#    - otherwise the change set decides. `by_pnu_panel_delta.py` compares row_digest between the
#      reflected snapshot and the new one; it refuses when the comparison snapshot is gone or the
#      change set is more than max_delta_fraction of the table, and then nothing is published. An
#      empty change set only advances the reflected snapshot ("reflect"); any other is a patch.
# 3. bakes PNU-prefix shards into the chosen directory, create-only:
#    - full: a new generation above the published base and every generation holding objects; a
#      patch: a new patch above the newest served patch and every patch directory holding objects.
#      An earlier run's half-written target of the same snapshot (in-progress.json) is resumed; a
#      target this lane did not record is never resumed. A new target's shards demand an empty key
#      range until the export has recorded that check (the shard's .fresh-checked marker).
#    - full bakes start from the remembered shard plan (1..9 at first); a patch keeps only its
#      change set's rows, so it starts as one shard over the whole table. A shard over the export's
#      row cap is split into ten longer prefixes. A crash is retried and resumes. Each shard is told
#      the Gold snapshot the bake is of, so a table that moves mid-bake stops the bake there.
# 4. publishes only a complete bake: a full bake's shards exported exactly the Gold row count; a
#    patch's shards exported exactly the change set's upserts and tombstones. The publisher then
#    checks the patch against its change set again (ADR-0141 §7) before it moves the manifest.
#
# The run summary (runs/<snapshot>-<mode>/run-summary.json) records the choice, its reason and the
# counts. Never overwrites an object and never repoints a published generation. Work files live
# under the state root on /data.
set -euo pipefail

UNIT="${1:-}"
case "${UNIT}" in
  parcel) LANE=PARCEL ;;
  building) LANE=BUILDING ;;
  all)
    status=0
    "${BASH_SOURCE[0]}" parcel || status=$?
    "${BASH_SOURCE[0]}" building || { lane_status=$?; ((status)) || status=${lane_status}; }
    exit "${status}"
    ;;
  *) echo "by-pnu-serving-bake: expected parcel, building or all, got '${UNIT}'" >&2; exit 64 ;;
esac
log() { printf 'by-pnu-serving-bake %s: %s\n' "${UNIT}" "$*"; }
FORCE_FULL="${FOUNDATION_BY_PNU_BAKE_FORCE_FULL:-false}"
FORCE_FULL_REASON="${FOUNDATION_BY_PNU_BAKE_FORCE_FULL_REASON:-}"
if [[ "${FORCE_FULL}" == true && -z "${FORCE_FULL_REASON// /}" ]]; then
  log "refused: FOUNDATION_BY_PNU_BAKE_FORCE_FULL needs FOUNDATION_BY_PNU_BAKE_FORCE_FULL_REASON (the run summary records why)"
  exit 64
fi
[[ "${FORCE_FULL}" == true || "${FORCE_FULL}" == false ]] ||
  { log "refused: FOUNDATION_BY_PNU_BAKE_FORCE_FULL must be true or false"; exit 64; }
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
ENV_PREFIX="FOUNDATION_PLATFORM_${LANE}_BY_PNU_SERVING"
STATE_ROOT="${FOUNDATION_BY_PNU_BAKE_STATE_ROOT:-/data/foundation-platform/by-pnu-bake}/${UNIT}"
MAX_CONCURRENCY="${FOUNDATION_BY_PNU_BAKE_MAX_CONCURRENCY:-128}"
ATTEMPTS="${FOUNDATION_BY_PNU_BAKE_ATTEMPTS:-6}"
RETRY_SECONDS="${FOUNDATION_BY_PNU_BAKE_RETRY_SECONDS:-45}"
# The change set runs in the measured small Spark (compose `spark-small`, 2560m; root ADR-0138).
# Measured on ai-server on 2026-10-03, local[4], driver 1500m: the parcel comparison of two
# 39,861,511-row snapshots peaked at 1,508,237,312 bytes of anonymous memory in 152s; the building
# lane is a seventh of that. With the 4g driver the folds use, the parcel run reached the cap.
DELTA_DRIVER_MEMORY=1500m

# Create-only and forward-only are not the caller's to switch off; the removed in-place switches
# are refused by the publisher if they reach it.
unset "${ENV_PREFIX}_ALLOW_OVERWRITE" "${ENV_PREFIX}_ALLOW_REPOINT" "${ENV_PREFIX}_FIRST_PUBLICATION" \
  "${ENV_PREFIX}_PNU_ALLOWLIST_PATH" "${ENV_PREFIX}_DELETE_LIST_PATH" "${ENV_PREFIX}_TARGET_PATCH" \
  "${ENV_PREFIX}_EXPORT_SUMMARY_PATH" "${ENV_PREFIX}_OUTPUT_ROOT" "${ENV_PREFIX}_ROLLBACK_TO_MANIFEST_KEY"   "${ENV_PREFIX}_PUBLISH_PATCH" "${ENV_PREFIX}_PUBLISH_FROM_LISTING" "${ENV_PREFIX}_CHANGE_SET_SUMMARY_PATH"   "${ENV_PREFIX}_UPSERT_LIST_PATH"
export "${ENV_PREFIX}_OUTPUT_STORAGE_DRIVER=${FOUNDATION_BY_PNU_BAKE_STORAGE_DRIVER:-r2}"
export "${ENV_PREFIX}_RESUME_FROM_LISTING=true"

# The building export checks the approved building links in the runtime database before serving.
# Same source as FLOOR: compose's API connection on its loopback port, never a second stored URL.
if [[ "${UNIT}" == building && -z "${DATABASE_URL:-}" ]]; then
  if ! DATABASE_URL="$(docker compose --project-directory "${RELEASE_ROOT}" \
    --env-file /dev/null -f "${RELEASE_ROOT}/docker-compose.yml" \
    config --format json --no-env-resolution 2>/dev/null \
    | python3 "${RELEASE_ROOT}/scripts/ops/runtime-database-url.py")"; then
    log "refused: cannot resolve the runtime database connection the building export reads"
    exit 78
  fi
  export DATABASE_URL
fi

mkdir -p "${STATE_ROOT}"

# 1. What is there to do?
state="${STATE_ROOT}/state.json"
rm -f "${state}"
FOUNDATION_PLATFORM_BY_PNU_SERVING_STATE_PATH="${state}" \
  "${PUBLISHER_BIN}" "show-${UNIT}-by-pnu-serving-state"
read -r gold base base_objects reflected patch_count newest_patch cumulative served_schema \
  schema_now highest_listed highest_patch_listed max_patches max_ratio max_delta_fraction \
  served_prefix_length prefix_length < <(python3 -I - "${state}" <<'PY'
import json, sys
state = json.load(open(sys.argv[1]))
if state.get("schema_version") != "foundation-platform.by_pnu_serving_state.v2":
    sys.exit(f"unexpected lane state schema {state.get('schema_version')!r}")
published, policy = state["published"], state["policy"]
for name in ("generations_with_objects", "patches_with_objects"):
    listed = state[name]
    if not (isinstance(listed, list) and all(type(g) is int and g >= 1 for g in listed)):
        sys.exit(f"{name} is not a list of generations: {listed!r}")
print(state["gold_iceberg_snapshot_id"] or "-", published["base_generation"],
      published["base_object_count"], published["reflected_gold_iceberg_snapshot_id"],
      published["patch_count"], published["newest_patch"], published["cumulative_changes"],
      published["document_schema_version"] or "-", state["document_schema_version"],
      max(state["generations_with_objects"], default=0),
      max(state["patches_with_objects"], default=0),
      policy["max_patches"], policy["max_cumulative_change_ratio"], policy["max_delta_fraction"],
      published["pnu_prefix_length"] or "-", policy["pnu_prefix_length"])
PY
)
[[ -n "${prefix_length:-}" ]] || { log "refused: cannot read the lane state ${state}"; exit 65; }
if [[ "${gold}" == - ]]; then
  log "nothing to do: the Gold table has no snapshot"
  exit 0
fi
if [[ "${gold}" == "${reflected}" ]]; then
  log "nothing to do: the served state (generation ${base}, ${patch_count} patches) already reflects Gold snapshot ${gold}"
  exit 0
fi

# Every choice and count of this run, for the run summary.
declare -A summary=([unit]="${UNIT}" [gold_iceberg_snapshot_id]="${gold}"
  [reflected_gold_iceberg_snapshot_id]="${reflected}" [base_generation]="${base}"
  [base_object_count]="${base_objects}" [patches_before]="${patch_count}"
  [cumulative_changes_before]="${cumulative}" [max_patches]="${max_patches}"
  [max_cumulative_change_ratio]="${max_ratio}" [forced_full_reason]="${FORCE_FULL_REASON}")
write_summary() {
  local args=()
  for key in "${!summary[@]}"; do args+=("${key}=${summary[${key}]}"); done
  python3 -I - "${run}/run-summary.json" "${args[@]}" <<'PY'
import json, sys
path, pairs = sys.argv[1], sys.argv[2:]
summary = dict(pair.split("=", 1) for pair in pairs)
counts = {"base_generation", "base_object_count", "patches_before", "cumulative_changes_before",
          "max_patches", "upserts", "deletes", "new", "target_generation", "target_patch",
          "documents", "tombstones"}
for key in counts & summary.keys():
    summary[key] = int(summary[key])
summary["max_cumulative_change_ratio"] = float(summary["max_cumulative_change_ratio"])
summary["schema_version"] = "foundation-platform.by_pnu_serving_bake_run.v1"
with open(path, "w") as out:
    json.dump(summary, out, sort_keys=True, indent=2)
    out.write("\n")
PY
}

# 2. Patch, reflect or full?
mode="" reason=""
if [[ "${FORCE_FULL}" == true ]]; then
  mode=full reason="forced by the operator: ${FORCE_FULL_REASON}"
elif [[ "${served_schema}" != "${schema_now}" ]]; then
  mode=full reason="the export bakes ${schema_now} documents, the base holds ${served_schema}"
elif ((patch_count >= max_patches)); then
  mode=full reason="the base carries ${patch_count} patches, the contract's max_patches is ${max_patches}"
elif ((patch_count > 0)) && [[ "${served_prefix_length}" != "${prefix_length}" ]]; then
  mode=full reason="the patches are listed by ${served_prefix_length}-digit prefixes, the contract's pnu_prefix_length is ${prefix_length}"
fi

run_delta() {
  local work="$1" container="/workspace/target/lakehouse/${1#"${STATE_ROOT}/"}" rc=0
  mkdir -p "${work}"
  chmod 0777 "${work}" # Spark 컨테이너는 uid 185 로 쓴다.
  docker rm foundation-platform-lakehouse-target-init >/dev/null 2>&1 || true
  FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${STATE_ROOT}" \
  FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
  docker compose --project-directory "${RELEASE_ROOT}" -f "${RELEASE_ROOT}/compose.lakehouse.yml" \
    -p foundation-platform-compute --profile lakehouse-batch run --rm \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
    spark-small spark-submit --master 'local[4]' --driver-memory "${DELTA_DRIVER_MEMORY}" \
    --jars "${SPARK_RELEASE_JARS}" \
    /workspace/infra/lakehouse/spark/jobs/by_pnu_panel_delta.py \
    --unit "${UNIT}" --baseline-snapshot-id "${reflected}" --current-snapshot-id "${gold}" \
    --max-delta-fraction "${max_delta_fraction}" \
    --upsert-output "${container}/upserts.txt" --delete-output "${container}/deletes.txt" \
    --summary-output "${container}/change-set.json" >"${work}/delta.log" 2>&1 || rc=$?
  return "${rc}"
}

if [[ -z "${mode}" ]]; then
  delta="${STATE_ROOT}/runs/${gold}-delta"
  rm -rf "${delta}"
  delta_rc=0
  run_delta "${delta}" || delta_rc=$?
  case "${delta_rc}" in
    0) ;;
    3) tail -n 3 "${delta}/delta.log" >&2
       log "FAILED: no comparison snapshot for the change set (reflected ${reflected}); that is not 'no change'. Nothing was published. A full bake (FOUNDATION_BY_PNU_BAKE_FORCE_FULL with a reason) re-bases the lane"
       exit 1 ;;
    4) tail -n 3 "${delta}/delta.log" >&2
       log "FAILED: the change set from ${reflected} to ${gold} is more than max_delta_fraction ${max_delta_fraction} of the table — not a delta; nothing was published (ADR-0141 §3)"
       exit 1 ;;
    *) tail -n 5 "${delta}/delta.log" >&2
       log "FAILED: the change set job exited ${delta_rc}; nothing was published"
       exit 1 ;;
  esac
  change="$(python3 -I - "${delta}/change-set.json" <<'PY'
import json, sys
metrics = json.load(open(sys.argv[1]))["quality_metrics"]
print(metrics["upsert_count"], metrics["delete_count"], metrics["new_count"])
PY
)"
  read -r upserts deletes new_count <<<"${change}"
  summary[upserts]="${upserts}" summary[deletes]="${deletes}" summary[new]="${new_count}"
  over="$(python3 -I -c 'import sys; c, u, d, b, r = map(float, sys.argv[1:]); print("yes" if (c + u + d) / max(b, 1) > r else "no")' \
    "${cumulative}" "${upserts}" "${deletes}" "${base_objects}" "${max_ratio}")"
  if ((upserts + deletes == 0)); then
    mode=reflect reason="no row_digest differs between ${reflected} and ${gold}"
  elif [[ "${over}" == yes ]]; then
    mode=full reason="the patches would carry $((cumulative + upserts + deletes)) changes, over max_cumulative_change_ratio ${max_ratio} of the base's ${base_objects}"
  else
    mode=patch reason="${upserts} upserts and ${deletes} deletes"
  fi
fi
summary[mode]="${mode}" summary[reason]="${reason}"
log "Gold snapshot ${gold} (reflected: ${reflected}): ${mode} — ${reason}"
# One work directory per target, so a later run never counts an earlier target's shard summaries.
open_run() {
  run="${STATE_ROOT}/runs/$1"
  mkdir -p "${run}"
  [[ "${mode}" == full ]] || cp "${delta}/change-set.json" "${delta}/upserts.txt" "${delta}/deletes.txt" "${run}/"
  write_summary
}

if [[ "${mode}" == reflect ]]; then
  open_run "${gold}-reflect"
  env "${ENV_PREFIX}_CONFIRM_PUBLISH=true" "${ENV_PREFIX}_PUBLISH_PATCH=true" \
    "${ENV_PREFIX}_TARGET_GENERATION=${base}" \
    "${ENV_PREFIX}_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID=${gold}" \
    "${ENV_PREFIX}_CHANGE_SET_SUMMARY_PATH=${run}/change-set.json" \
    "${ENV_PREFIX}_UPSERT_LIST_PATH=${run}/upserts.txt" \
    "${ENV_PREFIX}_DELETE_LIST_PATH=${run}/deletes.txt" \
    "${PUBLISHER_BIN}" "publish-${UNIT}-by-pnu-serving-manifest"
  summary[published]=reflect
  write_summary
  log "published: generation ${base} now reflects Gold snapshot ${gold}; no object changed"
  exit 0
fi

# 3. Which generation, or which patch?
progress="${STATE_ROOT}/in-progress.json"
read -r target fresh < <(python3 -I - "${progress}" "${gold}" "${mode}" "${base}" "${highest_listed}" \
  "${newest_patch}" "${highest_patch_listed}" <<'PY'
import json, pathlib, sys
progress, gold, mode = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]
base, listed, newest_patch, listed_patch = map(int, sys.argv[4:])
earlier = json.loads(progress.read_text()) if progress.exists() else None
if earlier and "target" not in earlier:  # a record of the full-only bake before ADR-0141
    earlier = {**earlier, "mode": "full", "target": earlier["target_generation"]}
floor = base if mode == "full" else newest_patch
if (earlier and earlier["gold_iceberg_snapshot_id"] == gold and earlier.get("mode", "full") == mode
        and earlier.get("base_generation", base) == base and earlier["target"] > floor):
    print(earlier["target"], "false")  # resume what this lane half-wrote for this snapshot
else:
    # A target half-written for another snapshot, or by a run that left no record here, is never
    # reused: its objects carry another snapshot. It is left in place, unserved.
    above = (base, listed) if mode == "full" else (newest_patch, listed_patch)
    same_kind = earlier and earlier.get("mode", "full") == mode and earlier.get("base_generation", base) == base
    print(max(*above, earlier["target"] if same_kind else 0) + 1, "true")
PY
)
[[ -n "${fresh:-}" ]] || { log "refused: cannot choose a target"; exit 65; }
# The target becomes this lane's own (a later run resumes it) only once one of its shards has been
# checked empty; before that, a refused or crashed run leaves no record to adopt it by.
record_target() {
  printf '{"gold_iceberg_snapshot_id": "%s", "mode": "%s", "base_generation": %s, "target": %s}\n' \
    "${gold}" "${mode}" "${base}" "${target}" >"${progress}.next"
  mv "${progress}.next" "${progress}"
}
[[ "${fresh}" == true ]] || record_target
if [[ "${mode}" == full ]]; then
  open_run "${gold}-g${target}"
  generation="${target}" patch_env=() where="generation ${target}"
  plan="${STATE_ROOT}/shard-plan.txt"
  if [[ -s "${plan}" ]]; then mapfile -t queue <"${plan}"; else queue=(1 2 3 4 5 6 7 8 9); fi
  summary[target_generation]="${target}"
else
  open_run "${gold}-g${base}p${target}"
  generation="${base}" where="generation ${base} patch ${target}"
  patch_env=("${ENV_PREFIX}_TARGET_PATCH=${target}" "${ENV_PREFIX}_PNU_ALLOWLIST_PATH=${run}/upserts.txt"
    "${ENV_PREFIX}_DELETE_LIST_PATH=${run}/deletes.txt")
  plan=""
  queue=(all) # the scan keeps only the change set's rows
  summary[target_generation]="${base}" summary[target_patch]="${target}"
fi
write_summary
log "baking Gold snapshot ${gold} into ${where} (highest generation holding objects: ${highest_listed}; new ${mode/full/generation}: ${fresh})"

done_shards=()
while ((${#queue[@]})); do
  prefix="${queue[0]}"; queue=("${queue[@]:1}")
  [[ "${prefix}" =~ ^[0-9]{1,10}$ || "${prefix}" == all ]] || { log "refused: shard plan holds '${prefix}'"; exit 65; }
  shard_summary="${run}/shard-${prefix}.json"
  if [[ -s "${shard_summary}" ]]; then done_shards+=("${prefix}"); continue; fi
  prefix_env=()
  [[ "${prefix}" == all ]] || prefix_env=("${ENV_PREFIX}_PNU_PREFIX=${prefix}")
  baked=""
  for attempt in $(seq 1 "${ATTEMPTS}"); do
    attempt_log="${run}/shard-${prefix}.attempt-${attempt}.log"
    # In a target this run started, a shard demands an empty key range until the export has
    # recorded the check passing (before its first write); only then may a retry resume over what
    # an earlier attempt wrote.
    checked="${run}/shard-${prefix}.fresh-checked"
    shard_fresh=false
    [[ "${fresh}" == true && ! -s "${checked}" ]] && shard_fresh=true
    if env "${ENV_PREFIX}_CONFIRM_EXPORT=true" "${ENV_PREFIX}_TARGET_GENERATION=${generation}" \
        "${patch_env[@]}" "${prefix_env[@]}" "${ENV_PREFIX}_MAX_CONCURRENCY=${MAX_CONCURRENCY}" \
        "${ENV_PREFIX}_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID=${gold}" \
        "${ENV_PREFIX}_FRESH_GENERATION=${shard_fresh}" \
        "${ENV_PREFIX}_FRESH_CHECK_MARKER_PATH=${checked}" \
        "${ENV_PREFIX}_SUMMARY_PATH=${shard_summary}.partial" \
        "${PUBLISHER_BIN}" "export-${UNIT}-by-pnu-serving" >"${attempt_log}" 2>&1; then
      mv "${shard_summary}.partial" "${shard_summary}"
      [[ "${fresh}" == true && -s "${checked}" ]] && record_target
      baked=yes
      break
    fi
    rm -f "${shard_summary}.partial"
    [[ "${fresh}" == true && -s "${checked}" ]] && record_target
    # Refusals a retry cannot change end the run now, not after every other shard.
    if grep -q 'moved during the bake' "${attempt_log}"; then
      tail -n 5 "${attempt_log}" >&2
      log "FAILED: the Gold table moved off snapshot ${gold} during the bake; nothing was published, the next run starts over"
      log "abandoned: ${where} holds the objects this bake wrote for Gold snapshot ${gold}; it is never served or resumed, and removing it is a manual step (runbook 8절)"
      exit 1
    fi
    if grep -q 'this run did not start' "${attempt_log}"; then
      tail -n 5 "${attempt_log}" >&2
      # Another writer holds this target: never let a later run resume it as this lane's own.
      rm -f "${progress}"
      log "FAILED: ${where} already holds objects this run did not write; nothing was published, the next run starts above it"
      exit 1
    fi
    if grep -q 'shard the run with' "${attempt_log}"; then
      if [[ "${prefix}" == all ]]; then
        log "the change set holds more rows than one run may keep; splitting it into shards 1..9"
        queue+=(1 2 3 4 5 6 7 8 9)
      else
        ((${#prefix} < 10)) || { log "refused: shard ${prefix} is at full PNU length and still too large"; exit 65; }
        log "shard ${prefix} holds more rows than one run may keep; splitting it into ${prefix}0..${prefix}9"
        for digit in 0 1 2 3 4 5 6 7 8 9; do queue+=("${prefix}${digit}"); done
      fi
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
[[ -z "${plan}" ]] || { printf '%s\n' "${done_shards[@]}" >"${plan}.next" && mv "${plan}.next" "${plan}"; }

# 4. Complete? Then publish.
counts="$(python3 -I - "${run}" "${gold}" "${mode}" "${generation}" "${target}" \
  "${upserts:-0}" "${deletes:-0}" "${done_shards[@]}" <<'PY'
import json, pathlib, sys
run, gold, mode = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]
generation, target, upserts, deletes = map(int, sys.argv[4:8])
shards = sys.argv[8:]
def refuse(reason):
    sys.exit("by-pnu-serving-bake: refused to publish: " + reason)
named = [shard for shard in shards if shard != "all"]
nested = sorted(f"{a} inside {b}" for a in named for b in named if a != b and a.startswith(b))
if nested or len(set(shards)) != len(shards) or ("all" in shards and len(shards) > 1):
    refuse(f"shards overlap ({', '.join(nested) or 'repeated or whole-table shard'}); their rows would count twice")
gold_rows, exported, tombstones = set(), 0, 0
for prefix in shards:
    summary = json.loads((run / f"shard-{prefix}.json").read_text())
    if summary["gold_iceberg_snapshot_id"] != gold:
        refuse(f"shard {prefix} baked Gold snapshot {summary['gold_iceberg_snapshot_id']}, not {gold}; "
               "the table moved during the bake, the next run starts over")
    want_patch = None if mode == "full" else target
    if (summary["target_generation"] != generation or summary.get("target_patch") != want_patch
            or summary.get("pnu_prefix") != (None if prefix == "all" else prefix)):
        refuse(f"shard {prefix}'s summary is for generation {summary['target_generation']} patch "
               f"{summary.get('target_patch')} prefix {summary.get('pnu_prefix')}")
    gold_rows.add(summary["scanned_row_count"])
    exported += summary["exported_row_count"]
    tombstones += summary.get("tombstone_count", 0)
if len(gold_rows) != 1:
    refuse(f"shards scanned different Gold row counts {sorted(gold_rows)}")
(total,) = gold_rows
if mode == "full":
    if exported != total or tombstones:
        refuse(f"incomplete bake: shards exported {exported} of the Gold snapshot's {total} rows "
               f"({total - exported} missing); the manifest stays where it is")
    print(total, 0)
else:
    if exported != upserts or tombstones != deletes:
        refuse(f"incomplete patch: shards wrote {exported} of {upserts} upserts and {tombstones} of "
               f"{deletes} tombstones; the manifest stays where it is")
    print(exported, tombstones)
PY
)"
read -r expected tombstones <<<"${counts}"
if [[ "${mode}" == full ]]; then
  log "complete: ${#done_shards[@]} shards exported all ${expected} rows; publishing generation ${target}"
  env "${ENV_PREFIX}_CONFIRM_PUBLISH=true" "${ENV_PREFIX}_PUBLISH_FROM_LISTING=true" \
    "${ENV_PREFIX}_TARGET_GENERATION=${target}" \
    "${ENV_PREFIX}_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID=${gold}" \
    "${ENV_PREFIX}_EXPECTED_OBJECT_COUNT=${expected}" \
    "${PUBLISHER_BIN}" "publish-${UNIT}-by-pnu-serving-manifest"
else
  log "complete: patch ${target} holds ${expected} documents and ${tombstones} tombstones; publishing it"
  env "${ENV_PREFIX}_CONFIRM_PUBLISH=true" "${ENV_PREFIX}_PUBLISH_PATCH=true" \
    "${ENV_PREFIX}_TARGET_GENERATION=${base}" "${ENV_PREFIX}_TARGET_PATCH=${target}" \
    "${ENV_PREFIX}_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID=${gold}" \
    "${ENV_PREFIX}_CHANGE_SET_SUMMARY_PATH=${run}/change-set.json" \
    "${ENV_PREFIX}_UPSERT_LIST_PATH=${run}/upserts.txt" \
    "${ENV_PREFIX}_DELETE_LIST_PATH=${run}/deletes.txt" \
    "${PUBLISHER_BIN}" "publish-${UNIT}-by-pnu-serving-manifest"
fi
rm -f "${progress}"
summary[published]="${mode}" summary[documents]="${expected}" summary[tombstones]="${tombstones}"
write_summary
# The shard summaries list every object (hundreds of MB for a national run). Keep their counts.
python3 -I - "${run}" <<'PY'
import json, pathlib, sys
for path in pathlib.Path(sys.argv[1]).glob("shard-*.json"):
    summary = json.loads(path.read_text())
    summary.pop("artifacts", None)
    path.write_text(json.dumps(summary, sort_keys=True) + "\n")
PY
log "published ${where}: ${expected} documents, ${tombstones} tombstones from Gold snapshot ${gold}"
