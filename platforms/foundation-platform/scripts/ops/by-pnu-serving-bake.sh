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
#      reflected snapshot and the new one; it refuses when the comparison snapshot is gone (exit
#      3), carries no row_digest (exit 5), or the change set is more than max_delta_fraction of the
#      table (exit 4); each refusal is named in the log and the run summary, and nothing is
#      published. An empty change set only advances the reflected snapshot ("reflect"); any other
#      is a patch.
#    - a verified re-base (FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE with a
#      FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE_REASON; parcel lane only; root ADR-0146 §1) stands
#      in for the change set when the reflected snapshot cannot be compared:
#      `verify-parcel-by-pnu-serving-rebase` reads every served object and compares it with the
#      current Gold render. Its change set then decides exactly as above, and the manifest and the
#      run summary record its run id and counts. A rerun over the same served state resumes
#      runs/<snapshot>-rebase; one left over another served state is kept under superseded/.
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
# Once the building lane serves from section packs (its manifest names a `section_packs` block, root
# ADR-0147), the same run bakes packs instead of objects, every choice made against the packs'
# state: the change set goes from the packs' reflected snapshot, a patch is
# `export-building-by-pnu-section-packs` with TARGET_PATCH (each section's packs under the
# generation the lane serves that section from, deletes as tombstones), a full bake is a new
# generation of every section, and each is published with `publish-building-by-pnu-section-packs`
# (a reflect publishes the empty change set alone). The object fields of the manifest then stay as
# they were at the cut-over. Until then the lane bakes objects exactly as above. The job entry in
# orchestration/jobs.v1.json declares this (`capabilities`), and the first pack publish refuses a
# release whose job does not.
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
    # The verified re-base is the parcel lane's alone (root ADR-0146 §1).
    env -u FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE -u FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE_REASON \
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
REBASE="${FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE:-false}"
REBASE_REASON="${FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE_REASON:-}"
[[ "${REBASE}" == true || "${REBASE}" == false ]] ||
  { log "refused: FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE must be true or false"; exit 64; }
if [[ "${REBASE}" == true ]]; then
  [[ -n "${REBASE_REASON// /}" ]] ||
    { log "refused: FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE needs FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE_REASON (the manifest and the run summary record why)"; exit 64; }
  [[ "${FORCE_FULL}" == false ]] ||
    { log "refused: a verified re-base and a forced full bake answer the same question; state one"; exit 64; }
  [[ "${UNIT}" == parcel ]] ||
    { log "refused: the verified re-base renders parcel documents; the ${UNIT} lane re-bases with a full bake"; exit 64; }
fi
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/by-pnu-bake-shards.sh"
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
  "${ENV_PREFIX}_EXPORT_SUMMARY_PATH" "${ENV_PREFIX}_OUTPUT_ROOT" "${ENV_PREFIX}_ROLLBACK_TO_MANIFEST_KEY" \
  "${ENV_PREFIX}_PUBLISH_PATCH" "${ENV_PREFIX}_PUBLISH_FROM_LISTING" "${ENV_PREFIX}_CHANGE_SET_SUMMARY_PATH" \
  "${ENV_PREFIX}_UPSERT_LIST_PATH" "${ENV_PREFIX}_REBASE_SAMPLE_PREFIXES" "${ENV_PREFIX}_REBASE_WORK_DIR" \
  "${ENV_PREFIX}_PACK_GENERATION" "${ENV_PREFIX}_PACK_SECTIONS" "${ENV_PREFIX}_PACK_SUMMARY_PATH" \
  "${ENV_PREFIX}_PACK_SUMMARY_DIR" "${ENV_PREFIX}_PACK_EXPECTED_DOCUMENT_COUNT" \
  "${ENV_PREFIX}_PACK_EQUALITY_EVIDENCE_PATH" "${ENV_PREFIX}_PACK_LATENCY_EVIDENCE_PATH" \
  "${ENV_PREFIX}_INSTALLED_JOBS_PATH"
export "${ENV_PREFIX}_OUTPUT_STORAGE_DRIVER=${FOUNDATION_BY_PNU_BAKE_STORAGE_DRIVER:-r2}"
export "${ENV_PREFIX}_RESUME_FROM_LISTING=true"

# The building export checks the approved building links in the runtime database before serving.
# Same source as FLOOR: compose's API connection on its loopback port, never a second stored URL.
if [[ "${UNIT}" == building && -z "${DATABASE_URL:-}" ]]; then
  if ! DATABASE_URL="$(by_pnu_runtime_database_url)"; then
    log "refused: cannot resolve the runtime database connection the building export reads"
    exit 78
  fi
  export DATABASE_URL
fi

mkdir -p "${STATE_ROOT}"
# One run of a lane at a time, publish included: two publishes racing over the manifest and its
# pins is what the lock rules out (root ADR-0146 §2). Like the release build lock, it is taken
# without waiting; the scheduler retries later. A hand-run publish takes the same lock (runbook).
exec 9>>"${STATE_ROOT}/lane.lock"
if ! flock -n 9; then
  log "refused: another run of the ${UNIT} lane holds ${STATE_ROOT}/lane.lock; this run starts after it finishes"
  exit 75
fi

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
# Does the lane serve from section packs (root ADR-0147)? Then every choice below is made against
# the packs: their reflected snapshot, document schema and count, the patches some section still
# reads, the highest section generation, and every generation and patch number holding a pack.
read -r packs packs_reflected packs_schema packs_count packs_newest packs_live packs_cumulative \
  packs_generation packs_listed packs_patch_listed < <(python3 -I - "${state}" <<'PY'
import json, sys
packs = json.load(open(sys.argv[1])).get("section_packs")
if packs is None:
    print("no", *["-"] * 9)
else:
    for name in ("generations_with_packs", "patches_with_packs"):
        listed = packs[name]
        if not (isinstance(listed, list) and all(type(g) is int and g >= 1 for g in listed)):
            sys.exit(f"{name} is not a list of numbers: {listed!r}")
    print("yes", packs["reflected_gold_iceberg_snapshot_id"], packs["document_schema_version"],
          packs["document_count"], packs["newest_patch"], packs["max_live_patches"],
          packs["cumulative_changes"], max(s["generation"] for s in packs["sections"]),
          max(packs["generations_with_packs"], default=0), max(packs["patches_with_packs"], default=0))
PY
)
[[ -n "${packs_patch_listed:-}" ]] || { log "refused: cannot read the section packs of the lane state ${state}"; exit 65; }
LANE_SERVES=objects
if [[ "${packs}" == yes ]]; then
  # A verified re-base reads the served objects (root ADR-0146), which stop taking the daily
  # changes once packs serve: from them it would publish a stale view of the lane.
  [[ "${REBASE}" == false ]] || { log "refused: the ${UNIT} lane serves section packs; a verified re-base reads served objects and does not apply to it (root ADR-0147)"; exit 64; }
  LANE_SERVES=packs
  base="${packs_generation}" base_objects="${packs_count}" reflected="${packs_reflected}"
  patch_count="${packs_live}" newest_patch="${packs_newest}" cumulative="${packs_cumulative}"
  served_schema="${packs_schema}" highest_listed="${packs_listed}" highest_patch_listed="${packs_patch_listed}"
  # Pack patches name legal dongs, not the object lane's prefixes: the prefix rule does not apply.
  served_prefix_length="${prefix_length}"
fi
if [[ "${gold}" == - ]]; then
  log "nothing to do: the Gold table has no snapshot"
  exit 0
fi
if [[ "${gold}" == "${reflected}" ]]; then
  if [[ "${LANE_SERVES}" == packs ]]; then
    log "nothing to do: the served section packs (highest generation ${base}, ${patch_count} patches) already reflect Gold snapshot ${gold}"
  else
    log "nothing to do: the served state (generation ${base}, ${patch_count} patches) already reflects Gold snapshot ${gold}"
  fi
  exit 0
fi

# Every choice and count of this run, for the run summary.
declare -A summary=([unit]="${UNIT}" [gold_iceberg_snapshot_id]="${gold}"
  [reflected_gold_iceberg_snapshot_id]="${reflected}" [base_generation]="${base}"
  [base_object_count]="${base_objects}" [patches_before]="${patch_count}"
  [cumulative_changes_before]="${cumulative}" [max_patches]="${max_patches}"
  [max_cumulative_change_ratio]="${max_ratio}" [forced_full_reason]="${FORCE_FULL_REASON}"
  [verified_rebase_reason]="${REBASE_REASON}" [lane_serves]="${LANE_SERVES}")
write_summary() {
  local args=()
  for key in "${!summary[@]}"; do args+=("${key}=${summary[${key}]}"); done
  python3 -I - "${run}/run-summary.json" "${args[@]}" <<'PY'
import json, sys
path, pairs = sys.argv[1], sys.argv[2:]
summary = dict(pair.split("=", 1) for pair in pairs)
counts = {"base_generation", "base_object_count", "patches_before", "cumulative_changes_before",
          "max_patches", "upserts", "deletes", "new", "target_generation", "target_patch",
          "documents", "tombstones", "rebase_served_objects_read", "rebase_equal",
          "rebase_changed", "rebase_only_served", "rebase_only_gold"}
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
if [[ "${REBASE}" == true && -n "${mode}" ]]; then
  log "refused: the lane needs a full bake (${reason}); a verified re-base cannot stand in for it"
  exit 1
fi

run_delta() {
  local work="$1" container="/workspace/target/lakehouse/${1#"${STATE_ROOT}/"}" rc=0
  mkdir -p "${work}"
  # Spark 컨테이너는 uid 185 로 쓴다. 그 init 단계는 마운트한 상태 루트 자체가 쓰기 가능한지 본다
  # (compose.lakehouse.yml lakehouse-target-init) — 실행 디렉터리만 열면 첫 운영 실행처럼 거부된다.
  chmod 0777 "${STATE_ROOT}" "${work}"
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

# A refused change set: name the refusal in the log and the run summary, publish nothing.
refuse_change_set() {
  summary[refused]="$1"
  run="${delta}"
  write_summary
  log "FAILED: $2"
  exit 1
}
REBASE_OFFER="A verified re-base (FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE with a reason; parcel lane) or a full bake (FOUNDATION_BY_PNU_BAKE_FORCE_FULL with a reason) re-bases the lane"

# The verified re-base (root ADR-0146 §1). One run id per re-base: a rerun of the same snapshot
# over the same served state resumes it, and the publisher refuses a work directory of another
# run. A work directory left by a re-base over another served state (the manifest moved since,
# e.g. a rollback) can never be resumed: it is moved aside, kept, and a new re-base starts.
rotate_stale_rebase() {
  local work="$1" found
  [[ -s "${work}/verify/state.json" ]] || return 0
  found="$(python3 -I - "${work}/verify/state.json" "${reflected}" "${base}" "${patch_count}" "${newest_patch}" <<'PY'
import json, sys
state = json.load(open(sys.argv[1]))
reflected, base, count, newest = sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
patches = state["patches"]
if (state["reflected_gold_iceberg_snapshot_id"], state["base_generation"], len(patches), max(patches, default=0)) != (reflected, base, count, newest):
    print(f"reflected {state['reflected_gold_iceberg_snapshot_id']}, generation {state['base_generation']}, patches {patches}")
PY
)" || { log "refused: cannot read the re-base state ${work}/verify/state.json"; exit 65; }
  [[ -n "${found}" ]] || return 0
  local kept
  kept="${STATE_ROOT}/superseded/${work##*/}-$(date -u +%Y%m%dT%H%M%SZ)"
  mkdir -p "${STATE_ROOT}/superseded"
  mv "${work}" "${kept}"
  log "the re-base work directory ${work} compared another served state (${found}); the lane now serves reflected ${reflected}, generation ${base}, ${patch_count} patches. It is kept as ${kept} and a new re-base starts"
}
run_rebase() {
  local work="$1" rc=0
  mkdir -p "${work}"
  # The random part keeps a re-base started in the same second as a moved-aside one its own id.
  [[ -s "${work}/run-id" ]] || printf 'rebase-%s-%s-%s\n' "${gold}" "$(date -u +%Y%m%dT%H%M%SZ)" \
    "$(python3 -I -c 'import secrets; print(secrets.token_hex(4))')" >"${work}/run-id"
  summary[verified_rebase_run_id]="$(<"${work}/run-id")"
  env "${ENV_PREFIX}_REBASE_REASON=${REBASE_REASON}" \
    "${ENV_PREFIX}_REBASE_RUN_ID=${summary[verified_rebase_run_id]}" \
    "${ENV_PREFIX}_REBASE_WORK_DIR=${work}/verify" \
    "${ENV_PREFIX}_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID=${gold}" \
    "${ENV_PREFIX}_MAX_CONCURRENCY=${MAX_CONCURRENCY}" \
    "${PUBLISHER_BIN}" "verify-${UNIT}-by-pnu-serving-rebase" >>"${work}/verify.log" 2>&1 || rc=$?
  return "${rc}"
}

if [[ -z "${mode}" && "${REBASE}" == true ]]; then
  delta="${STATE_ROOT}/runs/${gold}-rebase"
  log "verified re-base of Gold snapshot ${gold} against what the lane serves (reflected ${reflected}): ${REBASE_REASON}"
  rotate_stale_rebase "${delta}"
  if ! run_rebase "${delta}"; then
    tail -n 5 "${delta}/verify.log" >&2
    if grep -q 'not a delta' "${delta}/verify.log"; then
      refuse_change_set not_a_delta "the verified re-base found more than max_delta_fraction ${max_delta_fraction} of the table differing from what the lane serves — not a delta; nothing was published (ADR-0141 §3)"
    fi
    refuse_change_set verified_rebase_incomplete "the verified re-base did not complete (a read failure, a count that does not add up, or another run's work directory); nothing was published. A rerun resumes ${delta}"
  fi
  for name in change-set.json upserts.txt deletes.txt; do cp "${delta}/verify/${name}" "${delta}/${name}"; done
  read -r rebase_read rebase_equal rebase_changed rebase_only_served rebase_only_gold < <(python3 -I - "${delta}/change-set.json" <<'PY'
import json, sys
found = json.load(open(sys.argv[1]))["verification"]
print(found["served_objects_read"], found["equal"], found["changed"], found["only_served"], found["only_gold"])
PY
)
  summary[rebase_served_objects_read]="${rebase_read}" summary[rebase_equal]="${rebase_equal}"
  summary[rebase_changed]="${rebase_changed}" summary[rebase_only_served]="${rebase_only_served}"
  summary[rebase_only_gold]="${rebase_only_gold}"
elif [[ -z "${mode}" ]]; then
  delta="${STATE_ROOT}/runs/${gold}-delta"
  rm -rf "${delta}"
  delta_rc=0
  run_delta "${delta}" || delta_rc=$?
  case "${delta_rc}" in
    0) ;;
    3) tail -n 3 "${delta}/delta.log" >&2
       refuse_change_set no_comparison_snapshot "Gold no longer holds the reflected snapshot ${reflected} (expired, or never existed), so the change set is unknown — that is not 'no change'. Nothing was published. ${REBASE_OFFER}" ;;
    5) tail -n 3 "${delta}/delta.log" >&2
       refuse_change_set comparison_snapshot_has_no_row_digest "the reflected snapshot ${reflected} has rows without row_digest (written before the ADR-0099 fingerprint), so it cannot be compared — that is not 'no change'. Nothing was published. ${REBASE_OFFER}" ;;
    4) tail -n 3 "${delta}/delta.log" >&2
       refuse_change_set not_a_delta "the change set from ${reflected} to ${gold} is more than max_delta_fraction ${max_delta_fraction} of the table — not a delta; nothing was published (ADR-0141 §3)" ;;
    *) tail -n 5 "${delta}/delta.log" >&2
       refuse_change_set change_set_job_failed "the change set job exited ${delta_rc}; nothing was published" ;;
  esac
fi
if [[ -z "${mode}" ]]; then
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
  if ((upserts + deletes == 0)) && [[ "${REBASE}" == true ]]; then
    mode=reflect reason="the verified re-base read ${rebase_read} served objects and found every one equal to Gold ${gold}"
  elif ((upserts + deletes == 0)); then
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

# Pack runs keep their own work directories and target record beside the object lane's.
RUN_TAG="" PROGRESS_NAME=in-progress.json
[[ "${LANE_SERVES}" == packs ]] && RUN_TAG=packs- PROGRESS_NAME=in-progress-packs.json

if [[ "${mode}" == reflect ]]; then
  open_run "${gold}-${RUN_TAG}reflect"
  if [[ "${LANE_SERVES}" == packs ]]; then
    env "${ENV_PREFIX}_CONFIRM_PACK_PUBLISH=true" \
      "${ENV_PREFIX}_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID=${gold}" \
      "${ENV_PREFIX}_CHANGE_SET_SUMMARY_PATH=${run}/change-set.json" \
      "${ENV_PREFIX}_UPSERT_LIST_PATH=${run}/upserts.txt" \
      "${ENV_PREFIX}_DELETE_LIST_PATH=${run}/deletes.txt" \
      "${PUBLISHER_BIN}" "publish-${UNIT}-by-pnu-section-packs"
  else
    env "${ENV_PREFIX}_CONFIRM_PUBLISH=true" "${ENV_PREFIX}_PUBLISH_PATCH=true" \
      "${ENV_PREFIX}_TARGET_GENERATION=${base}" \
      "${ENV_PREFIX}_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID=${gold}" \
      "${ENV_PREFIX}_CHANGE_SET_SUMMARY_PATH=${run}/change-set.json" \
      "${ENV_PREFIX}_UPSERT_LIST_PATH=${run}/upserts.txt" \
      "${ENV_PREFIX}_DELETE_LIST_PATH=${run}/deletes.txt" \
      "${PUBLISHER_BIN}" "publish-${UNIT}-by-pnu-serving-manifest"
  fi
  summary[published]=reflect
  write_summary
  log "published: the served ${LANE_SERVES} now reflect Gold snapshot ${gold}; nothing was baked"
  exit 0
fi

# 3. Which generation, or which patch?
progress="${STATE_ROOT}/${PROGRESS_NAME}"
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
# Packs need no empty-range check: a pack directory holding another snapshot's packs is refused
# by the export before its first write, and a re-run writes the same bytes. So a pack target is
# this lane's own from the start.
[[ "${fresh}" == true && "${LANE_SERVES}" == objects ]] || record_target
if [[ "${mode}" == full ]]; then
  open_run "${gold}-${RUN_TAG}g${target}"
  generation="${target}" patch_env=() where="generation ${target}"
  plan="${STATE_ROOT}/shard-plan.txt"
  if [[ -s "${plan}" ]]; then mapfile -t queue <"${plan}"; else queue=("${BY_PNU_FIRST_SHARDS[@]}"); fi
  summary[target_generation]="${target}"
else
  open_run "${gold}-${RUN_TAG}g${base}p${target}"
  generation="${base}" where="generation ${base} patch ${target}"
  patch_env=("${ENV_PREFIX}_TARGET_PATCH=${target}" "${ENV_PREFIX}_PNU_ALLOWLIST_PATH=${run}/upserts.txt"
    "${ENV_PREFIX}_DELETE_LIST_PATH=${run}/deletes.txt")
  plan=""
  queue=(all) # the scan keeps only the change set's rows
  summary[target_generation]="${base}" summary[target_patch]="${target}"
fi
if [[ "${LANE_SERVES}" == packs ]]; then
  # A pack patch goes under each section's own served generation; the publisher reads them from
  # the manifest, so the patch names none. Pack summaries get a directory of their own: the
  # publisher reads every *.json in it.
  [[ "${mode}" == full ]] || where="patch ${target} of every section's served generation"
  where="section packs ${where}"
  mkdir -p "${run}/summaries"
fi
write_summary
log "baking Gold snapshot ${gold} into ${where} (highest generation holding ${LANE_SERVES}: ${highest_listed}; new ${mode/full/generation}: ${fresh})"

# What each shard runs: the object export, or the pack export (a base names its generation; a
# patch names none, its sections go under the generations the manifest serves them from).
if [[ "${LANE_SERVES}" == packs ]]; then
  export_command="export-${UNIT}-by-pnu-section-packs" summary_dir="${run}/summaries"
  export_env=("${ENV_PREFIX}_CONFIRM_PACK_EXPORT=true")
  [[ "${mode}" != full ]] || export_env+=("${ENV_PREFIX}_PACK_GENERATION=${generation}")
else
  export_command="export-${UNIT}-by-pnu-serving" summary_dir="${run}"
  export_env=("${ENV_PREFIX}_CONFIRM_EXPORT=true" "${ENV_PREFIX}_TARGET_GENERATION=${generation}")
fi
done_shards=()
while ((${#queue[@]})); do
  prefix="${queue[0]}"; queue=("${queue[@]:1}")
  by_pnu_shard_valid "${prefix}" || [[ "${prefix}" == all ]] || { log "refused: shard plan holds '${prefix}'"; exit 65; }
  shard_summary="${summary_dir}/shard-${prefix}.json"
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
    attempt_env=("${ENV_PREFIX}_FRESH_GENERATION=${shard_fresh}"
      "${ENV_PREFIX}_FRESH_CHECK_MARKER_PATH=${checked}" "${ENV_PREFIX}_SUMMARY_PATH=${shard_summary}.partial")
    [[ "${LANE_SERVES}" == objects ]] || attempt_env=("${ENV_PREFIX}_PACK_SUMMARY_PATH=${shard_summary}.partial")
    if env "${export_env[@]}" \
        "${patch_env[@]}" "${prefix_env[@]}" "${ENV_PREFIX}_MAX_CONCURRENCY=${MAX_CONCURRENCY}" \
        "${ENV_PREFIX}_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID=${gold}" \
        "${attempt_env[@]}" \
        "${PUBLISHER_BIN}" "${export_command}" >"${attempt_log}" 2>&1; then
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
    if grep -q 'holds packs of Gold snapshot' "${attempt_log}"; then
      tail -n 5 "${attempt_log}" >&2
      # Another snapshot's packs sit in the target: never let a later run resume it.
      rm -f "${progress}"
      log "FAILED: ${where} already holds packs of another Gold snapshot; nothing was published, the next run starts above it"
      exit 1
    fi
    if grep -q 'this run did not start' "${attempt_log}"; then
      tail -n 5 "${attempt_log}" >&2
      # Another writer holds this target: never let a later run resume it as this lane's own.
      rm -f "${progress}"
      log "FAILED: ${where} already holds objects this run did not write; nothing was published, the next run starts above it"
      exit 1
    fi
    if by_pnu_shard_over_row_cap "${attempt_log}"; then
      children="$(by_pnu_shard_children "${prefix}")" ||
        { log "refused: shard ${prefix} is at full PNU length and still too large"; exit 65; }
      if [[ "${prefix}" == all ]]; then
        log "the change set holds more rows than one run may keep; splitting it into shards 1..9"
      else
        log "shard ${prefix} holds more rows than one run may keep; splitting it into ${prefix}0..${prefix}9"
      fi
      mapfile -t -O "${#queue[@]}" queue <<<"${children}"
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
counts="$(by_pnu_bake_complete "by-pnu-serving-bake: refused to publish" "${summary_dir}" "${gold}" "${mode}" \
  "${LANE_SERVES}" "${generation}" "${target}" "${upserts:-0}" "${deletes:-0}" "${done_shards[@]}")"
read -r expected tombstones <<<"${counts}"
if [[ "${LANE_SERVES}" == packs ]]; then
  # The publisher holds a base to the Gold row count the catalog records, and a patch to its
  # change set, before it writes the manifest's section_packs block.
  publish_env=("${ENV_PREFIX}_CONFIRM_PACK_PUBLISH=true" "${ENV_PREFIX}_PACK_SUMMARY_DIR=${summary_dir}"
    "${ENV_PREFIX}_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID=${gold}")
  [[ "${mode}" == full ]] || publish_env+=("${ENV_PREFIX}_CHANGE_SET_SUMMARY_PATH=${run}/change-set.json"
    "${ENV_PREFIX}_UPSERT_LIST_PATH=${run}/upserts.txt" "${ENV_PREFIX}_DELETE_LIST_PATH=${run}/deletes.txt")
  log "complete: ${where} holds ${expected} documents and ${tombstones} tombstones; publishing it"
  env "${publish_env[@]}" "${PUBLISHER_BIN}" "publish-${UNIT}-by-pnu-section-packs"
elif [[ "${mode}" == full ]]; then
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
# The shard summaries list every object or pack (hundreds of MB for a national object run). Keep
# their counts.
python3 -I - "${summary_dir}" <<'PY'
import json, pathlib, sys
for path in pathlib.Path(sys.argv[1]).glob("shard-*.json"):
    summary = json.loads(path.read_text())
    summary.pop("artifacts", None)
    summary.pop("packs", None)
    path.write_text(json.dumps(summary, sort_keys=True) + "\n")
PY
log "published ${where}: ${expected} documents, ${tombstones} tombstones from Gold snapshot ${gold}"
