#!/usr/bin/env bash
# The scheduled by-PNU serving bake (root ADR-0122, ADR-0141; ADR-0096 parcels, ADR-0100 buildings).
#
#   by-pnu-serving-bake.sh parcel|building|<a by_pnu_lanes lane>|all
#
# `all` (the registered job) bakes the parcel lane, then the building lane, then every pack-only
# lane config/r2-connections.contract.json names in `by_pnu_lanes` (root ADR-0190), in this one
# process tree: the host memory budget fits one lane's export at a time (root ADR-0138). A failed
# lane does not stop the others; `all` fails if any did. A pack-only lane that has not been cut
# over yet (no manifest: its first publish is the pack-only gate, run by the operator) is
# "nothing to do".
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
#      schema than the served documents hold and it does not expand theirs (a version that only
#      adds optional fields to the served ones is patched over them, root ADR-0320; the full
#      compaction later re-renders every document under it), when the base already carries
#      max_patches patches (or more, after max_patches was lowered), when its patches are listed
#      by another prefix length than
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
#      the Gold snapshot the bake is of and reads that snapshot, wherever the table has moved since.
#    - a long bake finishes at its pin, then patches forward (root ADR-0310): a full bake left
#      unfinished (a deploy stopped it, a shard failed) is resumed at the snapshot it pinned even
#      when Gold has moved since, its finished shards skipped; once it is published, the next run
#      patches it forward to the current snapshot through the change set of step 2. Not resumed at
#      its pin: a bake whose shards hold another document schema than the export bakes now, and
#      one refused as incomplete; both are left unserved and the next full bake goes above them.
#      A pin Gold no longer holds (expired) abandons its generation the same way.
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
# `all` ends with `foundation-job-outcome changed` when a lane published (a patch, a base or a
# reflect-only manifest), `unchanged` otherwise (root ADR-0171), and `failed <lanes>; published:
# <lanes>` when any lane failed: the last word matches the exit status. When nothing failed or
# published and a lane found its lock held by another run (an operator's attended bake, 2026-10-19),
# the run waits for it like any job (held-lock.sh): `waiting the lanes <lanes> is held by another run`.
# A lane run on its own ends with its own line: `changed`, `unchanged` or `failed <reason>`.
#
# The run summary (runs/<snapshot>-<mode>/run-summary.json) records the choice, its reason and the
# counts. Never overwrites an object and never repoints a published generation. Work files live
# under the state root on /data.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/held-lock.sh"

UNIT="${1:-}"
# The pack-only lanes, by name, from the contract this script ships with.
mapfile -t PACK_LANES < <(python3 -I - "$(dirname "${BASH_SOURCE[0]}")/../../config/r2-connections.contract.json" <<'PY'
import json, sys
print("\n".join(json.load(open(sys.argv[1])).get("by_pnu_lanes", {})))
PY
)
case "${UNIT}" in
  parcel | building) ;;
  all)
    status=0
    # Each lane that publishes names itself here; the job's last line says whether any did (root
    # ADR-0171).
    BY_PNU_BAKE_PUBLISHED="$(mktemp)"
    export BY_PNU_BAKE_PUBLISHED
    # A lane that published but withheld a document names itself here (root ADR-0325 §4).
    BY_PNU_BAKE_WITHHELD="$(mktemp)"
    export BY_PNU_BAKE_WITHHELD
    # The lanes run as hand runs (exit 75 when held) and name what they found held; this run decides.
    FOUNDATION_HELD_LOCKS="$(mktemp)"
    export FOUNDATION_HELD_LOCKS
    job_service="${FOUNDATION_JOB_SERVICE:-}"
    unset FOUNDATION_JOB_SERVICE
    failed=" " waiting=" "
    # A lane's exit: 75 from a lane that found its lock held is waiting; anything else non-zero failed.
    lane_exit() {
      local lane="$1" code="$2"
      if ((code == 75)) && grep -qx -- "the ${lane} lane" "${FOUNDATION_HELD_LOCKS}"; then
        waiting+="${lane} "
      else
        failed+="${lane} "
        ((status)) || status=${code}
      fi
    }
    "${BASH_SOURCE[0]}" parcel || lane_exit parcel $?
    # The verified re-base is the parcel lane's alone (root ADR-0146 §1).
    for lane in building ${PACK_LANES[@]+"${PACK_LANES[@]}"}; do
      [[ -n "${lane}" ]] || continue
      # A lane whose packs another job makes (the job that carries the pipeline-graph edge into
      # `<lane>-by-pnu-section-packs`) bakes in that job, concurrently with this one: the own-producer
      # public-auction kind (root ADR-0303) and the unit price lane (root ADR-0333, one job per
      # asset). `all` is by_pnu_serving_bake's program. Its superseded generations are still retired
      # below, like every lane's. A job list that cannot be read leaves the lane here, as before.
      baked_by=""
      [[ "${lane}" == building ]] || baked_by="$(python3 -I -B "$(dirname "${BASH_SOURCE[0]}")/../../orchestration/dags/job_specs.py" --writer "${lane}-by-pnu-section-packs")" || baked_by=""
      if [[ -n "${baked_by}" && "${baked_by}" != by_pnu_serving_bake ]]; then
        echo "by-pnu-serving-bake ${lane}: skipped: the lane bakes in its own job ${baked_by} (root ADR-0303, ADR-0333)"
        continue
      fi
      # A lane whose documents follow another's (contract documents_follow_lane, root ADR-0190) is
      # not baked in a run where that lane failed: it would serve units where the served documents
      # of the other do not place them yet. The failed lane fails the run already.
      # A kind line follows its host lane unless it names another (root ADR-0193 §9; by_pnu_kinds.py).
      follows=""
      [[ "${lane}" == building ]] || follows="$(python3 -I "$(dirname "${BASH_SOURCE[0]}")/../../infra/lakehouse/spark/jobs/by_pnu_kinds.py" "${lane}" documents_follow_lane)"
      if [[ -n "${follows}" && "${failed}" == *" ${follows} "* ]]; then
        echo "by-pnu-serving-bake ${lane}: skipped: the ${follows} lane it follows failed in this run; it is baked with it on the next run"
        continue
      fi
      if [[ -n "${follows}" && "${waiting}" == *" ${follows} "* ]]; then
        echo "by-pnu-serving-bake ${lane}: waiting: the ${follows} lane it follows is held by another run; it is baked with it on the next run"
        waiting+="${lane} "
        continue
      fi
      env -u FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE -u FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE_REASON \
        "${BASH_SOURCE[0]}" "${lane}" || lane_exit "${lane}" $?
    done
    # Then every lane's superseded pack generations past their rollback window (root ADR-0242).
    "${BASH_SOURCE[0]}" retire || { lane_status=$?; failed+="retire "; ((status)) || status=${lane_status}; }
    # A lane that withheld a document published the rest and fails the run here (root ADR-0325 §4).
    while read -r lane; do
      [[ -n "${lane}" ]] || continue
      failed+="${lane}-withheld "
      ((status)) || status=1
    done <"${BY_PNU_BAKE_WITHHELD}"
    # The last word matches the exit: a run that exits non-zero says failed, though another lane
    # published (2026-10-16: `changed` and then exit 1). What did publish is named with it.
    # A run where nothing failed or published but a lane waited ends as that lane's wait.
    outcome=unchanged
    if ((status)); then
      published="$(sort -u "${BY_PNU_BAKE_PUBLISHED}" | tr '\n' ' ')"
      published="${published% }"
      outcome="failed${failed% }; published: ${published:-none}"
    elif [[ -s "${BY_PNU_BAKE_PUBLISHED}" ]]; then
      outcome=changed
    elif [[ "${waiting}" != " " ]]; then
      outcome=waiting
    fi
    rm -f "${BY_PNU_BAKE_PUBLISHED}" "${BY_PNU_BAKE_WITHHELD}" "${FOUNDATION_HELD_LOCKS}"
    if [[ "${outcome}" == waiting ]]; then
      waiting="${waiting# }"
      export FOUNDATION_JOB_SERVICE="${job_service}" FOUNDATION_HELD_LOCKS=""
      lock_held_by_another_run "the lanes ${waiting% }"
    fi
    echo "foundation-job-outcome ${outcome}"
    exit "${status}"
    ;;
  retire)
    # Root ADR-0242: a pack generation the manifest no longer serves is deleted once the one that
    # replaced it has been live for the contract's superseded_generation_keep_days; until then it is
    # the rollback. Each lane under its own lock, so no publish of that lane runs beside it.
    source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
    status=0
    for lane in parcel building ${PACK_LANES[@]+"${PACK_LANES[@]}"}; do
      [[ -n "${lane}" ]] || continue
      prefix="FOUNDATION_PLATFORM_$(tr 'a-z-' 'A-Z_' <<<"${lane}")_BY_PNU_SERVING"
      lock="${FOUNDATION_BY_PNU_BAKE_STATE_ROOT:-/data/foundation-platform/by-pnu-bake}/${lane}/lane.lock"
      mkdir -p "$(dirname "${lock}")"
      if ! (
        flock -n 9 || { echo "by-pnu-serving-bake ${lane}: retire skipped: another run holds ${lock}"; exit 0; }
        env "${prefix}_OUTPUT_STORAGE_DRIVER=${FOUNDATION_BY_PNU_BAKE_STORAGE_DRIVER:-r2}"           "${prefix}_PACK_RETIRE_EXECUTE=true" "${PUBLISHER_BIN}" "retire-${lane}-superseded-pack-generations"
      ) 9>>"${lock}"; then
        echo "by-pnu-serving-bake ${lane}: retiring superseded pack generations failed; the next run tries again"
        status=1
      fi
    done
    exit "${status}"
    ;;
  *)
    [[ -n "${UNIT}" && " ${PACK_LANES[*]-} " == *" ${UNIT} "* ]] ||
      { echo "by-pnu-serving-bake: expected parcel, building, a by_pnu_lanes lane or all, got '${UNIT}'" >&2; exit 64; }
    ;;
esac
# The lane's environment prefix (FOUNDATION_PLATFORM_<LANE>_BY_PNU_SERVING), as the publisher derives it.
LANE="$(tr 'a-z-' 'A-Z_' <<<"${UNIT}")"
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
  lock_held_by_another_run "the ${UNIT} lane"
fi
# A lane run on its own (not a lane of `all`, which decides the run's outcome above) ends with the
# outcome line itself, whichever way it exits (root ADR-0260: a run without one is failed).
OWN_RUN=false LANE_WAITING=""
if [[ -z "${BY_PNU_BAKE_PUBLISHED:-}" ]]; then
  OWN_RUN=true
  BY_PNU_BAKE_PUBLISHED="$(mktemp)"
  lane_outcome() {
    local code=$?
    if ((code)); then
      echo "foundation-job-outcome failed the ${UNIT} lane exited ${code}"
    elif [[ -n "${LANE_WAITING}" ]]; then
      echo "foundation-job-outcome waiting ${LANE_WAITING}"
    elif [[ -s "${BY_PNU_BAKE_PUBLISHED}" ]]; then
      echo "foundation-job-outcome changed"
    else
      echo "foundation-job-outcome unchanged"
    fi
    rm -f "${BY_PNU_BAKE_PUBLISHED}"
  }
  trap lane_outcome EXIT
fi
# Root ADR-0333 §7: every run that leaves the lane serving a Gold snapshot records which one it read,
# appended to the lane's reads.jsonl (never rewritten) and as a result line. The manifest is the
# commit record; this is the asset reconciler's cheap view of it (scripts/ops/asset_reconcile.py),
# which starts the lane again when its Gold moves past it. A run waiting for the lane its documents
# follow records what it waits for, so the reconciler starts it again once that lane moves.
# record_read <served snapshot> [<gold snapshot it waits to bake> <followed lane> <followed snapshot>]
record_read() {
  python3 -I - "${STATE_ROOT}/reads.jsonl" "${UNIT}" "$@" <<'PY'
import datetime, json, sys
path, lane, served, *waiting = sys.argv[1:]
record = {"at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "lane": lane,
          "read_snapshot_id": served, "outcome": "waiting" if waiting else "served"}
if waiting:
    record["gold_snapshot_id"], follows, followed = waiting
    record["waiting_on"] = {"lane": follows, "read_snapshot_id": followed}
with open(path, "a", encoding="utf-8") as reads:
    reads.write(json.dumps(record, sort_keys=True) + "\n")
PY
  echo "foundation-asset-read ${UNIT} gold=$1"
}

# 1. What is there to do?
state="${STATE_ROOT}/state.json"
rm -f "${state}"
FOUNDATION_PLATFORM_BY_PNU_SERVING_STATE_PATH="${state}" \
  "${PUBLISHER_BIN}" "show-${UNIT}-by-pnu-serving-state"
if python3 -I -c 'import json, sys; sys.exit(json.load(open(sys.argv[1])).get("cut_over") is not False)' "${state}"; then
  log "nothing to do: the pack-only ${UNIT} lane is not cut over yet; its first publish is the operator's pack-only gate (root ADR-0190)"
  exit 0
fi
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
# The packs may serve documents of more than one schema (root ADR-0320): the base's, and a later
# one a patch expanded it to. `same`: all are the one the export bakes; `expands`: the export's
# version only adds optional fields to each (the state's readable versions), so a patch of it goes
# over them; `breaks`: one of them is not readable, and only a new generation serves the export's.
read -r packs packs_reflected packs_schema packs_count packs_newest packs_live packs_cumulative \
  packs_generation packs_listed packs_patch_listed packs_served packs_schemas < <(python3 -I - "${state}" <<'PY'
import json, sys
state = json.load(open(sys.argv[1]))
packs = state.get("section_packs")
if packs is None:
    print("no", *["-"] * 11)
else:
    for name in ("generations_with_packs", "patches_with_packs"):
        listed = packs[name]
        if not (isinstance(listed, list) and all(type(g) is int and g >= 1 for g in listed)):
            sys.exit(f"{name} is not a list of numbers: {listed!r}")
    now = state["document_schema_version"]
    served = packs.get("served_document_schema_versions") or [packs["document_schema_version"]]
    readable = state.get("readable_document_schema_versions") or [now]
    if not all(isinstance(v, str) and v and " " not in v for v in [*served, *readable]):
        sys.exit(f"the document schemas are not names: {served!r} {readable!r}")
    verdict = ("same" if set(served) == {now}
               else "expands" if readable[0] == now and set(served) <= set(readable) else "breaks")
    print("yes", packs["reflected_gold_iceberg_snapshot_id"], packs["document_schema_version"],
          packs["document_count"], packs["newest_patch"], packs["max_live_patches"],
          packs["cumulative_changes"], max(s["generation"] for s in packs["sections"]),
          max(packs["generations_with_packs"], default=0), max(packs["patches_with_packs"], default=0),
          ",".join(sorted(served)), verdict)
PY
)
[[ -n "${packs_schemas:-}" ]] || { log "refused: cannot read the section packs of the lane state ${state}"; exit 65; }
LANE_SERVES=objects
# An object base holds one document schema; only packs carry patches of an expanding one.
schemas=same
[[ "${served_schema}" == "${schema_now}" ]] || schemas=breaks
if [[ "${packs}" == yes ]]; then
  # A verified re-base reads the served objects (root ADR-0146), which stop taking the daily
  # changes once packs serve: from them it would publish a stale view of the lane.
  [[ "${REBASE}" == false ]] || { log "refused: the ${UNIT} lane serves section packs; a verified re-base reads served objects and does not apply to it (root ADR-0147)"; exit 64; }
  LANE_SERVES=packs
  base="${packs_generation}" base_objects="${packs_count}" reflected="${packs_reflected}"
  patch_count="${packs_live}" newest_patch="${packs_newest}" cumulative="${packs_cumulative}"
  served_schema="${packs_served}" schemas="${packs_schemas}"
  highest_listed="${packs_listed}" highest_patch_listed="${packs_patch_listed}"
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
  record_read "${reflected}"
  exit 0
fi

# Pack runs keep their own work directories and target record beside the object lane's.
RUN_TAG="" PROGRESS_NAME=in-progress.json
[[ "${LANE_SERVES}" == packs ]] && RUN_TAG=packs- PROGRESS_NAME=in-progress-packs.json
progress="${STATE_ROOT}/${PROGRESS_NAME}"

# Root ADR-0310: a full bake this lane left unfinished at the Gold snapshot it pinned is finished
# there, though Gold has moved since; the run after its publish patches it forward. On 2026-10-24
# generation 5 of the parcel lane was thrown away after 15 h because Gold moved, and generation 6
# started from zero. Not when its shards hold another document schema than the export bakes now
# (one generation is one schema), nor once it was refused as incomplete (resuming changes nothing).
gold_now="${gold}"
pinned="$(python3 -I - "${progress}" "${gold}" "${base}" "${schema_now}" "${STATE_ROOT}/runs" "${RUN_TAG}" <<'PY'
import json, pathlib, sys
progress, gold, base, schema = pathlib.Path(sys.argv[1]), sys.argv[2], int(sys.argv[3]), sys.argv[4]
runs, tag = pathlib.Path(sys.argv[5]), sys.argv[6]
earlier = json.loads(progress.read_text()) if progress.exists() else None
if earlier and "target" not in earlier:  # a record of the full-only bake before ADR-0141
    earlier = {**earlier, "mode": "full", "target": earlier["target_generation"]}
if (earlier and earlier.get("mode", "full") == "full" and not earlier.get("refused_incomplete")
        and earlier["gold_iceberg_snapshot_id"] != gold
        and earlier.get("base_generation", base) == base and earlier["target"] > base):
    pin = earlier["gold_iceberg_snapshot_id"]
    run = runs / f"{pin}-{tag}g{earlier['target']}"
    baked = {json.loads(path.read_text()).get("document_schema_version", schema)
             for path in [*run.glob("shard-*.json"), *run.glob("summaries/shard-*.json")]}
    if baked <= {schema}:
        print(pin)
PY
)"
[[ -z "${pinned}" ]] || gold="${pinned}"

# A lane whose documents follow another's, baked by its own job (root ADR-0333: the unit price lane
# follows the building lane, root ADR-0190), bakes a Gold snapshot only once the followed lane serves
# one built from the same pinned inputs (scripts/ops/lane_follow_gate.py); otherwise it waits, and
# the asset reconciler starts it again when the followed lane moves. Inside `all` the lanes run in
# order and a lane is not baked in a run where the one it follows failed (above), so this is not asked.
if [[ "${OWN_RUN}" == true && "${UNIT}" != parcel && "${UNIT}" != building ]]; then
  follows="$(python3 -I -B "$(dirname "${BASH_SOURCE[0]}")/lane_follow_gate.py" "${UNIT}" --follows)" \
    || { log "refused: cannot read which lane the ${UNIT} lane's documents follow"; exit 65; }
  if [[ -n "${follows}" ]]; then
    followed_state="${STATE_ROOT}/follows-${follows}.json"
    rm -f "${followed_state}"
    # The followed lane's own store setting: the publisher reads it under that lane's prefix, and
    # unset it is the local driver, which refused for want of a root (2026-10-29 07:19Z and 07:40Z,
    # every by_pnu_unit_price_bake run failed with "the local driver requires an output root").
    env FOUNDATION_PLATFORM_BY_PNU_SERVING_STATE_PATH="${followed_state}" \
      "FOUNDATION_PLATFORM_$(tr 'a-z-' 'A-Z_' <<<"${follows}")_BY_PNU_SERVING_OUTPUT_STORAGE_DRIVER=${FOUNDATION_BY_PNU_BAKE_STORAGE_DRIVER:-r2}" \
      "${PUBLISHER_BIN}" "show-${follows}-by-pnu-serving-state"
    gate="$(python3 -I -B "$(dirname "${BASH_SOURCE[0]}")/lane_follow_gate.py" "${UNIT}" "${gold}" "${followed_state}")" \
      || { log "refused: cannot tell whether the ${follows} lane serves what Gold snapshot ${gold} was built from"; exit 65; }
    if [[ "${gate}" == waiting* ]]; then
      read -r _ followed_snapshot gate_reason <<<"${gate}"
      log "waiting: ${gate_reason}; baked once the ${follows} lane serves it"
      record_read "${reflected}" "${gold}" "${follows}" "${followed_snapshot}"
      LANE_WAITING="the ${follows} lane it follows does not serve what Gold snapshot ${gold} was built from"
      exit 0
    fi
  fi
fi

# Every choice and count of this run, for the run summary.
declare -A summary=([unit]="${UNIT}" [gold_iceberg_snapshot_id]="${gold}" [gold_now_iceberg_snapshot_id]="${gold_now}"
  [reflected_gold_iceberg_snapshot_id]="${reflected}" [base_generation]="${base}"
  [base_object_count]="${base_objects}" [patches_before]="${patch_count}"
  [cumulative_changes_before]="${cumulative}" [max_patches]="${max_patches}"
  [max_cumulative_change_ratio]="${max_ratio}" [forced_full_reason]="${FORCE_FULL_REASON}"
  [verified_rebase_reason]="${REBASE_REASON}" [lane_serves]="${LANE_SERVES}")
write_summary() {
  local args=()
  for key in "${!summary[@]}"; do args+=("${key}=${summary[${key}]}"); done
  python3 -I - "${run}/run-summary.json" "${summary_dir:-}" "${args[@]}" <<'PY'
import json, pathlib, sys
path, shards, pairs = sys.argv[1], sys.argv[2], sys.argv[3:]
summary = dict(pair.split("=", 1) for pair in pairs)
counts = {"base_generation", "base_object_count", "patches_before", "cumulative_changes_before",
          "max_patches", "upserts", "deletes", "new", "target_generation", "target_patch",
          "documents", "tombstones", "rebase_served_objects_read", "rebase_equal",
          "rebase_changed", "rebase_only_served", "rebase_only_gold"}
for key in counts & summary.keys():
    summary[key] = int(summary[key])
summary["max_cumulative_change_ratio"] = float(summary["max_cumulative_change_ratio"])
# What the run read, wrote and changed (root ADR-0314 §4), from the shard summaries written so far:
# a shard that skipped none of several data files read the whole lane Gold.
reads, written = [], 0
for shard in sorted(pathlib.Path(shards).glob("shard-*.json")) if shards else []:
    shard = json.loads(shard.read_text())
    reads += [shard["read"]] if shard.get("read") else []
    written += sum(pack.get("bytes", 0) for pack in shard.get("packs", []))
if reads:
    summary["data_files"] = max(read["data_files"] for read in reads)
    summary["bytes_read"] = sum(read["bytes_read"] for read in reads)
    summary["shards_reading_every_data_file"] = sum(
        1 for read in reads if read["skipped_data_files"] == 0 and read["data_files"] > 1)
    summary["bytes_written"] = written
if {"upserts", "deletes"} <= summary.keys():
    summary["keys_changed"] = summary["upserts"] + summary["deletes"]
summary["schema_version"] = "foundation-platform.by_pnu_serving_bake_run.v1"
with open(path, "w") as out:
    json.dump(summary, out, sort_keys=True, indent=2)
    out.write("\n")
PY
}

# 2. Patch, reflect or full?
mode="" reason=""
if [[ -n "${pinned}" ]]; then
  mode=full reason="finishing the full bake pinned to Gold snapshot ${pinned} (Gold is now at ${gold_now}); the next run patches it forward (root ADR-0310)"
elif [[ "${FORCE_FULL}" == true ]]; then
  mode=full reason="forced by the operator: ${FORCE_FULL_REASON}"
elif [[ "${schemas}" == breaks ]]; then
  mode=full reason="the export bakes ${schema_now} documents, the base holds ${served_schema}"
elif ((patch_count >= max_patches)); then
  mode=full reason="the base carries ${patch_count} patches, the contract's max_patches is ${max_patches}"
elif ((patch_count > 0)) && [[ "${served_prefix_length}" != "${prefix_length}" ]]; then
  mode=full reason="the patches are listed by ${served_prefix_length}-digit prefixes, the contract's pnu_prefix_length is ${prefix_length}"
fi
if [[ -z "${mode}" && "${schemas}" == expands ]]; then
  # Expand/contract (root ADR-0320): the served documents stay as they are, and the change set
  # alone decides which ones the new version re-renders; the next whole generation contracts.
  summary[document_schema_expansion]="${served_schema} -> ${schema_now}"
  log "the export bakes ${schema_now}, which only adds optional fields to the served ${served_schema}: the change set decides, not the version (root ADR-0320)"
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
  FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${STATE_ROOT}" \
  FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
  lakehouse_spark_run foundation-platform-compute \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
    spark-small spark-submit --master 'local[4,8]' --driver-memory "${DELTA_DRIVER_MEMORY}" \
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
  [[ -z "${BY_PNU_BAKE_PUBLISHED:-}" ]] || echo "${UNIT}" >>"${BY_PNU_BAKE_PUBLISHED}"
  record_read "${gold}"
  log "published: the served ${LANE_SERVES} now reflect Gold snapshot ${gold}; nothing was baked"
  exit 0
fi

# 3. Which generation, or which patch?
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
# Root ADR-0310 §4: Gold no longer holds the snapshot this bake reads (expiry). Nothing baked for it
# can be finished or mixed with another snapshot: the target is left unserved, never resumed, and
# the next run starts over from the current snapshot.
abandon_an_expired_pin() {
  grep -q 'no longer holds snapshot' "$1" || return 0
  tail -n 5 "$1" >&2
  rm -f "${progress}"
  log "FAILED: Gold no longer holds snapshot ${gold}, which ${where} is pinned to (expired); nothing was published, the next run starts over from the current snapshot"
  log "abandoned: ${where} holds what this bake wrote for Gold snapshot ${gold}; it is never served or resumed, and removing it is a manual step (runbook 8절)"
  exit 1
}
# A publish: its output is kept beside the shard logs, so an expired pin is found there too.
publish_bake() {
  "$@" 2>&1 | tee "${run}/publish.log" && return 0
  abandon_an_expired_pin "${run}/publish.log"
  log "FAILED: the publish of ${where} was refused; the next run resumes it"
  exit 1
}
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
    abandon_an_expired_pin "${attempt_log}"
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
if ! counts="$(by_pnu_bake_complete "by-pnu-serving-bake: refused to publish" "${summary_dir}" "${gold}" "${mode}" \
  "${LANE_SERVES}" "${generation}" "${target}" "${upserts:-0}" "${deletes:-0}" "${done_shards[@]}")"; then
  # A rerun of this snapshot still resumes it (a summary removed by hand re-bakes that shard), but
  # once Gold moves it is not finished at its pin (root ADR-0310): resuming would change nothing.
  python3 -I - "${progress}" <<'PY'
import json, pathlib, sys
progress = pathlib.Path(sys.argv[1])
if progress.exists():
    progress.write_text(json.dumps({**json.loads(progress.read_text()), "refused_incomplete": True}) + "\n")
PY
  exit 1
fi
read -r expected tombstones <<<"${counts}"
if [[ "${LANE_SERVES}" == packs ]]; then
  # The publisher holds a base to the Gold row count the catalog records, and a patch to its
  # change set, before it writes the manifest's section_packs block.
  publish_env=("${ENV_PREFIX}_CONFIRM_PACK_PUBLISH=true" "${ENV_PREFIX}_PACK_SUMMARY_DIR=${summary_dir}"
    "${ENV_PREFIX}_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID=${gold}")
  [[ "${mode}" == full ]] || publish_env+=("${ENV_PREFIX}_CHANGE_SET_SUMMARY_PATH=${run}/change-set.json"
    "${ENV_PREFIX}_UPSERT_LIST_PATH=${run}/upserts.txt" "${ENV_PREFIX}_DELETE_LIST_PATH=${run}/deletes.txt")
  log "complete: ${where} holds ${expected} documents and ${tombstones} tombstones; publishing it"
  publish_bake env "${publish_env[@]}" "${PUBLISHER_BIN}" "publish-${UNIT}-by-pnu-section-packs"
elif [[ "${mode}" == full ]]; then
  log "complete: ${#done_shards[@]} shards exported all ${expected} rows; publishing generation ${target}"
  publish_bake env "${ENV_PREFIX}_CONFIRM_PUBLISH=true" "${ENV_PREFIX}_PUBLISH_FROM_LISTING=true" \
    "${ENV_PREFIX}_TARGET_GENERATION=${target}" \
    "${ENV_PREFIX}_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID=${gold}" \
    "${ENV_PREFIX}_EXPECTED_OBJECT_COUNT=${expected}" \
    "${PUBLISHER_BIN}" "publish-${UNIT}-by-pnu-serving-manifest"
else
  log "complete: patch ${target} holds ${expected} documents and ${tombstones} tombstones; publishing it"
  publish_bake env "${ENV_PREFIX}_CONFIRM_PUBLISH=true" "${ENV_PREFIX}_PUBLISH_PATCH=true" \
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
[[ -z "${BY_PNU_BAKE_PUBLISHED:-}" ]] || echo "${UNIT}" >>"${BY_PNU_BAKE_PUBLISHED}"
record_read "${gold}"
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
# Root ADR-0325 §4: a document no cut into pages fits is withheld (its page says so) and every other
# is published, so one document never stops the lane again (2026-10-28: one oversized building
# document refused the whole lane six times; it had served nothing new for 585 hours). The run still
# fails after publishing, so the unit's failure notice names it. Under `all` the lane exits 0 and the
# run fails at its end, so a lane that follows this one is still baked.
withheld="$(python3 -I - "${summary_dir}" <<'PY'
import json, pathlib, sys
keys = sorted(document["key"] for path in pathlib.Path(sys.argv[1]).glob("shard-*.json")
              for document in json.loads(path.read_text()).get("withheld", []))
print(f"{len(keys)} ({' '.join(keys[:20])})" if keys else "")
PY
)"
if [[ -n "${withheld}" ]]; then
  log "WITHHELD (root ADR-0325 §4): ${withheld} documents answer only that they are withheld; the rest is published"
  [[ -z "${BY_PNU_BAKE_WITHHELD:-}" ]] || { echo "${UNIT}" >>"${BY_PNU_BAKE_WITHHELD}"; exit 0; }
  exit 1
fi
