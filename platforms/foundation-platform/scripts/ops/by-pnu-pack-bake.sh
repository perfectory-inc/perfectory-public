#!/usr/bin/env bash
# One full section pack generation of a lane, baked for the cut-over gates (root ADR-0147 §8,
# ADR-0161, ADR-0166). The operator starts it through the dispatcher, never by hand:
#
#   by-pnu-pack-operator.sh <building|parcel> bake <generation>
#     → a transient unit, as the service user, running
#   by-pnu-pack-bake.sh <building|parcel> <generation>
#
# It writes the generation's packs to the lane's R2 output, create-only, and one export summary per
# shard into <state root>/<lane>-pack-g<generation>/summaries/shard-<prefix>.json, where the gates
# (가)/(나) and the publish read them. It never publishes: those stay separate dispatcher actions.
#
# - One run of the lane at a time: it holds the lane lock the scheduled bake holds
#   (<state root>/<lane>/lane.lock), without waiting.
# - Shards are PNU prefixes, from 1..9. A shard the export refuses for its row cap is replaced by its
#   ten children (by-pnu-bake-shards.sh, the scheduled bake's own rule), and the split is recorded
#   (shards/shard-<prefix>.split) so a rerun goes straight to the children.
# - Resumable: a shard whose summary exists is done and is not run again.
# - One Gold snapshot: the first shard runs alone and unpinned; its summary's snapshot pins every
#   later shard (…_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID), and a rerun pins from the summaries already
#   there. Summaries of two snapshots are refused, and so is a table that moved (the export stops).
# - BY_PNU_PACK_BAKE_WORKERS shards side by side, each with BY_PNU_PACK_BAKE_ATTEMPTS attempts
#   (by-pnu-bake-shards.sh). A retry resumes: the export rewrites the same bytes create-only.
# - Each shard's result goes to the log (the dispatcher's logs/bake.log); each attempt's output to
#   shards/shard-<prefix>.attempt-<n>.log. At the end the summaries are checked with the scheduled
#   bake's completeness rule: one snapshot, no overlap, every Gold row exported.
set -euo pipefail

LANE="${1:-}" GENERATION="${2:-}"
log() { printf '%s by-pnu-pack-bake %s: %s\n' "$(date -u +%FT%TZ)" "${LANE}" "$*"; }
if [[ "$#" != 2 || ! ("${LANE}" == building || "${LANE}" == parcel) || ! "${GENERATION}" =~ ^[1-9][0-9]{0,3}$ ]]; then
  echo "usage: by-pnu-pack-bake.sh <building|parcel> <generation>" >&2
  exit 64
fi
# Workers are reaped one by one with `wait -n -p` (bash 5.1).
((BASH_VERSINFO[0] * 100 + BASH_VERSINFO[1] >= 501)) || { log "refused: needs bash 5.1 or later"; exit 69; }
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/by-pnu-bake-shards.sh"
P="FOUNDATION_PLATFORM_${LANE^^}_BY_PNU_SERVING"
STATE_ROOT="${FOUNDATION_BY_PNU_BAKE_STATE_ROOT:-/data/foundation-platform/by-pnu-bake}"
WORK="${STATE_ROOT}/${LANE}-pack-g${GENERATION}"
SUMMARIES="${WORK}/summaries" SHARDS="${WORK}/shards"
RETRY_SECONDS="${FOUNDATION_BY_PNU_BAKE_RETRY_SECONDS:-45}"

# Only what this bake sets reaches the export: no patch, no section subset, no local output.
for name in $(compgen -e | grep "^${P}_" || true); do unset "${name}"; done
export "${P}_OUTPUT_STORAGE_DRIVER=r2" "${P}_CONFIRM_PACK_EXPORT=true" "${P}_PACK_GENERATION=${GENERATION}"

if [[ "${LANE}" == building && -z "${DATABASE_URL:-}" ]]; then
  if ! DATABASE_URL="$(by_pnu_runtime_database_url)"; then
    log "refused: cannot resolve the runtime database connection the building export reads"
    exit 78
  fi
  export DATABASE_URL
fi

mkdir -p "${STATE_ROOT}/${LANE}" "${SUMMARIES}" "${SHARDS}"
exec 9>>"${STATE_ROOT}/${LANE}/lane.lock"
if ! flock -n 9; then
  log "refused: another run of the ${LANE} lane holds ${STATE_ROOT}/${LANE}/lane.lock; start again after it finishes"
  exit 75
fi

# The snapshot the summaries already there were baked from: none, or exactly one.
pin="$(python3 -I - "${SUMMARIES}" "${GENERATION}" <<'PY'
import json, pathlib, sys
found = {}
for path in sorted(pathlib.Path(sys.argv[1]).glob("shard-*.json")):
    summary = json.loads(path.read_text())
    if summary.get("generation") != int(sys.argv[2]) or summary.get("patch") is not None:
        sys.exit(f"{path.name} is not of base generation {sys.argv[2]}")
    found.setdefault(summary["gold_iceberg_snapshot_id"], []).append(path.name)
if len(found) > 1:
    sys.exit("the summaries hold more than one Gold snapshot: "
             + "; ".join(f"{snapshot} ({', '.join(names)})" for snapshot, names in sorted(found.items())))
print(next(iter(found), ""))
PY
)" || { log "refused: ${SUMMARIES} cannot be resumed; a generation holds one Gold snapshot (start a new generation)"; exit 65; }
if [[ -n "${pin}" ]]; then
  log "resuming generation ${GENERATION} of Gold snapshot ${pin}"
else
  log "baking generation ${GENERATION}; the first shard's Gold snapshot pins the rest"
fi

# One shard, all its attempts. Exit 0 baked, 10 over the row cap, 1 failed.
bake_shard() {
  local prefix="$1" attempt attempt_log summary="${SUMMARIES}/shard-$1.json" pinned=()
  [[ -z "${pin}" ]] || pinned=("${P}_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID=${pin}")
  for attempt in $(seq 1 "${BY_PNU_PACK_BAKE_ATTEMPTS}"); do
    attempt_log="${SHARDS}/shard-${prefix}.attempt-${attempt}.log"
    if env ${pinned[@]+"${pinned[@]}"} "${P}_PNU_PREFIX=${prefix}" "${P}_PACK_SUMMARY_PATH=${summary}.partial" \
        "${PUBLISHER_BIN}" "export-${LANE}-by-pnu-section-packs" >"${attempt_log}" 2>&1; then
      mv "${summary}.partial" "${summary}"
      return 0
    fi
    rm -f "${summary}.partial"
    by_pnu_shard_over_row_cap "${attempt_log}" && return 10
    # Refusals a retry cannot change.
    if grep -q -e 'moved during the bake' -e 'holds packs of Gold snapshot' "${attempt_log}"; then
      tail -n 3 "${attempt_log}"
      return 1
    fi
    tail -n 3 "${attempt_log}"
    if ((attempt < BY_PNU_PACK_BAKE_ATTEMPTS)); then
      log "shard ${prefix} attempt ${attempt}/${BY_PNU_PACK_BAKE_ATTEMPTS} failed; again in ${RETRY_SECONDS}s"
      sleep "${RETRY_SECONDS}"
    fi
  done
  return 1
}

queue=("${BY_PNU_FIRST_SHARDS[@]}"); done_shards=(); failed=()
declare -A running=()
while ((${#queue[@]} || ${#running[@]})); do
  # Unpinned, one shard at a time: the first summary decides the snapshot. After a failure, start
  # nothing new and let the running shards finish.
  limit="${BY_PNU_PACK_BAKE_WORKERS}"
  [[ -n "${pin}" ]] || limit=1
  while ((${#queue[@]} && ${#running[@]} < limit && ${#failed[@]} == 0)); do
    prefix="${queue[0]}"; queue=("${queue[@]:1}")
    by_pnu_shard_valid "${prefix}" || { log "refused: shard '${prefix}'"; exit 65; }
    if [[ -e "${SHARDS}/shard-${prefix}.split" ]]; then
      mapfile -t -O "${#queue[@]}" queue < <(by_pnu_shard_children "${prefix}")
    elif [[ -s "${SUMMARIES}/shard-${prefix}.json" ]]; then
      done_shards+=("${prefix}")
    else
      bake_shard "${prefix}" &
      running[$!]="${prefix}"
    fi
  done
  ((${#running[@]})) || { ((${#failed[@]} == 0)) && continue; break; }
  status=0
  wait -n -p finished "${!running[@]}" || status=$?
  prefix="${running[${finished}]}"
  unset "running[${finished}]"
  case "${status}" in
    0)
      done_shards+=("${prefix}")
      read -r snapshot rows < <(python3 -I -c 'import json,sys; s=json.load(open(sys.argv[1])); print(s["gold_iceberg_snapshot_id"], s["exported_row_count"])' \
        "${SUMMARIES}/shard-${prefix}.json")
      if [[ -z "${pin}" ]]; then
        pin="${snapshot}"
        log "Gold snapshot ${pin} pins every later shard"
      fi
      log "shard ${prefix} baked: ${rows} documents of Gold snapshot ${snapshot}"
      ;;
    10)
      if ! children="$(by_pnu_shard_children "${prefix}")"; then
        log "FAILED: shard ${prefix} is at full PNU length and still over the export's row cap"
        failed+=("${prefix}")
        continue
      fi
      : >"${SHARDS}/shard-${prefix}.split"
      log "shard ${prefix} holds more rows than one run may keep; split into ${prefix}0..${prefix}9"
      mapfile -t -O "${#queue[@]}" queue <<<"${children}"
      ;;
    *)
      log "FAILED: shard ${prefix} did not bake in ${BY_PNU_PACK_BAKE_ATTEMPTS} attempts (${SHARDS}/shard-${prefix}.attempt-*.log)"
      failed+=("${prefix}")
      ;;
  esac
done
if ((${#failed[@]})); then
  log "FAILED: shards ${failed[*]} did not bake; ${#done_shards[@]} shards did. Nothing was published; the same action resumes"
  exit 1
fi
if ! counts="$(by_pnu_bake_complete "by-pnu-pack-bake: refused" "${SUMMARIES}" "${pin}" full packs \
    "${GENERATION}" "${GENERATION}" 0 0 "${done_shards[@]}" 2>&1)"; then
  log "FAILED: ${counts}"
  exit 1
fi
read -r documents _ <<<"${counts}"
log "complete: generation ${GENERATION} holds ${documents} documents of Gold snapshot ${pin} in ${#done_shards[@]} shards; nothing was published (next: equality ${GENERATION})"
