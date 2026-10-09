#!/usr/bin/env bash
# Sourced by the by-PNU bakes: the scheduled bake (by-pnu-serving-bake.sh) and the operator's pack
# generation bake (by-pnu-pack-bake.sh, root ADR-0166). What both mean by a shard, how a shard over
# the export's row cap is split, which shards a complete bake holds, and how the runtime database
# connection is found, defined once.
#
# A shard is a PNU prefix of 1 to 10 digits (or `all`, the scheduled patch's whole change set). A
# bake starts from the nine one-digit prefixes. The export refuses a shard that keeps more than its
# row cap with an error naming the prefix variable ("shard the run with ..._PNU_PREFIX"); that shard
# is then replaced by its ten one-digit-longer children, which partition it.

# The first shard plan: every PNU starts with one of these digits.
BY_PNU_FIRST_SHARDS=(1 2 3 4 5 6 7 8 9)

# The operator's pack generation bake (by-pnu-pack-bake.sh): shards exported side by side, and the
# attempts each gets before the bake fails. Four workers and three attempts baked the parcel lane's
# first generation on 2026-10-08 (R2 body reads failed transiently, a retry passed). The dispatcher
# sizes the unit's memory from the worker count (by-pnu-pack-operator.sh).
BY_PNU_PACK_BAKE_WORKERS=4
BY_PNU_PACK_BAKE_ATTEMPTS=3

# Is $1 a shard prefix?
by_pnu_shard_valid() {
  [[ "$1" =~ ^[0-9]{1,10}$ ]]
}

# Did the export, whose output is the file $1, refuse its shard for the row cap?
by_pnu_shard_over_row_cap() {
  grep -q 'shard the run with' "$1"
}

# The shards that replace shard $1, one per line. Fails for a prefix at full PNU length: nothing
# longer can split it.
by_pnu_shard_children() {
  if [[ "$1" == all ]]; then
    printf '%s\n' "${BY_PNU_FIRST_SHARDS[@]}"
    return 0
  fi
  ((${#1} < 10)) || return 1
  local digit
  for digit in 0 1 2 3 4 5 6 7 8 9; do printf '%s\n' "$1${digit}"; done
}

# The runtime database connection, from compose's API connection on its loopback port, never a
# second stored URL (the building export reads the approved building links). Needs RELEASE_ROOT.
by_pnu_runtime_database_url() {
  docker compose --project-directory "${RELEASE_ROOT}" \
    --env-file /dev/null -f "${RELEASE_ROOT}/docker-compose.yml" \
    config --format json --no-env-resolution 2>/dev/null \
    | python3 "${RELEASE_ROOT}/scripts/ops/runtime-database-url.py"
}

# Whether the shard summaries in a directory make one complete bake; prints "<documents>
# <tombstones>" or exits non-zero with the reason.
#
#   by_pnu_bake_complete <refusal prefix> <summary dir> <gold snapshot> <full|patch> <objects|packs> \
#     <generation> <target> <upserts> <deletes> <shard>...
#
# The shards may not overlap, every one is of the one Gold snapshot, the generation (and patch) and
# its own prefix, they all scanned the same Gold row count, and a full bake exported exactly that
# many rows (a patch: exactly its change set's upserts and tombstones).
by_pnu_bake_complete() {
  python3 -I - "$@" <<'PY'
import json, pathlib, sys
label, run, gold, mode, serves = sys.argv[1], pathlib.Path(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5]
generation, target, upserts, deletes = map(int, sys.argv[6:10])
shards = sys.argv[10:]
def refuse(reason):
    sys.exit(f"{label}: {reason}")
if not shards:
    refuse("no shard baked")
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
    if serves == "packs":
        # A pack patch's sections each carry their served generation; the publisher holds them
        # to the manifest. A base is one generation of every section.
        said = (summary["generation"] if mode == "full" and not summary.get("section_generations")
                else None, summary["patch"], summary["pnu_prefix"])
        rows = summary.get("gold_record_count")
    else:
        said = (summary["target_generation"], summary.get("target_patch"), summary.get("pnu_prefix"))
        rows = summary["scanned_row_count"]
    want_generation = generation if serves == "objects" or mode == "full" else None
    if said != (want_generation, want_patch, None if prefix == "all" else prefix):
        refuse(f"shard {prefix}'s summary is for generation {said[0]} patch {said[1]} prefix {said[2]}")
    gold_rows.add(rows)
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
}
