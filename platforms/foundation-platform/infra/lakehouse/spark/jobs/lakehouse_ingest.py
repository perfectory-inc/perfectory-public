#!/usr/bin/env python3
"""Make a re-run of an Iceberg append land the same rows as the first run (ADR-0062).

A job that appends and then verifies has a gap between the two: the rows are committed but
nothing durable says so yet. A run that dies in that gap leaves a loader believing the batch
never landed, and the retry appends it again. On 2026-08-27 that gap ran three times and put
1,865,891 parcels into `silver.parcel_boundaries` three times over.

The fix is not a better retry — it is to stop keeping the record of an append anywhere but in
the append. Iceberg writes a snapshot summary in the same commit as the data files, so a
record placed there cannot outlive the rows or be outlived by them. Apache Iceberg's own
Flink sink resolves the identical problem the identical way, with
`flink.max-committed-checkpoint-id` in the summary; Delta Lake's `txnAppId`/`txnVersion` is
the same idea under another name.

No PySpark import here on purpose. The lane that runs `infra/lakehouse/spark/tests` has no
PySpark install, and a module-level import would make every check that touches this file skip
itself — which reports the same green as passing.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from platform_contracts import load_identity_from_value, load_unit

# Written in the same Iceberg commit as the data files. `TOKEN` names the batch for a human
# reading `<table>.snapshots`; `OBJECTS` is what the skip decision actually reads.
INGEST_BATCH_TOKEN_KEY = "foundation.ingest-batch-token"
INGEST_BATCH_OBJECTS_KEY = "foundation.ingest-batch-objects"
# The durable copy of that record (root ADR-0332): a table property per ingest commit,
# `<prefix><snapshot id>` = the commit's summary value. The maintenance job writes it for every
# record of main older than the expiry window before it expires anything
# (`ingest_record_properties`), so a record is always in a summary, a property, or both.
INGEST_OBJECTS_PROPERTY_PREFIX = f"{INGEST_BATCH_OBJECTS_KEY}."
# The edition a commit of a versioned table loaded (root ADR-0317): an edition with no changed key
# appends no row, so the commit, not the rows, says that it was loaded.
EDITION_KEY = "foundation.edition"
# The durable copy of that record (ADR-0328): a table property per edition, `<prefix><edition>` =
# the snapshot that published it, which `publish_branch` (iceberg_rest_catalog.rs) sets in the same
# catalog commit that moves main. Snapshot summaries expire after 30 days (root ADR-0237); a table
# property does not.
EDITION_PROPERTY_PREFIX = f"{EDITION_KEY}."

# Iceberg's Spark writer strips this prefix and puts the rest into the snapshot summary.
SNAPSHOT_PROPERTY_PREFIX = "snapshot-property."

# `,` separates object names inside one summary value, so a name containing one would split
# into two names and the skip decision would ask about objects that do not exist.
OBJECT_NAME_SEPARATOR = ","

# The column the read-back predicate filters on. It is not the column the registry reads —
# three tables identify a load by `source_snapshot_id` — so this name belongs to the read-back
# and nothing else. `load_unit` answers the registry's question.
SOURCE_RECORD_COLUMN = "source_record_id"

# How many source objects one run may append before its read-back predicate grows unwieldy.
# The national parcel extract is 255 objects and does not fit one Spark run on a small host,
# so it arrives in batches; this bounds a batch rather than the dataset.
MAX_BATCH_SOURCE_RECORDS = 64


def ingest_batch_token(record_ids: Sequence[str]) -> str:
    """Derive a batch's identity from what it contains, not from when it ran.

    A counter would name the third run "3" whether or not the second one landed, so a resumed
    loader could hand the same number to a different set of objects. The digest of the object
    names cannot drift from the batch: re-running the same objects always yields the same
    token. Sorted here rather than by the caller, so the guarantee holds for every caller.
    """
    return hashlib.sha256("\n".join(sorted(record_ids)).encode("utf-8")).hexdigest()


def snapshot_property_options(record_ids: Sequence[str], token: str) -> dict[str, str]:
    """Return the writer options that carry the record of this append into its own commit.

    Assembled here rather than at each call site so the prefix is spelled once. A call site
    that misspelled it would write a plain, ignored option and lose the whole guarantee while
    still appearing to ask for it.
    """
    for name in record_ids:
        if OBJECT_NAME_SEPARATOR in name:
            raise ValueError(
                f"Source object name cannot contain {OBJECT_NAME_SEPARATOR!r} because the "
                f"snapshot summary separates names with it: {name!r}"
            )

    return {
        f"{SNAPSHOT_PROPERTY_PREFIX}{INGEST_BATCH_TOKEN_KEY}": token,
        f"{SNAPSHOT_PROPERTY_PREFIX}{INGEST_BATCH_OBJECTS_KEY}": OBJECT_NAME_SEPARATOR.join(
            sorted(record_ids)
        ),
    }


MAIN = "main"
# A load branch is named by the Silver refresh lane that publishes it (`load_branch`,
# remote_lakehouse_job/silver_refresh/execute.rs); here it only has to be an identifier, since a
# write names it as `branch_<name>`.
BRANCH_NAME = re.compile(r"[a-z][a-z0-9_]{0,62}")


def checked_ref(ref: str) -> str:
    if ref != MAIN and not BRANCH_NAME.fullmatch(ref):
        raise ValueError(f"not a load branch name: {ref!r}")
    return ref


def ref_lineage(spark: Any, qualified_table: str, ref: str = MAIN) -> set[int]:
    """The snapshots of `ref`'s lineage: its head and each parent back as far as the metadata still
    holds them. Empty when the reference does not exist (an empty table, a branch not yet made)."""
    checked_ref(ref)
    heads = spark.sql(f"SELECT snapshot_id FROM {qualified_table}.refs WHERE name = '{ref}'").collect()
    if not heads:
        return set()
    parents = {int(row.snapshot_id): (None if row.parent_id is None else int(row.parent_id))
               for row in spark.sql(f"SELECT snapshot_id, parent_id FROM {qualified_table}.snapshots").collect()}
    lineage, current = set(), int(heads[0].snapshot_id)
    while current is not None and current in parents and current not in lineage:
        lineage.add(current)
        current = parents[current]
    return lineage


def read_ingested_objects(spark: Any, qualified_table: str, unquoted_table: str, ref: str = MAIN) -> dict[str, int]:
    """Map every source object `ref` records as appended to the snapshot that appended it.

    Reads the snapshot summaries of `ref`'s lineage, so the answer comes from the same commit log
    that holds the rows. `main` is what readers see; a load staged on its branch is recorded only
    on that branch until it is published (root ADR-0183), so a retry asks the branch and "is this
    release loaded" asks main. The Rust reader (`load_ingested_batch_objects`) asks main the same
    way. Iceberg drops summaries when snapshots expire, so the table's durable copy
    (`INGEST_OBJECTS_PROPERTY_PREFIX`, root ADR-0332) is read as well: a record of main is in the
    lineage of any ref made from it, and its snapshot may be gone. Delta Lake's checkpoints carry
    its `txn` idempotency records forward the same way before the log they came from is cleaned.
    """
    if not spark.catalog.tableExists(unquoted_table):
        return {}

    lineage = ref_lineage(spark, qualified_table, ref)
    ingested: dict[str, int] = {}
    for row in spark.sql(f"SELECT snapshot_id, summary FROM {qualified_table}.snapshots").collect():
        if int(row.snapshot_id) not in lineage:
            continue
        recorded = (row.summary or {}).get(INGEST_BATCH_OBJECTS_KEY)
        if not recorded:
            continue
        for name in recorded.split(OBJECT_NAME_SEPARATOR):
            ingested.setdefault(name, int(row.snapshot_id))
    for key, recorded in spark.sql(f"SHOW TBLPROPERTIES {qualified_table}").collect():
        if key.startswith(INGEST_OBJECTS_PROPERTY_PREFIX) and recorded:
            for name in recorded.split(OBJECT_NAME_SEPARATOR):
                ingested.setdefault(name, int(key[len(INGEST_OBJECTS_PROPERTY_PREFIX):]))
    return ingested


def ingest_record_properties(
    snapshots: dict[int, dict[str, Any]], lineage: Sequence[int], before_ms: int, properties: dict[str, str]
) -> dict[str, str]:
    """The table properties that make main's older ingest records durable (root ADR-0332).

    `snapshots` maps an id to its `committed_ms` and `summary`; `lineage` is main's. Every record
    committed before `before_ms` (the expiry window) whose property the table does not hold yet.
    The maintenance job sets these in a commit before it expires anything, so no expiry can take a
    record's summary while the record has no other copy.
    """
    carried = {}
    for snapshot_id in lineage:
        snapshot = snapshots[snapshot_id]
        recorded = snapshot["summary"].get(INGEST_BATCH_OBJECTS_KEY)
        key = f"{INGEST_OBJECTS_PROPERTY_PREFIX}{snapshot_id}"
        if recorded and snapshot["committed_ms"] < before_ms and key not in properties:
            carried[key] = recorded
    return carried


def edition_property_option(edition: str) -> dict[str, str]:
    """The writer option that records in its commit which edition the commit loaded."""
    if not edition or OBJECT_NAME_SEPARATOR in edition:
        raise ValueError(f"an edition is a non-empty value without {OBJECT_NAME_SEPARATOR!r}: {edition!r}")
    return {f"{SNAPSHOT_PROPERTY_PREFIX}{EDITION_KEY}": edition}


def read_editions(spark: Any, qualified_table: str, unquoted_table: str, ref: str = MAIN) -> set[str]:
    """Every edition `ref`'s lineage records as loaded (`edition_property_option`), and every edition
    the table's durable record holds (`EDITION_PROPERTY_PREFIX`: published to main, so in the lineage
    of any ref made from it); empty for a table that does not exist."""
    if not spark.catalog.tableExists(unquoted_table):
        return set()
    lineage = ref_lineage(spark, qualified_table, ref)
    durable = {row[0][len(EDITION_PROPERTY_PREFIX):]
               for row in spark.sql(f"SHOW TBLPROPERTIES {qualified_table}").collect()
               if row[0].startswith(EDITION_PROPERTY_PREFIX)}
    return durable | {row.summary[EDITION_KEY]
                      for row in spark.sql(f"SELECT snapshot_id, summary FROM {qualified_table}.snapshots").collect()
                      if int(row.snapshot_id) in lineage and (row.summary or {}).get(EDITION_KEY)}


def table_holds_rows(spark: Any, qualified_table: str, unquoted_table: str, ref: str = MAIN) -> bool:
    """Whether the snapshot `ref` names holds any rows.

    Read from the snapshot summary rather than counted, because the question is only whether the
    table is empty and a count of 113,813,264 rows would open every file to answer it.
    """
    if not spark.catalog.tableExists(unquoted_table):
        return False
    checked_ref(ref)
    rows = spark.sql(
        f"SELECT summary FROM {qualified_table}.snapshots "
        f"WHERE snapshot_id = (SELECT snapshot_id FROM {qualified_table}.refs WHERE name = '{ref}')"
    ).collect()
    for row in rows:
        total = (row.summary or {}).get("total-records")
        if total is not None and int(total) > 0:
            return True
    return False


def read_recorded_append(spark: Any, table: str, snapshot_id: int) -> Any:
    """Read only rows added by one authenticated append, never its earlier derivations."""
    snapshots = spark.sql(
        f"SELECT parent_id, operation FROM {table}.snapshots "
        f"WHERE snapshot_id = {int(snapshot_id)}"
    ).collect()
    if len(snapshots) != 1 or snapshots[0].operation != "append":
        raise ValueError("Cannot authenticate retry: recorded append snapshot is unavailable")
    reader = spark.read.format("iceberg")
    parent_id = snapshots[0].parent_id
    if parent_id is None:
        reader = reader.option("snapshot-id", str(snapshot_id))
    else:
        reader = reader.option("start-snapshot-id", str(parent_id)).option(
            "end-snapshot-id", str(snapshot_id)
        )
    return reader.load(unquoted_table_name(table))


def decide_whether_to_append(
    ingested: dict[str, int],
    record_ids: Sequence[str],
) -> list[str]:
    """Return the objects still to append, or raise when the batch straddles the boundary.

    Keyed on the source object rather than on the batch, because the batch is a property of
    the loader's mood — how many files it grouped that day — and not of the data. A batch
    token alone would let regrouping the same objects produce a token the table has never
    seen, and append them a second time. Proving this on the live table is what found it: a
    two-object run left rows an eight-object batch would not have recognised.

    A batch that is part in and part out is not a resume. Either the loader regrouped its
    objects mid-load or two loaders are running, and appending the remainder would write rows
    whose run the read-back gate cannot bound. Say which objects straddle, and stop.
    """
    already = [name for name in record_ids if name in ingested]
    missing = [name for name in record_ids if name not in ingested]
    if already and missing:
        raise ValueError(
            "Refusing to append a batch that is partly already in the table. "
            f"in={already[:4]} out={missing[:4]} "
            f"in_count={len(already)} out_count={len(missing)}. "
            "Re-run with the object grouping the earlier run used, or load the missing "
            "objects on their own."
        )
    return missing


def batch_source_record_ids(
    frame: Any,
    column: str = SOURCE_RECORD_COLUMN,
    limit: int = MAX_BATCH_SOURCE_RECORDS,
) -> list[str]:
    """Name the raw lineage values this run carries, in a fixed order.

    These are what the read-back predicate filters the table by, so they stay exactly as the
    rows spell them. The registry asks a different question — what identity a load is counted
    in — and `batch_load_identities` answers that one; the two were the same function until
    2026-09-01, which is how three tables came to be compared on a column they do not use.

    Sorted so that the same objects yield the same list however the input files were globbed
    or ordered.

    One more than the limit is fetched so that exceeding it is an error rather than a silent
    truncation. A truncated list would read back fewer rows than the commit appended and then
    report the shortfall as a write problem.
    """
    rows = frame.select(column).distinct().limit(limit + 1).collect()
    record_ids = sorted(getattr(row, column) for row in rows)
    if not record_ids:
        raise ValueError(f"Cannot identify this batch because {column} is empty")
    if len(record_ids) > limit:
        raise ValueError(
            f"An ingest batch may carry at most {limit} source objects; "
            "split the input into smaller batches"
        )
    return record_ids


def batch_load_identities(
    frame: Any,
    contract_table: str,
    limit: int = MAX_BATCH_SOURCE_RECORDS,
) -> list[str]:
    """Name what this batch would add to the registry, in the unit the contract declares.

    Two things the caller must not decide for itself, because a caller deciding them is the
    defect this exists to remove:

    - **Which column.** `silver.building_register_units` and `_unit_areas` identify a load by
      `source_snapshot_id`; reading `source_record_id` there compares a column those tables
      leave null, so nothing ever matches and every re-run appends. Between them they hold
      133,583,046 rows.
    - **Whether the value is the identity.** `silver.industrial_complexes` writes one value per
      row — `foundation-platform:bronze:{key}#{code}` — so its 1,442 rows are one object, not
      1,442 of them. Compared whole they exceed the per-batch limit and the load is refused
      rather than protected.

    The scan is not truncated when a derivation applies. Cutting the raw values at the limit
    and reducing afterwards could see only the rows of one object out of several, and would
    then record fewer objects than the commit appended (root ADR-0069).
    """
    declared = load_unit(contract_table)
    if declared["unit"] == "derived":
        raise ValueError(
            f"{contract_table} is derived and its producer replaces rather than appends, so "
            "there is no load identity to record"
        )
    column = declared["column"]
    reduces = bool(declared.get("object_prefix") or declared.get("object_suffix_separator"))

    values = frame.select(column).distinct()
    rows = values.collect() if reduces else values.limit(limit + 1).collect()
    raw = [getattr(row, column) for row in rows]
    if not raw:
        raise ValueError(
            f"Cannot identify this batch because {contract_table} carries no {column} value"
        )
    identities = sorted({load_identity_from_value(contract_table, value) for value in raw})
    if len(identities) > limit:
        raise ValueError(
            f"An ingest batch may carry at most {limit} {declared['unit']} identities and this "
            f"one carries {len(identities)}; split the input into smaller batches"
        )
    return identities


# Separates a load identity from the derivation it was appended under. Neither it nor the object
# separator may appear in a derivation label, so `object@label` always splits back the same way.
DERIVATION_SEPARATOR = "@"


def identities_under_derivation(identities: Sequence[str], derivation: str | None) -> list[str]:
    """Name a batch by its objects and, when given, the derivation it is appended under.

    The registry answers "has this object been appended?", and for a given way of deriving rows
    that answer must stay yes forever (ADR-0062). But the way rows are derived changes: a contract
    gains columns, and the same source object then yields rows the table has never held. Without a
    second key there is no honest way to append them: the object reads as loaded, and the only
    alternatives are rewriting rows in place or renaming the object, both forbidden.

    `derivation` is that second key, given by the operator on the run that re-derives, so the
    same object is appended once per derivation and a repeated run of that derivation is still
    skipped. Absent, identities are exactly what they were, so no existing load changes meaning
    (root ADR-0120).
    """
    if derivation is None:
        return list(identities)
    label = derivation.strip()
    if not label or DERIVATION_SEPARATOR in label or OBJECT_NAME_SEPARATOR in label:
        raise ValueError(
            f"A derivation label must be non-empty and hold neither {DERIVATION_SEPARATOR!r} nor "
            f"{OBJECT_NAME_SEPARATOR!r}: {derivation!r}"
        )
    return sorted(f"{identity}{DERIVATION_SEPARATOR}{label}" for identity in identities)


# The lane contracts (`*-source-objects.json`, root ADR-0169), beside the table contracts.
LANE_CONTRACTS_DIR = Path(__file__).resolve().parents[2] / "contracts"
COMPARE = "compare"
FULL_REASON = "full_reason"


def overwrite_declaration(contract_table: str, lanes_dir: Path = LANE_CONTRACTS_DIR) -> str:
    """How the Silver lane writing `contract_table` answers for an overwrite: `compare` or `full_reason`.

    An overwrite rewrites the whole table for whatever changed. `silver.land_forest_ledger` did that
    every week, 39.9M rows for about 5K changes, and nothing anywhere said so (root ADR-0311). So an
    overwrite is refused unless the lane's contract names `compare.content_columns` (the release is
    staged on its load branch and only the changed keys reach `main`, root ADR-0275) or a
    `full_reason` with a measured cost. The lane parser (`silver_refresh/lane.rs`) refuses the same
    contracts; this is the same rule where the write happens, for any other caller.
    """
    for path in sorted(lanes_dir.glob("*-source-objects.json")):
        block = json.loads(path.read_text(encoding="utf-8")).get("silver_refresh") or {}
        if block.get("table") != contract_table:
            continue
        if (block.get(COMPARE) or {}).get("content_columns"):
            return COMPARE
        full = block.get(FULL_REASON) or {}
        if str(full.get("reason", "")).strip() and str(full.get("measured_cost", "")).strip():
            return FULL_REASON
    raise ValueError(
        f"Refusing to overwrite {contract_table}: no Silver lane contract names compare.content_columns "
        "(append only the changed keys, root ADR-0275) or a full_reason with its measured cost (root ADR-0311)"
    )


def unquoted_table_name(qualified_table: str) -> str:
    """Strip the identifier quotes a `spark.sql` name carries.

    `spark.catalog.tableExists` wants the plain name and `spark.sql` wants the quoted one, so
    the two spellings existed side by side and were passed as two arguments. A caller that got
    one of them wrong would ask about a table that does not exist and append a batch the table
    already held — the exact answer the whole check is here to avoid — so the second spelling
    is derived rather than supplied.
    """
    return qualified_table.replace("`", "")


def append_batch_once(
    spark: Any,
    frame: Any,
    columns: Sequence[str],
    qualified_table: str,
    contract_table: str,
    write_mode: str = "append",
    derivation: str | None = None,
    branch: str | None = None,
) -> dict[str, Any]:
    """Append this batch, or report that the table already holds it.

    The whole decision lives here rather than in each job because it is one rule, and a rule
    restated once per job is a rule that will differ per job. Four jobs wrote through SQL
    `INSERT`, which cannot carry writer options at all, so each of them had no record of what
    it appended and no way to acquire one — the same shape that put 1,865,891 parcels into a
    table three times.

    `contract_table` names the contract, not the Iceberg table: the target may be a `_smoke`
    copy, and what a load carries is a property of the dataset either way. It is required
    rather than defaulted because the default was `source_record_id` and no caller ever
    overrode it, so three tables were compared on a column they do not fill (root ADR-0069).

    Returns what happened, so the caller can log its own line without repeating the decision:
    `appended` says whether rows were written, `record_ids` and `token` name the batch, and
    `existing_snapshot` is the snapshot that already holds it when nothing was written.

    `derivation` re-derives objects the table already holds under a new label; see
    `identities_under_derivation`.

    `branch` stages the batch on that branch of the table, made from `main` when it does not exist
    yet (root ADR-0183): a release loaded in several batches is written there and published to
    `main` in one commit, so readers never see part of it. Whether the batch is already held is
    asked of the branch, so a retried load continues where it stopped.
    """
    unquoted = unquoted_table_name(qualified_table)
    # A `_smoke` copy is a disposable proof table, overwritten by design (the jobs' --allow-non-smoke-overwrite).
    declared = (overwrite_declaration(contract_table)
                if write_mode == "overwrite" and not unquoted.endswith("_smoke") else None)
    record_ids = identities_under_derivation(
        batch_load_identities(frame, contract_table), derivation
    )
    token = ingest_batch_token(record_ids)
    ref = MAIN
    target = qualified_table
    # A table with no snapshot yet has nothing a branch could be made from, and no reader yet.
    if branch is not None and ref_lineage(spark, qualified_table, MAIN):
        ref = checked_ref(branch)
        spark.sql(f"ALTER TABLE {qualified_table} CREATE BRANCH IF NOT EXISTS {ref}")
        target = f"{qualified_table}.`branch_{ref}`"
    ingested = read_ingested_objects(spark, qualified_table, unquoted, ref)
    # A comparing lane overwrites only its load branch; `main` gets the changed keys (root ADR-0275).
    if declared == COMPARE and ref == MAIN and table_holds_rows(spark, qualified_table, unquoted, MAIN):
        raise ValueError(
            f"Refusing to overwrite {qualified_table} on main: its lane compares, so a release is staged "
            "on a load branch and only its changed keys are appended to main (root ADR-0275, ADR-0311)"
        )

    # A table with rows and no registry is not an empty table. Read as one — which is what
    # happened until 2026-09-01 — every object looks unloaded and the whole table is appended a
    # second time. Five of this repository's six tables were in that state, holding 133,583,046
    # rows between them, because the guard arrived after they were loaded (root ADR-0069).
    #
    # Overwrite replaces what it writes, so the question does not arise there.
    if write_mode != "overwrite" and not ingested and table_holds_rows(spark, qualified_table, unquoted, ref):
        raise ValueError(
            f"{qualified_table} already holds rows but records no ingested objects, so this "
            "append cannot tell whether it would be the second one. Backfill the registry from "
            "the rows the table already holds before loading it again (root ADR-0069)."
        )

    if not decide_whether_to_append(ingested, record_ids):
        return {
            "appended": False,
            "record_ids": record_ids,
            "token": token,
            "existing_snapshot": ingested[record_ids[0]],
            "ref": ref,
        }

    # The DataFrame writer, not SQL: only this writer accepts the options that carry the
    # record of the append into the same commit as the rows.
    writer = frame.select(*columns).writeTo(target)
    for key, value in snapshot_property_options(record_ids, token).items():
        writer = writer.option(key, value)

    if write_mode == "overwrite":
        writer.overwritePartitions()
    else:
        writer.append()

    return {
        "appended": True,
        "record_ids": record_ids,
        "token": token,
        "existing_snapshot": None,
        "ref": ref,
    }


def append_source_object(spark: Any, frame: Any, columns: Sequence[str], qualified_table: str, object_key: str) -> bool:
    """Append one source object's rows in a commit that records the object, even with no rows.

    `append_batch_once` names a batch by the identities in its rows, so an object whose load
    writes nothing (a night with no change, a release compare with no difference) leaves no
    record, and a load that decides what it owes from the table's records
    (`plan-silver-load-objects`) would fetch it again on every run. This records the object
    itself, as `silver_release_compare` does. Returns False, writing nothing, when `main` already
    records it.
    """
    ingested = read_ingested_objects(spark, qualified_table, unquoted_table_name(qualified_table))
    if not decide_whether_to_append(ingested, [object_key]):
        return False
    writer = frame.select(*columns).writeTo(qualified_table)
    for key, value in snapshot_property_options([object_key], ingest_batch_token([object_key])).items():
        writer = writer.option(key, value)
    writer.append()
    return True
