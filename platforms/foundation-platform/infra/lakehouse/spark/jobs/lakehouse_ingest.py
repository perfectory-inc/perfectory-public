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
from collections.abc import Sequence
from typing import Any

# Written in the same Iceberg commit as the data files. `TOKEN` names the batch for a human
# reading `<table>.snapshots`; `OBJECTS` is what the skip decision actually reads.
INGEST_BATCH_TOKEN_KEY = "foundation.ingest-batch-token"
INGEST_BATCH_OBJECTS_KEY = "foundation.ingest-batch-objects"

# Iceberg's Spark writer strips this prefix and puts the rest into the snapshot summary.
SNAPSHOT_PROPERTY_PREFIX = "snapshot-property."

# `,` separates object names inside one summary value, so a name containing one would split
# into two names and the skip decision would ask about objects that do not exist.
OBJECT_NAME_SEPARATOR = ","

# The column every load carries that names the Bronze object a row came from.
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


def read_ingested_objects(spark: Any, qualified_table: str, unquoted_table: str) -> dict[str, int]:
    """Map every source object already appended to the snapshot that appended it.

    Reads the table's own snapshot summaries, so the answer comes from the same commit log
    that holds the rows. Iceberg drops summaries when snapshots expire, so a retention window
    shorter than a load leaves an appended object looking un-appended; keep the snapshots a
    load produced until the load is finished. Iceberg's Flink documentation carries the same
    warning about expiring the snapshots its committer reads back.
    """
    if not spark.catalog.tableExists(unquoted_table):
        return {}

    ingested: dict[str, int] = {}
    for row in spark.sql(f"SELECT snapshot_id, summary FROM {qualified_table}.snapshots").collect():
        recorded = (row.summary or {}).get(INGEST_BATCH_OBJECTS_KEY)
        if not recorded:
            continue
        for name in recorded.split(OBJECT_NAME_SEPARATOR):
            ingested.setdefault(name, int(row.snapshot_id))
    return ingested


def table_holds_rows(spark: Any, qualified_table: str, unquoted_table: str) -> bool:
    """Whether the table's current snapshot holds any rows.

    Read from the snapshot summary rather than counted, because the question is only whether the
    table is empty and a count of 113,813,264 rows would open every file to answer it.
    """
    if not spark.catalog.tableExists(unquoted_table):
        return False
    rows = spark.sql(
        f"SELECT summary FROM {qualified_table}.snapshots "
        f"WHERE snapshot_id = (SELECT snapshot_id FROM {qualified_table}.refs WHERE name = 'main')"
    ).collect()
    for row in rows:
        total = (row.summary or {}).get("total-records")
        if total is not None and int(total) > 0:
            return True
    return False


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
    """Name the source objects this run carries, in a fixed order.

    Sorted so that the same objects yield the same list however the input files were globbed
    or ordered, which is what lets the token identify a batch by its content.

    One more than the limit is fetched so that exceeding it is an error rather than a silent
    truncation. A truncated list would produce a token for a batch nobody ran, and record
    fewer objects than the commit actually appended — which is the defect this module exists
    to remove, reintroduced one layer down.
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
    write_mode: str = "append",
    record_id_column: str = SOURCE_RECORD_COLUMN,
) -> dict[str, Any]:
    """Append this batch, or report that the table already holds it.

    The whole decision lives here rather than in each job because it is one rule, and a rule
    restated once per job is a rule that will differ per job. Four jobs wrote through SQL
    `INSERT`, which cannot carry writer options at all, so each of them had no record of what
    it appended and no way to acquire one — the same shape that put 1,865,891 parcels into a
    table three times.

    Returns what happened, so the caller can log its own line without repeating the decision:
    `appended` says whether rows were written, `record_ids` and `token` name the batch, and
    `existing_snapshot` is the snapshot that already holds it when nothing was written.
    """
    record_ids = batch_source_record_ids(frame, record_id_column)
    token = ingest_batch_token(record_ids)
    unquoted = unquoted_table_name(qualified_table)
    ingested = read_ingested_objects(spark, qualified_table, unquoted)

    # A table with rows and no registry is not an empty table. Read as one — which is what
    # happened until 2026-09-01 — every object looks unloaded and the whole table is appended a
    # second time. Five of this repository's six tables were in that state, holding 133,583,046
    # rows between them, because the guard arrived after they were loaded (root ADR-0069).
    #
    # Overwrite replaces what it writes, so the question does not arise there.
    if write_mode != "overwrite" and not ingested and table_holds_rows(spark, qualified_table, unquoted):
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
        }

    # The DataFrame writer, not SQL: only this writer accepts the options that carry the
    # record of the append into the same commit as the rows.
    writer = frame.select(*columns).writeTo(qualified_table)
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
    }
