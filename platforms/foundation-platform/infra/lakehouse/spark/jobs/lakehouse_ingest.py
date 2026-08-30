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
