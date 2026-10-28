"""Append a staged Silver release to `main` as its changes (root ADR-0275).

The Silver refresh lane stages a full-dump release on its load branch (root ADR-0183). For a lane
whose contract declares `compare`, the branch is not published. This job compares the staged
release with `main`'s current rows (`platform_contracts.current_rows`) by the table's row key and
the lane's content columns, and appends to `main`, in one commit:

- every staged row of a key that is new, or whose rows differ from the key's current rows (a key's
  release is a set of rows: the source repeats a key, root ADR-0090);
- for each key with current rows and no staged row, a removal marker: one of those rows with
  `removal_column = removed`, the release's `source_snapshot_id` and this run's `ingested_at_utc`.

A key whose rows are unchanged gets nothing. No row of `main` is rewritten or deleted. The commit
records every Bronze object of the release in its snapshot summary, as `append_batch_once` does, so "is this release
loaded" reads the same as for an overwrite, and a re-run after the commit writes nothing. A release
with no change still commits, empty, to record that it was loaded.

A table whose versions are ordered by an edition (a vintage, root ADR-0317) gets the release's
edition and one commit instant on every appended row (`commit_stamp`), the commit records the
edition (`read_editions`), and a release older than an edition already held is refused.

The last stdout line, and `--summary-output`, is `silver-release-compare {json}`.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from functools import reduce
from operator import and_
from pathlib import Path
from typing import Any

from lakehouse_engine import apply_catalog_settings, assert_iceberg_runtime_loaded, iceberg_packages
from lakehouse_ingest import (
    MAIN,
    checked_ref,
    edition_property_option,
    ingest_batch_token,
    read_editions,
    read_ingested_objects,
    ref_lineage,
    snapshot_property_options,
)
from platform_contracts import (REMOVED, CurrentRowLatest, column_names, current_row_latest, current_rows,
                                load_lakehouse_contract)

JOB_NAME = "silver-release-compare"
# Written by the load, never by the source: comparing them would make every row look changed.
LOAD_COLUMNS = ("source_record_id", "source_snapshot_id", "ingested_at_utc")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--contract", required=True, help="The Silver table, e.g. silver.land_transfer_history")
    parser.add_argument("--branch", required=True, help="The load branch the release is staged on")
    parser.add_argument("--source-snapshot-id", required=True, help="The release's source_snapshot_id")
    parser.add_argument("--objects", required=True, help="Comma-separated Bronze object keys of the release")
    parser.add_argument("--content-columns", required=True, help="Comma-separated columns a row's content is")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    parser.add_argument("--summary-output")
    args = parser.parse_args(argv)
    checked_ref(args.branch)
    if args.branch == MAIN:
        raise ValueError("the release is staged on a load branch, not main")
    return args


def compare_rule(contract: dict[str, Any], content: list[str]) -> tuple[tuple[str, ...], str]:
    """The table's row key and removal column; refuses content columns the comparison cannot use."""

    table = contract.get("table_name")
    latest = current_row_latest(contract)
    if latest is None or latest.removal_column is None:
        raise ValueError(f"{table} declares no current_row_latest with a removal_column to append changes under")
    names = set(column_names(contract))
    excluded = {*latest.key, *LOAD_COLUMNS, latest.removal_column}
    unknown = sorted(set(content) - names)
    loaded = sorted(set(content) & excluded)
    if not content or unknown or loaded:
        raise ValueError(f"{table} content columns must be its own source columns, not its key or load columns: "
                         f"unknown {unknown}, not content {loaded}")
    return latest.key, latest.removal_column


def release_changes(staged: Any, current: Any, *, key: tuple[str, ...], content: list[str], removal: str,
                    source_snapshot_id: str, ingested_at: Any, columns: list[str],
                    stamp: dict[str, Any] | None = None) -> Any:
    """The rows to append: every staged row of a key that is new or whose rows changed, and one
    removal marker per key with current rows and none staged.

    A key's release is a set of rows (the source repeats a key, root ADR-0090), so a key is compared
    by the multiset of its rows' content fingerprints, repeats counted. Keys match null-safe: a
    source row may lack a key column (a price line without a management key) and is still one key.
    `stamp` sets columns on every appended row, changed and marker alike (`commit_stamp`).
    """

    from pyspark.sql import Window  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415

    # Null-ness hashed apart from the value, so a null and an empty string differ.
    fingerprint = F.xxhash64(*(F.col(c).isNull() for c in content), *(F.col(c) for c in content))
    renamed = [f"_key{index}" for index in range(len(key))]

    def signatures(frame: Any, name: str) -> Any:
        return frame.groupBy(*key).agg(F.xxhash64(F.sort_array(F.collect_list(fingerprint))).alias(name))

    def keys_of(frame: Any) -> Any:
        # Renamed, so a join of two frames of one lineage never compares a column with itself.
        return frame.select(*(F.col(k).alias(r) for k, r in zip(key, renamed)))

    def matching(frame: Any, keys: Any) -> Any:
        return reduce(and_, (frame[k].eqNullSafe(keys[r]) for k, r in zip(key, renamed)))

    staged_signatures = signatures(staged, "_staged")
    old = signatures(current, "_current")
    old = old.select(*(F.col(k).alias(r) for k, r in zip(key, renamed)), "_current")
    changed_keys = (staged_signatures.join(old, matching(staged_signatures, old), "left")
                    .where(F.col("_current").isNull() | (F.col("_current") != F.col("_staged")))
                    .select(*(staged_signatures[k].alias(r) for k, r in zip(key, renamed))))
    changed = staged.join(changed_keys, matching(staged, changed_keys), "left_semi")
    # One marker per vanished key, made from one of its current rows (the required columns need values).
    first = Window.partitionBy(*key).orderBy(fingerprint)
    staged_keys = keys_of(staged).distinct()
    removed = (current.join(staged_keys, matching(current, staged_keys), "left_anti")
               .withColumn("_n", F.row_number().over(first)).where(F.col("_n") == 1)
               .withColumn(removal, F.lit(REMOVED))
               .withColumn("source_snapshot_id", F.lit(source_snapshot_id))
               .withColumn("ingested_at_utc", ingested_at))
    for column, value in (stamp or {}).items():
        changed, removed = changed.withColumn(column, value), removed.withColumn(column, value)
    return changed.select(*columns).unionByName(removed.select(*columns))


def commit_stamp(latest: CurrentRowLatest, edition: str | None, ingested_at: Any) -> tuple[dict[str, Any], str | None]:
    """The values that order this commit's rows after every version they replace, and the edition column.

    `current_rows` ranks a key's versions by `newest_first`. A release ordered by its source id
    (the land lanes) already carries it on every row. A table ordered by an edition (a vintage) and
    the commit's instant (root ADR-0317) gets both on every appended row: the release's edition, and
    one instant for the commit, so a key's staged rows that loaded in different batches stay one
    version, and an edition loaded again (a full load after a partial one, root ADR-0185) wins.
    """

    from pyspark.sql import functions as F  # noqa: PLC0415

    stamp: dict[str, Any] = {}
    edition_column = None
    for column in latest.newest_first:
        if column == "ingested_at_utc":
            stamp[column] = ingested_at
        elif column in LOAD_COLUMNS:
            continue
        elif edition_column is None:
            edition_column = column
        else:
            raise ValueError(f"versions are ordered by one edition column, not {list(latest.newest_first)}")
    if edition_column is not None:
        if not edition:
            raise ValueError(f"a release of a table ordered by {edition_column} names its edition")
        stamp[edition_column] = F.lit(edition)
    return stamp, edition_column


def release_edition(staged: Any, latest: CurrentRowLatest | None) -> str | None:
    """The one edition a staged release is, when its table is ordered by one; None otherwise."""

    columns = [c for c in (latest.newest_first if latest else ()) if c not in LOAD_COLUMNS]
    if not columns:
        return None
    values = [row[0] for row in staged.select(columns[0]).distinct().limit(2).collect()]
    if len(values) != 1 or not values[0]:
        raise ValueError(f"a staged release is one {columns[0]}, not {values}")
    return str(values[0])


def refuse_older_edition(edition: str, held: set[str]) -> None:
    """An edition older than one already held would change the state the later editions were
    compared against, so their versions would no longer reconstruct them (root ADR-0317)."""

    newer = sorted(held_edition for held_edition in held if held_edition > edition)
    if newer:
        raise ValueError(f"edition {edition} is older than the held editions {newer}; versions are appended in edition order")


def compare_and_append(spark: Any, args: argparse.Namespace) -> dict[str, Any]:
    from pyspark.sql import functions as F  # noqa: PLC0415
    from pyspark.storagelevel import StorageLevel  # noqa: PLC0415

    contract = load_lakehouse_contract(args.contract)
    content = [c for c in args.content_columns.split(",") if c]
    key, removal = compare_rule(contract, content)
    objects = sorted(o for o in args.objects.split(",") if o)
    namespace, name = args.contract.split(".", 1)
    unquoted = f"{args.iceberg_catalog_name}.{namespace}.{name}"
    qualified = f"`{args.iceberg_catalog_name}`.`{namespace}`.`{name}`"
    report: dict[str, Any] = {"table": args.contract, "source_snapshot_id": args.source_snapshot_id,
                              "branch": args.branch, "objects": len(objects)}

    recorded = read_ingested_objects(spark, qualified, unquoted, MAIN)
    held = [o for o in objects if o in recorded]
    if held:
        if len(held) != len(objects):
            raise ValueError(f"main records {len(held)} of the release's {len(objects)} objects; a release is one commit")
        return {**report, "outcome": "already_appended"}
    if not ref_lineage(spark, qualified, args.branch):
        raise ValueError(f"{args.contract} has no branch {args.branch} to compare")
    staged_objects = read_ingested_objects(spark, qualified, unquoted, args.branch)
    unstaged = [o for o in objects if o not in staged_objects]
    if unstaged:
        raise ValueError(f"branch {args.branch} does not record {len(unstaged)} of the release's objects: {unstaged[:3]}")

    columns = column_names(contract)
    staged = (spark.table(f"{unquoted}.branch_{args.branch}")
              .where(F.col("source_snapshot_id") == args.source_snapshot_id).select(*columns))
    edition = release_edition(staged, current_row_latest(contract))
    # One instant for the commit: the markers and, for a table ordered by it, every row.
    instant = F.lit(datetime.now(timezone.utc)).cast("timestamp")
    stamp, edition_column = commit_stamp(current_row_latest(contract), edition, instant)
    options = snapshot_property_options(objects, ingest_batch_token(objects))
    if edition_column is not None:
        refuse_older_edition(edition, read_editions(spark, qualified, unquoted, MAIN))
        options.update(edition_property_option(edition))
        report["edition"] = edition
    current = current_rows(spark.table(unquoted), contract).select(*columns)
    changes = release_changes(staged, current, key=key, content=content, removal=removal,
                              source_snapshot_id=args.source_snapshot_id, ingested_at=instant,
                              columns=columns, stamp=stamp).persist(StorageLevel.DISK_ONLY)
    counts = changes.groupBy(F.col(removal) == F.lit(REMOVED)).count().collect()
    removed = sum(row["count"] for row in counts if row[0])
    appended = sum(row["count"] for row in counts if not row[0])
    print(f"{JOB_NAME} {args.contract}: appending {appended} rows of changed keys and {removed} removal markers",
          flush=True)

    writer = changes.writeTo(unquoted)
    for option, value in options.items():
        writer = writer.option(option, value)
    writer.append()
    changes.unpersist()
    return {**report, "outcome": "appended", "staged_rows": staged.count(), "changed_key_rows": appended,
            "removed_keys": removed}


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    from pyspark.sql import SparkSession  # noqa: PLC0415

    builder = (SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}")
               .config("spark.sql.session.timeZone", "UTC"))
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        assert_iceberg_runtime_loaded(spark, args.iceberg_packages)
        summary = compare_and_append(spark, args)
    finally:
        spark.stop()
    line = f"{JOB_NAME} " + json.dumps(summary, sort_keys=True)
    if args.summary_output:
        Path(args.summary_output).write_text(json.dumps(summary, sort_keys=True), encoding="utf-8")
    print(line, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
