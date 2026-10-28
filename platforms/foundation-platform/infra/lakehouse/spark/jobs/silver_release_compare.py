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

The commit is also the release's change record (root ADR-0191 as amended by ADR-0311): its summary
names it one (`changelog.change_record_options`), so a consumer reads only these rows
(`changelog.changes_between`) instead of diffing two snapshots of the table.

The last stdout line, and `--summary-output`, is `silver-release-compare {json}`.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from changelog import change_record_options
from lakehouse_engine import apply_catalog_settings, assert_iceberg_runtime_loaded, iceberg_packages
from lakehouse_ingest import (
    MAIN,
    checked_ref,
    ingest_batch_token,
    read_ingested_objects,
    ref_lineage,
    snapshot_property_options,
)
from platform_contracts import REMOVED, column_names, current_row_latest, current_rows, load_lakehouse_contract

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
                    source_snapshot_id: str, ingested_at: Any, columns: list[str]) -> Any:
    """The rows to append: every staged row of a key that is new or whose rows changed, and one
    removal marker per key with current rows and none staged.

    A key's release is a set of rows (the source repeats a key, root ADR-0090), so a key is compared
    by the multiset of its rows' content fingerprints, repeats counted.
    """

    from pyspark.sql import Window  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415

    # Null-ness hashed apart from the value, so a null and an empty string differ.
    fingerprint = F.xxhash64(*(F.col(c).isNull() for c in content), *(F.col(c) for c in content))

    def signatures(frame: Any, name: str) -> Any:
        return frame.groupBy(*key).agg(F.xxhash64(F.sort_array(F.collect_list(fingerprint))).alias(name))

    old = signatures(current, "_current")
    changed_keys = (signatures(staged, "_staged").join(old, list(key), "left")
                    .where(F.col("_current").isNull() | (F.col("_current") != F.col("_staged"))).select(*key))
    changed = staged.join(changed_keys, list(key), "left_semi")
    # One marker per vanished key, made from one of its current rows (the required columns need values).
    first = Window.partitionBy(*key).orderBy(fingerprint)
    removed = (current.join(staged.select(*key).distinct(), list(key), "left_anti")
               .withColumn("_n", F.row_number().over(first)).where(F.col("_n") == 1)
               .withColumn(removal, F.lit(REMOVED))
               .withColumn("source_snapshot_id", F.lit(source_snapshot_id))
               .withColumn("ingested_at_utc", ingested_at))
    return changed.select(*columns).unionByName(removed.select(*columns))


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
    current = current_rows(spark.table(unquoted), contract).select(*columns)
    changes = release_changes(staged, current, key=key, content=content, removal=removal,
                              source_snapshot_id=args.source_snapshot_id,
                              ingested_at=F.current_timestamp(), columns=columns).persist(StorageLevel.DISK_ONLY)
    counts = changes.groupBy(F.col(removal) == F.lit(REMOVED)).count().collect()
    removed = sum(row["count"] for row in counts if row[0])
    appended = sum(row["count"] for row in counts if not row[0])
    print(f"{JOB_NAME} {args.contract}: appending {appended} rows of changed keys and {removed} removal markers",
          flush=True)

    writer = changes.writeTo(unquoted)
    options = {**snapshot_property_options(objects, ingest_batch_token(objects)),
               **change_record_options(appended, removed)}
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
