"""Append a staged Silver release to `main` as its changes (root ADR-0275), bucket by bucket (ADR-0318).

The Silver refresh lane stages a full-dump release on its load branch (root ADR-0183). For a lane
whose contract declares `compare`, the branch is not published. This job compares the staged
release with `main`'s current rows (`platform_contracts.current_rows`) by the table's row key and
the lane's content columns, and stages on the release's changes branch:

- every staged row of a key that is new, or whose rows differ from the key's current rows (a key's
  release is a set of rows: the source repeats a key, root ADR-0090);
- for each key with current rows and no staged row, a removal marker: one of those rows with
  `removal_column = removed`, the release's `source_snapshot_id` and this run's `ingested_at_utc`.

A key whose rows are unchanged gets nothing. No row of `main` is rewritten or deleted.

**Buckets (ADR-0318).** With `--bucket-prefix-length N` the keys are split by the first N characters
of the key's first column (a 시도 lane: the PNU's 시도, N = 2), and each bucket is compared alone and
committed alone to the changes branch, naming its bucket in the commit's summary. Each bucket reads
only its own rows of both sides (`startsWith`, which Iceberg prunes by file and row-group bounds).
A run that reaches `--deadline-utc` starts no new bucket and reports `deadline`; the next run
compares only the buckets the branch does not record yet. The 2026-10-21 compare of 131,664,668 rows
took 87 minutes in one piece, and `silver.land_use_plan` (247.58M) would not fit the unit's 150.

When every bucket is recorded, a last empty commit records every Bronze object of the release in
its snapshot summary, as `append_batch_once` does (`staged`). The lane then publishes the changes
branch as `main` in one fast-forward (`publish_branch`, root ADR-0183), so a reader never sees half
a release, "is this release loaded" reads the same as for an overwrite, and a re-run writes
nothing. A release with no change still commits its empty buckets, to record that it was compared.

Every commit is also part of the release's change record (root ADR-0191 as amended by ADR-0311):
its summary names it one (`changelog.change_record_options`), so a consumer reads only these rows
(`changelog.changes_between`) instead of diffing two snapshots of the table.

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

from changelog import RELEASE_VERSION_KEY, change_record_options
from lakehouse_engine import apply_catalog_settings, assert_iceberg_runtime_loaded, iceberg_packages
from lakehouse_ingest import (
    MAIN,
    SNAPSHOT_PROPERTY_PREFIX,
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
# On a changes-branch commit: the release it belongs to and the bucket of keys it compared (ADR-0318).
COMPARE_RELEASE_KEY = "foundation.compare-release"
COMPARE_BUCKET_KEY = "foundation.compare-bucket"
# The one bucket of a compare that is not split.
ALL = "*"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--contract", required=True, help="The Silver table, e.g. silver.land_transfer_history")
    parser.add_argument("--branch", required=True, help="The load branch the release is staged on")
    parser.add_argument("--source-snapshot-id", required=True, help="The release's source_snapshot_id")
    parser.add_argument("--objects", required=True, help="Comma-separated Bronze object keys of the release")
    parser.add_argument("--content-columns", required=True, help="Comma-separated columns a row's content is")
    parser.add_argument("--changes-branch", required=True, help="The branch the changes are staged on until published")
    parser.add_argument("--bucket-prefix-length", type=int, default=0,
                        help="Compare bucket by bucket: the leading characters of the key's first column (0: one)")
    parser.add_argument("--deadline-utc", help="ISO-8601 instant after which no new bucket starts")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    parser.add_argument("--summary-output")
    args = parser.parse_args(argv)
    checked_ref(args.branch)
    checked_ref(args.changes_branch)
    if MAIN in (args.branch, args.changes_branch) or args.branch == args.changes_branch:
        raise ValueError("the release is staged on a load branch and its changes on another branch, neither main")
    if args.bucket_prefix_length < 0:
        raise ValueError("--bucket-prefix-length is 0 or more")
    if args.deadline_utc is not None:
        args.deadline_utc = deadline(args.deadline_utc)
    return args


def deadline(raw: str) -> datetime:
    """An aware instant: a deadline without a zone would be read in whatever zone the driver has."""
    instant = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        raise ValueError(f"--deadline-utc {raw!r} names no zone")
    return instant


def bucket_label(bucket: str | None) -> str:
    """How a bucket is named in a commit summary: JSON, so the null bucket and "" stay apart."""
    return json.dumps(bucket)


def bucket_predicate(column: str, length: int, bucket: str | None) -> Any:
    """The rows of `bucket`. A value shorter than `length` is its own bucket, matched exactly, so
    no value is in two buckets."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    if bucket is None:
        return F.col(column).isNull()
    if len(bucket) < length:
        return F.col(column) == F.lit(bucket)
    return F.col(column).startswith(bucket)


def ordered_buckets(found: set[str | None]) -> list[str | None]:
    """Null first, then in order: the same order on every run."""
    return sorted(found, key=lambda b: (b is not None, b or ""))


def buckets(frames: list[Any], column: str, length: int) -> list[str | None]:
    """Every bucket either side holds, reading only the key column."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    if length == 0:
        return [ALL]
    prefix = F.substring(F.col(column), 1, length).alias("_bucket")
    found: set[str | None] = set()
    for frame in frames:
        found |= {row["_bucket"] for row in frame.select(prefix).distinct().collect()}
    return ordered_buckets(found)


def recorded_buckets(snapshots: list[dict[str, Any]], lineage: set[int], source_snapshot_id: str) -> set[str]:
    """The buckets of this release that the commits of `lineage` record as compared."""
    return {s["summary"][COMPARE_BUCKET_KEY] for s in snapshots
            if int(s["snapshot_id"]) in lineage and (s.get("summary") or {}).get(COMPARE_RELEASE_KEY) == source_snapshot_id
            and COMPARE_BUCKET_KEY in s["summary"]}


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


def release_versions(staged: Any, newest_first: tuple[str, ...]) -> dict[str, Any]:
    """The release's value of each `newest_first` column the load does not stamp itself (a vintage):
    read once for the whole release, since a bucket that only lost keys has no staged row to read."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    stamped = {"source_snapshot_id", "ingested_at_utc"}
    return {c: staged.agg(F.max(c)).first()[0] for c in newest_first if c not in stamped}


def release_changes(staged: Any, current: Any, *, key: tuple[str, ...], content: list[str], removal: str,
                    source_snapshot_id: str, ingested_at: Any, columns: list[str],
                    versions: dict[str, Any] | None = None,
                    stamp: dict[str, Any] | None = None) -> Any:
    """The rows to append: every staged row of a key that is new or whose rows changed, and one
    removal marker per key with current rows and none staged.

    A key's release is a set of rows (the source repeats a key, root ADR-0090), so a key is compared
    by the multiset of its rows' content fingerprints, repeats counted. Keys match null-safe: a
    source row may lack a key column (a price line without a management key) and is still one key.
    A marker carries the release's `source_snapshot_id`, this run's `ingested_at_utc` and the
    release's `versions` (`release_versions`): a marker that kept the vanished row's vintage would
    tie with it and leave it current (ADR-0318). `stamp` then sets columns on every appended row,
    changed and marker alike (`commit_stamp`, ADR-0317).
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
    for column, version in (versions or {}).items():
        if version is None:
            raise ValueError(f"the release carries no {column}, so a removal marker could not outrank the row it removes")
        removed = removed.withColumn(column, F.lit(version).cast(current.schema[column].dataType))
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


def compare_and_append(spark: Any, args: argparse.Namespace, now: Any = None) -> dict[str, Any]:
    from pyspark.sql import functions as F  # noqa: PLC0415
    from pyspark.storagelevel import StorageLevel  # noqa: PLC0415

    now = now or (lambda: datetime.now(timezone.utc))
    contract = load_lakehouse_contract(args.contract)
    content = [c for c in args.content_columns.split(",") if c]
    key, removal = compare_rule(contract, content)
    objects = sorted(o for o in args.objects.split(",") if o)
    namespace, name = args.contract.split(".", 1)
    unquoted = f"{args.iceberg_catalog_name}.{namespace}.{name}"
    qualified = f"`{args.iceberg_catalog_name}`.`{namespace}`.`{name}`"
    report: dict[str, Any] = {"table": args.contract, "source_snapshot_id": args.source_snapshot_id,
                              "branch": args.branch, "changes_branch": args.changes_branch, "objects": len(objects)}

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

    # Made from main's head; the lane drops it first when main moved since (remove_stale_branch).
    spark.sql(f"ALTER TABLE {qualified} CREATE BRANCH IF NOT EXISTS {args.changes_branch}")
    target = f"{unquoted}.branch_{args.changes_branch}"
    # main records none of the objects, so a record of them on the changes branch is its seal.
    sealed = read_ingested_objects(spark, qualified, unquoted, args.changes_branch)
    if all(o in sealed for o in objects):
        return {**report, "outcome": "staged", "resumed": True}

    columns = column_names(contract)
    staged = (spark.table(f"{unquoted}.branch_{args.branch}")
              .where(F.col("source_snapshot_id") == args.source_snapshot_id).select(*columns))
    main = spark.table(unquoted)
    length = args.bucket_prefix_length
    every = buckets([staged, main], key[0], length)
    snapshots = [{"snapshot_id": r.snapshot_id, "summary": r.summary or {}}
                 for r in spark.sql(f"SELECT snapshot_id, summary FROM {qualified}.snapshots").collect()]
    done = recorded_buckets(snapshots, ref_lineage(spark, qualified, args.changes_branch), args.source_snapshot_id)
    pending = [b for b in every if bucket_label(b) not in done]
    latest = current_row_latest(contract)
    versions = release_versions(staged, latest.newest_first)
    edition = release_edition(staged, latest)
    # One instant for the run: the markers and, for a table ordered by it, every row (ADR-0317).
    instant = F.lit(now()).cast("timestamp")
    stamp, edition_column = commit_stamp(latest, edition, instant)
    if edition_column is not None:
        refuse_older_edition(edition, read_editions(spark, qualified, unquoted, MAIN))
        report["edition"] = edition
    totals = {"changed_key_rows": 0, "removed_keys": 0}
    progress = {"buckets": len(every), "buckets_done": len(every) - len(pending)}
    for bucket in pending:
        if args.deadline_utc is not None and now() >= args.deadline_utc:
            return {**report, "outcome": "deadline", **progress, **totals}
        if bucket == ALL:
            staged_part, main_part = staged, main
        else:
            predicate = bucket_predicate(key[0], length, bucket)
            staged_part, main_part = staged.where(predicate), main.where(predicate)
        # A key's rows all share its first column, so a bucket's current rows are the table's for it.
        current = current_rows(main_part, contract).select(*columns)
        changes = release_changes(staged_part, current, key=key, content=content, removal=removal,
                                  source_snapshot_id=args.source_snapshot_id,
                                  ingested_at=instant, columns=columns,
                                  versions=versions, stamp=stamp).persist(StorageLevel.DISK_ONLY)
        counts = changes.groupBy(F.col(removal) == F.lit(REMOVED)).count().collect()
        removed = sum(row["count"] for row in counts if row[0])
        appended = sum(row["count"] for row in counts if not row[0])
        print(f"{JOB_NAME} {args.contract} bucket {bucket_label(bucket)}: appending {appended} rows of changed "
              f"keys and {removed} removal markers", flush=True)
        write(changes, target, {**change_record_options(appended, removed),
                                f"{SNAPSHOT_PROPERTY_PREFIX}{COMPARE_RELEASE_KEY}": args.source_snapshot_id,
                                f"{SNAPSHOT_PROPERTY_PREFIX}{COMPARE_BUCKET_KEY}": bucket_label(bucket)})
        changes.unpersist()
        totals["changed_key_rows"] += appended
        totals["removed_keys"] += removed
        progress["buckets_done"] += 1
    # The seal: an empty commit that records the release's objects, so the published main says it is
    # loaded, and the version the release carries, which a release that changed nothing has no row of.
    seal = {**snapshot_property_options(objects, ingest_batch_token(objects)), **change_record_options(0, 0),
            **(edition_property_option(edition) if edition_column is not None else {})}
    if len(latest.newest_first) == 1:
        version = (versions.get(latest.newest_first[0]) if latest.newest_first[0] in versions
                   else staged.agg(F.max(latest.newest_first[0])).first()[0])
        if version is not None:
            seal[f"{SNAPSHOT_PROPERTY_PREFIX}{RELEASE_VERSION_KEY}"] = str(version)
    write(staged.limit(0), target, seal)
    return {**report, "outcome": "staged", "resumed": False, "staged_rows": staged.count(), **progress, **totals}


def write(frame: Any, target: str, options: dict[str, str]) -> None:
    writer = frame.writeTo(target)
    for option, value in options.items():
        writer = writer.option(option, value)
    writer.append()


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
