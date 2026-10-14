#!/usr/bin/env python3
"""Run Iceberg table maintenance: compact, expire snapshots, remove orphans.

None of these had ever run against this lakehouse. On 2026-08-30 the tables held 3,160 data
files averaging 5 MB, no snapshot had ever been expired, and 41 of 58 GB in R2 was serving
17 GB of live rows. Iceberg ships all three operations; we simply never called them.

The order is not interchangeable and the contract states why: compaction creates the
superseded files, expiry releases the snapshots pinning them, and orphan cleanup is the only
step that reaches files no snapshot ever referenced — the debris a job leaves when it dies
mid-write. Expiry cannot see those, so running cleanup first leaves them for the next pass
and running it never leaves them forever.

Thresholds live in `lakehouse-maintenance.contract.json`, not here. A number written in a job
is a number the next job disagrees with.

No PySpark import at module scope: the lane that runs `infra/lakehouse/spark/tests` has none,
and a module-level import would make every check that touches this file skip itself.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

CONTRACT_PATH_ENV = "FOUNDATION_PLATFORM_LAKEHOUSE_MAINTENANCE_CONTRACT_PATH"
DEFAULT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[2] / "contracts" / "lakehouse-maintenance.contract.json"
)
CONTRACT_SCHEMA_VERSION = 1

# Iceberg refuses any orphan-cleanup interval under this, because deleting a live writer's
# output corrupts the commit it is about to make. Stated here so a contract that tried to go
# below it fails before the procedure does, with a reason instead of a stack trace.
MINIMUM_ORPHAN_SAFETY_HOURS = 24


def load_maintenance_contract() -> dict[str, Any]:
    path = Path(os.getenv(CONTRACT_PATH_ENV, str(DEFAULT_CONTRACT_PATH)))
    contract = json.loads(path.read_text(encoding="utf-8"))
    version = contract.get("schema_version")
    if version != CONTRACT_SCHEMA_VERSION:
        raise ValueError(
            f"unsupported maintenance contract schema_version {version!r}; "
            f"expected {CONTRACT_SCHEMA_VERSION!r}"
        )
    validate_maintenance_contract(contract)
    return contract


def validate_maintenance_contract(contract: dict[str, Any]) -> None:
    """Refuse a contract whose numbers would do harm, before any table is touched."""
    order = contract.get("order")
    if order != ["compaction", "snapshot_expiry", "orphan_cleanup"]:
        raise ValueError(
            "maintenance order must be compaction, snapshot_expiry, orphan_cleanup: "
            f"got {order!r}. Cleanup last is what reaches files no snapshot references."
        )

    safety_days = contract["orphan_cleanup"]["safety_days"]
    if safety_days * 24 < MINIMUM_ORPHAN_SAFETY_HOURS:
        raise ValueError(
            f"orphan_cleanup.safety_days={safety_days} is under Iceberg's {MINIMUM_ORPHAN_SAFETY_HOURS}h "
            "floor; deleting a running writer's output corrupts the commit it is about to make"
        )

    expiry = contract["snapshot_expiry"]
    if expiry["retain_last"] < 1:
        raise ValueError("snapshot_expiry.retain_last must keep at least the current snapshot")
    if expiry["retain_days"] < 1:
        raise ValueError("snapshot_expiry.retain_days must leave at least a day of rollback")

    monthly = expiry["month_end_tag"]
    if "%Y" not in monthly["name_format"] or "%m" not in monthly["name_format"]:
        raise ValueError(
            f"snapshot_expiry.month_end_tag.name_format={monthly['name_format']!r} must contain %Y "
            "and %m: a name two months share makes the second month's tag a no-op, and its "
            "snapshot expires"
        )
    if monthly["keep_months"] < 1:
        raise ValueError("snapshot_expiry.month_end_tag.keep_months must keep at least one month")

    yearly = expiry["year_end_tag"]["name_format"]
    if "%Y" not in yearly or "%m" in yearly:
        raise ValueError(
            f"snapshot_expiry.year_end_tag.name_format={yearly!r} must contain %Y and not %m: "
            "one name per year"
        )
    if month_of_tag(month_start(2026, 12).strftime(yearly), monthly["name_format"]) is not None:
        raise ValueError(
            "a year-end tag name must not read as a month-end tag name: the monthly window would "
            "drop the tag that is meant to be kept forever"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compact, expire and clean an Iceberg table.")
    parser.add_argument("--table", required=True, help="Logical table, e.g. silver.parcel_boundaries")
    parser.add_argument(
        "--catalog",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_CATALOG_NAME", "r2"),
        help="Spark catalog name.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what each step would do without deleting anything.",
    )
    parser.add_argument(
        "--skip",
        action="append",
        default=[],
        choices=["compaction", "snapshot_expiry", "orphan_cleanup"],
        help="Skip a step. Orphan cleanup lists the whole table location and is slow on a small host.",
    )
    return parser.parse_args()


def timestamp_before(days: int, now: datetime | None = None) -> str:
    """Iceberg's procedures take a literal timestamp, not an interval."""
    now = now or datetime.now(timezone.utc)
    return (now - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S.000")


def month_start(year: int, month: int) -> datetime:
    return datetime(year, month, 1, tzinfo=timezone.utc)


def next_month(year: int, month: int) -> tuple[int, int]:
    return (year + 1, 1) if month == 12 else (year, month + 1)


def month_end_snapshots(
    history: list[tuple[datetime, int]], now: datetime
) -> list[tuple[int, int, int]]:
    """The snapshot `main` pointed at when each finished month ended, as (year, month, id).

    `history` is the table's snapshot log: when each snapshot became current. A month counts once
    it is over (UTC). The first month is the one the oldest surviving entry falls in; a month
    before that cannot be answered, because the snapshot that was current then is already gone.
    A month with no commit of its own ends on the snapshot the previous month ended on.
    """
    if not history:
        return []
    ordered = sorted(history)
    year, month = ordered[0][0].year, ordered[0][0].month
    result: list[tuple[int, int, int]] = []
    while month_start(*next_month(year, month)) <= now:
        boundary = month_start(*next_month(year, month))
        current = [snapshot_id for made_current_at, snapshot_id in ordered if made_current_at < boundary]
        result.append((year, month, current[-1]))
        year, month = next_month(year, month)
    return result


def months_ago(year: int, month: int, now: datetime) -> int:
    """How many month boundaries lie between that month and the month `now` falls in."""
    return (now.year * 12 + now.month) - (year * 12 + month)


def month_of_tag(name: str, name_format: str) -> tuple[int, int] | None:
    """The (year, month) a month-end tag names, or None for any other reference."""
    try:
        parsed = datetime.strptime(name, name_format)
    except ValueError:
        return None
    if parsed.strftime(name_format) != name:
        return None
    return parsed.year, parsed.month


def retention_tags(
    history: list[tuple[datetime, int]], existing_refs: set[str], expiry: dict[str, Any], now: datetime
) -> tuple[list[tuple[str, int]], list[str]]:
    """The tags to create and the month-end tags to drop: grandfather-father-son (root ADR-0237).

    Every finished month inside the monthly window gets a month-end tag, and every finished year a
    year-end tag on December's month-end snapshot. A month-end tag older than `keep_months` is
    dropped; a year-end tag never is. An existing tag is never moved or recreated.

    The window is counted by the month the tag names, not by the age of its snapshot. Iceberg's own
    `max-ref-age-ms` (`RETAIN`) counts from the snapshot's commit, so a table untouched since
    January would lose its September tag the next January, four months in.
    """
    monthly = expiry["month_end_tag"]
    yearly_format = expiry["year_end_tag"]["name_format"]
    create = []
    for year, month, snapshot_id in month_end_snapshots(history, now):
        names = []
        if months_ago(year, month, now) <= monthly["keep_months"]:
            names.append(month_start(year, month).strftime(monthly["name_format"]))
        if month == 12:
            names.append(month_start(year, month).strftime(yearly_format))
        create.extend((name, snapshot_id) for name in names if name not in existing_refs)
    drop = []
    for name in sorted(existing_refs):
        named = month_of_tag(name, monthly["name_format"])
        if named is not None and months_ago(*named, now) > monthly["keep_months"]:
            drop.append(name)
    return create, drop


def create_tag_sql(catalog: str, table: str, name: str, snapshot_id: int) -> str:
    """A tag with no `RETAIN` clause has no `max-ref-age-ms`: only this job's window removes it."""
    return (
        f"ALTER TABLE {catalog}.{table} CREATE TAG IF NOT EXISTS `{name}` "
        f"AS OF VERSION {snapshot_id}"
    )


def drop_tag_sql(catalog: str, table: str, name: str) -> str:
    return f"ALTER TABLE {catalog}.{table} DROP TAG IF EXISTS `{name}`"


def expire_snapshots_sql(catalog: str, table: str, cutoff: str, retain_last: int) -> str:
    """The one expiry call this lakehouse makes.

    Iceberg's `expire_snapshots` never removes a snapshot a live reference names: a branch, or a
    tag without `max-ref-age-ms`. The by-PNU publish tags the Gold snapshot each serving manifest
    reflects (`served-<lane>-...`, root ADR-0146 §2), so that snapshot outlives this call for as
    long as the manifest serves it; the month-end and year-end tags (root ADR-0237) are kept the
    same way, until `run_snapshot_expiry` drops a month-end tag past its window. Nothing here may
    pass options that drop references.
    """
    return (
        f"CALL {catalog}.system.expire_snapshots(table => '{table}', "
        f"older_than => TIMESTAMP '{cutoff}', retain_last => {retain_last})"
    )


def run_snapshot_expiry(
    spark: Any, catalog: str, table: str, expiry: dict[str, Any], now: datetime, dry_run: bool
) -> None:
    """Tag month and year ends, drop month-end tags past their window, then expire.

    The tags come first and in the same run: a month-end snapshot is pinned before the expiry
    that would otherwise remove it, so a month is lost only if this job does not run at all for
    `retain_days` after the month ends.
    """
    qualified = f"{catalog}.{table}"
    history = [
        (datetime.fromtimestamp(row.ms / 1000, tz=timezone.utc), int(row.snapshot_id))
        for row in spark.sql(
            f"SELECT unix_millis(made_current_at) AS ms, snapshot_id FROM {qualified}.history"
        ).collect()
    ]
    refs = {row.name for row in spark.sql(f"SELECT name FROM {qualified}.refs").collect()}
    alive = {
        int(row.snapshot_id)
        for row in spark.sql(f"SELECT snapshot_id FROM {qualified}.snapshots").collect()
    }
    create, drop = retention_tags(history, refs, expiry, now)
    for name, snapshot_id in create:
        if snapshot_id not in alive:
            # The month ended on a snapshot that is already gone; tagging another would record a
            # state the table was never in at that month's end.
            print(f"maintenance-skip step=retention_tag tag={name} reason=snapshot_expired", flush=True)
            continue
        if dry_run:
            print(f"maintenance-would step=retention_tag tag={name} snapshot={snapshot_id}", flush=True)
        else:
            spark.sql(create_tag_sql(catalog, table, name, snapshot_id)).collect()
            print(f"maintenance-done step=retention_tag tag={name} snapshot={snapshot_id}", flush=True)
    for name in drop:
        if dry_run:
            print(f"maintenance-would step=drop_month_end_tag tag={name}", flush=True)
        else:
            spark.sql(drop_tag_sql(catalog, table, name)).collect()
            print(f"maintenance-done step=drop_month_end_tag tag={name}", flush=True)

    cutoff = timestamp_before(expiry["retain_days"], now)
    if dry_run:
        print(f"maintenance-would step=snapshot_expiry older_than={cutoff}", flush=True)
        return
    result = spark.sql(expire_snapshots_sql(catalog, table, cutoff, expiry["retain_last"])).collect()[0]
    print(
        f"maintenance-done step=snapshot_expiry data_files={result[0]} "
        f"manifests={result[2]} older_than={cutoff}",
        flush=True,
    )


def file_shape(spark: Any, qualified: str) -> tuple[int, int]:
    row = spark.sql(f"SELECT count(*) c, coalesce(sum(file_size_in_bytes), 0) b FROM {qualified}.files").collect()[0]
    return int(row.c), int(row.b)


def small_file_count(spark: Any, qualified: str, target_bytes: int, fraction: float) -> int:
    """How many files sit under a fraction of the target size.

    Compaction rewrites every byte it reads, so it has to be worth doing. Counting the files
    that are actually small is the cheapest signal available — it comes from the manifest,
    without opening a single data file.
    """
    threshold = int(target_bytes * fraction)
    row = spark.sql(
        f"SELECT count(*) c FROM {qualified}.files WHERE file_size_in_bytes < {threshold}"
    ).collect()[0]
    return int(row.c)


def should_compact(small_files: int, total_files: int, trigger_count: int) -> bool:
    """Compact when enough files are small enough to be worth rewriting.

    A single small file is normal — the last file of any write is a remainder. The question is
    whether there are enough of them that reading the table pays the per-file cost repeatedly.
    """
    return small_files >= trigger_count and total_files > 1


def main() -> int:
    args = parse_args()
    contract = load_maintenance_contract()

    from pyspark.sql import SparkSession  # noqa: PLC0415 - deferred on purpose, see module docstring

    spark = SparkSession.builder.appName(f"lakehouse-maintenance-{args.table}").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    qualified = f"{args.catalog}.{args.table}"
    compaction = contract["compaction"]
    expiry = contract["snapshot_expiry"]
    cleanup = contract["orphan_cleanup"]

    before_files, before_bytes = file_shape(spark, qualified)
    small = small_file_count(
        spark, qualified, compaction["target_file_bytes"], compaction["trigger_small_file_fraction"]
    )
    print(
        f"maintenance-before table={args.table} files={before_files} "
        f"bytes={before_bytes} small_files={small}",
        flush=True,
    )

    if "compaction" in args.skip:
        print("maintenance-skip step=compaction", flush=True)
    elif not should_compact(small, before_files, compaction["trigger_small_file_count"]):
        print(
            f"maintenance-skip step=compaction reason=below_trigger "
            f"small_files={small} trigger={compaction['trigger_small_file_count']}",
            flush=True,
        )
    elif args.dry_run:
        print(f"maintenance-would step=compaction small_files={small}", flush=True)
    else:
        result = spark.sql(
            f"CALL {args.catalog}.system.rewrite_data_files(table => '{args.table}', "
            f"options => map("
            f"'min-input-files','{compaction['min_input_files']}',"
            f"'target-file-size-bytes','{compaction['target_file_bytes']}'))"
        ).collect()[0]
        print(f"maintenance-done step=compaction read={result[0]} wrote={result[1]}", flush=True)

    if "snapshot_expiry" in args.skip:
        print("maintenance-skip step=snapshot_expiry", flush=True)
    else:
        run_snapshot_expiry(
            spark, args.catalog, args.table, expiry, datetime.now(timezone.utc), args.dry_run
        )

    if "orphan_cleanup" in args.skip:
        print("maintenance-skip step=orphan_cleanup", flush=True)
    else:
        cutoff = timestamp_before(cleanup["safety_days"])
        rows = spark.sql(
            f"CALL {args.catalog}.system.remove_orphan_files(table => '{args.table}', "
            f"older_than => TIMESTAMP '{cutoff}', dry_run => {str(args.dry_run).lower()})"
        ).collect()
        verb = "would" if args.dry_run else "done"
        print(
            f"maintenance-{verb} step=orphan_cleanup files={len(rows)} older_than={cutoff}",
            flush=True,
        )

    after_files, after_bytes = file_shape(spark, qualified)
    print(
        f"maintenance-after table={args.table} files={before_files}->{after_files} "
        f"bytes={before_bytes}->{after_bytes}",
        flush=True,
    )
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
