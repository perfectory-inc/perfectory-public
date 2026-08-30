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

    if contract["snapshot_expiry"]["retain_last"] < 1:
        raise ValueError("snapshot_expiry.retain_last must keep at least the current snapshot")


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


def timestamp_before(days: int) -> str:
    """Iceberg's procedures take a literal timestamp, not an interval."""
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S.000")


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
        cutoff = timestamp_before(expiry["retain_days"])
        if args.dry_run:
            print(f"maintenance-would step=snapshot_expiry older_than={cutoff}", flush=True)
        else:
            result = spark.sql(
                f"CALL {args.catalog}.system.expire_snapshots(table => '{args.table}', "
                f"older_than => TIMESTAMP '{cutoff}', retain_last => {expiry['retain_last']})"
            ).collect()[0]
            print(
                f"maintenance-done step=snapshot_expiry data_files={result[0]} "
                f"manifests={result[2]} older_than={cutoff}",
                flush=True,
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
