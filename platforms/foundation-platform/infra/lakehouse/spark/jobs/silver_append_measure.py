"""Measures one Silver release appended as changes, in a scratch namespace (root ADR-0275 step 1).

The live table is only read: the release before (`--old-snapshot-id`, time travel) and the current
one. Writes go only to `<catalog>.<scratch namespace>`, which must start with `scratch`. Writing a
release whole was measured by the first run (2026-10-18: 131,464,796 rows, 1,442 MB in about 3.5 minutes),
so this run times only the other path, phase by phase:

  copy     the old release, copied as the table the new release lands on;
  compare  one shuffle of row fingerprints (old -1, new +1): the fingerprints whose count differs;
  append   the new release's rows of a fingerprint it has more of, and one removal marker per old row
           of a fingerprint it has fewer of, appended once (ADR-0275 §1).

Each phase prints `silver-append-measure-phase {json}` when it starts and ends (UTC time, rows,
seconds), so a run stopped by its limit still leaves its numbers in the journal. The last line is
`silver-append-measure {json}`. The scratch tables and namespace are dropped whatever happens.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any

from lakehouse_engine import apply_catalog_settings, iceberg_packages

JOB_NAME = "silver-append-measure"
# The columns a row's content is, per table: the fingerprint reads these and nothing else. Not "every
# column but a list": the run of 2026-10-18 05:01 counted every row as changed because the release's
# own date (data_reference_date) differs on every row, and a column a list forgets does the same.
CONTENT_COLUMNS: dict[str, tuple[str, ...]] = {
    "land_transfer_history": (
        "pnu", "legal_dong_code", "legal_dong_name", "ledger_kind_code", "ledger_kind_name", "jibun",
        "transfer_history_seq", "closure_seq", "land_category_code", "land_category", "area_m2", "reason_code",
        "reason", "moved_at", "erased_at", "parcel_history_seq", "source_sigungu_code",
    ),
}
SCRATCH = re.compile(r"^scratch[a-z0-9_]*$")
NAME = re.compile(r"^[a-z][a-z0-9_]*$")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--table", default="land_transfer_history")
    parser.add_argument("--source-namespace", default="silver")
    parser.add_argument("--old-snapshot-id", required=True, help="The live table's snapshot holding the release before")
    parser.add_argument("--scratch-namespace", default="scratch_adr0275")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    args = parser.parse_args(argv)
    check_args(args)
    return args


def check_args(args: argparse.Namespace) -> None:
    """Writes only ever go to a scratch namespace, and every name is a plain identifier."""
    if not SCRATCH.fullmatch(args.scratch_namespace):
        raise ValueError(f"--scratch-namespace must start with 'scratch': {args.scratch_namespace!r}")
    for value in (args.table, args.source_namespace, args.iceberg_catalog_name):
        if not NAME.fullmatch(value):
            raise ValueError(f"not a plain identifier: {value!r}")
    if args.source_namespace == args.scratch_namespace:
        raise ValueError("the scratch namespace cannot be the source namespace")
    if args.table not in CONTENT_COLUMNS:
        raise ValueError(f"{args.table} declares no content columns; known: {sorted(CONTENT_COLUMNS)}")
    if not args.old_snapshot_id.isdigit():
        raise ValueError(f"--old-snapshot-id must be a snapshot id: {args.old_snapshot_id!r}")


def content_columns(table: str, old: list[str], new: list[str]) -> list[str]:
    """The table's declared content columns; both releases must hold every one of them."""
    declared = list(CONTENT_COLUMNS[table])
    missing = sorted({c for c in declared if c not in old or c not in new})
    if missing:
        raise ValueError(f"{table}: a release lacks declared content columns {missing}")
    return declared


def written(spark: Any, table: str) -> dict[str, int]:
    """Rows and bytes the table's newest snapshot added."""
    summary = spark.sql(f"SELECT summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()["summary"]
    return {"rows": int(summary.get("added-records", 0)), "bytes": int(summary.get("added-files-size", 0))}


def phase(name: str, event: str, **values: Any) -> None:
    """One journal line for a phase boundary."""
    at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print("silver-append-measure-phase " + json.dumps({"phase": name, "event": event, "at": at, **values},
                                                      sort_keys=True), flush=True)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    from pyspark.sql import SparkSession  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415
    from pyspark.storagelevel import StorageLevel  # noqa: PLC0415

    builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).config("spark.jars.packages", args.iceberg_packages).getOrCreate()
    cat = f"`{args.iceberg_catalog_name}`"
    scratch = f"{cat}.`{args.scratch_namespace}`"
    source = f"{cat}.`{args.source_namespace}`.`{args.table}`"
    appended = f"{scratch}.`{args.table}_append`"

    def drop() -> None:
        spark.sql(f"DROP TABLE IF EXISTS {appended} PURGE")

    report: dict[str, Any] = {"table": f"{args.source_namespace}.{args.table}", "old_snapshot_id": args.old_snapshot_id}
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {scratch}")
        drop()
        old = spark.read.option("snapshot-id", args.old_snapshot_id).table(source.replace("`", ""))
        new = spark.table(source.replace("`", ""))
        shared = [c for c in new.columns if c in old.columns]
        content = content_columns(args.table, old.columns, new.columns)
        old, new = old.select(*shared), new.select(*shared)
        report["content_columns"] = content

        started = time.monotonic()
        phase("copy", "start")
        old.withColumn("row_state", F.lit("present")).writeTo(appended.replace("`", "")).using("iceberg").create()
        report["copy"] = {**written(spark, appended), "seconds": round(time.monotonic() - started, 1)}
        phase("copy", "end", **report["copy"])

        started = time.monotonic()
        phase("compare", "start")
        hashed = [*(F.col(c).isNull() for c in content), *(F.col(c) for c in content)]
        fingerprint = F.xxhash64(*hashed)
        current = spark.table(appended.replace("`", "")).where(F.col("row_state") == "present").select(*shared)
        net = (current.select(fingerprint.alias("_fp"), F.lit(-1).alias("_d"))
               .unionByName(new.select(fingerprint.alias("_fp"), F.lit(1).alias("_d")))
               .groupBy("_fp").agg(F.sum("_d").alias("_n")).where(F.col("_n") != 0)
               .persist(StorageLevel.DISK_ONLY))
        counts = net.agg(F.sum(F.when(F.col("_n") > 0, F.col("_n")).otherwise(0)).alias("added"),
                         F.sum(F.when(F.col("_n") < 0, -F.col("_n")).otherwise(0)).alias("removed")).first()
        report["compare"] = {"added_rows": int(counts["added"] or 0), "removed_rows": int(counts["removed"] or 0),
                             "seconds": round(time.monotonic() - started, 1)}
        phase("compare", "end", **report["compare"])

        started = time.monotonic()
        phase("append", "start")
        more, fewer = net.where(F.col("_n") > 0).select("_fp"), net.where(F.col("_n") < 0).select("_fp")
        added = new.withColumn("_fp", fingerprint).join(more, "_fp", "left_semi").drop("_fp")
        removed = current.withColumn("_fp", fingerprint).join(fewer, "_fp", "left_semi").drop("_fp")
        delta = added.withColumn("row_state", F.lit("present")).unionByName(
            removed.withColumn("row_state", F.lit("removed")))
        delta.writeTo(appended.replace("`", "")).append()
        report["append"] = {**written(spark, appended), "seconds": round(time.monotonic() - started, 1)}
        phase("append", "end", **report["append"])
        net.unpersist()
        print("silver-append-measure " + json.dumps(report, sort_keys=True), flush=True)
        return 0
    finally:
        drop()
        spark.sql(f"DROP NAMESPACE IF EXISTS {scratch}")
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
