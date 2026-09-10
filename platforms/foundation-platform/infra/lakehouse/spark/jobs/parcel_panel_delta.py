#!/usr/bin/env python3
"""Name the parcels whose serving documents must be re-baked (root ADR-0099).

The Gold panel is rebuilt whole every day, so Iceberg's incremental read answers "what
changed between snapshots" with "everything". The real question is content: this job joins
two snapshots of `gold.parcel_panel` on PNU and compares `row_digest` — the fingerprint of
exactly the columns the serving document is built from, lineage excluded. Changed and new
PNUs land in an allowlist file the by-PNU serving export consumes directly; deleted PNUs
land in their own file (objects are not deleted inside a generation — a generation swap
absorbs deletions).

Two designed refusals:

* A baseline without `row_digest` is not comparable — the job says so instead of treating
  every row as changed.
* A delta larger than half the table is not a delta. That shape means the fingerprint
  definition drifted (or the baseline is wrong), and re-baking everything through the
  daily lane would hide it; the full-bake procedure exists for that day.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages

JOB_NAME = "parcel_panel_delta"
RUN_SUMMARY_SCHEMA_VERSION = "foundation-platform.spark_run_summary.v1"
GOLD_TABLE = "gold.parcel_panel"
MAX_DELTA_FRACTION = 0.5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--baseline-snapshot-id",
        required=True,
        help="Iceberg snapshot the current serving generation was baked from.",
    )
    parser.add_argument(
        "--current-snapshot-id",
        default=None,
        help="Snapshot to compare against; the table's current snapshot when omitted.",
    )
    parser.add_argument(
        "--iceberg-catalog-name",
        default="r2",
        help="Spark catalog name for Iceberg REST catalog reads.",
    )
    parser.add_argument(
        "--allowlist-output",
        required=True,
        help="File that receives one changed-or-new PNU per line.",
    )
    parser.add_argument(
        "--deleted-output",
        required=True,
        help="File that receives one deleted PNU per line.",
    )
    parser.add_argument(
        "--summary-output",
        required=True,
        help="File that receives the run summary JSON.",
    )
    return parser.parse_args()


def snapshot_frame(spark: SparkSession, catalog: str, snapshot_id: str) -> DataFrame:
    frame = spark.sql(
        f"SELECT pnu, row_digest FROM `{catalog}`.{GOLD_TABLE} VERSION AS OF {int(snapshot_id)}"
    )
    if "row_digest" not in frame.columns:
        raise ValueError(
            f"snapshot {snapshot_id} of {GOLD_TABLE} carries no row_digest; "
            "a digest-less snapshot cannot be compared — full-bake it first (ADR-0099)"
        )
    return frame


def main() -> int:
    args = parse_args()
    assert_catalog_env(args.iceberg_catalog_name)
    builder = (
        SparkSession.builder.appName(JOB_NAME)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", iceberg_packages())
        .config("spark.ui.enabled", "false")
    )
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()

    current_snapshot_id = args.current_snapshot_id
    if current_snapshot_id is None:
        row = spark.sql(
            f"SELECT snapshot_id FROM `{args.iceberg_catalog_name}`.{GOLD_TABLE}.snapshots "
            "ORDER BY committed_at DESC LIMIT 1"
        ).first()
        if row is None:
            raise ValueError(f"{GOLD_TABLE} has no snapshot to compare")
        current_snapshot_id = str(row.snapshot_id)
    if str(current_snapshot_id) == str(args.baseline_snapshot_id):
        raise ValueError(
            "baseline and current name the same snapshot; there is nothing to compare"
        )

    baseline = snapshot_frame(spark, args.iceberg_catalog_name, args.baseline_snapshot_id)
    current = snapshot_frame(spark, args.iceberg_catalog_name, current_snapshot_id)

    joined = baseline.alias("b").join(current.alias("c"), on="pnu", how="full_outer")
    classified = joined.select(
        F.col("pnu"),
        F.when(F.col("b.row_digest").isNull(), F.lit("new"))
        .when(F.col("c.row_digest").isNull(), F.lit("deleted"))
        .when(F.col("b.row_digest") != F.col("c.row_digest"), F.lit("changed"))
        .otherwise(F.lit("same"))
        .alias("verdict"),
    )

    counts = {
        row["verdict"]: int(row["count"])
        for row in classified.groupBy("verdict").count().collect()
    }
    changed = counts.get("changed", 0)
    new = counts.get("new", 0)
    deleted = counts.get("deleted", 0)
    same = counts.get("same", 0)
    current_total = changed + new + same

    if current_total == 0:
        raise ValueError(f"snapshot {current_snapshot_id} of {GOLD_TABLE} is empty")
    if (changed + new) > current_total * MAX_DELTA_FRACTION:
        raise ValueError(
            f"{changed + new} of {current_total} parcels differ — that is not a delta. "
            "The fingerprint definition or the baseline is wrong; "
            "use the full-bake procedure instead (ADR-0099)"
        )

    def write_pnus(path: str, verdict: str) -> None:
        rows = classified.where(F.col("verdict") == verdict).select("pnu")
        with open(path, "w", encoding="utf-8", newline="") as handle:
            for row in rows.toLocalIterator():
                handle.write(f"{row.pnu}\n")

    write_pnus(args.allowlist_output, "changed")
    with open(args.allowlist_output, "a", encoding="utf-8", newline="") as handle:
        for row in (
            classified.where(F.col("verdict") == "new").select("pnu").toLocalIterator()
        ):
            handle.write(f"{row.pnu}\n")
    write_pnus(args.deleted_output, "deleted")

    summary = {
        "schema_version": RUN_SUMMARY_SCHEMA_VERSION,
        "job_name": JOB_NAME,
        "contract": GOLD_TABLE,
        "row_count": changed + new,
        "quality_metrics": {
            "changed_count": changed,
            "new_count": new,
            "deleted_count": deleted,
            "unchanged_count": same,
            "current_total": current_total,
        },
        "input": {
            "baseline_snapshot_id": str(args.baseline_snapshot_id),
            "current_snapshot_id": str(current_snapshot_id),
        },
    }
    Path(args.summary_output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.summary_output, "w", encoding="utf-8", newline="") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)
        handle.write("\n")

    print(
        f"parcel-panel-delta-ok changed={changed} new={new} deleted={deleted} "
        f"unchanged={same} baseline={args.baseline_snapshot_id} current={current_snapshot_id}"
    )
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
