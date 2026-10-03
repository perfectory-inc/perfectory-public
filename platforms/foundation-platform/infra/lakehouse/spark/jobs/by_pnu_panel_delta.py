#!/usr/bin/env python3
"""Name the PNUs a by-PNU serving patch must carry (root ADR-0141 §3, ADR-0099).

The Gold panels are rebuilt whole, so Iceberg's incremental read answers "what changed between
snapshots" with "everything". The real question is content: this job joins two snapshots of
`gold.parcel_panel` or `gold.building_panel` on PNU and compares `row_digest` — the fingerprint of
exactly the columns the serving document is built from, lineage excluded. Changed and new PNUs
are the patch's upserts; PNUs gone from Gold are its tombstones.

The comparison snapshot is the one the serving manifest last reflected
(`reflected_gold_iceberg_snapshot_id`), never a guess. Designed refusals, each a distinct exit:

* no comparison snapshot (none named, or Iceberg no longer has it) — exit 3. That is not
  "nothing changed": the change set is unknown, and only a full bake can say what Gold holds.
* a snapshot whose rows lack `row_digest` — exit 3, for the same reason.
* a change set larger than the contract's `max_delta_fraction` of the current table — exit 4.
  That shape means the fingerprint drifted or the baseline is wrong; re-baking everything as a
  "patch" would hide it (ADR-0099 §5).

Writes nothing to the lakehouse: two PNU lists and a run summary, all local files.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

JOB_NAME = "by_pnu_panel_delta"
RUN_SUMMARY_SCHEMA_VERSION = "foundation-platform.spark_run_summary.v1"
GOLD_TABLES = {"parcel": "gold.parcel_panel", "building": "gold.building_panel"}
EXIT_NO_COMPARISON = 3
EXIT_NOT_A_DELTA = 4


class Refusal(Exception):
    """A change set this job will not name; `exit_code` tells the bake which one."""

    def __init__(self, exit_code: int, message: str) -> None:
        super().__init__(message)
        self.exit_code = exit_code


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--unit", required=True, choices=sorted(GOLD_TABLES))
    parser.add_argument(
        "--baseline-snapshot-id",
        default="",
        help="The snapshot the serving manifest last reflected; empty is a refusal.",
    )
    parser.add_argument("--current-snapshot-id", required=True)
    parser.add_argument(
        "--max-delta-fraction",
        required=True,
        type=float,
        help="by_pnu_serving_patches.max_delta_fraction of config/r2-connections.contract.json.",
    )
    parser.add_argument("--iceberg-catalog-name", default="r2")
    parser.add_argument("--upsert-output", required=True, help="One changed-or-new PNU per line.")
    parser.add_argument("--delete-output", required=True, help="One deleted PNU per line.")
    parser.add_argument("--summary-output", required=True)
    return parser.parse_args(argv)


def check_snapshot_ids(baseline: str, current: str) -> None:
    if not baseline.strip():
        raise Refusal(
            EXIT_NO_COMPARISON,
            "no comparison snapshot was named: the change set is unknown, which is not 'no change'; "
            "the serving lane needs a full bake (ADR-0141)",
        )
    for label, value in (("baseline", baseline), ("current", current)):
        if not value.isdigit():
            raise Refusal(EXIT_NO_COMPARISON, f"{label} snapshot id {value!r} is not an Iceberg snapshot id")
    if baseline == current:
        raise Refusal(
            EXIT_NO_COMPARISON,
            f"baseline and current both name snapshot {current}; there is nothing to compare",
        )


def check_delta_fraction(fraction: float) -> None:
    if not 0 < fraction <= 1:
        raise ValueError(f"--max-delta-fraction must be in (0, 1], got {fraction}")


def judge(counts: dict[str, int], max_delta_fraction: float) -> dict[str, int]:
    """The verdict on one comparison's counts; refuses an empty table and a non-delta."""
    changed, new = counts.get("changed", 0), counts.get("new", 0)
    deleted, same = counts.get("deleted", 0), counts.get("same", 0)
    current_total = changed + new + same
    if current_total == 0:
        raise Refusal(EXIT_NOT_A_DELTA, "the current snapshot is empty; that is not a delta")
    changes = changed + new + deleted
    if changes > current_total * max_delta_fraction:
        raise Refusal(
            EXIT_NOT_A_DELTA,
            f"{changes} of {current_total} PNUs differ, more than the contract's "
            f"max_delta_fraction {max_delta_fraction} — that is not a delta. The fingerprint "
            "definition or the baseline is wrong; nothing is patched (ADR-0141 §3, ADR-0099 §5)",
        )
    return {
        "changed_count": changed,
        "new_count": new,
        "deleted_count": deleted,
        "unchanged_count": same,
        "current_total": current_total,
        "upsert_count": changed + new,
        "delete_count": deleted,
    }


def classify(baseline: Any, current: Any) -> Any:
    """Each PNU of either side, with its verdict: new, deleted, changed or same."""
    from pyspark.sql import functions as F

    joined = baseline.alias("b").join(current.alias("c"), on="pnu", how="full_outer")
    return joined.select(
        F.col("pnu"),
        F.when(F.col("b.present").isNull(), F.lit("new"))
        .when(F.col("c.present").isNull(), F.lit("deleted"))
        .when(F.col("b.row_digest") != F.col("c.row_digest"), F.lit("changed"))
        .otherwise(F.lit("same"))
        .alias("verdict"),
    )


def snapshot_frame(spark: Any, catalog: str, table: str, snapshot_id: str) -> Any:
    from pyspark.sql import functions as F

    known = spark.sql(
        f"SELECT 1 FROM `{catalog}`.{table}.snapshots WHERE snapshot_id = {int(snapshot_id)}"
    ).limit(1).collect()
    if not known:
        raise Refusal(
            EXIT_NO_COMPARISON,
            f"{table} no longer has snapshot {snapshot_id} (expired or never existed); the change "
            "set against it is unknown, which is not 'no change' — the lane needs a full bake",
        )
    frame = spark.sql(f"SELECT * FROM `{catalog}`.{table} VERSION AS OF {int(snapshot_id)}")
    if "row_digest" not in frame.columns:
        raise Refusal(
            EXIT_NO_COMPARISON,
            f"snapshot {snapshot_id} of {table} carries no row_digest; a digest-less snapshot "
            "cannot be compared — full-bake it (ADR-0099)",
        )
    return frame.select("pnu", "row_digest", F.lit(True).alias("present"))


def refuse_missing_digests(frame: Any, label: str) -> None:
    from pyspark.sql import functions as F

    missing = frame.where(F.col("row_digest").isNull()).limit(1).count()
    if missing:
        raise Refusal(
            EXIT_NO_COMPARISON,
            f"the {label} snapshot has rows without row_digest; they cannot be compared",
        )


def write_pnus(frame: Any, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        for row in frame.orderBy("pnu").toLocalIterator():
            handle.write(f"{row.pnu}\n")


def compare(spark: Any, args: argparse.Namespace) -> dict[str, Any]:
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    table = GOLD_TABLES[args.unit]
    baseline = snapshot_frame(spark, args.iceberg_catalog_name, table, args.baseline_snapshot_id)
    current = snapshot_frame(spark, args.iceberg_catalog_name, table, args.current_snapshot_id)
    refuse_missing_digests(baseline, "baseline")
    refuse_missing_digests(current, "current")
    # One join, kept on disk: the counts and both lists read it instead of joining again.
    classified = classify(baseline, current).persist(StorageLevel.DISK_ONLY)
    counts = {row["verdict"]: int(row["count"]) for row in classified.groupBy("verdict").count().collect()}
    metrics = judge(counts, args.max_delta_fraction)
    write_pnus(classified.where(F.col("verdict").isin("changed", "new")).select("pnu"), args.upsert_output)
    write_pnus(classified.where(F.col("verdict") == "deleted").select("pnu"), args.delete_output)
    classified.unpersist()
    return metrics


def write_summary(path: str, summary: dict[str, Any]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    check_delta_fraction(args.max_delta_fraction)
    try:
        check_snapshot_ids(args.baseline_snapshot_id, args.current_snapshot_id)
    except Refusal as refusal:
        print(f"{JOB_NAME}-refused: {refusal}")
        return refusal.exit_code

    from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages
    from pyspark.sql import SparkSession

    assert_catalog_env(args.iceberg_catalog_name)
    started = time.monotonic()
    builder = (
        SparkSession.builder.appName(JOB_NAME)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", iceberg_packages())
        .config("spark.ui.enabled", "false")
    )
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    try:
        metrics = compare(spark, args)
    except Refusal as refusal:
        print(f"{JOB_NAME}-refused: {refusal}")
        spark.stop()
        return refusal.exit_code

    table = GOLD_TABLES[args.unit]
    write_summary(args.summary_output, {
        "schema_version": RUN_SUMMARY_SCHEMA_VERSION,
        "job_name": JOB_NAME,
        "contract": table,
        "row_count": metrics["upsert_count"],
        "quality_metrics": metrics,
        "input": {
            "unit": args.unit,
            "baseline_snapshot_id": args.baseline_snapshot_id,
            "current_snapshot_id": args.current_snapshot_id,
            "max_delta_fraction": args.max_delta_fraction,
        },
        "wall_seconds": round(time.monotonic() - started, 1),
    })
    print(
        f"{JOB_NAME}-ok unit={args.unit} changed={metrics['changed_count']} new={metrics['new_count']} "
        f"deleted={metrics['deleted_count']} unchanged={metrics['unchanged_count']} "
        f"baseline={args.baseline_snapshot_id} current={args.current_snapshot_id}"
    )
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
