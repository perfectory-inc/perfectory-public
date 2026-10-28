#!/usr/bin/env python3
"""Name the PNUs a by-PNU serving patch must carry (root ADR-0141 §3, ADR-0099).

The question is content: which PNUs' `row_digest` — the fingerprint of exactly the columns the
serving document is built from, lineage excluded — differs between two snapshots of a lane's Gold.
Changed and new PNUs are the patch's upserts; PNUs gone from Gold are its tombstones.

An incremental Gold commit records the PNUs it merged (`gold.<t>__changes`, root ADR-0191 §4,
ADR-0312 §1). When every commit between the two snapshots is recorded (or a compaction), only those
PNUs are compared: both snapshots are semi-joined to them, so nothing is shuffled but the recorded
keys, and a run with no recorded change reads no row. A step without a record (a full rebuild, a
schema backfill) falls back to joining the two snapshots whole. Both paths give the same counts.

The comparison snapshot is the one the serving manifest last reflected
(`reflected_gold_iceberg_snapshot_id`), never a guess. Designed refusals, each a distinct exit:

* no comparison snapshot (none named, or Iceberg no longer has it) — exit 3. That is not
  "nothing changed": the change set is unknown, and only a re-base can say what Gold holds.
* a snapshot whose rows lack `row_digest` (written before the fingerprint existed) — exit 5, for
  the same reason. The bake names the two apart: one is an expiry, the other a pre-ADR-0099
  snapshot (root ADR-0146).
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
R2_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "config" / "r2-connections.contract.json"
# The parcel and building lanes, and every pack-only lane the R2 contract names (root ADR-0190; a
# kind line's Gold is gold.<lane with - as _>, root ADR-0193): each lane's Gold panel is one row per
# PNU with a row_digest, so one comparison serves them all.
_LANES = json.loads(R2_CONTRACT_PATH.read_text(encoding="utf-8")).get("by_pnu_lanes", {})
GOLD_TABLES = {"parcel": "gold.parcel_panel", "building": "gold.building_panel",
               **{name: lane.get("gold_table") or "gold." + name.replace("-", "_") for name, lane in _LANES.items()}}
# A lane keyed narrower than a PNU (root ADR-0204) names the Gold column that follows the PNU in its
# document key; its change set lists keys (PNU then that column's value), which the bake's upserts
# and tombstones are by.
KEY_SUFFIX_COLUMNS = {name: lane["document_key_suffix"]["column"] for name, lane in _LANES.items()
                      if lane.get("document_key_suffix")}
EXIT_NO_COMPARISON = 3
EXIT_NOT_A_DELTA = 4
EXIT_NO_DIGEST = 5


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


def snapshot_frame(spark: Any, catalog: str, table: str, snapshot_id: str,
                   key_suffix_column: str | None = None, pnus: Any | None = None) -> Any:
    """The snapshot's rows as (pnu, row_digest, present), `pnu` being the lane's document key; only
    the rows of `pnus` (a list of PNUs, pushed into the scan, or a frame of `pnu`) when given."""
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
            EXIT_NO_DIGEST,
            f"snapshot {snapshot_id} of {table} carries no row_digest; a digest-less snapshot "
            "cannot be compared — re-base the lane (ADR-0099, ADR-0146)",
        )
    if isinstance(pnus, list):
        frame = frame.where(F.col("pnu").isin(pnus))
    elif pnus is not None:
        frame = frame.join(pnus, "pnu", "left_semi")
    key = F.col("pnu") if key_suffix_column is None else F.concat(F.col("pnu"), F.col(key_suffix_column))
    return frame.select(key.alias("pnu"), "row_digest", F.lit(True).alias("present"))


def refuse_missing_digests(frame: Any, label: str) -> None:
    from pyspark.sql import functions as F

    missing = frame.where(F.col("row_digest").isNull()).limit(1).count()
    if missing:
        raise Refusal(
            EXIT_NO_DIGEST,
            f"the {label} snapshot has rows without row_digest; they cannot be compared",
        )


def write_pnus(frame: Any, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        for row in frame.orderBy("pnu").toLocalIterator():
            handle.write(f"{row.pnu}\n")


def unchanged_rows(baseline: str | None, current: str | None) -> int | None:
    """The row count when both snapshots record the same content fingerprint, else None."""
    import gold_fingerprint

    if baseline is None or current is None or baseline != current:
        return None
    rows = baseline.split(";", 1)[0]
    if not rows.startswith("rows:") or not rows[5:].isdigit():
        raise ValueError(f"unreadable {gold_fingerprint.PROPERTY} {baseline!r}")
    return int(rows[5:])


def same_content(spark: Any, table: str, baseline_id: str, current_id: str) -> int | None:
    """unchanged_rows of the two snapshots' recorded fingerprints (a missing snapshot is left to the scan's refusal)."""
    import gold_fingerprint

    return unchanged_rows(gold_fingerprint.recorded(spark, table, baseline_id),
                          gold_fingerprint.recorded(spark, table, current_id))


def unchanged_rest(current_rows: int, counts: dict[str, int]) -> int:
    """The current rows outside the recorded PNUs, with the same verdict among them (`same`)."""
    rest = current_rows - counts.get("changed", 0) - counts.get("new", 0)
    if rest < counts.get("same", 0):
        raise ValueError(f"the current snapshot holds {current_rows} rows, fewer than the recorded PNUs' {counts}")
    return rest


def snapshot_rows(spark: Any, table: str, snapshot_id: str) -> int:
    """The row count the snapshot's summary records."""
    row = spark.sql(f"SELECT summary['total-records'] AS rows FROM {table}.snapshots "
                    f"WHERE snapshot_id = {int(snapshot_id)}").first()
    if row is None or row.rows is None:
        raise Refusal(EXIT_NO_COMPARISON, f"{table} snapshot {snapshot_id} records no row count")
    return int(row.rows)


# Up to this many recorded PNUs are a filter Iceberg prunes data files with (its metrics evaluators
# treat a longer IN as "might match"), so a small change set reads only the files holding it and an
# empty one reads none. Above it they are a semi-join, broadcast up to BROADCAST_PNUS.
PUSHDOWN_PNUS = 200
BROADCAST_PNUS = 500_000


def recorded_pnus(spark: Any, args: argparse.Namespace, table: str) -> Any | None:
    """The PNUs the Gold commits between the two snapshots recorded, or None (compare whole)."""
    from pyspark.sql import functions as F

    import gold_incremental

    pnus = gold_incremental.recorded_changes(spark, f"`{args.iceberg_catalog_name}`.{table}",
                                             args.baseline_snapshot_id, args.current_snapshot_id)
    if pnus is None:
        print(f"{JOB_NAME}-change-source diff: a commit between the snapshots recorded no change set")
        return None
    pnus = pnus.persist()
    count = pnus.count()
    print(f"{JOB_NAME}-change-source recorded pnus={count}")
    if count <= PUSHDOWN_PNUS:
        return sorted(row.pnu for row in pnus.collect())
    return F.broadcast(pnus) if count <= BROADCAST_PNUS else pnus


def compare(spark: Any, args: argparse.Namespace) -> dict[str, Any]:
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    table = GOLD_TABLES[args.unit]
    unchanged = same_content(spark, f"`{args.iceberg_catalog_name}`.{table}", args.baseline_snapshot_id,
                             args.current_snapshot_id)
    if unchanged is not None:
        # Root ADR-0274 §1: the producer recorded the same content in both snapshots; nothing to scan.
        write_pnus(spark.createDataFrame([], "pnu string"), args.upsert_output)
        write_pnus(spark.createDataFrame([], "pnu string"), args.delete_output)
        return judge({"same": unchanged}, args.max_delta_fraction)
    suffix = KEY_SUFFIX_COLUMNS.get(args.unit)
    pnus = recorded_pnus(spark, args, table)
    baseline = snapshot_frame(spark, args.iceberg_catalog_name, table, args.baseline_snapshot_id, suffix, pnus)
    current = snapshot_frame(spark, args.iceberg_catalog_name, table, args.current_snapshot_id, suffix, pnus)
    refuse_missing_digests(baseline, "baseline")
    refuse_missing_digests(current, "current")
    # One join, kept on disk: the counts and both lists read it instead of joining again.
    classified = classify(baseline, current).persist(StorageLevel.DISK_ONLY)
    counts = {row["verdict"]: int(row["count"]) for row in classified.groupBy("verdict").count().collect()}
    if pnus is not None:
        # No commit between the two touched any other row, so the rest of the current rows are the same.
        counts["same"] = unchanged_rest(snapshot_rows(spark, f"`{args.iceberg_catalog_name}`.{table}",
                                                      args.current_snapshot_id), counts)
    metrics = judge(counts, args.max_delta_fraction)
    metrics["change_source"] = "diff" if pnus is None else "recorded"
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
