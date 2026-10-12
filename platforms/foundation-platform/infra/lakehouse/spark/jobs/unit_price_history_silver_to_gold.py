#!/usr/bin/env python3
"""Build gold.unit_price_history: one row per unit and reference date (root ADR-0189).

A unit's official price history is a time series with its own cadence (a monthly vintage, a new
reference date a year), so it is kept out of the building panel's per-PNU document and stored
long: one small row per (unit, reference date), placed under the PNU whose building document
carries the unit (`building_panel_silver_to_gold.unit_placement`, the one placement function).

Each run projects the newest price batch whole (the same batch the building panel reads, root
ADR-0185) and writes only what differs from the current Gold, keyed by (pnu, register_pk,
base_date) so a file holding none of the changed PNUs is not touched:

* nothing differs — an empty append that records the new Silver pins (the planner's state);
* only new rows (a new reference date, new units) — one append, no file rewritten;
* otherwise — one copy-on-write MERGE of the changed, new and gone rows.

A table that does not exist yet is written whole. Every commit records its Silver pins
(root ADR-0139), and old snapshots stay in the table history (root ADR-0072).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession, functions as F
from pyspark.storagelevel import StorageLevel

from lakehouse_engine import apply_catalog_settings, assert_catalog_env, assert_iceberg_runtime_loaded, iceberg_packages
from lakehouse_ingest import SNAPSHOT_PROPERTY_PREFIX
from lakehouse_snapshot_pins import load_source_snapshot_pins
from platform_contracts import (column_names, ensure_contract_table, load_lakehouse_contract,
    required_column_names, sort_order)
from parcel_panel_silver_to_gold import normalize_utc_timestamp, validate_identifier

import building_panel_silver_to_gold as panel
from gold_incremental import refuse_merge_on_read, with_commit_properties
from gold_rebuild import SOURCE_SNAPSHOTS_PROPERTY, assert_minimum_row_count, pins_property_value

JOB_NAME = "unit_price_history_silver_to_gold"
RUN_SUMMARY_SCHEMA_VERSION = panel.RUN_SUMMARY_SCHEMA_VERSION
GOLD_CONTRACT_NAME = "gold.unit_price_history"
GOLD_CONTRACT = load_lakehouse_contract(GOLD_CONTRACT_NAME)
GOLD_COLUMNS = column_names(GOLD_CONTRACT)
REQUIRED_GOLD_COLUMNS = required_column_names(GOLD_CONTRACT)
CONTENT_DIGEST_COLUMNS = tuple(c for c in GOLD_COLUMNS if c not in
                              {"row_digest", "source_snapshot_id", "published_at_utc"})
# The row's identity: a register and a reference date, under the PNU that serves it. The PNU is
# part of it so a merge's changed keys prune by the files' PNU ranges; a unit that moves PNU is a
# row gone under the old PNU and a row new under the other.
KEY = ("pnu", "register_pk", "base_date")
TITLE_SOURCE = panel.TITLE_SOURCE
UNIT_SOURCE = panel.UNIT_SOURCE
PRICE_SOURCE = panel.PRICE_SOURCE
ALL_SOURCES = (TITLE_SOURCE, UNIT_SOURCE, PRICE_SOURCE)
CHANGE = "_foundation_change"
MERGE_SOURCE_VIEW = "foundation_unit_price_history_changes"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-mode", choices=("parquet", "iceberg"), default="iceberg")
    parser.add_argument("--input-root")
    parser.add_argument("--write-mode", choices=("iceberg",), default="iceberg")
    parser.add_argument("--iceberg-catalog-name", default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_CATALOG_NAME", "r2"))
    parser.add_argument("--source-iceberg-namespace", default="silver")
    parser.add_argument("--target-iceberg-namespace", default="gold")
    parser.add_argument("--target-iceberg-table", default="unit_price_history")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    parser.add_argument("--iceberg-snapshot-id", required=True,
                        help="Title-table Iceberg snapshot to read; local source identity in parquet mode.")
    parser.add_argument("--source-snapshots-path", help="JSON file mapping all Silver tables to pinned Iceberg snapshot IDs.")
    parser.add_argument("--price-source-snapshot-id", help="Exact logical price batch within the pinned append-only Silver snapshot.")
    parser.add_argument("--published-at-utc")
    parser.add_argument("--allow-non-smoke-overwrite", action="store_true",
                        help="Accepted for the Gold rebuild's common flags; this producer writes only differences.")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--expected-count", type=int)
    parser.add_argument("--minimum-count", type=int,
                        help="Refuse, before writing, a Gold with fewer rows (root ADR-0139).")
    parser.add_argument("--summary-output")
    parser.add_argument("--lineage-output")
    return parser.parse_args(argv)


def validate_args(args):
    if args.input_mode == "parquet" and not args.input_root:
        raise ValueError("--input-root is required when --input-mode=parquet")
    for name in ("iceberg_catalog_name", "source_iceberg_namespace", "target_iceberg_namespace", "target_iceberg_table"):
        validate_identifier(name, getattr(args, name))
    for name in ("expected_count", "minimum_count"):
        if getattr(args, name) is not None and getattr(args, name) < 0:
            raise ValueError(f"--{name.replace('_', '-')} must be non-negative")
    if args.price_source_snapshot_id is not None and not args.price_source_snapshot_id.strip():
        raise ValueError("--price-source-snapshot-id must be non-empty")
    assert_catalog_env(args.iceberg_catalog_name)
    return load_source_snapshot_pins(args.input_mode, args.source_snapshots_path,
                                     ALL_SOURCES, TITLE_SOURCE, args.iceberg_snapshot_id)


def read_inputs(spark, args, pins, snapshots):
    """Every input at `pins` (the price table at its newest batch); `snapshots` gets each batch id."""
    frames = {}
    for name in ALL_SOURCES:
        frame = panel.read_source(spark, args, name, pins)
        snapshots[name] = panel.assert_single_snapshot(frame, name)
        frames[name] = frame
    return frames


def row_digest_column():
    return F.sha2(F.to_json(F.struct(*CONTENT_DIGEST_COLUMNS), panel.JSON_OPTIONS), 256)


def build_gold_frame(frames, source_snapshot_id, published_at_utc, counters):
    titles = panel.valid_pnus(frames[TITLE_SOURCE], TITLE_SOURCE, counters)
    units = panel.valid_pnus(frames[UNIT_SOURCE], UNIT_SOURCE, counters)
    panel.assert_unique(titles, ["mgm_bldrgst_pk"], TITLE_SOURCE)
    panel.assert_unique(units, ["mgm_bldrgst_pk"], UNIT_SOURCE)
    prices = panel.checked_prices(frames[PRICE_SOURCE]).select(
        F.col("mgm_bldrgst_pk").alias("register_pk"), "base_date", "price_won")
    placed = panel.unit_placement(units, titles).select(
        F.col("serving_pnu").alias("pnu"), panel.canonical_id("building_unit", F.col("mgm_bldrgst_pk")).alias("unit_id"),
        F.col("mgm_bldrgst_pk").alias("register_pk"))
    # A price whose register names no unit has nowhere to be served; counted, as the panel did.
    counters["unplaced_price_count"] = prices.join(placed, "register_pk", "left_anti").count()
    gold = placed.join(prices, "register_pk").select(
        "pnu", "unit_id", "register_pk", "base_date", "price_won",
        F.lit(source_snapshot_id).alias("source_snapshot_id"), F.lit(published_at_utc).alias("published_at_utc"))
    return gold.withColumn("row_digest", row_digest_column()).select(*GOLD_COLUMNS)


def validate_gold_frame(gold, expected_count):
    if tuple(gold.columns) != GOLD_COLUMNS:
        raise ValueError("Gold columns disagree with the lakehouse contract")
    panel.assert_unique(gold, ["register_pk", "base_date"], GOLD_CONTRACT_NAME)
    checks = [F.count(F.lit(1)).alias("row_count")]
    checks += [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c + "__null_count") for c in REQUIRED_GOLD_COLUMNS]
    checks += [F.sum(F.when(~F.col("pnu").rlike(panel.PNU_PATTERN), 1).otherwise(0)).alias("invalid_pnu_count"),
               F.sum(F.when(~F.col("base_date").rlike("^[0-9]{8}$") | (F.col("price_won") < 0), 1).otherwise(0))
               .alias("invalid_price_count")]
    metrics = {k: int(v or 0) for k, v in gold.agg(*checks).first().asDict().items()}
    if any(v for k, v in metrics.items() if k != "row_count"):
        raise ValueError(f"Gold quality gates failed: {metrics}")
    if expected_count is not None and metrics["row_count"] != expected_count:
        raise ValueError(f"Expected {expected_count} Gold rows, found {metrics['row_count']}")
    return metrics["row_count"], metrics


def changes(current, new):
    """Rows of `new` that are new or differ from `current`, and keys of `current` gone from `new`.

    `CHANGE` is "insert", "update" or "delete"; a delete row carries only its key.
    """
    old = current.select(*KEY, F.col("row_digest").alias("_old_digest"))
    joined = new.join(old, list(KEY), "full_outer")
    kind = (F.when(F.col("row_digest").isNull(), F.lit("delete"))
            .when(F.col("_old_digest").isNull(), F.lit("insert"))
            .when(F.col("row_digest") != F.col("_old_digest"), F.lit("update")))
    return joined.withColumn(CHANGE, kind).where(F.col(CHANGE).isNotNull()).select(*GOLD_COLUMNS, CHANGE)


def merge_sql(table):
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in KEY)
    update = ", ".join(f"t.`{c}` = s.`{c}`" for c in GOLD_COLUMNS)
    names = ", ".join(f"`{c}`" for c in GOLD_COLUMNS)
    values = ", ".join(f"s.`{c}`" for c in GOLD_COLUMNS)
    return (f"MERGE INTO {table} t USING {MERGE_SOURCE_VIEW} s ON {on} "
            f"WHEN MATCHED AND s.`{CHANGE}` = 'delete' THEN DELETE "
            f"WHEN MATCHED AND s.`{CHANGE}` = 'update' THEN UPDATE SET {update} "
            f"WHEN NOT MATCHED AND s.`{CHANGE}` = 'insert' THEN INSERT ({names}) VALUES ({values})")


def counts_of(changed):
    found = {row[CHANGE]: int(row["count"]) for row in changed.groupBy(CHANGE).count().collect()}
    return {kind: found.get(kind, 0) for kind in ("insert", "update", "delete")}


def commit(spark, table, gold, row_count, pins, exists):
    """Write `gold` into `table` as its differences; the commit's disposition and change counts."""
    pinned = {SOURCE_SNAPSHOTS_PROPERTY: pins_property_value(pins)}
    ordered = gold.repartitionByRange(*sort_order(GOLD_CONTRACT)).sortWithinPartitions(*sort_order(GOLD_CONTRACT))
    if not exists:
        writer = ordered.writeTo(table)
        for key, value in pinned.items():
            writer = writer.option(f"{SNAPSHOT_PROPERTY_PREFIX}{key}", value)
        writer.append()
        return "iceberg_create", {"insert": row_count, "update": 0, "delete": 0}
    refuse_merge_on_read(spark, table)
    changed = changes(spark.table(table), gold).persist(StorageLevel.DISK_ONLY)
    try:
        counts = counts_of(changed)
        if counts["update"] == 0 and counts["delete"] == 0:
            # Only new rows (or none): an append, so no data file is rewritten.
            writer = changed.drop(CHANGE).repartitionByRange(*sort_order(GOLD_CONTRACT)) \
                .sortWithinPartitions(*sort_order(GOLD_CONTRACT)).writeTo(table)
            for key, value in pinned.items():
                writer = writer.option(f"{SNAPSHOT_PROPERTY_PREFIX}{key}", value)
            writer.append()
            return ("iceberg_append" if counts["insert"] else "iceberg_pins_only"), counts
        changed.createOrReplaceTempView(MERGE_SOURCE_VIEW)
        try:
            with_commit_properties(spark, pinned, lambda: spark.sql(merge_sql(table)))
        finally:
            spark.catalog.dropTempView(MERGE_SOURCE_VIEW)
        return "iceberg_merge", counts
    finally:
        changed.unpersist()


def build_session(args):
    builder = SparkSession.builder.appName(JOB_NAME).config("spark.sql.session.timeZone", "UTC")
    builder = apply_catalog_settings(builder, args.iceberg_catalog_name)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    assert_iceberg_runtime_loaded(spark, args.iceberg_packages)
    return spark


def main():
    args = parse_args()
    pins = validate_args(args)
    args.published_at_utc = normalize_utc_timestamp(args.published_at_utc)
    spark = build_session(args)
    try:
        return run(spark, args, pins)
    finally:
        spark.stop()


def run(spark, args, pins):
    target_table = f"`{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`.`{args.target_iceberg_table}`"
    snapshots, counters = {}, {}
    frames = read_inputs(spark, args, pins, snapshots)
    source_id = hashlib.sha256(json.dumps(snapshots, sort_keys=True).encode()).hexdigest()
    gold = build_gold_frame(frames, source_id, args.published_at_utc, counters).persist(StorageLevel.DISK_ONLY)
    try:
        row_count, metrics = validate_gold_frame(gold, args.expected_count)
        assert_minimum_row_count(row_count, args.minimum_count)
        disposition, counts, added, persisted_count = "validate_only", None, (), None
        if not args.validate_only:
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`")
            exists = spark.catalog.tableExists(target_table.replace("`", ""))
            added = ensure_contract_table(spark, target_table, GOLD_CONTRACT)
            disposition, counts = commit(spark, target_table, gold, row_count, pins, exists)
            persisted_count, metrics = validate_gold_frame(spark.table(target_table).select(*GOLD_COLUMNS), row_count)
    finally:
        gold.unpersist()
    changed = counts is not None and any(counts.values())
    summary = {
        "schema_version": RUN_SUMMARY_SCHEMA_VERSION, "job_name": JOB_NAME, "contract": GOLD_CONTRACT_NAME,
        "created_at_utc": normalize_utc_timestamp(None),
        "input": {"kind": args.input_mode, "sources": list(ALL_SOURCES)},
        "target": {"kind": "iceberg", "catalog": args.iceberg_catalog_name, "namespace": args.target_iceberg_namespace,
                   "table": args.target_iceberg_table},
        "write_mode": args.write_mode, "write_disposition": disposition,
        "row_count": row_count, "persisted_row_count": persisted_count, "quality_metrics": {**metrics, **counters},
        "columns": list(GOLD_COLUMNS), "column_count": len(GOLD_COLUMNS), "required_columns": list(REQUIRED_GOLD_COLUMNS),
        "schema_evolution_added_columns": list(added), "source_snapshot_count": len(snapshots),
        "source_snapshot_ids": [snapshots[n] for n in sorted(snapshots)], "source_snapshots_by_dataset": snapshots,
        "source_iceberg_snapshots_by_dataset": dict(sorted(pins.items())),
        "source_snapshot_truncated": False,
        # gold-panel-rebuild.sh reads `mode` and `merged` (False: only the pins were recorded).
        "rebuild": {"mode": "differences", "counts": counts, "merged": changed},
    }
    panel.emit_json(summary, args.summary_output, "gold-unit-price-history-summary-json")
    if args.lineage_output and not args.validate_only:
        panel.emit_json(build_lineage_event(args, summary), args.lineage_output, "gold-unit-price-history-lineage-json")
    return 0


def build_lineage_event(args: argparse.Namespace, summary: dict[str, Any]) -> dict[str, Any]:
    event = panel.build_lineage_event(args, summary)
    output = {"qualified_name": GOLD_CONTRACT_NAME, "namespace": "gold", "table": "unit_price_history",
              "storage_format": args.write_mode}
    event.update({
        "job_name": JOB_NAME, "run_id": f"{JOB_NAME}:{summary['created_at_utc']}", "output_dataset": output,
        "additional_input_datasets": [{"qualified_name": n, "namespace": "silver", "table": n.split(".", 1)[1],
                                       "storage_format": args.input_mode} for n in ALL_SOURCES[1:]],
        "column_lineage": [{"output_column": c, "inputs": [{"dataset": n, "column": "*", "transform": "unit_price_history"}
                                                           for n in ALL_SOURCES]} for c in GOLD_COLUMNS],
    })
    event["openlineage_mapping"]["job_name"] = JOB_NAME
    return event


if __name__ == "__main__":
    raise SystemExit(main())
