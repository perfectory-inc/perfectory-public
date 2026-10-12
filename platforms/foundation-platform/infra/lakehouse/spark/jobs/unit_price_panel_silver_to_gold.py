#!/usr/bin/env python3
"""Build gold.unit_price_panel: one row per PNU with its units' official price histories (root ADR-0190).

A unit's price history is a time series on its own cadence (a monthly vintage, a new reference
date a year). Its record of truth stays long and append-only in `silver.unit_official_price`; this
Gold is its by-PNU serving projection, apart from `gold.building_panel` so that a price vintage
never touches a building row or a building pack, and a building change never re-encodes prices.

A unit is placed under the PNU whose building document carries it, by the building panel's own
`unit_placement`, so the web joins the two documents by unit id. Only PNUs with at least one
priced unit have a row. The incremental rebuild (root ADR-0180) recomputes the PNUs whose units or
prices changed between two Silver snapshots and merges them; the full rebuild builds every PNU.
"""
from __future__ import annotations

import hashlib
import json
import os
from typing import Any

from pyspark.sql import SparkSession, functions as F

from lakehouse_engine import apply_catalog_settings, assert_catalog_env, assert_iceberg_runtime_loaded, iceberg_packages
from lakehouse_snapshot_pins import load_source_snapshot_pins
from platform_contracts import (column_names, ensure_contract_table, load_lakehouse_contract,
    partition_column_names, required_column_names, sort_order)
from parcel_panel_silver_to_gold import normalize_utc_timestamp, validate_identifier

import building_panel_silver_to_gold as panel
from gold_rebuild import assert_minimum_row_count
import gold_incremental as incremental

JOB_NAME = "unit_price_panel_silver_to_gold"
RUN_SUMMARY_SCHEMA_VERSION = panel.RUN_SUMMARY_SCHEMA_VERSION
GOLD_CONTRACT_NAME = "gold.unit_price_panel"
GOLD_CONTRACT = load_lakehouse_contract(GOLD_CONTRACT_NAME)
GOLD_COLUMNS = column_names(GOLD_CONTRACT)
REQUIRED_GOLD_COLUMNS = required_column_names(GOLD_CONTRACT)
CONTENT_DIGEST_COLUMNS = tuple(c for c in GOLD_COLUMNS if c not in
                              {"row_digest", "source_snapshot_id", "published_at_utc"})
TITLE_SOURCE = panel.TITLE_SOURCE
UNIT_SOURCE = panel.UNIT_SOURCE
PRICE_SOURCE = panel.PRICE_SOURCE
ALL_SOURCES = (TITLE_SOURCE, UNIT_SOURCE, PRICE_SOURCE)
# What the build reads of each input (root ADR-0180 §1): placement and the price rows.
BUILD_COLUMNS = {name: panel.BUILD_COLUMNS[name] for name in ALL_SOURCES}
WHOLE_TABLE_INPUTS: tuple[str, ...] = ()


def parse_args(argv=None):
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-mode", choices=("parquet", "iceberg"), default="iceberg")
    parser.add_argument("--input-root")
    parser.add_argument("--output")
    parser.add_argument("--write-mode", choices=("parquet", "iceberg"), default="parquet")
    parser.add_argument("--iceberg-catalog-name", default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_CATALOG_NAME", "r2"))
    parser.add_argument("--source-iceberg-namespace", default="silver")
    parser.add_argument("--target-iceberg-namespace", default="gold")
    parser.add_argument("--target-iceberg-table", default="unit_price_panel")
    parser.add_argument("--iceberg-write-mode", choices=("append", "overwrite"), default="overwrite")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    parser.add_argument("--iceberg-snapshot-id", required=True,
                        help="Title-table Iceberg snapshot to read; local source identity in parquet mode.")
    parser.add_argument("--source-snapshots-path", help="JSON file mapping all Silver tables to pinned Iceberg snapshot IDs.")
    parser.add_argument("--price-source-snapshot-id", help="Exact logical price batch within the pinned append-only Silver snapshot.")
    parser.add_argument("--published-at-utc")
    parser.add_argument("--allow-non-smoke-overwrite", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--expected-count", type=int)
    parser.add_argument("--minimum-count", type=int,
                        help="Refuse, before writing, a Gold with fewer rows (root ADR-0139).")
    parser.add_argument("--summary-output")
    parser.add_argument("--lineage-output")
    incremental.add_arguments(parser)
    return parser.parse_args(argv)


def validate_args(args):
    if args.input_mode == "parquet" and not args.input_root:
        raise ValueError("--input-root is required when --input-mode=parquet")
    if args.write_mode == "parquet" and not args.output:
        raise ValueError("--output is required when --write-mode=parquet")
    for name in ("iceberg_catalog_name", "source_iceberg_namespace", "target_iceberg_namespace", "target_iceberg_table"):
        validate_identifier(name, getattr(args, name))
    for name in ("expected_count", "minimum_count"):
        if getattr(args, name) is not None and getattr(args, name) < 0:
            raise ValueError(f"--{name.replace('_', '-')} must be non-negative")
    if args.price_source_snapshot_id is not None and not args.price_source_snapshot_id.strip():
        raise ValueError("--price-source-snapshot-id must be non-empty")
    if (args.write_mode == "iceberg" and args.iceberg_write_mode == "overwrite" and
            not args.validate_only and not args.target_iceberg_table.endswith("_smoke") and
            not args.allow_non_smoke_overwrite):
        raise ValueError("Refusing non-smoke overwrite without --allow-non-smoke-overwrite")
    if args.input_mode == "iceberg" or args.write_mode == "iceberg":
        assert_catalog_env(args.iceberg_catalog_name)
    if incremental.validate_arguments(args, ALL_SOURCES) is not None and args.price_source_snapshot_id:
        raise ValueError("--incremental-from-snapshots cannot compare a --price-source-snapshot-id batch")
    return load_source_snapshot_pins(args.input_mode, args.source_snapshots_path,
                                     ALL_SOURCES, TITLE_SOURCE, args.iceberg_snapshot_id)


def read_inputs(spark, args, pins, snapshots=None):
    """Every input at `pins` (the price table at its newest batch); `snapshots` collects batch ids."""
    frames = {}
    for name in ALL_SOURCES:
        frame = panel.read_source(spark, args, name, pins)
        if snapshots is not None:
            snapshots[name] = panel.assert_single_snapshot(frame, name)
        frames[name] = frame
    return incremental.project_build_inputs(frames, BUILD_COLUMNS)


def build_gold_frame(frames, source_snapshot_id, published_at_utc, counters):
    titles = panel.valid_pnus(frames[TITLE_SOURCE], TITLE_SOURCE, counters)
    units = panel.valid_pnus(frames[UNIT_SOURCE], UNIT_SOURCE, counters)
    panel.assert_unique(titles, ["mgm_bldrgst_pk"], TITLE_SOURCE)
    panel.assert_unique(units, ["mgm_bldrgst_pk"], UNIT_SOURCE)
    prices = panel.checked_prices(frames[PRICE_SOURCE])
    histories = prices.groupBy("mgm_bldrgst_pk").agg(
        F.sort_array(F.collect_list(F.struct("base_date", "price_won")), asc=False).alias("official_price_history"))
    placed = panel.unit_placement(units, titles).select(
        "mgm_bldrgst_pk", F.col("serving_pnu").alias("pnu"),
        panel.canonical_id("building_unit", F.col("mgm_bldrgst_pk")).alias("id"))
    # A price whose register names no unit has nowhere to be served; counted, as the panel does.
    counters["unplaced_price_unit_count"] = histories.join(placed, "mgm_bldrgst_pk", "left_anti").count()
    priced = placed.join(histories, "mgm_bldrgst_pk")
    grouped = panel.sorted_objects(priced, ["pnu"], ["id", "official_price_history"], "units")
    gold = grouped.select("pnu", F.to_json("units", panel.JSON_OPTIONS).alias("units_json"),
                          F.lit(source_snapshot_id).alias("source_snapshot_id"),
                          F.lit(published_at_utc).alias("published_at_utc"))
    return gold.withColumn("row_digest", row_digest_column()).select(*GOLD_COLUMNS)


def row_digest_column():
    """The row's content fingerprint (root ADR-0099), from the row's own content columns."""
    return F.sha2(F.to_json(F.struct(*CONTENT_DIGEST_COLUMNS), panel.JSON_OPTIONS), 256)


def validate_gold_frame(gold, expected_count):
    if tuple(gold.columns) != GOLD_COLUMNS:
        raise ValueError("Gold columns disagree with the lakehouse contract")
    panel.assert_unique(gold, ["pnu"], GOLD_CONTRACT_NAME)
    checks = [F.count(F.lit(1)).alias("row_count")]
    checks += [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c + "__null_count") for c in REQUIRED_GOLD_COLUMNS]
    checks += [F.sum(F.when(~F.col("pnu").rlike(panel.PNU_PATTERN), 1).otherwise(0)).alias("invalid_pnu_count"),
               F.sum(F.when(F.col("units_json") == "[]", 1).otherwise(0)).alias("empty_units_count")]
    metrics = {k: int(v or 0) for k, v in gold.agg(*checks).first().asDict().items()}
    if any(v for k, v in metrics.items() if k != "row_count"):
        raise ValueError(f"Gold quality gates failed: {metrics}")
    if expected_count is not None and metrics["row_count"] != expected_count:
        raise ValueError(f"Expected {expected_count} Gold rows, found {metrics['row_count']}")
    return metrics["row_count"], metrics


def _with_building_inputs(frames):
    """`frames` and the floors and areas the building panel's dependency maps also read, empty: this
    panel places units exactly as the building panel does, so it uses those maps, not a copy."""
    empty = frames[TITLE_SOURCE].select("mgm_bldrgst_pk").limit(0)
    return {**frames, panel.FLOOR_SOURCE: empty, panel.AREA_SOURCE: empty}


def affected_pnus(old, new, changed, appended=None):
    """Every PNU whose row can read a row that differs between `old` and `new` (ADR-0180 §2): the
    building panel's map, since a unit's prices are served under the PNU its building document is."""
    return panel.affected_pnus(_with_building_inputs(old), _with_building_inputs(new), changed, appended)


def restrict_inputs(frames, keys):
    """The input rows the Gold rows of `keys` (a frame of `pnu`) are built from (ADR-0180 §2)."""
    restricted = panel.restrict_inputs(_with_building_inputs(frames), keys)
    return {name: restricted[name] for name in ALL_SOURCES}


def build_session(args):
    builder = SparkSession.builder.appName(JOB_NAME).config("spark.sql.session.timeZone", "UTC")
    if args.input_mode == "iceberg" or args.write_mode == "iceberg":
        builder = apply_catalog_settings(builder, args.iceberg_catalog_name)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if args.input_mode == "iceberg" or args.write_mode == "iceberg":
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
    """Build, check and write the Gold: whole, or only the changed PNUs merged (root ADR-0180)."""
    previous = incremental.validate_arguments(args, ALL_SOURCES)
    target_table = f"`{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`.`{args.target_iceberg_table}`"
    # Rows hold a PNU's whole price history as JSON; read row-wise like the building panel.
    if args.write_mode == "iceberg":
        panel.apply_gold_read_properties(spark, target_table)
    snapshots, counters = {}, {}
    frames = read_inputs(spark, args, pins, snapshots)
    source_id = hashlib.sha256(json.dumps(snapshots, sort_keys=True).encode()).hexdigest()
    lane = incremental.Lane(
        build=lambda inputs: build_gold_frame(inputs, source_id, args.published_at_utc, counters),
        restrict=restrict_inputs, whole_table_inputs=WHOLE_TABLE_INPUTS,
        affected=lambda changed, appended: affected_pnus(
            read_inputs(spark, args, previous), frames, changed,
            panel.appended_changes(spark, args, previous, pins, snapshots, frames, appended)),
        append_inputs={name: panel.iceberg_table(args, name) for name in ALL_SOURCES},
        content_columns=CONTENT_DIGEST_COLUMNS, gold_columns=GOLD_COLUMNS,
        validate=lambda gold: validate_gold_frame(gold, None))
    outcome = incremental.build_or_merge(spark, args, lane, frames, pins, previous, target_table,
                                         validate_gold_frame, assert_minimum_row_count)
    try:
        persisted_count, metrics, added = None, outcome.metrics, ()
        if not args.validate_only:
            if args.write_mode == "parquet":
                (outcome.gold.repartition(*partition_column_names(GOLD_CONTRACT))
                 .sortWithinPartitions(*sort_order(GOLD_CONTRACT))
                 .write.mode("overwrite").partitionBy(*partition_column_names(GOLD_CONTRACT)).parquet(args.output))
                persisted = spark.read.parquet(args.output).select(*GOLD_COLUMNS)
            else:
                spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`")
                added = ensure_contract_table(spark, target_table, GOLD_CONTRACT)
                panel.apply_gold_read_properties(spark, target_table)
                outcome.commit(spark, target_table, pins, args.iceberg_write_mode)
                incremental.retain_input_snapshots(spark, args.iceberg_catalog_name, args.source_iceberg_namespace,
                                                   GOLD_CONTRACT_NAME, pins)
                persisted = spark.table(target_table).select(*GOLD_COLUMNS)
            persisted_count, metrics = validate_gold_frame(persisted, outcome.row_count)
        summary = {
            "schema_version": RUN_SUMMARY_SCHEMA_VERSION, "job_name": JOB_NAME, "contract": GOLD_CONTRACT_NAME,
            "created_at_utc": normalize_utc_timestamp(None),
            "input": {"kind": args.input_mode, "sources": list(ALL_SOURCES)},
            "target": {"kind": args.write_mode, "path": args.output} if args.write_mode == "parquet" else
                      {"kind": "iceberg", "catalog": args.iceberg_catalog_name, "namespace": args.target_iceberg_namespace,
                       "table": args.target_iceberg_table},
            "write_mode": args.write_mode,
            "write_disposition": "validate_only" if args.validate_only else (
                "parquet_overwrite" if args.write_mode == "parquet" else outcome.disposition(args)),
            "row_count": outcome.row_count, "persisted_row_count": persisted_count, "quality_metrics": {**metrics, **counters},
            "columns": list(GOLD_COLUMNS), "column_count": len(GOLD_COLUMNS), "required_columns": list(REQUIRED_GOLD_COLUMNS),
            "schema_evolution_added_columns": list(added), "source_snapshot_count": len(snapshots),
            "source_snapshot_ids": [snapshots[n] for n in sorted(snapshots)], "source_snapshots_by_dataset": snapshots,
            "source_iceberg_snapshots_by_dataset": dict(sorted(pins.items())),
            "source_snapshot_truncated": False,
            "rebuild": outcome.report(),
        }
    finally:
        outcome.release()
    panel.emit_json(summary, args.summary_output, "gold-unit-price-panel-summary-json")
    if args.lineage_output and not args.validate_only:
        panel.emit_json(build_lineage_event(args, summary), args.lineage_output, "gold-unit-price-panel-lineage-json")
    return 0


def build_lineage_event(args, summary: dict[str, Any]) -> dict[str, Any]:
    event = panel.build_lineage_event(args, summary)
    event.update({
        "job_name": JOB_NAME, "run_id": f"{JOB_NAME}:{summary['created_at_utc']}",
        "output_dataset": {"qualified_name": GOLD_CONTRACT_NAME, "namespace": "gold", "table": "unit_price_panel",
                           "storage_format": args.write_mode},
        "additional_input_datasets": [{"qualified_name": n, "namespace": "silver", "table": n.split(".", 1)[1],
                                       "storage_format": args.input_mode} for n in ALL_SOURCES[1:]],
        "column_lineage": [{"output_column": c, "inputs": [{"dataset": n, "column": "*", "transform": "unit_price_panel"}
                                                           for n in ALL_SOURCES]} for c in GOLD_COLUMNS],
    })
    event["openlineage_mapping"]["job_name"] = JOB_NAME
    return event


if __name__ == "__main__":
    raise SystemExit(main())
