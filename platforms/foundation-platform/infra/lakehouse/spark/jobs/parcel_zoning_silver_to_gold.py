#!/usr/bin/env python3
"""Build gold.parcel_zoning: one row per served parcel with its zoning verdict (root ADR-0193).

The zoning verdict is one data kind of the parcel panel: the land use plan rows a parcel is zoned
by (inclusion 1|2), each resolved to its anchor through the zone code table. It gets its own Gold
so that a land use plan vintage rebuilds only this table, and a price or transfer vintage never
re-encodes zonings. The rules are the parcel panel's own functions (`build_zonings`,
`resolve_zoning_anchors`, `carry_section`), called here, not copied: until the panel retires, both
tables come from the same code, and `--parity-panel-snapshot-id` proves it on production pins.

Only PNUs with a verdict have a row. A parcel without its own verdict takes its nearest lineage
predecessor's (root ADR-0113 §6) and names the path in `attached_via`. A zone code table change
rebuilds whole; any other input change recomputes only the PNUs it reaches (root ADR-0180).
"""
from __future__ import annotations

import argparse
import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_rebuild import assert_minimum_row_count
import gold_incremental as incremental
from lakehouse_engine import apply_catalog_settings, assert_catalog_env, assert_iceberg_runtime_loaded, iceberg_packages
from lakehouse_snapshot_pins import load_source_snapshot_pins
from platform_contracts import (column_names, ensure_contract_table, load_lakehouse_contract,
    partition_column_names, required_column_names, sort_order)
import parcel_panel_silver_to_gold as panel

JOB_NAME = "parcel_zoning_silver_to_gold"
GOLD_CONTRACT_NAME = "gold.parcel_zoning"
GOLD_CONTRACT = load_lakehouse_contract(GOLD_CONTRACT_NAME)
GOLD_COLUMNS = column_names(GOLD_CONTRACT)
REQUIRED_GOLD_COLUMNS = required_column_names(GOLD_CONTRACT)
CONTENT_DIGEST_COLUMNS = tuple(c for c in GOLD_COLUMNS if c not in panel.LINEAGE_COLUMNS)
PARCEL_SOURCE = panel.PARCEL_SOURCE
ZONING_SOURCE = panel.ZONING_SOURCE
ZONE_CODE_SOURCE = panel.ZONE_CODE_SOURCE
LINEAGE_SOURCE = panel.LINEAGE_SOURCE
ALL_SOURCES = (PARCEL_SOURCE, ZONING_SOURCE, ZONE_CODE_SOURCE)
BUILD_COLUMNS = {name: panel.BUILD_COLUMNS[name] for name in ALL_SOURCES}
WHOLE_TABLE_INPUTS: tuple[str, ...] = (ZONE_CODE_SOURCE,)
PANEL_TABLE = "gold.parcel_panel"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-mode", choices=("parquet", "iceberg"), default="iceberg")
    parser.add_argument("--input-root", help="Parquet input root: one subdirectory per source table.")
    parser.add_argument("--output")
    parser.add_argument("--write-mode", choices=("parquet", "iceberg"), default="parquet")
    parser.add_argument("--iceberg-catalog-name", default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_CATALOG_NAME", "r2"))
    parser.add_argument("--source-iceberg-namespace", default="silver")
    parser.add_argument("--target-iceberg-namespace", default="gold")
    parser.add_argument("--target-iceberg-table", default="parcel_zoning")
    parser.add_argument("--iceberg-write-mode", choices=("append", "overwrite"), default="overwrite")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    parser.add_argument("--iceberg-snapshot-id", required=True,
                        help="Iceberg snapshot id of silver.parcel_boundaries represented by this projection.")
    parser.add_argument("--source-snapshots-path", help="JSON file pinning every input table.")
    parser.add_argument("--published-at-utc")
    parser.add_argument("--region-prefix", help="Optional PNU prefix every source is filtered by.")
    parser.add_argument("--allow-non-smoke-overwrite", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--expected-count", type=int)
    parser.add_argument("--minimum-count", type=int, help="Refuse, before writing, a Gold with fewer rows (root ADR-0139).")
    parser.add_argument("--no-carry-lineage", dest="carry_lineage", action="store_false",
                        help="Do not attach a predecessor's verdict through silver.parcel_lineage (root ADR-0113 §6).")
    parser.add_argument("--parity-panel-snapshot-id",
                        help="Refuse, before writing, a Gold whose verdicts differ from this gold.parcel_panel "
                             "snapshot's zonings (root ADR-0193 §6-2); pass the panel's own pins.")
    parser.add_argument("--summary-output")
    parser.add_argument("--lineage-output")
    incremental.add_arguments(parser)
    return parser.parse_args(argv)


def input_sources(args) -> tuple[str, ...]:
    return (*ALL_SOURCES, *((LINEAGE_SOURCE,) if args.carry_lineage else ()))


def validate_args(args) -> dict[str, str]:
    if args.input_mode == "parquet" and not args.input_root:
        raise ValueError("--input-root is required when --input-mode=parquet")
    if args.write_mode == "parquet" and not args.output:
        raise ValueError("--output is required when --write-mode=parquet")
    if args.region_prefix is not None and not (args.region_prefix.isdigit() and len(args.region_prefix) <= 10):
        raise ValueError("--region-prefix must be 1 to 10 digits")
    for name in ("expected_count", "minimum_count"):
        if getattr(args, name) is not None and getattr(args, name) < 0:
            raise ValueError(f"--{name.replace('_', '-')} must be non-negative")
    if args.parity_panel_snapshot_id is not None and (
            args.input_mode != "iceberg" or not args.parity_panel_snapshot_id.isdigit()):
        raise ValueError("--parity-panel-snapshot-id is an Iceberg snapshot id of gold.parcel_panel (iceberg input)")
    for name in ("iceberg_catalog_name", "source_iceberg_namespace", "target_iceberg_namespace", "target_iceberg_table"):
        panel.validate_identifier(name, getattr(args, name))
    if (args.write_mode == "iceberg" and args.iceberg_write_mode == "overwrite" and not args.validate_only
            and not args.target_iceberg_table.endswith("_smoke") and not args.allow_non_smoke_overwrite):
        raise ValueError("Refusing to overwrite a non-smoke Iceberg table without --allow-non-smoke-overwrite")
    if args.input_mode == "iceberg" or args.write_mode == "iceberg":
        assert_catalog_env(args.iceberg_catalog_name)
    incremental.validate_arguments(args, input_sources(args))
    return load_source_snapshot_pins(args.input_mode, args.source_snapshots_path, input_sources(args),
                                     PARCEL_SOURCE, args.iceberg_snapshot_id)


def read_inputs(spark, args, pins, counters, snapshots=None):
    """Every input at `pins` reduced to BUILD_COLUMNS, and the lineage's carry candidates."""
    frames = {PARCEL_SOURCE: panel.read_served_parcels(spark, args, pins)}
    candidates = panel.read_carry_candidates(spark, args, counters, pins)
    frames[ZONING_SOURCE] = panel.with_lineage_sources(
        panel.read_source(spark, args, ZONING_SOURCE, pins), args.region_prefix, candidates)
    frames[ZONE_CODE_SOURCE] = panel.read_source(spark, args, ZONE_CODE_SOURCE, pins)
    if snapshots is not None:
        for name, frame in frames.items():
            snapshots[name] = panel.assert_single_snapshot(frame, name)
    return incremental.project_build_inputs(frames, BUILD_COLUMNS), candidates


def build_gold_frame(spark, frames, candidates, counters, source_snapshot_id, published_at_utc) -> DataFrame:
    anchors = panel.resolve_zoning_anchors(frames[ZONE_CODE_SOURCE])
    zonings = panel.build_zonings(frames[ZONING_SOURCE], anchors, spark, counters)
    carried = panel.carry_section(zonings, "attached_via", candidates)
    parcels = frames[PARCEL_SOURCE].select("pnu").distinct()
    gold = parcels.join(carried, "pnu").select(
        "pnu", "zonings_json", "attached_via",
        F.lit(source_snapshot_id).alias("source_snapshot_id"), F.lit(published_at_utc).alias("published_at_utc"))
    return gold.withColumn("row_digest", row_digest_column()).select(*GOLD_COLUMNS)


def row_digest_column():
    """The row's content fingerprint (root ADR-0099); a null `attached_via` drops out of the JSON."""
    return F.sha2(F.to_json(F.struct(*CONTENT_DIGEST_COLUMNS)), 256)


def validate_gold_frame(gold, expected_count):
    if tuple(gold.columns) != GOLD_COLUMNS:
        raise ValueError(f"Gold columns disagree with the lakehouse contract: {gold.columns}")
    checks = [F.count(F.lit(1)).alias("row_count")]
    checks += [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c + "__null_count") for c in REQUIRED_GOLD_COLUMNS]
    checks += [F.sum(F.when(~F.col("pnu").rlike(panel.PNU_PATTERN), 1).otherwise(0)).alias("invalid_pnu_count"),
               F.sum(F.when(F.col("zonings_json") == "[]", 1).otherwise(0)).alias("empty_zonings_count")]
    metrics = {k: int(v or 0) for k, v in gold.agg(*checks).first().asDict().items()}
    if any(v for k, v in metrics.items() if k != "row_count"):
        raise ValueError(f"Gold quality gates failed: {metrics}")
    duplicates = gold.groupBy("pnu").count().where(F.col("count") > 1).limit(3).collect()
    if duplicates:
        raise ValueError(f"Gold must hold one row per pnu: {[r.pnu for r in duplicates]}")
    if expected_count is not None and metrics["row_count"] != expected_count:
        raise ValueError(f"Expected {expected_count} Gold rows, found {metrics['row_count']}")
    return metrics["row_count"], metrics


def parity_mismatches(gold: DataFrame, panel_rows: DataFrame) -> DataFrame:
    """PNUs whose verdict here differs from the panel's `zonings` section (root ADR-0193 §6-2).

    The panel writes `[]` for a parcel without a verdict, where this table has no row; a carried
    verdict's path is the `zonings` member of the panel's `attached_via_json`.
    """
    expected = panel_rows.where(F.col("zonings_json") != "[]").select(
        "pnu", F.col("zonings_json").alias("expected_zonings"),
        F.get_json_object("attached_via_json", "$.zonings").alias("expected_via"))
    joined = gold.select("pnu", "zonings_json", "attached_via").join(expected, "pnu", "full_outer")
    return joined.where(~(F.col("zonings_json").eqNullSafe(F.col("expected_zonings"))
                          & F.col("attached_via").eqNullSafe(F.col("expected_via")))).select("pnu")


def assert_parity(spark, args, gold: DataFrame) -> dict[str, int]:
    if args.parity_panel_snapshot_id is None:
        return {}
    rows = spark.sql(f"SELECT pnu, zonings_json, attached_via_json FROM `{args.iceberg_catalog_name}`."
                     f"{PANEL_TABLE} VERSION AS OF {int(args.parity_panel_snapshot_id)}")
    rows = panel.filtered_by_region(rows, args.region_prefix)
    mismatched = parity_mismatches(gold, rows)
    count = mismatched.count()
    if count:
        examples = sorted(r.pnu for r in mismatched.limit(3).collect())
        raise ValueError(f"{count} PNUs' zonings differ from {PANEL_TABLE} snapshot "
                         f"{args.parity_panel_snapshot_id} (e.g. {examples}); built from different pins, "
                         "or the two producers drifted")
    return {"parity_mismatch_count": 0}


def affected_pnus(old, old_candidates, new, new_candidates, changed):
    """The panel's map (root ADR-0180 §2): every input here is keyed by its own PNU or the lineage's."""
    return panel.affected_pnus(old, old_candidates, new, new_candidates, changed)


def restrict_inputs(frames, keys, candidates):
    """The input rows the Gold rows of `keys` are built from: their own and their lineage sources'."""
    sources = keys
    if candidates is not None:
        successors = keys.select(F.col("pnu").alias("successor_pnu"))
        sources = incremental.union_keys([keys, candidates.join(successors, "successor_pnu", "left_semi")
                                          .select(F.col("source_pnu").alias("pnu"))])
    return {
        PARCEL_SOURCE: incremental.rows_matching_any(frames[PARCEL_SOURCE], [("pnu", keys, "pnu", False)]),
        ZONING_SOURCE: incremental.rows_matching_any(frames[ZONING_SOURCE], [("pnu", sources, "pnu", False)]),
        ZONE_CODE_SOURCE: frames[ZONE_CODE_SOURCE],
    }


def build_session(args) -> SparkSession:
    builder = SparkSession.builder.appName(JOB_NAME).config("spark.sql.session.timeZone", "UTC")
    if args.input_mode == "iceberg" or args.write_mode == "iceberg":
        builder = apply_catalog_settings(builder, args.iceberg_catalog_name)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if args.input_mode == "iceberg" or args.write_mode == "iceberg":
        assert_iceberg_runtime_loaded(spark, args.iceberg_packages)
    return spark


def main() -> int:
    args = parse_args()
    args.published_at_utc = panel.normalize_utc_timestamp(args.published_at_utc)
    pins = validate_args(args)
    spark = build_session(args)
    try:
        return run(spark, args, pins)
    finally:
        spark.stop()


def run(spark, args, pins) -> int:
    """Build, check and write the Gold: whole, or only the changed PNUs merged (root ADR-0180)."""
    previous = incremental.validate_arguments(args, input_sources(args))
    target_table = f"`{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`.`{args.target_iceberg_table}`"
    counters: dict[str, int] = {}
    snapshots: dict[str, str] = {}
    frames, candidates = read_inputs(spark, args, pins, counters, snapshots)
    source_id = snapshots[PARCEL_SOURCE]
    lane = incremental.Lane(
        build=lambda inputs: build_gold_frame(spark, inputs, candidates, counters, source_id, args.published_at_utc),
        restrict=lambda inputs, keys: restrict_inputs(inputs, keys, candidates),
        affected=lambda changed, _appended: affected_pnus(
            *read_inputs(spark, args, previous, {}), frames, candidates, changed),
        validate=lambda gold: validate_gold_frame(gold, None),
        content_columns=CONTENT_DIGEST_COLUMNS, gold_columns=GOLD_COLUMNS,
        whole_table_inputs=WHOLE_TABLE_INPUTS)
    outcome = incremental.build_or_merge(spark, args, lane, frames, pins, previous, target_table,
                                         validate_gold_frame, assert_minimum_row_count)
    try:
        if outcome.mode == "full":
            counters.update(assert_parity(spark, args, outcome.gold))
        persisted_count, metrics, added = None, outcome.metrics, ()
        if not args.validate_only:
            if args.write_mode == "parquet":
                (outcome.gold.repartition(*partition_column_names(GOLD_CONTRACT))
                 .sortWithinPartitions(*sort_order(GOLD_CONTRACT))
                 .write.mode("overwrite").partitionBy(*partition_column_names(GOLD_CONTRACT)).parquet(args.output))
                persisted = spark.read.parquet(args.output).select(*GOLD_COLUMNS)
            else:
                spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`")
                # Range-ordered by pnu, so a by-PNU bake shard reads only the files of its prefix (ADR-0164).
                added = ensure_contract_table(spark, target_table, GOLD_CONTRACT)
                outcome.commit(spark, target_table, pins, args.iceberg_write_mode)
                incremental.retain_input_snapshots(spark, args.iceberg_catalog_name, args.source_iceberg_namespace,
                                                   GOLD_CONTRACT_NAME, pins)
                persisted = spark.table(target_table).select(*GOLD_COLUMNS)
            persisted_count, metrics = validate_gold_frame(persisted, outcome.row_count)
        summary = build_run_summary(args, outcome, persisted_count, {**metrics, **counters}, snapshots, pins, added)
    finally:
        outcome.release()
    panel.emit_run_summary(summary, args.summary_output)
    if args.lineage_output and not args.validate_only:
        panel.emit_lineage_event(build_lineage_event(args, summary), args.lineage_output)
    print(f"gold-parcel-zoning-{'validate' if args.validate_only else 'write'}-ok "
          f"rows={persisted_count or outcome.row_count} mode={outcome.mode}")
    return 0


def build_run_summary(args, outcome, persisted_count, metrics, snapshots, pins, added) -> dict[str, Any]:
    return {
        "schema_version": panel.RUN_SUMMARY_SCHEMA_VERSION, "job_name": JOB_NAME, "contract": GOLD_CONTRACT_NAME,
        "created_at_utc": panel.normalize_utc_timestamp(None),
        "input": {"kind": args.input_mode, "sources": list(input_sources(args)), "region_prefix": args.region_prefix},
        "target": {"kind": "parquet", "path": args.output} if args.write_mode == "parquet" else
                  {"kind": "iceberg", "catalog": args.iceberg_catalog_name,
                   "namespace": args.target_iceberg_namespace, "table": args.target_iceberg_table},
        "write_mode": args.write_mode,
        "write_disposition": "validate_only" if args.validate_only else (
            "parquet_overwrite" if args.write_mode == "parquet" else outcome.disposition(args)),
        "row_count": outcome.row_count, "persisted_row_count": persisted_count, "quality_metrics": metrics,
        "columns": list(GOLD_COLUMNS), "column_count": len(GOLD_COLUMNS), "required_columns": list(REQUIRED_GOLD_COLUMNS),
        "schema_evolution_added_columns": list(added), "source_snapshot_count": len(snapshots),
        "source_snapshot_ids": [snapshots[n] for n in sorted(snapshots)],
        "source_snapshots_by_dataset": dict(sorted(snapshots.items())),
        "source_iceberg_snapshots_by_dataset": dict(sorted(pins.items())),
        "source_snapshot_truncated": False,
        "rebuild": outcome.report(),
    }


def column_lineage(carry_lineage: bool) -> list[dict[str, Any]]:
    def one(dataset: str, column: str, transform: str) -> list[dict[str, str]]:
        return [{"dataset": dataset, "column": column, "transform": transform}]

    zonings = (one(ZONING_SOURCE, "zone_code,zone_name,inclusion_code", "loader_semantics_json")
               + one(ZONE_CODE_SOURCE, "ucode,parent_ucode", "anchor_walk"))
    lineage = {
        "pnu": one(PARCEL_SOURCE, "pnu", "identity"),
        "zonings_json": zonings,
        "attached_via": one(LINEAGE_SOURCE, "predecessor_pnu,successor_pnu,relation,grade",
                            "steward_resolved_nearest_holder_path") if carry_lineage
        else one("foundation-platform.job_arguments", "carry_lineage", "literal_null_when_disabled"),
        "source_snapshot_id": one(PARCEL_SOURCE, "source_snapshot_id", "identity"),
        "published_at_utc": one("foundation-platform.job_arguments", "published_at_utc", "literal"),
    }
    lineage["row_digest"] = [{**s, "transform": f"sha256_content:{s['transform']}"}
                             for c in CONTENT_DIGEST_COLUMNS for s in lineage[c]]
    return [{"output_column": c, "inputs": lineage[c]} for c in GOLD_COLUMNS]


def build_lineage_event(args, summary: dict[str, Any]) -> dict[str, Any]:
    event = panel.build_lineage_event(args, summary)
    event.update({
        "job_name": JOB_NAME, "run_id": f"{JOB_NAME}:{summary['created_at_utc']}",
        "output_dataset": {"qualified_name": GOLD_CONTRACT_NAME, "namespace": "gold", "table": "parcel_zoning",
                           "storage_format": args.write_mode},
        "column_lineage": column_lineage(args.carry_lineage),
    })
    event["openlineage_mapping"]["job_name"] = JOB_NAME
    return event


if __name__ == "__main__":
    raise SystemExit(main())
