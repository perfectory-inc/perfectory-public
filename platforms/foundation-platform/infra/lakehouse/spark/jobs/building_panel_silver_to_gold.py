#!/usr/bin/env python3
"""Build the independent building/floor/unit by-PNU Gold projection (root ADR-0100).

The title and unit handoffs define the field semantics. This job adds deterministic
ordering, nests current floor rows, preserves unlinked units, and refuses duplicate
natural keys rather than choosing an arbitrary row. Unknown facts remain null.
Catalog UUIDs are projections of catalog-domain's versioned identity functions;
the Rust baker verifies every projected UUID against those functions before writing.
No runtime timestamp is included in the sections or their content digest.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window, functions as F, types as T
from pyspark.storagelevel import StorageLevel

from lakehouse_engine import apply_catalog_settings, assert_catalog_env, assert_iceberg_runtime_loaded, iceberg_packages
from platform_contracts import (column_names, create_table_columns_sql, current_row_predicate,
    evolve_iceberg_table_to_contract, load_lakehouse_contract, partition_clause_sql,
    partition_column_names, required_column_names, sort_order)
from parcel_panel_silver_to_gold import normalize_utc_timestamp, validate_identifier

JOB_NAME = "building_panel_silver_to_gold"
RUN_SUMMARY_SCHEMA_VERSION = "foundation-platform.spark_run_summary.v1"
GOLD_CONTRACT_NAME = "gold.building_panel"
GOLD_CONTRACT = load_lakehouse_contract(GOLD_CONTRACT_NAME)
GOLD_COLUMNS = column_names(GOLD_CONTRACT)
REQUIRED_GOLD_COLUMNS = required_column_names(GOLD_CONTRACT)
CONTENT_DIGEST_COLUMNS = tuple(c for c in GOLD_COLUMNS if c not in
                              {"row_digest", "source_snapshot_id", "published_at_utc"})
TITLE_SOURCE = "silver.building_register_titles"
FLOOR_SOURCE = "silver.building_register_floors"
UNIT_SOURCE = "silver.building_register_units"
AREA_SOURCE = "silver.building_register_unit_areas"
PRICE_SOURCE = "silver.unit_official_price"
ALL_SOURCES = (TITLE_SOURCE, FLOOR_SOURCE, UNIT_SOURCE, AREA_SOURCE, PRICE_SOURCE)
R2_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "config" / "r2-connections.contract.json"
PNU_PATTERN = "^(?:" + json.loads(R2_CONTRACT_PATH.read_text(encoding="utf-8"))["building_by_pnu_gateway"]["object_key"]["pnu_pattern"] + ")$"
JSON_OPTIONS = {"ignoreNullFields": "false"}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-mode", choices=("parquet", "iceberg"), default="iceberg")
    parser.add_argument("--input-root")
    parser.add_argument("--output")
    parser.add_argument("--write-mode", choices=("parquet", "iceberg"), default="parquet")
    parser.add_argument("--iceberg-catalog-name", default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_CATALOG_NAME", "r2"))
    parser.add_argument("--source-iceberg-namespace", default="silver")
    parser.add_argument("--target-iceberg-namespace", default="gold")
    parser.add_argument("--target-iceberg-table", default="building_panel")
    parser.add_argument("--iceberg-write-mode", choices=("append", "overwrite"), default="overwrite")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    parser.add_argument("--iceberg-snapshot-id", required=True,
                        help="Title-table Iceberg snapshot to read; local source identity in parquet mode.")
    parser.add_argument("--source-snapshots-path", help="JSON file mapping all Silver tables to pinned Iceberg snapshot IDs.")
    parser.add_argument("--published-at-utc")
    parser.add_argument("--region-prefix")
    parser.add_argument("--pnu-prefix")
    parser.add_argument("--allow-non-smoke-overwrite", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--expected-count", type=int)
    parser.add_argument("--summary-output")
    parser.add_argument("--lineage-output")
    return parser.parse_args(argv)


def validate_args(args):
    if args.input_mode == "parquet" and not args.input_root:
        raise ValueError("--input-root is required when --input-mode=parquet")
    if args.write_mode == "parquet" and not args.output:
        raise ValueError("--output is required when --write-mode=parquet")
    for name in ("region_prefix", "pnu_prefix"):
        value = getattr(args, name)
        if value is not None and not re.fullmatch(r"[0-9]{1,10}", value):
            raise ValueError(f"--{name.replace('_', '-')} must be 1 to 10 digits")
    if args.region_prefix and args.pnu_prefix and not args.pnu_prefix.startswith(args.region_prefix):
        raise ValueError("--pnu-prefix must be inside --region-prefix")
    for name in ("iceberg_catalog_name", "source_iceberg_namespace", "target_iceberg_namespace", "target_iceberg_table"):
        validate_identifier(name, getattr(args, name))
    if args.expected_count is not None and args.expected_count < 0:
        raise ValueError("--expected-count must be non-negative")
    if (args.write_mode == "iceberg" and args.iceberg_write_mode == "overwrite" and
            not args.validate_only and not args.target_iceberg_table.endswith("_smoke") and
            not args.allow_non_smoke_overwrite):
        raise ValueError("Refusing non-smoke overwrite without --allow-non-smoke-overwrite")
    if args.input_mode == "iceberg" or args.write_mode == "iceberg":
        assert_catalog_env(args.iceberg_catalog_name)
    if args.source_snapshots_path:
        load_snapshot_pins(args)


def load_snapshot_pins(args):
    if not args.source_snapshots_path:
        return {TITLE_SOURCE: str(int(args.iceberg_snapshot_id))} if args.input_mode == "iceberg" else {}
    pins = json.loads(Path(args.source_snapshots_path).read_text(encoding="utf-8"))
    if not isinstance(pins, dict) or set(pins) != set(ALL_SOURCES):
        raise ValueError("--source-snapshots-path must name every Silver source exactly once")
    if any(not re.fullmatch(r"[1-9][0-9]*", str(v)) for v in pins.values()):
        raise ValueError("source snapshot IDs must be positive integers")
    if str(pins[TITLE_SOURCE]) != args.iceberg_snapshot_id:
        raise ValueError("title snapshot pin disagrees with --iceberg-snapshot-id")
    return pins


def read_source(spark, args, name, pins):
    contract = load_lakehouse_contract(name)
    table = name.split(".", 1)[1]
    if args.input_mode == "parquet":
        frame = spark.read.parquet(str(Path(args.input_root) / table))
    else:
        reader = spark.read.format("iceberg")
        if name in pins:
            reader = reader.option("snapshot-id", str(pins[name]))
        frame = reader.load(f"{args.iceberg_catalog_name}.{args.source_iceberg_namespace}.{table}")
    missing = set(column_names(contract)) - set(frame.columns)
    if missing:
        raise ValueError(f"{name} input is missing columns: {sorted(missing)}")
    frame = frame.select(*column_names(contract))
    predicate = current_row_predicate(contract)
    return frame.where(F.expr(predicate)) if predicate else frame


def assert_single_snapshot(frame, name):
    values = [r.source_snapshot_id for r in frame.select("source_snapshot_id").distinct().limit(3).collect()]
    if len(values) != 1 or not values[0]:
        raise ValueError(f"{name} must carry exactly one non-empty source_snapshot_id: {values}")
    return values[0]


def assert_unique(frame, keys, name, allow_empty=False):
    if frame.groupBy(*keys).count().where(F.col("count") > 1).limit(1).count():
        raise ValueError(f"{name} has duplicate identity {keys}")
    invalid = " OR ".join(f"`{k}` IS NULL" + ("" if allow_empty else f" OR trim(cast(`{k}` as string)) = ''") for k in keys)
    if frame.where(F.expr(invalid)).limit(1).count():
        raise ValueError(f"{name} has an absent identity {keys}")


def canonical_id(kind, key):
    """Catalog-domain UUIDv8 projection; every result is checked again by the Rust baker."""
    digest = F.sha2(F.concat(F.lit(f"perfectory.catalog.{kind}.v1\x00"), key), 256)
    variant = F.lower(F.conv((F.conv(F.substring(digest, 17, 1), 16, 10).cast("int").bitwiseAND(3) + 8).cast("string"), 10, 16))
    return F.concat_ws("-", F.substring(digest, 1, 8), F.substring(digest, 9, 4),
                       F.concat(F.lit("8"), F.substring(digest, 14, 3)),
                       F.concat(variant, F.substring(digest, 18, 3)), F.substring(digest, 21, 12))


def text_or_null(name):
    value = F.trim(F.col(name))
    return F.when(F.length(value) > 0, value)


def sorted_objects(frame, keys, fields, output):
    # struct field order defines a complete, stable lexicographic ordering.
    return frame.groupBy(*keys).agg(F.sort_array(F.collect_list(F.struct(*fields))).alias(output))


def valid_pnus(frame, name, counters):
    absent = F.col("pnu").isNull() | (F.length(F.trim("pnu")) == 0)
    counters[f"{name.split('.')[-1]}_null_pnu_count"] = frame.where(absent).count()
    valid = frame.where(~absent).withColumn("pnu", F.trim("pnu"))
    if valid.where(~F.col("pnu").rlike(PNU_PATTERN)).limit(1).count():
        raise ValueError(f"{name} has an invalid cadastral PNU")
    return valid


def unit_areas(areas):
    exclusive = areas.where(F.col("area_kind") == "exclusive")
    # Decimal accumulation avoids floating-point shuffle-order drift in exact JSON bytes.
    sums = exclusive.groupBy("mgm_bldrgst_pk").agg(F.sum(F.col("area_m2").cast("decimal(38,6)")).cast("double").alias("exclusive_area_m2"))
    order = Window.partitionBy("mgm_bldrgst_pk").orderBy(F.col("area_m2").desc_nulls_last(),
             F.col("usage_name_raw").asc_nulls_last(), F.col("structure_name_raw").asc_nulls_last(), "area_row_id")
    top = exclusive.withColumn("_rank", F.row_number().over(order)).where("_rank = 1").select(
        "mgm_bldrgst_pk", F.col("usage_name_raw").alias("usage_name"), F.col("structure_name_raw").alias("structure_name"))
    return sums.join(top, "mgm_bldrgst_pk")


def build_units(units, titles, areas, prices):
    links = titles.select("pnu", F.col("mgm_bldrgst_pk").alias("building_register_pk"), F.col("id").alias("building_id"))
    units = units.withColumn("building_register_pk", text_or_null("building_mgm_bldrgst_pk"))
    joined = units.join(links, ["pnu", "building_register_pk"], "left").join(unit_areas(areas), "mgm_bldrgst_pk", "left")
    number, kind = F.col("floor_number"), F.col("floor_kind")
    label = (F.when(number.isNull(), F.lit(""))
             .when(kind == "basement", F.concat(F.lit("지하 "), number.cast("string"), F.lit("층")))
             .when(kind == "rooftop", F.lit("옥탑"))
             .otherwise(F.concat(number.cast("string"), F.lit("층"))))
    projected = joined.select("pnu", "building_register_pk", "building_id",
        F.col("mgm_bldrgst_pk").alias("register_pk"), canonical_id("building_unit", F.col("mgm_bldrgst_pk")).alias("id"),
        canonical_id("parcel", F.col("pnu")).alias("parcel_id"),
        F.coalesce("dong_join_name", F.lit("")).alias("building_name"),
        F.coalesce("dong_name_raw", F.lit("")).alias("dong_name"),
        F.coalesce("unit_label_ko", "unit_name_raw", F.lit("")).alias("ho_name"), label.alias("floor_label"),
        "exclusive_area_m2", F.coalesce("usage_name", F.lit("")).alias("usage_name"),
        F.coalesce("structure_name", F.lit("")).alias("structure_name"))
    assert_unique(prices, ["pnu", "dong_name", "ho_name", "base_year"], PRICE_SOURCE, allow_empty=True)
    if prices.where((F.col("base_year") <= 0) | (F.col("base_year") > 32767) | (F.col("price_won") < 0) |
                    F.col("base_year").isNull() | F.col("price_won").isNull()).limit(1).count():
        raise ValueError("unit official price has an invalid year or negative price")
    histories = prices.groupBy("pnu", "dong_name", "ho_name").agg(
        F.sort_array(F.collect_list(F.struct("base_year", "price_won")), asc=False).alias("official_price_history"))
    # Exact name matching is the Catalog API's unit_responses contract.
    result = projected.join(histories, ["pnu", "dong_name", "ho_name"], "left")
    return result.withColumn("official_price_history", F.coalesce("official_price_history", F.array()))


def build_gold_panel_frame(frames, source_snapshot_id, published_at_utc, counters):
    titles = valid_pnus(frames[TITLE_SOURCE], TITLE_SOURCE, counters)
    units = valid_pnus(frames[UNIT_SOURCE], UNIT_SOURCE, counters)
    areas, floors = frames[AREA_SOURCE], frames[FLOOR_SOURCE]
    for frame, keys, name in ((titles, ["mgm_bldrgst_pk"], TITLE_SOURCE),
                             (units, ["mgm_bldrgst_pk"], UNIT_SOURCE),
                             (floors, ["floor_row_id"], FLOOR_SOURCE),
                             (areas, ["area_row_id"], AREA_SOURCE)):
        assert_unique(frame, keys, name)
    for frame, columns in ((titles, ["floor_area_m2"]), (areas, ["area_m2"])):
        for column in columns:
            if frame.where(F.isnan(column) | (F.abs(F.col(column)) == float("inf")) | (F.col(column) < 0)).limit(1).count():
                raise ValueError(f"{column} must be finite and non-negative")
    for column in ("ground_floor_count", "basement_floor_count"):
        if titles.where((F.col(column) < 0) | (F.col(column) > 32767)).limit(1).count():
            raise ValueError(f"{column} must fit the canonical floor count")
    titles = titles.withColumn("id", canonical_id("building", F.col("mgm_bldrgst_pk")))
    counters["unlinked_floor_count"] = floors.join(titles.select("mgm_bldrgst_pk"), "mgm_bldrgst_pk", "left_anti").count()
    projected_units = build_units(units, titles, areas, frames[PRICE_SOURCE])
    counters["unlinked_unit_count"] = projected_units.where(F.col("building_id").isNull()).count()
    unit_fields = ["dong_name", "ho_name", "id", "parcel_id", "building_id", "register_pk",
                   "building_name", "floor_label", "exclusive_area_m2", "usage_name", "structure_name", "official_price_history"]
    nested_units = sorted_objects(projected_units.where(F.col("building_id").isNotNull()),
                                  ["pnu", "building_id"], unit_fields, "units")
    unlinked = sorted_objects(projected_units.where(F.col("building_id").isNull()), ["pnu"], unit_fields, "unlinked_units")
    floor_fields = ["floor_row_id", "floor_kind", "floor_number", "floor_index", "floor_display_ko"]
    nested_floors = sorted_objects(floors, ["mgm_bldrgst_pk"], floor_fields, "floors")
    roof = floors.groupBy("mgm_bldrgst_pk").agg(F.max(F.when(F.col("floor_kind") == "rooftop", 1).otherwise(0)).alias("_roof"))
    # Common rooftop rows may name their title directly or an exclusive register linked to it.
    area_links = units.select(F.col("mgm_bldrgst_pk").alias("_unit_pk"), F.col("building_mgm_bldrgst_pk").alias("_title_pk"))
    roof_areas = areas.where((F.col("area_kind") == "common") & (F.col("floor_kind") == "rooftop"))
    roof_areas = roof_areas.join(area_links, roof_areas.mgm_bldrgst_pk == area_links._unit_pk, "left").withColumn(
        "_building_pk", F.coalesce("_title_pk", "mgm_bldrgst_pk"))
    roof_areas = roof_areas.groupBy("_building_pk").agg(
        F.sum(F.col("area_m2").cast("decimal(38,6)")).cast("double").alias("rooftop_area_m2"),
        F.concat_ws(" · ", F.sort_array(F.collect_set(text_or_null("usage_name_raw")))).alias("rooftop_usage"))
    buildings = titles.join(nested_floors, "mgm_bldrgst_pk", "left").join(roof, "mgm_bldrgst_pk", "left")
    buildings = buildings.join(roof_areas, buildings.mgm_bldrgst_pk == roof_areas._building_pk, "left")
    buildings = buildings.join(nested_units, (buildings.pnu == nested_units.pnu) & (buildings.id == nested_units.building_id), "left").drop(nested_units.pnu).drop(nested_units.building_id)
    buildings = buildings.select("pnu", "id", canonical_id("parcel", F.col("pnu")).alias("parcel_id"),
        F.col("mgm_bldrgst_pk").alias("register_pk"), text_or_null("purpose_code_raw").alias("purpose_code"),
        text_or_null("structure_code_raw").alias("structure_code"),
        F.when(F.col("floor_area_m2") > 0, F.col("floor_area_m2")).alias("floor_area_m2"),
        F.col("ground_floor_count").alias("stories"), F.coalesce("basement_floor_count", F.lit(0)).alias("below_ground_floors"),
        (F.coalesce("_roof", F.lit(0)) == 1).alias("has_rooftop"), "rooftop_area_m2",
        F.coalesce("rooftop_usage", F.lit("")).alias("rooftop_usage"), F.col("approval_year").alias("built_year"),
        F.coalesce("floors", F.array()).alias("floors"), F.coalesce("units", F.array()).alias("units"))
    fields = ["register_pk"] + [c for c in buildings.columns if c not in ("pnu", "register_pk")]
    grouped = sorted_objects(buildings, ["pnu"], fields, "buildings")
    pnus = titles.select("pnu").union(units.select("pnu")).distinct()
    gold = pnus.join(grouped, "pnu", "left").join(unlinked, "pnu", "left").select("pnu",
        F.to_json(F.coalesce("buildings", F.array()), JSON_OPTIONS).alias("buildings_json"),
        F.to_json(F.coalesce("unlinked_units", F.array()), JSON_OPTIONS).alias("unlinked_units_json"),
        F.lit(source_snapshot_id).alias("source_snapshot_id"), F.lit(published_at_utc).alias("published_at_utc"))
    return gold.withColumn("row_digest", F.sha2(F.to_json(F.struct(*CONTENT_DIGEST_COLUMNS), JSON_OPTIONS), 256)).select(*GOLD_COLUMNS)


def validate_gold_frame(gold, expected_count):
    if tuple(gold.columns) != GOLD_COLUMNS:
        raise ValueError("Gold columns disagree with the lakehouse contract")
    assert_unique(gold, ["pnu"], GOLD_CONTRACT_NAME)
    checks = [F.count(F.lit(1)).alias("row_count")]
    checks += [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c + "__null_count") for c in REQUIRED_GOLD_COLUMNS]
    checks += [F.sum(F.when(~F.col("pnu").rlike(PNU_PATTERN), 1).otherwise(0)).alias("invalid_pnu_count")]
    row = gold.agg(*checks).first()
    metrics = {k: int(v or 0) for k, v in row.asDict().items()}
    if any(v for k, v in metrics.items() if k != "row_count"):
        raise ValueError(f"Gold quality gates failed: {metrics}")
    if expected_count is not None and metrics["row_count"] != expected_count:
        raise ValueError(f"Expected {expected_count} Gold rows, found {metrics['row_count']}")
    return metrics["row_count"], metrics


def emit_json(value, path, label):
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(payload + "\n", encoding="utf-8")
    print(f"{label} {payload}")


def build_lineage_event(args: argparse.Namespace, summary: dict[str, Any]) -> dict[str, Any]:
    payload = json.dumps(summary, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return {
        "schema_version": "foundation-platform.lakehouse_lineage_event.v1",
        "event_type": "lakehouse.lineage.dataset_materialized.v1",
        "occurred_at": summary["created_at_utc"],
        "producer": "foundation-platform.lakehouse",
        "job_name": JOB_NAME,
        "run_id": f"{JOB_NAME}:{summary['created_at_utc']}",
        "run_summary_schema_version": RUN_SUMMARY_SCHEMA_VERSION,
        "run_summary_ref": {
            "path": args.summary_output or "stdout",
            "checksum_sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        },
        "input_dataset": {
            "qualified_name": TITLE_SOURCE,
            "namespace": "silver",
            "table": TITLE_SOURCE.split(".", 1)[1],
            "storage_format": args.input_mode,
        },
        "additional_input_datasets": [
            {
                "qualified_name": name,
                "namespace": "silver",
                "table": name.split(".", 1)[1],
                "storage_format": args.input_mode,
            }
            for name in ALL_SOURCES[1:]
        ],
        "output_dataset": {
            "qualified_name": GOLD_CONTRACT_NAME,
            "namespace": "gold",
            "table": "building_panel",
            "storage_format": args.write_mode,
        },
        "source_snapshot_ids": summary["source_snapshot_ids"],
        "source_snapshot_truncated": False,
        "iceberg_snapshot": {
            "catalog": args.iceberg_catalog_name,
            "namespace": args.target_iceberg_namespace,
            "table": args.target_iceberg_table,
            "snapshot_id": args.iceberg_snapshot_id,
        },
        "quality_metrics": {
            **summary["quality_metrics"],
            "row_count": int(summary["row_count"]),
        },
        "column_lineage": [{"output_column": c, "inputs": [{"dataset": n, "column": "*", "transform": "building_panel"} for n in ALL_SOURCES]} for c in GOLD_COLUMNS],
        "openlineage_mapping": {
            "event_type": "COMPLETE",
            "job_namespace": "foundation-platform.lakehouse",
            "job_name": JOB_NAME,
            "input_namespace": f"{args.iceberg_catalog_name}.silver",
            "output_namespace": f"{args.iceberg_catalog_name}.gold",
        },
    }



def main():
    args = parse_args()
    validate_args(args)
    args.published_at_utc = normalize_utc_timestamp(args.published_at_utc)
    builder = SparkSession.builder.appName(JOB_NAME).config("spark.sql.session.timeZone", "UTC")
    if args.input_mode == "iceberg" or args.write_mode == "iceberg":
        builder = apply_catalog_settings(builder, args.iceberg_catalog_name)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if args.input_mode == "iceberg" or args.write_mode == "iceberg":
        assert_iceberg_runtime_loaded(spark, args.iceberg_packages)
    gold = None
    try:
        pins, frames, snapshots, counters = load_snapshot_pins(args), {}, {}, {}
        for name in ALL_SOURCES:
            frame = read_source(spark, args, name, pins)
            snapshots[name] = assert_single_snapshot(frame, name)
            prefix = args.pnu_prefix or args.region_prefix
            if prefix and "pnu" in frame.columns:
                frame = frame.where(F.col("pnu").startswith(prefix))
            frames[name] = frame
        if args.pnu_prefix or args.region_prefix:
            frames[FLOOR_SOURCE] = frames[FLOOR_SOURCE].join(frames[TITLE_SOURCE].select("mgm_bldrgst_pk").distinct(), "mgm_bldrgst_pk", "left_semi")
        source_id = hashlib.sha256(json.dumps(snapshots, sort_keys=True).encode()).hexdigest()
        gold = build_gold_panel_frame(frames, source_id, args.published_at_utc, counters).persist(StorageLevel.MEMORY_AND_DISK)
        count, metrics = validate_gold_frame(gold, args.expected_count)
        persisted_count, added = None, ()
        target_table = f"`{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`.`{args.target_iceberg_table}`"
        if not args.validate_only:
            if args.write_mode == "parquet":
                (gold.repartition(*partition_column_names(GOLD_CONTRACT)).sortWithinPartitions(*sort_order(GOLD_CONTRACT))
                 .write.mode("overwrite").partitionBy(*partition_column_names(GOLD_CONTRACT)).parquet(args.output))
                persisted = spark.read.parquet(args.output).select(*GOLD_COLUMNS)
            else:
                spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`")
                spark.sql(f"CREATE TABLE IF NOT EXISTS {target_table} ({create_table_columns_sql(GOLD_CONTRACT)}) USING iceberg {partition_clause_sql(GOLD_CONTRACT)} TBLPROPERTIES ('format-version'='2','write.parquet.compression-codec'='zstd','write.distribution-mode'='hash')")
                added = evolve_iceberg_table_to_contract(spark, target_table, GOLD_CONTRACT)
                gold.createOrReplaceTempView("gold_building_panel_candidate")
                statement = "INSERT OVERWRITE" if args.iceberg_write_mode == "overwrite" else "INSERT INTO"
                spark.sql(f"{statement} {target_table} SELECT {', '.join(GOLD_COLUMNS)} FROM gold_building_panel_candidate")
                persisted = spark.table(target_table).select(*GOLD_COLUMNS)
            persisted_count, metrics = validate_gold_frame(persisted, count)
        summary = {
            "schema_version": RUN_SUMMARY_SCHEMA_VERSION, "job_name": JOB_NAME, "contract": GOLD_CONTRACT_NAME,
            "created_at_utc": normalize_utc_timestamp(None),
            "input": {"kind": args.input_mode, "sources": list(ALL_SOURCES), "region_prefix": args.region_prefix, "pnu_prefix": args.pnu_prefix},
            "target": {"kind": args.write_mode, "path": args.output} if args.write_mode == "parquet" else
                      {"kind": "iceberg", "catalog": args.iceberg_catalog_name, "namespace": args.target_iceberg_namespace, "table": args.target_iceberg_table},
            "write_mode": args.write_mode, "write_disposition": "validate_only" if args.validate_only else ("parquet_overwrite" if args.write_mode == "parquet" else "iceberg_" + args.iceberg_write_mode),
            "row_count": count, "persisted_row_count": persisted_count, "quality_metrics": {**metrics, **counters},
            "columns": list(GOLD_COLUMNS), "column_count": len(GOLD_COLUMNS), "required_columns": list(REQUIRED_GOLD_COLUMNS),
            "schema_evolution_added_columns": list(added), "source_snapshot_count": len(snapshots),
            "source_snapshot_ids": [snapshots[n] for n in sorted(snapshots)], "source_snapshots_by_dataset": snapshots,
            "source_snapshot_truncated": False,
        }
        emit_json(summary, args.summary_output, "gold-building-panel-summary-json")
        if args.lineage_output and not args.validate_only:
            emit_json(build_lineage_event(args, summary), args.lineage_output,
                      "gold-building-panel-lineage-json")
        return 0
    finally:
        if gold is not None:
            gold.unpersist()
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
