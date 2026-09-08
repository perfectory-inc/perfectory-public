#!/usr/bin/env python3
"""Reindex one province's apartment assessments by unit identity (root ADR-0095).

The coordinator measured the Seoul join at 17 seconds, 29,070,232 unit-year rows
and 1,872,891 units. One invocation handles one two-digit province; the operator
runs the seventeen invocations. Source snapshots stay immutable and untouched.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path

from lakehouse_engine import apply_catalog_settings, assert_catalog_env, assert_iceberg_runtime_loaded, iceberg_packages
from lakehouse_ingest import append_batch_once
from platform_contracts import column_names, create_table_columns_sql, load_lakehouse_contract, partition_clause_sql

CONTRACT = "silver.unit_official_price"
# Shared SQL is executed by both Spark and relational fixture tests.
DICTIONARY_SQL = """
SELECT DISTINCT mgmt_key, pnu, COALESCE(dong_name, '') AS dong_name,
       COALESCE(ho_name, '') AS ho_name
FROM exclusive_source
WHERE mgmt_key IS NOT NULL AND TRIM(mgmt_key) <> ''
"""
CONFLICTING_SQL = """
SELECT COUNT(*) AS conflicting FROM (
    SELECT mgmt_key FROM unit_dictionary GROUP BY mgmt_key HAVING COUNT(*) > 1
) conflicts
"""
PRICE_VALID = """mgmt_key IS NOT NULL AND TRIM(mgmt_key) <> ''
    AND TRIM(base_date) REGEXP '^[0-9]{8}$'
    AND CAST(SUBSTR(TRIM(base_date), 1, 4) AS INT) BETWEEN 1000 AND 9999
    AND TRIM(price_won) REGEXP '^[0-9]{1,18}$'"""
PRICES_SQL = f"""
SELECT DISTINCT mgmt_key, TRIM(base_date) AS base_date,
       CAST(TRIM(price_won) AS BIGINT) AS price_won
FROM price_source WHERE {PRICE_VALID}
"""
JOIN_SQL = """
SELECT DISTINCT d.pnu, d.dong_name, d.ho_name,
       CAST(SUBSTR(p.base_date, 1, 4) AS INT) AS base_year, p.price_won
FROM annual_prices p JOIN unit_dictionary d ON p.mgmt_key = d.mgmt_key
WHERE d.pnu REGEXP '^[0-9]{19}$'
"""


def refuse_conflicting(conflicting: int) -> None:
    if conflicting:
        raise ValueError(f"management keys name multiple units: conflicting={conflicting}")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sido", required=True, help="Exactly one two-digit province.")
    parser.add_argument("--vintage")
    parser.add_argument("--iceberg-catalog-name", default="r2")
    parser.add_argument("--price-snapshot-id", type=int)
    parser.add_argument("--exclusive-snapshot-id", type=int)
    parser.add_argument("--summary-output")
    args = parser.parse_args(argv)
    if args.vintage is None:
        contracts_dir = Path(__file__).resolve().parents[2] / "contracts"
        selected = {
            json.loads((contracts_dir / f"hub-building-register-{name}-source-objects.json").read_text(encoding="utf-8"))["selected_vintage"]
            for name in ("apartment-price", "exclusive-unit")
        }
        if len(selected) != 1:
            parser.error("source contracts select different vintages; specify --vintage")
        args.vintage = selected.pop()
    for value, pattern, name in [
        (args.sido, r"[0-9]{2}", "sido"),
        (args.vintage, r"[0-9]{6}", "vintage"),
        (args.iceberg_catalog_name, r"[A-Za-z_][A-Za-z0-9_]*", "catalog"),
    ]:
        if not re.fullmatch(pattern, value):
            parser.error(f"invalid {name}")
    for snapshot in (args.price_snapshot_id, args.exclusive_snapshot_id):
        if snapshot is not None and snapshot <= 0:
            parser.error("snapshot ids must be positive")
    return args


def read_snapshot(spark, qualified_table, requested):
    snapshot = requested
    if snapshot is None:
        row = spark.sql(f"SELECT snapshot_id FROM {qualified_table}.refs WHERE name = 'main'").first()
        if row is None:
            raise ValueError(f"no current snapshot for {qualified_table}")
        snapshot = int(row[0])
    return spark.read.format("iceberg").option("snapshot-id", str(snapshot)).load(qualified_table), snapshot


def main(argv=None):
    args = parse_args(argv)
    from pyspark.sql import SparkSession, functions as F

    assert_catalog_env()
    started = time.monotonic()
    builder = (SparkSession.builder.appName("foundation-platform-unit-official-price")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.autoBroadcastJoinThreshold", "-1")
               .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
               .config("spark.sql.shuffle.partitions", "64"))
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    try:
        assert_iceberg_runtime_loaded(spark, iceberg_packages())
        prefix = f"`{args.iceberg_catalog_name}`.`silver`"
        exclusive, exclusive_snapshot = read_snapshot(spark, f"{prefix}.`building_register_exclusive_unit`", args.exclusive_snapshot_id)
        prices, price_snapshot = read_snapshot(spark, f"{prefix}.`building_register_apartment_price`", args.price_snapshot_id)
        exclusive.where(F.col("vintage") == args.vintage).createOrReplaceTempView("exclusive_source")
        # Check the entire dictionary first: a key split across provinces must also be refused.
        dictionary = spark.sql(DICTIONARY_SQL)
        dictionary.createOrReplaceTempView("unit_dictionary")
        conflicting = int(spark.sql(CONFLICTING_SQL).first()[0])
        print(json.dumps({"sido": args.sido, "conflicting": conflicting}), flush=True)
        refuse_conflicting(conflicting)
        dictionary.where(F.substring("pnu", 1, 2) == args.sido).createOrReplaceTempView("unit_dictionary")
        price_source = prices.where((F.col("vintage") == args.vintage) & (F.substring("sigungu_cd", 1, 2) == args.sido))
        price_source.createOrReplaceTempView("price_source")
        annual_prices = spark.sql(PRICES_SQL)
        annual_prices.createOrReplaceTempView("annual_prices")
        invalid = int(spark.sql(f"SELECT COUNT(*) FROM price_source WHERE COALESCE(({PRICE_VALID}), FALSE) = FALSE").first()[0])
        unmatched = int(spark.sql("""SELECT COUNT(*) FROM annual_prices p
            LEFT JOIN unit_dictionary d ON p.mgmt_key = d.mgmt_key
            WHERE d.mgmt_key IS NULL OR d.pnu IS NULL OR NOT (d.pnu REGEXP '^[0-9]{19}$')""").first()[0])
        source_snapshot = f"price:{price_snapshot}|exclusive:{exclusive_snapshot}|vintage:{args.vintage}"
        source_record = f"unit-official-price/{args.sido}/{source_snapshot}"
        frame = (spark.sql(JOIN_SQL)
                 .withColumn("sido", F.lit(args.sido))
                 .withColumn("source_snapshot_id", F.lit(source_snapshot))
                 .withColumn("source_record_id", F.lit(source_record)))
        row_count = frame.count()
        if row_count == 0:
            raise ValueError("province produced no price rows; refusing an empty append")
        contract = load_lakehouse_contract(CONTRACT)
        target = f"{prefix}.`unit_official_price`"
        spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({create_table_columns_sql(contract)}) USING iceberg {partition_clause_sql(contract)}")
        appended = append_batch_once(spark, frame, column_names(contract), target, CONTRACT)
        summary = {
            "sido": args.sido, "source_snapshot_id": source_snapshot,
            "price_snapshot_id": price_snapshot, "exclusive_snapshot_id": exclusive_snapshot,
            "rows": row_count, "conflicting": conflicting, "invalid_prices": invalid,
            "unmatched_prices": unmatched, "elapsed_seconds": time.monotonic() - started,
            **appended,
        }
        text = json.dumps(summary, ensure_ascii=False, indent=2) + "\n"
        if args.summary_output:
            Path(args.summary_output).write_text(text, encoding="utf-8")
        print(text, flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
