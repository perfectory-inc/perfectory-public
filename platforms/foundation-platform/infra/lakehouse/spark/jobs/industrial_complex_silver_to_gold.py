#!/usr/bin/env python3
"""Build the Gold industrial-complex catalog projection from Silver rows."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.storagelevel import StorageLevel

from platform_contracts import (
    column_names,
    create_table_columns_sql,
    load_lakehouse_contract,
    partition_spec_sql,
    required_column_names,
    required_string_column_names,
)


DEFAULT_ICEBERG_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
    "org.apache.iceberg:iceberg-aws-bundle:1.6.1"
)
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
JOB_NAME = "industrial_complex_silver_to_gold"
RUN_SUMMARY_SCHEMA_VERSION = "foundation-platform.spark_run_summary.v1"
LINEAGE_EVENT_SCHEMA_VERSION = "foundation-platform.lakehouse_lineage_event.v1"
LINEAGE_EVENT_TYPE = "lakehouse.lineage.dataset_materialized.v1"
SILVER_CONTRACT_NAME = "silver.industrial_complexes"
GOLD_CONTRACT_NAME = "gold.complex_catalog"
SILVER_CONTRACT = load_lakehouse_contract(SILVER_CONTRACT_NAME)
GOLD_CONTRACT = load_lakehouse_contract(GOLD_CONTRACT_NAME)
SILVER_COLUMNS: tuple[str, ...] = column_names(SILVER_CONTRACT)
GOLD_COLUMNS: tuple[str, ...] = column_names(GOLD_CONTRACT)
REQUIRED_GOLD_COLUMNS: tuple[str, ...] = required_column_names(GOLD_CONTRACT)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build gold.complex_catalog from silver.industrial_complexes."
    )
    parser.add_argument(
        "--input-mode",
        choices=("jsonl", "parquet", "iceberg"),
        default="jsonl",
        help="Read Silver rows from JSONL, Parquet, or an Iceberg REST catalog table.",
    )
    parser.add_argument("--input", help="Silver input path for jsonl/parquet input modes.")
    parser.add_argument(
        "--output",
        help="Gold Parquet output path when --write-mode=parquet.",
    )
    parser.add_argument(
        "--write-mode",
        choices=("parquet", "iceberg"),
        default="parquet",
        help="Write local Parquet or an Iceberg REST catalog table.",
    )
    parser.add_argument(
        "--iceberg-catalog-name",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_CATALOG_NAME", "r2"),
        help="Spark catalog name for Iceberg REST catalog reads and writes.",
    )
    parser.add_argument(
        "--source-iceberg-namespace",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_GOLD_SOURCE_NAMESPACE", "silver"),
        help="Iceberg namespace for the source Silver table.",
    )
    parser.add_argument(
        "--source-iceberg-table",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_GOLD_SOURCE_TABLE", "industrial_complexes"),
        help="Iceberg table name for the source Silver table.",
    )
    parser.add_argument(
        "--target-iceberg-namespace",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_GOLD_TARGET_NAMESPACE", "gold"),
        help="Iceberg namespace for the target Gold table.",
    )
    parser.add_argument(
        "--target-iceberg-table",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_GOLD_TARGET_TABLE", "complex_catalog"),
        help="Iceberg table name for the target Gold table.",
    )
    parser.add_argument(
        "--iceberg-write-mode",
        choices=("append", "overwrite"),
        default="overwrite",
        help="How Gold rows are written to the Iceberg table.",
    )
    parser.add_argument(
        "--iceberg-packages",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_PACKAGES", DEFAULT_ICEBERG_PACKAGES),
        help="Comma-separated Iceberg Spark packages used for REST catalog writes.",
    )
    parser.add_argument(
        "--iceberg-snapshot-id",
        required=True,
        help="Iceberg snapshot id represented by this Gold projection.",
    )
    parser.add_argument(
        "--published-at-utc",
        default=None,
        help="Publication timestamp in UTC. Defaults to current UTC time.",
    )
    parser.add_argument(
        "--boundary-object-key",
        default=None,
        help="Optional object key for the boundary artifact represented by this Gold projection.",
    )
    parser.add_argument(
        "--allow-non-smoke-overwrite",
        action="store_true",
        help="Allow overwrite mode for target tables whose names do not end with _smoke.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate input, target config, and Gold quality gates without writing.",
    )
    parser.add_argument(
        "--expected-count",
        type=int,
        default=None,
        help="Optional row-count assertion for smoke tests.",
    )
    parser.add_argument(
        "--summary-output",
        help="Optional path for a machine-readable Spark run summary JSON file.",
    )
    parser.add_argument(
        "--lineage-output",
        help="Optional path for a machine-readable lakehouse lineage event JSON file.",
    )
    return parser.parse_args()


def required_iceberg_env() -> tuple[str, ...]:
    return (
        "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI",
        "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE",
        "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN",
    )


def require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value.strip()


def lakehouse_oauth2_server_uri(catalog_uri: str) -> str:
    configured_uri = os.getenv("FOUNDATION_PLATFORM_LAKEHOUSE_OAUTH2_SERVER_URI")
    if configured_uri is not None and configured_uri.strip() != "":
        return configured_uri.strip()
    return f"{catalog_uri.rstrip('/')}/v1/oauth/tokens"


def validate_identifier(label: str, value: str) -> None:
    if IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{label} must be a simple identifier: {value}")


def trim_isoformat_fraction_to_microseconds(value: str) -> str:
    match = re.fullmatch(
        r"(?P<prefix>.+T\d{2}:\d{2}:\d{2})\.(?P<fraction>\d{7,})(?P<suffix>Z|[+-]\d{2}:\d{2})",
        value,
    )
    if match is None:
        return value
    fraction = match.group("fraction")
    return f"{match.group('prefix')}.{fraction[:6]}{match.group('suffix')}"


def normalize_utc_timestamp(value: str | None) -> str:
    if value is None or value.strip() == "":
        return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    raw = trim_isoformat_fraction_to_microseconds(value.strip())
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("--published-at-utc must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def validate_args(args: argparse.Namespace) -> None:
    if args.summary_output is not None and args.summary_output.strip() == "":
        raise ValueError("--summary-output must not be empty")
    if args.lineage_output is not None and args.lineage_output.strip() == "":
        raise ValueError("--lineage-output must not be empty")

    if args.input_mode in ("jsonl", "parquet") and not args.input:
        raise ValueError("--input is required when --input-mode is jsonl or parquet")

    if args.write_mode == "parquet" and not args.output:
        raise ValueError("--output is required when --write-mode=parquet")

    validate_identifier("iceberg catalog name", args.iceberg_catalog_name)
    validate_identifier("source iceberg namespace", args.source_iceberg_namespace)
    validate_identifier("source iceberg table", args.source_iceberg_table)
    validate_identifier("target iceberg namespace", args.target_iceberg_namespace)
    validate_identifier("target iceberg table", args.target_iceberg_table)

    if args.write_mode == "iceberg" and args.iceberg_write_mode == "overwrite":
        is_smoke_table = args.target_iceberg_table.endswith("_smoke")
        if not is_smoke_table and not args.allow_non_smoke_overwrite:
            raise ValueError(
                "Refusing to overwrite a non-smoke Iceberg table without "
                "--allow-non-smoke-overwrite"
            )

    if args.input_mode == "iceberg" or args.write_mode == "iceberg":
        for name in required_iceberg_env():
            require_env(name)


def read_silver_input(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    if args.input_mode == "jsonl":
        frame = spark.read.json(args.input)
    elif args.input_mode == "parquet":
        frame = spark.read.parquet(args.input)
    else:
        frame = spark.table(qualified_source_table(args))

    missing_columns = sorted(set(SILVER_COLUMNS) - set(frame.columns))
    if missing_columns:
        raise ValueError(f"Silver input is missing columns: {', '.join(missing_columns)}")
    return frame.select(*SILVER_COLUMNS)


def build_gold_catalog_frame(
    silver: DataFrame,
    iceberg_snapshot_id: str,
    published_at_utc: str,
    boundary_object_key: str | None,
) -> DataFrame:
    active = silver.where(F.col("valid_to_utc").isNull())
    window = Window.partitionBy("complex_id").orderBy(
        F.col("valid_from_utc").desc_nulls_last(),
        F.col("ingested_at_utc").desc_nulls_last(),
        F.col("official_complex_code").asc(),
    )
    latest = active.withColumn("_row_number", F.row_number().over(window)).where(
        F.col("_row_number") == 1
    )

    return latest.select(
        F.col("complex_id"),
        F.col("official_complex_code"),
        F.col("complex_name").alias("name"),
        F.col("complex_kind").alias("kind"),
        F.col("status"),
        F.col("sido_code"),
        F.col("sigungu_code"),
        F.col("address_text"),
        F.col("official_area_sqm").cast(T.DecimalType(18, 2)).alias("official_area_sqm"),
        F.lit(None).cast(T.DecimalType(18, 2)).alias("calculated_area_sqm"),
        F.lit(0).cast(T.LongType()).alias("parcel_count"),
        F.lit(boundary_object_key).cast(T.StringType()).alias("boundary_object_key"),
        F.col("source_snapshot_id"),
        F.lit(iceberg_snapshot_id).alias("iceberg_snapshot_id"),
        F.to_timestamp(F.lit(published_at_utc), "yyyy-MM-dd'T'HH:mm:ssX").alias(
            "published_at_utc"
        ),
    ).select(*GOLD_COLUMNS)


def assert_columns(frame: DataFrame, expected_columns: Sequence[str]) -> None:
    actual_columns = tuple(frame.columns)
    if actual_columns != tuple(expected_columns):
        raise ValueError(
            "Unexpected Gold columns. "
            f"expected={list(expected_columns)} actual={list(actual_columns)}"
        )


def invalid_count(predicate: F.Column, alias: str) -> F.Column:
    return F.sum(F.when(predicate, F.lit(1)).otherwise(F.lit(0))).cast("long").alias(alias)


def required_string_columns() -> tuple[str, ...]:
    return required_string_column_names(GOLD_CONTRACT)


def sample_invalid_rows(frame: DataFrame, predicate: F.Column) -> list[str]:
    return [str(sample) for sample in frame.where(predicate).limit(5).toJSON().collect()]


def assert_no_invalid_rows(
    frame: DataFrame,
    metric_count: int,
    predicate: F.Column,
    message: str,
) -> None:
    if metric_count == 0:
        return

    samples = sample_invalid_rows(frame, predicate)
    raise ValueError(f"{message}. count={metric_count} samples={samples}")


def collect_quality_metrics(gold: DataFrame) -> dict[str, int]:
    expressions: list[F.Column] = [F.count(F.lit(1)).cast("long").alias("row_count")]

    for column in REQUIRED_GOLD_COLUMNS:
        expressions.append(invalid_count(F.col(column).isNull(), f"{column}__null_count"))

    for column in required_string_columns():
        expressions.append(invalid_count(F.length(F.col(column)) == 0, f"{column}__empty_count"))

    expressions.extend(
        (
            invalid_count(F.col("parcel_count") < 0, "invalid_parcel_count"),
            invalid_count(
                F.col("published_at_utc").isNull(),
                "invalid_published_at_count",
            ),
        )
    )

    row = gold.agg(*expressions).first()
    if row is None:
        raise ValueError("Gold quality metric aggregation returned no row")
    return {key: int(value or 0) for key, value in row.asDict().items()}


def assert_quality_metrics(gold: DataFrame, metrics: dict[str, int]) -> None:
    for column in REQUIRED_GOLD_COLUMNS:
        assert_no_invalid_rows(
            gold,
            metrics[f"{column}__null_count"],
            F.col(column).isNull(),
            f"{column} must not be null",
        )

    for column in required_string_columns():
        assert_no_invalid_rows(
            gold,
            metrics[f"{column}__empty_count"],
            F.length(F.col(column)) == 0,
            f"{column} must not be empty",
        )

    assert_no_invalid_rows(
        gold,
        metrics["invalid_parcel_count"],
        F.col("parcel_count") < 0,
        "parcel_count must be non-negative",
    )
    assert_no_invalid_rows(
        gold,
        metrics["invalid_published_at_count"],
        F.col("published_at_utc").isNull(),
        "published_at_utc must be present",
    )


def assert_unique_complex_ids(gold: DataFrame) -> None:
    duplicate_keys = (
        gold.groupBy("complex_id").count().where(F.col("count") > 1).limit(5).toJSON().collect()
    )
    if duplicate_keys:
        raise ValueError(f"Gold projection must contain one row per complex_id: {duplicate_keys}")


def validate_gold_frame(
    gold: DataFrame,
    expected_count: int | None,
) -> tuple[int, dict[str, int]]:
    assert_columns(gold, GOLD_COLUMNS)

    metrics = collect_quality_metrics(gold)
    assert_quality_metrics(gold, metrics)
    assert_unique_complex_ids(gold)

    actual_count = metrics["row_count"]
    if expected_count is not None and actual_count != expected_count:
        raise ValueError(f"Expected {expected_count} Gold rows, found {actual_count}")

    return actual_count, metrics


def collect_source_snapshot_summary(gold: DataFrame) -> dict[str, Any]:
    snapshots = gold.select("source_snapshot_id").distinct()
    snapshot_count = int(snapshots.count())
    snapshot_ids = [
        row.source_snapshot_id for row in snapshots.orderBy("source_snapshot_id").collect()
    ]
    return {
        "source_snapshot_count": snapshot_count,
        "source_snapshot_ids": snapshot_ids,
        "source_snapshot_truncated": False,
    }


def quoted_table(catalog: str, namespace: str, table: str) -> str:
    return f"`{catalog}`.`{namespace}`.`{table}`"


def qualified_source_table(args: argparse.Namespace) -> str:
    return quoted_table(
        args.iceberg_catalog_name,
        args.source_iceberg_namespace,
        args.source_iceberg_table,
    )


def qualified_target_table(args: argparse.Namespace) -> str:
    return quoted_table(
        args.iceberg_catalog_name,
        args.target_iceberg_namespace,
        args.target_iceberg_table,
    )


def unquoted_target_table(args: argparse.Namespace) -> str:
    return (
        f"{args.iceberg_catalog_name}."
        f"{args.target_iceberg_namespace}."
        f"{args.target_iceberg_table}"
    )


def run_summary_input(args: argparse.Namespace) -> dict[str, str]:
    if args.input_mode in ("jsonl", "parquet"):
        return {
            "kind": args.input_mode,
            "path": args.input,
        }

    return {
        "kind": "iceberg",
        "catalog": args.iceberg_catalog_name,
        "namespace": args.source_iceberg_namespace,
        "table": args.source_iceberg_table,
        "qualified_table": (
            f"{args.iceberg_catalog_name}."
            f"{args.source_iceberg_namespace}."
            f"{args.source_iceberg_table}"
        ),
    }


def run_summary_target(args: argparse.Namespace) -> dict[str, str]:
    if args.write_mode == "parquet":
        return {
            "kind": "parquet",
            "path": args.output,
        }

    return {
        "kind": "iceberg",
        "catalog": args.iceberg_catalog_name,
        "namespace": args.target_iceberg_namespace,
        "table": args.target_iceberg_table,
        "qualified_table": unquoted_target_table(args),
    }


def run_summary_disposition(args: argparse.Namespace) -> str:
    if args.validate_only:
        return "validate_only"
    if args.write_mode == "parquet":
        return "parquet_overwrite"
    return f"iceberg_{args.iceberg_write_mode}"


def summary_quality_metrics(
    quality_metrics: dict[str, int],
    persisted_row_count: int | None,
) -> dict[str, int]:
    metrics = dict(quality_metrics)
    if persisted_row_count is not None:
        metrics["persisted_row_count"] = int(persisted_row_count)
    return metrics


def build_run_summary(
    args: argparse.Namespace,
    row_count: int,
    persisted_row_count: int | None,
    quality_metrics: dict[str, int],
    source_snapshot_summary: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": RUN_SUMMARY_SCHEMA_VERSION,
        "job_name": JOB_NAME,
        "contract": GOLD_CONTRACT_NAME,
        "created_at_utc": datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "input": run_summary_input(args),
        "target": run_summary_target(args),
        "write_mode": args.write_mode,
        "write_disposition": run_summary_disposition(args),
        "row_count": row_count,
        "persisted_row_count": persisted_row_count,
        "quality_metrics": summary_quality_metrics(quality_metrics, persisted_row_count),
        "column_count": len(GOLD_COLUMNS),
        "columns": list(GOLD_COLUMNS),
        "required_columns": list(REQUIRED_GOLD_COLUMNS),
        **source_snapshot_summary,
    }


def json_payload(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def emit_run_summary(summary: dict[str, Any], output_path: str | None) -> None:
    payload = json_payload(summary)
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{payload}\n", encoding="utf-8")

    print(f"gold-complex-catalog-summary-json {payload}")


def lineage_run_summary_ref(summary: dict[str, Any], output_path: str | None) -> dict[str, str]:
    payload = json_payload(summary)
    return {
        "path": output_path or "stdout",
        "checksum_sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
    }


def lineage_quality_metrics(summary: dict[str, Any]) -> dict[str, int]:
    metrics = dict(summary["quality_metrics"])
    metrics["row_count"] = int(summary["row_count"])
    persisted_row_count = summary.get("persisted_row_count")
    if persisted_row_count is not None:
        metrics["persisted_row_count"] = int(persisted_row_count)
    return metrics


def column_lineage() -> list[dict[str, Any]]:
    lineage_map: dict[str, list[dict[str, str]]] = {
        "complex_id": [
            {"dataset": SILVER_CONTRACT_NAME, "column": "complex_id", "transform": "identity"}
        ],
        "official_complex_code": [
            {
                "dataset": SILVER_CONTRACT_NAME,
                "column": "official_complex_code",
                "transform": "identity",
            }
        ],
        "name": [
            {"dataset": SILVER_CONTRACT_NAME, "column": "complex_name", "transform": "rename"}
        ],
        "kind": [
            {"dataset": SILVER_CONTRACT_NAME, "column": "complex_kind", "transform": "rename"}
        ],
        "status": [
            {"dataset": SILVER_CONTRACT_NAME, "column": "status", "transform": "identity"}
        ],
        "sido_code": [
            {"dataset": SILVER_CONTRACT_NAME, "column": "sido_code", "transform": "identity"}
        ],
        "sigungu_code": [
            {"dataset": SILVER_CONTRACT_NAME, "column": "sigungu_code", "transform": "identity"}
        ],
        "address_text": [
            {"dataset": SILVER_CONTRACT_NAME, "column": "address_text", "transform": "identity"}
        ],
        "official_area_sqm": [
            {
                "dataset": SILVER_CONTRACT_NAME,
                "column": "official_area_sqm",
                "transform": "cast_decimal_18_2",
            }
        ],
        "calculated_area_sqm": [
            {"dataset": "foundation-platform.job_arguments", "column": "null", "transform": "literal_null"}
        ],
        "parcel_count": [
            {"dataset": "foundation-platform.job_arguments", "column": "0", "transform": "literal_zero"}
        ],
        "boundary_object_key": [
            {
                "dataset": "foundation-platform.job_arguments",
                "column": "boundary_object_key",
                "transform": "literal",
            }
        ],
        "source_snapshot_id": [
            {
                "dataset": SILVER_CONTRACT_NAME,
                "column": "source_snapshot_id",
                "transform": "identity",
            }
        ],
        "iceberg_snapshot_id": [
            {
                "dataset": "foundation-platform.job_arguments",
                "column": "iceberg_snapshot_id",
                "transform": "literal",
            }
        ],
        "published_at_utc": [
            {
                "dataset": "foundation-platform.job_arguments",
                "column": "published_at_utc",
                "transform": "literal_timestamp",
            }
        ],
    }
    return [
        {"output_column": column, "inputs": lineage_map[column]}
        for column in GOLD_COLUMNS
    ]


def build_lineage_event(args: argparse.Namespace, summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": LINEAGE_EVENT_SCHEMA_VERSION,
        "event_type": LINEAGE_EVENT_TYPE,
        "occurred_at": summary["created_at_utc"],
        "producer": "foundation-platform.lakehouse",
        "job_name": JOB_NAME,
        "run_id": f"{JOB_NAME}:{summary['created_at_utc']}",
        "run_summary_schema_version": RUN_SUMMARY_SCHEMA_VERSION,
        "run_summary_ref": lineage_run_summary_ref(summary, args.summary_output),
        "input_dataset": {
            "qualified_name": SILVER_CONTRACT_NAME,
            "namespace": "silver",
            "table": "industrial_complexes",
            "storage_format": args.input_mode,
        },
        "output_dataset": {
            "qualified_name": GOLD_CONTRACT_NAME,
            "namespace": "gold",
            "table": "complex_catalog",
            "storage_format": args.write_mode,
        },
        "source_snapshot_ids": summary["source_snapshot_ids"],
        "source_snapshot_truncated": summary["source_snapshot_truncated"],
        "iceberg_snapshot": {
            "catalog": args.iceberg_catalog_name,
            "namespace": args.target_iceberg_namespace,
            "table": args.target_iceberg_table,
            "snapshot_id": args.iceberg_snapshot_id,
        },
        "quality_metrics": lineage_quality_metrics(summary),
        "column_lineage": column_lineage(),
        "openlineage_mapping": {
            "event_type": "COMPLETE",
            "job_namespace": "foundation-platform.lakehouse",
            "job_name": JOB_NAME,
            "input_namespace": f"{args.iceberg_catalog_name}.silver",
            "output_namespace": f"{args.iceberg_catalog_name}.gold",
        },
    }


def emit_lineage_event(event: dict[str, Any], output_path: str | None) -> None:
    if not output_path:
        return

    payload = json_payload(event)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{payload}\n", encoding="utf-8")
    print(f"gold-complex-catalog-lineage-json {payload}")


def write_gold_parquet(gold: DataFrame, output_path: str) -> None:
    (
        gold.repartition("sido_code")
        .sortWithinPartitions("sigungu_code", "name", "complex_id")
        .write.mode("overwrite")
        .partitionBy("sido_code")
        .parquet(output_path)
    )


def create_gold_iceberg_table_if_missing(spark: SparkSession, args: argparse.Namespace) -> None:
    namespace = f"`{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`"
    table = qualified_target_table(args)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
{create_table_columns_sql(GOLD_CONTRACT)}
        )
        USING iceberg
        PARTITIONED BY ({partition_spec_sql(GOLD_CONTRACT)})
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'zstd',
            'write.distribution-mode' = 'hash'
        )
        """
    )


def write_gold_iceberg(
    spark: SparkSession,
    gold: DataFrame,
    args: argparse.Namespace,
) -> None:
    table = qualified_target_table(args)
    temp_view = "gold_complex_catalog_candidate"

    create_gold_iceberg_table_if_missing(spark, args)
    gold.select(*GOLD_COLUMNS).createOrReplaceTempView(temp_view)

    statement = "INSERT INTO"
    if args.iceberg_write_mode == "overwrite":
        statement = "INSERT OVERWRITE"

    spark.sql(
        f"""
        {statement} {table}
        SELECT {", ".join(GOLD_COLUMNS)}
        FROM {temp_view}
        """
    )


def build_spark_session(args: argparse.Namespace) -> SparkSession:
    builder = (
        SparkSession.builder.appName("foundation-platform-industrial-complex-silver-to-gold")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
    )

    if args.input_mode == "iceberg" or args.write_mode == "iceberg":
        catalog = args.iceberg_catalog_name
        catalog_uri = require_env("FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI")
        builder = (
            builder.config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                f"spark.sql.catalog.{catalog}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(f"spark.sql.catalog.{catalog}.type", "rest")
            .config(f"spark.sql.catalog.{catalog}.uri", catalog_uri)
            .config(
                f"spark.sql.catalog.{catalog}.oauth2-server-uri",
                lakehouse_oauth2_server_uri(catalog_uri),
            )
            .config(
                f"spark.sql.catalog.{catalog}.warehouse",
                require_env("FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE"),
            )
            .config(
                f"spark.sql.catalog.{catalog}.token",
                require_env("FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN"),
            )
            .config(
                f"spark.sql.catalog.{catalog}.header.X-Iceberg-Access-Delegation",
                "vended-credentials",
            )
            .config(f"spark.sql.catalog.{catalog}.s3.remote-signing-enabled", "false")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if args.input_mode == "iceberg" or args.write_mode == "iceberg":
        assert_iceberg_runtime_loaded(spark, args.iceberg_packages)
    return spark


def assert_iceberg_runtime_loaded(spark: SparkSession, packages: str) -> None:
    class_loader = spark._jvm.java.lang.Thread.currentThread().getContextClassLoader()
    try:
        class_loader.loadClass("org.apache.iceberg.spark.SparkCatalog")
        class_loader.loadClass("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    except Exception as exc:
        raise RuntimeError(
            "Iceberg Spark runtime is not loaded. Run spark-submit with "
            f"--packages {packages} and a writable Ivy cache, for example "
            "--conf spark.jars.ivy=/tmp/.ivy2"
        ) from exc


def read_iceberg_snapshot_for_batch(
    spark: SparkSession,
    gold: DataFrame,
    args: argparse.Namespace,
) -> DataFrame:
    snapshot_rows = gold.select("source_snapshot_id").distinct().limit(17).collect()
    snapshot_ids = [row.source_snapshot_id for row in snapshot_rows]
    if not snapshot_ids:
        raise ValueError("Cannot verify Iceberg write because source_snapshot_id is empty")
    if len(snapshot_ids) > 16:
        raise ValueError("Iceberg write verification supports at most 16 source snapshots")

    return (
        spark.table(qualified_target_table(args))
        .where(F.col("source_snapshot_id").isin(snapshot_ids))
        .select(*GOLD_COLUMNS)
    )


def main() -> int:
    args = parse_args()
    args.published_at_utc = normalize_utc_timestamp(args.published_at_utc)
    validate_args(args)
    spark = build_spark_session(args)

    try:
        silver = read_silver_input(spark, args)
        gold = build_gold_catalog_frame(
            silver,
            iceberg_snapshot_id=args.iceberg_snapshot_id,
            published_at_utc=args.published_at_utc,
            boundary_object_key=args.boundary_object_key,
        ).persist(StorageLevel.MEMORY_AND_DISK)
        row_count, quality_metrics = validate_gold_frame(gold, args.expected_count)
        source_snapshot_summary = collect_source_snapshot_summary(gold)

        if args.validate_only:
            emit_run_summary(
                build_run_summary(
                    args,
                    row_count=row_count,
                    persisted_row_count=None,
                    quality_metrics=quality_metrics,
                    source_snapshot_summary=source_snapshot_summary,
                ),
                args.summary_output,
            )
            print(f"gold-complex-catalog-validate-ok rows={row_count}")
            return 0

        if args.write_mode == "parquet":
            write_gold_parquet(gold, args.output)
            persisted = spark.read.parquet(args.output).select(*GOLD_COLUMNS)
            success_target = f"output={args.output}"
            success_label = "gold-complex-catalog-write-ok"
        else:
            write_gold_iceberg(spark, gold, args)
            persisted = read_iceberg_snapshot_for_batch(spark, gold, args)
            success_target = f"table={args.target_iceberg_namespace}.{args.target_iceberg_table}"
            success_label = "gold-complex-catalog-iceberg-write-ok"

        persisted_count, persisted_quality_metrics = validate_gold_frame(
            persisted,
            args.expected_count,
        )
        if persisted_count != row_count:
            raise ValueError(
                f"Persisted row count changed. before={row_count} after={persisted_count}"
            )

        emit_run_summary(
            summary := build_run_summary(
                args,
                row_count=row_count,
                persisted_row_count=persisted_count,
                quality_metrics=persisted_quality_metrics,
                source_snapshot_summary=source_snapshot_summary,
            ),
            args.summary_output,
        )
        emit_lineage_event(build_lineage_event(args, summary), args.lineage_output)
        print(f"{success_label} rows={persisted_count} {success_target}")
        return 0
    finally:
        if "gold" in locals():
            gold.unpersist()
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
