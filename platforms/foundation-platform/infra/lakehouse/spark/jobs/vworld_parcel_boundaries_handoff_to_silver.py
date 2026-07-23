#!/usr/bin/env python3
"""Write VWorld cadastral parcel-boundary handoff rows into the Silver table shape.

Rust foundation-platform owns the VWorld normalization contract and emits a writer-neutral
JSONL handoff. This Spark job owns the storage-engine step: decode transport-only
fields, verify Silver quality gates, and write Parquet or Iceberg rows whose columns
match `catalog_domain::SILVER_PARCEL_BOUNDARIES`.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
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
JOB_NAME = "vworld_parcel_boundaries_handoff_to_silver"
RUN_SUMMARY_SCHEMA_VERSION = "foundation-platform.spark_run_summary.v1"
RUN_SUMMARY_CONTRACT = "silver.parcel_boundaries"
RUN_SUMMARY_INPUT_KIND = "silver_handoff_jsonl"
TABLE_CONTRACT = load_lakehouse_contract(RUN_SUMMARY_CONTRACT)

SILVER_COLUMNS: tuple[str, ...] = column_names(TABLE_CONTRACT)

HANDOFF_INPUT_COLUMNS: tuple[str, ...] = (
    *SILVER_COLUMNS,
    "geometry_wkb_hex",
    "geometry_wkb_encoding",
)

REQUIRED_SILVER_COLUMNS: tuple[str, ...] = required_column_names(TABLE_CONTRACT)

REQUIRED_STRING_COLUMNS: tuple[str, ...] = required_string_column_names(TABLE_CONTRACT)

TRANSPORT_COLUMNS: tuple[str, ...] = (
    "_geometry_wkb_hex",
    "_geometry_wkb_encoding",
)

PARCEL_SPECIFIC_QUALITY_METRICS: tuple[str, ...] = (
    "invalid_pnu_count",
    "invalid_code_derivation_count",
    "invalid_geometry_srid_count",
    "invalid_geometry_encoding_count",
    "invalid_geometry_wkb_hex_count",
    "invalid_geometry_wkb_count",
    "invalid_bbox_count",
    "invalid_checksum_count",
    "duplicate_active_pnu_count",
)


def trim_to_null(column_name: str) -> F.Column:
    trimmed = F.trim(F.col(column_name))
    return F.when(F.length(trimmed) == 0, F.lit(None)).otherwise(trimmed)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build silver.parcel_boundaries from VWorld handoff JSONL input."
    )
    parser.add_argument("--input", required=True, help="Silver handoff JSONL input path.")
    parser.add_argument("--output", help="Silver Parquet output path.")
    parser.add_argument(
        "--write-mode",
        choices=("parquet", "iceberg"),
        default="parquet",
        help="Write local Parquet or an Iceberg REST catalog table.",
    )
    parser.add_argument(
        "--iceberg-catalog-name",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_CATALOG_NAME", "r2"),
        help="Spark catalog name for Iceberg REST catalog writes.",
    )
    parser.add_argument(
        "--iceberg-namespace",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_NAMESPACE", "silver"),
        help="Iceberg namespace for the target Silver table.",
    )
    parser.add_argument(
        "--iceberg-table",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_TABLE", "parcel_boundaries"),
        help="Iceberg table name for the target Silver table.",
    )
    parser.add_argument(
        "--iceberg-write-mode",
        choices=("append", "overwrite"),
        default="append",
        help="How candidate rows are written to the Iceberg table.",
    )
    parser.add_argument(
        "--iceberg-packages",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_PACKAGES", DEFAULT_ICEBERG_PACKAGES),
        help="Comma-separated Iceberg Spark packages used for REST catalog writes.",
    )
    parser.add_argument(
        "--allow-non-smoke-overwrite",
        action="store_true",
        help="Allow overwrite mode for tables whose names do not end with _smoke.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate input, target config, and Silver quality gates without writing.",
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


def validate_args(args: argparse.Namespace) -> None:
    if args.summary_output is not None and args.summary_output.strip() == "":
        raise ValueError("--summary-output must not be empty")

    if args.write_mode == "parquet" and not args.output:
        raise ValueError("--output is required when --write-mode=parquet")

    if args.write_mode == "iceberg":
        validate_identifier("iceberg catalog name", args.iceberg_catalog_name)
        validate_identifier("iceberg namespace", args.iceberg_namespace)
        validate_identifier("iceberg table", args.iceberg_table)

        if args.iceberg_write_mode == "overwrite":
            is_smoke_table = args.iceberg_table.endswith("_smoke")
            if not is_smoke_table and not args.allow_non_smoke_overwrite:
                raise ValueError(
                    "Refusing to overwrite a non-smoke Iceberg table without "
                    "--allow-non-smoke-overwrite"
                )

        for name in required_iceberg_env():
            require_env(name)


def read_handoff_jsonl(spark: SparkSession, input_path: str) -> DataFrame:
    handoff = spark.read.json(input_path)
    missing_columns = sorted(set(HANDOFF_INPUT_COLUMNS) - set(handoff.columns))
    if missing_columns:
        raise ValueError(f"Parcel-boundary handoff is missing columns: {', '.join(missing_columns)}")
    return handoff.select(*HANDOFF_INPUT_COLUMNS)


def build_candidate_frame(handoff: DataFrame) -> DataFrame:
    geometry_wkb_hex = F.lower(F.trim(F.col("geometry_wkb_hex")))
    geometry_wkb_encoding = F.lower(F.trim(F.col("geometry_wkb_encoding")))

    return handoff.select(
        F.trim(F.col("boundary_id")).alias("boundary_id"),
        F.trim(F.col("pnu")).alias("pnu"),
        F.trim(F.col("sido_code")).alias("sido_code"),
        F.trim(F.col("sigungu_code")).alias("sigungu_code"),
        F.trim(F.col("bjdong_code")).alias("bjdong_code"),
        trim_to_null("jibun").alias("jibun"),
        trim_to_null("bonbun").alias("bonbun"),
        trim_to_null("bubun").alias("bubun"),
        F.unhex(geometry_wkb_hex).alias("geometry_wkb"),
        F.col("geometry_srid").cast(T.IntegerType()).alias("geometry_srid"),
        F.col("bbox_min_x").cast(T.DoubleType()).alias("bbox_min_x"),
        F.col("bbox_min_y").cast(T.DoubleType()).alias("bbox_min_y"),
        F.col("bbox_max_x").cast(T.DoubleType()).alias("bbox_max_x"),
        F.col("bbox_max_y").cast(T.DoubleType()).alias("bbox_max_y"),
        F.lower(F.trim(F.col("geometry_checksum_sha256"))).alias(
            "geometry_checksum_sha256"
        ),
        F.trim(F.col("source_record_id")).alias("source_record_id"),
        F.trim(F.col("source_snapshot_id")).alias("source_snapshot_id"),
        F.to_timestamp(F.col("valid_from_utc"), "yyyy-MM-dd'T'HH:mm:ssX").alias(
            "valid_from_utc"
        ),
        F.to_timestamp(F.col("valid_to_utc"), "yyyy-MM-dd'T'HH:mm:ssX").alias(
            "valid_to_utc"
        ),
        F.to_timestamp(F.col("ingested_at_utc"), "yyyy-MM-dd'T'HH:mm:ssX").alias(
            "ingested_at_utc"
        ),
        geometry_wkb_hex.alias("_geometry_wkb_hex"),
        geometry_wkb_encoding.alias("_geometry_wkb_encoding"),
    )


def assert_columns(frame: DataFrame, expected_columns: Sequence[str]) -> None:
    actual_columns = tuple(frame.select(*expected_columns).columns)
    if actual_columns != tuple(expected_columns):
        raise ValueError(
            "Unexpected Silver columns. "
            f"expected={list(expected_columns)} actual={list(actual_columns)}"
        )


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


def invalid_count(predicate: F.Column, alias: str) -> F.Column:
    return F.sum(F.when(predicate, F.lit(1)).otherwise(F.lit(0))).cast("long").alias(alias)


def is_invalid_double(column_name: str) -> F.Column:
    column = F.col(column_name)
    return column.isNull() | F.isnan(column)


def pnu_is_invalid() -> F.Column:
    return ~F.col("pnu").rlike(r"^[0-9]{19}$")


def code_derivation_is_invalid() -> F.Column:
    return (
        (F.col("sido_code") != F.substring(F.col("pnu"), 1, 2))
        | (F.col("sigungu_code") != F.substring(F.col("pnu"), 1, 5))
        | (F.col("bjdong_code") != F.substring(F.col("pnu"), 1, 10))
    )


def geometry_wkb_hex_is_invalid() -> F.Column:
    return (
        F.col("_geometry_wkb_hex").isNull()
        | (F.length(F.col("_geometry_wkb_hex")) == 0)
        | ((F.length(F.col("_geometry_wkb_hex")) % 2) != 0)
        | ~F.col("_geometry_wkb_hex").rlike(r"^[0-9a-f]+$")
    )


def geometry_wkb_is_invalid() -> F.Column:
    geometry_hex = F.lower(F.hex(F.col("geometry_wkb")))
    return (
        F.col("geometry_wkb").isNull()
        | (F.length(F.col("geometry_wkb")) <= 9)
        | ~geometry_hex.rlike(r"^(0103000000|0106000000)")
    )


def bbox_is_invalid() -> F.Column:
    return (
        is_invalid_double("bbox_min_x")
        | is_invalid_double("bbox_min_y")
        | is_invalid_double("bbox_max_x")
        | is_invalid_double("bbox_max_y")
        | (F.col("bbox_min_x") > F.col("bbox_max_x"))
        | (F.col("bbox_min_y") > F.col("bbox_max_y"))
    )


def checksum_is_invalid() -> F.Column:
    return (
        ~F.col("geometry_checksum_sha256").rlike(r"^[0-9a-f]{64}$")
        | (F.col("geometry_checksum_sha256") != F.sha2(F.col("geometry_wkb"), 256))
    )


def collect_duplicate_active_pnu_count(frame: DataFrame) -> int:
    duplicate_rows = (
        frame.where(F.col("valid_to_utc").isNull())
        .groupBy("pnu")
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    return int(duplicate_rows)


def collect_quality_metrics(frame: DataFrame, include_transport: bool) -> dict[str, int]:
    expressions: list[F.Column] = [F.count(F.lit(1)).cast("long").alias("row_count")]

    for column in REQUIRED_SILVER_COLUMNS:
        expressions.append(invalid_count(F.col(column).isNull(), f"{column}__null_count"))

    for column in REQUIRED_STRING_COLUMNS:
        expressions.append(invalid_count(F.length(F.col(column)) == 0, f"{column}__empty_count"))

    if include_transport:
        invalid_encoding = F.col("_geometry_wkb_encoding") != F.lit("hex")
        invalid_hex = geometry_wkb_hex_is_invalid()
    else:
        invalid_encoding = F.lit(False)
        invalid_hex = F.lit(False)

    expressions.extend(
        (
            invalid_count(pnu_is_invalid(), "invalid_pnu_count"),
            invalid_count(code_derivation_is_invalid(), "invalid_code_derivation_count"),
            invalid_count(F.col("geometry_srid") != 4326, "invalid_geometry_srid_count"),
            invalid_count(invalid_encoding, "invalid_geometry_encoding_count"),
            invalid_count(invalid_hex, "invalid_geometry_wkb_hex_count"),
            invalid_count(geometry_wkb_is_invalid(), "invalid_geometry_wkb_count"),
            invalid_count(bbox_is_invalid(), "invalid_bbox_count"),
            invalid_count(checksum_is_invalid(), "invalid_checksum_count"),
        )
    )

    row = frame.agg(*expressions).first()
    if row is None:
        raise ValueError("Silver quality metric aggregation returned no row")
    metrics = {key: int(value or 0) for key, value in row.asDict().items()}
    metrics["duplicate_active_pnu_count"] = collect_duplicate_active_pnu_count(frame)
    for metric in PARCEL_SPECIFIC_QUALITY_METRICS:
        metrics.setdefault(metric, 0)
    return metrics


def assert_no_duplicate_active_pnu(frame: DataFrame, metric_count: int) -> None:
    if metric_count == 0:
        return

    samples = (
        frame.where(F.col("valid_to_utc").isNull())
        .groupBy("pnu")
        .count()
        .where(F.col("count") > 1)
        .limit(5)
        .toJSON()
        .collect()
    )
    raise ValueError(
        "active parcel boundaries must be unique by pnu. "
        f"count={metric_count} samples={samples}"
    )


def assert_quality_metrics(
    frame: DataFrame,
    metrics: dict[str, int],
    include_transport: bool,
) -> None:
    for column in REQUIRED_SILVER_COLUMNS:
        assert_no_invalid_rows(
            frame,
            metrics[f"{column}__null_count"],
            F.col(column).isNull(),
            f"{column} must not be null",
        )

    for column in REQUIRED_STRING_COLUMNS:
        assert_no_invalid_rows(
            frame,
            metrics[f"{column}__empty_count"],
            F.length(F.col(column)) == 0,
            f"{column} must not be empty",
        )

    assert_no_invalid_rows(
        frame,
        metrics["invalid_pnu_count"],
        pnu_is_invalid(),
        "pnu must be a 19-digit parcel number",
    )
    assert_no_invalid_rows(
        frame,
        metrics["invalid_code_derivation_count"],
        code_derivation_is_invalid(),
        "sido_code, sigungu_code, and bjdong_code must be derived from pnu",
    )
    assert_no_invalid_rows(
        frame,
        metrics["invalid_geometry_srid_count"],
        F.col("geometry_srid") != 4326,
        "geometry_srid must be 4326",
    )
    if include_transport:
        assert_no_invalid_rows(
            frame,
            metrics["invalid_geometry_encoding_count"],
            F.col("_geometry_wkb_encoding") != F.lit("hex"),
            "geometry_wkb_encoding must be hex",
        )
        assert_no_invalid_rows(
            frame,
            metrics["invalid_geometry_wkb_hex_count"],
            geometry_wkb_hex_is_invalid(),
            "geometry_wkb_hex must be non-empty lowercase even-length hex",
        )
    assert_no_invalid_rows(
        frame,
        metrics["invalid_geometry_wkb_count"],
        geometry_wkb_is_invalid(),
        "geometry_wkb must be non-empty little-endian Polygon or MultiPolygon WKB",
    )
    assert_no_invalid_rows(
        frame,
        metrics["invalid_bbox_count"],
        bbox_is_invalid(),
        "bbox min/max ordering must be valid",
    )
    assert_no_invalid_rows(
        frame,
        metrics["invalid_checksum_count"],
        checksum_is_invalid(),
        "geometry_checksum_sha256 must match geometry_wkb",
    )
    assert_no_duplicate_active_pnu(frame, metrics["duplicate_active_pnu_count"])


def validate_parcel_frame(
    frame: DataFrame,
    expected_count: int | None,
    include_transport: bool,
) -> tuple[int, dict[str, int]]:
    assert_columns(frame, SILVER_COLUMNS)

    metrics = collect_quality_metrics(frame, include_transport)
    assert_quality_metrics(frame, metrics, include_transport)

    actual_count = metrics["row_count"]
    if expected_count is not None and actual_count != expected_count:
        raise ValueError(f"Expected {expected_count} Silver rows, found {actual_count}")

    return actual_count, metrics


def merge_transport_metrics(
    persisted_metrics: dict[str, int],
    candidate_metrics: dict[str, int],
) -> dict[str, int]:
    merged = dict(persisted_metrics)
    for metric in ("invalid_geometry_encoding_count", "invalid_geometry_wkb_hex_count"):
        merged[metric] = candidate_metrics[metric]
    return merged


def collect_source_snapshot_summary(frame: DataFrame) -> dict[str, Any]:
    snapshots = frame.select("source_snapshot_id").distinct()
    snapshot_count = int(snapshots.count())
    snapshot_ids = [
        row.source_snapshot_id
        for row in snapshots.orderBy("source_snapshot_id").collect()
    ]
    return {
        "source_snapshot_count": snapshot_count,
        "source_snapshot_ids": snapshot_ids,
        "source_snapshot_truncated": False,
    }


def unquoted_qualified_iceberg_table(args: argparse.Namespace) -> str:
    return f"{args.iceberg_catalog_name}.{args.iceberg_namespace}.{args.iceberg_table}"


def run_summary_target(args: argparse.Namespace) -> dict[str, str]:
    if args.write_mode == "parquet":
        return {
            "kind": "parquet",
            "path": args.output,
        }

    return {
        "kind": "iceberg",
        "catalog": args.iceberg_catalog_name,
        "namespace": args.iceberg_namespace,
        "table": args.iceberg_table,
        "qualified_table": unquoted_qualified_iceberg_table(args),
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
        "contract": RUN_SUMMARY_CONTRACT,
        "created_at_utc": datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "input": {
            "kind": RUN_SUMMARY_INPUT_KIND,
            "path": args.input,
        },
        "target": run_summary_target(args),
        "write_mode": args.write_mode,
        "write_disposition": run_summary_disposition(args),
        "row_count": row_count,
        "persisted_row_count": persisted_row_count,
        "quality_metrics": summary_quality_metrics(quality_metrics, persisted_row_count),
        "column_count": len(SILVER_COLUMNS),
        "columns": list(SILVER_COLUMNS),
        "required_columns": list(REQUIRED_SILVER_COLUMNS),
        **source_snapshot_summary,
    }


def emit_run_summary(summary: dict[str, Any], output_path: str | None) -> None:
    payload = json.dumps(summary, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{payload}\n", encoding="utf-8")

    print(f"silver-parcel-boundaries-summary-json {payload}")


def write_silver_parquet(silver: DataFrame, output_path: str) -> None:
    (
        silver.repartition("sigungu_code")
        .sortWithinPartitions("pnu", "valid_from_utc")
        .write.mode("overwrite")
        .partitionBy("sigungu_code")
        .parquet(output_path)
    )


def qualified_iceberg_table(args: argparse.Namespace) -> str:
    return (
        f"`{args.iceberg_catalog_name}`."
        f"`{args.iceberg_namespace}`."
        f"`{args.iceberg_table}`"
    )


def create_iceberg_table_if_missing(spark: SparkSession, args: argparse.Namespace) -> None:
    namespace = f"`{args.iceberg_catalog_name}`.`{args.iceberg_namespace}`"
    table = qualified_iceberg_table(args)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
{create_table_columns_sql(TABLE_CONTRACT)}
        )
        USING iceberg
        PARTITIONED BY ({partition_spec_sql(TABLE_CONTRACT)})
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'zstd',
            'write.distribution-mode' = 'hash'
        )
        """
    )


def write_silver_iceberg(
    spark: SparkSession,
    silver: DataFrame,
    args: argparse.Namespace,
) -> None:
    table = qualified_iceberg_table(args)
    temp_view = "silver_parcel_boundaries_candidate"

    create_iceberg_table_if_missing(spark, args)
    silver.select(*SILVER_COLUMNS).createOrReplaceTempView(temp_view)

    statement = "INSERT INTO"
    if args.iceberg_write_mode == "overwrite":
        statement = "INSERT OVERWRITE"

    spark.sql(
        f"""
        {statement} {table}
        SELECT {", ".join(SILVER_COLUMNS)}
        FROM {temp_view}
        """
    )


def build_spark_session(args: argparse.Namespace) -> SparkSession:
    builder = (
        SparkSession.builder.appName("foundation-platform-vworld-parcel-boundaries-handoff-to-silver")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
    )

    if args.write_mode == "iceberg":
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
            .config(
                f"spark.sql.catalog.{catalog}.uri",
                catalog_uri,
            )
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
    if args.write_mode == "iceberg":
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
    silver: DataFrame,
    args: argparse.Namespace,
) -> DataFrame:
    snapshot_rows = silver.select("source_snapshot_id").distinct().limit(17).collect()
    snapshot_ids = [row.source_snapshot_id for row in snapshot_rows]
    if not snapshot_ids:
        raise ValueError("Cannot verify Iceberg write because source_snapshot_id is empty")
    if len(snapshot_ids) > 16:
        raise ValueError("Iceberg write verification supports at most 16 source snapshots")

    return (
        spark.table(qualified_iceberg_table(args))
        .where(F.col("source_snapshot_id").isin(snapshot_ids))
        .select(*SILVER_COLUMNS)
    )


def main() -> int:
    args = parse_args()
    validate_args(args)
    spark = build_spark_session(args)

    try:
        handoff = read_handoff_jsonl(spark, args.input)
        candidate = build_candidate_frame(handoff).persist(StorageLevel.MEMORY_AND_DISK)
        silver = candidate.select(*SILVER_COLUMNS).persist(StorageLevel.MEMORY_AND_DISK)
        row_count, candidate_quality_metrics = validate_parcel_frame(
            candidate,
            args.expected_count,
            include_transport=True,
        )
        source_snapshot_summary = collect_source_snapshot_summary(silver)

        if args.validate_only:
            emit_run_summary(
                build_run_summary(
                    args,
                    row_count=row_count,
                    persisted_row_count=None,
                    quality_metrics=candidate_quality_metrics,
                    source_snapshot_summary=source_snapshot_summary,
                ),
                args.summary_output,
            )
            print(f"silver-parcel-boundaries-validate-ok rows={row_count}")
            return 0

        if args.write_mode == "parquet":
            write_silver_parquet(silver, args.output)
            persisted = spark.read.parquet(args.output).select(*SILVER_COLUMNS)
            success_target = f"output={args.output}"
            success_label = "silver-parcel-boundaries-write-ok"
        else:
            write_silver_iceberg(spark, silver, args)
            persisted = read_iceberg_snapshot_for_batch(spark, silver, args)
            success_target = f"table={args.iceberg_namespace}.{args.iceberg_table}"
            success_label = "silver-parcel-boundaries-iceberg-write-ok"

        persisted_count, persisted_quality_metrics = validate_parcel_frame(
            persisted,
            args.expected_count,
            include_transport=False,
        )
        if persisted_count != row_count:
            raise ValueError(
                f"Persisted row count changed. before={row_count} after={persisted_count}"
            )

        emit_run_summary(
            build_run_summary(
                args,
                row_count=row_count,
                persisted_row_count=persisted_count,
                quality_metrics=merge_transport_metrics(
                    persisted_quality_metrics,
                    candidate_quality_metrics,
                ),
                source_snapshot_summary=source_snapshot_summary,
            ),
            args.summary_output,
        )
        print(f"{success_label} rows={persisted_count} {success_target}")
        return 0
    finally:
        if "candidate" in locals():
            candidate.unpersist()
        if "silver" in locals():
            silver.unpersist()
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
