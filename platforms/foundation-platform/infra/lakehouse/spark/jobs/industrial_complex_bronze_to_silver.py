#!/usr/bin/env python3
"""Convert industrial-complex Bronze JSONL records into the Silver table shape.

This local job writes Parquet so it can run without live R2/Iceberg credentials.
The same column contract is intended to feed the Iceberg writer once the catalog
credentials are available.
"""

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


UUID_NAMESPACE_URL_HEX = "6ba7b8119dad11d180b400c04fd430c8"
DEFAULT_ICEBERG_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
    "org.apache.iceberg:iceberg-aws-bundle:1.6.1"
)
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
JOB_NAME = "industrial_complex_bronze_to_silver"
RUN_SUMMARY_SCHEMA_VERSION = "foundation-platform.spark_run_summary.v1"
LINEAGE_EVENT_SCHEMA_VERSION = "foundation-platform.lakehouse_lineage_event.v1"
LINEAGE_EVENT_TYPE = "lakehouse.lineage.dataset_materialized.v1"
BRONZE_DATASET_NAME = "bronze.industrial_complexes_raw_jsonl"
RUN_SUMMARY_CONTRACT = "silver.industrial_complexes"
TABLE_CONTRACT = load_lakehouse_contract(RUN_SUMMARY_CONTRACT)

INPUT_COLUMNS: tuple[str, ...] = (
    "official_complex_code",
    "complex_name",
    "complex_kind",
    "status",
    "sido_code",
    "sigungu_code",
    "primary_bjdong_code",
    "address_text",
    "management_agency_name",
    "developer_name",
    "designated_date",
    "completion_date",
    "official_area_sqm",
    "source_record_id",
    "source_snapshot_id",
    "valid_from_utc",
    "ingested_at_utc",
)

OPTIONAL_INPUT_COLUMNS: tuple[str, ...] = (
    "complex_id",
)

SILVER_COLUMNS: tuple[str, ...] = column_names(TABLE_CONTRACT)

CHECKSUM_COLUMNS: tuple[str, ...] = tuple(
    column for column in SILVER_COLUMNS if column != "row_checksum_sha256"
)

REQUIRED_SILVER_COLUMNS: tuple[str, ...] = required_column_names(TABLE_CONTRACT)

ALLOWED_COMPLEX_KINDS: tuple[str, ...] = (
    "national",
    "general",
    "agricultural",
    "urban_high_tech",
)

ALLOWED_STATUSES: tuple[str, ...] = (
    "planned",
    "developing",
    "operating",
    "changed",
    "abolished",
    "unknown",
)


def trim_to_null(column_name: str) -> F.Column:
    trimmed = F.trim(F.col(column_name))
    return F.when(F.length(trimmed) == 0, F.lit(None)).otherwise(trimmed)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build silver.industrial_complexes from Bronze JSONL input."
    )
    parser.add_argument("--input", required=True, help="Bronze JSONL input path.")
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
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_TABLE", "industrial_complexes"),
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


def validate_args(args: argparse.Namespace) -> None:
    if args.summary_output is not None and args.summary_output.strip() == "":
        raise ValueError("--summary-output must not be empty")
    if args.lineage_output is not None and args.lineage_output.strip() == "":
        raise ValueError("--lineage-output must not be empty")

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


def stable_uuid_v5(seed: F.Column) -> F.Column:
    """Return a deterministic RFC 4122 version-5 UUID string from a seed."""

    digest = F.sha1(F.concat(F.unhex(F.lit(UUID_NAMESPACE_URL_HEX)), F.encode(seed, "UTF-8")))
    variant_source = F.lower(F.substring(digest, 17, 1))
    variant_nibble = (
        F.when(variant_source.isin("0", "1", "2", "3"), F.lit("8"))
        .when(variant_source.isin("4", "5", "6", "7"), F.lit("9"))
        .when(variant_source.isin("8", "9", "a", "b"), F.lit("a"))
        .otherwise(F.lit("b"))
    )

    return F.lower(
        F.concat(
            F.substring(digest, 1, 8),
            F.lit("-"),
            F.substring(digest, 9, 4),
            F.lit("-5"),
            F.substring(digest, 14, 3),
            F.lit("-"),
            variant_nibble,
            F.substring(digest, 18, 3),
            F.lit("-"),
            F.substring(digest, 21, 12),
        )
    )


def read_bronze_jsonl(spark: SparkSession, input_path: str) -> DataFrame:
    bronze = spark.read.json(input_path)
    missing_columns = sorted(set(INPUT_COLUMNS) - set(bronze.columns))
    if missing_columns:
        raise ValueError(f"Bronze input is missing columns: {', '.join(missing_columns)}")
    selected_columns = [
        column
        for column in (*OPTIONAL_INPUT_COLUMNS, *INPUT_COLUMNS)
        if column in bronze.columns
    ]
    return bronze.select(*selected_columns)


def build_silver_frame(bronze: DataFrame) -> DataFrame:
    trimmed_code = F.trim(F.col("official_complex_code"))
    name = F.trim(F.col("complex_name"))
    normalized_name = F.lower(F.trim(F.regexp_replace(name, r"\s+", " ")))
    seed = F.concat(F.lit("foundation-platform:catalog:industrial_complex:"), trimmed_code)
    complex_id = stable_uuid_v5(seed)
    if "complex_id" in bronze.columns:
        supplied_complex_id = trim_to_null("complex_id")
        complex_id = F.coalesce(supplied_complex_id, complex_id)

    silver = bronze.select(
        complex_id.alias("complex_id"),
        trimmed_code.alias("official_complex_code"),
        name.alias("complex_name"),
        normalized_name.alias("complex_name_normalized"),
        F.trim(F.col("complex_kind")).alias("complex_kind"),
        F.trim(F.col("status")).alias("status"),
        F.trim(F.col("sido_code")).alias("sido_code"),
        F.trim(F.col("sigungu_code")).alias("sigungu_code"),
        trim_to_null("primary_bjdong_code").alias("primary_bjdong_code"),
        trim_to_null("address_text").alias("address_text"),
        trim_to_null("management_agency_name").alias("management_agency_name"),
        trim_to_null("developer_name").alias("developer_name"),
        F.to_date(F.col("designated_date"), "yyyy-MM-dd").alias("designated_date"),
        F.to_date(F.col("completion_date"), "yyyy-MM-dd").alias("completion_date"),
        F.col("official_area_sqm").cast(T.DecimalType(18, 2)).alias("official_area_sqm"),
        F.trim(F.col("source_record_id")).alias("source_record_id"),
        F.trim(F.col("source_snapshot_id")).alias("source_snapshot_id"),
        F.to_timestamp(F.col("valid_from_utc"), "yyyy-MM-dd'T'HH:mm:ssX").alias(
            "valid_from_utc"
        ),
        F.lit(None).cast(T.TimestampType()).alias("valid_to_utc"),
        F.to_timestamp(F.col("ingested_at_utc"), "yyyy-MM-dd'T'HH:mm:ssX").alias(
            "ingested_at_utc"
        ),
    )

    checksum_payload = F.to_json(F.struct(*[F.col(column) for column in CHECKSUM_COLUMNS]))
    return silver.withColumn("row_checksum_sha256", F.sha2(checksum_payload, 256)).select(
        *SILVER_COLUMNS
    )


def assert_columns(frame: DataFrame, expected_columns: Sequence[str]) -> None:
    actual_columns = tuple(frame.columns)
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


def required_string_columns() -> tuple[str, ...]:
    return required_string_column_names(TABLE_CONTRACT)


def collect_quality_metrics(silver: DataFrame) -> dict[str, int]:
    expressions: list[F.Column] = [F.count(F.lit(1)).cast("long").alias("row_count")]

    for column in REQUIRED_SILVER_COLUMNS:
        expressions.append(invalid_count(F.col(column).isNull(), f"{column}__null_count"))

    for column in required_string_columns():
        expressions.append(invalid_count(F.length(F.col(column)) == 0, f"{column}__empty_count"))

    expressions.extend(
        (
            invalid_count(
                ~F.col("complex_kind").isin(*ALLOWED_COMPLEX_KINDS),
                "invalid_complex_kind_count",
            ),
            invalid_count(
                ~F.col("status").isin(*ALLOWED_STATUSES),
                "invalid_status_count",
            ),
            invalid_count(
                F.col("official_area_sqm").isNotNull()
                & (F.col("official_area_sqm") <= 0),
                "invalid_official_area_count",
            ),
            invalid_count(
                ~F.col("complex_id").rlike(
                    "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
                ),
                "invalid_complex_id_count",
            ),
            invalid_count(
                ~F.col("row_checksum_sha256").rlike("^[0-9a-f]{64}$"),
                "invalid_checksum_count",
            ),
        )
    )

    row = silver.agg(*expressions).first()
    if row is None:
        raise ValueError("Silver quality metric aggregation returned no row")
    return {key: int(value or 0) for key, value in row.asDict().items()}


def assert_quality_metrics(silver: DataFrame, metrics: dict[str, int]) -> None:
    for column in REQUIRED_SILVER_COLUMNS:
        assert_no_invalid_rows(
            silver,
            metrics[f"{column}__null_count"],
            F.col(column).isNull(),
            f"{column} must not be null",
        )

    for column in required_string_columns():
        assert_no_invalid_rows(
            silver,
            metrics[f"{column}__empty_count"],
            F.length(F.col(column)) == 0,
            f"{column} must not be empty",
        )

    assert_no_invalid_rows(
        silver,
        metrics["invalid_complex_kind_count"],
        ~F.col("complex_kind").isin(*ALLOWED_COMPLEX_KINDS),
        "complex_kind is outside the allowed domain",
    )
    assert_no_invalid_rows(
        silver,
        metrics["invalid_status_count"],
        ~F.col("status").isin(*ALLOWED_STATUSES),
        "status is outside the allowed domain",
    )
    assert_no_invalid_rows(
        silver,
        metrics["invalid_official_area_count"],
        F.col("official_area_sqm").isNotNull() & (F.col("official_area_sqm") <= 0),
        "official_area_sqm must be positive when present",
    )
    assert_no_invalid_rows(
        silver,
        metrics["invalid_complex_id_count"],
        ~F.col("complex_id").rlike(
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
        ),
        "complex_id must be a lowercase UUID string",
    )
    assert_no_invalid_rows(
        silver,
        metrics["invalid_checksum_count"],
        ~F.col("row_checksum_sha256").rlike("^[0-9a-f]{64}$"),
        "row_checksum_sha256 must be lowercase sha256 hex",
    )


def assert_unique_source_keys(silver: DataFrame) -> None:
    duplicate_keys = (
        silver.groupBy("official_complex_code", "source_snapshot_id")
        .count()
        .where(F.col("count") > 1)
        .limit(5)
        .toJSON()
        .collect()
    )

    if duplicate_keys:
        raise ValueError(
            "(official_complex_code, source_snapshot_id) must be unique. "
            f"samples={duplicate_keys}"
        )


def validate_silver_frame(
    silver: DataFrame,
    expected_count: int | None,
) -> tuple[int, dict[str, int]]:
    assert_columns(silver, SILVER_COLUMNS)

    metrics = collect_quality_metrics(silver)
    assert_quality_metrics(silver, metrics)
    assert_unique_source_keys(silver)

    actual_count = metrics["row_count"]
    if expected_count is not None and actual_count != expected_count:
        raise ValueError(f"Expected {expected_count} Silver rows, found {actual_count}")

    return actual_count, metrics


def collect_source_snapshot_summary(silver: DataFrame) -> dict[str, Any]:
    snapshots = silver.select("source_snapshot_id").distinct()
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
            "kind": "bronze_jsonl",
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


def json_payload(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def emit_run_summary(summary: dict[str, Any], output_path: str | None) -> None:
    payload = json_payload(summary)
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{payload}\n", encoding="utf-8")

    print(f"silver-industrial-complexes-summary-json {payload}")


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
            {
                "dataset": BRONZE_DATASET_NAME,
                "column": "official_complex_code",
                "transform": "uuidv5_seed",
            },
            {"dataset": BRONZE_DATASET_NAME, "column": "complex_id", "transform": "coalesce"},
        ],
        "official_complex_code": [
            {"dataset": BRONZE_DATASET_NAME, "column": "official_complex_code", "transform": "trim"}
        ],
        "complex_name": [
            {"dataset": BRONZE_DATASET_NAME, "column": "complex_name", "transform": "trim"}
        ],
        "complex_name_normalized": [
            {
                "dataset": BRONZE_DATASET_NAME,
                "column": "complex_name",
                "transform": "trim_lower_collapse_whitespace",
            }
        ],
        "complex_kind": [
            {"dataset": BRONZE_DATASET_NAME, "column": "complex_kind", "transform": "trim"}
        ],
        "status": [
            {"dataset": BRONZE_DATASET_NAME, "column": "status", "transform": "trim"}
        ],
        "sido_code": [
            {"dataset": BRONZE_DATASET_NAME, "column": "sido_code", "transform": "trim"}
        ],
        "sigungu_code": [
            {"dataset": BRONZE_DATASET_NAME, "column": "sigungu_code", "transform": "trim"}
        ],
        "primary_bjdong_code": [
            {"dataset": BRONZE_DATASET_NAME, "column": "primary_bjdong_code", "transform": "trim_to_null"}
        ],
        "address_text": [
            {"dataset": BRONZE_DATASET_NAME, "column": "address_text", "transform": "trim_to_null"}
        ],
        "management_agency_name": [
            {
                "dataset": BRONZE_DATASET_NAME,
                "column": "management_agency_name",
                "transform": "trim_to_null",
            }
        ],
        "developer_name": [
            {"dataset": BRONZE_DATASET_NAME, "column": "developer_name", "transform": "trim_to_null"}
        ],
        "designated_date": [
            {"dataset": BRONZE_DATASET_NAME, "column": "designated_date", "transform": "to_date"}
        ],
        "completion_date": [
            {"dataset": BRONZE_DATASET_NAME, "column": "completion_date", "transform": "to_date"}
        ],
        "official_area_sqm": [
            {
                "dataset": BRONZE_DATASET_NAME,
                "column": "official_area_sqm",
                "transform": "cast_decimal_18_2",
            }
        ],
        "source_record_id": [
            {"dataset": BRONZE_DATASET_NAME, "column": "source_record_id", "transform": "trim"}
        ],
        "source_snapshot_id": [
            {"dataset": BRONZE_DATASET_NAME, "column": "source_snapshot_id", "transform": "trim"}
        ],
        "valid_from_utc": [
            {"dataset": BRONZE_DATASET_NAME, "column": "valid_from_utc", "transform": "to_timestamp"}
        ],
        "valid_to_utc": [
            {"dataset": "foundation-platform.job_arguments", "column": "null", "transform": "literal_null"}
        ],
        "ingested_at_utc": [
            {"dataset": BRONZE_DATASET_NAME, "column": "ingested_at_utc", "transform": "to_timestamp"}
        ],
        "row_checksum_sha256": [
            {
                "dataset": RUN_SUMMARY_CONTRACT,
                "column": "all_non_checksum_columns",
                "transform": "sha256_json_struct",
            }
        ],
    }
    return [
        {"output_column": column, "inputs": lineage_map[column]}
        for column in SILVER_COLUMNS
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
            "qualified_name": BRONZE_DATASET_NAME,
            "namespace": "bronze",
            "table": "industrial_complexes_raw_jsonl",
            "storage_format": "jsonl",
        },
        "output_dataset": {
            "qualified_name": RUN_SUMMARY_CONTRACT,
            "namespace": args.iceberg_namespace,
            "table": args.iceberg_table,
            "storage_format": args.write_mode,
        },
        "source_snapshot_ids": summary["source_snapshot_ids"],
        "source_snapshot_truncated": summary["source_snapshot_truncated"],
        "iceberg_snapshot": {
            "catalog": args.iceberg_catalog_name,
            "namespace": args.iceberg_namespace,
            "table": args.iceberg_table,
            "snapshot_id": None,
        },
        "quality_metrics": lineage_quality_metrics(summary),
        "column_lineage": column_lineage(),
        "openlineage_mapping": {
            "event_type": "COMPLETE",
            "job_namespace": "foundation-platform.lakehouse",
            "job_name": JOB_NAME,
            "input_namespace": "foundation-platform.bronze",
            "output_namespace": f"{args.iceberg_catalog_name}.{args.iceberg_namespace}",
        },
    }


def emit_lineage_event(event: dict[str, Any], output_path: str | None) -> None:
    if not output_path:
        return

    payload = json_payload(event)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{payload}\n", encoding="utf-8")
    print(f"silver-industrial-complexes-lineage-json {payload}")


def write_silver_parquet(silver: DataFrame, output_path: str) -> None:
    (
        silver.repartition("sido_code")
        .sortWithinPartitions(
            "sigungu_code",
            "complex_name_normalized",
            "official_complex_code",
        )
        .write.mode("overwrite")
        .partitionBy("sido_code")
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
    temp_view = "silver_industrial_complexes_candidate"

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
        SparkSession.builder.appName("foundation-platform-industrial-complex-bronze-to-silver")
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
        bronze = read_bronze_jsonl(spark, args.input)
        silver = build_silver_frame(bronze).persist(StorageLevel.MEMORY_AND_DISK)
        row_count, quality_metrics = validate_silver_frame(silver, args.expected_count)
        source_snapshot_summary = collect_source_snapshot_summary(silver)

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
            print(f"silver-industrial-complexes-validate-ok rows={row_count}")
            return 0

        if args.write_mode == "parquet":
            write_silver_parquet(silver, args.output)
            persisted = spark.read.parquet(args.output).select(*SILVER_COLUMNS)
            success_target = f"output={args.output}"
            success_label = "silver-industrial-complexes-write-ok"
        else:
            write_silver_iceberg(spark, silver, args)
            persisted = read_iceberg_snapshot_for_batch(spark, silver, args)
            success_target = f"table={args.iceberg_namespace}.{args.iceberg_table}"
            success_label = "silver-industrial-complexes-iceberg-write-ok"

        persisted_count, persisted_quality_metrics = validate_silver_frame(
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
        if "silver" in locals():
            silver.unpersist()
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
