#!/usr/bin/env python3
"""Write scalar Silver handoff rows to Parquet or Iceberg.

Rust foundation-platform owns deterministic normalization and emits writer-neutral
Silver handoff rows. This job is intentionally storage-focused: it loads
the Rust-exported table contract, checks required fields and basic quality
metrics, then writes the rows to a local Parquet path or an Iceberg REST catalog.

It supports scalar contracts only. Geometry/binary transport remains owned by
specialized jobs such as `vworld_parcel_boundaries_handoff_to_silver.py`.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from platform_contracts import (
    column_names,
    columns,
    create_table_columns_sql,
    load_lakehouse_contract,
    partition_spec_sql,
    required_column_names,
    required_string_column_names,
    spark_sql_type,
)


DEFAULT_ICEBERG_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
    "org.apache.iceberg:iceberg-aws-bundle:1.6.1"
)
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
BUCKET_PARTITION_PATTERN = re.compile(r"^bucket\((\d+),\s*([A-Za-z_][A-Za-z0-9_]*)\)$")
ICEBERG_BUCKET_SPLIT_COUNT = 16
RUN_SUMMARY_SCHEMA_VERSION = "foundation-platform.spark_run_summary.v1"
RUN_SUMMARY_INPUT_KIND_BY_FORMAT = {
    "jsonl": "silver_handoff_jsonl",
    "parquet": "silver_handoff_parquet",
}
SKIP_SPARK_STOP_AFTER_SUCCESS_ENV = "FOUNDATION_PLATFORM_SPARK_SKIP_STOP_ON_SUCCESS"


def default_iceberg_target(contract_name: str) -> tuple[str, str]:
    parts = contract_name.split(".")
    if len(parts) != 2 or not all(parts):
        raise ValueError(f"contract name must be namespace.table: {contract_name}")
    return parts[0], parts[1]


def simple_parquet_partition_columns(contract: dict[str, Any]) -> tuple[str, ...]:
    return tuple(
        item
        for item in contract.get("partition_spec", [])
        if isinstance(item, str) and IDENTIFIER_PATTERN.fullmatch(item) is not None
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write scalar Silver handoff rows to Parquet or Iceberg."
    )
    parser.add_argument("--input", required=True, help="Silver handoff input path.")
    parser.add_argument(
        "--input-format",
        choices=("jsonl", "parquet"),
        default="jsonl",
        help="Physical format of the Silver handoff input.",
    )
    parser.add_argument("--contract", required=True, help="Lakehouse table contract name.")
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
    parser.add_argument("--iceberg-namespace", help="Iceberg namespace override.")
    parser.add_argument("--iceberg-table", help="Iceberg table override.")
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
        "--input-file-batch-size",
        type=int,
        default=0,
        help=(
            "When greater than zero, process an input directory in sorted file "
            "batches. Supported for Iceberg writes to avoid one giant input frame."
        ),
    )
    parser.add_argument(
        "--defer-iceberg-readback-validation",
        action="store_true",
        help=(
            "Do not read the Iceberg table back from the same Spark JVM after writing. "
            "Use when readback is validated by a separate query engine such as Trino."
        ),
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


def should_skip_spark_stop_after_success(lookup: Any = os.getenv) -> bool:
    return lookup(SKIP_SPARK_STOP_AFTER_SUCCESS_ENV) == "1"


def exit_after_success_if_requested() -> None:
    if not should_skip_spark_stop_after_success():
        return
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)


def lakehouse_oauth2_server_uri(catalog_uri: str) -> str:
    configured_uri = os.getenv("FOUNDATION_PLATFORM_LAKEHOUSE_OAUTH2_SERVER_URI")
    if configured_uri is not None and configured_uri.strip() != "":
        return configured_uri.strip()
    return f"{catalog_uri.rstrip('/')}/v1/oauth/tokens"


def validate_identifier(label: str, value: str) -> None:
    if IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{label} must be a simple identifier: {value}")


def validate_scalar_contract(contract: dict[str, Any]) -> None:
    binary_columns = [
        column["name"]
        for column in contract["columns"]
        if column["logical_type"] == "binary"
    ]
    if binary_columns:
        raise ValueError(
            "silver_scalar_handoff_to_lakehouse does not support binary columns: "
            + ", ".join(binary_columns)
        )


def resolve_iceberg_namespace(args: argparse.Namespace) -> str:
    namespace, _ = default_iceberg_target(args.contract)
    return args.iceberg_namespace or namespace


def resolve_iceberg_table(args: argparse.Namespace) -> str:
    _, table = default_iceberg_target(args.contract)
    return args.iceberg_table or table


def validate_args(args: argparse.Namespace) -> None:
    if args.input_file_batch_size < 0:
        raise ValueError("--input-file-batch-size must be zero or greater")
    if args.input_file_batch_size > 0 and args.write_mode != "iceberg":
        raise ValueError("input file batching is only supported for --write-mode=iceberg")
    if args.defer_iceberg_readback_validation and args.write_mode != "iceberg":
        raise ValueError(
            "deferred Iceberg readback validation is only supported for --write-mode=iceberg"
        )
    if args.summary_output is not None and args.summary_output.strip() == "":
        raise ValueError("--summary-output must not be empty")
    if args.write_mode == "parquet" and not args.output:
        raise ValueError("--output is required when --write-mode=parquet")
    if args.write_mode != "iceberg":
        return

    namespace = resolve_iceberg_namespace(args)
    table = resolve_iceberg_table(args)
    validate_identifier("iceberg catalog name", args.iceberg_catalog_name)
    validate_identifier("iceberg namespace", namespace)
    validate_identifier("iceberg table", table)

    if args.iceberg_write_mode == "overwrite":
        is_smoke_table = table.endswith("_smoke")
        if not is_smoke_table and not args.allow_non_smoke_overwrite:
            raise ValueError(
                "Refusing to overwrite a non-smoke Iceberg table without "
                "--allow-non-smoke-overwrite"
            )
    for name in required_iceberg_env():
        require_env(name)


def load_pyspark() -> tuple[Any, Any, Any, Any]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.storagelevel import StorageLevel

    return SparkSession, F, T, StorageLevel


def spark_type(logical_type: str, T: Any) -> Any:
    if logical_type == "string":
        return T.StringType()
    if logical_type == "int":
        return T.IntegerType()
    if logical_type == "long":
        return T.LongType()
    if logical_type == "double":
        return T.DoubleType()
    if logical_type == "date":
        return T.DateType()
    if logical_type == "timestamp":
        return T.TimestampType()
    if logical_type.startswith("decimal(") and logical_type.endswith(")"):
        precision, scale = logical_type.removeprefix("decimal(").removesuffix(")").split(",")
        return T.DecimalType(int(precision), int(scale))
    raise ValueError(f"unsupported scalar lakehouse logical type: {logical_type}")


def spark_struct_schema(contract: dict[str, Any], T: Any) -> Any:
    return T.StructType(
        [
            T.StructField(
                column["name"],
                spark_type(column["logical_type"], T),
                not column["required"],
            )
            for column in contract["columns"]
        ]
    )


def read_handoff_jsonl(
    spark: Any,
    input_path: str | Sequence[str],
    contract: dict[str, Any],
    T: Any,
) -> Any:
    handoff = spark.read.schema(spark_struct_schema(contract, T)).json(input_path)
    expected_columns = column_names(contract)
    missing_columns = sorted(set(expected_columns) - set(handoff.columns))
    if missing_columns:
        raise ValueError(f"Silver handoff is missing columns: {', '.join(missing_columns)}")
    return handoff.select(*expected_columns)


def read_handoff_parquet(
    spark: Any,
    input_path: str | Sequence[str],
    contract: dict[str, Any],
) -> Any:
    if isinstance(input_path, str):
        handoff = spark.read.parquet(input_path)
    else:
        handoff = spark.read.parquet(*input_path)
    expected_columns = column_names(contract)
    missing_columns = sorted(set(expected_columns) - set(handoff.columns))
    if missing_columns:
        raise ValueError(f"Silver handoff is missing columns: {', '.join(missing_columns)}")
    return handoff.select(*expected_columns)


def read_handoff(
    spark: Any,
    input_path: str | Sequence[str],
    contract: dict[str, Any],
    input_format: str,
    T: Any,
) -> Any:
    if input_format == "jsonl":
        return read_handoff_jsonl(spark, input_path, contract, T)
    if input_format == "parquet":
        return read_handoff_parquet(spark, input_path, contract)
    raise ValueError(f"unsupported Silver handoff input format: {input_format}")


def collect_input_batches(input_path: str, batch_size: int, input_format: str) -> list[list[str]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")
    path = Path(input_path)
    if path.is_file():
        return [[str(path)]]
    if not path.is_dir():
        raise ValueError(f"input path does not exist: {input_path}")
    suffix_by_format = {
        "jsonl": ".jsonl",
        "parquet": ".parquet",
    }
    if input_format not in suffix_by_format:
        raise ValueError(f"unsupported Silver handoff input format: {input_format}")
    allowed_suffix = suffix_by_format[input_format]
    input_files = sorted(
        str(item) for item in path.iterdir() if item.is_file() and item.suffix == allowed_suffix
    )
    if not input_files:
        raise ValueError(f"input directory has no {allowed_suffix} handoff files: {input_path}")
    return [
        input_files[index : index + batch_size]
        for index in range(0, len(input_files), batch_size)
    ]


def iceberg_write_mode_for_input_batch(args: argparse.Namespace, batch_index: int) -> str:
    if batch_index == 0:
        return args.iceberg_write_mode
    return "append"


def cast_handoff_frame(handoff: Any, contract: dict[str, Any], F: Any) -> Any:
    expressions = []
    for column in contract["columns"]:
        name = column["name"]
        logical_type = column["logical_type"]
        source = F.col(name)
        if logical_type == "timestamp":
            expressions.append(F.to_timestamp(source, "yyyy-MM-dd'T'HH:mm:ssX").alias(name))
        elif logical_type == "date":
            expressions.append(F.to_date(source, "yyyy-MM-dd").alias(name))
        elif logical_type in {"int", "long", "double"} or logical_type.startswith("decimal("):
            expressions.append(source.cast(logical_type).alias(name))
        else:
            expressions.append(source.alias(name))
    return handoff.select(*expressions)


def handoff_storage_level(StorageLevel: Any) -> Any:
    return StorageLevel.DISK_ONLY


def invalid_count(predicate: Any, alias: str, F: Any) -> Any:
    return F.sum(F.when(predicate, F.lit(1)).otherwise(F.lit(0))).cast("long").alias(alias)


def collect_quality_metrics(frame: Any, contract: dict[str, Any], F: Any) -> dict[str, int]:
    expressions = [F.count(F.lit(1)).cast("long").alias("row_count")]
    for column in required_column_names(contract):
        expressions.append(invalid_count(F.col(column).isNull(), f"{column}__null_count", F))
    for column in required_string_column_names(contract):
        expressions.append(invalid_count(F.length(F.col(column)) == 0, f"{column}__empty_count", F))
    if "row_checksum_sha256" in frame.columns:
        expressions.append(
            invalid_count(
                ~F.col("row_checksum_sha256").rlike("^[0-9a-f]{64}$"),
                "invalid_checksum_count",
                F,
            )
        )
    if "normalization_status" in frame.columns:
        expressions.append(
            invalid_count(
                F.col("normalization_status") == "proposal_required",
                "proposal_required_count",
                F,
            )
        )
        expressions.append(
            invalid_count(
                ~F.col("normalization_status").isin("accepted", "proposal_required"),
                "invalid_normalization_status_count",
                F,
            )
        )

    row = frame.agg(*expressions).first()
    if row is None:
        raise ValueError("Silver handoff quality metric aggregation returned no row")
    metrics = {key: int(value or 0) for key, value in row.asDict().items()}
    metrics.setdefault("invalid_checksum_count", 0)
    metrics.setdefault("proposal_required_count", 0)
    metrics.setdefault("invalid_normalization_status_count", 0)
    return metrics


def assert_no_invalid_rows(
    frame: Any,
    metric_count: int,
    predicate: Any,
    message: str,
) -> None:
    if metric_count == 0:
        return
    samples = [str(sample) for sample in frame.where(predicate).limit(5).toJSON().collect()]
    raise ValueError(f"{message}. count={metric_count} samples={samples}")


def assert_quality_metrics(frame: Any, contract: dict[str, Any], metrics: dict[str, int], F: Any) -> None:
    for column in required_column_names(contract):
        assert_no_invalid_rows(
            frame,
            metrics[f"{column}__null_count"],
            F.col(column).isNull(),
            f"{column} must not be null",
        )
    for column in required_string_column_names(contract):
        assert_no_invalid_rows(
            frame,
            metrics[f"{column}__empty_count"],
            F.length(F.col(column)) == 0,
            f"{column} must not be empty",
        )
    if "row_checksum_sha256" in frame.columns:
        assert_no_invalid_rows(
            frame,
            metrics["invalid_checksum_count"],
            ~F.col("row_checksum_sha256").rlike("^[0-9a-f]{64}$"),
            "row_checksum_sha256 must be lowercase sha256 hex",
        )
    if "normalization_status" in frame.columns:
        assert_no_invalid_rows(
            frame,
            metrics["invalid_normalization_status_count"],
            ~F.col("normalization_status").isin("accepted", "proposal_required"),
            "normalization_status is outside the allowed domain",
        )


def validate_frame(frame: Any, contract: dict[str, Any], expected_count: int | None, F: Any) -> tuple[int, dict[str, int]]:
    actual_columns = tuple(frame.columns)
    expected_columns = column_names(contract)
    if actual_columns != expected_columns:
        raise ValueError(
            "Unexpected Silver columns. "
            f"expected={list(expected_columns)} actual={list(actual_columns)}"
        )
    metrics = collect_quality_metrics(frame, contract, F)
    assert_quality_metrics(frame, contract, metrics, F)
    actual_count = metrics["row_count"]
    if expected_count is not None and actual_count != expected_count:
        raise ValueError(f"Expected {expected_count} Silver rows, found {actual_count}")
    return actual_count, metrics


def merge_quality_metrics(metrics: Sequence[dict[str, int]]) -> dict[str, int]:
    merged: dict[str, int] = {}
    for item in metrics:
        for key, value in item.items():
            merged[key] = merged.get(key, 0) + int(value)
    return merged


def source_snapshot_summary_from_ids(source_snapshot_ids: Sequence[str]) -> dict[str, Any]:
    snapshot_ids = sorted(set(source_snapshot_ids))
    return {
        "source_snapshot_count": len(snapshot_ids),
        "source_snapshot_ids": snapshot_ids,
        "source_snapshot_truncated": False,
    }


def collect_source_snapshot_summary(frame: Any) -> dict[str, Any]:
    snapshots = frame.select("source_snapshot_id").distinct()
    snapshot_ids = [
        row.source_snapshot_id
        for row in snapshots.orderBy("source_snapshot_id").collect()
    ]
    return source_snapshot_summary_from_ids(snapshot_ids)


def run_summary_target(args: argparse.Namespace) -> dict[str, str]:
    if args.write_mode == "parquet":
        return {"kind": "parquet", "path": args.output}
    namespace = resolve_iceberg_namespace(args)
    table = resolve_iceberg_table(args)
    return {
        "kind": "iceberg",
        "catalog": args.iceberg_catalog_name,
        "namespace": namespace,
        "table": table,
        "qualified_table": f"{args.iceberg_catalog_name}.{namespace}.{table}",
    }


def run_summary_disposition(args: argparse.Namespace) -> str:
    if args.validate_only:
        return "validate_only"
    if args.write_mode == "parquet":
        return "parquet_overwrite"
    return f"iceberg_{args.iceberg_write_mode}"


def build_run_summary(
    args: argparse.Namespace,
    contract: dict[str, Any],
    row_count: int,
    persisted_row_count: int | None,
    quality_metrics: dict[str, int],
    source_snapshot_summary: dict[str, Any],
) -> dict[str, Any]:
    summary = {
        "schema_version": RUN_SUMMARY_SCHEMA_VERSION,
        "job_name": "silver_scalar_handoff_to_lakehouse",
        "contract": args.contract,
        "created_at_utc": datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "input": {
            "kind": RUN_SUMMARY_INPUT_KIND_BY_FORMAT[args.input_format],
            "path": args.input,
        },
        "target": run_summary_target(args),
        "write_mode": args.write_mode,
        "write_disposition": run_summary_disposition(args),
        "row_count": row_count,
        "persisted_row_count": persisted_row_count,
        "quality_metrics": dict(quality_metrics, persisted_row_count=persisted_row_count)
        if persisted_row_count is not None
        else quality_metrics,
        "column_count": len(column_names(contract)),
        "columns": list(column_names(contract)),
        "required_columns": list(required_column_names(contract)),
        **source_snapshot_summary,
    }
    if args.write_mode == "iceberg":
        summary["iceberg_readback_validation"] = (
            "deferred"
            if getattr(args, "defer_iceberg_readback_validation", False)
            else "spark"
        )
    return summary


def emit_run_summary(summary: dict[str, Any], output_path: str | None) -> None:
    payload = json.dumps(summary, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{payload}\n", encoding="utf-8")
    print(f"silver-scalar-handoff-summary-json {payload}")


def write_silver_parquet(frame: Any, output_path: str, contract: dict[str, Any]) -> None:
    writer = frame.write.mode("overwrite")
    partition_columns = simple_parquet_partition_columns(contract)
    if partition_columns:
        writer.partitionBy(*partition_columns).parquet(output_path)
    else:
        writer.parquet(output_path)


def bucket_partition(contract: dict[str, Any]) -> tuple[int, str] | None:
    for item in contract.get("partition_spec", []):
        if not isinstance(item, str):
            continue
        match = BUCKET_PARTITION_PATTERN.fullmatch(item)
        if match is None:
            continue
        return int(match.group(1)), match.group(2)
    return None


def cluster_frame_for_iceberg_write(frame: Any, contract: dict[str, Any], F: Any) -> Any:
    partition = bucket_partition(contract)
    if partition is None:
        return frame

    bucket_count, bucket_column = partition
    identity_column = column_names(contract)[0]
    bucket_temp_column = "__iceberg_write_bucket"
    split_temp_column = "__iceberg_write_split"
    sort_columns = [
        item
        for item in contract.get("sort_order", [])
        if isinstance(item, str) and IDENTIFIER_PATTERN.fullmatch(item)
    ]
    if not sort_columns:
        sort_columns = [bucket_column, identity_column]

    return (
        frame.withColumn(
            bucket_temp_column,
            F.pmod(F.xxhash64(F.col(bucket_column)), F.lit(bucket_count)),
        )
        .withColumn(
            split_temp_column,
            F.pmod(F.xxhash64(F.col(identity_column)), F.lit(ICEBERG_BUCKET_SPLIT_COUNT)),
        )
        .repartition(bucket_count * ICEBERG_BUCKET_SPLIT_COUNT, bucket_temp_column, split_temp_column)
        .sortWithinPartitions(bucket_temp_column, *sort_columns)
        .drop(bucket_temp_column, split_temp_column)
    )


def qualified_iceberg_table(args: argparse.Namespace) -> str:
    return (
        f"`{args.iceberg_catalog_name}`."
        f"`{resolve_iceberg_namespace(args)}`."
        f"`{resolve_iceberg_table(args)}`"
    )


def create_iceberg_table_if_missing(spark: Any, args: argparse.Namespace, contract: dict[str, Any]) -> None:
    namespace = f"`{args.iceberg_catalog_name}`.`{resolve_iceberg_namespace(args)}`"
    table = qualified_iceberg_table(args)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
{create_table_columns_sql(contract)}
        )
        USING iceberg
        PARTITIONED BY ({partition_spec_sql(contract)})
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'zstd',
            'write.distribution-mode' = 'hash'
        )
        """
    )


def describe_table_column_names(spark: Any, table: str) -> set[str]:
    names: set[str] = set()
    for row in spark.sql(f"DESCRIBE {table}").collect():
        col_name = getattr(row, "col_name", None)
        if col_name is None:
            col_name = row[0]
        col_name = str(col_name).strip()
        if col_name == "" or col_name.startswith("#"):
            continue
        names.add(col_name)
    return names


def add_missing_nullable_iceberg_columns(
    spark: Any,
    args: argparse.Namespace,
    contract: dict[str, Any],
) -> None:
    table = qualified_iceberg_table(args)
    existing_columns = describe_table_column_names(spark, table)
    missing_required_columns: list[str] = []
    missing_nullable_columns: list[str] = []

    for column in columns(contract):
        name = column["name"]
        if name in existing_columns:
            continue
        if column["required"]:
            missing_required_columns.append(name)
            continue
        missing_nullable_columns.append(f"{name} {spark_sql_type(column['logical_type'])}")

    if missing_required_columns:
        raise ValueError(
            "existing Iceberg table is missing required contract columns: "
            + ", ".join(missing_required_columns)
        )
    if missing_nullable_columns:
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({', '.join(missing_nullable_columns)})")


def write_silver_iceberg(
    spark: Any,
    frame: Any,
    args: argparse.Namespace,
    contract: dict[str, Any],
    F: Any,
    iceberg_write_mode: str | None = None,
) -> None:
    table = qualified_iceberg_table(args)
    temp_view = "silver_scalar_handoff_candidate"
    create_iceberg_table_if_missing(spark, args, contract)
    add_missing_nullable_iceberg_columns(spark, args, contract)
    cluster_frame_for_iceberg_write(
        frame.select(*column_names(contract)),
        contract,
        F,
    ).createOrReplaceTempView(temp_view)
    statement = "INSERT INTO"
    if (iceberg_write_mode or args.iceberg_write_mode) == "overwrite":
        statement = "INSERT OVERWRITE"
    target_columns = ", ".join(column_names(contract))
    spark.sql(
        f"""
        {statement} {table} ({target_columns})
        SELECT {target_columns}
        FROM {temp_view}
        """
    )


def build_spark_session(args: argparse.Namespace, SparkSession: Any) -> Any:
    builder = (
        SparkSession.builder.appName("foundation-platform-silver-scalar-handoff-to-lakehouse")
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
            .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
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
    if args.write_mode == "iceberg":
        assert_iceberg_runtime_loaded(spark, args.iceberg_packages)
    return spark


def assert_iceberg_runtime_loaded(spark: Any, packages: str) -> None:
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


def read_iceberg_snapshot_for_batch(spark: Any, frame: Any, args: argparse.Namespace, contract: dict[str, Any], F: Any) -> Any:
    snapshot_rows = frame.select("source_snapshot_id").distinct().limit(17).collect()
    snapshot_ids = [row.source_snapshot_id for row in snapshot_rows]
    return read_iceberg_snapshot_for_source_ids(spark, snapshot_ids, args, contract, F)


def read_iceberg_snapshot_for_source_ids(
    spark: Any,
    source_snapshot_ids: Sequence[str],
    args: argparse.Namespace,
    contract: dict[str, Any],
    F: Any,
) -> Any:
    snapshot_ids = sorted(set(source_snapshot_ids))
    if not snapshot_ids:
        raise ValueError("Cannot verify Iceberg write because source_snapshot_id is empty")
    if len(snapshot_ids) > 16:
        raise ValueError("Iceberg write verification supports at most 16 source snapshots")
    return (
        spark.table(qualified_iceberg_table(args))
        .where(F.col("source_snapshot_id").isin(snapshot_ids))
        .select(*column_names(contract))
    )


def run_batched_input(
    spark: Any,
    args: argparse.Namespace,
    contract: dict[str, Any],
    F: Any,
    T: Any,
    StorageLevel: Any,
) -> int:
    batches = collect_input_batches(args.input, args.input_file_batch_size, args.input_format)
    row_count = 0
    batch_metrics: list[dict[str, int]] = []
    source_snapshot_ids: list[str] = []

    for batch_index, batch in enumerate(batches):
        frame = None
        try:
            handoff = read_handoff(spark, batch, contract, args.input_format, T)
            candidate = (
                cast_handoff_frame(handoff, contract, F)
                if args.input_format == "jsonl"
                else handoff.select(*column_names(contract))
            )
            frame = candidate.persist(
                handoff_storage_level(StorageLevel)
            )
            current_count, current_metrics = validate_frame(frame, contract, None, F)
            row_count += current_count
            batch_metrics.append(current_metrics)
            source_snapshot_ids.extend(
                collect_source_snapshot_summary(frame)["source_snapshot_ids"]
            )

            if not args.validate_only:
                write_silver_iceberg(
                    spark,
                    frame,
                    args,
                    contract,
                    F,
                    iceberg_write_mode_for_input_batch(args, batch_index),
                )
        finally:
            if frame is not None:
                frame.unpersist()

    if args.expected_count is not None and row_count != args.expected_count:
        raise ValueError(f"Expected {args.expected_count} Silver rows, found {row_count}")

    quality_metrics = merge_quality_metrics(batch_metrics)
    source_snapshot_summary = source_snapshot_summary_from_ids(source_snapshot_ids)
    if args.validate_only:
        emit_run_summary(
            build_run_summary(
                args,
                contract,
                row_count=row_count,
                persisted_row_count=None,
                quality_metrics=quality_metrics,
                source_snapshot_summary=source_snapshot_summary,
            ),
            args.summary_output,
        )
        print(f"silver-scalar-handoff-validate-ok rows={row_count} contract={args.contract}")
        exit_after_success_if_requested()
        return 0

    if args.defer_iceberg_readback_validation:
        persisted_count = row_count
        persisted_quality_metrics = quality_metrics
    else:
        persisted = read_iceberg_snapshot_for_source_ids(
            spark,
            source_snapshot_ids,
            args,
            contract,
            F,
        )
        persisted_count, persisted_quality_metrics = validate_frame(
            persisted,
            contract,
            args.expected_count,
            F,
        )
    if persisted_count != row_count:
        raise ValueError(
            f"Persisted row count changed. before={row_count} after={persisted_count}"
        )
    emit_run_summary(
        build_run_summary(
            args,
            contract,
            row_count=row_count,
            persisted_row_count=persisted_count,
            quality_metrics=persisted_quality_metrics,
            source_snapshot_summary=source_snapshot_summary,
        ),
        args.summary_output,
    )
    success_target = f"table={resolve_iceberg_namespace(args)}.{resolve_iceberg_table(args)}"
    print(f"silver-scalar-handoff-iceberg-write-ok rows={persisted_count} contract={args.contract} {success_target}")
    exit_after_success_if_requested()
    return 0


def main() -> int:
    args = parse_args()
    validate_args(args)
    contract = load_lakehouse_contract(args.contract)
    validate_scalar_contract(contract)
    SparkSession, F, T, StorageLevel = load_pyspark()
    spark = build_spark_session(args, SparkSession)
    try:
        if args.input_file_batch_size > 0:
            return run_batched_input(spark, args, contract, F, T, StorageLevel)

        handoff = read_handoff(spark, args.input, contract, args.input_format, T)
        candidate = (
            cast_handoff_frame(handoff, contract, F)
            if args.input_format == "jsonl"
            else handoff.select(*column_names(contract))
        )
        frame = candidate.persist(
            handoff_storage_level(StorageLevel)
        )
        row_count, quality_metrics = validate_frame(frame, contract, args.expected_count, F)
        source_snapshot_summary = collect_source_snapshot_summary(frame)
        if args.validate_only:
            emit_run_summary(
                build_run_summary(
                    args,
                    contract,
                    row_count=row_count,
                    persisted_row_count=None,
                    quality_metrics=quality_metrics,
                    source_snapshot_summary=source_snapshot_summary,
                ),
                args.summary_output,
            )
            print(f"silver-scalar-handoff-validate-ok rows={row_count} contract={args.contract}")
            exit_after_success_if_requested()
            return 0

        if args.write_mode == "parquet":
            write_silver_parquet(frame, args.output, contract)
            persisted = spark.read.parquet(args.output).select(*column_names(contract))
            success_target = f"output={args.output}"
            success_label = "silver-scalar-handoff-write-ok"
        else:
            write_silver_iceberg(spark, frame, args, contract, F)
            success_target = (
                f"table={resolve_iceberg_namespace(args)}.{resolve_iceberg_table(args)}"
            )
            success_label = "silver-scalar-handoff-iceberg-write-ok"

        if args.write_mode == "iceberg" and args.defer_iceberg_readback_validation:
            persisted_count = row_count
            persisted_quality_metrics = quality_metrics
        else:
            if args.write_mode == "iceberg":
                persisted = read_iceberg_snapshot_for_batch(spark, frame, args, contract, F)
            persisted_count, persisted_quality_metrics = validate_frame(
                persisted,
                contract,
                args.expected_count,
                F,
            )
        if persisted_count != row_count:
            raise ValueError(
                f"Persisted row count changed. before={row_count} after={persisted_count}"
            )
        emit_run_summary(
            build_run_summary(
                args,
                contract,
                row_count=row_count,
                persisted_row_count=persisted_count,
                quality_metrics=persisted_quality_metrics,
                source_snapshot_summary=source_snapshot_summary,
            ),
            args.summary_output,
        )
        print(f"{success_label} rows={persisted_count} contract={args.contract} {success_target}")
        exit_after_success_if_requested()
        return 0
    finally:
        if "frame" in locals():
            frame.unpersist()
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
