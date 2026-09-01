#!/usr/bin/env python3
"""Write industrial-complex boundary handoff rows into the Silver table shape.

Rust foundation-platform owns the boundary normalization contract and emits a writer-neutral JSONL
handoff. This job owns the storage-engine step: decode the transport-only geometry, resolve the two
columns the handoff deliberately leaves null, verify the Silver quality gates, and write Parquet or
Iceberg rows whose columns match `lakehouse_domain::SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES`.

**The join is what makes this job different from its parcel sibling.** A boundary row arrives
carrying `official_complex_code` and a null `complex_id` and `sido_code`, because:

- `complex_id` has one definition in this repository, the derivation in
  `industrial_complex_bronze_to_silver.py` that writes `silver.industrial_complexes`. This job reads
  the id off that table rather than deriving it a second time. Two derivations that agree today
  disagree the day one changes, and the failure is silent: both sides still look like lowercase
  UUIDs and only the join count moves.
- `sido_code` is required here and is a partition key, and the boundary source carries no
  administrative code at all. It comes off the same row.

Three counts fall out of that join and are recorded rather than dropped:

- `orphan_boundary_count` — boundaries whose code names no complex. The provider ships 19.
- `complex_without_boundary_count` — complexes the boundary source never drew. The provider leaves
  98 without one. Neither is a defect; both are facts about the source.
- `complex_without_sido_code_count` — complexes whose own row states no province. A boundary cannot
  be written without one because it is a required partition column.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lakehouse_engine import iceberg_packages
from lakehouse_ingest import append_batch_once, batch_source_record_ids
from platform_contracts import (
    partition_clause_sql,
    column_names,
    create_table_columns_sql,
    current_row_predicate,
    declared_geometry_srid,
    load_lakehouse_contract,
    partition_spec_sql,
    required_column_names,
    required_string_column_names,
)


DEFAULT_ICEBERG_PACKAGES = iceberg_packages()
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
JOB_NAME = "industrial_complex_boundaries_handoff_to_silver"
RUN_SUMMARY_SCHEMA_VERSION = "foundation-platform.spark_run_summary.v1"
RUN_SUMMARY_CONTRACT = "silver.industrial_complex_boundaries"
COMPLEXES_CONTRACT_NAME = "silver.industrial_complexes"
RUN_SUMMARY_INPUT_KIND = "silver_handoff_jsonl"

TABLE_CONTRACT = load_lakehouse_contract(RUN_SUMMARY_CONTRACT)
COMPLEXES_CONTRACT = load_lakehouse_contract(COMPLEXES_CONTRACT_NAME)

SILVER_COLUMNS: tuple[str, ...] = column_names(TABLE_CONTRACT)
REQUIRED_SILVER_COLUMNS: tuple[str, ...] = required_column_names(TABLE_CONTRACT)
REQUIRED_STRING_COLUMNS: tuple[str, ...] = required_string_column_names(TABLE_CONTRACT)
GEOMETRY_SRID: int = declared_geometry_srid(TABLE_CONTRACT)

# The join key and the geometry transport. `complex_id` and `sido_code` are in the handoff as JSON
# nulls so the file still has the contract's shape; this job is what fills them.
HANDOFF_TRANSPORT_COLUMNS: tuple[str, ...] = (
    "official_complex_code",
    "geometry_wkb_hex",
    "geometry_wkb_encoding",
)
HANDOFF_INPUT_COLUMNS: tuple[str, ...] = (*SILVER_COLUMNS, *HANDOFF_TRANSPORT_COLUMNS)

# The columns this job reads off `silver.industrial_complexes`, and nothing else: a boundary row
# takes identity and province from its complex, not the complex's name, status, or area.
COMPLEX_JOIN_COLUMNS: tuple[str, ...] = ("official_complex_code", "complex_id", "sido_code")

# The columns the handoff leaves for this job to fill. They arrive as JSON nulls so the handoff
# still has the contract's shape, and they are dropped before the join so the two sides cannot both
# offer a column of the same name.
JOINED_COLUMNS: tuple[str, ...] = ("complex_id", "sido_code")

# This job loads the authority's own published boundary, which is the `official` value of
# `boundary_kind` (`docs/catalog/industrial-complex-lakehouse-poc.md` §4.2). A row arriving with any
# other value came from somewhere this job does not read.
OFFICIAL_BOUNDARY_KIND = "official"

BOUNDARY_SPECIFIC_QUALITY_METRICS: tuple[str, ...] = (
    "invalid_boundary_kind_count",
    "invalid_geometry_srid_count",
    "invalid_geometry_encoding_count",
    "invalid_geometry_wkb_hex_count",
    "invalid_geometry_wkb_count",
    "invalid_bbox_count",
    "centroid_outside_bbox_count",
    "invalid_checksum_count",
    "non_positive_area_count",
    "duplicate_active_complex_count",
)

# Counts the join produces. They describe the source, not a defect, so they are recorded even when
# they are zero rather than appearing only when something goes wrong.
JOIN_COUNT_METRICS: tuple[str, ...] = (
    "orphan_boundary_count",
    "complex_without_boundary_count",
    "complex_without_sido_code_count",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build silver.industrial_complex_boundaries from the boundary handoff JSONL, joined to "
            "silver.industrial_complexes for complex_id and sido_code."
        )
    )
    parser.add_argument("--input", required=True, help="Silver handoff JSONL input path.")
    parser.add_argument("--output", help="Silver Parquet output path.")
    parser.add_argument(
        "--complexes-mode",
        choices=("iceberg", "jsonl"),
        default="iceberg",
        help="Where to read silver.industrial_complexes from.",
    )
    parser.add_argument(
        "--complexes-input",
        help="Path to silver.industrial_complexes rows when --complexes-mode=jsonl.",
    )
    parser.add_argument(
        "--complexes-iceberg-namespace",
        default="silver",
        help="Iceberg namespace holding silver.industrial_complexes.",
    )
    parser.add_argument(
        "--complexes-iceberg-table",
        default="industrial_complexes",
        help="Iceberg table name for silver.industrial_complexes.",
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
        "--iceberg-namespace",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_NAMESPACE", "silver"),
        help="Iceberg namespace for the target Silver table.",
    )
    parser.add_argument(
        "--iceberg-table",
        default=os.getenv(
            "FOUNDATION_PLATFORM_SPARK_ICEBERG_TABLE", "industrial_complex_boundaries"
        ),
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
        help="Comma-separated Iceberg Spark packages used for REST catalog access.",
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
    return parser.parse_args(argv)


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


def needs_iceberg_catalog(args: argparse.Namespace) -> bool:
    """Whether this run has to reach the Iceberg REST catalog at all.

    Reading the complexes from Iceberg needs it just as much as writing does — the join is not
    optional, so a run that can write but cannot read its complexes is not a run.
    """

    return args.write_mode == "iceberg" or args.complexes_mode == "iceberg"


def validate_args(args: argparse.Namespace) -> None:
    if args.summary_output is not None and args.summary_output.strip() == "":
        raise ValueError("--summary-output must not be empty")

    if args.write_mode == "parquet" and not args.output:
        raise ValueError("--output is required when --write-mode=parquet")

    if args.complexes_mode == "jsonl" and not args.complexes_input:
        raise ValueError("--complexes-input is required when --complexes-mode=jsonl")

    if args.complexes_mode == "iceberg":
        validate_identifier("complexes iceberg namespace", args.complexes_iceberg_namespace)
        validate_identifier("complexes iceberg table", args.complexes_iceberg_table)

    if args.write_mode == "iceberg":
        validate_identifier("iceberg namespace", args.iceberg_namespace)
        validate_identifier("iceberg table", args.iceberg_table)

        if args.iceberg_write_mode == "overwrite":
            is_smoke_table = args.iceberg_table.endswith("_smoke")
            if not is_smoke_table and not args.allow_non_smoke_overwrite:
                raise ValueError(
                    "Refusing to overwrite a non-smoke Iceberg table without "
                    "--allow-non-smoke-overwrite"
                )

    if needs_iceberg_catalog(args):
        validate_identifier("iceberg catalog name", args.iceberg_catalog_name)
        for name in required_iceberg_env():
            require_env(name)


def load_pyspark() -> tuple[Any, Any, Any, Any]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.storagelevel import StorageLevel

    return SparkSession, F, T, StorageLevel


def read_handoff_jsonl(spark: Any, input_path: str) -> Any:
    handoff = spark.read.json(input_path)
    missing_columns = sorted(set(HANDOFF_INPUT_COLUMNS) - set(handoff.columns))
    if missing_columns:
        raise ValueError(
            f"Industrial-complex boundary handoff is missing columns: {', '.join(missing_columns)}"
        )
    return handoff.select(*HANDOFF_INPUT_COLUMNS)


def qualified_complexes_table(args: argparse.Namespace) -> str:
    return (
        f"`{args.iceberg_catalog_name}`."
        f"`{args.complexes_iceberg_namespace}`."
        f"`{args.complexes_iceberg_table}`"
    )


def read_complexes(spark: Any, args: argparse.Namespace, F: Any) -> Any:
    """Read the complex identity this job joins on, and nothing beyond it."""

    if args.complexes_mode == "jsonl":
        frame = spark.read.json(args.complexes_input)
    else:
        frame = spark.table(qualified_complexes_table(args))

    missing_columns = sorted(set(COMPLEX_JOIN_COLUMNS) - set(frame.columns))
    if missing_columns:
        raise ValueError(
            f"{COMPLEXES_CONTRACT_NAME} is missing columns: {', '.join(missing_columns)}"
        )

    predicate = current_row_predicate(COMPLEXES_CONTRACT)
    if predicate is not None:
        frame = frame.where(F.expr(predicate))
    return frame.select(
        F.trim(F.col("official_complex_code")).alias("official_complex_code"),
        F.trim(F.col("complex_id")).alias("complex_id"),
        F.trim(F.col("sido_code")).alias("sido_code"),
    )


def assert_one_row_per_complex(complexes: Any, F: Any) -> None:
    """Refuse an ambiguous complex list rather than picking a row.

    `silver.industrial_complexes` declares no `current_row_predicate`, so this job has no
    contract-stated way to choose between two rows for one code. Inventing one — "the latest
    `valid_from_utc`", say — would put a versioning rule in a boundary loader, where nobody would
    look for it. When the table starts carrying history, the predicate belongs in the contract and
    this check is what will say so.
    """

    duplicates = (
        complexes.groupBy("official_complex_code").count().where(F.col("count") > 1)
    )
    duplicate_count = int(duplicates.count())
    if duplicate_count == 0:
        return

    samples = [str(sample) for sample in duplicates.limit(5).toJSON().collect()]
    predicate = current_row_predicate(COMPLEXES_CONTRACT)
    raise ValueError(
        f"{COMPLEXES_CONTRACT_NAME} holds {duplicate_count} official_complex_code values with more "
        f"than one row (current_row_predicate={predicate!r}), so this job cannot say which row a "
        f"boundary belongs to. samples={samples}"
    )


def collect_join_counts(handoff: Any, complexes: Any, F: Any) -> dict[str, int]:
    """Count what the join drops on each side, before anything is dropped."""

    boundary_codes = handoff.select("official_complex_code").distinct()
    complex_codes = complexes.select("official_complex_code")
    return {
        "orphan_boundary_count": int(
            boundary_codes.join(complex_codes, "official_complex_code", "left_anti").count()
        ),
        "complex_without_boundary_count": int(
            complex_codes.join(boundary_codes, "official_complex_code", "left_anti").count()
        ),
        "complex_without_sido_code_count": int(
            complexes.join(boundary_codes, "official_complex_code", "left_semi")
            .where(F.col("sido_code").isNull() | (F.length(F.col("sido_code")) == 0))
            .count()
        ),
    }


def assert_handoff_leaves_joined_columns_null(handoff: Any, F: Any) -> None:
    """Refuse a handoff that already filled the columns this job is here to resolve.

    A producer that arrived at a `complex_id` on its own derived it a second time, and the two
    derivations agreeing today is not the same as their agreeing after one of them changes. The
    handoff's job is to say `official_complex_code`; the id comes off the complex table.
    """

    filled = handoff.where(
        F.col("complex_id").isNotNull() | F.col("sido_code").isNotNull()
    )
    filled_count = int(filled.count())
    if filled_count == 0:
        return

    samples = [
        str(sample)
        for sample in filled.drop(*SAMPLE_SUPPRESSED_COLUMNS).limit(5).toJSON().collect()
    ]
    raise ValueError(
        f"{filled_count} handoff rows already carry complex_id or sido_code; this job resolves "
        f"both from {COMPLEXES_CONTRACT_NAME} and must not accept a second derivation. "
        f"samples={samples}"
    )


def join_handoff_to_complexes(handoff: Any, complexes: Any, F: Any) -> Any:
    """Attach identity and province to each boundary, keeping only rows that have both.

    A boundary whose complex states no province cannot be written: `sido_code` is required and is
    the first partition column. It is dropped here and counted in `complex_without_sido_code_count`,
    which is the difference between a row this job refused and a row nobody noticed was missing.
    """

    joined = handoff.drop(*JOINED_COLUMNS).join(complexes, "official_complex_code", "inner")
    return joined.where(F.col("sido_code").isNotNull() & (F.length(F.col("sido_code")) > 0))


def build_candidate_frame(joined: Any, F: Any, T: Any) -> Any:
    geometry_wkb_hex = F.lower(F.trim(F.col("geometry_wkb_hex")))
    geometry_wkb_encoding = F.lower(F.trim(F.col("geometry_wkb_encoding")))

    return joined.select(
        F.trim(F.col("boundary_id")).alias("boundary_id"),
        F.col("complex_id").alias("complex_id"),
        F.col("sido_code").alias("sido_code"),
        F.trim(F.col("boundary_kind")).alias("boundary_kind"),
        F.unhex(geometry_wkb_hex).alias("geometry_wkb"),
        F.col("geometry_srid").cast(T.IntegerType()).alias("geometry_srid"),
        F.col("bbox_min_x").cast(T.DoubleType()).alias("bbox_min_x"),
        F.col("bbox_min_y").cast(T.DoubleType()).alias("bbox_min_y"),
        F.col("bbox_max_x").cast(T.DoubleType()).alias("bbox_max_x"),
        F.col("bbox_max_y").cast(T.DoubleType()).alias("bbox_max_y"),
        F.col("centroid_x").cast(T.DoubleType()).alias("centroid_x"),
        F.col("centroid_y").cast(T.DoubleType()).alias("centroid_y"),
        F.col("area_sqm_calculated").cast(T.DecimalType(18, 2)).alias("area_sqm_calculated"),
        F.lower(F.trim(F.col("geometry_checksum_sha256"))).alias("geometry_checksum_sha256"),
        F.trim(F.col("source_record_id")).alias("source_record_id"),
        F.trim(F.col("source_snapshot_id")).alias("source_snapshot_id"),
        F.to_timestamp(F.col("valid_from_utc"), "yyyy-MM-dd'T'HH:mm:ssX").alias("valid_from_utc"),
        F.to_timestamp(F.col("valid_to_utc"), "yyyy-MM-dd'T'HH:mm:ssX").alias("valid_to_utc"),
        F.to_timestamp(F.col("ingested_at_utc"), "yyyy-MM-dd'T'HH:mm:ssX").alias("ingested_at_utc"),
        geometry_wkb_hex.alias("_geometry_wkb_hex"),
        geometry_wkb_encoding.alias("_geometry_wkb_encoding"),
    )


def assert_columns(frame: Any, expected_columns: tuple[str, ...]) -> None:
    actual_columns = tuple(frame.select(*expected_columns).columns)
    if actual_columns != tuple(expected_columns):
        raise ValueError(
            "Unexpected Silver columns. "
            f"expected={list(expected_columns)} actual={list(actual_columns)}"
        )


# Columns a failure sample must not carry. One boundary's WKB is tens of kilobytes of hex, and five
# of them bury the field that says which complex failed.
SAMPLE_SUPPRESSED_COLUMNS: tuple[str, ...] = (
    "geometry_wkb",
    "geometry_wkb_hex",
    "_geometry_wkb_hex",
)


def sample_invalid_rows(frame: Any, predicate: Any) -> list[str]:
    readable = frame.drop(*SAMPLE_SUPPRESSED_COLUMNS)
    return [str(sample) for sample in readable.where(predicate).limit(5).toJSON().collect()]


def assert_no_invalid_rows(frame: Any, metric_count: int, predicate: Any, message: str) -> None:
    if metric_count == 0:
        return

    samples = sample_invalid_rows(frame, predicate)
    raise ValueError(f"{message}. count={metric_count} samples={samples}")


def invalid_count(predicate: Any, alias: str, F: Any) -> Any:
    return F.sum(F.when(predicate, F.lit(1)).otherwise(F.lit(0))).cast("long").alias(alias)


def is_invalid_double(column_name: str, F: Any) -> Any:
    column = F.col(column_name)
    return column.isNull() | F.isnan(column)


def geometry_wkb_hex_is_invalid(F: Any) -> Any:
    return (
        F.col("_geometry_wkb_hex").isNull()
        | (F.length(F.col("_geometry_wkb_hex")) == 0)
        | ((F.length(F.col("_geometry_wkb_hex")) % 2) != 0)
        | ~F.col("_geometry_wkb_hex").rlike(r"^[0-9a-f]+$")
    )


def geometry_wkb_is_invalid(F: Any) -> Any:
    geometry_hex = F.lower(F.hex(F.col("geometry_wkb")))
    return (
        F.col("geometry_wkb").isNull()
        | (F.length(F.col("geometry_wkb")) <= 9)
        | ~geometry_hex.rlike(r"^(0103000000|0106000000)")
    )


def bbox_is_invalid(F: Any) -> Any:
    return (
        is_invalid_double("bbox_min_x", F)
        | is_invalid_double("bbox_min_y", F)
        | is_invalid_double("bbox_max_x", F)
        | is_invalid_double("bbox_max_y", F)
        | (F.col("bbox_min_x") > F.col("bbox_max_x"))
        | (F.col("bbox_min_y") > F.col("bbox_max_y"))
    )


def centroid_is_outside_bbox(F: Any) -> Any:
    return (
        is_invalid_double("centroid_x", F)
        | is_invalid_double("centroid_y", F)
        | (F.col("centroid_x") < F.col("bbox_min_x"))
        | (F.col("centroid_x") > F.col("bbox_max_x"))
        | (F.col("centroid_y") < F.col("bbox_min_y"))
        | (F.col("centroid_y") > F.col("bbox_max_y"))
    )


def checksum_is_invalid(F: Any) -> Any:
    return ~F.col("geometry_checksum_sha256").rlike(r"^[0-9a-f]{64}$") | (
        F.col("geometry_checksum_sha256") != F.sha2(F.col("geometry_wkb"), 256)
    )


def area_is_non_positive(F: Any) -> Any:
    """An area column that is present must state an area.

    The column is optional, so null is allowed — it says the source drew no boundary this job could
    measure. Zero or negative is a different claim, and a false one.
    """

    return F.col("area_sqm_calculated").isNotNull() & (F.col("area_sqm_calculated") <= 0)


def collect_duplicate_active_complex_count(frame: Any, F: Any) -> int:
    """How many complexes this batch would give more than one official boundary.

    The contract allows at most one. Counted within the batch, which is what this job controls;
    append mode leaves earlier snapshots alone by design.
    """

    duplicate_rows = (
        frame.where(F.col("boundary_kind") == F.lit(OFFICIAL_BOUNDARY_KIND))
        .groupBy("complex_id")
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    return int(duplicate_rows)


def collect_quality_metrics(frame: Any, include_transport: bool, F: Any) -> dict[str, int]:
    expressions: list[Any] = [F.count(F.lit(1)).cast("long").alias("row_count")]

    for column in REQUIRED_SILVER_COLUMNS:
        expressions.append(invalid_count(F.col(column).isNull(), f"{column}__null_count", F))

    for column in REQUIRED_STRING_COLUMNS:
        expressions.append(invalid_count(F.length(F.col(column)) == 0, f"{column}__empty_count", F))

    if include_transport:
        invalid_encoding = F.col("_geometry_wkb_encoding") != F.lit("hex")
        invalid_hex = geometry_wkb_hex_is_invalid(F)
    else:
        invalid_encoding = F.lit(False)
        invalid_hex = F.lit(False)

    expressions.extend(
        (
            invalid_count(
                F.col("boundary_kind") != F.lit(OFFICIAL_BOUNDARY_KIND),
                "invalid_boundary_kind_count",
                F,
            ),
            invalid_count(
                F.col("geometry_srid") != GEOMETRY_SRID, "invalid_geometry_srid_count", F
            ),
            invalid_count(invalid_encoding, "invalid_geometry_encoding_count", F),
            invalid_count(invalid_hex, "invalid_geometry_wkb_hex_count", F),
            invalid_count(geometry_wkb_is_invalid(F), "invalid_geometry_wkb_count", F),
            invalid_count(bbox_is_invalid(F), "invalid_bbox_count", F),
            invalid_count(centroid_is_outside_bbox(F), "centroid_outside_bbox_count", F),
            invalid_count(checksum_is_invalid(F), "invalid_checksum_count", F),
            invalid_count(area_is_non_positive(F), "non_positive_area_count", F),
        )
    )

    row = frame.agg(*expressions).first()
    if row is None:
        raise ValueError("Silver quality metric aggregation returned no row")
    metrics = {key: int(value or 0) for key, value in row.asDict().items()}
    metrics["duplicate_active_complex_count"] = collect_duplicate_active_complex_count(frame, F)
    for metric in BOUNDARY_SPECIFIC_QUALITY_METRICS:
        metrics.setdefault(metric, 0)
    return metrics


def assert_no_duplicate_active_complex(frame: Any, metric_count: int, F: Any) -> None:
    if metric_count == 0:
        return

    samples = (
        frame.where(F.col("boundary_kind") == F.lit(OFFICIAL_BOUNDARY_KIND))
        .groupBy("complex_id")
        .count()
        .where(F.col("count") > 1)
        .limit(5)
        .toJSON()
        .collect()
    )
    raise ValueError(
        "at most one active official boundary is allowed per complex_id. "
        f"count={metric_count} samples={samples}"
    )


def assert_quality_metrics(frame: Any, metrics: dict[str, int], include_transport: bool, F: Any) -> None:
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
        metrics["invalid_boundary_kind_count"],
        F.col("boundary_kind") != F.lit(OFFICIAL_BOUNDARY_KIND),
        f"this job loads the authority's published boundary, so boundary_kind must be "
        f"{OFFICIAL_BOUNDARY_KIND}",
    )
    assert_no_invalid_rows(
        frame,
        metrics["invalid_geometry_srid_count"],
        F.col("geometry_srid") != GEOMETRY_SRID,
        f"geometry_srid must be {GEOMETRY_SRID}",
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
            geometry_wkb_hex_is_invalid(F),
            "geometry_wkb_hex must be non-empty lowercase even-length hex",
        )
    assert_no_invalid_rows(
        frame,
        metrics["invalid_geometry_wkb_count"],
        geometry_wkb_is_invalid(F),
        "geometry_wkb must be non-empty little-endian Polygon or MultiPolygon WKB",
    )
    assert_no_invalid_rows(
        frame,
        metrics["invalid_bbox_count"],
        bbox_is_invalid(F),
        "bbox min/max ordering must be valid",
    )
    assert_no_invalid_rows(
        frame,
        metrics["centroid_outside_bbox_count"],
        centroid_is_outside_bbox(F),
        "centroid must sit inside its bbox",
    )
    assert_no_invalid_rows(
        frame,
        metrics["invalid_checksum_count"],
        checksum_is_invalid(F),
        "geometry_checksum_sha256 must match geometry_wkb",
    )
    assert_no_invalid_rows(
        frame,
        metrics["non_positive_area_count"],
        area_is_non_positive(F),
        "area_sqm_calculated must be positive when present",
    )
    assert_no_duplicate_active_complex(frame, metrics["duplicate_active_complex_count"], F)


def validate_boundary_frame(
    frame: Any,
    expected_count: int | None,
    include_transport: bool,
    F: Any,
) -> tuple[int, dict[str, int]]:
    assert_columns(frame, SILVER_COLUMNS)

    metrics = collect_quality_metrics(frame, include_transport, F)
    assert_quality_metrics(frame, metrics, include_transport, F)

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


def merge_join_counts(metrics: dict[str, int], join_counts: dict[str, int]) -> dict[str, int]:
    """Carry the join counts into whichever metric set the summary reports.

    They are properties of the run, not of the persisted frame, so re-measuring them off the
    written rows would report zero and read as "nothing was dropped".
    """

    merged = dict(metrics)
    for metric in JOIN_COUNT_METRICS:
        merged[metric] = int(join_counts.get(metric, 0))
    return merged


def collect_source_snapshot_summary(frame: Any) -> dict[str, Any]:
    snapshots = frame.select("source_snapshot_id").distinct()
    snapshot_count = int(snapshots.count())
    snapshot_ids = [
        row.source_snapshot_id for row in snapshots.orderBy("source_snapshot_id").collect()
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
        return {"kind": "parquet", "path": args.output}

    return {
        "kind": "iceberg",
        "catalog": args.iceberg_catalog_name,
        "namespace": args.iceberg_namespace,
        "table": args.iceberg_table,
        "qualified_table": unquoted_qualified_iceberg_table(args),
    }


def run_summary_complexes_source(args: argparse.Namespace) -> dict[str, str]:
    if args.complexes_mode == "jsonl":
        return {"kind": "jsonl", "contract": COMPLEXES_CONTRACT_NAME, "path": args.complexes_input}

    return {
        "kind": "iceberg",
        "contract": COMPLEXES_CONTRACT_NAME,
        "qualified_table": (
            f"{args.iceberg_catalog_name}."
            f"{args.complexes_iceberg_namespace}."
            f"{args.complexes_iceberg_table}"
        ),
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
        "input": {"kind": RUN_SUMMARY_INPUT_KIND, "path": args.input},
        "complexes_source": run_summary_complexes_source(args),
        "target": run_summary_target(args),
        "write_mode": args.write_mode,
        "write_disposition": run_summary_disposition(args),
        "geometry_srid": GEOMETRY_SRID,
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

    print(f"silver-industrial-complex-boundaries-summary-json {payload}")


def write_silver_parquet(silver: Any, output_path: str) -> None:
    (
        silver.repartition("sido_code")
        .sortWithinPartitions("complex_id", "boundary_kind", "valid_from_utc")
        .write.mode("overwrite")
        .partitionBy("sido_code")
        .parquet(output_path)
    )


def qualified_iceberg_table(args: argparse.Namespace) -> str:
    return (
        f"`{args.iceberg_catalog_name}`.`{args.iceberg_namespace}`.`{args.iceberg_table}`"
    )


def create_iceberg_table_if_missing(spark: Any, args: argparse.Namespace) -> None:
    namespace = f"`{args.iceberg_catalog_name}`.`{args.iceberg_namespace}`"
    table = qualified_iceberg_table(args)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
{create_table_columns_sql(TABLE_CONTRACT)}
        )
        USING iceberg
        {partition_clause_sql(TABLE_CONTRACT)}
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'zstd',
            'write.distribution-mode' = 'hash'
        )
        """
    )


def write_silver_iceberg(spark: Any, silver: Any, args: argparse.Namespace) -> dict[str, Any]:
    """Appends this batch once, whatever a re-run does.

    Was a SQL `INSERT`, which cannot carry writer options, so nothing recorded what the
    commit appended and a second run appended it again. `silver.parcel_boundaries` is what
    that costs when the table is large enough to notice: 1,865,891 rows, three times over
    (root ADR-0062).
    """
    create_iceberg_table_if_missing(spark, args)
    return append_batch_once(
        spark,
        silver,
        SILVER_COLUMNS,
        qualified_iceberg_table(args),
        RUN_SUMMARY_CONTRACT,
        write_mode=args.iceberg_write_mode,
    )


def build_spark_session(args: argparse.Namespace, SparkSession: Any) -> Any:
    builder = (
        SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
    )

    if needs_iceberg_catalog(args):
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
    if needs_iceberg_catalog(args):
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


def read_iceberg_snapshot_for_batch(spark: Any, silver: Any, args: argparse.Namespace, F: Any) -> Any:
    """Reads back only the rows this run appended.

    Filtered on the source object, not on `source_snapshot_id`. The snapshot id names the
    provider's extract, so every object of one national extract carries the same value and a
    second batch would read the first batch's rows back as its own — then fail the count
    check that follows, reporting a write problem where there is none.
    """
    record_ids = batch_source_record_ids(silver)
    return (
        spark.table(qualified_iceberg_table(args))
        .where(F.col("source_record_id").isin(record_ids))
        .select(*SILVER_COLUMNS)
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_args(args)
    SparkSession, F, T, StorageLevel = load_pyspark()
    spark = build_spark_session(args, SparkSession)

    try:
        handoff = read_handoff_jsonl(spark, args.input).persist(StorageLevel.MEMORY_AND_DISK)
        complexes = read_complexes(spark, args, F).persist(StorageLevel.MEMORY_AND_DISK)
        assert_handoff_leaves_joined_columns_null(handoff, F)
        assert_one_row_per_complex(complexes, F)
        join_counts = collect_join_counts(handoff, complexes, F)
        joined = join_handoff_to_complexes(handoff, complexes, F)
        candidate = build_candidate_frame(joined, F, T).persist(StorageLevel.MEMORY_AND_DISK)
        silver = candidate.select(*SILVER_COLUMNS).persist(StorageLevel.MEMORY_AND_DISK)

        row_count, candidate_quality_metrics = validate_boundary_frame(
            candidate, args.expected_count, include_transport=True, F=F
        )
        source_snapshot_summary = collect_source_snapshot_summary(silver)

        if args.validate_only:
            emit_run_summary(
                build_run_summary(
                    args,
                    row_count=row_count,
                    persisted_row_count=None,
                    quality_metrics=merge_join_counts(candidate_quality_metrics, join_counts),
                    source_snapshot_summary=source_snapshot_summary,
                ),
                args.summary_output,
            )
            print(f"silver-industrial-complex-boundaries-validate-ok rows={row_count}")
            return 0

        if args.write_mode == "parquet":
            write_silver_parquet(silver, args.output)
            persisted = spark.read.parquet(args.output).select(*SILVER_COLUMNS)
            success_target = f"output={args.output}"
            success_label = "silver-industrial-complex-boundaries-write-ok"
        else:
            outcome = write_silver_iceberg(spark, silver, args)
            if not outcome["appended"]:
                # Count what the table holds rather than trusting the summary alone, so a
                # skip reports the rows it is standing on instead of asserting them.
                already = read_iceberg_snapshot_for_batch(spark, silver, args, F).count()
                print(
                    "silver-industrial-complex-boundaries-iceberg-already-ingested "
                    f"rows={already} token={outcome['token']} "
                    f"snapshot={outcome['existing_snapshot']} "
                    f"objects={len(outcome['record_ids'])}"
                )
                return 0
            persisted = read_iceberg_snapshot_for_batch(spark, silver, args, F)
            success_target = f"table={args.iceberg_namespace}.{args.iceberg_table}"
            success_label = "silver-industrial-complex-boundaries-iceberg-write-ok"

        persisted_count, persisted_quality_metrics = validate_boundary_frame(
            persisted, args.expected_count, include_transport=False, F=F
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
                quality_metrics=merge_join_counts(
                    merge_transport_metrics(persisted_quality_metrics, candidate_quality_metrics),
                    join_counts,
                ),
                source_snapshot_summary=source_snapshot_summary,
            ),
            args.summary_output,
        )
        print(f"{success_label} rows={persisted_count} {success_target}")
        return 0
    finally:
        for name in ("silver", "candidate", "complexes", "handoff"):
            frame = locals().get(name)
            if frame is not None:
                frame.unpersist()
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
