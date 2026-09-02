#!/usr/bin/env python3
"""Write VWorld cadastral parcel-boundary handoff rows into the Silver table shape.

Rust foundation-platform owns the VWorld normalization contract and emits a writer-neutral
JSONL handoff. This Spark job owns the storage-engine step: decode transport-only
fields, verify Silver quality gates, and write Parquet or Iceberg rows whose columns
match `catalog_domain::SILVER_PARCEL_BOUNDARIES`.

**What is here is what makes a parcel a parcel.** The PNU and the three administrative codes
derived from it, and the rule that one PNU has one active boundary. Everything a geometry handoff
does regardless of source — the transport decoding, the geometry gates, the target, the run
summary — lives in `spatial_silver_handoff` and is called from there. It used to be copied into
this file and into the industrial-complex job, and the two copies had drifted: thirty functions
shared a name and five shared their text.
"""

from __future__ import annotations

import argparse
from typing import Any

import spatial_silver_handoff as shared
from lakehouse_object_store import is_object_store_path
from platform_contracts import (
    column_names,
    current_row_predicate,
    declared_geometry_srid,
    load_lakehouse_contract,
    required_column_names,
    required_string_column_names,
)

JOB_NAME = "vworld_parcel_boundaries_handoff_to_silver"
RUN_SUMMARY_CONTRACT = "silver.parcel_boundaries"
RUN_SUMMARY_INPUT_KIND = "silver_handoff_jsonl"
LABELS = shared.HandoffLabels("silver-parcel-boundaries")

TABLE_CONTRACT = load_lakehouse_contract(RUN_SUMMARY_CONTRACT)
CURRENT_ROW_PREDICATE = current_row_predicate(TABLE_CONTRACT)
if CURRENT_ROW_PREDICATE is None:
    raise ValueError(f"{RUN_SUMMARY_CONTRACT} must define a current-row predicate")

SILVER_COLUMNS: tuple[str, ...] = column_names(TABLE_CONTRACT)
REQUIRED_SILVER_COLUMNS: tuple[str, ...] = required_column_names(TABLE_CONTRACT)
REQUIRED_STRING_COLUMNS: tuple[str, ...] = required_string_column_names(TABLE_CONTRACT)

# Read off the contract rather than written here. Silver geometry tables carry the CRS their source
# published and declare it per table (root ADR-0042), so a job that spelled the number out would be
# the second place the answer lives.
GEOMETRY_SRID: int = declared_geometry_srid(TABLE_CONTRACT)

HANDOFF_INPUT_COLUMNS: tuple[str, ...] = (*SILVER_COLUMNS, *shared.TRANSPORT_COLUMNS)

# Reported whether or not they fired. A metric that appears only when something went wrong cannot
# be told apart from a metric nobody collected.
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


def load_pyspark() -> None:
    """Bind the PySpark namespaces this job uses.

    Deferred, and bound through the shared module so the shared checks and this file are looking at
    the same `F`. A module-level import would make the plain-Python checks in
    `infra/lakehouse/spark/tests` skip themselves on the CI lane, which has no PySpark install —
    and a check that skips reports the same green as a check that passes.
    """

    global DataFrame, SparkSession, F, T, StorageLevel

    DataFrame, SparkSession, F, T, StorageLevel = shared.bind_pyspark()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build silver.parcel_boundaries from VWorld handoff JSONL input."
    )
    parser.add_argument(
        "--input-format",
        choices=("jsonl", "parquet"),
        default="jsonl",
        help="Physical format of the Silver handoff input.",
    )
    shared.add_common_arguments(parser, default_iceberg_table="parcel_boundaries")
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    """This job reaches the catalog only to write, so that is when it demands one."""

    shared.validate_common_args(args, needs_catalog=args.write_mode == "iceberg")


def read_handoff(spark: Any, input_path: str, input_format: str) -> Any:
    return shared.read_handoff(
        spark,
        input_path,
        input_format,
        HANDOFF_INPUT_COLUMNS,
        label="Parcel-boundary handoff",
    )


def build_candidate_frame(handoff: Any) -> Any:
    geometry_wkb_hex = F.lower(F.trim(F.col("geometry_wkb_hex")))
    geometry_wkb_encoding = F.lower(F.trim(F.col("geometry_wkb_encoding")))

    return handoff.select(
        F.trim(F.col("boundary_id")).alias("boundary_id"),
        F.trim(F.col("pnu")).alias("pnu"),
        F.trim(F.col("sido_code")).alias("sido_code"),
        F.trim(F.col("sigungu_code")).alias("sigungu_code"),
        F.trim(F.col("bjdong_code")).alias("bjdong_code"),
        shared.trim_to_null("jibun").alias("jibun"),
        shared.trim_to_null("bonbun").alias("bonbun"),
        shared.trim_to_null("bubun").alias("bubun"),
        F.unhex(geometry_wkb_hex).alias("geometry_wkb"),
        F.col("geometry_srid").cast(T.IntegerType()).alias("geometry_srid"),
        F.col("bbox_min_x").cast(T.DoubleType()).alias("bbox_min_x"),
        F.col("bbox_min_y").cast(T.DoubleType()).alias("bbox_min_y"),
        F.col("bbox_max_x").cast(T.DoubleType()).alias("bbox_max_x"),
        F.col("bbox_max_y").cast(T.DoubleType()).alias("bbox_max_y"),
        F.lower(F.trim(F.col("geometry_checksum_sha256"))).alias("geometry_checksum_sha256"),
        F.trim(F.col("source_record_id")).alias("source_record_id"),
        F.trim(F.col("source_snapshot_id")).alias("source_snapshot_id"),
        F.to_timestamp(F.col("valid_from_utc"), "yyyy-MM-dd'T'HH:mm:ssX").alias("valid_from_utc"),
        F.to_timestamp(F.col("valid_to_utc"), "yyyy-MM-dd'T'HH:mm:ssX").alias("valid_to_utc"),
        F.to_timestamp(F.col("ingested_at_utc"), "yyyy-MM-dd'T'HH:mm:ssX").alias(
            "ingested_at_utc"
        ),
        geometry_wkb_hex.alias("_geometry_wkb_hex"),
        geometry_wkb_encoding.alias("_geometry_wkb_encoding"),
    )


# ---------------------------------------------------------------------------
# What makes a parcel a parcel
# ---------------------------------------------------------------------------


def pnu_is_invalid() -> Any:
    return ~F.col("pnu").rlike(r"^[0-9]{19}$")


def code_derivation_is_invalid() -> Any:
    """The three administrative codes are prefixes of the PNU, not a second lookup.

    A row whose codes disagree with its own PNU states two different places for one parcel.
    """

    return (
        (F.col("sido_code") != F.substring(F.col("pnu"), 1, 2))
        | (F.col("sigungu_code") != F.substring(F.col("pnu"), 1, 5))
        | (F.col("bjdong_code") != F.substring(F.col("pnu"), 1, 10))
    )


def collect_duplicate_active_pnu_count(frame: Any) -> int:
    duplicate_rows = (
        frame.where(F.expr(CURRENT_ROW_PREDICATE))
        .groupBy("pnu")
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    return int(duplicate_rows)


def assert_no_duplicate_active_pnu(frame: Any, metric_count: int) -> None:
    if metric_count == 0:
        return

    samples = (
        frame.where(F.expr(CURRENT_ROW_PREDICATE))
        .groupBy("pnu")
        .count()
        .where(F.col("count") > 1)
        .limit(5)
        .toJSON()
        .collect()
    )
    raise ValueError(
        f"active parcel boundaries must be unique by pnu. count={metric_count} samples={samples}"
    )


def collect_quality_metrics(frame: Any, include_transport: bool) -> dict[str, int]:
    expressions = shared.required_column_expressions(
        REQUIRED_SILVER_COLUMNS, REQUIRED_STRING_COLUMNS
    )
    expressions.extend(shared.geometry_metric_expressions(include_transport, GEOMETRY_SRID))
    expressions.extend(
        (
            shared.invalid_count(pnu_is_invalid(), "invalid_pnu_count"),
            shared.invalid_count(code_derivation_is_invalid(), "invalid_code_derivation_count"),
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


def assert_quality_metrics(frame: Any, metrics: dict[str, int], include_transport: bool) -> None:
    shared.assert_required_columns(
        frame, metrics, REQUIRED_SILVER_COLUMNS, REQUIRED_STRING_COLUMNS
    )
    shared.assert_no_invalid_rows(
        frame,
        metrics["invalid_pnu_count"],
        pnu_is_invalid(),
        "pnu must be a 19-digit parcel number",
    )
    shared.assert_no_invalid_rows(
        frame,
        metrics["invalid_code_derivation_count"],
        code_derivation_is_invalid(),
        "sido_code, sigungu_code, and bjdong_code must be derived from pnu",
    )
    shared.assert_geometry(frame, metrics, include_transport, GEOMETRY_SRID)
    assert_no_duplicate_active_pnu(frame, metrics["duplicate_active_pnu_count"])


def validate_parcel_frame(
    frame: Any,
    expected_count: int | None,
    include_transport: bool,
) -> tuple[int, dict[str, int]]:
    shared.assert_columns(frame, SILVER_COLUMNS)

    metrics = collect_quality_metrics(frame, include_transport)
    assert_quality_metrics(frame, metrics, include_transport)

    actual_count = metrics["row_count"]
    if expected_count is not None and actual_count != expected_count:
        raise ValueError(f"Expected {expected_count} Silver rows, found {actual_count}")

    return actual_count, metrics


# ---------------------------------------------------------------------------
# Target and summary
# ---------------------------------------------------------------------------


def write_silver_parquet(silver: Any, output_path: str) -> None:
    (
        silver.repartition("sigungu_code")
        .sortWithinPartitions("pnu", "valid_from_utc")
        .write.mode("overwrite")
        .partitionBy("sigungu_code")
        .parquet(output_path)
    )


def build_run_summary(
    args: argparse.Namespace,
    row_count: int,
    persisted_row_count: int | None,
    quality_metrics: dict[str, int],
    source_snapshot_summary: dict[str, Any],
) -> dict[str, Any]:
    return shared.build_run_summary(
        args,
        job_name=JOB_NAME,
        contract=RUN_SUMMARY_CONTRACT,
        input_kind=RUN_SUMMARY_INPUT_KIND,
        row_count=row_count,
        persisted_row_count=persisted_row_count,
        quality_metrics=quality_metrics,
        source_snapshot_summary=source_snapshot_summary,
        columns=SILVER_COLUMNS,
        required_columns=REQUIRED_SILVER_COLUMNS,
    )


def emit_run_summary(summary: dict[str, Any], output_path: str | None) -> None:
    shared.emit_run_summary(summary, output_path, label=LABELS.summary_json)


def build_spark_session(args: argparse.Namespace) -> Any:
    return shared.build_spark_session(
        args,
        job_name=JOB_NAME,
        needs_catalog=args.write_mode == "iceberg",
        needs_object_store=is_object_store_path(args.input),
    )


def main(argv: list[str] | None = None) -> int:
    # Arguments first, so a run that could never have worked says so without starting a session.
    args = parse_args(argv)
    validate_args(args)
    load_pyspark()
    spark = build_spark_session(args)

    try:
        handoff = read_handoff(spark, args.input, args.input_format)
        candidate = build_candidate_frame(handoff).persist(StorageLevel.MEMORY_AND_DISK)
        silver = candidate.select(*SILVER_COLUMNS).persist(StorageLevel.MEMORY_AND_DISK)
        row_count, candidate_quality_metrics = validate_parcel_frame(
            candidate,
            args.expected_count,
            include_transport=True,
        )
        source_snapshot_summary = shared.collect_source_snapshot_summary(silver)

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
            print(f"{LABELS.validate_ok} rows={row_count}")
            return 0

        if args.write_mode == "parquet":
            write_silver_parquet(silver, args.output)
            persisted = spark.read.parquet(args.output).select(*SILVER_COLUMNS)
            outcome_label = LABELS.parquet_write_ok
            outcome_target = f"output={args.output}"
        else:
            persisted, skip_line, outcome_label, outcome_target = shared.append_and_read_back(
                spark,
                silver,
                args,
                columns=SILVER_COLUMNS,
                contract=RUN_SUMMARY_CONTRACT,
                table_contract=TABLE_CONTRACT,
                labels=LABELS,
            )
            if persisted is None:
                print(skip_line)
                return 0

        persisted_count, persisted_quality_metrics = validate_parcel_frame(
            persisted,
            args.expected_count,
            include_transport=False,
        )
        shared.assert_row_count_unchanged(row_count, persisted_count)

        emit_run_summary(
            build_run_summary(
                args,
                row_count=row_count,
                persisted_row_count=persisted_count,
                quality_metrics=shared.merge_transport_metrics(
                    persisted_quality_metrics,
                    candidate_quality_metrics,
                ),
                source_snapshot_summary=source_snapshot_summary,
            ),
            args.summary_output,
        )
        print(shared.outcome_line(outcome_label, persisted_count, outcome_target))
        return 0
    finally:
        if "candidate" in locals():
            candidate.unpersist()
        if "silver" in locals():
            silver.unpersist()
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
