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

Everything a geometry handoff does regardless of source — the transport decoding, the geometry
gates, the target, the run summary — lives in `spatial_silver_handoff`. It used to be copied here
and into the parcel job, and the two copies had drifted apart.
"""

from __future__ import annotations

import argparse
from typing import Any

import spatial_silver_handoff as shared
from platform_contracts import (
    column_names,
    current_row_predicate,
    declared_geometry_srid,
    load_lakehouse_contract,
    required_column_names,
    required_string_column_names,
)

JOB_NAME = "industrial_complex_boundaries_handoff_to_silver"
RUN_SUMMARY_CONTRACT = "silver.industrial_complex_boundaries"
COMPLEXES_CONTRACT_NAME = "silver.industrial_complexes"
RUN_SUMMARY_INPUT_KIND = "silver_handoff_jsonl"
LABELS = shared.HandoffLabels("silver-industrial-complex-boundaries")

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
    *shared.TRANSPORT_COLUMNS,
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


def load_pyspark() -> None:
    """Bind the PySpark namespaces this job uses, through the shared module.

    One binding for both, so the shared checks and this file look at the same `F`. Deferred because
    the lane that runs `infra/lakehouse/spark/tests` has no PySpark install, and a module-level
    import would make every check that touches this file skip itself.
    """

    global DataFrame, SparkSession, F, T, StorageLevel

    DataFrame, SparkSession, F, T, StorageLevel = shared.bind_pyspark()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build silver.industrial_complex_boundaries from the boundary handoff JSONL, joined to "
            "silver.industrial_complexes for complex_id and sido_code."
        )
    )
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
    shared.add_common_arguments(
        parser,
        default_iceberg_table="industrial_complex_boundaries",
        input_help="Silver handoff JSONL input path.",
    )
    return parser.parse_args(argv)


def needs_iceberg_catalog(args: argparse.Namespace) -> bool:
    """Whether this run has to reach the Iceberg REST catalog at all.

    Reading the complexes from Iceberg needs it just as much as writing does — the join is not
    optional, so a run that can write but cannot read its complexes is not a run.
    """

    return args.write_mode == "iceberg" or args.complexes_mode == "iceberg"


def validate_args(args: argparse.Namespace) -> None:
    """Check the complexes source this job alone has, then hand the rest to the shared rules."""

    if args.complexes_mode == "jsonl" and not args.complexes_input:
        raise ValueError("--complexes-input is required when --complexes-mode=jsonl")

    if args.complexes_mode == "iceberg":
        shared.validate_identifier(
            "complexes iceberg namespace", args.complexes_iceberg_namespace
        )
        shared.validate_identifier("complexes iceberg table", args.complexes_iceberg_table)

    shared.validate_common_args(args, needs_catalog=needs_iceberg_catalog(args))


def read_handoff(spark: Any, input_path: str) -> Any:
    return shared.read_handoff(
        spark,
        input_path,
        "jsonl",
        HANDOFF_INPUT_COLUMNS,
        label="Industrial-complex boundary handoff",
    )


# ---------------------------------------------------------------------------
# The join, which is what makes this job different
# ---------------------------------------------------------------------------


def qualified_complexes_table(args: argparse.Namespace) -> str:
    return (
        f"`{args.iceberg_catalog_name}`."
        f"`{args.complexes_iceberg_namespace}`."
        f"`{args.complexes_iceberg_table}`"
    )


def read_complexes(spark: Any, args: argparse.Namespace) -> Any:
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


def assert_one_row_per_complex(complexes: Any) -> None:
    """Refuse an ambiguous complex list rather than picking a row.

    `silver.industrial_complexes` declares no `current_row_predicate`, so this job has no
    contract-stated way to choose between two rows for one code. Inventing one — "the latest
    `valid_from_utc`", say — would put a versioning rule in a boundary loader, where nobody would
    look for it. When the table starts carrying history, the predicate belongs in the contract and
    this check is what will say so.
    """

    duplicates = complexes.groupBy("official_complex_code").count().where(F.col("count") > 1)
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


def collect_join_counts(handoff: Any, complexes: Any) -> dict[str, int]:
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


def assert_handoff_leaves_joined_columns_null(handoff: Any) -> None:
    """Refuse a handoff that already filled the columns this job is here to resolve.

    A producer that arrived at a `complex_id` on its own derived it a second time, and the two
    derivations agreeing today is not the same as their agreeing after one of them changes. The
    handoff's job is to say `official_complex_code`; the id comes off the complex table.
    """

    filled = handoff.where(F.col("complex_id").isNotNull() | F.col("sido_code").isNotNull())
    filled_count = int(filled.count())
    if filled_count == 0:
        return

    samples = [
        str(sample)
        for sample in filled.drop(*shared.SAMPLE_SUPPRESSED_COLUMNS).limit(5).toJSON().collect()
    ]
    raise ValueError(
        f"{filled_count} handoff rows already carry complex_id or sido_code; this job resolves "
        f"both from {COMPLEXES_CONTRACT_NAME} and must not accept a second derivation. "
        f"samples={samples}"
    )


def join_handoff_to_complexes(handoff: Any, complexes: Any) -> Any:
    """Attach identity and province to each boundary, keeping only rows that have both.

    A boundary whose complex states no province cannot be written: `sido_code` is required and is
    the first partition column. It is dropped here and counted in `complex_without_sido_code_count`,
    which is the difference between a row this job refused and a row nobody noticed was missing.
    """

    joined = handoff.drop(*JOINED_COLUMNS).join(complexes, "official_complex_code", "inner")
    return joined.where(F.col("sido_code").isNotNull() & (F.length(F.col("sido_code")) > 0))


def build_candidate_frame(joined: Any) -> Any:
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
        F.to_timestamp(F.col("ingested_at_utc"), "yyyy-MM-dd'T'HH:mm:ssX").alias(
            "ingested_at_utc"
        ),
        geometry_wkb_hex.alias("_geometry_wkb_hex"),
        geometry_wkb_encoding.alias("_geometry_wkb_encoding"),
    )


# ---------------------------------------------------------------------------
# What makes a complex boundary a complex boundary
# ---------------------------------------------------------------------------


def centroid_is_outside_bbox() -> Any:
    return (
        shared.is_invalid_double("centroid_x")
        | shared.is_invalid_double("centroid_y")
        | (F.col("centroid_x") < F.col("bbox_min_x"))
        | (F.col("centroid_x") > F.col("bbox_max_x"))
        | (F.col("centroid_y") < F.col("bbox_min_y"))
        | (F.col("centroid_y") > F.col("bbox_max_y"))
    )


def area_is_non_positive() -> Any:
    """An area column that is present must state an area.

    The column is optional, so null is allowed — it says the source drew no boundary this job could
    measure. Zero or negative is a different claim, and a false one.
    """

    return F.col("area_sqm_calculated").isNotNull() & (F.col("area_sqm_calculated") <= 0)


def boundary_kind_is_invalid() -> Any:
    return F.col("boundary_kind") != F.lit(OFFICIAL_BOUNDARY_KIND)


def collect_duplicate_active_complex_count(frame: Any) -> int:
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


def assert_no_duplicate_active_complex(frame: Any, metric_count: int) -> None:
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


def collect_quality_metrics(frame: Any, include_transport: bool) -> dict[str, int]:
    expressions = shared.required_column_expressions(
        REQUIRED_SILVER_COLUMNS, REQUIRED_STRING_COLUMNS
    )
    expressions.extend(shared.geometry_metric_expressions(include_transport, GEOMETRY_SRID))
    expressions.extend(
        (
            shared.invalid_count(boundary_kind_is_invalid(), "invalid_boundary_kind_count"),
            shared.invalid_count(centroid_is_outside_bbox(), "centroid_outside_bbox_count"),
            shared.invalid_count(area_is_non_positive(), "non_positive_area_count"),
        )
    )

    row = frame.agg(*expressions).first()
    if row is None:
        raise ValueError("Silver quality metric aggregation returned no row")
    metrics = {key: int(value or 0) for key, value in row.asDict().items()}
    metrics["duplicate_active_complex_count"] = collect_duplicate_active_complex_count(frame)
    for metric in BOUNDARY_SPECIFIC_QUALITY_METRICS:
        metrics.setdefault(metric, 0)
    return metrics


def assert_quality_metrics(frame: Any, metrics: dict[str, int], include_transport: bool) -> None:
    shared.assert_required_columns(
        frame, metrics, REQUIRED_SILVER_COLUMNS, REQUIRED_STRING_COLUMNS
    )
    shared.assert_no_invalid_rows(
        frame,
        metrics["invalid_boundary_kind_count"],
        boundary_kind_is_invalid(),
        "this job loads the authority's published boundary, so boundary_kind must be "
        f"{OFFICIAL_BOUNDARY_KIND}",
    )
    shared.assert_geometry(frame, metrics, include_transport, GEOMETRY_SRID)
    shared.assert_no_invalid_rows(
        frame,
        metrics["centroid_outside_bbox_count"],
        centroid_is_outside_bbox(),
        "centroid must sit inside its bbox",
    )
    shared.assert_no_invalid_rows(
        frame,
        metrics["non_positive_area_count"],
        area_is_non_positive(),
        "area_sqm_calculated must be positive when present",
    )
    assert_no_duplicate_active_complex(frame, metrics["duplicate_active_complex_count"])


def validate_boundary_frame(
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


def merge_join_counts(metrics: dict[str, int], join_counts: dict[str, int]) -> dict[str, int]:
    """Carry the join counts into whichever metric set the summary reports.

    They are properties of the run, not of the persisted frame, so re-measuring them off the
    written rows would report zero and read as "nothing was dropped".
    """

    merged = dict(metrics)
    for metric in JOIN_COUNT_METRICS:
        merged[metric] = int(join_counts.get(metric, 0))
    return merged


# ---------------------------------------------------------------------------
# Target and summary
# ---------------------------------------------------------------------------


def write_silver_parquet(silver: Any, output_path: str) -> None:
    (
        silver.repartition("sido_code")
        .sortWithinPartitions("complex_id", "boundary_kind", "valid_from_utc")
        .write.mode("overwrite")
        .partitionBy("sido_code")
        .parquet(output_path)
    )


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
        extra={
            "complexes_source": run_summary_complexes_source(args),
            "geometry_srid": GEOMETRY_SRID,
        },
    )


def emit_run_summary(summary: dict[str, Any], output_path: str | None) -> None:
    shared.emit_run_summary(summary, output_path, label=LABELS.summary_json)


def build_spark_session(args: argparse.Namespace) -> Any:
    return shared.build_spark_session(
        args,
        job_name=JOB_NAME,
        needs_catalog=needs_iceberg_catalog(args),
        needs_object_store=False,
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_args(args)
    load_pyspark()
    spark = build_spark_session(args)

    try:
        handoff = read_handoff(spark, args.input).persist(StorageLevel.MEMORY_AND_DISK)
        complexes = read_complexes(spark, args).persist(StorageLevel.MEMORY_AND_DISK)
        assert_handoff_leaves_joined_columns_null(handoff)
        assert_one_row_per_complex(complexes)
        join_counts = collect_join_counts(handoff, complexes)
        joined = join_handoff_to_complexes(handoff, complexes)
        candidate = build_candidate_frame(joined).persist(StorageLevel.MEMORY_AND_DISK)
        silver = candidate.select(*SILVER_COLUMNS).persist(StorageLevel.MEMORY_AND_DISK)

        row_count, candidate_quality_metrics = validate_boundary_frame(
            candidate, args.expected_count, include_transport=True
        )
        source_snapshot_summary = shared.collect_source_snapshot_summary(silver)

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

        persisted_count, persisted_quality_metrics = validate_boundary_frame(
            persisted, args.expected_count, include_transport=False
        )
        shared.assert_row_count_unchanged(row_count, persisted_count)

        emit_run_summary(
            build_run_summary(
                args,
                row_count=row_count,
                persisted_row_count=persisted_count,
                quality_metrics=merge_join_counts(
                    shared.merge_transport_metrics(
                        persisted_quality_metrics, candidate_quality_metrics
                    ),
                    join_counts,
                ),
                source_snapshot_summary=source_snapshot_summary,
            ),
            args.summary_output,
        )
        print(shared.outcome_line(outcome_label, persisted_count, outcome_target))
        return 0
    finally:
        for name in ("silver", "candidate", "complexes", "handoff"):
            frame = locals().get(name)
            if frame is not None:
                frame.unpersist()
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
