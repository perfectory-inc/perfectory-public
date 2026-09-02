#!/usr/bin/env python3
"""What a geometry handoff → Silver job does regardless of which source it reads.

Two jobs write a geometry handoff into a Silver table: `vworld_parcel_boundaries_handoff_to_silver`
and `industrial_complex_boundaries_handoff_to_silver`. They differ in real ways — one joins its
rows to `silver.industrial_complexes` for identity and province, the other derives nothing and
checks a 19-digit PNU instead — and those differences belong in the jobs.

Everything else had been copied. Measured 2026-09-02, thirty functions carried the same name in
both files and only five were the same text. The rest had drifted:

- `assert_quality_metrics` — 75 lines against 72, forty-one of them different. **The quality gate.**
  Two datasets were being judged by two implementations of one check.
- `build_spark_session` — different signatures, and two different expressions for "does this run
  need the catalog".
- `sample_invalid_rows` — the industrial-complex job learned to drop the geometry columns from a
  failure sample, because one boundary's WKB is tens of kilobytes of hex and five of them bury the
  field naming what failed. The parcel job never learned it, and it is the job with 39.8M rows.

That last one is what drift costs: a fix goes into one copy and the other keeps the defect, and
nothing anywhere says the two were ever meant to agree. This module is where they agree.

`lakehouse_object_store` records the same lesson one layer down — eight copies of the catalog
settings had drifted into three different key sets before anyone counted.

No PySpark import at module scope. The lane that runs `infra/lakehouse/spark/tests` has no PySpark
install, and a module-level import would make every check that touches this file skip itself —
which reports the same green as passing.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lakehouse_engine import (
    apply_catalog_settings,
    assert_catalog_env,
    assert_iceberg_runtime_loaded,
    iceberg_packages,
)
from lakehouse_ingest import append_batch_once, batch_source_record_ids
from lakehouse_object_store import (
    apply_object_store_settings,
    input_paths,
)
from platform_contracts import (
    create_table_columns_sql,
    partition_clause_sql,
)

DEFAULT_ICEBERG_PACKAGES = iceberg_packages()
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
RUN_SUMMARY_SCHEMA_VERSION = "foundation-platform.spark_run_summary.v1"

# The transport-only columns a geometry handoff carries: hex because JSONL has no bytes type, and
# the encoding beside it so a reader never has to assume which one was used.
TRANSPORT_COLUMNS: tuple[str, ...] = ("geometry_wkb_hex", "geometry_wkb_encoding")

# Columns a failure sample must not carry. One boundary's WKB is tens of kilobytes of hex, and five
# of them bury the field that says which row failed. Held here rather than in one job, because it
# was in one job: the parcel loader, with 39.8M rows, printed the blobs.
SAMPLE_SUPPRESSED_COLUMNS: tuple[str, ...] = (
    "geometry_wkb",
    "geometry_wkb_hex",
    "_geometry_wkb_hex",
)


def bind_pyspark() -> tuple[Any, Any, Any, Any, Any]:
    """Bind the PySpark namespaces this module uses, and hand them back to the caller.

    One binding point for both the shared checks and the job that calls them. The two jobs used to
    bind separately and in two different styles — one set module globals, the other threaded `F`
    through every signature — which is why functions doing the identical thing could not be
    compared, let alone shared.
    """

    global DataFrame, SparkSession, F, T, StorageLevel

    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.storagelevel import StorageLevel

    return DataFrame, SparkSession, F, T, StorageLevel


# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------


def add_common_arguments(
    parser: argparse.ArgumentParser,
    *,
    default_iceberg_table: str,
    input_help: str = "Silver handoff input path.",
) -> argparse.ArgumentParser:
    """Add the flags every handoff job takes, spelled once.

    `--iceberg-table` differs per job and is the argument; the rest are the same question asked of
    every job, so asking it in two files was two chances to word it differently.
    """

    parser.add_argument("--input", required=True, help=input_help)
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
        help="Spark catalog name for Iceberg REST catalog reads and writes.",
    )
    parser.add_argument(
        "--iceberg-namespace",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_NAMESPACE", "silver"),
        help="Iceberg namespace for the target Silver table.",
    )
    parser.add_argument(
        "--iceberg-table",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_TABLE", default_iceberg_table),
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
    return parser


def validate_identifier(label: str, value: str) -> None:
    if IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{label} must be a simple identifier: {value}")


def validate_common_args(args: argparse.Namespace, *, needs_catalog: bool) -> None:
    """Refuse an argument set no run could honour.

    `needs_catalog` is the caller's, because the two jobs answer it differently and both are right:
    the parcel job needs the catalog only to write, and the boundary job needs it to read its
    complexes even when writing Parquet. What must not differ is what happens once the answer is
    yes, which is why the check lives here and the answer does not.
    """

    if args.summary_output is not None and args.summary_output.strip() == "":
        raise ValueError("--summary-output must not be empty")

    if args.write_mode == "parquet" and not args.output:
        raise ValueError("--output is required when --write-mode=parquet")

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

    if needs_catalog:
        validate_identifier("iceberg catalog name", args.iceberg_catalog_name)
        assert_catalog_env()


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------


def build_spark_session(
    args: argparse.Namespace,
    *,
    job_name: str,
    needs_catalog: bool,
    needs_object_store: bool,
) -> Any:
    """Build the session this run needs and nothing more.

    Both conditions are the caller's answer, and both exist for a measured reason. Object-store
    settings need R2 reader credentials, so demanding them for a local-file run would make a job
    that touches no bucket fail on a missing bucket variable.
    """

    builder = (
        SparkSession.builder.appName(f"foundation-platform-{job_name}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
    )

    if needs_object_store:
        builder = apply_object_store_settings(builder)

    if needs_catalog:
        builder = apply_catalog_settings(builder, args.iceberg_catalog_name)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if needs_catalog:
        assert_iceberg_runtime_loaded(spark, args.iceberg_packages)
    return spark


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


def read_handoff(
    spark: Any,
    input_path: str,
    input_format: str,
    expected_columns: Sequence[str],
    *,
    label: str,
) -> Any:
    """Read one batch of handoff, in whichever format it was written.

    The argument is one string because a job takes one `--input`, but a batch of objects is several
    paths: a glob can name every key under a prefix and cannot name sixteen of them. `input_paths`
    is what decides, so the rule for splitting is not restated per job.

    JSONL is what these handoffs have always been and stays readable by anything. Parquet is what
    the three building-register handoffs already use, and it is the reason this choice exists: the
    same national extract is 46.8 GB as text and a fraction of that as compressed columns, and the
    conversion is bound by how fast those bytes cross a wire (measured 2026-08-31 — 86 MB/s
    saturated, CPU at six percent of the machine).
    """

    paths = input_paths(input_path)
    if input_format == "jsonl":
        handoff = spark.read.json(paths)
    elif input_format == "parquet":
        handoff = (
            spark.read.parquet(*paths) if isinstance(paths, list) else spark.read.parquet(paths)
        )
    else:
        raise ValueError(f"unsupported Silver handoff input format: {input_format}")

    missing_columns = sorted(set(expected_columns) - set(handoff.columns))
    if missing_columns:
        raise ValueError(f"{label} is missing columns: {', '.join(missing_columns)}")
    return handoff.select(*expected_columns)


def trim_to_null(column_name: str) -> Any:
    """Trim a string column, and call an empty result absent rather than blank."""

    trimmed = F.trim(F.col(column_name))
    return F.when(F.length(trimmed) == 0, F.lit(None)).otherwise(trimmed)


# ---------------------------------------------------------------------------
# Quality gates
# ---------------------------------------------------------------------------


def assert_columns(frame: Any, expected_columns: Sequence[str]) -> None:
    actual_columns = tuple(frame.select(*expected_columns).columns)
    if actual_columns != tuple(expected_columns):
        raise ValueError(
            "Unexpected Silver columns. "
            f"expected={list(expected_columns)} actual={list(actual_columns)}"
        )


def sample_invalid_rows(frame: Any, predicate: Any) -> list[str]:
    """Five failing rows, with the geometry blobs left out.

    `drop` ignores a column the frame does not have, so one list serves every frame here.
    """

    readable = frame.drop(*SAMPLE_SUPPRESSED_COLUMNS)
    return [str(sample) for sample in readable.where(predicate).limit(5).toJSON().collect()]


def assert_no_invalid_rows(frame: Any, metric_count: int, predicate: Any, message: str) -> None:
    if metric_count == 0:
        return

    samples = sample_invalid_rows(frame, predicate)
    raise ValueError(f"{message}. count={metric_count} samples={samples}")


def invalid_count(predicate: Any, alias: str) -> Any:
    return F.sum(F.when(predicate, F.lit(1)).otherwise(F.lit(0))).cast("long").alias(alias)


def is_invalid_double(column_name: str) -> Any:
    column = F.col(column_name)
    return column.isNull() | F.isnan(column)


def geometry_wkb_hex_is_invalid() -> Any:
    return (
        F.col("_geometry_wkb_hex").isNull()
        | (F.length(F.col("_geometry_wkb_hex")) == 0)
        | ((F.length(F.col("_geometry_wkb_hex")) % 2) != 0)
        | ~F.col("_geometry_wkb_hex").rlike(r"^[0-9a-f]+$")
    )


def geometry_wkb_is_invalid() -> Any:
    geometry_hex = F.lower(F.hex(F.col("geometry_wkb")))
    return (
        F.col("geometry_wkb").isNull()
        | (F.length(F.col("geometry_wkb")) <= 9)
        | ~geometry_hex.rlike(r"^(0103000000|0106000000)")
    )


def bbox_is_invalid() -> Any:
    return (
        is_invalid_double("bbox_min_x")
        | is_invalid_double("bbox_min_y")
        | is_invalid_double("bbox_max_x")
        | is_invalid_double("bbox_max_y")
        | (F.col("bbox_min_x") > F.col("bbox_max_x"))
        | (F.col("bbox_min_y") > F.col("bbox_max_y"))
    )


def checksum_is_invalid() -> Any:
    return ~F.col("geometry_checksum_sha256").rlike(r"^[0-9a-f]{64}$") | (
        F.col("geometry_checksum_sha256") != F.sha2(F.col("geometry_wkb"), 256)
    )


def transport_predicates(include_transport: bool) -> tuple[Any, Any]:
    """The two transport checks, or two constant falses when the columns are gone.

    A frame read back from the target has no transport columns — they exist to carry bytes through
    JSON — so the same gate runs twice over frames of two shapes. Returning constants rather than
    branching at each call site is why both jobs can share one metric collector.
    """

    if include_transport:
        return (F.col("_geometry_wkb_encoding") != F.lit("hex"), geometry_wkb_hex_is_invalid())
    return (F.lit(False), F.lit(False))


def required_column_expressions(
    required_columns: Sequence[str],
    required_string_columns: Sequence[str],
) -> list[Any]:
    """Per-column null and empty counters, named off the contract's required set."""

    expressions = [F.count(F.lit(1)).cast("long").alias("row_count")]
    for column in required_columns:
        expressions.append(invalid_count(F.col(column).isNull(), f"{column}__null_count"))
    for column in required_string_columns:
        expressions.append(invalid_count(F.length(F.col(column)) == 0, f"{column}__empty_count"))
    return expressions


def assert_required_columns(
    frame: Any,
    metrics: dict[str, int],
    required_columns: Sequence[str],
    required_string_columns: Sequence[str],
) -> None:
    for column in required_columns:
        assert_no_invalid_rows(
            frame,
            metrics[f"{column}__null_count"],
            F.col(column).isNull(),
            f"{column} must not be null",
        )

    for column in required_string_columns:
        assert_no_invalid_rows(
            frame,
            metrics[f"{column}__empty_count"],
            F.length(F.col(column)) == 0,
            f"{column} must not be empty",
        )


def assert_geometry(
    frame: Any,
    metrics: dict[str, int],
    include_transport: bool,
    geometry_srid: int,
) -> None:
    """The checks every geometry row answers, in the order a reader would want them.

    SRID first because it says which numbers the rest mean; then the transport, then the decoded
    bytes, then the box, then the checksum that ties the box and the bytes together.
    """

    assert_no_invalid_rows(
        frame,
        metrics["invalid_geometry_srid_count"],
        F.col("geometry_srid") != geometry_srid,
        f"geometry_srid must be {geometry_srid}",
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


def geometry_metric_expressions(include_transport: bool, geometry_srid: int) -> tuple[Any, ...]:
    """The counters behind `assert_geometry`, in the same set and under the same names."""

    invalid_encoding, invalid_hex = transport_predicates(include_transport)
    return (
        invalid_count(F.col("geometry_srid") != geometry_srid, "invalid_geometry_srid_count"),
        invalid_count(invalid_encoding, "invalid_geometry_encoding_count"),
        invalid_count(invalid_hex, "invalid_geometry_wkb_hex_count"),
        invalid_count(geometry_wkb_is_invalid(), "invalid_geometry_wkb_count"),
        invalid_count(bbox_is_invalid(), "invalid_bbox_count"),
        invalid_count(checksum_is_invalid(), "invalid_checksum_count"),
    )


# ---------------------------------------------------------------------------
# Target
# ---------------------------------------------------------------------------


def qualified_iceberg_table(args: argparse.Namespace) -> str:
    return f"`{args.iceberg_catalog_name}`.`{args.iceberg_namespace}`.`{args.iceberg_table}`"


def unquoted_qualified_iceberg_table(args: argparse.Namespace) -> str:
    return f"{args.iceberg_catalog_name}.{args.iceberg_namespace}.{args.iceberg_table}"


# The parcel table carried `read.parquet.vectorization.enabled = false` from 2026-08-28 to 08-30.
# It was a workaround for a defect in Iceberg 1.6.1 whose fix shipped in 1.8.0, and root ADR-0065
# raised this deployment to 1.11.0. The reason is gone, so the property is gone: a workaround left
# standing after its cause is a setting the next reader has to disprove before touching.
def create_iceberg_table_if_missing(
    spark: Any,
    args: argparse.Namespace,
    contract: dict[str, Any],
) -> None:
    namespace = f"`{args.iceberg_catalog_name}`.`{args.iceberg_namespace}`"
    table = qualified_iceberg_table(args)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
{create_table_columns_sql(contract)}
        )
        USING iceberg
        {partition_clause_sql(contract)}
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'zstd',
            'write.distribution-mode' = 'hash'
        )
        """
    )


@dataclass(frozen=True)
class HandoffLabels:
    """The five lines a handoff job prints, derived from one prefix.

    Each job used to spell all five out. They only ever differ by the prefix, so five strings per
    job were five chances for one of them to stop matching the others — and the run summary is
    found by grepping for its label.
    """

    prefix: str

    @property
    def summary_json(self) -> str:
        return f"{self.prefix}-summary-json"

    @property
    def validate_ok(self) -> str:
        return f"{self.prefix}-validate-ok"

    @property
    def parquet_write_ok(self) -> str:
        return f"{self.prefix}-write-ok"

    @property
    def iceberg_write_ok(self) -> str:
        return f"{self.prefix}-iceberg-write-ok"

    @property
    def already_ingested(self) -> str:
        return f"{self.prefix}-iceberg-already-ingested"


def outcome_line(label: str, rows: int, target: str) -> str:
    """`<label> rows=<n> <target>` — and the order is a contract, not a preference.

    `scripts/load/lakehouse-batch-load.sh` decides whether a batch landed, was skipped, or failed
    by matching `<label> rows=[0-9]+` in the log. Moving `rows=` anywhere else makes a successful
    write unmatchable, and the loader would read a success as no outcome at all.
    `test_vworld_parcel_boundaries_handoff_to_silver` holds the two together.
    """

    return f"{label} rows={rows} {target}"


def append_and_read_back(
    spark: Any,
    silver: Any,
    args: argparse.Namespace,
    *,
    columns: Sequence[str],
    contract: str,
    table_contract: dict[str, Any],
    labels: HandoffLabels,
) -> tuple[Any | None, str | None, str, str]:
    """Append this batch once, then read back exactly what it appended.

    Three separate incidents live in these twenty lines, which is why they are here and not copied
    into each job:

    - **The table has to exist before anything reads it**, including the read the skip path does.
      Preparing it just before the write left that read unprotected, and it died there three times.
    - **The append has to be the thing that records itself** (root ADR-0062). A SQL `INSERT` carries
      no writer options, so four jobs had nowhere to put the record and a retry appended again:
      1,865,891 parcels, three times over.
    - **The read-back has to be bounded by this run**, which `read_iceberg_snapshot_for_batch`
      does on the source object rather than the provider snapshot.

    Returns `(persisted, skip_line, label, target)`. On a skip the frame is `None` and `skip_line`
    is the whole line to print, because the row count is already known there. On an append the
    caller prints `outcome_line(label, count, target)` once it has verified the count.
    """

    create_iceberg_table_if_missing(spark, args, table_contract)
    outcome = append_batch_once(
        spark,
        silver,
        columns,
        qualified_iceberg_table(args),
        contract,
        write_mode=args.iceberg_write_mode,
    )
    target = f"table={args.iceberg_namespace}.{args.iceberg_table}"

    if not outcome["appended"]:
        # Count what the table holds rather than trusting the summary alone, so a skip reports the
        # rows it is standing on instead of asserting them.
        already = read_iceberg_snapshot_for_batch(spark, silver, args, columns).count()
        skip_line = outcome_line(
            labels.already_ingested,
            already,
            f"token={outcome['token']} snapshot={outcome['existing_snapshot']} "
            f"objects={len(outcome['record_ids'])}",
        )
        return None, skip_line, labels.already_ingested, target

    persisted = read_iceberg_snapshot_for_batch(spark, silver, args, columns)
    return persisted, None, labels.iceberg_write_ok, target


def assert_row_count_unchanged(before: int, after: int) -> None:
    """The rows that came back must be the rows that went in.

    Not a formality: it is what caught the read-back reading more than this run had written.
    """

    if after != before:
        raise ValueError(f"Persisted row count changed. before={before} after={after}")


def read_iceberg_snapshot_for_batch(
    spark: Any,
    silver: Any,
    args: argparse.Namespace,
    columns: Sequence[str],
) -> Any:
    """Read back exactly the rows this run appended, and nothing else.

    Filtered on the source object, not on `source_snapshot_id`. The snapshot id names the provider's
    extract, so every object of one national extract carries the same value: filtering the table by
    it made a second append read the first append's rows back too, and the uniqueness gate then
    reported every earlier row as a duplicate of itself — a run that had written correctly was
    reported as failed.
    """

    record_ids = batch_source_record_ids(silver)
    return (
        spark.table(qualified_iceberg_table(args))
        .where(F.col("source_record_id").isin(record_ids))
        .select(*columns)
    )


# ---------------------------------------------------------------------------
# Run summary
# ---------------------------------------------------------------------------


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


def merge_transport_metrics(
    persisted_metrics: dict[str, int],
    candidate_metrics: dict[str, int],
) -> dict[str, int]:
    """Take the transport counts from the candidate frame, which is the only frame that has them.

    The persisted frame was read back from the target, where the transport columns do not exist, so
    its counters are the constant zero `transport_predicates` produces. Reporting those would say
    the encoding was checked and found clean when it was not checked at all.
    """

    merged = dict(persisted_metrics)
    for metric in ("invalid_geometry_encoding_count", "invalid_geometry_wkb_hex_count"):
        merged[metric] = candidate_metrics[metric]
    return merged


def build_run_summary(
    args: argparse.Namespace,
    *,
    job_name: str,
    contract: str,
    input_kind: str,
    row_count: int,
    persisted_row_count: int | None,
    quality_metrics: dict[str, int],
    source_snapshot_summary: dict[str, Any],
    columns: Sequence[str],
    required_columns: Sequence[str],
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Assemble one `foundation-platform.spark_run_summary.v1` payload.

    `extra` carries what a job knows and this module does not — the boundary job names where it read
    its complexes. It is a parameter rather than a per-job copy of this function because the schema
    is a contract: `lakehouse-domain::lakehouse_run_summary` reads these keys.
    """

    summary = {
        "schema_version": RUN_SUMMARY_SCHEMA_VERSION,
        "job_name": job_name,
        "contract": contract,
        "created_at_utc": datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "input": {"kind": input_kind, "path": args.input},
        "target": run_summary_target(args),
        "write_mode": args.write_mode,
        "write_disposition": run_summary_disposition(args),
        "row_count": row_count,
        "persisted_row_count": persisted_row_count,
        "quality_metrics": summary_quality_metrics(quality_metrics, persisted_row_count),
        "column_count": len(columns),
        "columns": list(columns),
        "required_columns": list(required_columns),
        **source_snapshot_summary,
    }
    if extra:
        summary.update(extra)
    return summary


def emit_run_summary(summary: dict[str, Any], output_path: str | None, *, label: str) -> None:
    payload = json.dumps(summary, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{payload}\n", encoding="utf-8")

    print(f"{label} {payload}")
