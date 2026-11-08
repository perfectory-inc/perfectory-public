#!/usr/bin/env python3
"""Build the Gold industrial-complex catalog projection from Silver rows.

A complex's 시도·시군구 codes are the source's codes carried to the current ones through the
recorded 법정동 code pairs (`reference.legal_dong_code_change`, `place_identity_resolver.current_region`),
and `sido_name` is the current code's name in the newest 법정동 code snapshot (root ADR-0426). The
codes are re-derived on every build, never kept as the load first read them.

`--follow-region-codes` re-places only the complexes a change of that table reaches: the codes of
every pair that differs from the rows the last follow read (`legal_dong_code_change_views.changed_codes`)
name the complexes under them, and only the ones whose carried codes differ from Gold are merged
(lineage_stewardship runs it after it records new pairs; the canonical load follows it).
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
from typing import Any, Iterable, Mapping, NamedTuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.storagelevel import StorageLevel

from lakehouse_engine import (
    apply_catalog_settings,
    assert_catalog_env,
    assert_iceberg_runtime_loaded,
    current_snapshot,
    iceberg_packages,
    SHUFFLE_SETTINGS,
)
from platform_contracts import (
    admit_write,
    partition_clause_sql,
    column_names,
    create_table_columns_sql,
    current_rows,
    evolve_iceberg_table_to_contract,
    load_lakehouse_contract,
    partition_column_names,
    partition_spec_sql,
    required_column_names,
    sort_order,
    string_column_names,
)
import legal_dong_code_change_views as code_change_views
import place_identity_resolver as resolver


DEFAULT_ICEBERG_PACKAGES = iceberg_packages()
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

# Read off the contract rather than spelled here, so the projection cannot keep partitioning by a
# column the canonical table stopped requiring (root ADR-0035).
GOLD_PARTITION_COLUMNS: tuple[str, ...] = partition_column_names(GOLD_CONTRACT)

GOLD_SORT_COLUMNS: tuple[str, ...] = sort_order(GOLD_CONTRACT)

CODE_CHANGE_TABLE = code_change_views.CHANGE_TABLE
LEGAL_DONG_SNAPSHOT_TABLE = "reference.legal_dong_code_snapshot"
CODE_CHANGE_COLUMNS = ("kind", "old_code", "new_code", "level", "effective_date", "source")
# What `changed_codes` compares two reads of the change table by; the follow's state file keeps these.
FOLLOWED_COLUMNS = ("kind", "old_code", "new_code", "level")
REGION_COLUMNS = ("sido_code", "sigungu_code", "sido_name")


class RegionCodes(NamedTuple):
    """The recorded pairs as old → current code, the codes that split, and the current 시도 names."""

    remap: Mapping[str, str]
    ambiguous: set[str]
    names: Mapping[str, str]


def region_codes(change_rows: Iterable[Mapping[str, Any]], snapshot_names: Iterable[tuple[str, str, str]]) -> RegionCodes:
    """``RegionCodes`` from the change table's rows (the pairs in force, root ADR-0329) and the newest
    snapshot's (code, full name, status) rows."""

    remap, ambiguous = resolver.code_crosswalk(
        (pair["old_code"], pair["new_code"]) for pair in code_change_views.pair_rows(change_rows))
    return RegionCodes(remap, ambiguous, resolver.sido_names(snapshot_names))


def carried_region(region: RegionCodes, sido: str | None, sigungu: str | None,
                   bjdong: str | None) -> tuple[str | None, str | None, str | None]:
    """(current 시도, current 시군구, the 시도's current name) for a complex's source codes."""

    sido_now, sigungu_now, _ = resolver.current_region(sido, sigungu, bjdong, region.remap, region.ambiguous)
    return sido_now, sigungu_now, region.names.get(sido_now) if sido_now else None


def region_replacements(
    gold: Mapping[str, tuple[str | None, str | None, str | None]],
    source: Mapping[str, tuple[str | None, str | None, str | None]],
    prefixes: set[str] | None,
    region: RegionCodes,
) -> list[tuple[str, str | None, str | None, str | None]]:
    """[(complex_id, sido_code, sigungu_code, sido_name)] for each Gold complex under ``prefixes``
    whose held region columns differ from what its source codes carry to now.

    ``gold`` is {complex_id: (sido_code, sigungu_code, sido_name)} as Gold holds them, ``source``
    {complex_id: (sido_code, sigungu_code, primary_bjdong_code)} as Silver's current rows state them.
    A complex is under a prefix when its source code or its held code starts with it (a pair's code
    prefix, ``legal_dong_code_change_views.code_prefix``): the source code finds a newly recorded
    pair, the held one a pair a supersession withdrew. ``prefixes`` None reaches every complex (the
    first follow, which has no earlier read to compare with). A Gold complex without a current
    Silver row is left to the full build.
    """

    out = []
    for complex_id in sorted(gold):
        codes = source.get(complex_id)
        if codes is None:
            continue
        held = gold[complex_id]
        if prefixes is not None and not any(
                code.startswith(prefix) for code in (*codes, *held[:2]) if code for prefix in prefixes):
            continue
        carried = carried_region(region, *codes)
        if carried != tuple(held):
            out.append((complex_id, *carried))
    return out


def changed_prefixes(previous: list[Mapping[str, Any]] | None, current: list[Mapping[str, Any]]) -> set[str] | None:
    """The code prefixes a change of the table between two reads reaches; None when there is no
    earlier read (everything is reached)."""

    if previous is None:
        return None
    return {code_change_views.code_prefix(code)
            for code in code_change_views.changed_codes(previous, current, FOLLOWED_COLUMNS)}


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
        default=None,
        help="Iceberg snapshot id represented by this Gold projection (required unless --follow-region-codes).",
    )
    parser.add_argument(
        "--follow-region-codes",
        action="store_true",
        help="Re-place only the complexes a change of the 법정동 code change table reaches (root ADR-0426).",
    )
    parser.add_argument(
        "--followed-code-changes",
        default=None,
        help="The change-table rows the last follow read (JSON); absent on the first follow.",
    )
    parser.add_argument(
        "--followed-code-changes-output",
        default=None,
        help="Where the follow writes the change-table rows it read, for the next follow to compare with.",
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
    if args.follow_region_codes:
        if args.input_mode != "iceberg" or args.write_mode != "iceberg" or not args.followed_code_changes_output:
            raise ValueError("--follow-region-codes reads and merges Iceberg tables "
                             "(--input-mode iceberg --write-mode iceberg) and needs --followed-code-changes-output")
    elif not args.iceberg_snapshot_id:
        raise ValueError("--iceberg-snapshot-id is required")
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

    # The follow only MERGEs region columns, so the overwrite default does not apply to it: it refused
    # its first scheduled run on 2026-11-07 for want of --allow-non-smoke-overwrite (incident W21).
    if args.write_mode == "iceberg" and args.iceberg_write_mode == "overwrite" and not args.follow_region_codes:
        is_smoke_table = args.target_iceberg_table.endswith("_smoke")
        if not is_smoke_table and not args.allow_non_smoke_overwrite:
            raise ValueError(
                "Refusing to overwrite a non-smoke Iceberg table without "
                "--allow-non-smoke-overwrite"
            )

    if args.input_mode == "iceberg" or args.write_mode == "iceberg":
        assert_catalog_env()


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


def with_current_region(latest: DataFrame, region: RegionCodes | None) -> DataFrame:
    """``latest`` with its 시도·시군구 codes carried to the current ones and ``sido_name`` added
    (root ADR-0426). The distinct source code triples are a few hundred, so they are carried in plain
    Python and joined back. Without ``region`` (a local build with no catalog) the source codes stay
    and no name is claimed."""

    if region is None:
        return latest.withColumn("sido_name", F.lit(None).cast(T.StringType()))
    triples = latest.select("sido_code", "sigungu_code", "primary_bjdong_code").distinct().collect()
    lookup = latest.sparkSession.createDataFrame(
        [(row[0], row[1], row[2], *carried_region(region, row[0], row[1], row[2])) for row in triples],
        "src_sido string, src_sigungu string, src_bjdong string, "
        "cur_sido string, cur_sigungu string, cur_sido_name string",
    )
    same = (latest["sido_code"].eqNullSafe(lookup["src_sido"])
            & latest["sigungu_code"].eqNullSafe(lookup["src_sigungu"])
            & latest["primary_bjdong_code"].eqNullSafe(lookup["src_bjdong"]))
    return (latest.join(F.broadcast(lookup), same, "left")
            .withColumn("sido_code", F.col("cur_sido"))
            .withColumn("sigungu_code", F.col("cur_sigungu"))
            .withColumn("sido_name", F.col("cur_sido_name"))
            .drop("src_sido", "src_sigungu", "src_bjdong", "cur_sido", "cur_sigungu", "cur_sido_name"))


def build_gold_catalog_frame(
    silver: DataFrame,
    iceberg_snapshot_id: str,
    published_at_utc: str,
    boundary_object_key: str | None,
    region: RegionCodes | None = None,
) -> DataFrame:
    # Which row is current is the contract's rule, the same one every reader of this table uses
    # (root ADR-0267), not a window this job keeps for itself.
    latest = with_current_region(current_rows(silver, SILVER_CONTRACT), region)

    return latest.select(
        F.col("complex_id"),
        F.col("official_complex_code"),
        F.col("complex_name").alias("name"),
        F.col("complex_kind").alias("kind"),
        F.col("status"),
        F.col("development_stage"),
        F.col("complex_kind_raw").alias("kind_raw"),
        F.col("status_raw"),
        F.col("sido_code"),
        F.col("sigungu_code"),
        F.col("sido_name"),
        F.col("address_text"),
        F.col("management_agency_name"),
        F.col("developer_name"),
        F.col("designated_date").cast(T.DateType()).alias("designated_date"),
        F.col("construction_start_date").cast(T.DateType()).alias("construction_start_date"),
        F.col("completion_date").cast(T.DateType()).alias("completion_date"),
        F.col("official_area_sqm").cast(T.DecimalType(18, 2)).alias("official_area_sqm"),
        F.col("development_progress_percent")
        .cast(T.DecimalType(5, 2))
        .alias("development_progress_percent"),
        F.col("lot_sales_status"),
        F.col("lot_sales_status_raw"),
        F.col("business_period_raw"),
        F.col("business_period_start_month"),
        F.col("business_period_end_month"),
        F.col("designation_basis_law_raw"),
        F.col("development_method_raw"),
        F.col("development_purpose_raw"),
        F.col("invited_industries_raw"),
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


def gated_string_columns() -> tuple[str, ...]:
    """Return the string columns whose emptiness is a defect.

    Every string column, not just the required ones: an optional column may be null and may never
    be `""` (root ADR-0035).
    """

    return string_column_names(GOLD_CONTRACT)


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

    for column in gated_string_columns():
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

    for column in gated_string_columns():
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
    schema_evolution_added_columns: Sequence[str] = (),
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
        # Which contract columns this run had to add to the live table. Empty on every run whose
        # target already matched the contract, which is what a steady state looks like.
        "schema_evolution_added_columns": list(schema_evolution_added_columns),
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
        "development_stage": [
            {"dataset": SILVER_CONTRACT_NAME, "column": "development_stage", "transform": "identity"}
        ],
        "kind_raw": [
            {"dataset": SILVER_CONTRACT_NAME, "column": "complex_kind_raw", "transform": "rename"}
        ],
        "status_raw": [
            {"dataset": SILVER_CONTRACT_NAME, "column": "status_raw", "transform": "identity"}
        ],
        # Carried to the current codes through the recorded pairs (root ADR-0426).
        "sido_code": [
            {"dataset": SILVER_CONTRACT_NAME, "column": "sido_code", "transform": "current_region_code"},
            {"dataset": CODE_CHANGE_TABLE, "column": "new_code", "transform": "current_region_code"},
        ],
        "sigungu_code": [
            {"dataset": SILVER_CONTRACT_NAME, "column": "sigungu_code", "transform": "current_region_code"},
            {"dataset": CODE_CHANGE_TABLE, "column": "new_code", "transform": "current_region_code"},
        ],
        "sido_name": [
            {"dataset": LEGAL_DONG_SNAPSHOT_TABLE, "column": "full_name", "transform": "sido_name"}
        ],
        "address_text": [
            {"dataset": SILVER_CONTRACT_NAME, "column": "address_text", "transform": "identity"}
        ],
        "management_agency_name": [
            {
                "dataset": SILVER_CONTRACT_NAME,
                "column": "management_agency_name",
                "transform": "identity",
            }
        ],
        "developer_name": [
            {"dataset": SILVER_CONTRACT_NAME, "column": "developer_name", "transform": "identity"}
        ],
        "designated_date": [
            {
                "dataset": SILVER_CONTRACT_NAME,
                "column": "designated_date",
                "transform": "cast_date",
            }
        ],
        "construction_start_date": [
            {
                "dataset": SILVER_CONTRACT_NAME,
                "column": "construction_start_date",
                "transform": "cast_date",
            }
        ],
        "completion_date": [
            {
                "dataset": SILVER_CONTRACT_NAME,
                "column": "completion_date",
                "transform": "cast_date",
            }
        ],
        "official_area_sqm": [
            {
                "dataset": SILVER_CONTRACT_NAME,
                "column": "official_area_sqm",
                "transform": "cast_decimal_18_2",
            }
        ],
        "development_progress_percent": [
            {
                "dataset": SILVER_CONTRACT_NAME,
                "column": "development_progress_percent",
                "transform": "cast_decimal_5_2",
            }
        ],
        "lot_sales_status": [
            {"dataset": SILVER_CONTRACT_NAME, "column": "lot_sales_status", "transform": "identity"}
        ],
        "lot_sales_status_raw": [
            {
                "dataset": SILVER_CONTRACT_NAME,
                "column": "lot_sales_status_raw",
                "transform": "identity",
            }
        ],
        "business_period_raw": [
            {
                "dataset": SILVER_CONTRACT_NAME,
                "column": "business_period_raw",
                "transform": "identity",
            }
        ],
        "business_period_start_month": [
            {
                "dataset": SILVER_CONTRACT_NAME,
                "column": "business_period_start_month",
                "transform": "identity",
            }
        ],
        "business_period_end_month": [
            {
                "dataset": SILVER_CONTRACT_NAME,
                "column": "business_period_end_month",
                "transform": "identity",
            }
        ],
        "designation_basis_law_raw": [
            {
                "dataset": SILVER_CONTRACT_NAME,
                "column": "designation_basis_law_raw",
                "transform": "identity",
            }
        ],
        "development_method_raw": [
            {
                "dataset": SILVER_CONTRACT_NAME,
                "column": "development_method_raw",
                "transform": "identity",
            }
        ],
        "development_purpose_raw": [
            {
                "dataset": SILVER_CONTRACT_NAME,
                "column": "development_purpose_raw",
                "transform": "identity",
            }
        ],
        "invited_industries_raw": [
            {
                "dataset": SILVER_CONTRACT_NAME,
                "column": "invited_industries_raw",
                "transform": "identity",
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
            # The Gold snapshot this run committed, not the Silver snapshot it was given
            # (`--iceberg-snapshot-id`, stamped into the rows).
            "snapshot_id": summary["target"].get("snapshot_id"),
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
    frame = gold
    if GOLD_PARTITION_COLUMNS:
        frame = frame.repartition(*[F.col(column) for column in GOLD_PARTITION_COLUMNS])
    if GOLD_SORT_COLUMNS:
        frame = frame.sortWithinPartitions(*GOLD_SORT_COLUMNS)
    writer = frame.write.mode("overwrite")
    if GOLD_PARTITION_COLUMNS:
        writer = writer.partitionBy(*GOLD_PARTITION_COLUMNS)
    writer.parquet(output_path)


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
        {partition_clause_sql(GOLD_CONTRACT)}
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'zstd',
            'write.distribution-mode' = 'hash'
        )
        """
    )


def evolve_gold_iceberg_table_to_contract(spark: SparkSession, table: str) -> tuple[str, ...]:
    """Bring the live Gold table up to the Gold contract before the positional INSERT.

    The step itself is shared with the Bronze-to-Silver job, because both target tables are
    published and both contracts widen; see ``platform_contracts.evolve_iceberg_table_to_contract``
    for why ``CREATE TABLE IF NOT EXISTS`` cannot do this and why column position matters.
    """

    return evolve_iceberg_table_to_contract(spark, table, GOLD_CONTRACT)


def write_gold_iceberg(
    spark: SparkSession,
    gold: DataFrame,
    args: argparse.Namespace,
) -> tuple[str, ...]:
    table = qualified_target_table(args)
    temp_view = "gold_complex_catalog_candidate"

    create_gold_iceberg_table_if_missing(spark, args)
    added_columns = evolve_gold_iceberg_table_to_contract(spark, table)
    gold.select(*GOLD_COLUMNS).createOrReplaceTempView(temp_view)

    admit_write(GOLD_CONTRACT_NAME, "replace" if args.iceberg_write_mode == "overwrite" else "append", target=table)
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
    return added_columns


def read_region_inputs(spark: SparkSession, catalog: str) -> tuple[list[dict[str, Any]], list[tuple[str, str, str]]]:
    """The change table's rows and the newest 법정동 snapshot's (code, full name, status) rows: about
    3,500 and 53,000 rows, read whole like ``address_pnu.lakehouse_sido_prefixes``."""

    schema, name = CODE_CHANGE_TABLE.split(".")
    changes = [row.asDict() for row in spark.sql(
        f"SELECT {', '.join(CODE_CHANGE_COLUMNS)} FROM `{catalog}`.`{schema}`.`{name}`").collect()]
    schema, name = LEGAL_DONG_SNAPSHOT_TABLE.split(".")
    table = f"`{catalog}`.`{schema}`.`{name}`"
    names = [(row["region_cd"], row["full_name"], row["status"]) for row in spark.sql(
        f"SELECT region_cd, full_name, status FROM {table} "
        f"WHERE snapshot_date = (SELECT max(snapshot_date) FROM {table})").collect()]
    return changes, names


def build_region(spark: SparkSession, args: argparse.Namespace) -> RegionCodes | None:
    """The region codes a build carries the source codes through. A live (non-smoke) Gold refuses to
    build without them: the source codes alone are how 135 complexes kept 29·46 after 2026-07-01."""

    catalog = args.iceberg_catalog_name
    if args.input_mode != "iceberg" and args.write_mode != "iceberg":
        return None
    present = all(spark.catalog.tableExists(f"{catalog}.{table}")
                  for table in (CODE_CHANGE_TABLE, LEGAL_DONG_SNAPSHOT_TABLE))
    if not present:
        if args.write_mode == "iceberg" and not args.target_iceberg_table.endswith("_smoke"):
            raise ValueError(f"{CODE_CHANGE_TABLE} and {LEGAL_DONG_SNAPSHOT_TABLE} are required to build a live Gold")
        return None
    return region_codes(*read_region_inputs(spark, catalog))


def followed_rows(changes: Iterable[Mapping[str, Any]]) -> list[dict[str, str]]:
    """The change-table rows as the follow's state file keeps them: the columns `changed_codes` reads."""

    rows = {tuple(str(row.get(column) or "") for column in FOLLOWED_COLUMNS) for row in changes}
    return [dict(zip(FOLLOWED_COLUMNS, row)) for row in sorted(rows)]


def follow_region_codes(spark: SparkSession, args: argparse.Namespace) -> dict[str, Any]:
    """Re-place the region columns of the complexes a change of the code change table reaches."""

    table = qualified_target_table(args)
    changes, names = read_region_inputs(spark, args.iceberg_catalog_name)
    followed_path = Path(args.followed_code_changes) if args.followed_code_changes else None
    previous = (json.loads(followed_path.read_text(encoding="utf-8"))
                if followed_path is not None and followed_path.exists() else None)
    prefixes = changed_prefixes(previous, changes)
    summary: dict[str, Any] = {
        "job_name": JOB_NAME, "mode": "follow_region_codes", "target": unquoted_target_table(args),
        "first_follow": previous is None, "changed_prefixes": None if prefixes is None else sorted(prefixes),
        "replaced_complexes": [],
    }
    if prefixes is not None and not prefixes:
        summary["followed"] = False
    elif not spark.catalog.tableExists(table.replace("`", "")):
        summary["followed"] = False
        summary["reason"] = "no_gold_table"
    else:
        # The merge replaces the region columns of the rows it covers (gold.complex_catalog is Replaced).
        admit_write(GOLD_CONTRACT_NAME, "replace", target=table)
        evolve_gold_iceberg_table_to_contract(spark, table)
        gold = {row["complex_id"]: (row["sido_code"], row["sigungu_code"], row["sido_name"])
                for row in spark.table(table).select("complex_id", *REGION_COLUMNS).collect()}
        silver = current_rows(read_silver_input(spark, args), SILVER_CONTRACT)
        source = {row["complex_id"]: (row["sido_code"], row["sigungu_code"], row["primary_bjdong_code"])
                  for row in silver.select("complex_id", "sido_code", "sigungu_code",
                                           "primary_bjdong_code").collect()}
        replacements = region_replacements(gold, source, prefixes, region_codes(changes, names))
        if replacements:
            view = "gold_complex_catalog_region_replacements"
            spark.createDataFrame(replacements, "complex_id string, sido_code string, sigungu_code string, "
                                  "sido_name string").createOrReplaceTempView(view)
            spark.sql(f"MERGE INTO {table} t USING {view} s ON t.complex_id = s.complex_id "
                      "WHEN MATCHED THEN UPDATE SET t.sido_code = s.sido_code, "
                      "t.sigungu_code = s.sigungu_code, t.sido_name = s.sido_name")
            spark.catalog.dropTempView(view)
        summary["followed"] = True
        summary["replaced_complexes"] = [{"complex_id": r[0], "sido_code": r[1], "sigungu_code": r[2],
                                          "sido_name": r[3]} for r in replacements]
    Path(args.followed_code_changes_output).write_text(
        json.dumps(followed_rows(changes), ensure_ascii=False) + "\n", encoding="utf-8")
    return summary


def build_spark_session(args: argparse.Namespace) -> SparkSession:
    builder = (
        SparkSession.builder.appName("foundation-platform-industrial-complex-silver-to-gold")
        .config("spark.sql.session.timeZone", "UTC")
        .config(map=SHUFFLE_SETTINGS)
    )

    if args.input_mode == "iceberg" or args.write_mode == "iceberg":
        builder = apply_catalog_settings(builder, args.iceberg_catalog_name)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if args.input_mode == "iceberg" or args.write_mode == "iceberg":
        assert_iceberg_runtime_loaded(spark, args.iceberg_packages)
    return spark



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
        if args.follow_region_codes:
            summary = follow_region_codes(spark, args)
            emit_run_summary(summary, args.summary_output)
            print(f"gold-complex-catalog-region-codes followed={str(summary['followed']).lower()} "
                  f"replaced={len(summary['replaced_complexes'])}")
            return 0
        silver = read_silver_input(spark, args)
        gold = build_gold_catalog_frame(
            silver,
            iceberg_snapshot_id=args.iceberg_snapshot_id,
            published_at_utc=args.published_at_utc,
            boundary_object_key=args.boundary_object_key,
            region=build_region(spark, args),
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

        added_columns: tuple[str, ...] = ()
        committed = None
        if args.write_mode == "parquet":
            write_gold_parquet(gold, args.output)
            persisted = spark.read.parquet(args.output).select(*GOLD_COLUMNS)
            success_target = f"output={args.output}"
            success_label = "gold-complex-catalog-write-ok"
        else:
            added_columns = write_gold_iceberg(spark, gold, args)
            committed = current_snapshot(spark, qualified_target_table(args))
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

        summary = build_run_summary(
            args,
            row_count=row_count,
            persisted_row_count=persisted_count,
            quality_metrics=persisted_quality_metrics,
            source_snapshot_summary=source_snapshot_summary,
            schema_evolution_added_columns=added_columns,
        )
        if committed is not None:
            summary["target"]["snapshot_id"] = committed
        emit_run_summary(summary, args.summary_output)
        emit_lineage_event(build_lineage_event(args, summary), args.lineage_output)
        print(f"{success_label} rows={persisted_count} {success_target}")
        return 0
    finally:
        if "gold" in locals():
            gold.unpersist()
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
