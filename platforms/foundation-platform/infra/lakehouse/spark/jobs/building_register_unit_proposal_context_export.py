#!/usr/bin/env python3
"""Export AI proposal context packs from Silver building-register units.

The canonical Silver table is the source of truth. This job only creates
proposal input artifacts for intelligence-platform workers; it never writes
canonical data and never bypasses the foundation-platform proposal inbox.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from silver_scalar_handoff_to_lakehouse import (
    DEFAULT_ICEBERG_PACKAGES,
    assert_iceberg_runtime_loaded,
    lakehouse_oauth2_server_uri,
    require_env,
)


SCHEMA_VERSION = "foundation-platform.unit_entity_context_pack.v1"
SUMMARY_SCHEMA_VERSION = (
    "foundation-platform.building_register_unit_proposal_context_export_summary.v1"
)
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def qualified_table(catalog: str, namespace: str, table: str) -> str:
    return f"`{catalog}`.`{namespace}`.`{table}`"


def validate_args(args: Any) -> None:
    validate_identifier("catalog", args.catalog)
    validate_identifier("namespace", args.namespace)
    validate_identifier("table", args.table)
    if args.input_parquet is not None and str(args.input_parquet).strip() == "":
        raise ValueError("--input-parquet must not be empty")
    if args.output is None or str(args.output).strip() == "":
        raise ValueError("--output must not be empty")
    if int(args.output_partitions) < 1:
        raise ValueError("output partitions must be greater than zero")
    if args.expected_proposal_count is not None and int(args.expected_proposal_count) < 0:
        raise ValueError("--expected-proposal-count must be zero or greater")
    if args.input_parquet is not None:
        return
    for name in required_iceberg_env():
        require_env(name)


def validate_identifier(label: str, value: str) -> None:
    if IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{label} must be a simple identifier: {value}")


def required_iceberg_env() -> tuple[str, ...]:
    return (
        "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI",
        "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE",
        "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN",
    )


def build_proposal_source_sql(source_table: str) -> str:
    return f"""
WITH units AS (
    SELECT
        *,
        concat_ws(
            '|',
            register_parcel_key,
            coalesce(dong_join_name, ''),
            coalesce(cast(floor_index AS string), '')
        ) AS __scope_key,
        concat_ws(
            '|',
            register_parcel_key,
            coalesce(building_mgm_bldrgst_pk, ''),
            coalesce(dong_join_name, '')
        ) AS __building_key
    FROM {source_table}
),
scope_stats AS (
    SELECT
        __scope_key,
        count(*) AS accepted_unit_count,
        min(unit_number) AS min_unit_number,
        max(unit_number) AS max_unit_number,
        count(DISTINCT unit_number) AS distinct_unit_number_count
    FROM units
    WHERE normalization_status = 'accepted'
      AND unit_number IS NOT NULL
    GROUP BY __scope_key
),
building_stats AS (
    SELECT
        __building_key,
        count(*) AS same_building_accepted_unit_count
    FROM units
    WHERE normalization_status = 'accepted'
      AND unit_number IS NOT NULL
    GROUP BY __building_key
),
building_row_stats AS (
    SELECT
        building_mgm_bldrgst_pk,
        count(*) AS building_row_total,
        sum(CASE WHEN unit_name_raw IS NULL OR trim(unit_name_raw) = '' THEN 1 ELSE 0 END)
            AS building_empty_row_total
    FROM units
    WHERE building_mgm_bldrgst_pk IS NOT NULL
    GROUP BY building_mgm_bldrgst_pk
),
accepted_scope_examples AS (
    SELECT
        __scope_key,
        collect_list(named_struct(
            'floor_index', floor_index,
            'unit_number', unit_number,
            'unit_name_raw', unit_name_raw
        )) AS neighbor_unit_examples
    FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY __scope_key
                ORDER BY unit_number, unit_row_id
            ) AS __example_rank
        FROM units
        WHERE normalization_status = 'accepted'
          AND unit_number IS NOT NULL
    )
    WHERE __example_rank <= 5
    GROUP BY __scope_key
),
proposal_rows AS (
    SELECT *
    FROM units
    WHERE normalization_status = 'proposal_required'
      AND normalization_application_id IS NULL
)
SELECT
    p.*,
    coalesce(s.accepted_unit_count, 0) AS accepted_unit_count,
    s.min_unit_number,
    s.max_unit_number,
    coalesce(s.distinct_unit_number_count, 0) AS distinct_unit_number_count,
    coalesce(b.same_building_accepted_unit_count, 0) AS same_building_accepted_unit_count,
    r.building_row_total,
    r.building_empty_row_total,
    coalesce(e.neighbor_unit_examples, array()) AS neighbor_unit_examples
FROM proposal_rows p
LEFT JOIN scope_stats s
  ON p.__scope_key = s.__scope_key
LEFT JOIN building_stats b
  ON p.__building_key = b.__building_key
LEFT JOIN building_row_stats r
  ON p.building_mgm_bldrgst_pk = r.building_mgm_bldrgst_pk
LEFT JOIN accepted_scope_examples e
  ON p.__scope_key = e.__scope_key
"""


def build_context_pack_value(row: dict[str, Any], scope_summary: dict[str, Any]) -> dict[str, Any]:
    context_pack_seed = (
        f"{SCHEMA_VERSION}:{row['unit_row_id']}:{row['row_checksum_sha256']}"
    )
    second_pass_decision = classify_second_pass_decision(
        unit_name_raw=row.get("unit_name_raw"),
        accepted_unit_count=scope_summary["accepted_unit_count"],
        building_main_or_annex=row.get("building_main_or_annex"),
        building_title_unit_count=row.get("building_title_unit_count"),
        building_row_total=row.get("building_row_total"),
        building_empty_row_total=row.get("building_empty_row_total"),
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "context_pack_id": f"unit-context-pack:{sha256_hex(context_pack_seed)}",
        "source_system": "foundation-platform.silver.building_register_units",
        "target": {
            "target_kind": "building_register_unit",
            "silver_row_id": row["unit_row_id"],
            "bronze_object_key": row["bronze_object_key"],
            "row_checksum_sha256": row["row_checksum_sha256"],
            "source_snapshot_id": row["source_snapshot_id"],
            "source_line_number": row.get("source_line_number"),
        },
        "unit_identity_candidate": {
            "mgm_bldrgst_pk": row["mgm_bldrgst_pk"],
            "pnu": row["pnu"],
            "dong_join_name": row.get("dong_join_name"),
            "dong_name_raw": row["dong_name_raw"],
            "unit_name_raw": row["unit_name_raw"],
            "unit_number": row.get("unit_number"),
            "unit_label_ko": row.get("unit_label_ko"),
            "floor_kind": row["floor_kind"],
            "floor_index": row.get("floor_index"),
            "floor_number": row.get("floor_number"),
            "building_mgm_bldrgst_pk": row.get("building_mgm_bldrgst_pk"),
            "building_link_method": row["building_link_method"],
        },
        "current_deterministic_normalization": {
            "status": row["normalization_status"],
            "reason": row["normalization_reason"],
            "unit_number": row.get("unit_number"),
            "unit_label_ko": row.get("unit_label_ko"),
            "building_mgm_bldrgst_pk": row.get("building_mgm_bldrgst_pk"),
            "building_link_method": row["building_link_method"],
        },
        "same_scope_unit_summary": {
            "scope_key": scope_summary["scope_key"],
            "accepted_unit_count": scope_summary["accepted_unit_count"],
            "min_unit_number": scope_summary.get("min_unit_number"),
            "max_unit_number": scope_summary.get("max_unit_number"),
            "distinct_unit_number_count": scope_summary["distinct_unit_number_count"],
        },
        "entity_context": {
            "entity_context_key": entity_context_key(row),
            "same_scope_accepted_unit_count": scope_summary["accepted_unit_count"],
            "same_building_accepted_unit_count": int(
                scope_summary.get("same_building_accepted_unit_count") or 0
            ),
            "neighbor_unit_examples": scope_summary.get("neighbor_unit_examples") or [],
            "conflict_flags": [],
        },
        "second_pass_decision": second_pass_decision,
        "policy_context": {
            "policy_id": "foundation-platform.unit-normalization",
            "policy_version": "v1",
            "default_locale": "ko-KR",
            "machine_values_language": "en-US",
            "ai_role": "proposal_only",
            "decision_owner": "foundation-platform",
            "canonical_write_path": "proposal_inbox_human_review_then_command",
        },
        "allowed_output_contract": {
            "required_locale": "ko-KR",
            "machine_fields": [
                "unit_number",
                "unit_label_ko",
                "building_mgm_bldrgst_pk",
                "building_link_method",
                "normalization_status",
                "normalization_reason",
            ],
            "localized_fields": [
                "review_message_ko",
            ],
        },
        "trace": {
            "valid_from_utc": row["valid_from_utc"],
            "ingested_at_utc": row["ingested_at_utc"],
        },
    }


def entity_context_key(row: dict[str, Any]) -> str:
    # 내부 엔티티 키는 register_parcel_key 기반 — 블록 필지도 키가 끊기지 않는다 (ADR 0023).
    return "|".join(
        [
            str(row["register_parcel_key"]),
            str(row.get("building_mgm_bldrgst_pk") or ""),
            str(row.get("dong_join_name") or ""),
            "" if row.get("floor_index") is None else str(row.get("floor_index")),
        ]
    )


def classify_second_pass_decision(
    *,
    unit_name_raw: Any,
    accepted_unit_count: int,
    building_main_or_annex: Any = None,
    building_title_unit_count: Any = None,
    building_row_total: Any = None,
    building_empty_row_total: Any = None,
) -> dict[str, Any]:
    unit_name = str(unit_name_raw or "").strip()
    has_scope_sequence = accepted_unit_count > 0
    has_digit = any(ch.isdigit() for ch in unit_name)

    # The title-register second pass uses the title card to settle empty-name
    # groups without guessing.
    if unit_name == "":
        title_reason = classify_empty_name_with_title_evidence(
            main_or_annex=building_main_or_annex,
            title_unit_count=building_title_unit_count,
            row_total=building_row_total,
            empty_row_total=building_empty_row_total,
        )
        if title_reason is not None:
            return {
                "status": "manual_review_required",
                "reason": title_reason,
                "ai_required": False,
            }

    if has_scope_sequence and has_digit:
        return {
            "status": "ai_required",
            "reason": "numeric_unit_name_with_context",
            "ai_required": True,
        }

    if not has_scope_sequence and has_digit:
        reason = "no_scope_sequence"
    elif unit_name == "" and not has_scope_sequence:
        reason = "empty_unit_name_and_no_scope_sequence"
    elif unit_name == "" and has_scope_sequence:
        reason = "scope_sequence_but_empty_unit_name"
    elif not has_scope_sequence:
        reason = "no_scope_sequence_and_no_numeric_unit_name"
    else:
        reason = "scope_sequence_but_no_numeric_unit_name"

    return {
        "status": "manual_review_required",
        "reason": reason,
        "ai_required": False,
    }


def classify_empty_name_with_title_evidence(
    *, main_or_annex: Any, title_unit_count: Any, row_total: Any, empty_row_total: Any
) -> str | None:
    if not isinstance(row_total, int):
        return None
    # Sole empty row of an annex building (관리동/경비실/주차장동) whose card also
    # says zero units. Annex cards claiming units stay in the baseline queue —
    # the card is supporting evidence, not authoritative truth.
    if row_total == 1 and main_or_annex == "부속건축물" and title_unit_count == 0:
        return "non_unit_annex_building_row"
    # Sole empty row of a main building whose card says exactly one unit.
    if row_total == 1 and main_or_annex == "주건축물" and title_unit_count == 1:
        return "single_unit_building_candidate"
    # Whole building is unnamed rows and the card count matches exactly.
    if (
        isinstance(empty_row_total, int)
        and row_total >= 2
        and empty_row_total == row_total
        and title_unit_count == row_total
    ):
        return "unnamed_units_count_confirmed"
    return None


def build_run_summary(
    *,
    output_path: str,
    proposal_count: int,
    source_table: str,
) -> dict[str, Any]:
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "job_name": "building_register_unit_proposal_context_export",
        "created_at_utc": datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "source_table": source_table,
        "target": {
            "kind": "ai_proposal_input_jsonl",
            "path": output_path,
        },
        "proposal_count": proposal_count,
        "canonical_write": False,
    }


def row_to_context_json_line(row: Any) -> str:
    try:
        values = row.asDict(recursive=True)
    except TypeError:
        values = row.asDict()
    values = {key: json_safe_value(value) for key, value in values.items()}
    scope_summary = {
        "scope_key": values["__scope_key"],
        "accepted_unit_count": int(values.get("accepted_unit_count") or 0),
        "min_unit_number": values.get("min_unit_number"),
        "max_unit_number": values.get("max_unit_number"),
        "distinct_unit_number_count": int(values.get("distinct_unit_number_count") or 0),
        "same_building_accepted_unit_count": int(
            values.get("same_building_accepted_unit_count") or 0
        ),
        "neighbor_unit_examples": values.get("neighbor_unit_examples") or [],
    }
    payload = build_context_pack_value(values, scope_summary)
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def json_safe_value(value: Any) -> Any:
    if isinstance(value, datetime):
        normalized = value
        if normalized.tzinfo is None:
            normalized = normalized.replace(tzinfo=timezone.utc)
        return normalized.isoformat(timespec="seconds").replace("+00:00", "Z")
    if isinstance(value, list):
        return [json_safe_value(item) for item in value]
    if hasattr(value, "asDict"):
        return {
            key: json_safe_value(item)
            for key, item in value.asDict(recursive=True).items()
        }
    return value


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def parse_args(argv: list[str] | None = None) -> Any:
    import argparse

    parser = argparse.ArgumentParser(
        description="Export AI proposal context packs from Silver building-register units."
    )
    parser.add_argument(
        "--catalog",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_CATALOG_NAME", "r2"),
        help="Spark Iceberg catalog name.",
    )
    parser.add_argument("--namespace", default="silver", help="Iceberg namespace.")
    parser.add_argument(
        "--table",
        default="building_register_units",
        help="Silver unit table name.",
    )
    parser.add_argument(
        "--input-parquet",
        default=None,
        help=(
            "Optional Silver handoff Parquet directory. When set, this job reads "
            "the Parquet handoff instead of the Iceberg catalog table."
        ),
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output directory for proposal JSONL part files.",
    )
    parser.add_argument(
        "--summary-output",
        help="Optional machine-readable summary JSON path.",
    )
    parser.add_argument(
        "--output-partitions",
        type=int,
        default=1,
        help="Number of JSONL output part files.",
    )
    parser.add_argument(
        "--expected-proposal-count",
        type=int,
        default=None,
        help="Optional proposal row-count assertion.",
    )
    parser.add_argument(
        "--iceberg-packages",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_PACKAGES", DEFAULT_ICEBERG_PACKAGES),
        help="Comma-separated Iceberg Spark packages used for REST catalog reads.",
    )
    return parser.parse_args(argv)


def load_pyspark() -> tuple[Any, Any]:
    from pyspark.sql import SparkSession

    return SparkSession, None


def build_spark_session(args: Any, SparkSession: Any) -> Any:
    if args.input_parquet is not None:
        spark = (
            SparkSession.builder.appName(
                "foundation-platform-building-register-unit-proposal-context-export"
            )
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        return spark

    catalog_uri = require_env("FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI")
    catalog = args.catalog
    builder = (
        SparkSession.builder.appName(
            "foundation-platform-building-register-unit-proposal-context-export"
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config(
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
    assert_iceberg_runtime_loaded(spark, args.iceberg_packages)
    return spark


def remove_existing_output(path: str) -> None:
    target = Path(path)
    if target.is_dir():
        shutil.rmtree(target)
    elif target.exists():
        target.unlink()


def write_summary(summary: dict[str, Any], output_path: str | None) -> None:
    payload = json.dumps(summary, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{payload}\n", encoding="utf-8")
    print(f"building-register-unit-proposal-context-summary-json {payload}")


def read_source_frame(spark: Any, args: Any) -> tuple[Any, str]:
    if args.input_parquet is not None:
        return (
            spark.read.parquet(args.input_parquet),
            f"silver_handoff_parquet:{args.input_parquet}",
        )
    source_table = qualified_table(args.catalog, args.namespace, args.table)
    return spark.sql(f"SELECT * FROM {source_table}"), f"{args.catalog}.{args.namespace}.{args.table}"


def run(args: Any) -> int:
    validate_args(args)
    SparkSession, _ = load_pyspark()
    spark = build_spark_session(args, SparkSession)
    try:
        source_frame, source_label = read_source_frame(spark, args)
        # 부분 이행 가드: register_parcel_key가 NULL인 행이 있으면 scope 키가
        # 조용히 붕괴하므로(concat_ws의 NULL 스킵) 시끄럽게 실패한다 (ADR 0023).
        null_key_rows = source_frame.where("register_parcel_key IS NULL").limit(1).count()
        if null_key_rows > 0:
            raise ValueError(
                "source rows are missing register_parcel_key — rerun the full "
                "silver overwrite before exporting proposal context"
            )
        source_frame.createOrReplaceTempView("__building_register_unit_proposal_source")
        proposal_frame = spark.sql(
            build_proposal_source_sql("__building_register_unit_proposal_source")
        )
        proposal_count = proposal_frame.count()
        if (
            args.expected_proposal_count is not None
            and proposal_count != args.expected_proposal_count
        ):
            raise ValueError(
                "Expected "
                f"{args.expected_proposal_count} proposal rows, found {proposal_count}"
            )
        remove_existing_output(args.output)
        (
            proposal_frame.rdd.map(row_to_context_json_line)
            .coalesce(args.output_partitions)
            .saveAsTextFile(args.output)
        )
        summary = build_run_summary(
            output_path=args.output,
            proposal_count=proposal_count,
            source_table=source_label,
        )
        write_summary(summary, args.summary_output)
        print(
            "building-register-unit-proposal-context-export-ok "
            f"proposals={proposal_count} output={args.output}"
        )
        return 0
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
