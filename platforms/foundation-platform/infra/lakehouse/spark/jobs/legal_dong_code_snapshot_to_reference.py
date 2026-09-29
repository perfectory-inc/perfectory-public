#!/usr/bin/env python3
"""Load one snapshot of the official 법정동코드 전체자료 into `reference.legal_dong_code_snapshot`.

Root ADR-0113 §5. The 행정표준코드관리시스템 full file (code, full name, 존재/폐지) carries no dates and
no successor codes, so the only way to see a merger or a split is to keep every snapshot and compare
two of them. One file is one load; loading the same file twice is refused by the ingest registry.
"""

from __future__ import annotations

import argparse
import io
import json
import re
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages
from lakehouse_ingest import append_batch_once
from parcel_lineage import parse_code_list
from platform_contracts import (
    column_names,
    create_table_columns_sql,
    evolve_iceberg_table_to_contract,
    load_lakehouse_contract,
    partition_clause_sql,
    spark_sql_type,
)

JOB_NAME = "legal_dong_code_snapshot_to_reference"
CONTRACT = load_lakehouse_contract("reference.legal_dong_code_snapshot")
COLUMNS: tuple[str, ...] = column_names(CONTRACT)
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def read_code_file(path: Path) -> str:
    """The zip the site serves holds one cp949 text file; a bare .txt is accepted too."""

    raw = path.read_bytes()
    if zipfile.is_zipfile(io.BytesIO(raw)):
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            names = [n for n in archive.namelist() if not n.endswith("/")]
            if len(names) != 1:
                raise ValueError(f"expected one file in the archive, found {len(names)}")
            raw = archive.read(names[0])
    for encoding in ("utf-8", "cp949"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError("the code file is neither UTF-8 nor CP949")


def snapshot_rows(text: str, snapshot_date: date, source_record_id: str, now: datetime) -> list[dict[str, Any]]:
    codes = parse_code_list(text)
    return [
        {
            "region_cd": code,
            "full_name": name,
            "status": status,
            "snapshot_date": snapshot_date,
            "source_record_id": source_record_id,
            "ingested_at_utc": now,
        }
        for code, (name, status) in sorted(codes.items())
    ]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--input", required=True, help="The downloaded 법정동코드 전체자료 (.zip or .txt).")
    parser.add_argument("--snapshot-date", required=True, help="YYYY-MM-DD the file was taken.")
    parser.add_argument("--source-record-id", required=True, help="The Bronze object key of the file.")
    parser.add_argument("--summary-output")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-namespace", default="reference")
    parser.add_argument("--iceberg-table", default="legal_dong_code_snapshot")
    parser.add_argument("--allow-non-smoke-write", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    for label in ("iceberg_catalog_name", "iceberg_namespace", "iceberg_table"):
        if not IDENTIFIER.fullmatch(getattr(args, label)):
            raise ValueError(f"{label} must be a plain SQL identifier")
    date.fromisoformat(args.snapshot_date)
    if not args.source_record_id.strip():
        raise ValueError("--source-record-id is required")
    if args.validate_only:
        return
    if not args.iceberg_table.endswith("_smoke") and not args.allow_non_smoke_write:
        raise ValueError(f"writing {args.iceberg_table} needs --allow-non-smoke-write")
    assert_catalog_env()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_args(args)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    rows = snapshot_rows(read_code_file(Path(args.input)), date.fromisoformat(args.snapshot_date), args.source_record_id, now)
    statuses: dict[str, int] = {}
    for row in rows:
        statuses[row["status"]] = statuses.get(row["status"], 0) + 1
    summary: dict[str, Any] = {
        "job": JOB_NAME,
        "contract": CONTRACT["table_name"],
        "snapshot_date": args.snapshot_date,
        "row_count": len(rows),
        "by_status": statuses,
        "status": "validated" if args.validate_only else "ready",
    }
    if not args.validate_only:
        from pyspark.sql import SparkSession  # noqa: PLC0415

        builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
        spark = apply_catalog_settings(builder, args.iceberg_catalog_name).config("spark.jars.packages", args.iceberg_packages).getOrCreate()
        try:
            table = f"`{args.iceberg_catalog_name}`.`{args.iceberg_namespace}`.`{args.iceberg_table}`"
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{args.iceberg_catalog_name}`.`{args.iceberg_namespace}`")
            spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
{create_table_columns_sql(CONTRACT)}
                )
                USING iceberg
                {partition_clause_sql(CONTRACT)}
                TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
                """
            )
            evolve_iceberg_table_to_contract(spark, table, CONTRACT)
            schema = ", ".join(f"{c['name']} {spark_sql_type(c['logical_type'])}" for c in CONTRACT["columns"])
            frame = spark.createDataFrame(rows, schema=schema).select(*COLUMNS)
            outcome = append_batch_once(spark, frame, COLUMNS, table, CONTRACT["table_name"])
            summary["appended"] = outcome["appended"]
        finally:
            spark.stop()
    if args.summary_output:
        Path(args.summary_output).write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("legal-dong-code-snapshot-summary-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
