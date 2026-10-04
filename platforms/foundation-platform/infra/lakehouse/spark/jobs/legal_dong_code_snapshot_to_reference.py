#!/usr/bin/env python3
"""Load one snapshot of the official 법정동 code list into `reference.legal_dong_code_snapshot`.

Two inputs, one table:

- `--input-format code-file` (root ADR-0113 §5): the 법정동코드 전체자료 file, code, full name and
  존재/폐지 only.
- `--input-format code-go-kr-html` (root ADR-0143 §2): the code.go.kr 전체 표 response the daily
  collector landed in Bronze, which adds the parent code, 생성일, 폐지일, the lowest name and the
  주민/지적 codes. Its shape is checked against `code-go-kr-legal-dong.contract.json`, and a table
  smaller than the contract's bounds allow (below the floor, or shrunk from the previous code.go.kr
  snapshot in the table) loads nothing and exits non-zero.

One file is one load; loading the same file twice is refused by the ingest registry.
"""

from __future__ import annotations

import argparse
import io
import json
import re
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages
from lakehouse_ingest import append_batch_once
import code_go_kr_legal_dong as cg
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
# Columns only the code.go.kr full table carries; a code-file load leaves them empty.
DATED_COLUMNS = ("parent_cd", "created_date", "abolished_date", "lowest_name", "jumin_cd", "jijuk_cd")
# The Bronze prefix of the collector's full-table objects. The shrink bound compares a code.go.kr
# snapshot only with earlier code.go.kr snapshots: a 전체자료 file lists a different set of codes.
CODE_GO_KR_TABLE_PREFIX = "bronze/source=codegokr__legal_dong_code_table/"


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
            **{column: None for column in DATED_COLUMNS},
        }
        for code, (name, status) in sorted(codes.items())
    ]


def code_go_kr_snapshot_rows(
    text: str, snapshot_date: date, source_record_id: str, now: datetime, contract: dict[str, Any]
) -> list[dict[str, Any]]:
    """The code.go.kr full table as snapshot rows. Raises `SourceFormatError` on any changed shape."""

    return [
        {
            "region_cd": row["region_cd"],
            "full_name": row["full_name"],
            "status": row["status"],
            "snapshot_date": snapshot_date,
            "source_record_id": source_record_id,
            "ingested_at_utc": now,
            **{column: row[column] or None for column in DATED_COLUMNS},
        }
        for row in sorted(cg.parse_full_table_html(text, contract), key=lambda row: row["region_cd"])
    ]


def read_html(path: Path) -> str:
    raw = path.read_bytes()
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise cg.SourceFormatError(f"{path.name} is not UTF-8") from error


def latest_snapshot_row_count(loads: Sequence[tuple[date, datetime, str, int]]) -> int | None:
    """Rows of the latest one of `loads`, each (snapshot date, ingested at, source record id, rows).

    One load is one table: two tables taken the same day are two snapshots, and counting every row
    of that day would double the baseline and refuse every later table as shrunken. The latest is
    the newest date, then the newest load on it, then the greatest record id (the collector's keys
    carry the second they were taken).
    """

    if not loads:
        return None
    return max(loads, key=lambda load: (load[0], load[1], load[2]))[3]


def previous_code_go_kr_row_count(spark, table: str, snapshot_date: date) -> int | None:
    """Rows of the latest earlier code.go.kr snapshot in the table, or None when there is none."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    loads = (
        spark.table(table)
        .filter(F.col("source_record_id").startswith(CODE_GO_KR_TABLE_PREFIX) & (F.col("snapshot_date") < F.lit(snapshot_date)))
        .groupBy("snapshot_date", "source_record_id")
        .agg(F.max("ingested_at_utc").alias("ingested_at_utc"), F.count(F.lit(1)).alias("rows"))
        .collect()
    )
    return latest_snapshot_row_count(
        [(load["snapshot_date"], load["ingested_at_utc"], load["source_record_id"], load["rows"]) for load in loads]
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--input", required=True, help="The 법정동코드 전체자료 (.zip/.txt) or the code.go.kr full table (.html).")
    parser.add_argument("--input-format", choices=("code-file", "code-go-kr-html"), default="code-file")
    parser.add_argument(
        "--previous-row-count",
        type=int,
        help="With --validate-only: the previous code.go.kr snapshot's row count for the shrink bound. "
        "A writing run reads it from the table instead.",
    )
    parser.add_argument("--snapshot-date", required=True, help="YYYY-MM-DD the file was taken.")
    parser.add_argument("--source-record-id", required=True, help="The Bronze object key of the file.")
    parser.add_argument("--summary-output")
    parser.add_argument(
        "--latest-marker-output",
        help="After a code.go.kr snapshot is appended: write which snapshot is now the latest. The hub "
        "exports refuse a crosswalk projection built from an older one (root ADR-0143 §5).",
    )
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
    snapshot_date = date.fromisoformat(args.snapshot_date)
    contract = cg.load_source_contract()
    if args.input_format == "code-go-kr-html":
        if not args.source_record_id.startswith(CODE_GO_KR_TABLE_PREFIX):
            raise ValueError(f"--source-record-id must be the collector's Bronze key under {CODE_GO_KR_TABLE_PREFIX}")
        rows = code_go_kr_snapshot_rows(read_html(Path(args.input)), snapshot_date, args.source_record_id, now, contract)
        if args.validate_only:
            cg.check_table_size(len(rows), args.previous_row_count, contract)
    else:
        rows = snapshot_rows(read_code_file(Path(args.input)), snapshot_date, args.source_record_id, now)
    statuses: dict[str, int] = {}
    for row in rows:
        statuses[row["status"]] = statuses.get(row["status"], 0) + 1
    summary: dict[str, Any] = {
        "job": JOB_NAME,
        "contract": CONTRACT["table_name"],
        "snapshot_date": args.snapshot_date,
        "input_format": args.input_format,
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
            if args.input_format == "code-go-kr-html":
                previous = previous_code_go_kr_row_count(spark, table, snapshot_date)
                summary["previous_row_count"] = previous
                cg.check_table_size(len(rows), previous, contract)
            schema = ", ".join(f"{c['name']} {spark_sql_type(c['logical_type'])}" for c in CONTRACT["columns"])
            frame = spark.createDataFrame(rows, schema=schema).select(*COLUMNS)
            outcome = append_batch_once(spark, frame, COLUMNS, table, CONTRACT["table_name"])
            summary["appended"] = outcome["appended"]
            if args.input_format == "code-go-kr-html" and args.latest_marker_output:
                marker = {"snapshot_date": args.snapshot_date, "source_record_id": args.source_record_id, "row_count": len(rows)}
                Path(args.latest_marker_output).write_text(json.dumps(marker, sort_keys=True) + "\n", encoding="utf-8")
        finally:
            spark.stop()
    if args.summary_output:
        Path(args.summary_output).write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("legal-dong-code-snapshot-summary-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
