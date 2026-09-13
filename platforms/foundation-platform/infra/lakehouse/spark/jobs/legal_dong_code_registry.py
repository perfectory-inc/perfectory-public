#!/usr/bin/env python3
"""Load the authority legal-dong code registry into the temporal dictionary (ADR-0103 ②).

`reference.legal_dong_code` is the versioned record of every 법정동코드 the authority
(행정표준코드, `getStanReginCdList`) has ever issued, with its 생성일 and 말소일. The
resolver reads it instead of any hardcoded code→region fact, so the next merger or split
is absorbed by data, not by an engineer.

The parse and resolve kernels are pure on purpose: the lane that runs
`infra/lakehouse/spark/tests` has no PySpark install, and a module-level import would make
every check that touches this file skip itself. Only `main` touches Spark.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from collections import namedtuple
from pathlib import Path
from typing import Any, Sequence

from lakehouse_engine import apply_catalog_settings, assert_catalog_env, assert_iceberg_runtime_loaded, iceberg_packages
from lakehouse_ingest import append_batch_once
from platform_contracts import (column_names, create_table_columns_sql, evolve_iceberg_table_to_contract,
                                load_lakehouse_contract, partition_clause_sql)

CONTRACT = "reference.legal_dong_code"
CROSSWALK_CONTRACT = "reference.sigungu_canonical_crosswalk"

# One authority row, already split into fields, in this order. The collector extracts them
# from the `getStanReginCdList` payload; this order is the only wire contract between the
# extract and the parse, so a reordered extract fails loudly on the digit checks below
# rather than landing names in code columns.
REGISTRY_FIELDS = (
    "region_cd",
    "sido_cd",
    "sgg_cd",
    "umd_cd",
    "ri_cd",
    "locatadd_nm",
    "locathigh_cd",
    "created_date",
    "abolished_date",
)

REGION_CD_PATTERN = re.compile(r"[0-9]{10}")
DATE_PATTERN = re.compile(r"[0-9]{8}")

SigunguResolution = namedtuple("SigunguResolution", ("canonical", "unresolved"))


def parse_registry_row(fields: Sequence[str]) -> dict[str, Any]:
    """Turn one authority registry row into a contract row.

    `is_current` is derived, not carried: the authority states currency by leaving 말소일
    empty, and deriving it here keeps the rule in one place. A malformed code or date is
    refused rather than repaired — a fabricated registry entry would later resolve real
    parcels onto a place that does not exist.
    """
    if len(fields) != len(REGISTRY_FIELDS):
        raise ValueError(
            f"registry row carries {len(fields)} fields; expected "
            f"{len(REGISTRY_FIELDS)} ({', '.join(REGISTRY_FIELDS)})"
        )
    row: dict[str, Any] = {
        name: ("" if value is None else str(value).strip())
        for name, value in zip(REGISTRY_FIELDS, fields)
    }
    if not REGION_CD_PATTERN.fullmatch(row["region_cd"]):
        raise ValueError(f"region_cd must be a 10-digit 법정동코드: {row['region_cd']!r}")
    if not DATE_PATTERN.fullmatch(row["created_date"]):
        raise ValueError(f"created_date must be YYYYMMDD: {row['created_date']!r}")
    if row["abolished_date"] and not DATE_PATTERN.fullmatch(row["abolished_date"]):
        raise ValueError(f"abolished_date must be empty or YYYYMMDD: {row['abolished_date']!r}")
    row["is_current"] = row["abolished_date"] == ""
    return row


def resolve_sigungu(code: str, as_of: str, crosswalk: Sequence[dict[str, Any]]) -> SigunguResolution:
    """Resolve one source 시군구 code through the crosswalk at a point in time.

    ``crosswalk`` rows are shaped like ``reference.sigungu_canonical_crosswalk``:
    ``source_code``, ``canonical_code``, ``valid_from``, ``valid_to`` (empty = open).
    A code the crosswalk does not name at ``as_of`` comes back ``unresolved`` — never a
    fabricated canonical code — so the caller quarantines it instead of misjoining
    (ADR-0103 ④). Two live rows naming different canonicals is a broken dictionary and is
    refused outright rather than picked from.
    """
    code = ("" if code is None else str(code).strip())
    as_of = ("" if as_of is None else str(as_of).strip())
    if not DATE_PATTERN.fullmatch(as_of):
        raise ValueError(f"as_of must be YYYYMMDD: {as_of!r}")
    canonicals = {
        row["canonical_code"]
        for row in crosswalk
        if row["source_code"] == code
        and (row.get("valid_from") or "") <= as_of
        and (not row.get("valid_to") or as_of <= row["valid_to"])
    }
    if len(canonicals) > 1:
        raise ValueError(
            f"crosswalk names {code} to multiple canonical codes at {as_of}: {sorted(canonicals)}"
        )
    if not canonicals:
        return SigunguResolution(canonical=None, unresolved=True)
    return SigunguResolution(canonical=canonicals.pop(), unresolved=False)


def crosswalk_rows_from_seed(seed: dict[str, Any]) -> list[dict[str, str]]:
    """Flatten the checked-in seed json into crosswalk table rows.

    The seed links each authority-current 시군구 code (12xxx) to the superseded cadastral
    code (29xxx/46xxx) the parcel map still carries, so the two sources meet on one place.
    The link opens at the merger date the seed's sido entry states and stays open until the
    authority says otherwise; per-entry provenance survives so the steward can later confirm
    or replace each pair against the authority's 생성/말소 file.
    """
    opened = {
        entry["current_code"]: entry.get("effective_from", "")
        for entry in seed.get("sido", [])
    }
    return [
        {
            "source_code": entry["current_code"],
            "canonical_code": entry["superseded_code"],
            "valid_from": opened.get(entry["current_code"][:2], ""),
            "valid_to": "",
            "provenance": entry["provenance"],
        }
        for entry in seed["sigungu"]
    ]


def derive_crosswalk_from_registry(rows: Sequence[dict[str, Any]]) -> list[dict[str, str]]:
    """Derive the 시군구 crosswalk from the authority registry alone, no external pairing file.

    A merger abolishes each old 시군구 code and creates a new one that keeps the same
    시군구 name on the same date (the authority stamps both the 말소일 and the 생성일). So a
    superseded code and a freshly-created code that share a 시군구 name at one date are the
    same place, and we link current -> superseded (the direction the parcel map still carries).
    This reproduces the checked-in seed from the authority's 생성/말소 dates alone (ADR-0103 ③).

    Only an unambiguous 1:1 name match at one date is emitted. A renamed 시군구 (no name match)
    or a many-to-one merger (several old codes onto one name) is left out on purpose so the
    steward decides it instead of the code guessing (ADR-0103 ④).
    """

    def is_sigungu(region_cd: str) -> bool:
        return len(region_cd) == 10 and region_cd[5:] == "00000"

    def sigungu_name(locatadd_nm: str) -> str:
        parts = (locatadd_nm or "").split()
        return parts[-1] if parts else ""

    created: dict[tuple[str, str], list[str]] = {}
    abolished: dict[tuple[str, str], list[str]] = {}
    for row in rows:
        region_cd = str(row.get("region_cd", "") or "")
        if not is_sigungu(region_cd):
            continue
        name = sigungu_name(row.get("locatadd_nm", ""))
        if not name:
            continue
        abolished_date = (row.get("abolished_date") or "").strip()
        created_date = (row.get("created_date") or "").strip()
        if abolished_date:
            abolished.setdefault((name, abolished_date), []).append(region_cd)
        elif created_date:
            created.setdefault((name, created_date), []).append(region_cd)

    crosswalk: list[dict[str, str]] = []
    for (name, date), old_codes in sorted(abolished.items()):
        new_codes = created.get((name, date), [])
        if len(old_codes) != 1 or len(new_codes) != 1:
            continue  # ambiguous (rename / many-to-one) -> steward, not a guess
        crosswalk.append(
            {
                "source_code": new_codes[0][:5],
                "canonical_code": old_codes[0][:5],
                "valid_from": date,
                "valid_to": "",
                "provenance": f"derived:registry:sigungu-name+date:{name}:{date}",
            }
        )
    return crosswalk


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True,
                        help="Authority 전체자료 extract: one delimited row per line, fields in REGISTRY_FIELDS order.")
    parser.add_argument("--delimiter", default="\t")
    parser.add_argument("--source-snapshot-id", required=True)
    parser.add_argument("--iceberg-catalog-name", default="r2")
    parser.add_argument("--summary-output")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.iceberg_catalog_name):
        parser.error("invalid catalog")
    # The snapshot id becomes the batch's source_record_id, and the ingest registry
    # separates object names with a comma inside one snapshot-summary value.
    if not args.source_snapshot_id.strip() or "," in args.source_snapshot_id:
        parser.error("invalid source snapshot id")
    return args


def main(argv=None):
    args = parse_args(argv)
    from pyspark.sql import SparkSession, types as T

    assert_catalog_env()
    started = time.monotonic()
    source_record = f"mois/legal-dong-code/{args.source_snapshot_id}"
    rows = []
    for line in Path(args.input).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = parse_registry_row(line.split(args.delimiter))
        row["source_snapshot_id"] = args.source_snapshot_id
        row["source_record_id"] = source_record
        rows.append(row)
    if not rows:
        raise ValueError("registry extract produced no rows; refusing an empty append")

    builder = (SparkSession.builder.appName("foundation-platform-legal-dong-code-registry")
               .config("spark.sql.session.timeZone", "UTC"))
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    try:
        assert_iceberg_runtime_loaded(spark, iceberg_packages())
        contract = load_lakehouse_contract(CONTRACT)
        names = column_names(contract)
        # An explicit schema rather than inference: a registry column that happens to be
        # empty on every row would otherwise fail type inference.
        field_types = {"string": T.StringType(), "boolean": T.BooleanType()}
        schema = T.StructType([
            T.StructField(column["name"], field_types[column["logical_type"]], not column["required"])
            for column in contract["columns"]
        ])
        frame = spark.createDataFrame([tuple(row[name] for name in names) for row in rows], schema=schema)
        prefix = f"`{args.iceberg_catalog_name}`.`reference`"
        target = f"{prefix}.`legal_dong_code`"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {prefix}")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({create_table_columns_sql(contract)}) USING iceberg {partition_clause_sql(contract)}")
        evolve_iceberg_table_to_contract(spark, target, contract)
        appended = append_batch_once(spark, frame, names, target, CONTRACT)
        current_rows = sum(1 for row in rows if row["is_current"])
        # Auto-derive the 시군구 crosswalk from this snapshot's 생성/말소 dates so the operator
        # sees how many old->new pairs the authority data yields on its own, with no external
        # pairing file (ADR-0103 ③). Persisting the derived rows to reference.sigungu_canonical_crosswalk
        # is the next step; ambiguous mergers are already excluded here for the steward (ADR-0103 ④).
        derived_crosswalk = derive_crosswalk_from_registry(rows)
        summary = {
            "source_snapshot_id": args.source_snapshot_id,
            "rows": len(rows), "current_rows": current_rows,
            "abolished_rows": len(rows) - current_rows,
            "derived_crosswalk_pairs": len(derived_crosswalk),
            "elapsed_seconds": time.monotonic() - started,
            **appended,
        }
        text = json.dumps(summary, ensure_ascii=False, indent=2) + "\n"
        if args.summary_output:
            Path(args.summary_output).write_text(text, encoding="utf-8")
        print(text, flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
