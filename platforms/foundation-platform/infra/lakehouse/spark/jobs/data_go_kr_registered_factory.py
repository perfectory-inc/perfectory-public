"""Load 한국산업단지공단 전국등록공장현황 from Bronze into `silver.registered_factory` (root ADR-0280 §4).

The collector (`collect-data-go-kr-registered-factory`) lands each new edition's zip; the collect
script stages it as a handoff (`pending/<run>/manifest.json` + the zip). For each handoff, oldest
first, this job reads the data member (the contract's name pattern, CP949, the exact header), refuses
an edition under the contract's row floor or shrunk past its bound against the last loaded one, gives
each row the PNU its address resolves to (`address_pnu`: the 지번 step, then the 도로명 step; each row
names the step or why none), appends the edition once, and moves the handoff to `loaded/`.

`parse --zip <file>` reads one zip without Spark and prints what a load would hold. The last stdout
line is `registered-factory-summary-json {json}`, with the match rate per step.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import shutil
import sys
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import address_pnu

JOB_NAME = "data_go_kr_registered_factory"
TABLE = "silver.registered_factory"
CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contracts" / "data-go-kr-registered-factory.contract.json"
MANIFEST_SCHEMA = "foundation-platform.data_go_kr_registered_factory_collect.v1"
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load_source_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def read_member(raw: bytes, contract: Mapping[str, Any]) -> tuple[str, list[list[str]]]:
    """(기준일, rows) of the zip's one data member; refuses anything else."""

    member = contract["member"]
    pattern = re.compile(member["name_pattern"])
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        names = [info for info in archive.infolist() if pattern.match(_member_name(info))]
        if len(names) != 1:
            raise ValueError(f"expected one data member matching {member['name_pattern']}, found {len(names)}")
        snapshot = pattern.match(_member_name(names[0])).group(1)
        text = archive.read(names[0]).decode(member["encoding"])  # strict: a byte it cannot read refuses
    rows = list(csv.reader(io.StringIO(text)))
    if not rows or [cell.strip().lstrip("﻿") for cell in rows[0]] != member["header"]:
        raise ValueError(f"the header is not {member['header']}")
    return snapshot, [row for row in rows[1:] if any(cell.strip() for cell in row)]


def _member_name(info: zipfile.ZipInfo) -> str:
    """A member's name as written: zips from Windows tools store UTF-8 names without the flag."""

    if info.flag_bits & 0x800:
        return info.filename
    try:
        return info.filename.encode("cp437").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return info.filename


def check_bounds(rows: int, previous: int | None, contract: Mapping[str, Any]) -> None:
    bounds = contract["member"]["bounds"]
    if rows < bounds["min_rows"]:
        raise ValueError(f"{rows} rows is under the floor of {bounds['min_rows']}")
    if previous and rows < previous * (1 - bounds["max_shrink_share"]):
        raise ValueError(f"{rows} rows shrank past {bounds['max_shrink_share']:.0%} of the last loaded {previous}")


def whole(value: str) -> int | None:
    text = (value or "").strip()
    return int(text) if text.isdigit() else None


def text_or_none(value: str) -> str | None:
    text = (value or "").strip()
    return text or None


def factory_rows(rows: Sequence[Sequence[str]], contract: Mapping[str, Any],
                 dongs: Mapping[tuple[str, str], str]) -> list[dict[str, Any]]:
    """One row per data line (line 2 is the first), with what the address offers the joins."""

    prefixes = contract["pnu_resolution"]["sido_prefixes"]
    out = []
    for number, row in enumerate(rows, start=2):
        cells = list(row) + [""] * (5 - len(row))
        offer = address_pnu.candidates(cells[4], prefixes, dongs)
        road = offer["road"]
        out.append({
            "row_id": number, "serial_no": whole(cells[0]), "company_name": text_or_none(cells[1]),
            "complex_name": text_or_none(cells[2]), "products": text_or_none(cells[3]),
            "factory_address_raw": text_or_none(cells[4]), "legal_dong_code": offer["legal_dong_code"],
            "jibun_pnu": offer["jibun_pnu"], "reason": offer["reason"],
            "road_sido": road.sido_code if road else None, "road_sigungu": road.sigungu if road else None,
            "road_name": road.road if road else None, "road_main_no": road.main_no if road else None,
            "road_sub_no": road.sub_no if road else None, "source_line_number": number,
        })
    return out


def summary_of(resolved: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Rows, rows per step, unresolved reasons, and the share that got a PNU."""

    total = len(resolved)
    basis = Counter(row["pnu_basis"] or "unresolved" for row in resolved)
    reasons = Counter(row["pnu_unresolved_reason"] for row in resolved if row["pnu_unresolved_reason"])
    return {"rows": total, "by_basis": dict(basis), "unresolved_reasons": dict(reasons),
            "match_rate": round((total - basis.get("unresolved", 0)) / total, 4) if total else None}


def pending_handoffs(state_dir: Path) -> list[Path]:
    pending = state_dir / "pending"
    return sorted(path for path in pending.iterdir() if path.is_dir()) if pending.is_dir() else []


def _summary_line(summary: Mapping[str, Any]) -> None:
    print("registered-factory-summary-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True, default=str))


def _parse(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="parse")
    parser.add_argument("--zip", required=True)
    args = parser.parse_args(argv)
    contract = load_source_contract()
    snapshot, rows = read_member(Path(args.zip).read_bytes(), contract)
    _summary_line({"job": JOB_NAME, "status": "parsed", "source_snapshot_id": snapshot, "rows": len(rows)})
    return 0


def _load(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="load")
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--table-suffix", default="")
    parser.add_argument("--allow-non-smoke-write", action="store_true")
    args = parser.parse_args(argv)
    if not IDENTIFIER.fullmatch(args.iceberg_catalog_name):
        parser.error("--iceberg-catalog-name must be a plain SQL identifier")
    if args.table_suffix not in ("", "_smoke"):
        parser.error("--table-suffix is empty or _smoke")
    if not args.table_suffix and not args.allow_non_smoke_write:
        parser.error("writing the Silver table needs --allow-non-smoke-write")

    from lakehouse_engine import apply_catalog_settings, assert_catalog_env  # noqa: PLC0415
    from lakehouse_ingest import append_batch_once  # noqa: PLC0415
    from platform_contracts import (  # noqa: PLC0415
        column_names, create_table_columns_sql, evolve_iceberg_table_to_contract, load_lakehouse_contract,
        partition_clause_sql,
    )
    from pyspark.sql import SparkSession, functions as F  # noqa: PLC0415

    assert_catalog_env()
    source = load_source_contract()
    state_dir = Path(args.state_dir)
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    contract = load_lakehouse_contract(TABLE)
    columns = list(column_names(contract))
    summary: dict[str, Any] = {"job": JOB_NAME, "status": "loaded", "editions": []}
    builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    try:
        table = f"`{args.iceberg_catalog_name}`.`silver`.`registered_factory{args.table_suffix}`"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{args.iceberg_catalog_name}`.`silver`")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({create_table_columns_sql(contract)}) USING iceberg "
                  f"{partition_clause_sql(contract)} TBLPROPERTIES ('format-version' = '2', "
                  "'write.parquet.compression-codec' = 'zstd')")
        evolve_iceberg_table_to_contract(spark, table, contract)
        dongs = address_pnu.legal_dong_index(address_pnu.current_legal_dongs(spark, args.iceberg_catalog_name))
        last_rows_path = state_dir / "last-loaded-rows.json"
        for handoff in pending_handoffs(state_dir):
            manifest = json.loads((handoff / "manifest.json").read_text(encoding="utf-8"))
            if manifest.get("schema_version") != MANIFEST_SCHEMA:
                raise ValueError(f"{handoff}: not a {MANIFEST_SCHEMA} manifest")
            [entry] = manifest["objects"]
            snapshot, rows = read_member((handoff / entry["local_path"]).read_bytes(), source)
            previous = json.loads(last_rows_path.read_text(encoding="utf-8"))["rows"] if last_rows_path.exists() else None
            check_bounds(len(rows), previous, source)
            offered = spark.createDataFrame(factory_rows(rows, source, dongs),
                                            schema=("row_id int, serial_no long, company_name string, complex_name string, "
                                                    "products string, factory_address_raw string, legal_dong_code string, "
                                                    "jibun_pnu string, reason string, road_sido string, road_sigungu string, "
                                                    "road_name string, road_main_no int, road_sub_no int, "
                                                    "source_line_number int"))
            resolved = (address_pnu.resolve(spark, offered, args.iceberg_catalog_name)
                        .withColumn("source_record_id", F.lit(entry["object_key"]))
                        .withColumn("source_snapshot_id", F.lit(snapshot))
                        .withColumn("ingested_at_utc", F.lit(now).cast("timestamp"))
                        .select(*columns).cache())
            counts = summary_of([row.asDict() for row in resolved.select("pnu_basis", "pnu_unresolved_reason").collect()])
            appended = append_batch_once(spark, resolved, columns, table, TABLE)["appended"]
            resolved.unpersist()
            loaded = state_dir / "loaded" / handoff.name
            loaded.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(handoff), str(loaded))
            last_rows_path.write_text(json.dumps({"rows": len(rows), "source_snapshot_id": snapshot}) + "\n", encoding="utf-8")
            summary["editions"].append({"handoff": handoff.name, "source_snapshot_id": snapshot, "appended": appended, **counts})
    finally:
        spark.stop()
    _summary_line(summary)
    return 0


COMMANDS = {"parse": _parse, "load": _load}


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] not in COMMANDS:
        print(f"usage: {Path(__file__).name} {{{','.join(COMMANDS)}}} ...", file=sys.stderr)
        return 2
    return COMMANDS[argv[0]](argv[1:])


if __name__ == "__main__":
    sys.exit(main())
