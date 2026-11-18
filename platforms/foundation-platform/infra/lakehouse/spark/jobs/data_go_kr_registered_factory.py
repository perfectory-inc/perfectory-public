"""Load 한국산업단지공단 전국등록공장현황 from Bronze into `silver.registered_factory` (root ADR-0280 §4).

The collector (`collect-data-go-kr-file-data`, root ADR-0401) lands each new edition's zip in Bronze. What
a load owes is decided from R2 alone (`plan-silver-load-objects`): the zips no commit of the table
records yet, fetched into a plan directory of the load's own run. For each, oldest first, this job
reads the data member (the contract's name pattern, CP949, the exact header), refuses an edition
under the contract's row floor or shrunk past its bound against the latest edition the table holds,
gives each row the PNU its address resolves to (`address_pnu`: the 지번 step, then the 도로명 step;
each row names the step or why none), and appends the edition once, in a commit that records the zip.

`parse --zip <file>` reads one zip without Spark and prints what a load would hold. The last stdout
line is `registered-factory-summary-json {json}`, with the match rate per step.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import address_pnu
import code_change_follow as follow
import place_identity_resolver

JOB_NAME = "data_go_kr_registered_factory"
TABLE = "silver.registered_factory"
CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contracts" / "data-go-kr-file-data.contract.json"
DATASET_SLUG = "registered_factory"
PLAN_SCHEMA = "foundation-platform.silver_load_objects.v1"
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load_source_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    """This file's entry of the data.go.kr file-data contract."""

    return {"dataset_slug": DATASET_SLUG, **json.loads(path.read_text(encoding="utf-8"))["files"][DATASET_SLUG]}


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


def factory_rows(rows: Sequence[Sequence[str]], prefixes: Mapping[str, str],
                 dongs: Mapping[tuple[str, str], str]) -> list[dict[str, Any]]:
    """One row per data line (line 2 is the first), with what the address offers the joins.

    ``prefixes`` are ``address_pnu.sido_prefixes`` of the 법정동 code table (root ADR-0403)."""

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


def source_slug(contract: Mapping[str, Any]) -> str:
    """The Bronze source this job loads (root ADR-0014 `{provider}__{dataset}`)."""

    return f"datagokr__{contract['dataset_slug']}"


def planned_objects(plan_dirs: Sequence[Path], contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Every zip the plans fetched, in plan order (oldest first), with its local file."""

    objects = []
    for plan_dir in plan_dirs:
        plan = json.loads((plan_dir / "plan.json").read_text(encoding="utf-8"))
        if plan.get("schema_version") != PLAN_SCHEMA:
            raise ValueError(f"{plan_dir}: not a {PLAN_SCHEMA} plan")
        for entry in plan["objects"]:
            if entry["source_slug"] != source_slug(contract):
                raise ValueError(f"{entry['object_key']}: {entry['source_slug']} is not the source this job loads")
            objects.append({"object_key": entry["object_key"], "path": plan_dir / entry["local_path"]})
    return objects


def latest_edition_rows(spark: Any, table: str) -> int | None:
    """Rows of the newest edition (기준일) the table holds, by the contract's edition order (root
    ADR-0286 §3, ADR-0328: the 기준일, then the load instant); None while it holds none."""

    from platform_contracts import current_rows, load_lakehouse_contract  # noqa: PLC0415

    rows = current_rows(spark.table(table), load_lakehouse_contract(TABLE)).count()
    return rows or None


OFFERED_SCHEMA = ("row_id int, serial_no long, company_name string, complex_name string, "
                  "products string, factory_address_raw string, legal_dong_code string, "
                  "jibun_pnu string, reason string, road_sido string, road_sigungu string, "
                  "road_name string, road_main_no int, road_sub_no int, "
                  "source_line_number int")
RESOLVED = ("pnu", "pnu_basis", "pnu_unresolved_reason", "pnu_candidate_count", "legal_dong_code")
LINE_KEY = ("source_record_id", "source_line_number")


def relinked_offers(rows: Sequence[Mapping[str, Any]], prefixes: Mapping[str, str],
                    dongs: Mapping[tuple[str, str], str]) -> list[dict[str, Any]]:
    """The current rows again with what each kept address offers the joins now (root ADR-0416); the
    values the edition gave stay, and ``row_id`` points back at the row."""

    out = []
    for number, current in enumerate(rows):
        offer = address_pnu.candidates(current["factory_address_raw"] or "", prefixes, dongs)
        road = offer["road"]
        out.append({
            "row_id": number, "serial_no": current["serial_no"], "company_name": current["company_name"],
            "complex_name": current["complex_name"], "products": current["products"],
            "factory_address_raw": current["factory_address_raw"], "legal_dong_code": offer["legal_dong_code"],
            "jibun_pnu": offer["jibun_pnu"], "reason": offer["reason"],
            "road_sido": road.sido_code if road else None, "road_sigungu": road.sigungu if road else None,
            "road_name": road.road if road else None, "road_main_no": road.main_no if road else None,
            "road_sub_no": road.sub_no if road else None, "source_line_number": current["source_line_number"],
        })
    return out


def follow_code_changes(spark: Any, catalog: str, table: str, prefixes: Mapping[str, str],
                        dongs: Mapping[tuple[str, str], str], now: datetime) -> dict[str, Any] | None:
    """Re-link the lines of the served edition the 법정동 code changes since the table's last follow
    can move (root ADR-0329 §3, ADR-0416). None when there is nothing to follow.

    The table reads one whole edition, the newest by (기준일, load instant) (root ADR-0286 §3): a
    reload of a 기준일 outranks its earlier load. So when any line's link moved, the edition is
    appended once more, every line, the moved ones re-linked and the rest as they were, at this
    run's instant. When none moved, only the follow record is written."""

    from lakehouse_ingest import append_source_object, read_ingested_objects, unquoted_table_name  # noqa: PLC0415
    from platform_contracts import column_names, current_rows, load_lakehouse_contract  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415

    after = follow.change_snapshot(spark, catalog)
    if after is None or not spark.catalog.tableExists(unquoted_table_name(table)):
        return None
    before = follow.last_followed(read_ingested_objects(spark, table, unquoted_table_name(table)))
    if before == after:
        return None
    contract = load_lakehouse_contract(TABLE)
    columns = list(column_names(contract))
    moved = follow.changed_prefixes(spark, catalog, before, after, JOB_NAME)
    edition = current_rows(spark.table(table), contract).cache()
    affected = follow.rows_under(edition, moved, ("legal_dong_code", "pnu"), "pnu").cache()
    rows = [r.asDict() for r in affected.collect()]
    record = follow.follow_record(after, now)
    instant = F.lit(now).cast("timestamp")
    relinked_lines = 0
    written = spark.createDataFrame([], schema=spark.table(table).schema).select(*columns)
    if rows:
        kept = spark.createDataFrame(
            [(n, r["source_record_id"], r["source_snapshot_id"]) for n, r in enumerate(rows)],
            schema="row_id int, source_record_id string, source_snapshot_id string")
        offered = spark.createDataFrame(relinked_offers(rows, prefixes, dongs), schema=OFFERED_SCHEMA)
        staged = (address_pnu.resolve(offered, *address_pnu.lakehouse_inputs(spark, catalog),
                                          place_identity_resolver.pairs_from_lakehouse(spark, catalog)[0])
                  .join(kept, "row_id")
                  .withColumn("ingested_at_utc", instant)
                  .select(*columns))
        changed = moved_lines(staged, affected).cache()
        relinked_lines = changed.count()
        if relinked_lines:
            unchanged = (edition.join(changed.select(*LINE_KEY), list(LINE_KEY), "left_anti")
                         .withColumn("ingested_at_utc", instant).select(*columns))
            written = unchanged.unionByName(changed.select(*columns))
    # The record is written even when no line moved: the next follow starts from this snapshot.
    append_source_object(spark, written, columns, table, record, contract_table=TABLE)
    affected.unpersist()
    edition.unpersist()
    return {"from_snapshot": before, "to_snapshot": after, "prefixes": None if moved is None else len(moved),
            "checked": len(rows), "relinked": relinked_lines,
            "appended": edition.count() if relinked_lines else 0}


def moved_lines(staged: Any, current: Any) -> Any:
    """The re-linked lines whose link differs from the line's current row (null-safe)."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    old = current.select(*LINE_KEY, *(F.col(c).alias(f"_old_{c}") for c in RESOLVED))
    joined = staged.join(old, list(LINE_KEY), "left")
    differs = F.lit(False)
    for column in RESOLVED:
        differs = differs | ~F.col(column).eqNullSafe(F.col(f"_old_{column}"))
    return joined.where(differs).select(*staged.columns)


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
    parser.add_argument("--plan-dir", action="append", default=[], required=True,
                        help="A directory `plan-silver-load-objects` wrote (plan.json + objects/); repeatable")
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
    objects = planned_objects([Path(d) for d in args.plan_dir], source)
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
        dongs = address_pnu.lakehouse_legal_dong_index(spark, args.iceberg_catalog_name)
        # Root ADR-0482 §1: a jibun the served edition holds under an earlier form of its code.
        remap, _ambiguous = place_identity_resolver.pairs_from_lakehouse(spark, args.iceberg_catalog_name)
        prefixes = address_pnu.lakehouse_sido_prefixes(spark, args.iceberg_catalog_name)
        for entry in objects:
            snapshot, rows = read_member(entry["path"].read_bytes(), source)
            check_bounds(len(rows), latest_edition_rows(spark, table), source)
            offered = spark.createDataFrame(factory_rows(rows, prefixes, dongs), schema=OFFERED_SCHEMA)
            resolved = (address_pnu.resolve(offered, *address_pnu.lakehouse_inputs(spark, args.iceberg_catalog_name),
                                               remap)
                        .withColumn("source_record_id", F.lit(entry["object_key"]))
                        .withColumn("source_snapshot_id", F.lit(snapshot))
                        .withColumn("ingested_at_utc", F.lit(now).cast("timestamp"))
                        .select(*columns).cache())
            counts = summary_of([row.asDict() for row in resolved.select("pnu_basis", "pnu_unresolved_reason").collect()])
            appended = append_batch_once(spark, resolved, columns, table, TABLE)["appended"]
            resolved.unpersist()
            summary["editions"].append({"object": entry["object_key"], "source_snapshot_id": snapshot,
                                        "appended": appended, **counts})
        summary["follow"] = follow_code_changes(spark, args.iceberg_catalog_name, table, prefixes, dongs, now)
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
