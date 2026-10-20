"""Load 법원경매정보 list rows from Bronze into Silver (root ADR-0280, ADR-0281).

The collector (`collect-courtauction-go-kr`) lands, per night, one gzip JSON Lines object per search;
each line is one page request with the response body as received. What a load owes is decided from
R2 alone (`plan-silver-load-objects`): the objects of each search that no commit of its table records
yet, fetched into a plan directory of the load's own run. For each object, oldest first, this job:

- reads the rows (`data.dlt_srchResult`) of every 200 answer, one row per 물건 (court office, case,
  property and item sequence), the later answer winning when the list shifted between pages;
- gives each row the PNU its address resolves to (`address_pnu`): the 행정구역 names with the
  representative lot as a jibun address, else the road name with the building number;
- appends to `silver.court_auction_item` the rows whose content changed and, only when the night read
  the item search completely, a removal marker for each 물건 that left it (root ADR-0275); to
  `silver.court_auction_sale_result` the rows whose content changed, never a marker; whether the
  night read the item search completely is read from the lines themselves (every condition read to
  its last page);
- commits each object on its own, empty if need be, so the table records it.

`parse` reads the plan directories without Spark. The last stdout line is `court-auction-summary-json {json}`.
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import address_pnu

JOB_NAME = "courtauction_go_kr"
ITEM_TABLE = "silver.court_auction_item"
RESULT_TABLE = "silver.court_auction_sale_result"
CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"
CONTRACT_PATH = CONTRACTS / "courtauction-go-kr.contract.json"
PLAN_SCHEMA = "foundation-platform.silver_load_objects.v1"
SNAPSHOT_ID = re.compile(r"-(\d{8}t\d{6}z)\.gz$")
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
KEY = ("court_office_code", "case_no", "property_seq", "item_seq")
LOAD_COLUMNS = ("source_record_id", "source_line_number", "source_snapshot_id", "ingested_at_utc", "row_state")
ROLE_TABLE = {"item_search": ITEM_TABLE, "sale_result_search": RESULT_TABLE}


def load_source_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def sido_prefixes(contract: Mapping[str, Any]) -> dict[str, str]:
    """The 시도 prefixes the contract points at (one copy, the registered-factory contract's)."""

    file, pointer = contract["pnu_resolution"]["sido_prefixes_from"].split("#", 1)
    value: Any = json.loads((CONTRACTS / file).read_text(encoding="utf-8"))
    for part in pointer.strip("/").split("/"):
        value = value[part]
    return dict(value)


def text_or_none(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    return text or None


def whole(value: Any) -> int | None:
    text = str(value or "").strip().replace(",", "")
    return int(text) if re.fullmatch(r"-?\d+", text) else None


def decimal(value: Any) -> float | None:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def ymd(value: Any) -> date | None:
    text = str(value or "").strip()
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError:
        return None


def item_row(item: Mapping[str, Any]) -> dict[str, Any]:
    sold = whole(item.get("maeAmt"))
    return {
        "court_office_code": str(item["boCd"]).strip(), "case_no": str(item["saNo"]).strip(),
        "property_seq": whole(item["maemulSer"]), "item_seq": whole(item["mokmulSer"]),
        "user_case_no": text_or_none(item.get("srnSaNo")), "court_name": text_or_none(item.get("jiwonNm")),
        "department_name": text_or_none(item.get("jpDeptNm")),
        "appraisal_amount_krw": whole(item.get("gamevalAmt")), "minimum_bid_price_krw": whole(item.get("minmaePrice")),
        "sale_date": ymd(item.get("maeGiil")), "sale_time_raw": text_or_none(item.get("maeHh1")),
        "sale_decision_date": ymd(item.get("maegyuljGiil")), "sale_place": text_or_none(item.get("maePlace")),
        "failed_bid_count": whole(item.get("yuchalCnt")),
        # The site writes 0 for an unsold item.
        "sale_amount_krw": sold if sold else None,
        "case_status_code": text_or_none(item.get("jinstatCd")), "item_status_code": text_or_none(item.get("mulStatcd")),
        "item_in_progress_yn": text_or_none(item.get("mulJinYn")),
        "usage_lcls_code": text_or_none(item.get("lclsUtilCd")), "usage_mcls_code": text_or_none(item.get("mclsUtilCd")),
        "usage_scls_code": text_or_none(item.get("sclsUtilCd")),
        "address_sido_name": text_or_none(item.get("hjguSido")), "address_sigungu_name": text_or_none(item.get("hjguSigu")),
        "address_dong_name": text_or_none(item.get("hjguDong")), "address_ri_name": text_or_none(item.get("hjguRd")),
        "representative_lot_raw": text_or_none(item.get("daepyoLotno")), "road_name": text_or_none(item.get("rdNm")),
        "building_no_raw": text_or_none(item.get("buldNo")), "building_name": text_or_none(item.get("buldNm")),
        "print_address_raw": text_or_none(item.get("printSt")),
        "wgs84_x": decimal(item.get("wgs84Xcordi")), "wgs84_y": decimal(item.get("wgs84Ycordi")),
    }


def offer(row: Mapping[str, Any], prefixes: Mapping[str, str], dongs: Mapping[tuple[str, str], str]) -> dict[str, Any]:
    """What a row's address offers the joins: the names with the lot as a jibun address, else the
    road name with the building number."""

    names = [row[k] for k in ("address_sido_name", "address_sigungu_name", "address_dong_name", "address_ri_name") if row[k]]
    jibun = address_pnu.candidates(" ".join([*names, row["representative_lot_raw"] or ""]), prefixes, dongs)
    if jibun["jibun_pnu"] or jibun["reason"] == "no_legal_dong" or not row["road_name"]:
        return jibun
    road = " ".join(n for n in (*names[:2], row["road_name"], row["building_no_raw"] or "") if n)
    return address_pnu.candidates(road, prefixes, dongs)


def answer_rows(line: Mapping[str, Any]) -> list[dict[str, Any]]:
    if line.get("http_status") != 200:
        return []
    try:
        body = json.loads(line["body"])
    except (TypeError, ValueError):
        return []
    rows = (body.get("data") or {}).get("dlt_srchResult")
    return list(rows) if isinstance(rows, list) else []


def rows_of(lines: Sequence[Mapping[str, Any]], object_key: str, snapshot_id: str, ingested_at: datetime,
            prefixes: Mapping[str, str], dongs: Mapping[tuple[str, str], str]) -> list[dict[str, Any]]:
    """One row per 물건 of the object, from its last line that holds it, with what its address offers."""

    found: dict[tuple, dict[str, Any]] = {}
    for number, line in enumerate(lines, start=1):
        for item in answer_rows(line):
            row = item_row(item)
            if not row["court_office_code"] or not row["case_no"] or row["property_seq"] is None or row["item_seq"] is None:
                raise ValueError(f"{object_key} line {number}: a row without its key")
            given = offer(row, prefixes, dongs)
            road = given["road"]
            row.update(
                legal_dong_code=given["legal_dong_code"], jibun_pnu=given["jibun_pnu"], reason=given["reason"],
                road_sido=road.sido_code if road else None, road_sigungu=road.sigungu if road else None,
                road_name_key=road.road if road else None, road_main_no=road.main_no if road else None,
                road_sub_no=road.sub_no if road else None,
                source_record_id=object_key, source_line_number=number, source_snapshot_id=snapshot_id,
                ingested_at_utc=ingested_at,
            )
            found[tuple(row[k] for k in KEY)] = row
    return list(found.values())


def condition_key(condition: Any) -> str:
    return json.dumps(condition or {}, sort_keys=True, ensure_ascii=False)


def search_complete(lines: Sequence[Mapping[str, Any]], conditions: Sequence[Mapping[str, Any]]) -> bool:
    """Whether the lines read every condition of a search to its last page.

    The same test the collector makes while paging: a condition ends at an empty page or once its
    pages hold totalCnt rows. A night stopped by a refusal, the request bound or the end of the quiet
    hours has a condition without such a line.
    """

    read: dict[str, int] = {}
    ended = set()
    for line in lines:
        if line.get("http_status") != 200:
            continue
        try:
            data = json.loads(line["body"]).get("data") or {}
        except (TypeError, ValueError, AttributeError):
            continue
        key = condition_key((line.get("request") or {}).get("dma_srchGdsDtlSrchInfo"))
        rows = data.get("dlt_srchResult")
        count = len(rows) if isinstance(rows, list) else 0
        read[key] = read.get(key, 0) + count
        total = whole((data.get("dma_pageInfo") or {}).get("totalCnt"))
        if count == 0 or (total is not None and read[key] >= total):
            ended.add(key)
    return {condition_key(c) for c in conditions} <= ended


def search_roles(contract: Mapping[str, Any]) -> dict[str, str]:
    """Source slug → the search its objects hold (root ADR-0014 `{provider}__{dataset}`)."""

    return {f"courtauctiongokr__{contract[role]['dataset_slug']}": role for role in ROLE_TABLE}


def snapshot_id(object_key: str) -> str:
    """The run id the collector put in the key (`...-20990102t030405z.gz` → `20990102T030405Z`)."""

    found = SNAPSHOT_ID.search(object_key)
    if not found:
        raise ValueError(f"{object_key} carries no run id")
    return found.group(1).upper()


def planned_objects(plan_dirs: Sequence[Path], contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Every object the plans fetched, with its search and local file, oldest night first."""

    roles = search_roles(contract)
    objects = []
    for plan_dir in plan_dirs:
        plan = json.loads((plan_dir / "plan.json").read_text(encoding="utf-8"))
        if plan.get("schema_version") != PLAN_SCHEMA:
            raise ValueError(f"{plan_dir}: not a {PLAN_SCHEMA} plan")
        for entry in plan["objects"]:
            if entry["source_slug"] not in roles:
                raise ValueError(f"{entry['object_key']}: {entry['source_slug']} is not a search this job loads")
            objects.append({"object_key": entry["object_key"], "role": roles[entry["source_slug"]],
                            "source_snapshot_id": snapshot_id(entry["object_key"]),
                            "path": plan_dir / entry["local_path"]})
    return sorted(objects, key=lambda o: (o["source_snapshot_id"], o["object_key"]))


def read_object(entry: Mapping[str, Any], contract: Mapping[str, Any], dongs: Mapping[tuple[str, str], str],
                ingested_at: datetime) -> dict[str, Any]:
    """What one object loads: its rows and whether its search was read completely."""

    with gzip.open(entry["path"], "rt", encoding="utf-8") as handle:
        lines = [json.loads(line) for line in handle if line.strip()]
    return {"object_key": entry["object_key"],
            "complete": search_complete(lines, contract[entry["role"]]["conditions"]),
            "rows": rows_of(lines, entry["object_key"], entry["source_snapshot_id"], ingested_at,
                            sido_prefixes(contract), dongs)}


def content_columns(contract: Mapping[str, Any]) -> list[str]:
    return [c["name"] for c in contract["columns"] if c["name"] not in (*KEY, *LOAD_COLUMNS)]


def _summary_line(summary: Mapping[str, Any]) -> None:
    print("court-auction-summary-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True, default=str))


RESOLVED = ("pnu", "pnu_basis", "pnu_unresolved_reason", "pnu_candidate_count")
# What the joins read beside the table's own columns (road_name is the table's raw road name, so the
# road key travels as road_name_key and takes the joins' name only inside resolve).
OFFER_COLUMNS = (("jibun_pnu", "string"), ("reason", "string"), ("road_sido", "string"), ("road_sigungu", "string"),
                 ("road_name_key", "string"), ("road_main_no", "int"), ("road_sub_no", "int"))


def offered_schema(contract: Mapping[str, Any]) -> tuple[list[str], str]:
    """The names and DDL of the frame handed to the joins: the table's columns it does not derive,
    the offer's columns, and a row id."""

    from platform_contracts import spark_sql_type  # noqa: PLC0415

    own = [(c["name"], spark_sql_type(c["logical_type"])) for c in contract["columns"]
           if c["name"] not in (*RESOLVED, "row_state")]
    fields = [*own, *OFFER_COLUMNS, ("row_id", "int")]
    return [name for name, _ in fields], ", ".join(f"{name} {kind}" for name, kind in fields)


def search_changes(spark: Any, role: str, search: Mapping[str, Any], contract: Mapping[str, Any], parcels: Any,
                   titles: Any, current: Any, snapshot_id: str, now: datetime) -> tuple[Any, Any]:
    """(staged rows, the rows to append) for one search of one night.

    The rows go through the joins (``address_pnu.resolve``) and are compared with the table's
    current rows (``release_changes``). A removal marker survives only for a complete item search.
    """

    from platform_contracts import column_names  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415
    from silver_release_compare import release_changes  # noqa: PLC0415

    columns = list(column_names(contract))
    names, ddl = offered_schema(contract)
    offered = spark.createDataFrame(
        [tuple(n if name == "row_id" else row.get(name) for name in names) for n, row in enumerate(search["rows"])],
        schema=ddl)
    resolved = address_pnu.resolve(
        offered.withColumnRenamed("road_name", "road_name_raw").withColumnRenamed("road_name_key", "road_name"),
        parcels, titles)
    staged = (resolved.drop("road_name").withColumnRenamed("road_name_raw", "road_name")
              .withColumn("row_state", F.lit(None).cast("string")).select(*columns))
    changes = release_changes(staged, current, key=KEY, content=content_columns(contract), removal="row_state",
                              source_snapshot_id=snapshot_id, ingested_at=F.lit(now).cast("timestamp"), columns=columns)
    if role != "item_search" or not search["complete"]:
        # Only a complete item search says what left it; the result search never does.
        changes = changes.join(staged.select(*KEY).distinct(), list(KEY), "left_semi")
    return staged, changes


def plan_dir_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--plan-dir", action="append", default=[], required=True,
                        help="A directory `plan-silver-load-objects` wrote (plan.json + objects/); repeatable")


def _load(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="load")
    plan_dir_arguments(parser)
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--table-suffix", default="")
    parser.add_argument("--allow-non-smoke-write", action="store_true")
    args = parser.parse_args(argv)
    if not IDENTIFIER.fullmatch(args.iceberg_catalog_name):
        parser.error("--iceberg-catalog-name must be a plain SQL identifier")
    if args.table_suffix not in ("", "_smoke"):
        parser.error("--table-suffix is empty or _smoke")
    if not args.table_suffix and not args.allow_non_smoke_write:
        parser.error("writing the Silver tables needs --allow-non-smoke-write")

    from lakehouse_engine import apply_catalog_settings, assert_catalog_env  # noqa: PLC0415
    from lakehouse_ingest import append_source_object  # noqa: PLC0415
    from platform_contracts import (  # noqa: PLC0415
        REMOVED, column_names, current_rows, ensure_contract_table, load_lakehouse_contract,
    )
    from pyspark.sql import SparkSession, functions as F  # noqa: PLC0415

    assert_catalog_env()
    source = load_source_contract()
    objects = planned_objects([Path(d) for d in args.plan_dir], source)
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    summary: dict[str, Any] = {"job": JOB_NAME, "status": "loaded", "objects": []}
    builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{args.iceberg_catalog_name}`.`silver`")
        dongs = address_pnu.legal_dong_index(address_pnu.current_legal_dongs(spark, args.iceberg_catalog_name))
        parcels, titles = address_pnu.lakehouse_inputs(spark, args.iceberg_catalog_name)
        for entry in objects:
            search = read_object(entry, source, dongs, now)
            role = entry["role"]
            contract_table = ROLE_TABLE[role]
            contract = load_lakehouse_contract(contract_table)
            table = f"`{args.iceberg_catalog_name}`.`silver`.`{contract_table.split('.', 1)[1]}{args.table_suffix}`"
            ensure_contract_table(spark, table, contract)
            columns = list(column_names(contract))
            counts: dict[str, Any] = {"object": entry["object_key"], "role": role, "rows": len(search["rows"]),
                                      "complete": search["complete"]}
            if search["rows"]:
                current = current_rows(spark.table(table), contract)
                staged, changes = search_changes(spark, role, search, contract, parcels, titles, current,
                                                 entry["source_snapshot_id"], now)
                changes = changes.withColumn("source_record_id", F.lit(entry["object_key"])).select(*columns).cache()
                counts.update(changes=changes.count(), with_pnu=staged.where(F.col("pnu").isNotNull()).count())
                if "row_state" in columns:
                    counts["removed"] = changes.where(F.col("row_state") == REMOVED).count()
            else:
                # A night that read no row (stopped at once) still commits, empty, so it is not owed again.
                changes = spark.createDataFrame([], schema=spark.table(table).schema).select(*columns)
            counts["appended"] = append_source_object(spark, changes, columns, table, entry["object_key"],
                                                         contract_table=contract_table)
            changes.unpersist()
            summary["objects"].append(counts)
    finally:
        spark.stop()
    _summary_line(summary)
    return 0


def _parse(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="parse")
    plan_dir_arguments(parser)
    args = parser.parse_args(argv)
    source = load_source_contract()
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    report = []
    for entry in planned_objects([Path(d) for d in args.plan_dir], source):
        search = read_object(entry, source, {}, now)
        report.append({"object": entry["object_key"], "role": entry["role"], "rows": len(search["rows"]),
                       "complete": search["complete"]})
    _summary_line({"job": JOB_NAME, "status": "parsed", "objects": report})
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
