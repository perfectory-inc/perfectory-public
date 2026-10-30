"""Load 국토교통부 실거래가 from Bronze into `silver.real_transaction` (root ADR-0348, ADR-0349).

The collector (`collect-data-go-kr-rtms`) lands gzip JSON Lines objects, one per dataset per commit;
each line is one request (`LAWD_CD`, `DEAL_YMD`, page) and its answer as received, and every
(dataset, 시군구, month) bucket in an object is whole (read to `totalCount`). What a load owes is
decided from R2 alone (`plan-silver-load-objects`). This job loads each object, oldest first, in one
commit of its own that records it:

- every row of the object gets its name (ADR-0349 §2): the bucket, plus `row_key`, the sha256 of its
  provider fields less the ones that change after the fact (해제, 등기일) and its rank among rows of
  the same name;
- each bucket is compared with its own current rows (`silver_release_compare.release_changes`): a
  new or changed name is appended, a current name the answer no longer holds gets a removal marker;
  a cancellation is a changed row (`cancelled`), never a removal (ADR-0349 §3–§4);
- each row's 법정동 code and PNU come from the one place-identity resolver (ADR-0349 §5).

`lawd-set` writes the 시군구 list the collector asks (ADR-0348 §2) from the same 법정동 snapshot.
`parse` reads the plan directories without Spark. The last stdout line of `load` is
`data-go-kr-rtms-summary-json {json}`.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

JOB_NAME = "data_go_kr_rtms"
TABLE = "silver.real_transaction"
CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contracts" / "data-go-kr-rtms.contract.json"
PLAN_SCHEMA = "foundation-platform.silver_load_objects.v1"
LAWD_SET_SCHEMA = "foundation-platform.rtms_lawd_set.v1"
SNAPSHOT_ID = re.compile(r"-(\d{8}t\d{6}z)\.gz$")
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
KEY = ("dataset_slug", "lawd_cd", "deal_ym", "row_key")
BUCKET = ("dataset_slug", "lawd_cd", "deal_ym")
LOAD_COLUMNS = ("source_record_id", "source_line_number", "source_snapshot_id", "ingested_at_utc", "row_state")
# Fields the provider fills in after the contract: a change in them is a new version of the same row.
MUTABLE_FIELDS = ("cdealType", "cdealDay", "rgstDate")
LOT = re.compile(r"^(산\s*)?(\d{1,4})(?:-(\d{1,4}))?$")

# provider field → (column, reader). The amounts are 만원 as the provider writes them.
FIELD_COLUMNS: dict[str, tuple[str, str]] = {
    "dealAmount": ("deal_amount_10k_krw", "amount"),
    "deposit": ("deposit_10k_krw", "amount"),
    "monthlyRent": ("monthly_rent_10k_krw", "amount"),
    "preDeposit": ("pre_deposit_10k_krw", "amount"),
    "preMonthlyRent": ("pre_monthly_rent_10k_krw", "amount"),
    "contractTerm": ("contract_term", "text"),
    "contractType": ("contract_type", "text"),
    "useRRRight": ("use_rr_right", "text"),
    "sggCd": ("sgg_cd", "text"),
    "sggNm": ("sgg_nm", "text"),
    "umdCd": ("umd_cd", "text"),
    "umdNm": ("umd_nm", "text"),
    "jibun": ("jibun_raw", "text"),
    "bonbun": ("bonbun", "text"),
    "bubun": ("bubun", "text"),
    "landCd": ("land_cd", "text"),
    "aptNm": ("building_name", "text"),
    "offiNm": ("building_name", "text"),
    "mhouseNm": ("building_name", "text"),
    "aptSeq": ("apt_seq", "text"),
    "aptDong": ("apt_dong", "text"),
    "buildingUse": ("building_use", "text"),
    "buildingType": ("building_type", "text"),
    "houseType": ("house_type", "text"),
    "floor": ("floor", "text"),
    "buildYear": ("build_year", "int"),
    "landUse": ("land_use", "text"),
    "jimok": ("jimok", "text"),
    "dealingGbn": ("dealing_gbn", "text"),
    "buyerGbn": ("buyer_gbn", "text"),
    "slerGbn": ("seller_gbn", "text"),
    "estateAgentSggNm": ("estate_agent_sgg_nm", "text"),
    "shareDealingType": ("share_dealing_type", "text"),
    "ownershipGbn": ("ownership_gbn", "text"),
    "landLeaseholdGbn": ("land_leasehold_gbn", "text"),
    "roadNm": ("road_name", "text"),
    "roadnm": ("road_name", "text"),
    "roadNmCd": ("road_name_code", "text"),
    "roadnmcd": ("road_name_code", "text"),
    "roadNmBonbun": ("road_name_bonbun", "text"),
    "roadnmbonbun": ("road_name_bonbun", "text"),
    "roadNmBubun": ("road_name_bubun", "text"),
    "roadnmbubun": ("road_name_bubun", "text"),
    "rgstDate": ("registration_date", "yymmdd"),
    "cdealType": ("cancel_type", "text"),
    "cdealDay": ("cancel_date", "yymmdd"),
}
# Areas: one column each for the building and the land, with which provider area it is.
AREA_FIELDS = {
    "buildingAr": ("building_area_m2", "building_area_kind", "building"),
    "excluUseAr": ("building_area_m2", "building_area_kind", "exclusive"),
    "totalFloorAr": ("building_area_m2", "building_area_kind", "total_floor"),
    "plottageAr": ("land_area_m2", "land_area_kind", "plottage"),
    "dealArea": ("land_area_m2", "land_area_kind", "deal"),
    "landAr": ("land_area_m2", "land_area_kind", "land_right"),
}
# Read for the row itself (계약일, the road-name extras kept in provider_fields_json only).
KNOWN_ELSEWHERE = frozenset({"dealYear", "dealMonth", "dealDay", "roadNmSggCd", "roadNmSeq", "roadNmbCd",
                             "roadnmsggcd", "roadnmseq", "roadnmbcd", "sggNm"})


def load_source_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def text_or_none(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    return text or None


def amount(value: Any) -> int | None:
    """`"146,500"` (만원) → 146500; None for anything that is not a whole number."""

    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip().replace(",", "")
    return int(text) if re.fullmatch(r"-?\d+", text) else None


def decimal(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(str(value).strip().replace(",", ""))
    except ValueError:
        return None


def yymmdd(value: Any) -> date | None:
    """The provider's `26.08.21` (or `2026.08.21`, `20260821`) as a date; None otherwise."""

    text = str(value or "").strip()
    for fmt in ("%y.%m.%d", "%Y.%m.%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


READERS = {"text": text_or_none, "amount": amount, "int": amount, "yymmdd": yymmdd}


def deal_date(item: Mapping[str, Any]) -> date | None:
    try:
        return date(int(item["dealYear"]), int(item["dealMonth"]), int(item["dealDay"]))
    except (KeyError, TypeError, ValueError):
        return None


def answer_items(line: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The items of one request's answer; [] for an empty answer."""

    if line.get("http_status") != 200:
        raise ValueError(f"a Bronze line holds HTTP {line.get('http_status')}: the collector keeps only answers")
    body = json.loads(line["body"])
    found = ((body.get("response") or {}).get("body") or {}).get("items")
    found = found.get("item") if isinstance(found, dict) else None
    if isinstance(found, dict):
        return [found]
    return list(found) if isinstance(found, list) else []


def bucket_of(line: Mapping[str, Any], dataset: str) -> tuple[str, str, str]:
    request = line.get("request") or {}
    lawd, month = str(request.get("LAWD_CD") or ""), str(request.get("DEAL_YMD") or "")
    if not (re.fullmatch(r"\d{5}", lawd) and re.fullmatch(r"\d{6}", month)):
        raise ValueError(f"a Bronze line asks no bucket: {request}")
    return dataset, lawd, month


def canonical(fields: Mapping[str, Any]) -> str:
    return json.dumps({k: fields[k] for k in sorted(fields)}, ensure_ascii=False, separators=(",", ":"))


def identity(dataset: str, lawd: str, month: str, item: Mapping[str, Any]) -> str:
    """The sha256 (first 32 hex) of a row's bucket and its provider fields less the mutable ones."""

    stable = {k: v for k, v in item.items() if k not in MUTABLE_FIELDS}
    text = "|".join((dataset, lawd, month, canonical(stable)))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]


def jibun_parts(row: Mapping[str, Any]) -> tuple[str, tuple[bool, int, int] | None]:
    """(reason, (mountain, 본번, 부번)) of a row's lot: reason `ok`, `jibun_masked`, `no_jibun` or `unparsed`."""

    bonbun, bubun = row.get("bonbun"), row.get("bubun")
    if bonbun and str(bonbun).strip().isdigit():
        mountain = str(row.get("land_cd") or "1").strip() == "2"
        sub = int(str(bubun).strip()) if bubun and str(bubun).strip().isdigit() else 0
        return "ok", (mountain, int(str(bonbun).strip()), sub)
    raw = row.get("jibun_raw")
    if not raw:
        return "no_jibun", None
    if "*" in raw:
        return "jibun_masked", None
    match = LOT.fullmatch(raw.strip())
    if not match:
        return "unparsed", None
    return "ok", (bool(match.group(1)), int(match.group(2)), int(match.group(3) or 0))


def candidate_pnu(code10: str | None, lot: tuple[bool, int, int] | None) -> str | None:
    if not code10 or lot is None or not 0 < lot[1] <= 9999 or lot[2] > 9999:
        return None
    return f"{code10}{'2' if lot[0] else '1'}{lot[1]:04d}{lot[2]:04d}"


def dong_index(rows: Iterable[tuple[str, str, str]]) -> dict[tuple[str, str], str]:
    """(시군구 5자리, the names after the 시군구) → 법정동 code, from (code, full name, status) rows of one
    snapshot. A current (존재) code wins over an abolished one; a name two codes of the same standing
    share is left out (guessing between them is a rule this has not)."""

    rows = [(c, n or "", s) for c, n, s in rows if re.fullmatch(r"\d{10}", c or "")]
    prefixes = {c[:5]: n for c, n, s in rows if c[5:] == "00000" and s == "존재"}
    prefixes.update({c[:5]: n for c, n, s in rows if c[5:] == "00000" and c[:5] not in prefixes})
    by_rank: dict[tuple[str, str], dict[int, set[str]]] = {}
    for code, name, status in rows:
        prefix = prefixes.get(code[:5])
        if code[5:] == "00000" or not prefix or not name.startswith(prefix):
            continue
        dong = " ".join(name[len(prefix):].split())
        if dong:
            by_rank.setdefault((code[:5], dong), {}).setdefault(0 if status == "존재" else 1, set()).add(code)
    index = {}
    for key, ranks in by_rank.items():
        codes = ranks[min(ranks)]
        if len(codes) == 1:
            index[key] = next(iter(codes))
    return index


def legal_dong_code(row: Mapping[str, Any], dongs: Mapping[tuple[str, str], str]) -> tuple[str | None, str | None]:
    """(10-digit code, reason when none)."""

    sgg, umd = row.get("sgg_cd") or "", row.get("umd_cd") or ""
    if re.fullmatch(r"\d{5}", sgg) and re.fullmatch(r"\d{5}", umd):
        return sgg + umd, None
    name = " ".join((row.get("umd_nm") or "").split())
    if not re.fullmatch(r"\d{5}", sgg) or not name:
        return None, "no_legal_dong"
    code = dongs.get((sgg, name))
    return (code, None) if code else (None, "legal_dong_not_found")


def lawd_codes(rows: Iterable[tuple[str, str, str]]) -> list[str]:
    """The 시군구 codes the API is asked by, from (code, full name, status) rows of the newest snapshot:
    every current 5-digit 시군구, less a 시 asked through its 구: a code whose name begins another current
    시군구's name (경기도 수원시 → 경기도 수원시 장안구). The digits alone cannot say it: 증평군 43745 shares
    영동군 43740's first four."""

    names = {c[:5]: " ".join((n or "").split()) for c, n, s in rows
             if re.fullmatch(r"\d{10}", c or "") and s == "존재" and c[2:5] != "000" and c[5:] == "00000"}
    return sorted(c for c, name in names.items()
                  if not any(o != c and other.startswith(name + " ") for o, other in names.items()))


def build_row(dataset: str, lawd: str, month: str, item: Mapping[str, Any]) -> dict[str, Any]:
    row: dict[str, Any] = {"dataset_slug": dataset, "lawd_cd": lawd, "deal_ym": month, "deal_date": deal_date(item)}
    for field, (column, reader) in FIELD_COLUMNS.items():
        if field in item and row.get(column) is None:
            row[column] = READERS[reader](item[field])
    for field, (column, kind_column, kind) in AREA_FIELDS.items():
        if field in item and row.get(column) is None:
            row[column] = decimal(item[field])
            row[kind_column] = kind if row[column] is not None else None
    row["cancelled"] = (row.get("cancel_type") or "").upper() == "O" or row.get("cancel_date") is not None
    row["jibun_masked"] = "*" in (row.get("jibun_raw") or "")
    row["provider_fields_json"] = canonical(item)
    return row


def unrepresented(items: Iterable[Mapping[str, Any]]) -> list[str]:
    known = set(FIELD_COLUMNS) | set(AREA_FIELDS) | KNOWN_ELSEWHERE
    return sorted({key for item in items for key in item if key not in known})


def read_lines(path: Path) -> list[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def rows_of(lines: Sequence[Mapping[str, Any]], dataset: str, object_key: str, snapshot_id: str,
            ingested_at: datetime) -> tuple[list[dict[str, Any]], list[tuple[str, str, str]], list[str]]:
    """(rows, the buckets the object answers, unrepresented fields). Each row is named (ADR-0349 §2)."""

    buckets: list[tuple[str, str, str]] = []
    grouped: dict[tuple[str, str, str, str], list[tuple[int, dict[str, Any]]]] = {}
    items_seen = []
    for number, line in enumerate(lines, start=1):
        bucket = bucket_of(line, dataset)
        if bucket not in buckets:
            buckets.append(bucket)
        for item in answer_items(line):
            items_seen.append(item)
            grouped.setdefault((*bucket, identity(*bucket, item)), []).append((number, item))
    rows = []
    for (ds, lawd, month, name), members in grouped.items():
        # Same name: ranked by the fields that change after the fact, so the rank is stable.
        members.sort(key=lambda m: canonical({k: m[1].get(k) for k in MUTABLE_FIELDS}))
        for rank, (number, item) in enumerate(members, start=1):
            row = build_row(ds, lawd, month, item)
            row.update(row_key=f"{name}-{rank}", source_record_id=object_key, source_line_number=number,
                       source_snapshot_id=snapshot_id, ingested_at_utc=ingested_at)
            rows.append(row)
    return rows, buckets, unrepresented(items_seen)


def dataset_of(source_slug: str, contract: Mapping[str, Any]) -> str:
    dataset = source_slug.removeprefix("datagokr__")
    if dataset == source_slug or dataset not in contract["datasets"]:
        raise ValueError(f"{source_slug} is not a 실거래 source this job loads")
    return dataset


def snapshot_id(object_key: str) -> str:
    found = SNAPSHOT_ID.search(object_key)
    if not found:
        raise ValueError(f"{object_key} carries no run id")
    return found.group(1).upper()


def planned_objects(plan_dirs: Sequence[Path], contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Every object the plans fetched, with its dataset and local file, oldest run first."""

    objects = []
    for plan_dir in plan_dirs:
        plan = json.loads((plan_dir / "plan.json").read_text(encoding="utf-8"))
        if plan.get("schema_version") != PLAN_SCHEMA:
            raise ValueError(f"{plan_dir}: not a {PLAN_SCHEMA} plan")
        if plan.get("table") not in (None, TABLE):
            raise ValueError(f"{plan_dir}: {plan['table']} is not the table this job loads")
        for entry in plan["objects"]:
            objects.append({"object_key": entry["object_key"],
                            "dataset": dataset_of(entry["source_slug"], contract),
                            "source_snapshot_id": snapshot_id(entry["object_key"]),
                            "path": plan_dir / entry["local_path"]})
    return sorted(objects, key=lambda o: (o["source_snapshot_id"], o["object_key"]))


def content_columns(contract: Mapping[str, Any]) -> list[str]:
    return [c["name"] for c in contract["columns"] if c["name"] not in (*KEY, *LOAD_COLUMNS)]


def link_report(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Per dataset: rows, rows with a pnu, rows with only a 법정동, and the reasons the rest have."""

    report: dict[str, dict[str, Any]] = {}
    for row in rows:
        entry = report.setdefault(row["dataset_slug"], {"rows": 0, "pnu": 0, "legal_dong_only": 0, "unresolved": {}})
        entry["rows"] += 1
        if row.get("pnu"):
            entry["pnu"] += 1
        elif row.get("legal_dong_code"):
            entry["legal_dong_only"] += 1
        reason = row.get("pnu_unresolved_reason")
        if reason:
            entry["unresolved"][reason] = entry["unresolved"].get(reason, 0) + 1
    return report


def unmasked_unresolved_share(rows: Sequence[Mapping[str, Any]]) -> float:
    """Of the rows whose lot the provider did not mask, the share that found no pnu (ADR-0349 §6)."""

    lots = [r for r in rows if r.get("pnu_unresolved_reason") not in ("jibun_masked", "no_jibun")]
    return sum(1 for r in lots if not r.get("pnu")) / len(lots) if lots else 0.0


def _summary_line(summary: Mapping[str, Any]) -> None:
    print("data-go-kr-rtms-summary-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True, default=str))


def plan_dir_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--plan-dir", action="append", default=[], required=True,
                        help="A directory `plan-silver-load-objects` wrote (plan.json + objects/); repeatable")


def _parse(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="parse")
    plan_dir_arguments(parser)
    args = parser.parse_args(argv)
    contract = load_source_contract()
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    report = []
    for entry in planned_objects([Path(d) for d in args.plan_dir], contract):
        rows, buckets, unknown = rows_of(read_lines(entry["path"]), entry["dataset"], entry["object_key"],
                                         entry["source_snapshot_id"], now)
        report.append({"object": entry["object_key"], "rows": len(rows), "buckets": len(buckets),
                       "cancelled": sum(1 for r in rows if r["cancelled"]), "unrepresented_fields": unknown})
    _summary_line({"job": JOB_NAME, "status": "parsed", "objects": report})
    return 0


def _tables(argv: Sequence[str]) -> int:
    """`<table> <source slugs, comma-separated>`: what the load script plans."""

    del argv
    slugs = ",".join(f"datagokr__{name}" for name in sorted(load_source_contract()["datasets"]))
    print(TABLE, slugs)
    return 0


def newest_snapshot_rows(spark: Any, catalog: str) -> list[tuple[str, str, str]]:
    table = f"`{catalog}`.`reference`.`legal_dong_code_snapshot`"
    newest = spark.sql(f"SELECT max(snapshot_date) AS d FROM {table}").collect()[0]["d"]
    rows = spark.sql(f"SELECT region_cd, full_name, status FROM {table} WHERE snapshot_date = DATE '{newest}'").collect()
    return [(row["region_cd"], row["full_name"], row["status"]) for row in rows]


def write_lawd_set(path: Path, codes: Sequence[str], snapshot_rows: int, now: datetime) -> None:
    if not codes:
        raise ValueError("the 법정동 snapshot gives no 시군구: refusing to write an empty list")
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps({"schema_version": LAWD_SET_SCHEMA, "written_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                               "snapshot_rows": snapshot_rows, "lawd_codes": list(codes)}, ensure_ascii=False),
                   encoding="utf-8")
    tmp.replace(path)


def spark_session(catalog: str) -> Any:
    from lakehouse_engine import apply_catalog_settings, assert_catalog_env  # noqa: PLC0415
    from pyspark.sql import SparkSession  # noqa: PLC0415

    assert_catalog_env()
    builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
    return apply_catalog_settings(builder, catalog).getOrCreate()


def _lawd_set(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="lawd-set")
    parser.add_argument("--output", required=True)
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    args = parser.parse_args(argv)
    spark = spark_session(args.iceberg_catalog_name)
    try:
        rows = newest_snapshot_rows(spark, args.iceberg_catalog_name)
    finally:
        spark.stop()
    codes = lawd_codes(rows)
    write_lawd_set(Path(args.output), codes, len(rows), datetime.now(timezone.utc))
    print(f"data-go-kr-rtms-lawd-set codes={len(codes)} snapshot_rows={len(rows)}")
    return 0


def ensure_table(spark: Any, args: argparse.Namespace) -> tuple[str, dict[str, Any], list[str], str]:
    from platform_contracts import (  # noqa: PLC0415
        column_names, create_table_columns_sql, evolve_iceberg_table_to_contract, load_lakehouse_contract,
        partition_clause_sql, spark_sql_type,
    )

    contract = load_lakehouse_contract(TABLE)
    table = f"`{args.iceberg_catalog_name}`.`{args.iceberg_namespace}`.`{TABLE.split('.', 1)[1] + args.table_suffix}`"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{args.iceberg_catalog_name}`.`{args.iceberg_namespace}`")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({create_table_columns_sql(contract)}) USING iceberg "
              f"{partition_clause_sql(contract)} TBLPROPERTIES ('format-version' = '2', "
              "'write.parquet.compression-codec' = 'zstd')")
    evolve_iceberg_table_to_contract(spark, table, contract)
    schema = ", ".join(f"{c['name']} {spark_sql_type(c['logical_type'])}" for c in contract["columns"])
    return table, contract, list(column_names(contract)), schema


def _load(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="load")
    plan_dir_arguments(parser)
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-namespace", default="silver")
    parser.add_argument("--table-suffix", default="", help="`_smoke` for a smoke run")
    parser.add_argument("--allow-non-smoke-write", action="store_true")
    args = parser.parse_args(argv)
    for label in ("iceberg_catalog_name", "iceberg_namespace"):
        if not IDENTIFIER.fullmatch(getattr(args, label)):
            parser.error(f"{label} must be a plain SQL identifier")
    if args.table_suffix not in ("", "_smoke"):
        parser.error("--table-suffix is empty or _smoke")
    if not args.table_suffix and not args.allow_non_smoke_write:
        parser.error("writing the Silver table needs --allow-non-smoke-write")

    import place_identity_resolver  # noqa: PLC0415
    from lakehouse_ingest import append_source_object  # noqa: PLC0415
    from platform_contracts import REMOVED, current_rows, load_lakehouse_contract  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415
    from silver_release_compare import release_changes  # noqa: PLC0415

    source = load_source_contract()
    objects = planned_objects([Path(d) for d in args.plan_dir], source)
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    summary: dict[str, Any] = {"job": JOB_NAME, "objects": [], "status": "loaded"}
    spark = spark_session(args.iceberg_catalog_name)
    failed_share: list[str] = []
    try:
        table, contract, columns, schema = ensure_table(spark, args)
        if objects:
            # ADR-0349 §5: names → codes from the newest snapshot; old codes → current through the
            # crosswalk; a pnu only when it is a current parcel. Read once for the run.
            dongs = dong_index(newest_snapshot_rows(spark, args.iceberg_catalog_name))
            remap, ambiguous = place_identity_resolver.pairs_from_lakehouse(spark, args.iceberg_catalog_name)
            parcels = current_rows(spark.table(f"`{args.iceberg_catalog_name}`.`silver`.`parcel_boundaries`"),
                                   load_lakehouse_contract("silver.parcel_boundaries")).select("pnu")
        for entry in objects:
            rows, buckets, unknown = rows_of(read_lines(entry["path"]), entry["dataset"], entry["object_key"],
                                             entry["source_snapshot_id"], now)
            for row in rows:
                code, reason = legal_dong_code(row, dongs)
                lot_reason, lot = jibun_parts(row)
                row["legal_dong_code"] = None if code in ambiguous else remap.get(code, code) if code else None
                row["pnu"] = candidate_pnu(code, lot)
                row["_reason"] = reason or (lot_reason if lot_reason != "ok" else None)
            frame = spark.createDataFrame([tuple(r.get(c) for c in columns) + (r["_reason"],) for r in rows],
                                          schema=schema + ", _reason string")
            resolved = place_identity_resolver.resolve(frame, remap, ambiguous, parcels)
            resolved = resolved.withColumn("pnu_unresolved_reason", F.when(F.col("pnu").isNotNull(), F.lit(None))
                                           .otherwise(F.coalesce(F.col("_reason"), F.col("pnu_unresolved_reason"))))
            staged = resolved.select(*columns).cache()
            linked = [r.asDict() for r in staged.select("dataset_slug", "pnu", "legal_dong_code",
                                                         "pnu_unresolved_reason").collect()]
            in_buckets = spark.createDataFrame(buckets, "dataset_slug string, lawd_cd string, deal_ym string")
            current = current_rows(
                spark.table(table).where(F.col("dataset_slug") == entry["dataset"])
                .where(F.col("lawd_cd").isin(sorted({b[1] for b in buckets})))
                .where(F.col("deal_ym").isin(sorted({b[2] for b in buckets}))), contract,
            ).join(in_buckets, list(BUCKET), "left_semi")
            changes = release_changes(
                staged, current, key=KEY, content=content_columns(contract), removal="row_state",
                source_snapshot_id=entry["source_snapshot_id"], ingested_at=F.lit(now).cast("timestamp"),
                columns=columns,
            ).withColumn("source_record_id", F.lit(entry["object_key"])).cache()
            share = unmasked_unresolved_share(linked)
            line = {"object": entry["object_key"], "dataset": entry["dataset"], "rows": len(rows),
                    "buckets": len(buckets), "cancelled": sum(1 for r in rows if r["cancelled"]),
                    "changes": changes.count(), "removed": changes.where(F.col("row_state") == REMOVED).count(),
                    "linked": link_report(linked), "unmasked_unresolved_share": round(share, 4),
                    "unrepresented_fields": unknown}
            line["appended"] = append_source_object(spark, changes, columns, table, entry["object_key"])
            if share > float(source["silver_load"]["max_unmasked_unresolved_share"]) and len(linked) >= 50:
                failed_share.append(entry["object_key"])
            changes.unpersist()
            staged.unpersist()
            summary["objects"].append(line)
    finally:
        spark.stop()
    if failed_share:
        summary["status"] = "loaded_link_rate_below_bound"
    _summary_line(summary)
    if failed_share:
        print(f"data-go-kr-rtms: {len(failed_share)} object(s) linked fewer unmasked lots than the contract's bound "
              f"({source['silver_load']['max_unmasked_unresolved_share']}); loaded, failing so it is seen", file=sys.stderr)
        return 1
    return 0


COMMANDS = {"parse": _parse, "load": _load, "tables": _tables, "lawd-set": _lawd_set}


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] not in COMMANDS:
        print(f"usage: {Path(__file__).name} {{{','.join(COMMANDS)}}} ...", file=sys.stderr)
        return 2
    return COMMANDS[argv[0]](argv[1:])


if __name__ == "__main__":
    sys.exit(main())
