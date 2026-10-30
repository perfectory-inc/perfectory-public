"""Load 국토교통부 실거래가 from the rt.molit.go.kr bulk exports into `silver.real_transaction`
(root ADR-0348, ADR-0349).

The collector (`collect-rt-molit-real-transaction`) lands one CSV per (dataset, 계약 달), nationwide
and whole, content-qualified (`export--sha256-<hex>.csv`): a re-pulled month whose bytes changed is
a new object beside the earlier one. What a load owes is decided from R2 alone
(`plan-silver-load-objects`); each object is one commit that records it, oldest landing first:

- every row of the export gets its name (ADR-0349 §2): the sha256 of its fields less the row number
  (`NO`) and the columns filled in after the fact (해제사유발생일, 등기일자), plus its rank among rows
  of the same name;
- the month is compared with its own current rows (`silver_release_compare.release_changes`): a new
  or changed name is appended, a current name the export no longer holds gets a removal marker. A
  cancellation is a changed row (`cancelled`), never a removal (ADR-0349 §3-§4);
- each row's 법정동 code comes from its full address (`시군구`, the code table's own names), its PNU
  from the lot when the provider did not mask it, both through the one place-identity resolver
  (ADR-0288 §1, ADR-0349 §5).

`parse` reads the plan directories without Spark. The last stdout line of `load` is
`rt-molit-real-transaction-summary-json {json}`.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, NamedTuple, Sequence

JOB_NAME = "rt_molit_real_transaction"
TABLE = "silver.real_transaction"
# Root ADR-0364: the lines an export carried that are not loaded, each with its Bronze reference.
REJECTED_TABLE = "silver.real_transaction_rejected"
CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contracts" / "rt-molit-real-transaction.contract.json"
PLAN_SCHEMA = "foundation-platform.silver_load_objects.v1"
PERIOD = re.compile(r"/period=(\d{4})-(\d{2})/")
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
KEY = ("dataset_slug", "deal_ym", "row_key")
BUCKET = ("dataset_slug", "deal_ym")
LOAD_COLUMNS = ("source_record_id", "source_line_number", "source_snapshot_id", "ingested_at_utc", "row_state")
LOT = re.compile(r"^(산\s*)?(\d{1,4})(?:-(\d{1,4}))?$")
EMPTY = ("", "-")

# CSV column → (silver column, reader). Amounts are 만원 as the provider writes them.
TEXT, AMOUNT, INT, DAY = "text", "amount", "int", "day"
COLUMNS: dict[str, tuple[str, str]] = {
    "시군구": ("address_raw", TEXT),
    "번지": ("jibun_raw", TEXT),
    "지번": ("jibun_raw", TEXT),
    "본번": ("bonbun", TEXT),
    "부번": ("bubun", TEXT),
    "단지명": ("building_name", TEXT),
    "건물명": ("building_name", TEXT),
    "유형": ("building_type", TEXT),
    "주택유형": ("house_type", TEXT),
    "건축물주용도": ("building_use", TEXT),
    "용도지역": ("land_use", TEXT),
    "지목": ("jimok", TEXT),
    "도로조건": ("road_condition", TEXT),
    "도로명": ("road_address", TEXT),
    "거래금액(만원)": ("deal_amount_10k_krw", AMOUNT),
    "보증금(만원)": ("deposit_10k_krw", AMOUNT),
    "월세금(만원)": ("monthly_rent_10k_krw", AMOUNT),
    "종전계약 보증금(만원)": ("pre_deposit_10k_krw", AMOUNT),
    "종전계약 월세(만원)": ("pre_monthly_rent_10k_krw", AMOUNT),
    "전월세구분": ("rent_kind", TEXT),
    "계약기간": ("contract_term", TEXT),
    "계약구분": ("contract_type", TEXT),
    "갱신요구권 사용": ("use_rr_right", TEXT),
    "동": ("apt_dong", TEXT),
    "층": ("floor", TEXT),
    "건축년도": ("build_year", INT),
    "매수자": ("buyer_gbn", TEXT),
    "매수": ("buyer_gbn", TEXT),
    "매도자": ("seller_gbn", TEXT),
    "매도": ("seller_gbn", TEXT),
    "거래유형": ("dealing_gbn", TEXT),
    "중개사소재지": ("estate_agent_sgg_nm", TEXT),
    "지분구분": ("share_dealing_type", TEXT),
    "분/입주권": ("ownership_gbn", TEXT),
    "해제사유발생일": ("cancel_date", DAY),
    "등기일자": ("registration_date", DAY),
}
# Areas: (column, kind column, kind). 계약면적 is the land's in the 토지 export and the house's in the
# 단독다가구 전월세 export: the dataset decides (`area_of`).
AREAS: dict[str, tuple[str, str, str]] = {
    "전용면적(㎡)": ("building_area_m2", "building_area_kind", "exclusive"),
    "전용/연면적(㎡)": ("building_area_m2", "building_area_kind", "exclusive_or_total_floor"),
    "연면적(㎡)": ("building_area_m2", "building_area_kind", "total_floor"),
    "계약면적(㎡)": ("building_area_m2", "building_area_kind", "contract"),
    "대지면적(㎡)": ("land_area_m2", "land_area_kind", "plottage"),
    "대지권면적(㎡)": ("land_area_m2", "land_area_kind", "land_right"),
}
LAND_CONTRACT_AREA = ("계약면적", ("land_area_m2", "land_area_kind", "deal"))
KNOWN_ELSEWHERE = frozenset({"NO", "계약년월", "계약일"})


def load_source_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def text_or_none(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    return None if text in EMPTY else text


def amount(value: Any) -> int | None:
    text = (text_or_none(value) or "").replace(",", "")
    return int(text) if re.fullmatch(r"-?\d+", text) else None


def decimal(value: Any) -> float | None:
    try:
        return float((text_or_none(value) or "").replace(",", ""))
    except ValueError:
        return None


def day(value: Any) -> date | None:
    """`20261028` (or `26.10.28`) as a date; None for `-`, blank or anything else."""

    text = text_or_none(value) or ""
    for fmt in ("%Y%m%d", "%y.%m.%d", "%Y.%m.%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


READERS = {TEXT: text_or_none, AMOUNT: amount, INT: amount, DAY: day}


class Rejected(NamedTuple):
    """A line of an export that is not loaded, with the Bronze reference that finds it again (root ADR-0364)."""

    line: int
    raw: str
    reason: str


class _Recorded:
    """The physical lines `csv.reader` consumed for its last record, so a rejected record keeps its raw text."""

    def __init__(self, lines: Iterable[str]) -> None:
        self._lines = iter(lines)
        self.taken: list[str] = []

    def __iter__(self) -> "_Recorded":
        return self

    def __next__(self) -> str:
        line = next(self._lines)
        self.taken.append(line)
        return line


def export_rows(data: bytes, encoding: str, header_first: str, month: str | None = None
                ) -> tuple[list[str], list[tuple[int, list[str]]], list[Rejected]]:
    """(header, [(line number, cells)], rejected lines) of one export.

    The text is read as CSV from the header line on, in one pass (a quoted field may span lines, and
    only CR/LF end a line: `str.splitlines` also broke on form feeds and other separators). An export
    without its header line, or whose preamble names another month than `month`, is refused whole.
    A record is rejected, not loaded, when its cell count is not the header's or its `NO` does not
    continue the export's 1..N (the 2026-10-30 export carried a second export's rows after its own).
    """

    text = data.decode(encoding)
    start = text.find(f'"{header_first}"')
    if start < 0 or (start > 0 and text[start - 1] not in "\r\n"):
        raise ValueError("the export has no header line: not the provider's CSV")
    if month is not None:
        dates = re.search(r"계약일자 : (\d{4})-(\d{2})-\d{2} ~ (\d{4})-(\d{2})-\d{2}", text[:start])
        if not dates or dates.group(1) + dates.group(2) != month or dates.group(3) + dates.group(4) != month:
            raise ValueError(f"the preamble does not name the month {month}")
    first_line = text.count("\n", 0, start) + 1
    source = _Recorded(io.StringIO(text[start:], newline=""))
    reader = csv.reader(source)
    header = next(reader)
    source.taken = []
    rows: list[tuple[int, list[str]]] = []
    rejected: list[Rejected] = []
    expected = 1
    line = first_line
    for cells in reader:
        raw, source.taken = "".join(source.taken).rstrip("\r\n"), []
        number, line = line + 1, line + raw.count("\n") + 1
        if not any(c.strip() for c in cells):
            continue
        if len(cells) != len(header):
            rejected.append(Rejected(number, raw, f"cells_{len(cells)}_of_{len(header)}"))
            # A broken row of this export still took its number; another export's row did not.
            expected += 1 if cells and cells[0].strip() == str(expected) else 0
            continue
        if cells[0].strip() != str(expected):
            rejected.append(Rejected(number, raw, f"row_number_{cells[0].strip()}_where_{expected}"))
            continue
        expected += 1
        rows.append((number, cells))
    return header, rows, rejected


def identity(dataset: str, month: str, fields: Mapping[str, str], drop: Iterable[str]) -> str:
    """The sha256 (first 32 hex) of a row's bucket and its fields less the row number and mutable ones."""

    stable = {k: v for k, v in fields.items() if k not in set(drop)}
    text = "|".join((dataset, month, json.dumps(stable, ensure_ascii=False, sort_keys=True, separators=(",", ":"))))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]


def build_row(dataset: str, month: str, fields: Mapping[str, str]) -> dict[str, Any]:
    row: dict[str, Any] = {"dataset_slug": dataset, "deal_ym": month}
    for name, value in fields.items():
        if name in COLUMNS:
            column, reader = COLUMNS[name]
            row[column] = READERS[reader](value)
        area = LAND_CONTRACT_AREA[1] if name == LAND_CONTRACT_AREA[0] else AREAS.get(name)
        if area is not None:
            column, kind_column, kind = area
            row[column] = decimal(value)
            row[kind_column] = kind if row[column] is not None else None
    try:
        row["deal_date"] = date(int(month[:4]), int(month[4:]), int(fields.get("계약일", "").strip()))
    except ValueError:
        row["deal_date"] = None
    row["cancelled"] = row.get("cancel_date") is not None
    row["jibun_masked"] = "*" in (row.get("jibun_raw") or "")
    row["provider_fields_json"] = json.dumps(dict(fields), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return row


def unrepresented(header: Sequence[str]) -> list[str]:
    known = set(COLUMNS) | set(AREAS) | {LAND_CONTRACT_AREA[0]} | KNOWN_ELSEWHERE
    return sorted(name for name in header if name not in known)


def rows_of(data: bytes, dataset: str, month: str, object_key: str, snapshot_id: str, ingested_at: datetime,
            csv_contract: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[str], list[Rejected]]:
    """(named rows, unrepresented columns, rejected lines) of one export (ADR-0349 §2, ADR-0364)."""

    header, lines, rejected = export_rows(data, csv_contract["encoding"], csv_contract["header_first_column"], month)
    drop = [csv_contract["row_number_column"], *csv_contract["mutable_columns"]]
    grouped: dict[str, list[tuple[int, dict[str, str]]]] = {}
    for number, cells in lines:
        fields = dict(zip(header, cells))
        if fields.get("계약년월", month).strip() != month:
            rejected.append(Rejected(number, ",".join(f'"{c}"' for c in cells), "deal_month_outside_export"))
            continue
        grouped.setdefault(identity(dataset, month, fields, drop), []).append((number, fields))
    rows = []
    mutable = csv_contract["mutable_columns"]
    for name, members in grouped.items():
        # Same name: ranked by the columns filled in after the fact, so the rank is stable.
        members.sort(key=lambda m: [m[1].get(c, "") for c in mutable])
        for rank, (number, fields) in enumerate(members, start=1):
            row = build_row(dataset, month, {k: v for k, v in fields.items() if k != csv_contract["row_number_column"]})
            row.update(row_key=f"{name}-{rank}", source_record_id=object_key, source_line_number=number,
                       source_snapshot_id=snapshot_id, ingested_at_utc=ingested_at)
            rows.append(row)
    return rows, unrepresented(header), rejected


def read_export(entry: Mapping[str, Any], csv_contract: Mapping[str, Any], ingested_at: datetime
                ) -> tuple[list[dict[str, Any]], list[str], list[Rejected], bool]:
    """(rows, unrepresented columns, rejected lines, refused whole) of one planned object.

    An export that is not the provider's CSV for its month is refused whole: one rejected line 0 with
    the reason, and no rows (root ADR-0364)."""

    try:
        rows, unknown, rejected = rows_of(entry["path"].read_bytes(), entry["dataset"], entry["month"],
                                          entry["object_key"], entry["source_snapshot_id"], ingested_at, csv_contract)
    except (ValueError, UnicodeDecodeError) as error:
        return [], [], [Rejected(0, "", f"export_refused: {error}")], True
    return rows, unknown, rejected, False


def rejected_share(rows: int, rejected: int) -> float:
    return rejected / (rows + rejected) if rows + rejected else 0.0


def quarantine_reason(rows: int, rejected: Sequence[Rejected], refused: bool, max_share: float) -> str | None:
    """Why an export is quarantined whole, or None when its accepted rows load (root ADR-0364): refused
    outright, or more of its lines rejected than the contract allows (a mixed export: what passed the
    line checks is not trusted either; the collector asks the month again in a clean session)."""

    if refused:
        return "export_refused"
    share = rejected_share(rows, len(rejected))
    return f"export_quarantined: {share:.4%} of lines rejected" if share > max_share else None


def rejected_rows(entry: Mapping[str, Any], rejected: Sequence[Rejected], ingested_at: datetime) -> list[tuple[Any, ...]]:
    """The dead-letter rows of one object, in `silver.real_transaction_rejected`'s column order."""

    return [(entry["dataset"], entry["month"], entry["object_key"], r.line, r.raw, r.reason,
             entry["source_snapshot_id"], ingested_at) for r in rejected]


def lot_of(row: Mapping[str, Any]) -> tuple[str, tuple[bool, int, int] | None]:
    """(reason, (mountain, 본번, 부번)): `ok`, `jibun_masked`, `no_jibun` or `unparsed`."""

    raw = (row.get("jibun_raw") or "").strip()
    if "*" in raw:
        return "jibun_masked", None
    bonbun, bubun = (row.get("bonbun") or "").strip(), (row.get("bubun") or "").strip()
    if bonbun.isdigit() and int(bonbun) > 0:
        return "ok", (raw.startswith("산"), int(bonbun), int(bubun) if bubun.isdigit() else 0)
    if not raw:
        return "no_jibun", None
    match = LOT.fullmatch(raw)
    if not match:
        return "unparsed", None
    return "ok", (bool(match.group(1)), int(match.group(2)), int(match.group(3) or 0))


def candidate_pnu(code10: str | None, lot: tuple[bool, int, int] | None) -> str | None:
    if not code10 or lot is None or not 0 < lot[1] <= 9999 or lot[2] > 9999:
        return None
    return f"{code10}{'2' if lot[0] else '1'}{lot[1]:04d}{lot[2]:04d}"


def dong_index(rows: Iterable[tuple[str, str, str]]) -> dict[str, str]:
    """Full 법정동 name → code, from (code, full name, status) rows of one snapshot. A current (존재)
    code wins over an abolished one; a name two codes of the same standing share is left out."""

    by_rank: dict[str, dict[int, set[str]]] = {}
    for code, name, status in rows:
        if not re.fullmatch(r"\d{10}", code or "") or code[5:] == "00000":
            continue
        key = " ".join((name or "").split())
        by_rank.setdefault(key, {}).setdefault(0 if status == "존재" else 1, set()).add(code)
    index = {}
    for key, ranks in by_rank.items():
        codes = ranks[min(ranks)]
        if len(codes) == 1:
            index[key] = next(iter(codes))
    return index


def legal_dong_code(row: Mapping[str, Any], dongs: Mapping[str, str]) -> tuple[str | None, str | None]:
    """(10-digit code, reason when none) of a row's full address."""

    name = " ".join((row.get("address_raw") or "").split())
    if not name:
        return None, "no_address"
    code = dongs.get(name)
    return (code, None) if code else (None, "legal_dong_not_found")


def link_report(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
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
    lots = [r for r in rows if r.get("pnu_unresolved_reason") not in ("jibun_masked", "no_jibun")]
    return sum(1 for r in lots if not r.get("pnu")) / len(lots) if lots else 0.0


def dataset_of(source_slug: str, contract: Mapping[str, Any]) -> str:
    dataset = source_slug.removeprefix("rtmolitgokr__")
    if dataset == source_slug or dataset not in contract["datasets"]:
        raise ValueError(f"{source_slug} is not a 실거래 source this job loads")
    return dataset


def month_of(object_key: str) -> str:
    found = PERIOD.search(object_key)
    if not found:
        raise ValueError(f"{object_key} names no whole month (period=YYYY-MM)")
    return found.group(1) + found.group(2)


def snapshot_id(last_modified: str | None, object_key: str) -> str:
    """The object's landing time as `YYYYMMDDTHHMMSSZ`: the edition of a content-qualified export."""

    if not last_modified:
        raise ValueError(f"{object_key}: the plan carries no last_modified")
    landed = datetime.fromisoformat(last_modified.replace("Z", "+00:00")).astimezone(timezone.utc)
    return landed.strftime("%Y%m%dT%H%M%SZ")


def planned_objects(plan_dirs: Sequence[Path], contract: Mapping[str, Any]) -> list[dict[str, Any]]:
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
                            "month": month_of(entry["object_key"]),
                            "source_snapshot_id": snapshot_id(entry.get("last_modified"), entry["object_key"]),
                            "path": plan_dir / entry["local_path"]})
    return sorted(objects, key=lambda o: (o["source_snapshot_id"], o["object_key"]))


def content_columns(contract: Mapping[str, Any]) -> list[str]:
    return [c["name"] for c in contract["columns"] if c["name"] not in (*KEY, *LOAD_COLUMNS)]


def _summary_line(summary: Mapping[str, Any]) -> None:
    print("rt-molit-real-transaction-summary-json "
          + json.dumps(summary, ensure_ascii=False, sort_keys=True, default=str))


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
        rows, unknown, rejected, refused = read_export(entry, contract["csv"], now)
        report.append({"object": entry["object_key"], "rows": len(rows), "rejected": len(rejected),
                       "refused": refused, "cancelled": sum(1 for r in rows if r["cancelled"]),
                       "unrepresented_fields": unknown})
    _summary_line({"job": JOB_NAME, "status": "parsed", "objects": report})
    return 0


def _tables(argv: Sequence[str]) -> int:
    """`<table> <source slugs, comma-separated>`: what the load script plans."""

    del argv
    print(TABLE, ",".join(f"rtmolitgokr__{name}" for name in sorted(load_source_contract()["datasets"])))
    return 0


def newest_snapshot_rows(spark: Any, catalog: str) -> list[tuple[str, str, str]]:
    table = f"`{catalog}`.`reference`.`legal_dong_code_snapshot`"
    newest = spark.sql(f"SELECT max(snapshot_date) AS d FROM {table}").collect()[0]["d"]
    rows = spark.sql(f"SELECT region_cd, full_name, status FROM {table} WHERE snapshot_date = DATE '{newest}'").collect()
    return [(row["region_cd"], row["full_name"], row["status"]) for row in rows]


def ensure_table(spark: Any, args: argparse.Namespace, name: str = TABLE) -> tuple[str, dict[str, Any], list[str], str]:
    from platform_contracts import (  # noqa: PLC0415
        column_names, create_table_columns_sql, evolve_iceberg_table_to_contract, load_lakehouse_contract,
        partition_clause_sql, spark_sql_type,
    )

    contract = load_lakehouse_contract(name)
    table = f"`{args.iceberg_catalog_name}`.`{args.iceberg_namespace}`.`{name.split('.', 1)[1] + args.table_suffix}`"
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
    from lakehouse_engine import apply_catalog_settings, assert_catalog_env  # noqa: PLC0415
    from lakehouse_ingest import append_source_object  # noqa: PLC0415
    from platform_contracts import REMOVED, current_rows, load_lakehouse_contract  # noqa: PLC0415
    from pyspark.sql import SparkSession, functions as F  # noqa: PLC0415
    from silver_release_compare import release_changes  # noqa: PLC0415

    assert_catalog_env()
    source = load_source_contract()
    objects = planned_objects([Path(d) for d in args.plan_dir], source)
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    summary: dict[str, Any] = {"job": JOB_NAME, "objects": [], "status": "loaded"}
    builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    below_bound: list[str] = []
    over_rejected: list[str] = []
    max_rejected = float(source["silver_load"]["max_rejected_share"])
    try:
        table, contract, columns, schema = ensure_table(spark, args)
        dead_table, _dead_contract, dead_columns, dead_schema = ensure_table(spark, args, REJECTED_TABLE)
        if objects:
            # ADR-0349 §5: names → codes from the newest snapshot; old codes → current through the
            # crosswalk; a pnu only when it is a current parcel. Read once for the run.
            dongs = dong_index(newest_snapshot_rows(spark, args.iceberg_catalog_name))
            remap, ambiguous = place_identity_resolver.pairs_from_lakehouse(spark, args.iceberg_catalog_name)
            parcels = current_rows(spark.table(f"`{args.iceberg_catalog_name}`.`silver`.`parcel_boundaries`"),
                                   load_lakehouse_contract("silver.parcel_boundaries")).select("pnu")
        for entry in objects:
            rows, unknown, rejected, refused = read_export(entry, source["csv"], now)
            share_rejected = rejected_share(len(rows), len(rejected))
            quarantined = quarantine_reason(len(rows), rejected, refused, max_rejected)
            if quarantined:
                # The whole export waits for its clean copy: nothing of it loads, and it is recorded so
                # no later run stops on it again (root ADR-0364; the alert fires once, from the metric).
                if not refused:
                    rejected = [Rejected(0, "", quarantined), *rejected]
                rows = []
                over_rejected.append(entry["object_key"])
            # Root ADR-0364: the lines not loaded land first, in their own table, referencing Bronze.
            if rejected:
                dead = spark.createDataFrame(rejected_rows(entry, rejected, now), schema=dead_schema)
                append_source_object(spark, dead, dead_columns, dead_table, entry["object_key"])
            if quarantined:
                # Recorded with no rows and no comparison: an empty export compared with the month would
                # remove every row the month holds.
                empty = spark.createDataFrame([], schema=schema)
                summary["objects"].append({
                    "object": entry["object_key"], "dataset": entry["dataset"], "month": entry["month"],
                    "rows": 0, "rejected": len(rejected), "rejected_share": round(share_rejected, 6),
                    "refused": refused, "quarantined": quarantined,
                    "rejected_reasons": sorted({r.reason.split(":")[0] for r in rejected}),
                    "appended": append_source_object(spark, empty, columns, table, entry["object_key"])})
                continue
            for row in rows:
                code, reason = legal_dong_code(row, dongs)
                lot_reason, lot = lot_of(row)
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
            # The month's current rows only: an export is the whole month (root ADR-0349 §3).
            current = current_rows(spark.table(table).where((F.col("dataset_slug") == entry["dataset"])
                                                            & (F.col("deal_ym") == entry["month"])), contract)
            changes = release_changes(
                staged, current, key=KEY, content=content_columns(contract), removal="row_state",
                source_snapshot_id=entry["source_snapshot_id"], ingested_at=F.lit(now).cast("timestamp"),
                columns=columns,
            ).withColumn("source_record_id", F.lit(entry["object_key"])).cache()
            share = unmasked_unresolved_share(linked)
            line = {"object": entry["object_key"], "dataset": entry["dataset"], "month": entry["month"],
                    "rows": len(rows), "cancelled": sum(1 for r in rows if r["cancelled"]),
                    "changes": changes.count(), "removed": changes.where(F.col("row_state") == REMOVED).count(),
                    "linked": link_report(linked), "unmasked_unresolved_share": round(share, 4),
                    "rejected": len(rejected), "rejected_share": round(share_rejected, 6), "refused": refused,
                    "quarantined": None,
                    "rejected_reasons": sorted({r.reason.split(":")[0] for r in rejected}),
                    "unrepresented_fields": unknown}
            line["appended"] = append_source_object(spark, changes, columns, table, entry["object_key"])
            if share > float(source["silver_load"]["max_unmasked_unresolved_share"]) and len(linked) >= 50:
                below_bound.append(entry["object_key"])
            changes.unpersist()
            staged.unpersist()
            summary["objects"].append(line)
    finally:
        spark.stop()
    if below_bound:
        summary["status"] = "loaded_link_rate_below_bound"
    summary["quarantined_exports"] = len(over_rejected)
    summary["rejected"] = sum(line.get("rejected", 0) for line in summary["objects"])
    _summary_line(summary)
    if below_bound:
        print(f"rt-molit-real-transaction: {len(below_bound)} object(s) linked fewer unmasked lots than the "
              f"contract's bound; loaded, failing so it is seen", file=sys.stderr)
    if over_rejected:
        print(f"rt-molit-real-transaction: {len(over_rejected)} export(s) quarantined whole (refused, or more "
              f"lines rejected than max_rejected_share); their lines are in {REJECTED_TABLE}, the run goes on: "
              f"{', '.join(over_rejected)}", file=sys.stderr)
    return 1 if below_bound else 0


COMMANDS = {"parse": _parse, "load": _load, "tables": _tables}


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] not in COMMANDS:
        print(f"usage: {Path(__file__).name} {{{','.join(COMMANDS)}}} ...", file=sys.stderr)
        return 2
    return COMMANDS[argv[0]](argv[1:])


if __name__ == "__main__":
    sys.exit(main())
