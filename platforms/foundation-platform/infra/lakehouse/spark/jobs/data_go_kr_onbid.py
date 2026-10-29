"""Load 차세대 온비드 public sale from Bronze into Silver (root ADR-0280, ADR-0340).

The collector (`collect-data-go-kr-onbid`) lands, per run, one gzip JSON Lines object per API; each
line is one request: its parameters, its HTTP status and the response body as received. What a load
owes is decided from R2 alone (`plan-silver-load-objects`): the Bronze objects of the source that no
commit of the table records yet, fetched into a plan directory of the load's own run. This job reads
the plan directories and loads each object, oldest first, in one commit of its own that records it:

- an item list object into `silver.onbid_real_estate_item`: every row of an item condition that is
  new or whose content changed, and, only when the object holds the whole list (every
  (prptDivCd, pvctTrgtYn) combination read to its end, `item_pass_complete`), a removal marker for
  each condition with a current row that the pass no longer holds (root ADR-0275,
  `silver_release_compare.release_changes`);
- an item bid result object into `silver.onbid_item_bid_result`: every result answer, as given;
- an item detail object into `silver.onbid_real_estate_item_photo` (its media, by content hash) and
  into `silver.onbid_real_estate_item_detail` (everything else, per item);
- an item bid-info, announcement, announcement bid-result or announcement detail object into its own
  table (root ADR-0340 §6): the keys whose content changed (one row per key; nothing vanishes).

Which provider field becomes which column is not written here: `data-go-kr-onbid.fields.json` (the
field inventory, root ADR-0340 §1) names, per field, the Silver columns it lands in; the column's
type comes from the lakehouse contract. A field under a list (`sqmsList[].clandCont`) lands in the
list's `*_json` column, the list as received, less the private persons' names the inventory marks
`pii` (root ADR-0340 §5). Response keys the inventory does not know are counted per object
(`unrepresented_fields`), so a provider addition is seen at the first load.

An object with nothing to write still commits, empty, so the table records that it was loaded.

The provider's own PNU (`ltnoPnu`) is format-checked (`Pnu::parse`'s copy in the source contract,
held equal by a Rust test) and then resolved to the CURRENT pnu by the one place-identity resolver
(`place_identity_resolver`, root ADR-0288 §1): a superseded legal-dong code is carried old →
current and kept only when it is a current parcel. The raw text stays in `ltno_pnu_raw`; a code
that does not resolve leaves `pnu` null for the steward.

`parse` reads the plan directories and prints what it would load without Spark; `load` writes. The
last stdout line is `data-go-kr-onbid-summary-json {json}`.
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

JOB_NAME = "data_go_kr_onbid"
ITEM_TABLE = "silver.onbid_real_estate_item"
RESULT_TABLE = "silver.onbid_item_bid_result"
PHOTO_TABLE = "silver.onbid_real_estate_item_photo"
DETAIL_TABLE = "silver.onbid_real_estate_item_detail"
BID_INFO_TABLE = "silver.onbid_item_bid_info"
ANNOUNCEMENT_TABLE = "silver.onbid_announcement"
ANNOUNCEMENT_RESULT_TABLE = "silver.onbid_announcement_bid_result"
ANNOUNCEMENT_DETAIL_TABLE = "silver.onbid_announcement_detail"
CONTRACTS_DIR = Path(__file__).resolve().parents[2] / "contracts"
CONTRACT_PATH = CONTRACTS_DIR / "data-go-kr-onbid.contract.json"
FIELDS_PATH = CONTRACTS_DIR / "data-go-kr-onbid.fields.json"
PLAN_SCHEMA = "foundation-platform.silver_load_objects.v1"
SNAPSHOT_ID = re.compile(r"-(\d{8}t\d{6}z)\.gz$")
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
KST = timezone(timedelta(hours=9))
# Written by the load, never by the source: comparing them would make every row look changed.
LOAD_COLUMNS = ("source_record_id", "source_line_number", "source_snapshot_id", "ingested_at_utc", "row_state")
KEY = ("cltr_mng_no", "pbct_cdtn_no")
# role → (the inventory's API, the table it loads into, the source contract's endpoint its objects come from).
ROLES = {
    "item_list": ("item_list", ITEM_TABLE, "item_list"),
    "item_bid_result": ("item_bid_result", RESULT_TABLE, "item_bid_result"),
    "item_photo": ("item_detail", PHOTO_TABLE, "item_detail"),
    "item_detail": ("item_detail", DETAIL_TABLE, "item_detail"),
    "item_bid_info": ("item_bid_info", BID_INFO_TABLE, "item_bid_info"),
    "announcement": ("announcement_list", ANNOUNCEMENT_TABLE, "announcement_list"),
    "announcement_bid_result": ("announcement_bid_result_list", ANNOUNCEMENT_RESULT_TABLE,
                                "announcement_bid_result_list"),
    "announcement_detail": ("announcement_detail", ANNOUNCEMENT_DETAIL_TABLE, "announcement_detail"),
}
# The tables a load compares to their current rows and appends the changed keys of (root ADR-0311).
COMPARED = ("item_detail", "item_bid_info", "announcement", "announcement_bid_result", "announcement_detail")
# Which round answered an item-level response: kept, but not content (another round's answer with the
# same content is no change).
ASKED_THROUGH = ("pbct_cdtn_no",)


def load_source_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def load_inventory() -> dict[str, Any]:
    return json.loads(FIELDS_PATH.read_text(encoding="utf-8"))


def valid_pnu(value: Any, contract: Mapping[str, Any]) -> str | None:
    """The standard PNU of the provider's ltnoPnu, or None.

    The provider writes the 11th digit in the hub dialect (0 ordinary, 1 mountain); it is converted
    with the contract's map, then the result must pass what Pnu::parse checks: the declared digit
    count, ASCII digits and a standard 대장구분 digit.
    """

    rule = contract["pnu"]
    text = str(value or "").strip()
    if len(text) != int(rule["digits"]) or not text.isascii() or not text.isdigit():
        return None
    standard = rule["provider_ledger_kind_digits"].get(text[10])
    if standard is None:
        return None
    pnu = text[:10] + standard + text[11:]
    return pnu if pnu[10] in rule["ledger_kind_digits"] else None


def kst_minute(value: Any) -> datetime | None:
    """`yyyyMMddHHmm` (or with seconds) in Korean time, as naive UTC; None when it is not one."""

    text = str(value or "").strip()
    for fmt in ("%Y%m%d%H%M%S", "%Y%m%d%H%M"):
        try:
            local = datetime.strptime(text, fmt).replace(tzinfo=KST)
        except ValueError:
            continue
        return local.astimezone(timezone.utc).replace(tzinfo=None)
    return None


def ymd(value: Any) -> date | None:
    """`yyyyMMdd` as a date; None when it is not one."""

    try:
        return datetime.strptime(str(value or "").strip(), "%Y%m%d").date()
    except ValueError:
        return None


def whole(value: Any) -> int | None:
    """A whole number from a number or a string of digits; None for anything else."""

    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    text = str(value).strip().replace(",", "")
    return int(text) if re.fullmatch(r"-?\d+", text) else None


def decimal(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(str(value).replace(",", "")) if isinstance(value, str) else float(value)
    except (TypeError, ValueError):
        return None


def text_or_none(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    return text or None


def json_text(value: Any, drop: frozenset[str]) -> str | None:
    """A list (or object) as received, without the keys in `drop`, as compact sorted JSON; None when
    empty. Sorted keys keep one content one text, so an unchanged list is no change."""

    if value in (None, "", [], {}):
        return None
    return json.dumps(without_keys(value, drop), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def without_keys(node: Any, drop: frozenset[str]) -> Any:
    """`node` with the keys in `drop` removed from every object in it."""

    if isinstance(node, dict):
        return {k: without_keys(v, drop) for k, v in node.items() if k not in drop}
    if isinstance(node, list):
        return [without_keys(v, drop) for v in node]
    return node


READERS: dict[str, Callable[[Any], Any]] = {
    "string": text_or_none, "int": whole, "long": whole, "double": decimal, "timestamp": kst_minute, "date": ymd,
}


def answer_header(body: Mapping[str, Any]) -> Mapping[str, Any]:
    # An error body carries its code under `result`, as the collector reads it.
    header = body.get("header")
    return header if isinstance(header, dict) else (body.get("result") or {})


def answer_items(line: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The items of one request's 00 answer; [] for an empty (03) or quota answer."""

    if line.get("http_status") != 200:
        return []
    body = json.loads(line["body"])
    if answer_header(body).get("resultCode") != "00":
        return []
    found = ((body.get("body") or {}).get("items") or {})
    found = found.get("item") if isinstance(found, dict) else None
    if isinstance(found, dict):
        return [found]
    return list(found) if isinstance(found, list) else []


def item_pass_complete(lines: Sequence[Mapping[str, Any]], contract: Mapping[str, Any]) -> bool:
    """Whether an item list object read every (prptDivCd, pvctTrgtYn) combination to its end.

    The same test the collector makes while paging: a combination ends at a 03 answer, at an empty
    page, or once its pages hold totalCount items. A combination the run never reached (budget or
    quota stopped it) has no such line, so the pass is not complete.
    """

    codes = contract["result_codes"]
    combos = {(code, flag) for code in contract["property_division_codes"] if not code.startswith("$")
              for flag in contract["item_list"]["private_contract_flags"]}
    counted: dict[tuple[str, str], int] = {}
    ended = set()
    for line in lines:
        request = line.get("request") or {}
        combo = (str(request.get("prptDivCd")), str(request.get("pvctTrgtYn")))
        if line.get("http_status") != 200:
            continue
        body = json.loads(line["body"])
        code = answer_header(body).get("resultCode")
        if code == codes["no_data"]:
            ended.add(combo)
        elif code == codes["ok"]:
            items = answer_items(line)
            counted[combo] = counted.get(combo, 0) + len(items)
            total = whole((body.get("body") or {}).get("totalCount"))
            if not items or (total is not None and counted[combo] >= total):
                ended.add(combo)
    return combos <= ended


def read_lines(path: Path) -> list[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def top_field(path: str) -> str:
    """The response key a field path starts at: `sqmsList[].clandCont` → `sqmsList`."""

    return path.split("[]", 1)[0].split(".", 1)[0]


@lru_cache(maxsize=None)
def column_sources(api: str, table: str) -> tuple[tuple[str, str], ...]:
    """(column, response key) for every column of `table` the inventory's `api` fields claim, in the
    table's column order. A list column (`*_json`) reads the whole list."""

    from platform_contracts import load_lakehouse_contract  # noqa: PLC0415

    sources: dict[str, str] = {}
    for field in load_inventory()["apis"][api]["fields"]:
        for claim in field.get("silver", []):
            claimed_table, _, column = claim.rpartition(".")
            if claimed_table == table:
                sources.setdefault(column, top_field(field["path"]))
    order = [c["name"] for c in load_lakehouse_contract(table)["columns"]]
    return tuple((column, sources[column]) for column in order if column in sources)


@lru_cache(maxsize=None)
def private_keys(api: str) -> frozenset[str]:
    """The keys the inventory marks `pii` under `api`'s lists (root ADR-0340 §5)."""

    return frozenset(f["path"].rsplit(".", 1)[-1] for f in load_inventory()["apis"][api]["fields"] if f.get("pii"))


@lru_cache(maxsize=None)
def known_fields(api: str) -> frozenset[str]:
    """Every response key the inventory knows for `api`, with the collector's `<field>Media` siblings."""

    contract = load_source_contract()
    tops = {top_field(f["path"]) for f in load_inventory()["apis"][api]["fields"]}
    return frozenset(tops | {f"{field}Media" for field in contract["media"]["fields"]["pipe"]})


def column_types(table: str) -> dict[str, str]:
    from platform_contracts import load_lakehouse_contract  # noqa: PLC0415

    return {c["name"]: c["logical_type"] for c in load_lakehouse_contract(table)["columns"]}


def build_row(item: Mapping[str, Any], role: str, contract: Mapping[str, Any]) -> dict[str, Any]:
    """One table row of one response item, every column from the field the inventory maps to it."""

    api, table, _ = ROLES[role]
    types = column_types(table)
    drop = private_keys(api)
    row: dict[str, Any] = {}
    for column, source in column_sources(api, table):
        value = item.get(source)
        if column == "pnu":
            row[column] = valid_pnu(value, contract)
        elif column.endswith("_json"):
            row[column] = json_text(value, drop)
        else:
            row[column] = READERS[types[column]](value)
    return row


def item_row(item: Mapping[str, Any], contract: Mapping[str, Any]) -> dict[str, Any]:
    return build_row(item, "item_list", contract)


def result_row(item: Mapping[str, Any]) -> dict[str, Any]:
    return build_row(item, "item_bid_result", load_source_contract())


def unrepresented(lines: Sequence[Mapping[str, Any]], api: str) -> list[str]:
    """The response keys of an object's items the inventory does not know (root ADR-0340 §2)."""

    known = known_fields(api)
    return sorted({key for line in lines for item in answer_items(line) for key in item if key not in known})


def rows_of(lines: Sequence[Mapping[str, Any]], build: Any, object_key: str, snapshot_id: str,
            ingested_at: datetime, key: Sequence[str] = KEY, strict: bool = True) -> list[dict[str, Any]]:
    """One row per key of the object, from its last line that holds it.

    The list can shift while a pass pages through it, so a condition may appear on two pages; the
    later answer is the newer one. A row without its key fails the object when `strict` (the item
    tables, whose key every answer carries); otherwise it is skipped (counted by the caller).
    """

    rows: dict[tuple[Any, ...], dict[str, Any]] = {}
    for number, line in enumerate(lines, start=1):
        for item in answer_items(line):
            row = build(item)
            if any(row.get(k) in (None, "") for k in key):
                if strict:
                    raise ValueError(f"{object_key} line {number}: an item without {' and '.join(key)}")
                continue
            row.update(source_record_id=object_key, source_line_number=number, source_snapshot_id=snapshot_id,
                       ingested_at_utc=ingested_at)
            rows[tuple(row[k] for k in key)] = row
    return list(rows.values())


def photo_rows_of(lines: Sequence[Mapping[str, Any]], object_key: str, snapshot_id: str,
                  ingested_at: datetime) -> list[dict[str, Any]]:
    """One row per stored media file of each item: its content hash, kind and order in the list.

    The detail Bronze line carries each `[{urlAdr}]` media entry with a `sha256` ref (root ADR-0290
    §7), and each `|`-joined media field's stored files in `<field>Media` (root ADR-0340 §4); an entry
    the collector could not store has no `sha256`, so it is skipped. The media fields and their kinds
    are the source contract's `media.fields`. The last line that holds an (item, hash) wins, so a
    re-paged item keeps one row.
    """

    fields = load_source_contract()["media"]["fields"]
    sources = [(kind, field, field) for field, kind in fields["list"].items()]
    sources += [(kind, f"{field}Media", field) for field, kind in fields["pipe"].items()]
    rows: dict[tuple[str, int | None, str], dict[str, Any]] = {}
    for number, line in enumerate(lines, start=1):
        for item in answer_items(line):
            cltr = str(item.get("cltrMngNo") or "").strip()
            pbct = whole(item.get("pbctCdtnNo"))
            if not cltr or pbct is None:
                raise ValueError(f"{object_key} line {number}: an item without cltrMngNo and pbctCdtnNo")
            for media_kind, field, _ in sources:
                entries = item.get(field)
                if not isinstance(entries, list):
                    continue
                for position, entry in enumerate(entries):
                    if not isinstance(entry, dict):
                        continue
                    sha = text_or_none(entry.get("sha256"))
                    if sha is None:
                        continue
                    rows[(cltr, pbct, sha)] = {
                        "cltr_mng_no": cltr,
                        "pbct_cdtn_no": pbct,
                        "sha256": sha,
                        "media_kind": media_kind,
                        "sequence": whole(entry.get("sequence")) if "sequence" in entry else position,
                        # The appraisal attachment names its organisation; a photo has no caption field.
                        "title": text_or_none(entry.get("apslEvlOrgNm")),
                        "media_format": text_or_none(entry.get("media_format")),
                        "size_bytes": whole(entry.get("size_bytes")),
                        "source_record_id": object_key,
                        "source_line_number": number,
                        "source_snapshot_id": snapshot_id,
                        "ingested_at_utc": ingested_at,
                    }
    return list(rows.values())


def source_slugs(contract: Mapping[str, Any]) -> dict[str, str]:
    """Source slug → the role its objects load as when the plan names no table (root ADR-0014
    `{provider}__{dataset}`). The item detail feeds two tables; its plan names which."""

    slugs: dict[str, str] = {}
    for role, (_, _, endpoint) in ROLES.items():
        slugs.setdefault(f"datagokr__{contract[endpoint]['dataset_slug']}", role)
    return slugs


def table_roles() -> dict[str, str]:
    return {table: role for role, (_, table, _) in ROLES.items()}


def snapshot_id(object_key: str) -> str:
    """The run id the collector put in the key (`...-20990102t030405z.gz` → `20990102T030405Z`)."""

    found = SNAPSHOT_ID.search(object_key)
    if not found:
        raise ValueError(f"{object_key} carries no run id")
    return found.group(1).upper()


def planned_objects(plan_dirs: Sequence[Path], contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Every object the plans fetched, with its role and local file, oldest run first."""

    roles = source_slugs(contract)
    by_table = table_roles()
    objects = []
    for plan_dir in plan_dirs:
        plan = json.loads((plan_dir / "plan.json").read_text(encoding="utf-8"))
        if plan.get("schema_version") != PLAN_SCHEMA:
            raise ValueError(f"{plan_dir}: not a {PLAN_SCHEMA} plan")
        if plan.get("table") is not None and plan["table"] not in by_table:
            raise ValueError(f"{plan_dir}: {plan['table']} is not an Onbid table this job loads")
        for entry in plan["objects"]:
            if entry["source_slug"] not in roles:
                raise ValueError(f"{entry['object_key']}: {entry['source_slug']} is not an Onbid source this job loads")
            role = by_table[plan["table"]] if plan.get("table") is not None else roles[entry["source_slug"]]
            endpoint = ROLES[role][2]
            if entry["source_slug"] != f"datagokr__{contract[endpoint]['dataset_slug']}":
                raise ValueError(f"{entry['object_key']}: {entry['source_slug']} does not load into {ROLES[role][1]}")
            objects.append({"object_key": entry["object_key"], "role": role,
                            "source_snapshot_id": snapshot_id(entry["object_key"]),
                            "path": plan_dir / entry["local_path"]})
    return sorted(objects, key=lambda o: (o["source_snapshot_id"], o["object_key"]))


def table_key(role: str) -> tuple[str, ...]:
    from platform_contracts import load_lakehouse_contract  # noqa: PLC0415

    return tuple(load_lakehouse_contract(ROLES[role][1])["current_row_latest"]["key"])


def read_object(entry: Mapping[str, Any], contract: Mapping[str, Any], ingested_at: datetime) -> dict[str, Any]:
    """What one object loads: its rows, for an item list whether the pass was complete, and the
    response keys the inventory does not know."""

    lines = read_lines(entry["path"])
    role = entry["role"]
    loads: dict[str, Any] = {"complete": False, "unrepresented_fields": unrepresented(lines, ROLES[role][0])}
    if role == "item_photo":
        loads["rows"] = photo_rows_of(lines, entry["object_key"], entry["source_snapshot_id"], ingested_at)
        return loads
    strict = role in ("item_list", "item_bid_result")
    loads["rows"] = rows_of(lines, lambda item: build_row(item, role, contract), entry["object_key"],
                            entry["source_snapshot_id"], ingested_at, key=KEY if strict else table_key(role),
                            strict=strict)
    loads["complete"] = role == "item_list" and item_pass_complete(lines, contract)
    return loads


def content_columns(contract: Mapping[str, Any]) -> list[str]:
    key = tuple(contract.get("current_row_latest", {}).get("key") or KEY)
    return [c["name"] for c in contract["columns"] if c["name"] not in (*KEY, *key, *LOAD_COLUMNS, *ASKED_THROUGH)]


def changed_only(staged: Any, current: Any, key: Sequence[str], content: Sequence[str]) -> Any:
    """The staged rows (one per key) whose key is new or whose content differs from its current row
    (root ADR-0311: append only the changed keys). Null-ness is hashed apart from the value, so a
    null and an empty string differ, as `silver_release_compare` does."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    fingerprint = F.xxhash64(*(F.col(c).isNull() for c in content), *(F.col(c) for c in content))
    seen = current.select(*key, fingerprint.alias("_fingerprint"))
    return staged.withColumn("_fingerprint", fingerprint).join(seen, [*key, "_fingerprint"], "left_anti") \
        .drop("_fingerprint")


def _summary_line(summary: Mapping[str, Any]) -> None:
    print("data-go-kr-onbid-summary-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True, default=str))


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
        loads = read_object(entry, contract, now)
        line = {"object": entry["object_key"], "role": entry["role"], "rows": len(loads["rows"]),
                "unrepresented_fields": loads["unrepresented_fields"]}
        if entry["role"] == "item_list":
            line.update(complete=loads["complete"], rows_with_pnu=sum(1 for row in loads["rows"] if row["pnu"]))
        report.append(line)
    _summary_line({"job": JOB_NAME, "status": "parsed", "objects": report})
    return 0


def ensure_table(spark: Any, args: argparse.Namespace, contract_table: str) -> tuple[str, dict[str, Any], list[str], str]:
    """Creates the table when it is new and evolves it to its contract; its name, contract, columns, schema."""

    from platform_contracts import (  # noqa: PLC0415
        column_names, create_table_columns_sql, evolve_iceberg_table_to_contract, load_lakehouse_contract,
        partition_clause_sql, spark_sql_type,
    )

    contract = load_lakehouse_contract(contract_table)
    name = contract_table.split(".", 1)[1] + args.table_suffix
    table = f"`{args.iceberg_catalog_name}`.`{args.iceberg_namespace}`.`{name}`"
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
        parser.error("writing the Silver tables needs --allow-non-smoke-write")

    import address_pnu  # noqa: PLC0415
    import place_identity_resolver  # noqa: PLC0415
    from lakehouse_engine import apply_catalog_settings, assert_catalog_env  # noqa: PLC0415
    from lakehouse_ingest import append_source_object  # noqa: PLC0415
    from platform_contracts import REMOVED, current_rows  # noqa: PLC0415
    from pyspark.sql import SparkSession, functions as F  # noqa: PLC0415
    from silver_release_compare import release_changes  # noqa: PLC0415

    assert_catalog_env()
    source = load_source_contract()
    objects = planned_objects([Path(d) for d in args.plan_dir], source)
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    summary: dict[str, Any] = {"job": JOB_NAME, "objects": [], "status": "loaded"}
    builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()

    try:
        # Every table exists after any load, empty or not: the 공매 Gold reads them all as required
        # inputs (root ADR-0340 §7), and a missing one would refuse its rebuild.
        tables = {role: ensure_table(spark, args, ROLES[role][1]) for role in ROLES}
        if any(entry["role"] == "item_list" for entry in objects):
            # ADR-0288 §1: every parcel source resolves its code to the current pnu here, not by
            # copying the source code. The pairs and the current parcels are read once for the run.
            remap, ambiguous = place_identity_resolver.pairs_from_lakehouse(spark, args.iceberg_catalog_name)
            parcels, _titles = address_pnu.lakehouse_inputs(spark, args.iceberg_catalog_name)
        for entry in objects:
            loads = read_object(entry, source, now)
            rows = loads["rows"]
            role = entry["role"]
            table, table_contract, columns, schema = tables[role]
            line: dict[str, Any] = {"object": entry["object_key"], "role": role, "rows": len(rows),
                                    "unrepresented_fields": loads["unrepresented_fields"]}
            frame = spark.createDataFrame([tuple(r.get(c) for c in columns) for r in rows], schema=schema)
            if role == "item_list":
                # The source code (ltnoPnu, kept in ltno_pnu_raw) becomes the CURRENT pnu, or null.
                resolved = place_identity_resolver.resolve(frame, remap, ambiguous, parcels).cache()
                graded = {row["place_identity_grade"]: row["count"]
                          for row in resolved.groupBy("place_identity_grade").count().collect()}
                staged = resolved.select(*columns)
                current = current_rows(spark.table(table), table_contract)
                changes = release_changes(
                    staged, current, key=KEY, content=content_columns(table_contract), removal="row_state",
                    source_snapshot_id=entry["source_snapshot_id"], ingested_at=F.lit(now).cast("timestamp"),
                    columns=columns,
                )
                if not loads["complete"]:
                    # A pass that stopped early says nothing about what it did not read.
                    changes = changes.where(F.col("row_state").isNull() | (F.col("row_state") != REMOVED))
                # A marker is made from the condition's last row; it belongs to this object's load.
                changes = changes.withColumn("source_record_id", F.lit(entry["object_key"])).cache()
                line.update(complete=loads["complete"], source_coded=sum(1 for row in rows if row["pnu"]),
                            resolved_by_grade=graded,
                            changes=changes.count(), removed=changes.where(F.col("row_state") == REMOVED).count())
                line["appended"] = append_source_object(spark, changes, columns, table, entry["object_key"])
                changes.unpersist()
                resolved.unpersist()
            elif role in COMPARED:
                key = list(table_contract["current_row_latest"]["key"])
                current = current_rows(spark.table(table), table_contract)
                changes = changed_only(frame, current, key, content_columns(table_contract)).cache()
                line["changes"] = changes.count()
                line["appended"] = append_source_object(spark, changes, columns, table, entry["object_key"])
                changes.unpersist()
            else:
                line["appended"] = append_source_object(spark, frame, columns, table, entry["object_key"])
            summary["objects"].append(line)
    finally:
        spark.stop()
    _summary_line(summary)
    return 0


def _tables(argv: Sequence[str]) -> int:
    """`<table> <source slug>` per line: what the load script plans (one plan per table)."""

    del argv
    contract = load_source_contract()
    for _, table, endpoint in ROLES.values():
        print(table, f"datagokr__{contract[endpoint]['dataset_slug']}")
    return 0


COMMANDS = {"parse": _parse, "load": _load, "tables": _tables}


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] not in COMMANDS:
        print(f"usage: {Path(__file__).name} {{{','.join(COMMANDS)}}} ...", file=sys.stderr)
        return 2
    return COMMANDS[argv[0]](argv[1:])


if __name__ == "__main__":
    sys.exit(main())
