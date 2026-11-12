"""Load 법원경매정보 list rows from Bronze into Silver (root ADR-0280, ADR-0281).

The collector (`collect-courtauction-go-kr`) lands, per night, one gzip JSON Lines object per search;
each line is one page request with the response body as received. What a load owes is decided from
R2 alone (`plan-silver-load-objects`): the objects of each search that no commit of its table records
yet, fetched into a plan directory of the load's own run. For each object, oldest first, this job:

- reads the rows (`data.dlt_srchResult`) of every 200 answer, one row per 물건 (court office, case,
  property and item sequence), the later answer winning when the list shifted between pages;
- gives each row its PNU through the one place-identity resolver (root ADR-0288, ADR-0382): the
  representative parcel's legal-dong code with its lot (`source_code_pnu_raw`) carried to the current
  code (`place_identity_resolver.resolve`); a row that path leaves unresolved, by its address
  (`address_pnu`): the 행정구역 names with the representative lot as a jibun address, else the road
  name with the building number;
- appends to `silver.court_auction_item` the rows whose content changed and, only when the night read
  the item search completely, a removal marker for each 물건 that left it (root ADR-0275); to
  `silver.court_auction_sale_result` the rows whose content changed, never a marker; whether the
  night read the item search completely is read from the lines themselves (every condition read to
  its last page);
- commits each object on its own, empty if need be, so the table records it;
- for a 물건상세 object (root ADR-0384): appends to `silver.court_auction_item_photo` the case photos
  (by content hash) not yet recorded, then to `silver.court_auction_item_detail` one row per read,
  failed reads included: the record the collector's next detail frontier is computed from
  (root ADR-0352). The photo table commits first, so an object the detail table records has its
  photos. Between them, the document's parts (root ADR-0540): `silver.court_auction_case` and
  `silver.court_auction_item_spec` the rows whose content changed, the lists (기일내역, 목록내역,
  제시외건물, 감정요항표) a whole edition per property or case whose list changed. A field that names a
  person is not read; free text has its names masked (the contract's `personal_names`).

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
import code_change_follow as follow
import place_identity_resolver

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
DETAIL_ROLE = "item_detail"
DETAIL_TABLE = "silver.court_auction_item_detail"
PHOTO_TABLE = "silver.court_auction_item_photo"
PHOTO_KEY = ("court_office_code", "case_no", "sha256")
CASE = ("court_office_code", "case_no")
PROPERTY = (*CASE, "property_seq")
# The 물건상세 document, one Silver table per part (root ADR-0540 §4). A case or spec row is the newest
# per key; a list is one edition per property (or case) and is appended whole when it changed.
CASE_TABLE = "silver.court_auction_case"
SPEC_TABLE = "silver.court_auction_item_spec"
SCHEDULE_TABLE = "silver.court_auction_item_schedule"
OBJECT_TABLE = "silver.court_auction_item_object"
ANNEX_TABLE = "silver.court_auction_item_annex"
APPRAISAL_POINT_TABLE = "silver.court_auction_appraisal_point"
LATEST_KEYS = {CASE_TABLE: CASE, SPEC_TABLE: PROPERTY}
EDITION_PER = {SCHEDULE_TABLE: PROPERTY, OBJECT_TABLE: PROPERTY, ANNEX_TABLE: PROPERTY, APPRAISAL_POINT_TABLE: CASE}
DOCUMENT_TABLES = (*LATEST_KEYS, *EDITION_PER)
# What one 물건상세 object commits, in order: its photos, its documents, then the read record the
# collector's next frontier is computed from, so an object the record holds has everything else.
DETAIL_TABLES = (PHOTO_TABLE, *DOCUMENT_TABLES, DETAIL_TABLE)


def load_source_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


FROM_CODE = load_source_contract()["pnu_resolution"]["from_code"]
PERSONAL_NAMES = load_source_contract()["personal_names"]
# A listed object's legal-dong code: the 물건상세's own four fields, of the list rows' widths.
OBJECT_CODE = {"legal_dong_code_fields": load_source_contract()["collection"]["pnu_assembly"]["legal_dong_code_fields"],
               "legal_dong_code_widths": FROM_CODE["legal_dong_code_widths"]}


# The one-syllable particles a name may be followed by in the site's text.
PARTICLES = ("은", "는", "이", "가", "의", "에", "와", "과", "도", "로")


def name_patterns(rules: Mapping[str, Any]) -> tuple[re.Pattern[str], re.Pattern[str]]:
    """(a name before a `before` word, a name after an `after` word) of the contract's rules."""

    low, high = rules["name_syllables"]
    word = f"[가-힣]{{{low},{high}}}"
    before = "|".join(map(re.escape, rules["before"]))
    after = "|".join(map(re.escape, sorted(rules["after"], key=len, reverse=True)))
    ends = f"(?=[^가-힣]|$|{'|'.join(PARTICLES)}|씨|님)"
    # A role word starts a word ("다세대주택" holds no 세대주) and is set off from the name; the names
    # a comma joins to the first are masked too ("소유자 ○○○, ○○○").
    return (re.compile(f"({word})(?=\\s*(?:의\\s*)?(?:{before}))"),
            re.compile(f"((?<![가-힣])(?:{after})(?:\\s+|\\s*[:：(]\\s*))"
                       f"({word}{ends}(?:\\s*,\\s*(?!{after}){word}{ends})*)"))


NAME_PATTERNS = name_patterns(PERSONAL_NAMES)


def mask_names(text: str | None, rules: Mapping[str, Any] = PERSONAL_NAMES,
               patterns: tuple[re.Pattern[str], re.Pattern[str]] = NAME_PATTERNS) -> str | None:
    """``text`` with each person's name the contract's rules recognise replaced by its mask (root
    ADR-0540 §4). Bronze keeps the text as received."""

    if not text:
        return text
    stop, mask, low = set(rules["not_names"]), rules["mask"], rules["name_syllables"][0]

    def masked(word: str) -> str:
        # A particle the word ends in is kept, and the word is judged without it ("미상의" is "미상").
        stem, tail = (word[:-1], word[-1]) if word[-1] in PARTICLES and len(word) > low else (word, "")
        return word if stem in stop or word in stop else mask + tail

    def names(listed: str) -> str:
        return re.sub("[가-힣]+", lambda m: masked(m.group(0)), listed)

    before, after = patterns
    text = before.sub(lambda m: masked(m.group(1)), text)
    return after.sub(lambda m: m.group(1) + names(m.group(2)), text)


def masked_text(value: Any) -> str | None:
    return mask_names(text_or_none(value))


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


def code_of(item: Mapping[str, Any], from_code: Mapping[str, Any]) -> str | None:
    """The row's 10-digit legal-dong code from its four code fields, each of its width, or None."""

    parts = [str(item.get(name) or "").strip() for name in from_code["legal_dong_code_fields"]]
    if [len(p) for p in parts] != list(from_code["legal_dong_code_widths"]) or not all(p.isdigit() for p in parts):
        return None
    return "".join(parts)


def item_row(item: Mapping[str, Any]) -> dict[str, Any]:
    sold = whole(item.get("maeAmt"))
    return {
        "source_code_pnu_raw": place_identity_resolver.code_lot_pnu(code_of(item, FROM_CODE),
                                                                    item.get(FROM_CODE["lot_field"])),
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


def search_complete(lines: Sequence[Mapping[str, Any]], search: Mapping[str, Any]) -> bool:
    """Whether the lines read every condition of a search to its last page.

    The same test the collector makes while paging: a condition ends at an empty page or once its
    pages hold totalCnt rows. A night stopped by a refusal, the request bound or the end of the quiet
    hours has a condition without such a line. When the search has a `date_window` (root ADR-0290
    §2), a request without both of its fields does not count: the server's default range slides
    daily, so such a pass cannot say what left the list.
    """

    conditions = search["conditions"]
    fields = sorted({name for condition in conditions for name in condition})
    window = search.get("date_window")
    read: dict[str, int] = {}
    ended = set()
    for line in lines:
        if line.get("http_status") != 200:
            continue
        try:
            data = json.loads(line["body"]).get("data") or {}
        except (TypeError, ValueError, AttributeError):
            continue
        asked = (line.get("request") or {}).get("dma_srchGdsDtlSrchInfo") or {}
        if window and not (asked.get(window["start_field"]) and asked.get(window["end_field"])):
            continue
        key = condition_key({name: asked.get(name) for name in fields})
        rows = data.get("dlt_srchResult")
        count = len(rows) if isinstance(rows, list) else 0
        read[key] = read.get(key, 0) + count
        total = whole((data.get("dma_pageInfo") or {}).get("totalCnt"))
        if count == 0 or (total is not None and read[key] >= total):
            ended.add(key)
    return {condition_key({name: c.get(name) for name in fields}) for c in conditions} <= ended


def search_roles(contract: Mapping[str, Any]) -> dict[str, str]:
    """Source slug → the search its objects hold (root ADR-0014 `{provider}__{dataset}`)."""

    return {f"courtauctiongokr__{contract[role]['dataset_slug']}": role for role in (*ROLE_TABLE, DETAIL_ROLE)}


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
    seen: set[str] = set()
    for plan_dir in plan_dirs:
        plan = json.loads((plan_dir / "plan.json").read_text(encoding="utf-8"))
        if plan.get("schema_version") != PLAN_SCHEMA:
            raise ValueError(f"{plan_dir}: not a {PLAN_SCHEMA} plan")
        for entry in plan["objects"]:
            if entry["source_slug"] not in roles:
                raise ValueError(f"{entry['object_key']}: {entry['source_slug']} is not a search this job loads")
            # A 물건상세 object is planned against its read record and its last document table (each
            # records it on its own commit), so two plans may name it; it is loaded once.
            if entry["object_key"] in seen:
                continue
            seen.add(entry["object_key"])
            objects.append({"object_key": entry["object_key"], "role": roles[entry["source_slug"]],
                            "source_snapshot_id": snapshot_id(entry["object_key"]),
                            "path": plan_dir / entry["local_path"]})
    return sorted(objects, key=lambda o: (o["source_snapshot_id"], o["object_key"]))


def read_object(entry: Mapping[str, Any], contract: Mapping[str, Any], prefixes: Mapping[str, str],
                dongs: Mapping[tuple[str, str], str], ingested_at: datetime) -> dict[str, Any]:
    """What one object loads: its rows and whether its search was read completely. ``prefixes`` are
    ``address_pnu.sido_prefixes`` of the 법정동 code table (root ADR-0403)."""

    with gzip.open(entry["path"], "rt", encoding="utf-8") as handle:
        lines = [json.loads(line) for line in handle if line.strip()]
    if entry["role"] == DETAIL_ROLE:
        rows = detail_rows_of(lines, contract[DETAIL_ROLE], entry["object_key"], entry["source_snapshot_id"], ingested_at)
        return {"object_key": entry["object_key"], "complete": True, **rows}
    return {"object_key": entry["object_key"],
            "complete": search_complete(lines, contract[entry["role"]]),
            "rows": rows_of(lines, entry["object_key"], entry["source_snapshot_id"], ingested_at,
                            prefixes, dongs)}


def detail_rows_of(lines: Sequence[Mapping[str, Any]], detail: Mapping[str, Any], object_key: str, snapshot_id: str,
                   ingested_at: datetime) -> dict[str, list[dict[str, Any]]]:
    """A 물건상세 object's rows: one detail row per read (line), and one photo row per stored photo of
    a case, the last line naming it winning. The request key is read back from the line's request
    (`item_detail.key_fields`), so a failed read still names its property."""

    photos = detail["photos"]
    request_field = {source: request for request, source in detail["key_fields"].items()}
    details: list[dict[str, Any]] = []
    found: dict[tuple, dict[str, Any]] = {}
    # Per table, per key (case or spec) or per edition group (a list): the last line naming it wins.
    documents: dict[str, dict[tuple, Any]] = {table: {} for table in DOCUMENT_TABLES}
    for number, line in enumerate(lines, start=1):
        asked = (line.get("request") or {}).get(detail["request_object"]) or {}
        court, case, seq = (asked.get(request_field[name]) for name in ("boCd", "saNo", "maemulSer"))
        if not court or not case or whole(seq) is None:
            raise ValueError(f"{object_key} line {number}: a detail read without its key")
        load = {"source_record_id": object_key, "source_line_number": number, "source_snapshot_id": snapshot_id,
                "ingested_at_utc": ingested_at}
        details.append({"court_office_code": str(court), "case_no": str(case), "property_seq": whole(seq),
                        "requested_at_utc": _utc(line.get("requested_at_utc")) or ingested_at,
                        "http_status": whole(line.get("http_status")), "read_outcome": line.get("read_outcome") or "read",
                        "list_fingerprint": text_or_none(line.get("list_fingerprint")),
                        "photo_count": whole(line.get("photo_count")), **load})
        if line.get("read_outcome") != "read":
            continue
        try:
            body = json.loads(line.get("body") or "")
        except ValueError:
            continue
        result = body
        for part in detail["result_pointer"].strip("/").split("/"):
            result = result.get(part) if isinstance(result, dict) else None
        if isinstance(result, dict):
            keyed = {"court_office_code": str(court), "case_no": str(case), "property_seq": whole(seq)}
            for table, rows in document_rows(result, keyed).items():
                group = EDITION_PER.get(table) or LATEST_KEYS[table]
                stamped = [{**row, **load} for row in rows]
                if table in LATEST_KEYS:
                    documents[table].update({tuple(row[k] for k in group): row for row in stamped})
                else:
                    documents[table][tuple(keyed[k] for k in group)] = stamped
        for photo in (result or {}).get(photos["list_field"]) or []:
            sha256 = text_or_none(photo.get("sha256"))
            if not sha256 or not text_or_none(photo.get("media_format")):
                continue  # not stored (the collector counted it), so nothing to point at
            row = {"court_office_code": text_or_none(photo.get("cortOfcCd")) or str(court),
                   "case_no": text_or_none(photo.get("csNo")) or str(case), "sha256": sha256,
                   "photo_kind_code": text_or_none(photo.get(photos["kind_field"])),
                   "sequence": whole(photo.get(photos["sequence_field"])),
                   "page_sequence": whole(photo.get(photos["page_field"])),
                   "title": text_or_none(photo.get(photos["title_field"])),
                   "media_format": photo["media_format"], "size_bytes": whole(photo.get("size_bytes")), **load}
            found[(row["court_office_code"], row["case_no"], sha256)] = row
    return {"rows": details, "photo_rows": list(found.values()),
            "documents": {table: [row for value in rows.values() for row in (value if isinstance(value, list) else [value])]
                          for table, rows in documents.items()}}


def _list(value: Any) -> list[Mapping[str, Any]]:
    return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


def _sold(value: Any) -> int | None:
    amount = whole(value)
    return amount if amount else None  # The site writes 0 for an unsold item.


def document_rows(result: Mapping[str, Any], keyed: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """A 물건상세 result's rows for each document table (root ADR-0540 §4), keyed by the read's
    request (``keyed``: court office, case, property). The fields that name a person (the share text
    `dspslStkCtt`, the 사법보좌관 `jdgeAojAsstnNm`) are not read; free text is masked."""

    case_key = {k: keyed[k] for k in CASE}
    base = result.get("csBaseInfo") if isinstance(result.get("csBaseInfo"), Mapping) else {}
    deadlines = [d for d in (ymd(item.get("dstrtDemnLstprdYmd")) for item in _list(result.get("dstrtDemnInfo"))) if d]
    out: dict[str, list[dict[str, Any]]] = {table: [] for table in DOCUMENT_TABLES}
    if base:
        out[CASE_TABLE].append({
            **case_key, "court_name": text_or_none(base.get("cortOfcNm")),
            "court_branch_name": text_or_none(base.get("cortSptNm")), "user_case_no": text_or_none(base.get("userCsNo")),
            "case_name": text_or_none(base.get("csNm")), "case_received_date": ymd(base.get("csRcptYmd")),
            "commencement_date": ymd(base.get("csCmdcYmd")), "claim_amount_krw": whole(base.get("clmAmt")),
            "department_code": text_or_none(base.get("jdbnCd")), "department_name": text_or_none(base.get("cortAuctnJdbnNm")),
            "department_phone": text_or_none(base.get("jdbnTelno")),
            "execution_officer_phone": text_or_none(base.get("execrCsTelno")),
            "case_progress_status_code": text_or_none(base.get("csProgStatCd")),
            "suspension_status_code": text_or_none(base.get("auctnSuspStatCd")),
            "closure_kind_code": text_or_none(base.get("ultmtDvsCd")), "closed_date": ymd(base.get("csUltmtYmd")),
            "duplicate_merge_kind_code": text_or_none(base.get("auctnDpcnMrgDvsCd")),
            "appeal_yn": text_or_none(base.get("rletApalYn")), "lawsuit_kind_code": text_or_none(base.get("lwstDvsCd")),
            "dividend_demand_deadline": max(deadlines) if deadlines else None,
        })
    spec = result.get("dspslGdsDxdyInfo") if isinstance(result.get("dspslGdsDxdyInfo"), Mapping) else {}
    if spec:
        out[SPEC_TABLE].append({
            **keyed, "item_usage_code": text_or_none(spec.get("auctnGdsUsgCd")),
            "item_status_code": text_or_none(spec.get("auctnGdsStatCd")),
            "schedule_item_status_code": text_or_none(spec.get("auctnDxdyGdsStatCd")),
            "real_item_kind": text_or_none(spec.get("realMulKind")), "appraisal_amount_krw": whole(spec.get("aeeEvlAmt")),
            "minimum_bid_price_krw": whole(spec.get("fstPbancLwsDspslPrc")), "failed_bid_count": whole(spec.get("flbdNcnt")),
            "bid_kind_code": text_or_none(spec.get("bidDvsCd")), "sale_date": ymd(spec.get("dspslDxdyYmd")),
            "sale_time_raw": text_or_none(spec.get("fstDspslHm")), "sale_place": text_or_none(spec.get("dspslPlcNm")),
            "bid_start_date": ymd(spec.get("bidBgngYmd")), "bid_end_date": ymd(spec.get("bidEndYmd")),
            "sale_decision_date": ymd(spec.get("dspslDcsnDxdyYmd")),
            "sale_decision_time_raw": text_or_none(spec.get("dspslDcsnHm")),
            "sale_decision_place": text_or_none(spec.get("dspslDcsnPlcNm")),
            "deposit_rate_percent": whole(spec.get("prchDposRate")), "spec_written_date": ymd(spec.get("gdsSpcfcWrtYmd")),
            "assumed_rights": masked_text(spec.get("ndstrcRghCtt")), "superficies": masked_text(spec.get("sprfcExstcDts")),
            "spec_remarks": masked_text(spec.get("gdsSpcfcRmk")),
            "first_rank_setting": masked_text(spec.get("tprtyRnkHypthcStngDts")),
            "item_remarks": masked_text(spec.get("dspslGdsRmk")),
            "spec_document_id": text_or_none(spec.get("dspslGdsSpcfcEcdocId")),
        })
    for n, item in enumerate(_list(result.get("gdsDspslDxdyLst")), start=1):
        out[SCHEDULE_TABLE].append({
            **keyed, "schedule_seq": n, "schedule_date": ymd(item.get("dxdyYmd")),
            "schedule_time_raw": text_or_none(item.get("dxdyHm")), "schedule_kind_code": text_or_none(item.get("auctnDxdyKndCd")),
            "schedule_place": text_or_none(item.get("dxdyPlcNm")), "result_code": text_or_none(item.get("auctnDxdyRsltCd")),
            "schedule_item_status_code": text_or_none(item.get("auctnDxdyGdsStatCd")),
            "minimum_bid_price_krw": _sold(item.get("tsLwsDspslPrc")), "sale_amount_krw": _sold(item.get("dspslAmt")),
            "bid_start_date": ymd(item.get("bidBgngYmd")), "bid_end_date": ymd(item.get("bidEndYmd")),
        })
    seen_objects: set[int] = set()
    for item in _list(result.get("gdsDspslObjctLst")):
        seq = whole(item.get("dspslObjctSeq"))
        if seq is None or seq in seen_objects:
            continue
        seen_objects.add(seq)
        out[OBJECT_TABLE].append({
            **keyed, "object_seq": seq, "list_kind_code": text_or_none(item.get("auctnLstDvsCd")),
            "realty_kind": text_or_none(item.get("rletDvsDts")), "building_structure": text_or_none(item.get("pjbBuldList")),
            "area_raw": text_or_none(item.get("objctArDts")), "land_category": text_or_none(item.get("ldcgDts")),
            "appraisal_amount_krw": whole(item.get("aeeEvlAmt")),
            "usage_lcls_code": text_or_none(item.get("lclDspslGdsLstUsgCd")),
            "usage_mcls_code": text_or_none(item.get("mclDspslGdsLstUsgCd")),
            "usage_scls_code": text_or_none(item.get("sclDspslGdsLstUsgCd")),
            "legal_dong_code": code_of(item, OBJECT_CODE), "lot_raw": text_or_none(item.get("rprsLtnoAddr")),
            "source_pnu_raw": text_or_none(item.get("pnuNoCtt")), "building_name": text_or_none(item.get("bldNm")),
            "building_unit_raw": text_or_none(item.get("bldDtlDts")), "print_address_raw": text_or_none(item.get("userPrintSt")),
            "x_raw": decimal(item.get("stXcrd")), "y_raw": decimal(item.get("stYcrd")),
            "share_numerator": decimal(item.get("dspslStkNmrtVal")) or None,
            "share_denominator": decimal(item.get("dspslStkDnmnVal")) or None,
            "register_read_yn": text_or_none(item.get("rgstRdngYn")),
        })
    annex_seen: dict[int, int] = {}
    for group in result.get("gdsNotSugtBldLsstAll") or []:
        for item in _list(group):
            seq = whole(item.get("dspslObjctSeq"))
            if seq is None:
                continue
            annex_seen[seq] = annex_seen.get(seq, 0) + 1
            out[ANNEX_TABLE].append({
                **keyed, "object_seq": seq, "annex_seq": annex_seen[seq], "usage_raw": text_or_none(item.get("etcUsgCtt")),
                "structure_raw": text_or_none(item.get("bldStrcDts")), "area_raw": text_or_none(item.get("bldArDts")),
                "appraisal_amount_krw": whole(item.get("evlAmt")), "remarks": masked_text(item.get("sugtBsdsBldRmk")),
            })
    for n, item in enumerate(_list(result.get("aeeWevlMnpntLst")), start=1):
        out[APPRAISAL_POINT_TABLE].append({
            **case_key, "point_seq": n, "table_code": text_or_none(item.get("aeeWevlMnpntTbltDvsCd")),
            "item_code": text_or_none(item.get("aeeWevlMnpntItmCd")), "detail_seq": whole(item.get("aeeWevlMnpntDtlSeq")),
            "content": masked_text(item.get("aeeWevlMnpntCtt")),
        })
    return out


def _utc(value: Any) -> datetime | None:
    """An RFC 3339 time as a naive UTC datetime, the tables' timestamp form."""

    from platform_contracts import rfc3339_utc  # noqa: PLC0415

    return rfc3339_utc(value)


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


def link(offered: Any, names: Sequence[str], parcels: Any, titles: Any,
         crosswalk: tuple[Mapping[str, str], Any]) -> Any:
    """The offered rows with their PNU: the code path (``place_identity_resolver.resolve`` with
    ``crosswalk``) first, the address joins (``address_pnu.resolve``) for what it leaves unresolved.
    A night's load and a code-change follow both link through here (root ADR-0382, ADR-0416)."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    remap, ambiguous = crosswalk
    resolved = address_pnu.resolve(
        offered.withColumnRenamed("road_name", "road_name_raw").withColumnRenamed("road_name_key", "road_name"),
        parcels, titles, remap)
    if "source_code_pnu_raw" in names:
        by_code = place_identity_resolver.resolve(
            offered.select("row_id", F.col("source_code_pnu_raw").alias("pnu")), remap, ambiguous, parcels
        ).select("row_id", F.col("pnu").alias("_code_pnu"), F.col("place_identity_grade").alias("_grade"))
        resolved = resolved.join(by_code, "row_id", "left")
        by_code_ok = F.col("_code_pnu").isNotNull()
        resolved = (resolved
                    .withColumn("pnu", F.coalesce(F.col("_code_pnu"), F.col("pnu")))
                    .withColumn("pnu_basis", F.when(by_code_ok & (F.col("_grade") == F.lit("code_superseded")),
                                                    F.lit("legal_dong_code_superseded"))
                                .when(by_code_ok & (F.col("_grade") == F.lit(place_identity_resolver.SERVED_EDITION_GRADE)),
                                      F.lit("legal_dong_code_served_edition"))
                                .when(by_code_ok, F.lit("legal_dong_code")).otherwise(F.col("pnu_basis")))
                    .withColumn("pnu_candidate_count", F.when(by_code_ok, F.lit(1)).otherwise(F.col("pnu_candidate_count")))
                    .withColumn("pnu_unresolved_reason",
                                F.when(by_code_ok, F.lit(None).cast("string")).otherwise(F.col("pnu_unresolved_reason")))
                    .withColumn("legal_dong_code",
                                F.when(by_code_ok, F.substring(F.col("pnu"), 1, 10)).otherwise(F.col("legal_dong_code")))
                    .drop("_code_pnu", "_grade"))
    return resolved


def search_changes(spark: Any, role: str, search: Mapping[str, Any], contract: Mapping[str, Any], parcels: Any,
                   titles: Any, current: Any, snapshot_id: str, now: datetime,
                   crosswalk: tuple[Mapping[str, str], Any] = ({}, ())) -> tuple[Any, Any]:
    """(staged rows, the rows to append) for one search of one night.

    The rows go through the code path (``place_identity_resolver.resolve`` with ``crosswalk``, the
    pairs of ``pairs_from_lakehouse``), then the address joins (``address_pnu.resolve``) for what
    it leaves unresolved, and are compared with the table's current rows (``release_changes``). A
    removal marker survives only for a complete item search.
    """

    from platform_contracts import column_names  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415
    from silver_release_compare import release_changes  # noqa: PLC0415

    columns = list(column_names(contract))
    names, ddl = offered_schema(contract)
    offered = spark.createDataFrame(
        [tuple(n if name == "row_id" else row.get(name) for name in names) for n, row in enumerate(search["rows"])],
        schema=ddl)
    resolved = link(offered, names, parcels, titles, crosswalk)
    staged = (resolved.drop("road_name").withColumnRenamed("road_name_raw", "road_name")
              .withColumn("row_state", F.lit(None).cast("string")).select(*columns))
    changes = release_changes(staged, current, key=KEY, content=content_columns(contract), removal="row_state",
                              source_snapshot_id=snapshot_id, ingested_at=F.lit(now).cast("timestamp"), columns=columns)
    if role != "item_search" or not search["complete"]:
        # Only a complete item search says what left it; the result search never does.
        changes = changes.join(staged.select(*KEY).distinct(), list(KEY), "left_semi")
    return staged, changes


def changed_editions(staged: Any, current: Any, per: Sequence[str], content: Sequence[str]) -> Any:
    """The staged rows of each group (``per``) whose whole list is new or differs from the group's
    current edition (root ADR-0540 §3): an unchanged list appends nothing, a changed one is appended
    whole, so the group's newest edition is always its last read's list. Null-ness is hashed apart
    from the value, as ``changed_only`` does."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    row_hash = F.xxhash64(*(F.col(c).isNull() for c in content), *(F.col(c) for c in content))

    def lists(frame: Any) -> Any:
        return frame.groupBy(*per).agg(F.xxhash64(F.sort_array(F.collect_list(row_hash))).alias("_edition"))

    mine = lists(staged)
    unchanged = mine.join(lists(current.join(mine.select(*per), list(per), "left_semi")), [*per, "_edition"], "inner")
    return staged.join(unchanged.select(*per), list(per), "left_anti")


def load_detail_object(spark: Any, args: argparse.Namespace, entry: Mapping[str, Any],
                       search: Mapping[str, Any]) -> dict[str, Any]:
    """Commits one 물건상세 object, table by table in `DETAIL_TABLES` order: its new photos, its
    changed documents (root ADR-0540), then its reads (root ADR-0384)."""

    from data_go_kr_onbid import changed_only  # noqa: PLC0415 - the same changed-key compare
    from lakehouse_ingest import append_source_object  # noqa: PLC0415
    from platform_contracts import (  # noqa: PLC0415
        column_names, current_rows, ensure_contract_table, load_lakehouse_contract, spark_sql_type,
    )

    counts: dict[str, Any] = {"object": entry["object_key"], "role": DETAIL_ROLE, "rows": len(search["rows"]),
                              "photos": len(search["photo_rows"])}
    given = {PHOTO_TABLE: search["photo_rows"], DETAIL_TABLE: search["rows"], **search["documents"]}
    for name in DETAIL_TABLES:
        rows = given[name]
        contract = load_lakehouse_contract(name)
        table = f"`{args.iceberg_catalog_name}`.`silver`.`{name.split('.', 1)[1]}{args.table_suffix}`"
        ensure_contract_table(spark, table, contract)
        columns = list(column_names(contract))
        ddl = ", ".join(f"{c['name']} {spark_sql_type(c['logical_type'])}" for c in contract["columns"])
        staged = spark.createDataFrame([tuple(row.get(c) for c in columns) for row in rows], ddl)
        key = PHOTO_KEY if name == PHOTO_TABLE else LATEST_KEYS.get(name)
        if key is not None:
            content = [c for c in columns if c not in (*key, *LOAD_COLUMNS)]
            staged = changed_only(staged, current_rows(spark.table(table), contract), key, content)
        elif name in EDITION_PER:
            per = EDITION_PER[name]
            content = [c for c in columns if c not in (*per, *LOAD_COLUMNS)]
            staged = changed_editions(staged, current_rows(spark.table(table), contract), per, content)
        staged = staged.select(*columns).cache()
        short = name.split(".", 1)[1]
        counts[f"{short}_appended_rows"] = staged.count()
        counts[f"{short}_appended"] = append_source_object(spark, staged, columns, table, entry["object_key"],
                                                           contract_table=name)
        staged.unpersist()
    return counts


def relinked_rows(rows: Sequence[Mapping[str, Any]], record: str, snapshot: str, ingested_at: datetime,
                  prefixes: Mapping[str, str], dongs: Mapping[tuple[str, str], str]) -> list[dict[str, Any]]:
    """The current rows again, each with what its kept address offers the joins now (root ADR-0416).

    The row keeps every value it was observed with; only the link inputs are made again. It carries
    the follow's record and instant, so the compare stamps it newer than the row it re-places."""

    out = []
    for current in rows:
        row = dict(current)
        given = offer(row, prefixes, dongs)
        road = given["road"]
        row.update(
            legal_dong_code=given["legal_dong_code"], jibun_pnu=given["jibun_pnu"], reason=given["reason"],
            road_sido=road.sido_code if road else None, road_sigungu=road.sigungu if road else None,
            road_name_key=road.road if road else None, road_main_no=road.main_no if road else None,
            road_sub_no=road.sub_no if road else None,
            source_record_id=record, source_snapshot_id=snapshot, ingested_at_utc=ingested_at,
        )
        out.append(row)
    return out


def follow_code_changes(spark: Any, catalog: str, table: str, contract: Mapping[str, Any], prefixes: Mapping[str, str],
                        dongs: Mapping[tuple[str, str], str], parcels: Any, titles: Any, now: datetime,
                        crosswalk: tuple[Mapping[str, str], Any] = ({}, ())) -> dict[str, Any] | None:
    """Re-link the current rows the 법정동 code changes since the table's last follow can move, and
    append only the ones whose link moved (root ADR-0329 §3, ADR-0416). None when there is nothing
    to follow (no change table, or the links are as of its newest snapshot)."""

    from lakehouse_ingest import append_source_object, read_ingested_objects, unquoted_table_name  # noqa: PLC0415
    from platform_contracts import column_names, current_rows  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415
    from silver_release_compare import release_changes  # noqa: PLC0415

    after = follow.change_snapshot(spark, catalog)
    if after is None:
        return None
    before = follow.last_followed(read_ingested_objects(spark, table, unquoted_table_name(table)))
    if before == after:
        return None
    columns = list(column_names(contract))
    moved = follow.changed_prefixes(spark, catalog, before, after, JOB_NAME)
    current = current_rows(spark.table(table), contract)
    if "row_state" in columns:
        current = current.where(F.col("row_state").isNull())
    affected = follow.rows_under(current, moved, ("legal_dong_code", "pnu"), "pnu").cache()
    record = follow.follow_record(after, now)
    rows = relinked_rows([r.asDict() for r in affected.collect()], record, f"{now:%Y%m%dT%H%M%SZ}", now,
                         prefixes, dongs)
    appended = 0
    if rows:
        names, ddl = offered_schema(contract)
        offered = spark.createDataFrame(
            [tuple(n if name == "row_id" else row.get(name) for name in names) for n, row in enumerate(rows)], schema=ddl)
        resolved = link(offered, names, parcels, titles, crosswalk)
        staged = (resolved.drop("road_name").withColumnRenamed("road_name_raw", "road_name")
                  .withColumn("row_state", F.lit(None).cast("string")).select(*columns))
        # Same keys on both sides: only the rows whose link moved are appended, and no removal marker.
        changes = release_changes(staged, affected, key=KEY, content=content_columns(contract), removal="row_state",
                                  source_snapshot_id=f"{now:%Y%m%dT%H%M%SZ}", ingested_at=F.lit(now).cast("timestamp"),
                                  columns=columns)
        changes = changes.join(staged.select(*KEY).distinct(), list(KEY), "left_semi").cache()
        appended = changes.count()
    else:
        changes = spark.createDataFrame([], schema=spark.table(table).schema).select(*columns)
    # The record is written even with no moved row: the next follow starts from this snapshot.
    append_source_object(spark, changes, columns, table, record, contract_table=contract["table_name"])
    checked = affected.count()
    affected.unpersist()
    return {"table": unquoted_table_name(table), "from_snapshot": before, "to_snapshot": after,
            "prefixes": None if moved is None else len(moved), "checked": checked, "appended": appended}


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
        dongs = address_pnu.lakehouse_legal_dong_index(spark, args.iceberg_catalog_name)
        parcels, titles = address_pnu.lakehouse_inputs(spark, args.iceberg_catalog_name)
        prefixes = address_pnu.lakehouse_sido_prefixes(spark, args.iceberg_catalog_name)
        # ADR-0288 §1: the code pairs are read once for the run, as every parcel source does.
        crosswalk = place_identity_resolver.pairs_from_lakehouse(spark, args.iceberg_catalog_name)
        for entry in objects:
            search = read_object(entry, source, prefixes, dongs, now)
            role = entry["role"]
            if role == DETAIL_ROLE:
                summary["objects"].append(load_detail_object(spark, args, entry, search))
                continue
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
                                                 entry["source_snapshot_id"], now, crosswalk)
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
        summary["follows"] = []
        for contract_table in ROLE_TABLE.values():
            contract = load_lakehouse_contract(contract_table)
            table = f"`{args.iceberg_catalog_name}`.`silver`.`{contract_table.split('.', 1)[1]}{args.table_suffix}`"
            if spark.catalog.tableExists(table.replace("`", "")):
                followed = follow_code_changes(spark, args.iceberg_catalog_name, table, contract, prefixes, dongs,
                                               parcels, titles, now, crosswalk)
                if followed:
                    summary["follows"].append(followed)
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
        search = read_object(entry, source, {}, {}, now)
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
