"""What the 공매 (Onbid) by-PNU document carries beyond its rounds (root ADR-0340 §7).

A round names its item (`cltr_mng_no`) and its announcement round (`onbid_pbanc_no`, `pbct_no`). Facts
of the item (its detail, bid information, earlier rounds' results, maps) and of the announcement (its
text, attachments, organisation, opening) are carried once per document, beside the rounds:

    public_auction.items[]          one per item a served round names
    public_auction.announcements[]  one per announcement round a served round names

so an item with six open rounds on one parcel is not written six times (the JSON:API `included`
pattern). Every value comes from a Silver table; nothing is invented, and a code keeps the name the
provider gave it. Lists the provider gives (`sqmsList`, `leasInfList`, ...) are served as received,
in the provider's own keys, less what Silver never holds (private persons' names, root ADR-0340 §5)
and less the provider's expiring URLs (a stored file is named by its `sha256`, root ADR-0298).

`document_shape()` is the one description of these blocks: the Spark types are built from it, and
the schema registry's revision (contracts/schema-registry) is written from it; the Spark test holds a
written document to the registry's record.
"""
from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

ITEM = "cltr_mng_no"
ANNOUNCEMENT_KEY = ("onbid_pbanc_no", "pbct_no")
DETAIL_SOURCE = "silver.onbid_real_estate_item_detail"
BID_INFO_SOURCE = "silver.onbid_item_bid_info"
RESULT_SOURCE = "silver.onbid_item_bid_result"
ANNOUNCEMENT_SOURCE = "silver.onbid_announcement"
ANNOUNCEMENT_RESULT_SOURCE = "silver.onbid_announcement_bid_result"
ANNOUNCEMENT_DETAIL_SOURCE = "silver.onbid_announcement_detail"
PHOTO_SOURCE = "silver.onbid_real_estate_item_photo"
SOURCES = (DETAIL_SOURCE, BID_INFO_SOURCE, RESULT_SOURCE, ANNOUNCEMENT_SOURCE, ANNOUNCEMENT_RESULT_SOURCE,
           ANNOUNCEMENT_DETAIL_SOURCE)
LOAD_COLUMNS = ("source_record_id", "source_line_number", "source_snapshot_id", "ingested_at_utc", "row_state")
# The round's `photos` keep the kinds a reader of revision 2 knows; every other kind is the item's `media`.
ROUND_MEDIA_KINDS = ("photo", "appraisal")
# A closed earlier round of the item, as its result answer gives it.
HISTORY_COLUMNS = ("pbct_cdtn_no", "pbct_nsq", "pbct_stat_cd", "pbct_stat_nm", "opened_at", "lowest_bid_price_krw",
                   "winning_bid_amount_krw", "valid_bidder_count", "invalid_bidder_count",
                   "apsl_prc_ctrs_scfb_prc_rto", "lowst_bid_ctrs_scfb_prc_rto", "rtrcn_rsn_cont", "afsb_rtrcn_rsn_cont")
MEDIA_COLUMNS = ("sha256", "media_kind", "sequence", "media_format", "size_bytes")
# The provider's own attachment facts a stored file adds to its list entry (root ADR-0298).
STORED_FILE = {"sha256": "string", "media_format": "string", "size_bytes": "string"}
# An announcement's full text past this many characters is not served (a document holds one parcel's
# rounds within 256 KiB, root ADR-0193); `anncm_al_cont_omitted` then says so, and the attachments
# (the announcement as a file) are still served.
ANNOUNCEMENT_TEXT_LIMIT = 20_000
FIELDS_PATH = Path(__file__).resolve().parents[2] / "contracts" / "data-go-kr-onbid.fields.json"
SOURCE_PATH = Path(__file__).resolve().parents[2] / "contracts" / "data-go-kr-onbid.contract.json"


@lru_cache(maxsize=1)
def _inventory() -> dict[str, Any]:
    return json.loads(FIELDS_PATH.read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def _media_lists() -> frozenset[str]:
    return frozenset(json.loads(SOURCE_PATH.read_text(encoding="utf-8"))["media"]["fields"]["list"])


def _columns(source: str) -> list[tuple[str, str]]:
    from platform_contracts import load_lakehouse_contract  # noqa: PLC0415

    return [(c["name"], c["logical_type"]) for c in load_lakehouse_contract(source)["columns"]]


def snake(key: str) -> str:
    """A provider key in the document's case: `apslEvlAmt` → `apsl_evl_amt`."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", key).lower()


def provider_key(key: str) -> str:
    """The provider's key of a served key (the inverse of `snake`); a stored file's facts are ours."""
    if key in STORED_FILE:
        return key
    head, *rest = key.split("_")
    return head + "".join(part.capitalize() for part in rest)


def list_fields(api: str, top: str) -> dict[str, str]:
    """The keys a served list element carries: the provider's keys of `top` the inventory lists
    under `api` in the document's case, less the private names and the URL, plus a stored file's facts
    for a media list. All strings: the provider writes some numbers as text (15157251 V2.1 made
    `scfbAmt` VARCHAR)."""
    keys = {}
    for field in _inventory()["apis"][api]["fields"]:
        path = field["path"]
        if path.startswith(f"{top}[].") and not field.get("pii") and path.rsplit(".", 1)[1] != "urlAdr":
            keys[snake(path.rsplit(".", 1)[1])] = "string"
    if top in _media_lists():
        keys.update(STORED_FILE)
    return keys


def _top_of_json_column(api: str, column: str) -> str:
    """The provider list a `*_json` column holds (its inventory claim)."""
    for field in _inventory()["apis"][api]["fields"]:
        if any(claim.endswith(f".{column}") for claim in field.get("silver", [])):
            return field["path"].split("[]", 1)[0].split(".", 1)[0]
    raise ValueError(f"{column}: no inventory field of {api} claims it")


def _object_shape(source: str, api: str, drop: tuple[str, ...]) -> dict[str, Any]:
    shape: dict[str, Any] = {}
    for name, logical in _columns(source):
        if name in drop or name in LOAD_COLUMNS:
            continue
        if name.endswith("_json"):
            shape[name.removesuffix("_json")] = [list_fields(api, _top_of_json_column(api, name))]
        else:
            shape[name] = logical
    return shape


def document_shape() -> dict[str, Any]:
    """The item and announcement blocks: a dict is an object, a one-element list an array of its
    element, a string a lakehouse logical type."""
    detail = _object_shape(DETAIL_SOURCE, "item_detail", (ITEM, "pbct_cdtn_no"))
    bid_info = _object_shape(BID_INFO_SOURCE, "item_bid_info", (ITEM, "pbct_cdtn_no"))
    types = dict(_columns(RESULT_SOURCE))
    history = {name: types[name] for name in HISTORY_COLUMNS}
    photo = dict(_columns(PHOTO_SOURCE))
    media = {name: photo[name] for name in MEDIA_COLUMNS}
    announcement: dict[str, Any] = {name: logical for name, logical in _columns(ANNOUNCEMENT_SOURCE)
                                    if name not in LOAD_COLUMNS}
    result = dict(_columns(ANNOUNCEMENT_RESULT_SOURCE))
    announcement.update({name: result[name] for name in ("exct_stat_cd", "exct_stat_nm")})
    for name, logical in _object_shape(ANNOUNCEMENT_DETAIL_SOURCE, "announcement_detail", ANNOUNCEMENT_KEY).items():
        announcement.setdefault(name, logical)
    announcement["anncm_al_cont_omitted"] = "boolean"
    return {
        "items": [{ITEM: "string", "detail": detail, "bid_info": bid_info, "history": [history], "media": [media]}],
        "announcements": [announcement],
    }


def spark_type(shape: Any) -> str:
    """The Spark DDL type of a shape."""
    from platform_contracts import spark_sql_type  # noqa: PLC0415

    if isinstance(shape, list):
        return f"array<{spark_type(shape[0])}>"
    if isinstance(shape, dict):
        return "struct<" + ",".join(f"`{name}`:{spark_type(inner)}" for name, inner in shape.items()) + ">"
    return spark_sql_type(shape).lower()


JSON_TYPES = {"string": "string", "date": "string", "timestamp": "string", "boolean": "boolean",
              "int": "integer", "long": "integer", "double": "number"}


def json_schema(shape: Any) -> dict[str, Any]:
    """The registry's JSON Schema of a shape: every field optional (absent or null)."""
    if isinstance(shape, list):
        return {"type": "array", "items": json_schema(shape[0])}
    if isinstance(shape, dict):
        return {"type": ["object", "null"], "properties": {name: json_schema(inner) for name, inner in shape.items()}}
    return {"type": [JSON_TYPES[shape], "null"]}


def _current(frames: dict[str, Any], source: str) -> Any:
    """The current rows of `source`, or an empty frame of its columns when the build has none."""
    from platform_contracts import current_rows, load_lakehouse_contract, spark_sql_type  # noqa: PLC0415

    contract = load_lakehouse_contract(source)
    if source in frames:
        return current_rows(frames[source], contract)
    spark = next(iter(frames.values())).sparkSession
    schema = ", ".join(f"`{c['name']}` {spark_sql_type(c['logical_type'])}" for c in contract["columns"])
    return spark.createDataFrame([], schema)


def parsed_list(text: Any, inner: list[dict[str, str]]) -> Any:
    """A `*_json` column's list in the document's keys: parsed by the provider's keys, renamed."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    keys = list(inner[0])
    received = spark_type([{provider_key(k): t for k, t in inner[0].items()}])
    return F.transform(F.from_json(text, received),
                       lambda element: F.struct(*(element[provider_key(k)].alias(k) for k in keys)))


def _object(frame: Any, shape: dict[str, Any]) -> Any:
    """`shape` as one struct column of `frame`'s columns, a `*_json` column parsed to its list."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    parts = []
    for name, inner in shape.items():
        if isinstance(inner, list) and f"{name}_json" in frame.columns:
            text = F.col(f"{name}_json")
            # 공매재산명세(papsInf) was an object before the provider's V2 made it an array of 0..1.
            text = F.when(F.trim(text).startswith("{"), F.concat(F.lit("["), text, F.lit("]"))).otherwise(text)
            parts.append(parsed_list(text, inner).alias(name))
        else:
            parts.append(F.col(name).alias(name))
    return F.struct(*parts)


def item_blocks(frames: dict[str, Any], items: Any) -> Any:
    """One row per item of `items` (a frame with `cltr_mng_no`): `cltr_mng_no` and its block `_item`."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    shape = document_shape()["items"][0]
    keys = items.select(ITEM).distinct()
    detail = _current(frames, DETAIL_SOURCE)
    detail = detail.select(ITEM, _object(detail, shape["detail"]).alias("detail"))
    bid_info = _current(frames, BID_INFO_SOURCE)
    bid_info = bid_info.select(ITEM, _object(bid_info, shape["bid_info"]).alias("bid_info"))
    results = _current(frames, RESULT_SOURCE)
    history = results.groupBy(ITEM).agg(F.array_sort(F.collect_list(F.struct(
        F.col("opened_at").alias("_at"), F.col("pbct_cdtn_no").alias("_k"),
        F.struct(*HISTORY_COLUMNS).alias("_round")))).alias("_sorted")).select(
        ITEM, F.col("_sorted._round").alias("history"))
    photos = _current(frames, PHOTO_SOURCE).where(~F.col("media_kind").isin(*ROUND_MEDIA_KINDS))
    media = photos.groupBy(ITEM).agg(F.array_sort(F.collect_list(F.struct(
        F.col("media_kind").alias("_kind"), F.col("sequence").alias("_seq"),
        F.struct(*MEDIA_COLUMNS).alias("_media")))).alias("_sorted")).select(
        ITEM, F.col("_sorted._media").alias("media"))
    empty_history = F.expr(f"CAST(array() AS {spark_type([shape['history'][0]])})")
    empty_media = F.expr(f"CAST(array() AS {spark_type([shape['media'][0]])})")
    joined = (keys.join(detail, ITEM, "left").join(bid_info, ITEM, "left").join(history, ITEM, "left")
              .join(media, ITEM, "left"))
    return joined.select(ITEM, F.struct(
        F.col(ITEM), F.col("detail"), F.col("bid_info"),
        F.coalesce(F.col("history"), empty_history).alias("history"),
        F.coalesce(F.col("media"), empty_media).alias("media")).alias("_item"))


def announcement_blocks(frames: dict[str, Any], rounds: Any) -> Any:
    """One row per announcement round `rounds` names: its key and its block `_announcement`. The list,
    its opening state and its detail are joined on the announcement round; where two say the same
    field, the list's value is kept and the detail's fills a gap."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    shape = document_shape()["announcements"][0]
    keys = rounds.select(*ANNOUNCEMENT_KEY).where(
        F.col(ANNOUNCEMENT_KEY[0]).isNotNull() & F.col(ANNOUNCEMENT_KEY[1]).isNotNull()).distinct()
    parts = {"l": _current(frames, ANNOUNCEMENT_SOURCE), "r": _current(frames, ANNOUNCEMENT_RESULT_SOURCE),
             "d": _current(frames, ANNOUNCEMENT_DETAIL_SOURCE)}
    joined = keys
    for tag, frame in parts.items():
        frame = frame.where(F.col(ANNOUNCEMENT_KEY[0]).isNotNull())
        if tag == "d":
            frame = frame.withColumn("anncm_al_cont_omitted",
                                     F.coalesce(F.length("anncm_al_cont") > ANNOUNCEMENT_TEXT_LIMIT, F.lit(False)))
            frame = frame.withColumn("anncm_al_cont", F.when(~F.col("anncm_al_cont_omitted"), F.col("anncm_al_cont")))
        renamed = frame.select(*ANNOUNCEMENT_KEY, *(F.col(c).alias(f"{tag}__{c}") for c in frame.columns
                                                   if c not in ANNOUNCEMENT_KEY))
        joined = joined.join(renamed, list(ANNOUNCEMENT_KEY), "left")

    fields = [_announcement_value(joined.columns, name, inner).alias(name) for name, inner in shape.items()]
    return joined.select(*ANNOUNCEMENT_KEY, F.struct(*fields).alias("_announcement"))


def _announcement_value(columns: list[str], name: str, inner: Any) -> Any:
    """One announcement field: the key as is, a list parsed from the detail's text, else the first
    source holding it (the list, its opening state, the detail)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    if name in ANNOUNCEMENT_KEY:
        return F.col(name)
    if isinstance(inner, list):
        return parsed_list(F.col(f"d__{name}_json"), inner)
    found = [F.col(f"{tag}__{name}") for tag in ("l", "r", "d") if f"{tag}__{name}" in columns]
    return found[0] if len(found) == 1 else F.coalesce(*found)
