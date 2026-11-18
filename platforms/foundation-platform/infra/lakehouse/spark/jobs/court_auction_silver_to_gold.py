#!/usr/bin/env python3
"""Build the 법원경매 (court auction) by-PNU kind's Gold from its own Silver (root ADR-0280, ADR-0287,
ADR-0353).

A kind with its own producer (`by_pnu_kinds.Kind.producer`): served on the parcel lane like a
panel section, but built from `silver.court_auction_item`, not from the parcel panel, so it is
neither pinned to nor checked against it. It places items exactly as the 공매 kind places rounds
(`onbid_public_auction_silver_to_gold.placed`, shared, not copied): the court item's key, its "open"
rule and its served values are its own. A round carries its case's photos (`photos`, from
`silver.court_auction_item_photo`, root ADR-0384): the 물건상세 lists them per case, and the bytes
live once in the shared content-addressed media store the parcel edge serves (root ADR-0296 §5).

공공저작물: 법원경매 데이터는 출처 표시가 필요한 공공저작물이다(저작권법 제24조의2, 루트 ADR-0281
§6). 문서마다 `court_auction.source_attribution` 로 "대한민국법원 법원경매정보" 를 담아, 이 행을
보여 주는 화면이 출처를 구체적으로 표시하게 한다.

Which PNU an item belongs to (the 공매 placement, root ADR-0287):

- an item (`court_office_code`, `case_no`, `property_seq`, `item_seq`) takes the PNU of its current
  row (the contract's `current_rows`: newest per key by source_snapshot_id, root ADR-0267). An item
  whose current row is a removal marker has no current row and is not served (root ADR-0287 §7);
- a retired PNU follows the parcel lineage to every current successor, or the merge it went into;
  one no path leaves keeps the item under itself as `lineage_missing`, counted and never written to
  the servable Gold (root ADR-0292).

What a PNU's document holds: the items in progress as of the build, soonest sale date first, each
with its codes, money values, court identifiers, sale schedule and the address the court lists; and
the parcel's 매각결과 (`sale_results`, root ADR-0454): the current row of every item of
`silver.court_auction_sale_result` placed on it the same way, newest sale date first, at most
SALE_RESULT_BOUND, with `sale_result_total` counting them all. A PNU with sale results and no item in
progress has a document too. Values only: codes stay codes, and the words are the consumer's.

Which code the PNU carries (root ADR-0382, ADR-0329 §3): the Silver row's PNU, or its source code PNU
when the load left it unresolved, is carried to the CURRENT legal-dong code on every build through the
one resolver's crosswalk (`place_identity_resolver.carried_to_current`), never fixed at load time.

Partial (root ADR-0312 §2, ADR-0314): only the PNUs an item or sale-result change reaches are rebuilt
and merged (`affected_pnus`), plus those whose items' sale date passed since the current Gold was built and
those under a code a change of the code change table reaches (`views.changed_codes`). A
change to the served parcels or the lineage moves placements the item rows do not name, so those
two are whole-table inputs and rebuild the kind whole.

A detail's codes carry the site's name beside them (`<code column less _code>_name`, root ADR-0540 §5):
the 공통코드 the collector asks each run (`silver.court_auction_code`), for the columns the source
contract's `common_codes.groups[].names` lists; a code's name that changed rebuilds the PNUs of the
properties whose document carries that code.

Each row carries its parcel's stable `place_id` (`place_id_join`, root ADR-0483): a run also recomputes
the PNUs whose place id a release moved.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from dataclasses import dataclass
from functools import reduce
from typing import Any

import legal_dong_code_change_views as views
import onbid_public_auction_silver_to_gold as onbid
import place_id_join
import place_identity_resolver

JOB_NAME = "court_auction_silver_to_gold"
ITEM_SOURCE = "silver.court_auction_item"
SALE_RESULT_SOURCE = "silver.court_auction_sale_result"
PARCEL_SOURCE = onbid.PARCEL_SOURCE
LINEAGE_SOURCE = onbid.LINEAGE_SOURCE
CODE_CHANGE_SOURCE = "reference.legal_dong_code_change"
PHOTO_SOURCE = "silver.court_auction_item_photo"
# A case's photos: the 물건상세 lists them per (court office, case), for every property of it.
CASE = ("court_office_code", "case_no")
PROPERTY = (*CASE, "property_seq")
# The 물건상세's documents (root ADR-0540 §4-§5), one Silver table each: a round's `detail`. The case
# and its 감정요항표 are a case's; the rest a property's.
CASE_DOC = "silver.court_auction_case"
SPEC_DOC = "silver.court_auction_item_spec"
SCHEDULE_DOC = "silver.court_auction_item_schedule"
OBJECT_DOC = "silver.court_auction_item_object"
ANNEX_DOC = "silver.court_auction_item_annex"
APPRAISAL_DOC = "silver.court_auction_appraisal_point"
DOCUMENT_SOURCES = (CASE_DOC, SPEC_DOC, SCHEDULE_DOC, OBJECT_DOC, ANNEX_DOC, APPRAISAL_DOC)
CASE_DOCUMENTS = (CASE_DOC, APPRAISAL_DOC)
# The site's 공통코드 (root ADR-0540 §5): the name of each code a document carries.
CODE_SOURCE = "silver.court_auction_code"
CODE_GROUP = ("system_code", "owner_code", "group_code")
SOURCE_CONTRACT = Path(__file__).resolve().parents[2] / "contracts" / "courtauction-go-kr.contract.json"


def named_columns(contract_path: Path = SOURCE_CONTRACT) -> dict[str, tuple[tuple[str, tuple[str, str, str]], ...]]:
    """Document table -> ((code column, its group), ...): what each 공통코드 group names (the source
    contract's `common_codes.groups[].names`), the one record of which column a group names."""
    groups = json.loads(contract_path.read_text(encoding="utf-8"))["common_codes"]["groups"]
    out: dict[str, list[tuple[str, tuple[str, str, str]]]] = {}
    for group in groups:
        key = (group["system"], group.get("owner") or group["system"], group["group"])
        for named in group["names"]:
            table, column = named.rsplit(".", 1)
            out.setdefault(table, []).append((column, key))
    return {table: tuple(columns) for table, columns in out.items()}


NAMED = named_columns()


def name_of(column: str) -> str:
    """The column a code's name is served in: `result_code` -> `result_name`."""
    return column.removesuffix("_code") + "_name"
PHOTO_STRUCT = ("array<struct<sha256:string,media_kind:string,kind_code:string,sequence:int,title:string,"
                "media_format:string,size_bytes:bigint>>")
# The code change rows a build reads, the parcel panel's (`changed_codes` compares them).
CODE_CHANGE_COLUMNS = ("kind", "old_code", "new_code", "level", "effective_date", "source")
# The court item's four-part key (boCd, saNo, maemulSer, mokmulSer), the court_auction_item contract's.
KEY = ("court_office_code", "case_no", "property_seq", "item_seq")
IN_PROGRESS = "Y"
REMOVED = onbid.REMOVED
LINEAGE_MISSING = onbid.LINEAGE_MISSING
# 공공저작물 출처 표시(저작권법 제24조의2, 루트 ADR-0281 §6): a consumer of these rows must name it.
SOURCE_ATTRIBUTION = "대한민국법원 법원경매정보"
# The source a served item comes from, carried so the screen can name it.
SOURCE_DATASETS = ("courtauction.go.kr",)
# One document's ceiling (root ADR-0193): items beyond it are dropped from the document, latest sale
# date first; they stay in Silver.
DOCUMENT_BYTE_LIMIT = onbid.DOCUMENT_BYTE_LIMIT
DOCUMENT_ENVELOPE_BYTES = onbid.DOCUMENT_ENVELOPE_BYTES
WRITTEN = onbid.WRITTEN
# A served item's values, in order: identifiers, the court, the address, status and usage codes,
# money, the sale schedule.
ROUND_FIELDS = (
    "court_office_code", "case_no", "property_seq", "item_seq", "user_case_no", "court_name", "department_name",
    "building_name", "print_address_raw",
    "case_status_code", "item_status_code", "item_in_progress_yn",
    "usage_lcls_code", "usage_mcls_code", "usage_scls_code",
    "appraisal_amount_krw", "minimum_bid_price_krw", "sale_amount_krw",
    "sale_date", "sale_time_raw", "sale_decision_date", "sale_place", "failed_bid_count",
    "photos", "detail",
)
# A served sale result's values (root ADR-0454): the item's identifiers, the court, the address, the
# status and usage codes, money and the sale it reports. Measured 2026-11-09 on production Silver:
# 10,120 results (one pass, sale dates 2026-10-13..19), 9,817 with a PNU on 6,774 PNUs, at most 362 on
# one PNU (an apartment complex), 2,062 with a 매각가.
SALE_RESULT_FIELDS = (
    "court_office_code", "case_no", "property_seq", "item_seq", "user_case_no", "court_name", "department_name",
    "building_name", "print_address_raw", "item_status_code",
    "usage_lcls_code", "usage_mcls_code", "usage_scls_code",
    "appraisal_amount_krw", "minimum_bid_price_krw", "sale_amount_krw",
    "sale_date", "sale_decision_date", "failed_bid_count",
)
SALE_RESULT_BOUND = 50
# The share of the document ceiling the sale results may take; the items in progress get the rest.
# SALE_RESULT_BOUND results of about 600 bytes each fit well inside it.
SALE_RESULT_BYTES = 64 * 1024
GOLD_CONTENT = ("court_auction_json", "attached_via")
# The item identifiers of a served document, enough to find which PNUs an item is served under.
_KEYS = "array<struct<court_office_code:string,case_no:string,property_seq:int,item_seq:int>>"
DOCUMENT_ROUND_KEYS = f"struct<rounds:{_KEYS},sale_results:{_KEYS}>"


def lane_inputs(lane: str) -> tuple[tuple[str, ...], dict[str, str], str, tuple[str, ...]]:
    """(required inputs, optional input -> flag without it, anchor input, whole-table inputs), the
    seam `gold_rebuild.producer_inputs` reads. The item table and the code change table are not
    whole: `affected_pnus` maps their changed keys and codes to PNUs. The parcels and the lineage
    are: a new edition or link moves items the item rows do not name."""
    del lane
    return ((PARCEL_SOURCE, ITEM_SOURCE, SALE_RESULT_SOURCE, CODE_CHANGE_SOURCE, *place_id_join.INPUTS),
            {LINEAGE_SOURCE: "--no-carry-lineage", PHOTO_SOURCE: "--no-photos",
             **{source: "--no-documents" for source in (*DOCUMENT_SOURCES, CODE_SOURCE)}}, PARCEL_SOURCE,
            (PARCEL_SOURCE, LINEAGE_SOURCE))


def declared_sources(lane: str, carry_lineage: bool, photos: bool = True, documents: bool = True) -> tuple[str, ...]:
    """Every Silver source this build pins: exactly what the rebuild harness pins from `lane_inputs`
    (`load_source_snapshot_pins` rejects any mismatch, the 2026-10-27 공매 crash)."""
    required, _optional, _anchor, _whole = lane_inputs(lane)
    return (*required, *((LINEAGE_SOURCE,) if carry_lineage else ()), *((PHOTO_SOURCE,) if photos else ()),
            *((*DOCUMENT_SOURCES, CODE_SOURCE) if documents else ()))


def photos_by_case(photo_rows: Any) -> Any:
    """Each case's photos as one array, keyed by case: its current (case, hash) rows, ordered by the
    site's photo kind then its list order."""
    from pyspark.sql import functions as F  # noqa: PLC0415
    from platform_contracts import current_rows, load_lakehouse_contract  # noqa: PLC0415

    current = current_rows(photo_rows, load_lakehouse_contract(PHOTO_SOURCE))
    return (current.groupBy(*CASE).agg(F.array_sort(F.collect_list(F.struct(
        F.col("photo_kind_code").alias("_kind"), F.col("sequence").alias("_seq"), F.col("sha256").alias("_sha"),
        F.struct("sha256", F.lit("photo").alias("media_kind"), F.col("photo_kind_code").alias("kind_code"),
                 "sequence", "title", "media_format", F.col("size_bytes").cast("bigint").alias("size_bytes"),
                 ).alias("_photo"),
    ))).alias("_sorted")).select(*CASE, F.col("_sorted._photo").alias("photos")))


def with_photos(round_rows: Any, photo_rows: Any | None) -> Any:
    """Each round with its case's `photos`, an empty array when it has none or the table is absent,
    so every served round carries the field and the web reads one shape."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    empty = F.expr(f"CAST(array() AS {PHOTO_STRUCT})")
    if photo_rows is None:
        return round_rows.withColumn("photos", empty)
    return (round_rows.join(photos_by_case(photo_rows), list(CASE), "left")
            .withColumn("photos", F.coalesce(F.col("photos"), empty)))


def _listed(frame: Any, group: tuple[str, ...], order: tuple[str, ...], fields: list[Any], name: str) -> Any:
    """`frame`'s rows as one array per `group`, in `order`."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    keyed = F.struct(*[F.col(c).alias(f"_o{n}") for n, c in enumerate(order)], F.struct(*fields).alias("_e"))
    return frame.groupBy(*group).agg(F.array_sort(F.collect_list(keyed)).getField("_e").alias(name))


def with_names(frame: Any, source: str, codes: Any | None) -> Any:
    """`frame` with each code column `NAMED` lists for `source` joined to its group's name beside it
    (`name_of`), null without a name or without the code table (root ADR-0540 §5)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    for column, (system, owner, group) in NAMED.get(source, ()):
        if codes is None:
            frame = frame.withColumn(name_of(column), F.lit(None).cast("string"))
            continue
        names = (codes.where((F.col("system_code") == system) & (F.col("owner_code") == owner)
                             & (F.col("group_code") == group))
                 .select(F.col("code").alias("_named_code"), F.col("name").alias(name_of(column))))
        frame = (frame.join(F.broadcast(names), F.col(column) == F.col("_named_code"), "left")
                 .drop("_named_code"))
    return frame


def current_codes(frames: dict[str, Any]) -> Any | None:
    """The code table's current rows, or None when the build has none."""
    from platform_contracts import current_rows, load_lakehouse_contract  # noqa: PLC0415

    if CODE_SOURCE not in frames:
        return None
    return current_rows(frames[CODE_SOURCE], load_lakehouse_contract(CODE_SOURCE))


def detail_parts(frames: dict[str, Any]) -> dict[str, Any]:
    """Each document table's current rows (root ADR-0540 §4) as one column per property or case:
    `_case` and `_spec` structs, `_schedule`, `_objects`, `_annexes` and `_appraisal_points` arrays in
    the site's order. Each code keeps its code, with the site's name beside it (`with_names`)."""
    from pyspark.sql import functions as F  # noqa: PLC0415
    from platform_contracts import current_rows, load_lakehouse_contract  # noqa: PLC0415

    codes = current_codes(frames)

    def current(source: str) -> Any:
        return with_names(current_rows(frames[source], load_lakehouse_contract(source)), source, codes)

    case = current(CASE_DOC).select(*CASE, F.struct(
        "case_name", "case_received_date", "commencement_date", "claim_amount_krw", "department_name",
        "department_phone", "dividend_demand_deadline").alias("_case"))
    spec = current(SPEC_DOC).select(*PROPERTY, F.struct(
        "spec_written_date", "assumed_rights", "superficies", "spec_remarks", "first_rank_setting", "item_remarks",
        "item_usage_code", "item_usage_name", "bid_kind_code", "bid_kind_name",
        "bid_start_date", "bid_end_date", "sale_decision_date", "sale_decision_time_raw", "sale_decision_place",
        "deposit_rate_percent").alias("_spec"))
    schedule = _listed(current(SCHEDULE_DOC), PROPERTY, ("schedule_seq",), [
        "schedule_date", "schedule_time_raw", "schedule_kind_code", "schedule_kind_name", "schedule_place",
        "result_code", "result_name", "minimum_bid_price_krw", "sale_amount_krw"], "_schedule")
    objects = _listed(current(OBJECT_DOC), PROPERTY, ("object_seq",), [
        "object_seq", "list_kind_code", "list_kind_name", "realty_kind", "building_structure", "area_raw",
        "land_category", "appraisal_amount_krw", "print_address_raw", "building_unit_raw", "share_numerator", "share_denominator"], "_objects")
    annexes = _listed(current(ANNEX_DOC), PROPERTY, ("object_seq", "annex_seq"), [
        "object_seq", "usage_raw", "structure_raw", "area_raw", "appraisal_amount_krw", "remarks"], "_annexes")
    points = _listed(current(APPRAISAL_DOC), CASE, ("point_seq",), [
        "table_code", "table_name", "item_code", "item_name", "content"], "_appraisal_points")
    return {"case": case, "spec": spec, "schedule": schedule, "objects": objects, "annexes": annexes,
            "appraisal_points": points}


def with_detail(round_rows: Any, frames: dict[str, Any], spark: Any = None) -> Any:
    """Each round with its `detail` (root ADR-0540 §5): its case, its 매각물건명세서 summary and its
    lists, null for a property whose 물건상세 was never read (or a build without the documents:
    `--no-documents`, every detail null), so every served round carries the field."""
    from pyspark.sql import functions as F  # noqa: PLC0415
    from platform_contracts import load_lakehouse_contract, spark_sql_type  # noqa: PLC0415

    if not all(source in frames for source in DOCUMENT_SOURCES):
        session = spark or round_rows.sparkSession
        frames = {source: session.createDataFrame([], ", ".join(
            f"{c['name']} {spark_sql_type(c['logical_type'])}" for c in load_lakehouse_contract(source)["columns"]))
            for source in DOCUMENT_SOURCES}
    parts = detail_parts(frames)
    joined = round_rows
    for name, part in parts.items():
        on = [c for c in part.columns if not c.startswith("_")]
        joined = joined.join(part, on, "left")
    lists = ("schedule", "objects", "annexes", "appraisal_points")
    typed = {name: parts[name].schema[f"_{name}"].dataType.simpleString() for name in lists}
    present = F.col("_case").isNotNull() | F.col("_spec").isNotNull()
    detail = F.struct(F.col("_case").alias("case"), F.col("_spec").alias("spec"), *[
        F.coalesce(F.col(f"_{name}"), F.expr(f"CAST(array() AS {typed[name]})")).alias(name) for name in lists])
    return (joined.withColumn("detail", F.when(present, detail))
            .drop(*[f"_{name}" for name in parts]))


def located(items: Any, remap: dict[str, str]) -> Any:
    """`items` with `pnu` the PNU each row is placed by: its resolved PNU, else its source code PNU,
    carried to the current legal-dong code (root ADR-0382 §2). Every version is carried, so the
    old and new reads of a build compare like for like."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    return place_identity_resolver.carried_to_current(
        items.withColumn("pnu", F.coalesce(F.col("pnu"), F.col("source_code_pnu_raw"))), remap)


def code_change_keys(old: dict[str, Any], new: dict[str, Any]) -> Any | None:
    """The items placed under either code of a code change row that differs between the two reads
    (root ADR-0273, ADR-0329 §3), by their PNU before or after the build carries it, or None when
    no row differs. The table is a few thousand rows, read whole as the parcel panel does."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    before, after = ([row.asDict() for row in frames[CODE_CHANGE_SOURCE].select(*CODE_CHANGE_COLUMNS).collect()]
                     if CODE_CHANGE_SOURCE in frames else [] for frames in (old, new))
    by_digits: dict[int, set[str]] = {}
    for code in views.changed_codes(before, after, CODE_CHANGE_COLUMNS):
        prefix = views.code_prefix(code)
        by_digits.setdefault(len(prefix), set()).add(prefix)
    if not by_digits:
        return None
    # One isin per prefix length, as the parcel panel does: a first build sees every code changed.
    under = F.lit(False)
    for digits, prefixes in sorted(by_digits.items()):
        under = under | F.substring("pnu", 1, digits).isin(sorted(prefixes))
    reached = [frame[name].where(under).select(*KEY) for frame in (old, new)
               for name in (ITEM_SOURCE, SALE_RESULT_SOURCE) if name in frame]
    return reduce(lambda a, b: a.unionByName(b), reached)


def rounds(items: Any) -> Any:
    """One row per item: its current row, carrying that row's PNU as `source_pnu` (root ADR-0267)."""
    from pyspark.sql import functions as F  # noqa: PLC0415
    from platform_contracts import current_rows, load_lakehouse_contract  # noqa: PLC0415

    return (current_rows(items, load_lakehouse_contract(ITEM_SOURCE)).withColumnRenamed("pnu", "source_pnu")
            .withColumn("is_open", F.col("row_state").isNull() | (F.col("row_state") != F.lit(REMOVED))))


def sale_results(results: Any) -> Any:
    """One row per item's sale result: its current row (the contract's `current_rows`, newest per key by
    source_snapshot_id, root ADR-0267), carrying that row's PNU as `source_pnu`. The table has no
    removal marker: a reported result stays."""
    from platform_contracts import current_rows, load_lakehouse_contract  # noqa: PLC0415

    return current_rows(results, load_lakehouse_contract(SALE_RESULT_SOURCE)).withColumnRenamed("pnu", "source_pnu")


def served_rounds(round_rows: Any, as_of: Any) -> Any:
    """The items a document serves: open, marked in progress by the site, with a sale date today or
    ahead of `as_of` (a date). An item without a sale date is not served: the screen could never
    tell when it is auctioned."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    return round_rows.where(
        F.col("is_open") & (F.col("item_in_progress_yn") == F.lit(IN_PROGRESS))
        & F.col("sale_date").isNotNull() & (F.col("sale_date") >= as_of))


def _entries(placed: Any, fields: tuple[str, ...], first: Any, limit_bytes: int, bound: int | None) -> Any:
    """Per PNU: `_entries` (the rows' `fields`, `first` then the key, cut where their JSON passes
    `limit_bytes` or at `bound`), `_count` (every row), `_own` (1 when one is on its own PNU) and `_via`
    (the least carried path)."""
    from pyspark.sql import Window, functions as F  # noqa: PLC0415

    # An earlier snapshot of the item list (or a test frame) may lack a later column: it is null.
    entries = placed.withColumn("_entry", F.struct(*[
        (F.col(f) if f in placed.columns
         else F.expr(f"CAST(array() AS {PHOTO_STRUCT})") if f == "photos" else F.lit(None).cast("string")).alias(f)
        for f in (*fields, "attached_via")]))
    order = Window.partitionBy("pnu").orderBy(first, *[F.col(k).asc() for k in KEY])
    sized = (entries.withColumn("_bytes", F.length(F.to_json("_entry", WRITTEN)) + F.lit(1))
             .withColumn("_running", F.sum("_bytes").over(order.rowsBetween(Window.unboundedPreceding, 0)))
             .withColumn("_n", F.row_number().over(order)))
    kept = F.col("_running") <= F.lit(limit_bytes)
    if bound is not None:
        kept = kept & (F.col("_n") <= F.lit(bound))
    return sized.groupBy("pnu").agg(
        F.array_sort(F.collect_list(F.when(kept, F.struct(F.col("_n").alias("_sort"), F.col("_entry").alias("e")))))
        .getField("e").alias("_entries"),
        F.count(F.lit(1)).alias("_count"),
        F.max(F.col("attached_via").isNull().cast("int")).alias("_own"),
        F.min("attached_via").alias("_via"),
    )


def document_rows(placed_rounds: Any, placed_sales: Any, limit_bytes: int = DOCUMENT_BYTE_LIMIT) -> Any:
    """One row per PNU holding a served item or a sale result: `court_auction_json` (the served items,
    soonest sale date first, and the sale results, newest first, each cut at its share of the document
    ceiling, with the source and its attribution), `round_count`, and `attached_via` — null when the
    PNU holds an item or result of its own, else its nearest carried one's path."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    parts = ("_entries", "_count", "_own", "_via")
    items = _entries(placed_rounds, ROUND_FIELDS, F.col("sale_date").asc(),
                     limit_bytes - DOCUMENT_ENVELOPE_BYTES - SALE_RESULT_BYTES, None)
    sales = _entries(placed_sales, SALE_RESULT_FIELDS, F.col("sale_date").desc_nulls_last(), SALE_RESULT_BYTES,
                     SALE_RESULT_BOUND)
    types = {name: part.schema["_entries"].dataType.simpleString() for name, part in (("r", items), ("s", sales))}
    joined = (items.select("pnu", *[F.col(c).alias(f"r{c}") for c in parts])
              .join(sales.select("pnu", *[F.col(c).alias(f"s{c}") for c in parts]), "pnu", "full_outer"))

    def listed(side: str) -> Any:
        return F.coalesce(F.col(f"{side}_entries"), F.expr(f"CAST(array() AS {types[side]})"))

    own = F.greatest(F.coalesce(F.col("r_own"), F.lit(0)), F.coalesce(F.col("s_own"), F.lit(0)))
    # Nulls are written, so a document's field paths are always the registry's (root ADR-0336).
    return joined.select(
        "pnu",
        F.to_json(F.struct(
            listed("r").alias("rounds"),
            listed("s").alias("sale_results"),
            F.coalesce(F.col("s_count"), F.lit(0)).alias("sale_result_total"),
            F.array(*[F.lit(d) for d in SOURCE_DATASETS]).alias("source_datasets"),
            F.lit(SOURCE_ATTRIBUTION).alias("source_attribution"),
        ), WRITTEN).alias("court_auction_json"),
        F.coalesce(F.col("r_count"), F.lit(0)).alias("round_count"),
        F.when(own == 1, F.lit(None).cast("string")).otherwise(F.least("r_via", "s_via")).alias("attached_via"),
    )


@dataclass
class Placement:
    """What places an item besides its own rows: the lineage (whole-table inputs) and the build time."""

    as_of: str
    candidates: Any | None = None
    merges: Any | None = None


def placement(frames: dict[str, Any], how: Placement) -> Any:
    """The served items of `frames` placed on their PNUs (one row per item and PNU)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    served = served_rounds(rounds(frames[ITEM_SOURCE]), F.to_date(F.to_timestamp(F.lit(how.as_of))))
    return onbid.placed(with_detail(with_photos(served, frames.get(PHOTO_SOURCE)), frames), frames[PARCEL_SOURCE],
                        how.candidates, how.merges)


def sale_placement(frames: dict[str, Any], how: Placement) -> Any:
    """The sale results of `frames` placed on their PNUs as items are (one row per result and PNU)."""
    return onbid.placed(sale_results(frames[SALE_RESULT_SOURCE]), frames[PARCEL_SOURCE], how.candidates, how.merges)


def gold_rows(located: Any, located_sales: Any, as_of: str, source_snapshot_id: str, columns: tuple[str, ...]) -> Any:
    """The kind's Gold rows from placed items and sale results: one per PNU that holds a servable one."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    return document_rows(onbid.servable(located), onbid.servable(located_sales)).select(
        "pnu", *GOLD_CONTENT, F.lit(source_snapshot_id).alias("source_snapshot_id"),
        F.lit(as_of).alias("published_at_utc"),
    ).withColumn("row_digest", F.sha2(F.to_json(F.struct("pnu", *GOLD_CONTENT)), 256)).select(*columns)


def items_of(frames: dict[str, Any], keys: Any) -> dict[str, Any]:
    """`frames` with the item and sale-result rows cut to the items of `keys` (every version of each)
    and the photo and document rows to their cases."""
    cases = keys.select(*CASE).distinct()
    cut = {ITEM_SOURCE: keys.select(*KEY), SALE_RESULT_SOURCE: keys.select(*KEY), PHOTO_SOURCE: cases,
           **{source: cases for source in DOCUMENT_SOURCES}}
    return {name: frame.join(cut[name], cut[name].columns, "left_semi") if name in cut else frame
            for name, frame in frames.items()}


def restrict_inputs(frames: dict[str, Any], keys: Any, how: Placement) -> dict[str, Any]:
    """The input rows the Gold rows of the PNUs `keys` are built from: every item and sale result placed
    on them."""
    on = [placed.join(keys, "pnu", "left_semi").select(*KEY) for placed in (placement(frames, how),
                                                                            sale_placement(frames, how))]
    restricted = items_of(frames, on[0].unionByName(on[1]).distinct())
    if place_id_join.BRIDGE_SOURCE in restricted:
        restricted[place_id_join.BRIDGE_SOURCE] = restricted[place_id_join.BRIDGE_SOURCE].join(keys, "pnu", "left_semi")
    return restricted


def document_round_keys(gold: Any) -> Any:
    """(pnu, item key) for every item and sale result a Gold document serves."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    body = F.from_json("court_auction_json", DOCUMENT_ROUND_KEYS)
    listed = F.concat(F.coalesce(body["rounds"], F.expr(f"CAST(array() AS {_KEYS})")),
                      F.coalesce(body["sale_results"], F.expr(f"CAST(array() AS {_KEYS})")))
    return (gold.select("pnu", F.explode(listed).alias("_r"))
            .select("pnu", *[F.col(f"_r.{k}").alias(k) for k in KEY]))


def affected_pnus(old: dict[str, Any], new: dict[str, Any], how: Placement, current_gold: Any,
                  previous_as_of: str | None, changed: list[str], place_change: Any = None) -> Any:
    """The PNUs whose document can differ from the current Gold's (root ADR-0312 §2).

    An item is touched when a row of it changed in the item or the sale-result table, or when its
    sale date passed since the current Gold was built (`previous_as_of`: it left the served items with
    no input change). Its PNUs are those the current Gold serves it under and those it or its result
    is placed on now, so an item or result that moved, closed or appeared reaches both ends.
    ``place_change``: the PNUs whose place id moved (`place_id_join.changed_pnus`), or None."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    import gold_incremental as incremental  # noqa: PLC0415

    touched = []
    if ITEM_SOURCE in changed and ITEM_SOURCE in old and ITEM_SOURCE in new:
        touched.append(incremental.changed_rows(old[ITEM_SOURCE], new[ITEM_SOURCE], KEY).select(*KEY))
    if SALE_RESULT_SOURCE in changed and SALE_RESULT_SOURCE in old and SALE_RESULT_SOURCE in new:
        touched.append(incremental.changed_rows(old[SALE_RESULT_SOURCE], new[SALE_RESULT_SOURCE], KEY).select(*KEY))
    if CODE_CHANGE_SOURCE in changed and (moved := code_change_keys(old, new)) is not None:
        touched.append(moved)
    if PHOTO_SOURCE in changed and PHOTO_SOURCE in old and PHOTO_SOURCE in new:
        # A photo row is its case's: every property of the case serves it.
        cases = incremental.changed_rows(old[PHOTO_SOURCE], new[PHOTO_SOURCE], (*CASE, "sha256")).select(*CASE).distinct()
        touched.append(new[ITEM_SOURCE].join(cases, list(CASE), "left_semi").select(*KEY))
    for source in DOCUMENT_SOURCES:
        if source in changed and source in old and source in new:
            # A document row is its property's, or its case's (every property of it serves it).
            by = CASE if source in CASE_DOCUMENTS else PROPERTY
            owners = incremental.changed_rows(old[source], new[source], by).select(*by).distinct()
            touched.append(new[ITEM_SOURCE].join(owners, list(by), "left_semi").select(*KEY))
    if CODE_SOURCE in changed and CODE_SOURCE in old and CODE_SOURCE in new:
        touched.extend(named_code_owners(old[CODE_SOURCE], new[CODE_SOURCE], new))
    today = F.to_date(F.to_timestamp(F.lit(how.as_of)))
    passed = rounds(new[ITEM_SOURCE]).where(F.col("sale_date").isNotNull() & (F.col("sale_date") < today))
    if previous_as_of is not None:
        passed = passed.where(F.col("sale_date") >= F.to_date(F.to_timestamp(F.lit(previous_as_of))))
    touched.append(passed.select(*KEY))
    keys = reduce(lambda a, b: a.unionByName(b), touched).distinct()
    cut = items_of(new, keys)
    now = placement(cut, how).select("pnu").unionByName(sale_placement(cut, how).select("pnu"))
    before = document_round_keys(current_gold).join(keys, list(KEY), "left_semi").select("pnu")
    moved = [] if place_change is None else [place_change.select("pnu")]
    return reduce(lambda a, b: a.unionByName(b), [now, before, *moved]).distinct()


def named_code_owners(old_codes: Any, new_codes: Any, new: dict[str, Any]) -> list[Any]:
    """The items whose document names a code whose name changed (root ADR-0540 §5): for each document
    table and code column a group names (`NAMED`), the properties or cases of its current rows that
    carry a changed (group, code), as item keys."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    import gold_incremental as incremental  # noqa: PLC0415
    from platform_contracts import current_rows, load_lakehouse_contract  # noqa: PLC0415

    changed = incremental.changed_rows(old_codes, new_codes, (*CODE_GROUP, "code")).select(*CODE_GROUP, "code")
    owners = []
    for source, columns in NAMED.items():
        if source not in new:
            continue
        by = CASE if source in CASE_DOCUMENTS else PROPERTY
        rows = current_rows(new[source], load_lakehouse_contract(source))
        for column, (system, owner, group) in columns:
            codes = changed.where((F.col("system_code") == system) & (F.col("owner_code") == owner)
                                  & (F.col("group_code") == group)).select(F.col("code").alias(column))
            hit = rows.join(F.broadcast(codes), column, "left_semi").select(*by).distinct()
            owners.append(new[ITEM_SOURCE].join(hit, list(by), "left_semi").select(*KEY))
    return owners


def parse_args(argv=None) -> argparse.Namespace:
    """The arguments `scripts/ops/gold-panel-rebuild.sh` gives every producer (root ADR-0193 §7)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lane", required=True)
    parser.add_argument("--input-mode", choices=("iceberg",), default="iceberg")
    parser.add_argument("--write-mode", choices=("iceberg",), default="iceberg")
    parser.add_argument("--iceberg-catalog-name", default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_CATALOG_NAME", "r2"))
    parser.add_argument("--source-iceberg-namespace", default="silver")
    parser.add_argument("--target-iceberg-namespace", default="gold")
    parser.add_argument("--iceberg-snapshot-id", required=True, help="The silver.parcel_boundaries snapshot this build reads.")
    parser.add_argument("--source-snapshots-path", required=True, help="JSON pinning every input table.")
    parser.add_argument("--published-at-utc")
    parser.add_argument("--allow-non-smoke-overwrite", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--minimum-count", type=int, help="Refuse, before writing, a Gold with fewer rows (root ADR-0139).")
    parser.add_argument("--no-carry-lineage", dest="carry_lineage", action="store_false",
                        help="Place a retired PNU's item under itself, flagged, instead of following the lineage.")
    parser.add_argument("--no-photos", dest="photos", action="store_false",
                        help="Serve every round with an empty photos array, without the photo table.")
    parser.add_argument("--no-documents", dest="documents", action="store_false",
                        help="Serve every round with a null detail, without the 물건상세 document tables.")
    parser.add_argument("--summary-output")
    parser.add_argument("--lineage-output")
    # The harness passes the section-coverage flags to every kind producer (gold_rebuild.plan, root
    # ADR-0272) and the incremental flags when the plan says `incremental` (root ADR-0312 §2).
    import gold_incremental  # noqa: PLC0415
    import section_coverage  # noqa: PLC0415

    section_coverage.add_arguments(parser)
    gold_incremental.add_arguments(parser)
    args = parser.parse_args(argv)
    args.region_prefix, args.iceberg_write_mode, args.expected_count = None, "overwrite", None
    section_coverage.validate_arguments(args)
    if args.minimum_count is not None and args.minimum_count < 0:
        parser.error("--minimum-count must be non-negative")
    if not args.validate_only and not args.allow_non_smoke_overwrite:
        parser.error("overwriting the Gold needs --allow-non-smoke-overwrite")
    return args


def read_inputs(spark: Any, args: argparse.Namespace, pins: dict[str, str], counters: dict[str, int],
                previous: dict[str, str] | None) -> tuple[dict[str, Any], dict[str, Any] | None, Placement]:
    """(the inputs at `pins`, the item table at `previous` or None, how items are placed)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415
    from lakehouse_snapshot_pins import read_pinned_iceberg  # noqa: PLC0415
    from platform_contracts import load_lakehouse_contract, with_contract_columns  # noqa: PLC0415

    def versions(at: dict[str, str], source: str = ITEM_SOURCE) -> Any:
        # Every version, as the contract's columns (an older snapshot reads a later column as null).
        table = f"{args.iceberg_catalog_name}.{args.source_iceberg_namespace}.{source.split('.', 1)[1]}"
        return with_contract_columns(read_pinned_iceberg(spark, table, source, at), load_lakehouse_contract(source))

    def results(at: dict[str, str], remap: dict[str, str]) -> Any:
        # A result's PNU is carried to the current code on every build, as an item's (root ADR-0454 §3).
        return place_identity_resolver.carried_to_current(versions(at, SALE_RESULT_SOURCE), remap)

    def code_changes(at: dict[str, str]) -> tuple[Any, dict[str, str]]:
        changes = panel.read_source(spark, args, CODE_CHANGE_SOURCE, at).select(*CODE_CHANGE_COLUMNS)
        remap, _ambiguous = place_identity_resolver.crosswalk_of(row.asDict() for row in changes.collect())
        return changes, remap

    changes, remap = code_changes(pins)
    counters["carried_codes"] = len(remap)
    frames = {ITEM_SOURCE: located(versions(pins), remap).cache(), SALE_RESULT_SOURCE: results(pins, remap).cache(),
              CODE_CHANGE_SOURCE: changes}
    old = None
    if previous is not None and ITEM_SOURCE in previous:
        # The old read is carried by the old pairs, so a code change shows as the rows it moved.
        old_changes, old_remap = code_changes(previous) if CODE_CHANGE_SOURCE in previous else (None, {})
        old = {ITEM_SOURCE: located(versions(previous), old_remap)}
        # A Gold built before the sale results were an input pinned none; that build is whole
        # (gold_incremental.changed_inputs refuses a merge from it).
        if SALE_RESULT_SOURCE in previous:
            old[SALE_RESULT_SOURCE] = results(previous, old_remap)
        if old_changes is not None:
            old[CODE_CHANGE_SOURCE] = old_changes
    if args.photos and PHOTO_SOURCE in pins:
        frames[PHOTO_SOURCE] = versions(pins, PHOTO_SOURCE).cache()
        if old is not None and previous is not None and PHOTO_SOURCE in previous:
            old[PHOTO_SOURCE] = versions(previous, PHOTO_SOURCE)
    for source in (*DOCUMENT_SOURCES, CODE_SOURCE) if args.documents else ():
        if source in pins:
            frames[source] = versions(pins, source).cache()
            if old is not None and previous is not None and source in previous:
                old[source] = versions(previous, source)
    placed_pnus = frames[ITEM_SOURCE].select("pnu").unionByName(frames[SALE_RESULT_SOURCE].select("pnu"))
    named = placed_pnus.select(F.col("pnu").alias("predecessor_pnu")).where(F.col("predecessor_pnu").isNotNull()).distinct()
    sources = {r.predecessor_pnu for r in named.collect()}
    # Only the candidates from a PNU an item names, not the lineage's 6.5M.
    candidates = panel.read_carry_candidates(spark, args, counters, pins, sources) if args.carry_lineage else None
    merges = None
    if args.carry_lineage:
        merges = (panel.read_source(spark, args, LINEAGE_SOURCE, pins).where(F.col("relation") == F.lit("merge"))
                  .select("predecessor_pnu", "successor_pnu", "grade")
                  .join(F.broadcast(named), "predecessor_pnu", "left_semi").cache())
    # The served cadastral edition only (root ADR-0148), the one the host lane serves, cut to the
    # parcels an item or its lineage can reach (onbid.parcels_named).
    served = panel.read_served_parcels(spark, args, pins, region=False).select("pnu")
    # An item whose current code the served edition predates sits on the parcel an earlier form of
    # its code names (root ADR-0482 §1): 6,020 경매 items on 2026-11-11, as a lineage candidate does.
    forms = place_identity_resolver.served_form_candidates(spark, sources, remap, served, candidates)
    candidates = forms if candidates is None else candidates.unionByName(forms)
    frames[PARCEL_SOURCE] = onbid.parcels_named(served, placed_pnus, candidates, merges).cache()
    frames[place_id_join.BRIDGE_SOURCE] = place_id_join.read(spark, args, pins)
    if old is not None and place_id_join.BRIDGE_SOURCE in previous:
        old[place_id_join.BRIDGE_SOURCE] = place_id_join.read(spark, args, previous)
    return frames, old, Placement(args.published_at_utc, candidates, merges)


def count_placement(located: Any, located_sales: Any, counters: dict[str, int]) -> None:
    """The run summary's counts of a full build's placed items and sale results."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    counters["served_rounds"] = located.select(*KEY).distinct().count()
    counters["lineage_missing_rounds"] = located.where(F.col("attached_via") == F.lit(LINEAGE_MISSING)).count()
    counters["served_sale_results"] = located_sales.select(*KEY).distinct().count()
    counters["lineage_missing_sale_results"] = located_sales.where(F.col("attached_via") == F.lit(LINEAGE_MISSING)).count()


def main(argv=None) -> int:
    from pyspark.sql import SparkSession  # noqa: PLC0415

    import by_pnu_kinds  # noqa: PLC0415
    import gold_incremental as incremental  # noqa: PLC0415
    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415
    from gold_rebuild import assert_minimum_row_count  # noqa: PLC0415
    from lakehouse_engine import apply_catalog_settings, assert_catalog_env  # noqa: PLC0415
    from lakehouse_snapshot_pins import load_source_snapshot_pins  # noqa: PLC0415
    from platform_contracts import column_names, ensure_contract_table, load_lakehouse_contract  # noqa: PLC0415

    args = parse_args(argv)
    kind = by_pnu_kinds.kind(args.lane)
    if kind.producer != JOB_NAME:
        raise ValueError(f"{args.lane} names producer {kind.producer}, not {JOB_NAME}")
    args.published_at_utc = panel.normalize_utc_timestamp(args.published_at_utc)
    _required, _optional, anchor, whole = lane_inputs(args.lane)
    sources = declared_sources(args.lane, args.carry_lineage, args.photos, args.documents)
    pins = load_source_snapshot_pins("iceberg", args.source_snapshots_path, sources, anchor, args.iceberg_snapshot_id)
    previous = incremental.validate_arguments(args, sources)
    contract = load_lakehouse_contract(kind.gold_table)
    columns = tuple(column_names(contract))
    if columns != place_id_join.gold_columns(kind):
        raise ValueError(f"{kind.gold_table} columns {columns} are not the kind line's {kind.columns}")
    content = place_id_join.gold_content(kind)
    built = place_id_join.built(columns)
    table = f"`{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`.`{kind.gold_table.split('.', 1)[1]}`"
    assert_catalog_env(args.iceberg_catalog_name)
    spark = apply_catalog_settings(SparkSession.builder.appName(f"{JOB_NAME}:{args.lane}")
                                   .config("spark.sql.session.timeZone", "UTC"), args.iceberg_catalog_name).getOrCreate()
    counters: dict[str, int] = {}
    try:
        frames, old, how = read_inputs(spark, args, pins, counters, previous)

        def build(inputs: dict[str, Any]) -> Any:
            located, located_sales = placement(inputs, how).cache(), sale_placement(inputs, how).cache()
            if inputs is frames:
                count_placement(located, located_sales, counters)
            rows = gold_rows(located, located_sales, args.published_at_utc, pins[PARCEL_SOURCE], built)
            return place_id_join.with_place_ids(rows, inputs[place_id_join.BRIDGE_SOURCE], content, columns)

        place_change = place_id_join.lane_change(spark, args, previous or {}, pins, lambda: old or {}, frames)
        lane = incremental.Lane(
            build=build, restrict=lambda inputs, keys: restrict_inputs(inputs, keys, how),
            affected=lambda changed, appended: affected_pnus(
                old or {}, frames, how, spark.table(table), onbid.previous_build_time(spark, table), changed,
                place_change(changed, appended)),
            validate=onbid.validate_gold, content_columns=content[1:], gold_columns=columns,
            whole_table_inputs=whole,
            append_inputs={place_id_join.CHANGELOG_SOURCE: place_id_join.changelog_table(args)})
        outcome = incremental.build_or_merge(spark, args, lane, frames, pins, previous, table, onbid.validate_gold,
                                             assert_minimum_row_count)
        # Held to the whole-table row-loss floor (gold-panel-rebuild.contract.json kind_tables
        # overrides), not ADR-0272's per-시도 gate: the rows are the parcels with an item in progress,
        # which swing day to day as sale dates pass, the 공매 kind's reason (root ADR-0292).
        try:
            if not args.validate_only:
                spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`")
                ensure_contract_table(spark, table, contract)
                outcome.commit(spark, table, pins, args.iceberg_write_mode, contract_table=contract["table_name"])
                incremental.retain_input_snapshots(spark, args.iceberg_catalog_name, args.source_iceberg_namespace,
                                                   kind.gold_table, pins)
            summary = {
                "schema_version": panel.RUN_SUMMARY_SCHEMA_VERSION, "job_name": JOB_NAME, "contract": kind.gold_table,
                "lane": args.lane, "created_at_utc": panel.normalize_utc_timestamp(None), "row_count": outcome.row_count,
                "write_disposition": "validate_only" if args.validate_only else outcome.disposition(args),
                "quality_metrics": {**outcome.metrics, **counters}, "columns": list(columns),
                "source_iceberg_snapshots_by_dataset": dict(sorted(pins.items())), "rebuild": outcome.report(),
            }
        finally:
            outcome.release()
    finally:
        spark.stop()
    panel.emit_run_summary(summary, args.summary_output)
    if args.lineage_output and not args.validate_only:
        with open(args.lineage_output, "w", encoding="utf-8") as lineage:
            json.dump({"job_name": JOB_NAME, "output_dataset": kind.gold_table, "inputs": dict(sorted(pins.items()))},
                      lineage, sort_keys=True)
    print(f"gold-kind-{'validate' if args.validate_only else 'write'}-ok lane={args.lane} rows={outcome.row_count} "
          f"mode={outcome.mode}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
