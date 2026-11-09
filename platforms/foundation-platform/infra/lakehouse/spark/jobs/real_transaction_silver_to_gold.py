#!/usr/bin/env python3
"""Build the 실거래 by-PNU kind's Gold from its own Silver (root ADR-0350, ADR-0351, ADR-0371).

A kind with its own producer (`by_pnu_kinds.Kind.producer`, root ADR-0287 §9): served on the parcel
lane like a panel section, built from `silver.real_transaction`'s current rows.

Where a deal belongs is decided again on every build, from the provider's own columns, never from the
`pnu`/`legal_dong_code` the Silver load wrote (that is evidence of the load's day, root ADR-0351 §3):

- the 법정동: the deal's full address (`address_raw`) named in the newest `reference.legal_dong_code_
  snapshot` (`rt_molit_real_transaction.dong_index`), carried old → current through
  `reference.legal_dong_code_change` (`place_identity_resolver.code_crosswalk`);
- an unmasked lot: its PNU through the one resolver (`place_identity_resolver.resolve`); a PNU that is
  no longer a current parcel follows the parcel lineage like a 공매 sale (`onbid_public_auction_silver_to_gold.placed`);
- a masked lot (`3**`): the resolver's inference (`infer_masked`) by the levels the 실거래 contract's
  measurements enable (`enabled_levels`, root ADR-0370); the grade is `inferred_*` and the screen says 추정.
  A deal the 본번 group level links (`inferred_same_main_lot_group`, root ADR-0405) is placed on every
  current parcel of its 법정동, 산 flag and 본번, and each says 추정 · 같은 본번 beside the others.

What a PNU's document holds (`real_transaction`, root ADR-0350 §2):

- `transactions`: every deal placed on the PNU (all 12 datasets), newest contract first, cancelled
  ones too; past the 256 KiB document bound the oldest are left out (`transactions_omitted`);
- `main_lot_group_pnus`: when a deal reached the PNU by its 본번 group, the group's current parcels
  (the PNU's own 본번 group: a parcel is in one group, so every such deal names the same list);
- `comparables`: for a parcel whose 지목 is 공장용지 or 창고용지 or that has a 공장창고 deal of its own,
  the newest COMPARABLE_COUNT uncancelled 공장창고 deals of its current 법정동 contracted within
  COMPARABLE_MONTHS of the build month, masked lots included (that is what the list is for);
- `region`: the 법정동's 공장창고 and 토지 deals of the last REGION_MONTHS: counts and the m² price quartiles.

Partial by design (root ADR-0312 §2): the unit of change is the 법정동. Every deal, comparable and
inference candidate of a PNU lies in the PNU's own current 법정동 (or, through the lineage, in its
predecessors'), so a run recomputes the PNUs of the 법정동s a change reaches (`affected_dongs`): the
deals the load appended, both codes of every changed code pair (`views.changed_codes`, root ADR-0329
§3), the codes whose names changed, the 법정동 of every changed parcel, 지목/면적, 건축물대장, 토지소유
or lineage row, and the 법정동s whose window lost a month since the current Gold was built.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import date
from typing import Any

JOB_NAME = "real_transaction_silver_to_gold"
RT_SOURCE = "silver.real_transaction"
PARCEL_SOURCE = "silver.parcel_boundaries"
CODE_CHANGE_SOURCE = "reference.legal_dong_code_change"
LEGAL_DONG_SOURCE = "reference.legal_dong_code_snapshot"
LAND_SOURCE = "silver.land_characteristic"
TITLE_SOURCE = "silver.building_register_titles"
OWNERSHIP_SOURCE = "silver.land_ownership"
LINEAGE_SOURCE = "silver.parcel_lineage"
SECTION = "real_transaction"
INDUSTRIAL = "real_transaction_industrial_trade"
LAND = "real_transaction_land_trade"
COMPARABLE_JIMOK = ("공장용지", "창고용지")
COMPARABLE_MONTHS = 36
COMPARABLE_COUNT = 20
REGION_MONTHS = 12
REGION_DATASETS = (INDUSTRIAL, LAND)
DOCUMENT_BYTE_LIMIT = 256 * 1024
DOCUMENT_ENVELOPE_BYTES = 1024
WRITTEN = {"ignoreNullFields": "false"}
MASKED = r"^(산\s*)?(\d+)(\*+)$"
LOT = r"^(산\s*)?(\d{1,4})(?:-(\d{1,4}))?$"
# A deal as the document carries it: the provider's columns as the Silver names them, and how it was placed.
TRANSACTION_FIELDS = (
    "dataset_slug", "deal_date", "deal_amount_10k_krw", "deposit_10k_krw", "monthly_rent_10k_krw", "rent_kind",
    "building_type", "building_use", "building_name", "building_area_m2", "building_area_kind", "land_area_m2",
    "land_area_kind", "floor", "build_year", "land_use", "jimok", "jibun_raw", "share_dealing_type",
    "dealing_gbn", "cancelled", "cancel_date", "place_identity_grade", "attached_via",
)
COMPARABLE_FIELDS = ("deal_date", "deal_amount_10k_krw", "land_area_m2", "building_area_m2", "land_use",
                     "building_use", "build_year", "jibun_raw", "pnu", "place_identity_grade",
                     "main_lot_group_key", "main_lot_group_size")
# The columns read from each input, and the only ones the build is handed. The deal's Silver `pnu`,
# `legal_dong_code` and `place_identity_grade` are not among them (root ADR-0351 §3).
DEAL_COLUMNS = ("dataset_slug", "deal_ym", "row_key", "deal_date", "deal_amount_10k_krw", "deposit_10k_krw",
                "monthly_rent_10k_krw", "rent_kind", "address_raw", "jibun_raw", "bonbun", "bubun",
                "building_type", "building_use", "building_name", "building_area_m2", "building_area_kind",
                "land_area_m2", "land_area_kind", "floor", "build_year", "land_use", "jimok", "share_dealing_type",
                "dealing_gbn", "cancelled", "cancel_date")
BUILD_COLUMNS = {
    RT_SOURCE: (*DEAL_COLUMNS, "source_legal_dong", "legal_dong"),
    PARCEL_SOURCE: ("pnu",),
    CODE_CHANGE_SOURCE: ("kind", "old_code", "new_code", "level", "effective_date", "source"),
    LEGAL_DONG_SOURCE: ("region_cd", "full_name", "status"),
    LAND_SOURCE: ("pnu", "land_category", "area_m2", "zone_name_1"),
    TITLE_SOURCE: ("pnu", "approval_year", "floor_area_m2"),
    OWNERSHIP_SOURCE: ("pnu", "ownership_changed_on", "ownership_change_reason"),
    LINEAGE_SOURCE: ("predecessor_pnu", "successor_pnu", "relation", "grade", "evidence_kind", "evidence_ref"),
}
PNU_INPUTS = (PARCEL_SOURCE, LAND_SOURCE, TITLE_SOURCE, OWNERSHIP_SOURCE)
INFERENCE_ONLY = (TITLE_SOURCE, OWNERSHIP_SOURCE)
# The region statistics Gold (root ADR-0350 §4): written by this producer beside the kind, from the
# same deals, so the document's `region` and the table never disagree.
REGION_TABLE = "gold.real_transaction_region_stats"
REGION_KEY = ("dataset_slug", "region_level", "region_code", "period_kind", "period")
MONTH, TRAILING = "month", "trailing_12_months"


def lane_inputs(lane: str) -> tuple[tuple[str, ...], dict[str, str], str, tuple[str, ...]]:
    """(required inputs, optional input -> flag without it, anchor input, whole-table inputs), the
    seam `gold_rebuild.producer_inputs` reads. No input rebuilds the kind whole: each maps to 법정동s."""
    del lane
    return ((PARCEL_SOURCE, RT_SOURCE, CODE_CHANGE_SOURCE, LEGAL_DONG_SOURCE, LAND_SOURCE, TITLE_SOURCE,
             OWNERSHIP_SOURCE), {LINEAGE_SOURCE: "--no-carry-lineage"}, PARCEL_SOURCE, ())


def declared_sources(carry_lineage: bool) -> tuple[str, ...]:
    required, _optional, _anchor, _whole = lane_inputs("")
    return (*required, *((LINEAGE_SOURCE,) if carry_lineage else ()))


def month_start(day: date) -> date:
    return date(day.year, day.month, 1)


def add_months(day: date, months: int) -> date:
    index = day.year * 12 + day.month - 1 + months
    return date(index // 12, index % 12 + 1, 1)


def aged_out_months(previous: date | None, current: date) -> list[tuple[date, date]]:
    """[from, to) contract months that left a window between the build months `previous` and
    `current`: a 법정동 with a deal in one has a different document now. None: no earlier build."""
    if previous is None or month_start(previous) >= month_start(current):
        return []
    return [(add_months(month_start(previous), -span), add_months(month_start(current), -span))
            for span in (COMPARABLE_MONTHS, REGION_MONTHS)]


def address_codes(snapshot_rows: list[tuple[str, str, str]], change_rows: list[dict[str, Any]]
                  ) -> tuple[list[tuple[str, str, str | None]], dict[str, str], set[str]]:
    """([(address, its code in the snapshot, its current code or None)], remap, ambiguous).

    An address names a code of the newest snapshot (`dong_index`); the code is carried to the current
    one through the pairs in force (`code_crosswalk`); a code that split has no current code."""
    import legal_dong_code_change_views as views  # noqa: PLC0415
    import place_identity_resolver as resolver  # noqa: PLC0415
    import rt_molit_real_transaction as rt  # noqa: PLC0415

    remap, ambiguous = resolver.code_crosswalk((p["old_code"], p["new_code"]) for p in views.pair_rows(change_rows))
    names = rt.dong_index(snapshot_rows)
    return ([(name, code, None if code in ambiguous else remap.get(code, code)) for name, code in sorted(names.items())],
            remap, ambiguous)


def with_codes(deals: Any, codes: Any) -> Any:
    """`deals` with `source_legal_dong` (the code its address names) and `legal_dong` (current)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    key = F.trim(F.regexp_replace(F.coalesce("address_raw", F.lit("")), r"\s+", " "))
    return deals.withColumn("_address", key).join(F.broadcast(codes), "_address", "left").drop("_address")


def code_frame(spark: Any, rows: list[tuple[str, str, str | None]]) -> Any:
    return spark.createDataFrame(rows, "_address string, source_legal_dong string, legal_dong string")


def lot_pnu(frame: Any) -> Any:
    """The source-code PNU of an unmasked lot (`rt_molit_real_transaction.lot_of`, `candidate_pnu`) as `pnu`."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    raw = F.trim(F.coalesce("jibun_raw", F.lit("")))
    bon = F.trim(F.coalesce("bonbun", F.lit("")))
    bu = F.trim(F.coalesce("bubun", F.lit("")))
    from_columns = bon.rlike(r"^\d+$") & (bon.cast("int") > 0)
    main = F.when(from_columns, bon.cast("int")).otherwise(F.regexp_extract(raw, LOT, 2).cast("int"))
    sub = F.when(from_columns, F.when(bu.rlike(r"^\d+$"), bu.cast("int")).otherwise(F.lit(0))).otherwise(
        F.coalesce(F.when(F.regexp_extract(raw, LOT, 3) != "", F.regexp_extract(raw, LOT, 3)).cast("int"), F.lit(0)))
    ok = (~raw.contains("*") & (from_columns | raw.rlike(LOT)) & main.between(1, 9999) & (sub <= 9999)
          & F.col("source_legal_dong").isNotNull())
    return frame.withColumn("pnu", F.when(ok, F.concat(
        "source_legal_dong", F.when(raw.startswith("산"), F.lit("2")).otherwise(F.lit("1")),
        F.lpad(main.cast("string"), 4, "0"), F.lpad(sub.cast("string"), 4, "0"))))


def masked_deals(deals: Any) -> Any:
    """The deals whose lot the provider masked, as `infer_masked` reads them (지분 거래 left out:
    their area is the share's, not the parcel's, root ADR-0351 §1)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    raw = F.trim(F.coalesce("jibun_raw", F.lit("")))
    stars = F.length(F.regexp_extract(raw, MASKED, 3))
    low = F.regexp_extract(raw, MASKED, 2).cast("int") * F.pow(F.lit(10), stars).cast("int")
    zone = F.trim(F.coalesce("land_use", F.lit("")))
    return (deals.where(raw.rlike(MASKED) & F.col("legal_dong").isNotNull()
                        & ~F.coalesce(F.col("share_dealing_type"), F.lit("")).eqNullSafe(F.lit("지분")))
            .select("row_id", "dataset_slug", F.col("legal_dong").alias("legal_dong_code"),
                    raw.startswith("산").alias("lot_mountain"), low.alias("lot_low"),
                    (low + F.pow(F.lit(10), stars).cast("int") - 1).alias("lot_high"),
                    F.when(zone.isin("", "-"), None).when(zone.endswith("지역") | zone.endswith("구역"), zone)
                    .otherwise(F.concat(zone, F.lit("지역"))).alias("zone"),
                    F.when(F.trim("jimok") != "", F.trim("jimok")).alias("jimok"), "land_area_m2",
                    F.col("building_area_m2").alias("floor_area_m2"), F.col("build_year").alias("building_year"),
                    "deal_date"))


def lineage_candidates(lineage: Any, sources: list[str]) -> Any:
    """The lineage's carry candidates from `sources` (the parcel panel's walk, root ADR-0113 §6)."""
    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415

    wanted = set(sources)
    rows = [row.asDict() for row in lineage.distinct().collect()]
    links = [panel.Link(r["predecessor_pnu"] or "", r["successor_pnu"], r["relation"], r["grade"], "")
             for r in panel.steward_resolved(rows)]
    candidates, _stopped = panel.carry_candidates(links)
    picked = [(c.successor_pnu, c.source_pnu, c.hop, c.path) for c in candidates if c.source_pnu in wanted]
    return F.broadcast(lineage.sparkSession.createDataFrame(
        picked, "successor_pnu string, source_pnu string, hop int, path string"))


def placed_deals(frames: dict[str, Any], inference: dict[str, Any]) -> Any:
    """Every deal with the PNU it is placed on (`pnu`, null for none), `place_identity_grade` and
    `attached_via`; a deal the lineage places on several parcels is a row for each, and so is a deal
    linked to a 본번 group (`main_lot_group_key`, its 15-digit key, and `main_lot_group_size`)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    import onbid_public_auction_silver_to_gold as auction  # noqa: PLC0415
    import place_identity_resolver as resolver  # noqa: PLC0415

    changes = [row.asDict() for row in frames[CODE_CHANGE_SOURCE].collect()]
    _rows, remap, ambiguous = address_codes([], changes)
    parcels = frames[PARCEL_SOURCE].select("pnu").distinct()
    deals = frames[RT_SOURCE].withColumn("row_id", F.concat_ws("|", "dataset_slug", "deal_ym", "row_key"))
    resolved = resolver.resolve(lot_pnu(deals), remap, ambiguous, parcels, keep_candidate=True)
    # Masked lots, by the levels each dataset's measurements enable (none: the lot stays unlinked).
    land = frames[LAND_SOURCE].join(parcels, "pnu", "left_semi")
    masked = masked_deals(deals)
    inferred = []
    for dataset in sorted(r["dataset_slug"] for r in masked.select("dataset_slug").distinct().collect()):
        levels = resolver.enabled_levels(inference, dataset)
        if levels:
            inferred.append(resolver.infer_masked(
                masked.where(F.col("dataset_slug") == dataset), land, frames[TITLE_SOURCE], frames[OWNERSHIP_SOURCE],
                levels, float(inference["area_tolerance_m2"]), tuple(inference["ownership_window_days"]),
                inference["ownership_causes"], parcels).where(F.col("pnu").isNotNull() | F.col("pnu_group").isNotNull()))
    if inferred:
        found = inferred[0]
        for frame in inferred[1:]:
            found = found.unionByName(frame)
        found = found.select("row_id", F.col("pnu").alias("_inferred"), F.col("pnu_group").alias("_group"),
                             F.col("place_identity_grade").alias("_grade"))
        resolved = (resolved.join(found, "row_id", "left")
                    .withColumn("pnu", F.coalesce("pnu", "_inferred"))
                    .withColumn("place_identity_grade", F.coalesce("_grade", "place_identity_grade"))
                    .drop("_inferred", "_grade"))
    else:
        resolved = resolved.withColumn("_group", F.lit(None).cast("array<string>"))
    # A 본번 group deal is a row on each current parcel of the group (all current: no lineage to walk).
    group_key = F.substring(F.element_at("_group", 1), 1, resolver.MAIN_LOT_KEY_LENGTH)
    resolved = (resolved.withColumn("main_lot_group_key", F.when(F.col("_group").isNotNull(), group_key))
                .withColumn("main_lot_group_size", F.when(F.col("_group").isNotNull(), F.size("_group")).cast("int")))
    # One pass: a group deal explodes to its members, any other deal stays one row (its pnu or null).
    resolved = resolved.withColumn("pnu", F.explode(F.coalesce("_group", F.array(F.col("pnu"))))).drop("_group")
    sales = resolved.withColumn("source_pnu", F.coalesce("pnu", "pnu_candidate")).drop("pnu", "pnu_candidate")
    candidates = merges = None
    if LINEAGE_SOURCE in frames:
        retired = sales.where(F.col("source_pnu").isNotNull()).join(
            parcels.withColumnRenamed("pnu", "source_pnu"), "source_pnu", "left_anti")
        sources = [r["source_pnu"] for r in retired.select("source_pnu").distinct().collect()]
        candidates = lineage_candidates(frames[LINEAGE_SOURCE], sources)
        merges = frames[LINEAGE_SOURCE].where(F.col("relation") == F.lit("merge")).select(
            "predecessor_pnu", "successor_pnu", "grade")
    placed = auction.servable(auction.placed(sales, parcels, candidates, merges))
    unplaced = sales.join(placed.select("row_id").distinct(), "row_id", "left_anti").withColumn(
        "pnu", F.lit(None).cast("string")).withColumn("attached_via", F.lit(None).cast("string"))
    columns = [c for c in placed.columns if c != "source_pnu"]
    return placed.select(*columns).unionByName(unplaced.select(*columns))


def gold_rows(frames: dict[str, Any], inference: dict[str, Any], as_of: date, source_snapshot_id: str,
              published_at_utc: str, columns: tuple[str, ...]) -> Any:
    """One row per served PNU with a deal or a comparable: `real_transaction_json`, `transaction_total`."""
    from pyspark.sql import Window, functions as F  # noqa: PLC0415

    import place_identity_resolver as resolver  # noqa: PLC0415

    as_of = month_start(as_of)
    deals = placed_deals(frames, inference).cache()
    entry = F.struct(*[F.col(f).cast("string").alias(f) if f in ("deal_date", "cancel_date") else F.col(f)
                       for f in TRANSACTION_FIELDS])
    own = (deals.where(F.col("pnu").isNotNull())
           .withColumn("_entry", entry).withColumn("_bytes", F.length(F.to_json("_entry", WRITTEN)) + 1))
    # Comparables: one row per deal (the lowest PNU it is placed on, if any), newest first.
    first = Window.partitionBy("row_id").orderBy(F.col("pnu").asc_nulls_last())
    by_deal = deals.withColumn("_n", F.row_number().over(first)).where(F.col("_n") == 1).drop("_n")
    window_from = F.lit(add_months(as_of, -COMPARABLE_MONTHS))
    industrial = by_deal.where((F.col("dataset_slug") == INDUSTRIAL) & F.col("legal_dong").isNotNull()
                               & ~F.coalesce("cancelled", F.lit(False)) & (F.col("deal_date") >= window_from)
                               & (F.col("deal_date") < F.lit(add_months(as_of, 1))))
    newest = Window.partitionBy("legal_dong").orderBy(F.col("deal_date").desc(), F.col("row_id"))
    # A 본번 group deal is one comparable: its group's key and size, no one PNU (root ADR-0405).
    in_group = F.col("main_lot_group_key").isNotNull()
    comparable = F.struct(*[F.col(f).cast("string").alias(f) if f == "deal_date"
                            else F.when(~in_group, F.col(f)).alias(f) if f == "pnu" else F.col(f)
                            for f in COMPARABLE_FIELDS])
    comparables = (industrial.withColumn("_n", F.row_number().over(newest)).where(F.col("_n") <= COMPARABLE_COUNT)
                   .groupBy("legal_dong").agg(F.array_sort(F.collect_list(F.struct(
                       F.col("_n"), comparable.alias("_c")))).alias("_list"))
                   .select("legal_dong", F.transform("_list", lambda x: x["_c"]).alias("comparables")))
    # Region: the 법정동's 공장창고 and 토지 rows of the region statistics' trailing 12 months
    # (gold.real_transaction_region_stats, root ADR-0350 §4), m² prices in 만원.
    trailing = region_rows(frames[RT_SOURCE], as_of).where(
        (F.col("period_kind") == F.lit(TRAILING)) & (F.col("region_level") == F.lit("legal_dong"))
        & F.col("dataset_slug").isin(*REGION_DATASETS))
    quart = lambda prefix: F.when(F.col(f"{prefix}_median").isNotNull(), F.struct(  # noqa: E731
        F.col(f"{prefix}_p25").alias("p25"), F.col(f"{prefix}_median").alias("median"),
        F.col(f"{prefix}_p75").alias("p75")))
    region = (trailing.groupBy(F.col("region_code").alias("legal_dong")).agg(F.array_sort(F.collect_list(F.struct(
        "dataset_slug", "deal_count", "cancelled_count",
        quart("land_price_per_m2").alias("land_price_per_m2_10k_krw"),
        F.when(F.col("dataset_slug") == F.lit(INDUSTRIAL), quart("building_price_per_m2"))
        .alias("building_price_per_m2_10k_krw"))))
        .alias("datasets")))
    # Which PNUs carry comparables: 지목 공장용지·창고용지, or a 공장창고 deal of their own.
    parcels = frames[PARCEL_SOURCE].select("pnu").distinct()
    eligible = (frames[LAND_SOURCE].where(F.col("land_category").isin(*COMPARABLE_JIMOK)).select("pnu")
                .unionByName(own.where(F.col("dataset_slug") == INDUSTRIAL).select("pnu"))
                .distinct().join(parcels, "pnu", "left_semi"))
    newest_own = Window.partitionBy("pnu").orderBy(F.col("deal_date").desc_nulls_last(), F.col("row_id"))
    own = own.withColumn("_cum", F.sum("_bytes").over(newest_own.rowsBetween(Window.unboundedPreceding, 0)))
    listed = own.groupBy("pnu").agg(
        F.count(F.lit(1)).cast("long").alias("transaction_total"),
        F.array_sort(F.collect_list(F.struct(F.col("_cum"), F.col("_entry").alias("_e")))).alias("_all"))
    # The 본번 group a PNU's group deals name: its own group's current parcels, written once.
    keys = own.where(F.col("main_lot_group_key").isNotNull()).select("pnu", "main_lot_group_key").distinct()
    groups = (parcels.withColumn("main_lot_group_key", F.substring("pnu", 1, resolver.MAIN_LOT_KEY_LENGTH))
              .join(keys.select("main_lot_group_key").distinct(), "main_lot_group_key", "left_semi")
              .groupBy("main_lot_group_key").agg(F.array_sort(F.collect_set("pnu")).alias("_group")))
    group_of = keys.join(groups, "main_lot_group_key").select("pnu", "_group")
    pnus = listed.select("pnu").unionByName(eligible).distinct()
    joined = (pnus.withColumn("legal_dong", F.substring("pnu", 1, 10))
              .join(listed, "pnu", "left")
              .join(group_of, "pnu", "left")
              .join(comparables, "legal_dong", "left")
              .join(region, "legal_dong", "left")
              .join(eligible.withColumn("_eligible", F.lit(True)), "pnu", "left")
              .withColumn("comparables", F.when(F.col("_eligible"), F.col("comparables")))
              .where(F.col("_all").isNotNull() | (F.size(F.coalesce("comparables", F.array())) > 0)))
    group_pnus = F.coalesce("_group", F.array().cast("array<string>"))
    budget = (F.lit(DOCUMENT_BYTE_LIMIT - DOCUMENT_ENVELOPE_BYTES)
              - F.coalesce(F.length(F.to_json(F.struct("comparables", "datasets", group_pnus.alias("g")), WRITTEN)),
                           F.lit(0)))
    kept = F.transform(F.filter(F.coalesce("_all", F.array()), lambda x: x["_cum"] <= budget), lambda x: x["_e"])
    document = F.struct(
        F.lit(as_of.isoformat()).alias("as_of_month"),
        kept.alias("transactions"),
        (F.size(F.coalesce("_all", F.array())) - F.size(kept)).alias("transactions_omitted"),
        group_pnus.alias("main_lot_group_pnus"),
        F.coalesce("comparables", F.array()).alias("comparables"),
        F.lit(add_months(as_of, -COMPARABLE_MONTHS).isoformat()).alias("comparables_from"),
        F.struct(F.col("legal_dong").alias("legal_dong_code"),
                 F.lit(add_months(as_of, -REGION_MONTHS).isoformat()).alias("from"),
                 F.coalesce("datasets", F.array()).alias("datasets")).alias("region"),
    )
    content = ("pnu", "real_transaction_json", "transaction_total", "attached_via")
    return (joined.select(
        "pnu", F.to_json(document, WRITTEN).alias("real_transaction_json"),
        F.coalesce("transaction_total", F.lit(0)).cast("long").alias("transaction_total"),
        F.lit(None).cast("string").alias("attached_via"),
        F.lit(source_snapshot_id).alias("source_snapshot_id"), F.lit(published_at_utc).alias("published_at_utc"))
        .withColumn("row_digest", F.sha2(F.to_json(F.struct(*content)), 256)).select(*columns))


def region_rows(deals: Any, as_of: date) -> Any:
    """The region statistics of `deals` (root ADR-0350 §4): per (dataset, 법정동 or 시군구, period) the
    deal count, the cancelled count and the m² price quartiles in 만원 of the uncancelled deals, by
    land area (계약면적·대지면적) and, but for 토지, by building area. A period is a contract month
    (`month`, `YYYY-MM`) or the TRAILING 12 months before the build month (`as_of`'s month). The
    region is the deal's CURRENT 법정동 (`legal_dong`, the address through the code pairs), so a code
    change moves a deal's statistics with it."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    as_of = month_start(as_of)
    base = deals.where(F.col("legal_dong").isNotNull() & F.col("deal_date").isNotNull())
    live = ~F.coalesce("cancelled", F.lit(False))
    amount = F.col("deal_amount_10k_krw")
    base = (base.withColumn("_land", F.when(live & (F.col("land_area_m2") > 0), amount / F.col("land_area_m2")))
            .withColumn("_building", F.when(live & (F.col("building_area_m2") > 0) & (F.col("dataset_slug") != LAND),
                                            amount / F.col("building_area_m2")))
            .withColumn("_live", live))
    monthly = base.withColumn("period_kind", F.lit(MONTH)).withColumn("period", F.date_format("deal_date", "yyyy-MM"))
    recent = (base.where((F.col("deal_date") >= F.lit(add_months(as_of, -REGION_MONTHS)))
                         & (F.col("deal_date") < F.lit(as_of)))
              .withColumn("period_kind", F.lit(TRAILING)).withColumn("period", F.lit(as_of.strftime("%Y-%m"))))
    periods = monthly.unionByName(recent)
    levels = (periods.withColumn("region_level", F.lit("legal_dong")).withColumn("region_code", F.col("legal_dong"))
              .unionByName(periods.withColumn("region_level", F.lit("sigungu"))
                           .withColumn("region_code", F.concat(F.substring("legal_dong", 1, 5), F.lit("00000")))))
    quartiles = lambda c: F.expr(f"percentile({c}, array(0.25, 0.5, 0.75))")  # noqa: E731
    stats = levels.groupBy(*REGION_KEY).agg(
        F.count(F.lit(1)).cast("long").alias("deal_count"),
        F.sum(F.when(~F.col("_live"), 1).otherwise(0)).cast("long").alias("cancelled_count"),
        quartiles("_land").alias("_lq"), quartiles("_building").alias("_bq"))
    quart = [F.when(F.size(c) == 3, F.round(F.col(c)[i], 1)).alias(f"{name}_{label}")
             for c, name in (("_lq", "land_price_per_m2"), ("_bq", "building_price_per_m2"))
             for i, label in enumerate(("p25", "median", "p75"))]
    return stats.select(*REGION_KEY, "deal_count", "cancelled_count", *quart)


def region_gold_rows(deals: Any, as_of: date, source_snapshot_id: str, published_at_utc: str,
                     columns: tuple[str, ...]) -> Any:
    """`region_rows` as the region statistics Gold: with its digest of content only and its pins."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    rows = region_rows(deals, as_of)
    content = [c for c in columns if c not in ("row_digest", "source_snapshot_id", "published_at_utc")]
    return (rows.withColumn("row_digest", F.sha2(F.to_json(F.struct(*content)), 256))
            .withColumn("source_snapshot_id", F.lit(source_snapshot_id))
            .withColumn("published_at_utc", F.lit(published_at_utc)).select(*columns))


def region_scope(dongs: Any | None, prefixes: list[str]) -> list[str]:
    """The 시군구-wide code prefixes whose region statistics a change reaches: a 시군구's rows read
    every 법정동 under it, so the scope is never narrower than a 시군구 (5 digits)."""
    found = {p[:5] for p in prefixes}
    if dongs is not None:
        found |= {row["legal_dong"][:5] for row in dongs.select("legal_dong").distinct().collect()}
    return sorted(found)


def merge_region_stats(spark: Any, table: str, frames: dict[str, Any], scope: list[str] | None, as_of: date,
                       source_snapshot_id: str, published_at_utc: str, columns: tuple[str, ...],
                       properties: dict[str, str]) -> dict[str, int]:
    """Write the region statistics: whole (`scope` None, a full build) or only the 시군구 of `scope`,
    recomputed from their deals and merged by key; rows of those 시군구 the rebuild no longer makes
    are deleted (root ADR-0312 §2: a run writes only what its change reaches)."""
    from functools import reduce  # noqa: PLC0415

    from pyspark.sql import functions as F  # noqa: PLC0415

    import gold_incremental as incremental  # noqa: PLC0415
    from gold_rebuild import write_gold_snapshot  # noqa: PLC0415

    deals = frames[RT_SOURCE]
    if scope is None:
        rows = region_gold_rows(deals, as_of, source_snapshot_id, published_at_utc, columns)
        write_gold_snapshot(rows, table, "overwrite", {}, F.lit(True), properties, contract_table=REGION_TABLE)
        return {"region_scope_sigungu": -1, "region_rows": rows.count()}
    if not scope:
        return {"region_scope_sigungu": 0}
    inside = lambda column: reduce(lambda a, b: a | b, [F.col(column).startswith(p) for p in scope])  # noqa: E731
    rows = region_gold_rows(deals.where(inside("legal_dong")), as_of, source_snapshot_id, published_at_utc, columns)
    current = spark.table(table).where(inside("region_code"))
    source, counts = incremental.merge_source(rows, current, REGION_KEY)
    incremental.merge_into_gold(spark, table, source, {**properties,
                                incremental.KEYS_CHANGED_PROPERTY: str(incremental.keys_changed(counts))}, REGION_KEY,
                                contract_table=REGION_TABLE)
    return {"region_scope_sigungu": len(scope), **{f"region_{k}": v for k, v in counts.items()}}


def under(frame: Any, column: str, dongs: Any) -> Any:
    """The rows of `frame` whose `column` PNU lies in one of `dongs` (a frame of `legal_dong`)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    return frame.join(F.broadcast(dongs), F.substring(column, 1, 10) == F.col("legal_dong"), "left_semi")


def restrict_inputs(frames: dict[str, Any], keys: Any) -> dict[str, Any]:
    """The input rows the Gold rows of the PNUs `keys` are built from: everything under their
    법정동s and under the 법정동s of the parcels the lineage carries onto them; the code tables and
    the lineage whole (a few thousand rows, read by every deal)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    dongs = keys.select(F.substring("pnu", 1, 10).alias("legal_dong"))
    if LINEAGE_SOURCE in frames:
        sources = frames[LINEAGE_SOURCE].join(keys.withColumnRenamed("pnu", "successor_pnu"), "successor_pnu", "left_semi")
        dongs = dongs.unionByName(sources.select(F.substring("predecessor_pnu", 1, 10).alias("legal_dong")))
    dongs = dongs.distinct().cache()
    out = {name: under(frames[name], "pnu", dongs) for name in PNU_INPUTS}
    out[RT_SOURCE] = frames[RT_SOURCE].join(F.broadcast(dongs), "legal_dong", "left_semi")
    for name in (CODE_CHANGE_SOURCE, LEGAL_DONG_SOURCE, LINEAGE_SOURCE):
        if name in frames:
            out[name] = frames[name]
    return out


def affected_dongs(changed: list[str], appended: list[str], *, old: Any, new: dict[str, Any], appended_deals: Any,
                   aged: list[tuple[date, date]]) -> tuple[Any | None, list[str]]:
    """(a frame of the `legal_dong`s a change reaches, the code prefixes a changed code pair reaches).

    `old` is a zero-argument callable returning the inputs at the previous pins; it is called only
    when a difference is needed. `appended_deals` the deals the load appended, with their codes."""
    from functools import reduce  # noqa: PLC0415

    from pyspark.sql import functions as F  # noqa: PLC0415

    import gold_incremental as incremental  # noqa: PLC0415
    import legal_dong_code_change_views as views  # noqa: PLC0415

    parts, prefixes = [], []
    pick = lambda frame, column: frame.select(F.substring(column, 1, 10).alias("legal_dong"))  # noqa: E731
    if RT_SOURCE in appended:
        parts.append(appended_deals().select("legal_dong"))
    elif RT_SOURCE in changed:
        parts.append(incremental.changed_rows(old()[RT_SOURCE], new[RT_SOURCE], ("row_key",)).select("legal_dong"))
    if CODE_CHANGE_SOURCE in changed:
        columns = ("kind", "old_code", "new_code", "level")
        before, after = ([r.asDict() for r in frames[CODE_CHANGE_SOURCE].select(*columns).collect()]
                         for frames in (old(), new))
        prefixes = sorted({views.code_prefix(code) for code in views.changed_codes(before, after, columns)})
    if LEGAL_DONG_SOURCE in changed:
        parts.append(pick(incremental.changed_rows(old()[LEGAL_DONG_SOURCE], new[LEGAL_DONG_SOURCE], ("region_cd",)),
                          "region_cd"))
    for name in PNU_INPUTS:
        if name in changed:
            parts.append(pick(incremental.changed_rows(old()[name], new[name], ("pnu",)), "pnu"))
    if LINEAGE_SOURCE in changed and LINEAGE_SOURCE in new:
        edges = incremental.changed_rows(old()[LINEAGE_SOURCE], new[LINEAGE_SOURCE], ("predecessor_pnu", "successor_pnu"))
        parts.extend(pick(edges, side) for side in ("predecessor_pnu", "successor_pnu"))
    for start, end in aged:
        parts.append(new[RT_SOURCE].where((F.col("deal_date") >= F.lit(start)) & (F.col("deal_date") < F.lit(end)))
                     .select("legal_dong"))
    parts = [p.where(F.col("legal_dong").isNotNull()) for p in parts]
    dongs = reduce(lambda a, b: a.unionByName(b), parts).distinct() if parts else None
    return dongs, prefixes


def affected_pnus(dongs: Any | None, prefixes: list[str], parcels: list[Any]) -> Any:
    """Every PNU of `parcels` (the served parcels and the current Gold's PNUs) under a 법정동 or prefix."""
    from functools import reduce  # noqa: PLC0415

    from pyspark.sql import functions as F  # noqa: PLC0415

    found = []
    for frame in parcels:
        if dongs is not None:
            found.append(under(frame.select("pnu"), "pnu", dongs))
        if prefixes:
            hit = reduce(lambda a, b: a | b, [F.col("pnu").startswith(p) for p in prefixes])
            found.append(frame.select("pnu").where(hit))
    if not found:
        return parcels[0].select("pnu").limit(0)
    return reduce(lambda a, b: a.unionByName(b), found).distinct()


def validate_gold(gold: Any, expected: int | None = None) -> tuple[int, dict[str, int]]:
    """One row per valid PNU, each with its section (the quality gates the contract names)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415

    metrics = {k: int(v or 0) for k, v in gold.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.sum(F.when(~F.col("pnu").rlike(panel.PNU_PATTERN), 1).otherwise(0)).alias("invalid_pnu_count"),
        F.sum(F.when(F.col("real_transaction_json").isNull(), 1).otherwise(0)).alias("empty_section_count"),
        F.sum(F.when(F.length("real_transaction_json") > DOCUMENT_BYTE_LIMIT, 1).otherwise(0)).alias("oversize_count"),
    ).first().asDict().items()}
    duplicates = gold.groupBy("pnu").count().where(F.col("count") > 1).limit(3).count()
    if duplicates or metrics["invalid_pnu_count"] or metrics["empty_section_count"] or metrics["oversize_count"]:
        raise ValueError(f"Gold quality gates failed: {metrics}, {duplicates} duplicated PNUs")
    if expected is not None and metrics["row_count"] != expected:
        raise ValueError(f"Expected {expected} Gold rows, found {metrics['row_count']}")
    return metrics["row_count"], {k: v for k, v in metrics.items() if k != "row_count"}


def parse_args(argv=None) -> argparse.Namespace:
    """The arguments `scripts/ops/gold-panel-rebuild.sh` gives every producer (root ADR-0193 §7)."""
    import gold_incremental  # noqa: PLC0415
    import section_coverage  # noqa: PLC0415

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
    parser.add_argument("--no-carry-lineage", dest="carry_lineage", action="store_false",
                        help="Leave a deal on a retired PNU unplaced instead of following the lineage.")
    parser.add_argument("--allow-non-smoke-overwrite", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--minimum-count", type=int, help="Refuse, before writing, a Gold with fewer rows (root ADR-0139).")
    parser.add_argument("--summary-output")
    parser.add_argument("--lineage-output")
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


def read_inputs(spark: Any, args: argparse.Namespace, at: dict[str, str]) -> dict[str, Any]:
    """The build inputs at `at`, each its current rows as the build columns; the deals with their codes."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415
    from platform_contracts import current_rows, load_lakehouse_contract  # noqa: PLC0415

    def current(name: str) -> Any:
        return current_rows(panel.read_source(spark, args, name, at), load_lakehouse_contract(name))

    snapshots = panel.read_source(spark, args, LEGAL_DONG_SOURCE, at)
    newest = snapshots.agg(F.max("snapshot_date")).first()[0]
    names = snapshots.where(F.col("snapshot_date") == F.lit(newest)).select(*BUILD_COLUMNS[LEGAL_DONG_SOURCE])
    changes = panel.read_source(spark, args, CODE_CHANGE_SOURCE, at).select(*BUILD_COLUMNS[CODE_CHANGE_SOURCE])
    rows, _remap, _ambiguous = address_codes(
        [(r["region_cd"], r["full_name"], r["status"]) for r in names.collect()],
        [r.asDict() for r in changes.collect()])
    deals = with_codes(current(RT_SOURCE).select(*DEAL_COLUMNS), code_frame(spark, rows))
    frames = {
        RT_SOURCE: deals.select(*BUILD_COLUMNS[RT_SOURCE]),
        PARCEL_SOURCE: panel.read_served_parcels(spark, args, at, region=False).select("pnu"),
        CODE_CHANGE_SOURCE: changes,
        LEGAL_DONG_SOURCE: names,
        LAND_SOURCE: current(LAND_SOURCE).select(*BUILD_COLUMNS[LAND_SOURCE]),
        TITLE_SOURCE: current(TITLE_SOURCE).where(F.col("main_or_annex_kind") == F.lit("main"))
        .select(*BUILD_COLUMNS[TITLE_SOURCE]),
        OWNERSHIP_SOURCE: current(OWNERSHIP_SOURCE).select(*BUILD_COLUMNS[OWNERSHIP_SOURCE]),
    }
    if args.carry_lineage:
        frames[LINEAGE_SOURCE] = panel.read_source(spark, args, LINEAGE_SOURCE, at).select(*BUILD_COLUMNS[LINEAGE_SOURCE])
    return frames


def previous_as_of(spark: Any, table: str) -> date | None:
    """The build month of the current Gold: its newest row's publication."""
    if not spark.catalog.tableExists(table.replace("`", "")):
        return None
    value = spark.sql(f"SELECT max(published_at_utc) AS t FROM {table}").first()["t"]
    return None if not value else date.fromisoformat(str(value)[:10])


def main(argv=None) -> int:
    from pyspark.sql import SparkSession  # noqa: PLC0415

    import by_pnu_kinds  # noqa: PLC0415
    import gold_incremental as incremental  # noqa: PLC0415
    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415
    import rt_molit_real_transaction as rt  # noqa: PLC0415
    import section_coverage  # noqa: PLC0415
    from gold_rebuild import assert_minimum_row_count, pins_properties  # noqa: PLC0415
    from lakehouse_engine import apply_catalog_settings, assert_catalog_env  # noqa: PLC0415
    from lakehouse_snapshot_pins import load_source_snapshot_pins  # noqa: PLC0415
    from platform_contracts import column_names, ensure_contract_table, load_lakehouse_contract  # noqa: PLC0415

    args = parse_args(argv)
    kind = by_pnu_kinds.kind(args.lane)
    if kind.producer != JOB_NAME:
        raise ValueError(f"{args.lane} names producer {kind.producer}, not {JOB_NAME}")
    args.published_at_utc = panel.normalize_utc_timestamp(args.published_at_utc)
    as_of = date.fromisoformat(args.published_at_utc[:10])
    inference = rt.load_source_contract()["place_inference"]
    sources = declared_sources(args.carry_lineage)
    pins = load_source_snapshot_pins("iceberg", args.source_snapshots_path, sources, PARCEL_SOURCE, args.iceberg_snapshot_id)
    previous = incremental.validate_arguments(args, sources)
    contract = load_lakehouse_contract(kind.gold_table)
    columns = tuple(column_names(contract))
    if columns != ("pnu", *kind.columns, "attached_via", "row_digest", "source_snapshot_id", "published_at_utc"):
        raise ValueError(f"{kind.gold_table} columns {columns} are not the kind line's {kind.columns}")
    table = f"`{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`.`{kind.gold_table.split('.', 1)[1]}`"
    region_table = f"`{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`.`{REGION_TABLE.split('.', 1)[1]}`"
    rt_table = f"{args.iceberg_catalog_name}.{args.source_iceberg_namespace}.{RT_SOURCE.split('.', 1)[1]}"
    assert_catalog_env(args.iceberg_catalog_name)
    spark = apply_catalog_settings(SparkSession.builder.appName(f"{JOB_NAME}:{args.lane}")
                                   .config("spark.sql.session.timeZone", "UTC"), args.iceberg_catalog_name).getOrCreate()
    counters: dict[str, int] = {}
    try:
        # The registers only the inference reads stay uncached: with no level enabled they are never
        # scanned, and the 토지소유 table alone is ~80M rows (2026-11-03).
        frames = {name: frame if name in INFERENCE_ONLY else frame.cache()
                  for name, frame in read_inputs(spark, args, pins).items()}
        older: dict[str, Any] = {}

        def previous_frames() -> dict[str, Any]:
            if not older:
                older.update(read_inputs(spark, args, previous))
            return older

        def appended_deals() -> Any:
            # Removal markers too: a deal withdrawn from its month must leave its PNU's document.
            appended = incremental.read_appended(spark, rt_table, previous[RT_SOURCE], pins[RT_SOURCE])
            rows, _remap, _ambiguous = address_codes(
                [(r["region_cd"], r["full_name"], r["status"]) for r in frames[LEGAL_DONG_SOURCE].collect()],
                [r.asDict() for r in frames[CODE_CHANGE_SOURCE].collect()])
            return with_codes(appended.select("address_raw"), code_frame(spark, rows))

        reached: dict[str, Any] = {}

        def affected(changed: list[str], appended: list[str]) -> Any:
            dongs, prefixes = affected_dongs(changed, appended, old=previous_frames, new=frames,
                                             appended_deals=appended_deals,
                                             aged=aged_out_months(previous_as_of(spark, table), as_of))
            # The same 법정동s bound the region statistics' rewrite, widened to their 시군구.
            reached["scope"] = region_scope(dongs, prefixes)
            return affected_pnus(dongs, prefixes, [frames[PARCEL_SOURCE], spark.table(table)])

        lane = incremental.Lane(
            build=lambda inputs: gold_rows(inputs, inference, as_of, pins[PARCEL_SOURCE], args.published_at_utc, columns),
            restrict=restrict_inputs, affected=affected, validate=validate_gold,
            content_columns=("pnu", *kind.columns, "attached_via"), gold_columns=columns,
            append_inputs={RT_SOURCE: rt_table})
        coverage = section_coverage.guard_from_arguments(spark, args, table, {SECTION: kind.columns[0]},
                                                         frames[CODE_CHANGE_SOURCE], shares=False)
        outcome = incremental.build_or_merge(spark, args, lane, frames, pins, previous, table, validate_gold,
                                             assert_minimum_row_count, coverage)
        try:
            if not args.validate_only:
                spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`")
                ensure_contract_table(spark, table, contract)
                outcome.commit(spark, table, pins, args.iceberg_write_mode, contract_table=contract["table_name"])
                incremental.retain_input_snapshots(spark, args.iceberg_catalog_name, args.source_iceberg_namespace,
                                                   kind.gold_table, pins)
                # The region statistics: whole after a full build (or when the table is new), else
                # only the 시군구 the incremental change reached.
                region_contract = load_lakehouse_contract(REGION_TABLE)
                ensure_contract_table(spark, region_table, region_contract)
                whole = outcome.mode == "full" or spark.table(region_table).limit(1).count() == 0
                counters.update(merge_region_stats(
                    spark, region_table, frames, None if whole else reached.get("scope", []), as_of,
                    pins[PARCEL_SOURCE], args.published_at_utc, tuple(column_names(region_contract)),
                    pins_properties(region_table, pins)))
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
            json.dump({"job_name": JOB_NAME, "output_dataset": kind.gold_table, "output_datasets": [kind.gold_table, REGION_TABLE],
                       "inputs": dict(sorted(pins.items()))},
                      lineage, sort_keys=True)
    print(f"gold-kind-{'validate' if args.validate_only else 'write'}-ok lane={args.lane} rows={outcome.row_count} "
          f"mode={outcome.mode}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
