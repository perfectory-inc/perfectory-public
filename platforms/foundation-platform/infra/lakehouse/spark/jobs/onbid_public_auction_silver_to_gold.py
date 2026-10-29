#!/usr/bin/env python3
"""Build the 공매 (Onbid public sale) by-PNU kind's Gold from its own Silver (root ADR-0280, ADR-0287).

A kind with its own producer (`by_pnu_kinds.Kind.producer`): served on the parcel lane like a
panel section, but built from `silver.onbid_real_estate_item`, not from the parcel panel, so it is
neither pinned to nor checked against it.

Which PNU a sale belongs to:

- a round (`cltr_mng_no`, `pbct_cdtn_no`) takes the PNU of its current item row (the contract's
  `current_rows`: newest per key by source_snapshot_id, root ADR-0267). A closed round, whose
  current row is a removal marker, has no current row and is not served (root ADR-0287 §7);
- a sale whose PNU is no longer a current parcel follows the parcel lineage to every current
  successor (`parcel_attribute_carry`, the panel's candidates: identity chains and re-lotting), in
  addition to whatever the successor holds itself, with the path in `attached_via`;
- a sale on a parcel that merged into a current parcel is added to that parcel (`merged:<grade>`).
  The carry candidates never cross a merge because one attribute cannot come from several
  originals; a sale is added, not chosen, so the merged land shows each predecessor's sale. One
  merge hop into a current parcel; a link not yet in effect (needs_review, pending) is not followed;
- a retired PNU with no lineage path keeps the sale under itself, flagged `lineage_missing`. A sale
  is never dropped.

What a PNU's document holds (root ADR-0287 §7): the rounds open as of the build, soonest-closing
first, each with its codes and the names the provider gives them (root ADR-0340 §8), its money
values, its bid window and conditions, and Onbid's own identifiers. Closed rounds and their results
stay in Silver, append-only; the screen shows a round only while `bid_ends_at` lies ahead, so one
that closes between builds disappears without waiting for the next. Beside the rounds, once per
document, the facts of each item and announcement round the served rounds name
(`onbid_public_auction_document`, root ADR-0340 §7): the item's detail, bid information, earlier
rounds' results and maps; the announcement's text, attachments, organisation and opening.
"""
from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from typing import Any

import onbid_public_auction_document as document

JOB_NAME = "onbid_public_auction_silver_to_gold"
ITEM_SOURCE = "silver.onbid_real_estate_item"
PARCEL_SOURCE = "silver.parcel_boundaries"
LINEAGE_SOURCE = "silver.parcel_lineage"
KEY = ("cltr_mng_no", "pbct_cdtn_no")
# The media belong to the item, not to a round (root ADR-0339): every round of an item carries them.
ITEM = "cltr_mng_no"
USAGE_CODE_SOURCE = "reference.onbid_usage_code"
PHOTO_SOURCE = "silver.onbid_real_estate_item_photo"
# A round's media as the document carries it (root ADR-0296 §5): the content hash and its facts, from
# which the web builds the public URL. Order matches the Silver columns the struct is built from.
PHOTO_STRUCT = ("array<struct<sha256:string,media_kind:string,sequence:int,title:string,"
                "media_format:string,size_bytes:bigint>>")
REMOVED = "removed"
LINEAGE_MISSING = "lineage_missing"
# Lineage grades not yet in effect (root ADR-0113 §4): never followed.
NOT_IN_EFFECT = ("needs_review", "pending")
# The datasets a served round comes from (data.go.kr ids), carried so the screen can name its source.
SOURCE_DATASETS = ("data.go.kr:15157207", "data.go.kr:15157254", "data.go.kr:15157247", "data.go.kr:15157251",
                   "data.go.kr:15157216", "data.go.kr:15157222", "data.go.kr:15157218")
# One document's ceiling (root ADR-0193): rounds beyond it are dropped from the document, latest
# closing first; they stay in Silver.
DOCUMENT_BYTE_LIMIT = 256 * 1024
DOCUMENT_ENVELOPE_BYTES = 1024
# How a document is written: nulls too, so its paths are always the registry's — and so its size is
# measured the same way (a size that skipped nulls let a document of many null fields past the ceiling).
WRITTEN = {"ignoreNullFields": "false"}
# A served round's values, in order: identifiers, codes, money, the bid window, sizes.
ROUND_FIELDS = (
    "cltr_mng_no", "pbct_cdtn_no", "onbid_cltr_no", "onbid_pbanc_no", "pbct_no", "pbct_nsq", "item_name",
    "prpt_div_cd", "pbct_stat_cd", "dsps_mthod_cd", "bid_mthod_cd",
    "usg_lcls_ctgr_id", "usg_mcls_ctgr_id", "usg_scls_ctgr_id",
    "appraisal_amount_krw", "lowest_bid_price_krw", "lowest_bid_price_raw", "first_bid_price_krw", "usbd_nft",
    "bid_begins_at", "bid_ends_at", "land_area_m2", "building_area_m2", "share_item_yn",
    # Root ADR-0340 §7: the rest of the list row a buyer reads — the provider's names of its codes,
    # the bid conditions, the organisations, the notices it carries, the address it gives.
    "prpt_div_nm", "dsps_mthod_nm", "bid_mthod_nm", "cptn_mthod_cd", "cptn_mthod_nm", "totalamt_unpc_div_cd",
    "totalamt_unpc_div_nm", "pbct_stat_nm", "bid_div_cd", "bid_div_nm", "pvct_trgt_yn", "bid_prgn_nft",
    "batc_bid_yn", "crtn_yn", "fee_rate", "apsl_prc_ctrs_lowst_bid_rto", "frst_ctrs_lowst_bid_prc_rto",
    "org_name", "rqst_org_nm", "rent_mthod_nm", "rent_perd_cont", "dtbt_rqr_edtm_cont", "eltr_grpr_use_yn",
    "collb_bid_psbl_yn", "twtm_gthr_bid_psbl_yn", "subt_bid_psbl_yn", "evc_rsby_trgt_cont",
    "location_sido_name", "location_sigungu_name", "location_emd_name", "provider_modified_at",
)
USAGE_LEVELS = ("lcls", "mcls", "scls")
# The Silver tables beside the item list a document is built from (root ADR-0340 §7). Required: the
# Silver load creates every Onbid table on its first run, empty or not, and a table that is empty
# leaves its block's fields null.
DOCUMENT_SOURCES = document.SOURCES


def lane_inputs(lane: str) -> tuple[tuple[str, ...], dict[str, str], str, tuple[str, ...]]:
    """(required inputs, optional input -> flag without it, anchor input, whole-table inputs), the
    seam `gold_rebuild.producer_inputs` reads. The item table is read whole: `current_rows` reads
    every version to pick the current one per key, so any change to it rebuilds the kind."""
    del lane
    return ((PARCEL_SOURCE, ITEM_SOURCE, *DOCUMENT_SOURCES),
            {LINEAGE_SOURCE: "--no-carry-lineage", PHOTO_SOURCE: "--no-photos"}, PARCEL_SOURCE,
            (ITEM_SOURCE, PHOTO_SOURCE, *DOCUMENT_SOURCES))


def declared_sources(lane: str, carry_lineage: bool, photos: bool) -> tuple[str, ...]:
    """Every Silver source this build pins. It must name exactly what the rebuild harness pins from
    `lane_inputs` (the required inputs and every present optional), because `load_source_snapshot_pins`
    rejects any mismatch — a source pinned but not named here, or named but not pinned, fails the run.
    2026-10-27: the photo source was pinned (present) but omitted here, crashing the rebuild."""
    required, _optional, _anchor, _whole = lane_inputs(lane)
    return (*required,
            *((LINEAGE_SOURCE,) if carry_lineage else ()),
            *((PHOTO_SOURCE,) if photos else ()))


def rounds(items: Any) -> Any:
    """One row per round: its current item row, carrying that row's PNU as `source_pnu`.

    The current row is the contract's (`current_rows`): the newest per (cltr_mng_no, pbct_cdtn_no)
    by source_snapshot_id, with a round whose newest row is a removal marker dropped (root ADR-0267,
    ADR-0275). The round's PNU is that current row's PNU, with no scan back through the history for an
    earlier row that had one: measured 2026-10-20 on production Silver (79,246 items), 0 had a blank
    PNU on the current row while an earlier version had one, so no attachable sale loses its PNU this
    way; `pnu_null_share_guard` catches any future change in the source's behaviour."""
    from pyspark.sql import functions as F  # noqa: PLC0415
    from platform_contracts import current_rows, load_lakehouse_contract  # noqa: PLC0415

    return (current_rows(items, load_lakehouse_contract(ITEM_SOURCE)).withColumnRenamed("pnu", "source_pnu")
            .withColumn("is_open", F.col("row_state").isNull() | (F.col("row_state") != F.lit(REMOVED))))


def placed(sales: Any, current_pnus: Any, candidates: Any | None, merges: Any | None = None) -> Any:
    """`sales` (rows with `source_pnu`) placed on the PNUs they are served under: `pnu` and
    `attached_via` (null under its own current PNU, the lineage path under a successor,
    `merged:<grade>` under the current parcel it merged into, `lineage_missing` under a retired PNU
    no path leaves). `merges` are the lineage's merge links (`predecessor_pnu`, `successor_pnu`,
    `grade`). Sales without a PNU are not placed."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    current = current_pnus.select(F.col("pnu").alias("source_pnu")).distinct().withColumn("_current", F.lit(True))
    known = sales.where(F.col("source_pnu").isNotNull()).join(current, "source_pnu", "left")
    own = known.where(F.col("_current")).withColumn("pnu", F.col("source_pnu")).withColumn(
        "attached_via", F.lit(None).cast("string"))
    retired = known.where(F.col("_current").isNull())
    if candidates is None:
        followed = retired.limit(0).withColumn("pnu", F.col("source_pnu")).withColumn("attached_via", F.lit(""))
    else:
        successors = (candidates.join(current_pnus.select(F.col("pnu").alias("successor_pnu")).distinct(), "successor_pnu")
                      .select("source_pnu", F.col("successor_pnu").alias("pnu"), F.col("path").alias("attached_via")))
        followed = retired.join(successors, "source_pnu")
    if merges is not None:
        into = (merges.where(~F.col("grade").isin(*NOT_IN_EFFECT))
                .join(current_pnus.select(F.col("pnu").alias("successor_pnu")).distinct(), "successor_pnu")
                .select(F.col("predecessor_pnu").alias("source_pnu"), F.col("successor_pnu").alias("pnu"),
                        F.concat(F.lit("merged:"), F.col("grade")).alias("attached_via")).distinct())
        unfollowed = retired.join(followed.select("source_pnu").distinct(), "source_pnu", "left_anti")
        followed = followed.unionByName(unfollowed.join(into, "source_pnu"))
    stranded = (retired.join(followed.select("source_pnu").distinct(), "source_pnu", "left_anti")
                .withColumn("pnu", F.col("source_pnu")).withColumn("attached_via", F.lit(LINEAGE_MISSING)))
    columns = [c for c in own.columns if c != "_current"]
    return own.select(*columns).unionByName(followed.select(*columns)).unionByName(stranded.select(*columns))


def served_rounds(round_rows: Any, as_of: Any) -> Any:
    """The rounds a document serves: open, with a close time still ahead of `as_of`. A round
    without a close time is not served: the screen could never tell when it ended."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    return round_rows.where(F.col("is_open") & F.col("bid_ends_at").isNotNull() & (F.col("bid_ends_at") > F.lit(as_of)))


def with_usage_names(round_rows: Any, usage_codes: Any | None) -> Any:
    """Each usage code's name as the provider gives it: the name every list row carries beside the
    code (`cltr_usg_*_ctgr_nm`, root ADR-0340 §8), else Onbid's code service's (`code`, `name`) when
    that reference is loaded, else null. Never a name of ours."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    named = round_rows
    for level in USAGE_LEVELS:
        code = f"usg_{level}_ctgr_id"
        name = f"usg_{level}_ctgr_name"
        inline = f"cltr_usg_{level}_ctgr_nm"
        if usage_codes is None:
            named = named.withColumn(name, F.lit(None).cast("string"))
        else:
            names = usage_codes.select(F.col("code").alias(code), F.col("name").alias(name)).dropDuplicates([code])
            named = named.join(F.broadcast(names), code, "left")
        if inline in named.columns:
            named = named.withColumn(name, F.coalesce(F.col(inline), F.col(name)))
    return named


def photos_by_item(photo_rows: Any) -> Any:
    """Each item's media as one array, keyed by item: its current (item, hash) rows (the photo
    table's `current_rows`, root ADR-0339), ordered by kind then the provider's list order
    (`sequence`)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    photo_rows = photo_rows.where(F.col("media_kind").isin(*document.ROUND_MEDIA_KINDS))
    return (photo_rows.groupBy(ITEM).agg(F.array_sort(F.collect_list(F.struct(
        F.col("media_kind").alias("_kind"), F.col("sequence").alias("_seq"),
        F.struct("sha256", "media_kind", "sequence", "title", "media_format", "size_bytes").alias("_photo"),
    ))).alias("_sorted")).select(ITEM, F.col("_sorted._photo").alias("photos")))


def with_photos(round_rows: Any, photos: Any | None) -> Any:
    """Attach to each round its item's `photos` (the media index joined by item: one 물건상세 serves
    every round, root ADR-0296 §5, ADR-0339), an empty array when the item has none or the photo
    table is absent — so every served round carries the field and the web reads one shape."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    empty = F.expr(f"CAST(array() AS {PHOTO_STRUCT})")
    if photos is None:
        return round_rows.withColumn("photos", empty)
    return round_rows.join(photos, ITEM, "left").withColumn("photos", F.coalesce(F.col("photos"), empty))


def document_rows(placed_rounds: Any, limit_bytes: int = DOCUMENT_BYTE_LIMIT, items: Any | None = None,
                  announcements: Any | None = None) -> Any:
    """One row per PNU: `public_auction_json` (the served rounds, soonest-closing first, cut at the
    document ceiling, and once each the item and announcement-round blocks they name), `round_count`,
    and `attached_via` — null when the PNU holds a round of its own, else its nearest carried round's
    path. A block's bytes count against the ceiling with the first (soonest-closing) round naming it,
    so a block is kept exactly when a round naming it is."""
    from pyspark.sql import Window, functions as F  # noqa: PLC0415

    shape = document.document_shape()
    fields = [*ROUND_FIELDS, *(f"usg_{level}_ctgr_name" for level in USAGE_LEVELS), "photos", "attached_via"]
    # An earlier snapshot of the item list (or a test frame) may lack a later column: it is null.
    entries = placed_rounds.withColumn("_entry", F.struct(*[
        (F.col(f) if f in placed_rounds.columns else F.lit(None).cast("string")).alias(f) for f in fields]))
    if items is None:
        entries = entries.withColumn("_item", F.lit(None).cast(document.spark_type(shape["items"][0])))
    else:
        entries = entries.join(items, document.ITEM, "left")
    if announcements is None:
        entries = entries.withColumn("_announcement",
                                     F.lit(None).cast(document.spark_type(shape["announcements"][0])))
    else:
        entries = entries.join(announcements, list(document.ANNOUNCEMENT_KEY), "left")
    order = Window.partitionBy("pnu").orderBy(F.col("bid_ends_at").asc(), *[F.col(k).asc() for k in KEY])

    def first_of(*columns: str) -> Any:
        within = Window.partitionBy("pnu", *columns).orderBy(F.col("bid_ends_at").asc(), *[F.col(k).asc() for k in KEY])
        return F.row_number().over(within) == 1

    def block_bytes(block: str, first: Any) -> Any:
        return F.when(first & F.col(block).isNotNull(), F.length(F.to_json(block, WRITTEN)) + F.lit(1)).otherwise(F.lit(0))

    entries = (entries.withColumn("_first_item", first_of(document.ITEM))
               .withColumn("_first_announcement", first_of(*document.ANNOUNCEMENT_KEY)))
    sized = (entries.withColumn("_bytes", F.length(F.to_json("_entry", WRITTEN)) + F.lit(1)
                                + block_bytes("_item", F.col("_first_item"))
                                + block_bytes("_announcement", F.col("_first_announcement")))
             .withColumn("_running", F.sum("_bytes").over(order.rowsBetween(Window.unboundedPreceding, 0))))
    kept = sized.where(F.col("_running") <= F.lit(limit_bytes - DOCUMENT_ENVELOPE_BYTES))
    # Nulls are written, so a document's field paths are always its record's (the schema registry).
    return kept.groupBy("pnu").agg(
        F.to_json(F.struct(
            F.array_sort(F.collect_list(F.struct(F.col("bid_ends_at").alias("_sort"), F.col("cltr_mng_no").alias("_k1"),
                                                 F.col("pbct_cdtn_no").alias("_k2"), F.col("_entry").alias("round"))))
            .getField("round").alias("rounds"),
            # collect_list skips nulls: one block per item and announcement round, ordered by its key.
            F.array_sort(F.collect_list(F.when(F.col("_first_item"), F.col("_item")))).alias("items"),
            F.array_sort(F.collect_list(F.when(F.col("_first_announcement"), F.col("_announcement"))))
            .alias("announcements"),
            F.array(*[F.lit(d) for d in SOURCE_DATASETS]).alias("source_datasets"),
        ), WRITTEN).alias("public_auction_json"),
        F.count(F.lit(1)).alias("round_count"),
        F.when(F.max(F.col("attached_via").isNull().cast("int")) == 1, F.lit(None).cast("string"))
        .otherwise(F.min("attached_via")).alias("attached_via"),
    )


def servable(placed_rounds: Any) -> Any:
    """The placed rounds a document can carry: every one but `lineage_missing`. A sale a retired PNU
    strands (no lineage path to a current parcel) sits on a PNU the parcel panel serves no document
    for, and this kind is a section of that document (root ADR-0287), so it has nothing to attach to:
    `build_gold` counts it (`lineage_missing_rounds`) and silver keeps it, but it is never written to
    the servable Gold, where it would only 404 the by-PNU serving gate and never reach a screen. Root
    ADR-0292 holds this own-producer kind to the invariant every kind has (its rows are a subset of
    its host panel's, ADR-0193 §9) that ADR-0287 §7 had let it bypass."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    return placed_rounds.where(~F.col("attached_via").eqNullSafe(F.lit(LINEAGE_MISSING)))


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
                        help="Place a retired PNU's sale under itself, flagged, instead of following the lineage.")
    parser.add_argument("--no-photos", dest="photos", action="store_false",
                        help="Build without the media index (silver.onbid_real_estate_item_photo absent or empty).")
    parser.add_argument("--summary-output")
    parser.add_argument("--lineage-output")
    # The gold-rebuild harness passes these to every kind producer whose contract entry sets
    # section_coverage (gold_rebuild.plan, root ADR-0272); this own-producer must accept them like
    # the panel and by_pnu_kind producers, or the scheduled rebuild dies on unrecognized arguments.
    import section_coverage  # noqa: PLC0415

    section_coverage.add_arguments(parser)
    # Root ADR-0312 §2: the harness passes the incremental flags when the plan says `incremental`.
    import gold_incremental  # noqa: PLC0415

    gold_incremental.add_arguments(parser)
    args = parser.parse_args(argv)
    # The shared build_or_merge reads these; this producer always writes the whole kind or merges.
    args.region_prefix, args.iceberg_write_mode, args.expected_count = None, "overwrite", None
    section_coverage.validate_arguments(args)
    if args.minimum_count is not None and args.minimum_count < 0:
        parser.error("--minimum-count must be non-negative")
    if not args.validate_only and not args.allow_non_smoke_overwrite:
        parser.error("overwriting the Gold needs --allow-non-smoke-overwrite")
    return args


GOLD_CONTENT = ("public_auction_json", "attached_via")
# The round identifiers of a served document, enough to find which PNUs a round is served under.
DOCUMENT_ROUND_KEYS = "struct<rounds:array<struct<cltr_mng_no:string,pbct_cdtn_no:bigint>>>"


@dataclass
class Placement:
    """What places a round besides its own rows: the lineage (whole-table inputs) and the build time."""

    as_of: str
    candidates: Any | None = None
    merges: Any | None = None
    usage: Any | None = None


def placement(frames: dict[str, Any], how: Placement) -> Any:
    """The served rounds of `frames` placed on their PNUs (one row per round and PNU, with photos).

    `frames[PARCEL_SOURCE]` is a frame of `pnu`: the current parcels, of which only those a sale or
    its lineage names matter (`parcels_named`)."""
    from pyspark.sql import functions as F  # noqa: PLC0415
    from platform_contracts import current_rows, load_lakehouse_contract  # noqa: PLC0415

    served = served_rounds(rounds(frames[ITEM_SOURCE]), F.to_timestamp(F.lit(how.as_of)))
    photos = None
    if PHOTO_SOURCE in frames:
        photos = photos_by_item(current_rows(frames[PHOTO_SOURCE], load_lakehouse_contract(PHOTO_SOURCE)))
    return placed(with_photos(with_usage_names(served, how.usage), photos), frames[PARCEL_SOURCE],
                  how.candidates, how.merges)


def blocks(frames: dict[str, Any], located: Any) -> tuple[Any, Any]:
    """The item and announcement-round blocks of the rounds `located` holds (root ADR-0340 §7)."""
    return document.item_blocks(frames, located), document.announcement_blocks(frames, located)


def gold_rows(located: Any, as_of: str, source_snapshot_id: str, columns: tuple[str, ...],
              frames: dict[str, Any] | None = None) -> Any:
    """The kind's Gold rows from placed rounds: one per PNU that holds a servable round, with the
    blocks of `frames` (none when `frames` is not given: empty `items` and `announcements`)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    servable_rounds = servable(located)
    items, announcements = blocks(frames, servable_rounds) if frames is not None else (None, None)
    return document_rows(servable_rounds, items=items, announcements=announcements).select(
        "pnu", *GOLD_CONTENT, F.lit(source_snapshot_id).alias("source_snapshot_id"),
        F.lit(as_of).alias("published_at_utc"),
    ).withColumn("row_digest", F.sha2(F.to_json(F.struct("pnu", *GOLD_CONTENT)), 256)).select(*columns)


def parcels_named(parcels: Any, items: Any, candidates: Any | None, merges: Any | None) -> Any:
    """The current parcels placement can ask about: every PNU an item row names and every successor
    the lineage may move it to. `placed` only tests those for being current, so the rest of the
    ~40M parcels are never carried past this join (root ADR-0312 §2)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    sources = items.select(F.col("pnu").alias("source_pnu")).where(F.col("source_pnu").isNotNull()).distinct()
    named = sources.select(F.col("source_pnu").alias("pnu"))
    if candidates is not None:
        named = named.unionByName(candidates.join(sources, "source_pnu").select(F.col("successor_pnu").alias("pnu")))
    if merges is not None:
        named = named.unionByName(merges.join(sources.withColumnRenamed("source_pnu", "predecessor_pnu"), "predecessor_pnu")
                                  .select(F.col("successor_pnu").alias("pnu")))
    return parcels.join(F.broadcast(named.distinct()), "pnu", "left_semi")


def rounds_of(frames: dict[str, Any], keys: Any) -> dict[str, Any]:
    """`frames` with the item rows cut to the rounds of `keys` and the photo rows to their items
    (every version of each; the media are the item's, root ADR-0339)."""
    cut = {ITEM_SOURCE: keys.select(*KEY), PHOTO_SOURCE: keys.select(ITEM).distinct()}
    return {name: frame.join(cut[name], cut[name].columns, "left_semi") if name in cut else frame
            for name, frame in frames.items()}


def restrict_inputs(frames: dict[str, Any], keys: Any, how: Placement) -> dict[str, Any]:
    """The input rows the Gold rows of the PNUs `keys` are built from: every round placed on them."""
    return rounds_of(frames, placement(frames, how).join(keys, "pnu", "left_semi").select(*KEY).distinct())


def document_round_keys(gold: Any) -> Any:
    """(pnu, round key) for every round a Gold document serves."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    rounds_json = F.from_json("public_auction_json", DOCUMENT_ROUND_KEYS)["rounds"]
    return (gold.select("pnu", F.explode(rounds_json).alias("_r"))
            .select("pnu", *[F.col(f"_r.{k}").alias(k) for k in KEY]))


def affected_pnus(old: dict[str, Any], new: dict[str, Any], how: Placement, current_gold: Any,
                  previous_as_of: str | None, changed: list[str]) -> Any:
    """The PNUs whose document can differ from the current Gold's (root ADR-0312 §2).

    A round is touched when a row of it changed in the item table, when a media row of its item
    changed in the photo table (every round of the item carries the media, root ADR-0339), or when
    its close time passed since the current Gold was built (`previous_as_of`: it left the served
    rounds with no input change). Its PNUs are those the current Gold serves it under and those it
    is placed on now, so a round that moved, closed or appeared reaches both ends."""
    from functools import reduce  # noqa: PLC0415

    from pyspark.sql import functions as F  # noqa: PLC0415

    import gold_incremental as incremental  # noqa: PLC0415

    def present(name: str) -> bool:
        return name in changed and name in old and name in new

    touched = [incremental.changed_rows(old[ITEM_SOURCE], new[ITEM_SOURCE], KEY)] if present(ITEM_SOURCE) else []
    # A media, detail, bid-info or result row is its item's: every round of the item is touched.
    for name in (PHOTO_SOURCE, document.DETAIL_SOURCE, document.BID_INFO_SOURCE, document.RESULT_SOURCE):
        if present(name):
            items = incremental.changed_rows(old[name], new[name], (ITEM,)).distinct()
            touched.append(new[ITEM_SOURCE].join(items, ITEM, "left_semi").select(*KEY))
    # An announcement row is its announcement round's (root ADR-0340 §7).
    for name in (document.ANNOUNCEMENT_SOURCE, document.ANNOUNCEMENT_RESULT_SOURCE, document.ANNOUNCEMENT_DETAIL_SOURCE):
        if present(name):
            announced = incremental.changed_rows(old[name], new[name], document.ANNOUNCEMENT_KEY).distinct()
            touched.append(new[ITEM_SOURCE].join(announced, list(document.ANNOUNCEMENT_KEY), "left_semi").select(*KEY))
    open_rounds = rounds(new[ITEM_SOURCE]).where(F.col("is_open") & F.col("bid_ends_at").isNotNull())
    closed = open_rounds.where(F.col("bid_ends_at") <= F.to_timestamp(F.lit(how.as_of)))
    if previous_as_of is not None:
        closed = closed.where(F.col("bid_ends_at") > F.to_timestamp(F.lit(previous_as_of)))
    touched.append(closed.select(*KEY))
    keys = reduce(lambda a, b: a.unionByName(b), [t.select(*KEY) for t in touched]).distinct()
    now = placement(rounds_of(new, keys), how).select("pnu")
    before = document_round_keys(current_gold).join(keys, list(KEY), "left_semi").select("pnu")
    return now.unionByName(before).distinct()


def validate_gold(gold: Any, expected: int | None = None) -> tuple[int, dict[str, int]]:
    """One row per valid PNU (the quality gates the contract names)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415

    rows = gold.count()
    duplicates = gold.groupBy("pnu").count().where(F.col("count") > 1).count()
    invalid = gold.where(~F.col("pnu").rlike(panel.PNU_PATTERN)).count()
    if duplicates or invalid:
        raise ValueError(f"Gold quality gates failed: {duplicates} duplicated and {invalid} invalid PNUs")
    if expected is not None and rows != expected:
        raise ValueError(f"Expected {expected} Gold rows, found {rows}")
    return rows, {}


def read_inputs(spark: Any, args: argparse.Namespace, pins: dict[str, str], counters: dict[str, int],
                previous: dict[str, str] | None) -> tuple[dict[str, Any], dict[str, Any] | None, Placement]:
    """(the inputs at `pins`, the item and photo tables at `previous` or None, how rounds are placed)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415
    from lakehouse_snapshot_pins import read_pinned_iceberg  # noqa: PLC0415
    from platform_contracts import load_lakehouse_contract, with_contract_columns  # noqa: PLC0415

    def versions(source: str, at: dict[str, str]) -> Any:
        # Every version, as the contract's columns (an older snapshot reads a later column as null).
        table = f"{args.iceberg_catalog_name}.{args.source_iceberg_namespace}.{source.split('.', 1)[1]}"
        return with_contract_columns(read_pinned_iceberg(spark, table, source, at), load_lakehouse_contract(source))

    tables = [ITEM_SOURCE, *((PHOTO_SOURCE,) if args.photos and PHOTO_SOURCE in pins else ()), *DOCUMENT_SOURCES]
    frames = {name: versions(name, pins).cache() for name in tables}
    old = None if previous is None else {name: versions(name, previous) for name in tables if name in previous}
    named = frames[ITEM_SOURCE].select(F.col("pnu").alias("predecessor_pnu")).where(F.col("predecessor_pnu").isNotNull()).distinct()
    sources = {r.predecessor_pnu for r in named.collect()}
    # Only the candidates from a PNU a sale names: a few thousand, not the lineage's 6.5M.
    candidates = panel.read_carry_candidates(spark, args, counters, pins, sources) if args.carry_lineage else None
    merges = None
    if args.carry_lineage:
        merges = (panel.read_source(spark, args, LINEAGE_SOURCE, pins).where(F.col("relation") == F.lit("merge"))
                  .select("predecessor_pnu", "successor_pnu", "grade")
                  .join(F.broadcast(named), "predecessor_pnu", "left_semi").cache())
    usage = None
    if USAGE_CODE_SOURCE in pins:
        usage = panel.read_source(spark, args, USAGE_CODE_SOURCE, pins).select("code", "name")
    # The parcel universe is the SERVED edition only (root ADR-0148), the same one the panel, map and
    # catalog are built from — not every edition held side by side in silver.parcel_boundaries. Reading
    # all editions (the generic read_source) attached sales to parcels a newer, unpromoted edition
    # holds but the served map does not (2026-10-21: 632 PNUs of editions 202609/202610 served under
    # 202606), which the by-PNU serving gate then refused. read_served_parcels is the panel's own
    # served-edition reader, so this kind sees exactly the parcels the host lane serves. It is read
    # once, cut to the parcels a sale can reach, and kept.
    served = panel.read_served_parcels(spark, args, pins, region=False).select("pnu")
    frames[PARCEL_SOURCE] = parcels_named(served, frames[ITEM_SOURCE], candidates, merges).cache()
    return frames, old, Placement(args.published_at_utc, candidates, merges, usage)


def count_placement(located: Any, counters: dict[str, int]) -> None:
    """The run summary's counts of a full build's placed rounds."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    counters["served_rounds"] = located.select(*KEY).distinct().count()
    counters["rounds_with_photos"] = located.where(F.size("photos") > 0).count()
    counters["lineage_missing_rounds"] = located.where(F.col("attached_via") == F.lit(LINEAGE_MISSING)).count()


def previous_build_time(spark: Any, table: str) -> str | None:
    """When the current Gold's newest rows were built (its rows' latest published_at_utc), or None."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    if not spark.catalog.tableExists(table.replace("`", "")):
        return None
    return spark.table(table).agg(F.max("published_at_utc")).first()[0]


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
    sources = declared_sources(args.lane, args.carry_lineage, args.photos)
    pins = load_source_snapshot_pins("iceberg", args.source_snapshots_path, sources, anchor, args.iceberg_snapshot_id)
    previous = incremental.validate_arguments(args, sources)
    contract = load_lakehouse_contract(kind.gold_table)
    columns = tuple(column_names(contract))
    table = f"`{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`.`{kind.gold_table.split('.', 1)[1]}`"
    assert_catalog_env(args.iceberg_catalog_name)
    spark = apply_catalog_settings(SparkSession.builder.appName(f"{JOB_NAME}:{args.lane}")
                                   .config("spark.sql.session.timeZone", "UTC"), args.iceberg_catalog_name).getOrCreate()
    counters: dict[str, int] = {}
    try:
        frames, old, how = read_inputs(spark, args, pins, counters, previous)

        def build(inputs: dict[str, Any]) -> Any:
            located = placement(inputs, how).cache()
            if inputs is frames:
                count_placement(located, counters)
            return gold_rows(located, args.published_at_utc, pins[PARCEL_SOURCE], columns, inputs)

        lane = incremental.Lane(
            build=build, restrict=lambda inputs, keys: restrict_inputs(inputs, keys, how),
            affected=lambda changed, _appended: affected_pnus(
                old or {}, frames, how, spark.table(table), previous_build_time(spark, table), changed),
            validate=validate_gold, content_columns=GOLD_CONTENT, gold_columns=columns, whole_table_inputs=whole)
        outcome = incremental.build_or_merge(spark, args, lane, frames, pins, previous, table, validate_gold,
                                             assert_minimum_row_count)
        # This open-auction kind is held to the whole-table row-loss floor (contract
        # max_row_loss_fraction 0.5, #836), not ADR-0272's per-시도 section-coverage gate. That gate
        # refuses a Gold whose section lost a 시도's coverage, for sections that grow slowly (a
        # register); this kind's rows are the parcels with an OPEN Onbid auction (root ADR-0287 §7), so
        # a 시도's count swings day to day and legitimately reaches zero (every round there closed) — a
        # regional drop is not a defect here, and a regional gate waits until that volatility is
        # measured against a clean baseline (root ADR-0292). parse_args still accepts the harness's
        # section-coverage flags so the scheduled rebuild (gold_rebuild.plan) does not die on them.
        try:
            if not args.validate_only:
                spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`")
                ensure_contract_table(spark, table, contract)
                outcome.commit(spark, table, pins, args.iceberg_write_mode)
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
