#!/usr/bin/env python3
"""Build the 법원경매 (court auction) by-PNU kind's Gold from its own Silver (root ADR-0280, ADR-0287,
ADR-0353).

A kind with its own producer (`by_pnu_kinds.Kind.producer`): served on the parcel lane like a
panel section, but built from `silver.court_auction_item`, not from the parcel panel, so it is
neither pinned to nor checked against it. It places items exactly as the 공매 kind places rounds
(`onbid_public_auction_silver_to_gold.placed`, shared, not copied): the court item's key, its "open"
rule and its served values are its own. The list searches carry no photo (the 물건상세 that does is
not collected, root ADR-0353 §4), so a document has no media index.

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
with its codes, money values, court identifiers, sale schedule and the address the court lists.
Values only: codes stay codes, and the words are the consumer's.

Partial (root ADR-0312 §2, ADR-0314): only the PNUs an item change reaches are rebuilt and merged
(`affected_pnus`), plus those whose items' sale date passed since the current Gold was built. A
change to the served parcels or the lineage moves placements the item rows do not name, so those
two are whole-table inputs and rebuild the kind whole.
"""
from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from typing import Any

import onbid_public_auction_silver_to_gold as onbid

JOB_NAME = "court_auction_silver_to_gold"
ITEM_SOURCE = "silver.court_auction_item"
PARCEL_SOURCE = onbid.PARCEL_SOURCE
LINEAGE_SOURCE = onbid.LINEAGE_SOURCE
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
)
GOLD_CONTENT = ("court_auction_json", "attached_via")
# The item identifiers of a served document, enough to find which PNUs an item is served under.
DOCUMENT_ROUND_KEYS = ("struct<rounds:array<struct<court_office_code:string,case_no:string,"
                       "property_seq:int,item_seq:int>>>")


def lane_inputs(lane: str) -> tuple[tuple[str, ...], dict[str, str], str, tuple[str, ...]]:
    """(required inputs, optional input -> flag without it, anchor input, whole-table inputs), the
    seam `gold_rebuild.producer_inputs` reads. The item table is not whole: `affected_pnus` maps its
    changed keys to PNUs. The parcels and the lineage are: a new edition or link moves items the
    item rows do not name."""
    del lane
    return ((PARCEL_SOURCE, ITEM_SOURCE), {LINEAGE_SOURCE: "--no-carry-lineage"}, PARCEL_SOURCE,
            (PARCEL_SOURCE, LINEAGE_SOURCE))


def declared_sources(lane: str, carry_lineage: bool) -> tuple[str, ...]:
    """Every Silver source this build pins: exactly what the rebuild harness pins from `lane_inputs`
    (`load_source_snapshot_pins` rejects any mismatch, the 2026-10-27 공매 crash)."""
    required, _optional, _anchor, _whole = lane_inputs(lane)
    return (*required, *((LINEAGE_SOURCE,) if carry_lineage else ()))


def rounds(items: Any) -> Any:
    """One row per item: its current row, carrying that row's PNU as `source_pnu` (root ADR-0267)."""
    from pyspark.sql import functions as F  # noqa: PLC0415
    from platform_contracts import current_rows, load_lakehouse_contract  # noqa: PLC0415

    return (current_rows(items, load_lakehouse_contract(ITEM_SOURCE)).withColumnRenamed("pnu", "source_pnu")
            .withColumn("is_open", F.col("row_state").isNull() | (F.col("row_state") != F.lit(REMOVED))))


def served_rounds(round_rows: Any, as_of: Any) -> Any:
    """The items a document serves: open, marked in progress by the site, with a sale date today or
    ahead of `as_of` (a date). An item without a sale date is not served: the screen could never
    tell when it is auctioned."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    return round_rows.where(
        F.col("is_open") & (F.col("item_in_progress_yn") == F.lit(IN_PROGRESS))
        & F.col("sale_date").isNotNull() & (F.col("sale_date") >= as_of))


def document_rows(placed_rounds: Any, limit_bytes: int = DOCUMENT_BYTE_LIMIT) -> Any:
    """One row per PNU: `court_auction_json` (the served items, soonest sale date first, cut at the
    document ceiling, with the source and its attribution), `round_count`, and `attached_via` — null
    when the PNU holds an item of its own, else its nearest carried item's path."""
    from pyspark.sql import Window, functions as F  # noqa: PLC0415

    fields = [*ROUND_FIELDS, "attached_via"]
    # An earlier snapshot of the item list (or a test frame) may lack a later column: it is null.
    entries = placed_rounds.withColumn("_entry", F.struct(*[
        (F.col(f) if f in placed_rounds.columns else F.lit(None).cast("string")).alias(f) for f in fields]))
    order = Window.partitionBy("pnu").orderBy(F.col("sale_date").asc(), *[F.col(k).asc() for k in KEY])
    sized = (entries.withColumn("_bytes", F.length(F.to_json("_entry", WRITTEN)) + F.lit(1))
             .withColumn("_running", F.sum("_bytes").over(order.rowsBetween(Window.unboundedPreceding, 0))))
    kept = sized.where(F.col("_running") <= F.lit(limit_bytes - DOCUMENT_ENVELOPE_BYTES))
    # Nulls are written, so a document's field paths are always the registry's (root ADR-0336).
    return kept.groupBy("pnu").agg(
        F.to_json(F.struct(
            F.array_sort(F.collect_list(F.struct(
                F.col("sale_date").alias("_sort"), *[F.col(k).alias(f"_k{i}") for i, k in enumerate(KEY)],
                F.col("_entry").alias("round")))).getField("round").alias("rounds"),
            F.array(*[F.lit(d) for d in SOURCE_DATASETS]).alias("source_datasets"),
            F.lit(SOURCE_ATTRIBUTION).alias("source_attribution"),
        ), WRITTEN).alias("court_auction_json"),
        F.count(F.lit(1)).alias("round_count"),
        F.when(F.max(F.col("attached_via").isNull().cast("int")) == 1, F.lit(None).cast("string"))
        .otherwise(F.min("attached_via")).alias("attached_via"),
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
    return onbid.placed(served, frames[PARCEL_SOURCE], how.candidates, how.merges)


def gold_rows(located: Any, as_of: str, source_snapshot_id: str, columns: tuple[str, ...]) -> Any:
    """The kind's Gold rows from placed items: one per PNU that holds a servable item."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    return document_rows(onbid.servable(located)).select(
        "pnu", *GOLD_CONTENT, F.lit(source_snapshot_id).alias("source_snapshot_id"),
        F.lit(as_of).alias("published_at_utc"),
    ).withColumn("row_digest", F.sha2(F.to_json(F.struct("pnu", *GOLD_CONTENT)), 256)).select(*columns)


def items_of(frames: dict[str, Any], keys: Any) -> dict[str, Any]:
    """`frames` with the item rows cut to the items of `keys` (every version of each)."""
    return {name: frame.join(keys.select(*KEY), list(KEY), "left_semi") if name == ITEM_SOURCE else frame
            for name, frame in frames.items()}


def restrict_inputs(frames: dict[str, Any], keys: Any, how: Placement) -> dict[str, Any]:
    """The input rows the Gold rows of the PNUs `keys` are built from: every item placed on them."""
    return items_of(frames, placement(frames, how).join(keys, "pnu", "left_semi").select(*KEY).distinct())


def document_round_keys(gold: Any) -> Any:
    """(pnu, item key) for every item a Gold document serves."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    rounds_json = F.from_json("court_auction_json", DOCUMENT_ROUND_KEYS)["rounds"]
    return (gold.select("pnu", F.explode(rounds_json).alias("_r"))
            .select("pnu", *[F.col(f"_r.{k}").alias(k) for k in KEY]))


def affected_pnus(old: dict[str, Any], new: dict[str, Any], how: Placement, current_gold: Any,
                  previous_as_of: str | None, changed: list[str]) -> Any:
    """The PNUs whose document can differ from the current Gold's (root ADR-0312 §2).

    An item is touched when a row of it changed in the item table, or when its sale date passed
    since the current Gold was built (`previous_as_of`: it left the served items with no input
    change). Its PNUs are those the current Gold serves it under and those it is placed on now, so
    an item that moved, closed or appeared reaches both ends."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    import gold_incremental as incremental  # noqa: PLC0415

    touched = []
    if ITEM_SOURCE in changed and ITEM_SOURCE in old and ITEM_SOURCE in new:
        touched.append(incremental.changed_rows(old[ITEM_SOURCE], new[ITEM_SOURCE], KEY).select(*KEY))
    today = F.to_date(F.to_timestamp(F.lit(how.as_of)))
    passed = rounds(new[ITEM_SOURCE]).where(F.col("sale_date").isNotNull() & (F.col("sale_date") < today))
    if previous_as_of is not None:
        passed = passed.where(F.col("sale_date") >= F.to_date(F.to_timestamp(F.lit(previous_as_of))))
    touched.append(passed.select(*KEY))
    keys = touched[0] if len(touched) == 1 else touched[0].unionByName(touched[1])
    keys = keys.distinct()
    now = placement(items_of(new, keys), how).select("pnu")
    before = document_round_keys(current_gold).join(keys, list(KEY), "left_semi").select("pnu")
    return now.unionByName(before).distinct()


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

    def versions(at: dict[str, str]) -> Any:
        # Every version, as the contract's columns (an older snapshot reads a later column as null).
        table = f"{args.iceberg_catalog_name}.{args.source_iceberg_namespace}.{ITEM_SOURCE.split('.', 1)[1]}"
        return with_contract_columns(read_pinned_iceberg(spark, table, ITEM_SOURCE, at), load_lakehouse_contract(ITEM_SOURCE))

    frames = {ITEM_SOURCE: versions(pins).cache()}
    old = None if previous is None or ITEM_SOURCE not in previous else {ITEM_SOURCE: versions(previous)}
    named = frames[ITEM_SOURCE].select(F.col("pnu").alias("predecessor_pnu")).where(F.col("predecessor_pnu").isNotNull()).distinct()
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
    frames[PARCEL_SOURCE] = onbid.parcels_named(served, frames[ITEM_SOURCE], candidates, merges).cache()
    return frames, old, Placement(args.published_at_utc, candidates, merges)


def count_placement(located: Any, counters: dict[str, int]) -> None:
    """The run summary's counts of a full build's placed items."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    counters["served_rounds"] = located.select(*KEY).distinct().count()
    counters["lineage_missing_rounds"] = located.where(F.col("attached_via") == F.lit(LINEAGE_MISSING)).count()


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
    sources = declared_sources(args.lane, args.carry_lineage)
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
            return gold_rows(located, args.published_at_utc, pins[PARCEL_SOURCE], columns)

        lane = incremental.Lane(
            build=build, restrict=lambda inputs, keys: restrict_inputs(inputs, keys, how),
            affected=lambda changed, _appended: affected_pnus(
                old or {}, frames, how, spark.table(table), onbid.previous_build_time(spark, table), changed),
            validate=onbid.validate_gold, content_columns=GOLD_CONTENT, gold_columns=columns,
            whole_table_inputs=whole)
        outcome = incremental.build_or_merge(spark, args, lane, frames, pins, previous, table, onbid.validate_gold,
                                             assert_minimum_row_count)
        # Held to the whole-table row-loss floor (gold-panel-rebuild.contract.json kind_tables
        # overrides), not ADR-0272's per-시도 gate: the rows are the parcels with an item in progress,
        # which swing day to day as sale dates pass, the 공매 kind's reason (root ADR-0292).
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
