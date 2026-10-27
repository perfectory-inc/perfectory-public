#!/usr/bin/env python3
"""Build the 법원경매 (court auction) by-PNU kind's Gold from its own Silver (root ADR-0280, ADR-0281,
ADR-0287).

A kind with its own producer (`by_pnu_kinds.Kind.producer`): served on the parcel lane like a
panel section, but built from `silver.court_auction_item`, not from the parcel panel, so it is
neither pinned to nor checked against it. Mirrors `onbid_public_auction_silver_to_gold.py`; the
court item's key, "open" rule and served columns are its own (the silver.court_auction_item
contract). The court item table carries no media index and no usage-code reference, so those
onbid stages are absent here.

공공저작물: 법원경매 데이터는 출처 표시가 필요한 공공저작물이다(저작권법 제24조의2, 루트 ADR-0281
§6). 문서마다 `court_auction.source_attribution` 로 "대한민국법원 법원경매정보" 를 담아, 이 행을
보여 주는 화면이 출처를 구체적으로 표시하게 한다.

Which PNU an item belongs to (the same placement as 공매, root ADR-0287):

- an item (`court_office_code`, `case_no`, `property_seq`, `item_seq`) takes the PNU of its current
  row (the contract's `current_rows`: newest per key by source_snapshot_id, root ADR-0267). A 물건
  whose current row is a removal marker has no current row and is not served (root ADR-0287 §7);
- an item whose PNU is no longer a current parcel follows the parcel lineage to every current
  successor (`parcel_attribute_carry`), in addition to whatever the successor holds itself, with the
  path in `attached_via`; an item on a parcel that merged into a current parcel is added to that
  parcel (`merged:<grade>`); a link not yet in effect (needs_review, pending) is not followed;
- a retired PNU with no lineage path keeps the item under itself, flagged `lineage_missing`. An item
  is never dropped.

What a PNU's document holds: the items in progress as of the build, soonest sale date first, each
with its codes, money values, court identifiers and sale schedule. Values only: codes stay codes,
and the words are the consumer's. The court item's sale date is a date, not an instant, so an item
is shown while its sale date is today or ahead and the site still marks it in progress.
"""
from __future__ import annotations

import argparse
import json
import os
from typing import Any

JOB_NAME = "court_auction_silver_to_gold"
ITEM_SOURCE = "silver.court_auction_item"
PARCEL_SOURCE = "silver.parcel_boundaries"
LINEAGE_SOURCE = "silver.parcel_lineage"
# The court item's four-part key (boCd, saNo, maemulSer, mokmulSer), the court_auction_item contract's.
KEY = ("court_office_code", "case_no", "property_seq", "item_seq")
IN_PROGRESS = "Y"
REMOVED = "removed"
LINEAGE_MISSING = "lineage_missing"
# Lineage grades not yet in effect (root ADR-0113 §4): never followed.
NOT_IN_EFFECT = ("needs_review", "pending")
# 공공저작물 출처 표시(저작권법 제24조의2, 루트 ADR-0281 §6): a consumer of these rows must name it.
SOURCE_ATTRIBUTION = "대한민국법원 법원경매정보"
# The source a served item comes from, carried so the screen can name it.
SOURCE_DATASETS = ("courtauction.go.kr",)
# One document's ceiling (root ADR-0193): items beyond it are dropped from the document, soonest sale
# date first; they stay in Silver.
DOCUMENT_BYTE_LIMIT = 256 * 1024
DOCUMENT_ENVELOPE_BYTES = 1024
# A served item's values, in order: identifiers, status/usage codes, money, the sale schedule.
ROUND_FIELDS = (
    "court_office_code", "case_no", "property_seq", "item_seq", "user_case_no", "court_name", "department_name",
    "building_name",
    "case_status_code", "item_status_code", "item_in_progress_yn",
    "usage_lcls_code", "usage_mcls_code", "usage_scls_code",
    "appraisal_amount_krw", "minimum_bid_price_krw", "sale_amount_krw",
    "sale_date", "sale_time_raw", "sale_decision_date", "sale_place", "failed_bid_count",
)


def lane_inputs(lane: str) -> tuple[tuple[str, ...], dict[str, str], str, tuple[str, ...]]:
    """(required inputs, optional input -> flag without it, anchor input, whole-table inputs), the
    seam `gold_rebuild.producer_inputs` reads. The item table is read whole: `current_rows` reads
    every version to pick the current one per key, so any change to it rebuilds the kind."""
    del lane
    return ((PARCEL_SOURCE, ITEM_SOURCE), {LINEAGE_SOURCE: "--no-carry-lineage"}, PARCEL_SOURCE, (ITEM_SOURCE,))


def rounds(items: Any) -> Any:
    """One row per item: its current row, carrying that row's PNU as `source_pnu`.

    The current row is the contract's (`current_rows`): the newest per key by source_snapshot_id,
    with an item whose newest row is a removal marker dropped (root ADR-0267, ADR-0275). The item's
    PNU is that current row's PNU, with no scan back through the history for an earlier row that had
    one."""
    from pyspark.sql import functions as F  # noqa: PLC0415
    from platform_contracts import current_rows, load_lakehouse_contract  # noqa: PLC0415

    return (current_rows(items, load_lakehouse_contract(ITEM_SOURCE)).withColumnRenamed("pnu", "source_pnu")
            .withColumn("is_open", F.col("row_state").isNull() | (F.col("row_state") != F.lit(REMOVED))))


def placed(sales: Any, current_pnus: Any, candidates: Any | None, merges: Any | None = None) -> Any:
    """`sales` (rows with `source_pnu`) placed on the PNUs they are served under: `pnu` and
    `attached_via` (null under its own current PNU, the lineage path under a successor,
    `merged:<grade>` under the current parcel it merged into, `lineage_missing` under a retired PNU
    no path leaves). `merges` are the lineage's merge links (`predecessor_pnu`, `successor_pnu`,
    `grade`). Items without a PNU are not placed."""
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
    """The items a document serves: open (a current row that is not a removal marker), marked in
    progress by the site, with a sale date still today or ahead of `as_of`. An item without a sale
    date is not served: the screen could never tell when it is auctioned. The sale date is a date,
    so the comparison is by date, not instant (`as_of` is coerced to a date by the caller)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    return round_rows.where(
        F.col("is_open") & (F.col("item_in_progress_yn") == F.lit(IN_PROGRESS))
        & F.col("sale_date").isNotNull() & (F.col("sale_date") >= F.lit(as_of)))


def document_rows(placed_rounds: Any, limit_bytes: int = DOCUMENT_BYTE_LIMIT) -> Any:
    """One row per PNU: `court_auction_json` (the served items, soonest sale date first, cut at the
    document ceiling, with the source and its attribution), `round_count`, and `attached_via` — null
    when the PNU holds an item of its own, else its nearest carried item's path."""
    from pyspark.sql import Window, functions as F  # noqa: PLC0415

    fields = [*ROUND_FIELDS, "attached_via"]
    entries = placed_rounds.withColumn("_entry", F.struct(*[F.col(f) for f in fields]))
    order = Window.partitionBy("pnu").orderBy(F.col("sale_date").asc(), *[F.col(k).asc() for k in KEY])
    sized = (entries.withColumn("_bytes", F.length(F.to_json("_entry")) + F.lit(1))
             .withColumn("_running", F.sum("_bytes").over(order.rowsBetween(Window.unboundedPreceding, 0))))
    kept = sized.where(F.col("_running") <= F.lit(limit_bytes - DOCUMENT_ENVELOPE_BYTES))
    # Nulls are written, so a document's field paths are always its record's (contracts/by-pnu-documents).
    return kept.groupBy("pnu").agg(
        F.to_json(F.struct(
            F.array_sort(F.collect_list(F.struct(
                F.col("sale_date").alias("_sort"), *[F.col(k).alias(f"_k{i}") for i, k in enumerate(KEY)],
                F.col("_entry").alias("round")))).getField("round").alias("rounds"),
            F.array(*[F.lit(d) for d in SOURCE_DATASETS]).alias("source_datasets"),
            F.lit(SOURCE_ATTRIBUTION).alias("source_attribution"),
        ), {"ignoreNullFields": "false"}).alias("court_auction_json"),
        F.count(F.lit(1)).alias("round_count"),
        F.when(F.max(F.col("attached_via").isNull().cast("int")) == 1, F.lit(None).cast("string"))
        .otherwise(F.min("attached_via")).alias("attached_via"),
    )


def servable(placed_rounds: Any) -> Any:
    """The placed items a document can carry: every one but `lineage_missing`. An item a retired PNU
    strands (no lineage path to a current parcel) sits on a PNU the parcel panel serves no document
    for, and this kind is a section of that document (root ADR-0287), so it has nothing to attach to:
    `build_gold` counts it (`lineage_missing_rounds`) and silver keeps it, but it is never written to
    the servable Gold, where it would only 404 the by-PNU serving gate (root ADR-0292)."""
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
                        help="Place a retired PNU's item under itself, flagged, instead of following the lineage.")
    parser.add_argument("--summary-output")
    parser.add_argument("--lineage-output")
    # The gold-rebuild harness passes these to every kind producer whose contract entry sets
    # section_coverage (gold_rebuild.plan, root ADR-0272); this own-producer must accept them like
    # the panel and by_pnu_kind producers, or the scheduled rebuild dies on unrecognized arguments.
    import section_coverage  # noqa: PLC0415

    section_coverage.add_arguments(parser)
    args = parser.parse_args(argv)
    args.region_prefix = None
    section_coverage.validate_arguments(args)
    if args.minimum_count is not None and args.minimum_count < 0:
        parser.error("--minimum-count must be non-negative")
    if not args.validate_only and not args.allow_non_smoke_overwrite:
        parser.error("overwriting the Gold needs --allow-non-smoke-overwrite")
    return args


def build_gold(spark: Any, args: argparse.Namespace, pins: dict[str, str], counters: dict[str, int]) -> Any:
    """The kind's Gold rows at `pins`: one per PNU that holds a served item."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415
    from lakehouse_snapshot_pins import read_pinned_iceberg  # noqa: PLC0415

    as_of = panel.normalize_utc_timestamp(args.published_at_utc)
    # The served cadastral edition only (root ADR-0148), the same one the panel, map and catalog are
    # built from, so this kind sees exactly the parcels the host lane serves (the 공매 precedent).
    parcels = panel.read_served_parcels(spark, args, pins, region=False).select("pnu")
    # Read whole: rounds() runs the contract's current_rows over every version to take each item's current row.
    items = read_pinned_iceberg(spark, f"{args.iceberg_catalog_name}.{args.source_iceberg_namespace}.{ITEM_SOURCE.split('.', 1)[1]}",
                                ITEM_SOURCE, pins)
    candidates = panel.read_carry_candidates(spark, args, counters, pins) if args.carry_lineage else None
    merges = None
    if args.carry_lineage:
        merges = (panel.read_source(spark, args, LINEAGE_SOURCE, pins).where(F.col("relation") == F.lit("merge"))
                  .select("predecessor_pnu", "successor_pnu", "grade"))
    round_rows = rounds(items)
    served = served_rounds(round_rows, F.to_date(F.lit(as_of)))
    counters["served_rounds"] = served.count()
    counters["served_rounds_without_pnu"] = served.where(F.col("source_pnu").isNull()).count()
    located = placed(served, parcels, candidates, merges)
    counters["lineage_missing_rounds"] = located.where(F.col("attached_via") == F.lit(LINEAGE_MISSING)).count()
    documents = document_rows(servable(located))
    return documents.select(
        "pnu", "court_auction_json", "attached_via",
        F.lit(pins[PARCEL_SOURCE]).alias("source_snapshot_id"), F.lit(as_of).alias("published_at_utc"),
    ).withColumn("row_digest", F.sha2(F.to_json(F.struct("pnu", "court_auction_json", "attached_via")), 256))


def main(argv=None) -> int:
    from pyspark.sql import SparkSession, functions as F  # noqa: PLC0415

    import by_pnu_kinds  # noqa: PLC0415
    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415
    from gold_rebuild import assert_minimum_row_count, write_gold_snapshot  # noqa: PLC0415
    from lakehouse_engine import apply_catalog_settings, assert_catalog_env  # noqa: PLC0415
    from lakehouse_snapshot_pins import load_source_snapshot_pins  # noqa: PLC0415
    from platform_contracts import column_names, ensure_contract_table, load_lakehouse_contract  # noqa: PLC0415

    args = parse_args(argv)
    kind = by_pnu_kinds.kind(args.lane)
    if kind.producer != JOB_NAME:
        raise ValueError(f"{args.lane} names producer {kind.producer}, not {JOB_NAME}")
    args.published_at_utc = panel.normalize_utc_timestamp(args.published_at_utc)
    required, optional, anchor, _ = lane_inputs(args.lane)
    sources = (*required, *((LINEAGE_SOURCE,) if args.carry_lineage else ()))
    pins = load_source_snapshot_pins("iceberg", args.source_snapshots_path, sources, anchor, args.iceberg_snapshot_id)
    contract = load_lakehouse_contract(kind.gold_table)
    columns = column_names(contract)
    table = f"`{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`.`{kind.gold_table.split('.', 1)[1]}`"
    assert_catalog_env(args.iceberg_catalog_name)
    spark = apply_catalog_settings(SparkSession.builder.appName(f"{JOB_NAME}:{args.lane}")
                                   .config("spark.sql.session.timeZone", "UTC"), args.iceberg_catalog_name).getOrCreate()
    counters: dict[str, int] = {}
    try:
        gold = build_gold(spark, args, pins, counters).select(*columns).cache()
        rows = gold.count()
        duplicates = gold.groupBy("pnu").count().where(F.col("count") > 1).count()
        invalid = gold.where(~F.col("pnu").rlike(panel.PNU_PATTERN)).count()
        if duplicates or invalid:
            raise ValueError(f"Gold quality gates failed: {duplicates} duplicated and {invalid} invalid PNUs")
        assert_minimum_row_count(rows, args.minimum_count)
        # Like the 공매 kind, the court-auction kind's rows are the parcels with an in-progress court
        # auction as of the build, which swings day to day as items are sold or withdrawn; its row-loss
        # bound and the section-coverage flags parse_args accepts are tuned against a clean baseline once
        # collection runs (root ADR-0287, ADR-0292). parse_args accepts but does not enforce the
        # harness's section-coverage flags so the scheduled rebuild does not die on them.
        if not args.validate_only:
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`")
            ensure_contract_table(spark, table, contract)
            write_gold_snapshot(gold, table, "overwrite", pins, F.lit(True))
        summary = {
            "schema_version": panel.RUN_SUMMARY_SCHEMA_VERSION, "job_name": JOB_NAME, "contract": kind.gold_table,
            "lane": args.lane, "created_at_utc": panel.normalize_utc_timestamp(None), "row_count": rows,
            "write_disposition": "validate_only" if args.validate_only else "iceberg_overwrite",
            "quality_metrics": counters, "columns": list(columns),
            "source_iceberg_snapshots_by_dataset": dict(sorted(pins.items())), "rebuild": {"mode": "full"},
        }
        gold.unpersist()
    finally:
        spark.stop()
    panel.emit_run_summary(summary, args.summary_output)
    if args.lineage_output and not args.validate_only:
        with open(args.lineage_output, "w", encoding="utf-8") as lineage:
            json.dump({"job_name": JOB_NAME, "output_dataset": kind.gold_table, "inputs": dict(sorted(pins.items()))},
                      lineage, sort_keys=True)
    print(f"gold-kind-{'validate' if args.validate_only else 'write'}-ok lane={args.lane} rows={rows} mode=full")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
