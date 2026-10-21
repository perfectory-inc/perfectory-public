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
first, each with its codes (and the names Onbid's own code service gives them, when that reference
is loaded), its money values, its bid window and Onbid's own identifiers. Closed rounds and their
results stay in Silver, append-only; the screen shows a round only while `bid_ends_at` lies ahead,
so one that closes between builds disappears without waiting for the next. Values only: codes stay
codes, and the words are the consumer's.
"""
from __future__ import annotations

import argparse
import json
import os
from typing import Any

JOB_NAME = "onbid_public_sale_silver_to_gold"
ITEM_SOURCE = "silver.onbid_real_estate_item"
PARCEL_SOURCE = "silver.parcel_boundaries"
LINEAGE_SOURCE = "silver.parcel_lineage"
KEY = ("cltr_mng_no", "pbct_cdtn_no")
USAGE_CODE_SOURCE = "reference.onbid_usage_code"
REMOVED = "removed"
LINEAGE_MISSING = "lineage_missing"
# Lineage grades not yet in effect (root ADR-0113 §4): never followed.
NOT_IN_EFFECT = ("needs_review", "pending")
# The datasets a served round comes from (data.go.kr ids), carried so the screen can name its source.
SOURCE_DATASETS = ("data.go.kr:15157207", "data.go.kr:15157254")
# One document's ceiling (root ADR-0193): rounds beyond it are dropped from the document, latest
# closing first; they stay in Silver.
DOCUMENT_BYTE_LIMIT = 256 * 1024
DOCUMENT_ENVELOPE_BYTES = 1024
# A served round's values, in order: identifiers, codes, money, the bid window, sizes.
ROUND_FIELDS = (
    "cltr_mng_no", "pbct_cdtn_no", "onbid_cltr_no", "onbid_pbanc_no", "pbct_no", "pbct_nsq", "item_name",
    "prpt_div_cd", "pbct_stat_cd", "dsps_mthod_cd", "bid_mthod_cd",
    "usg_lcls_ctgr_id", "usg_mcls_ctgr_id", "usg_scls_ctgr_id",
    "appraisal_amount_krw", "lowest_bid_price_krw", "lowest_bid_price_raw", "first_bid_price_krw", "usbd_nft",
    "bid_begins_at", "bid_ends_at", "land_area_m2", "building_area_m2", "share_item_yn",
)
USAGE_LEVELS = ("lcls", "mcls", "scls")


def lane_inputs(lane: str) -> tuple[tuple[str, ...], dict[str, str], str, tuple[str, ...]]:
    """(required inputs, optional input -> flag without it, anchor input, whole-table inputs), the
    seam `gold_rebuild.producer_inputs` reads. The item table is read whole: `current_rows` reads
    every version to pick the current one per key, so any change to it rebuilds the kind."""
    del lane
    return ((PARCEL_SOURCE, ITEM_SOURCE), {LINEAGE_SOURCE: "--no-carry-lineage"}, PARCEL_SOURCE,
            (ITEM_SOURCE,))


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
    """Each usage code's name as Onbid's code service gives it (`code`, `name`), or null while the
    reference is not loaded. Never a name of ours."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    named = round_rows
    for level in USAGE_LEVELS:
        code = f"usg_{level}_ctgr_id"
        name = f"usg_{level}_ctgr_name"
        if usage_codes is None:
            named = named.withColumn(name, F.lit(None).cast("string"))
        else:
            names = usage_codes.select(F.col("code").alias(code), F.col("name").alias(name)).dropDuplicates([code])
            named = named.join(F.broadcast(names), code, "left")
    return named


def document_rows(placed_rounds: Any, limit_bytes: int = DOCUMENT_BYTE_LIMIT) -> Any:
    """One row per PNU: `public_sale_json` (the served rounds, soonest-closing first, cut at the
    document ceiling), `round_count`, and `attached_via` — null when the PNU holds a round of its
    own, else its nearest carried round's path."""
    from pyspark.sql import Window, functions as F  # noqa: PLC0415

    fields = [*ROUND_FIELDS, *(f"usg_{level}_ctgr_name" for level in USAGE_LEVELS), "attached_via"]
    entries = placed_rounds.withColumn("_entry", F.struct(*[F.col(f) for f in fields]))
    order = Window.partitionBy("pnu").orderBy(F.col("bid_ends_at").asc(), *[F.col(k).asc() for k in KEY])
    sized = (entries.withColumn("_bytes", F.length(F.to_json("_entry")) + F.lit(1))
             .withColumn("_running", F.sum("_bytes").over(order.rowsBetween(Window.unboundedPreceding, 0))))
    kept = sized.where(F.col("_running") <= F.lit(limit_bytes - DOCUMENT_ENVELOPE_BYTES))
    # Nulls are written, so a document's field paths are always its record's (contracts/by-pnu-documents).
    return kept.groupBy("pnu").agg(
        F.to_json(F.struct(
            F.array_sort(F.collect_list(F.struct(F.col("bid_ends_at").alias("_sort"), F.col("cltr_mng_no").alias("_k1"),
                                                 F.col("pbct_cdtn_no").alias("_k2"), F.col("_entry").alias("round"))))
            .getField("round").alias("rounds"),
            F.array(*[F.lit(d) for d in SOURCE_DATASETS]).alias("source_datasets"),
        ), {"ignoreNullFields": "false"}).alias("public_sale_json"),
        F.count(F.lit(1)).alias("round_count"),
        F.when(F.max(F.col("attached_via").isNull().cast("int")) == 1, F.lit(None).cast("string"))
        .otherwise(F.min("attached_via")).alias("attached_via"),
    )


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
    parser.add_argument("--summary-output")
    parser.add_argument("--lineage-output")
    args = parser.parse_args(argv)
    args.region_prefix = None
    if args.minimum_count is not None and args.minimum_count < 0:
        parser.error("--minimum-count must be non-negative")
    if not args.validate_only and not args.allow_non_smoke_overwrite:
        parser.error("overwriting the Gold needs --allow-non-smoke-overwrite")
    return args


def build_gold(spark: Any, args: argparse.Namespace, pins: dict[str, str], counters: dict[str, int]) -> Any:
    """The kind's Gold rows at `pins`: one per PNU that holds a served round."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415
    from lakehouse_snapshot_pins import read_pinned_iceberg  # noqa: PLC0415

    as_of = panel.normalize_utc_timestamp(args.published_at_utc)
    # The parcel universe is the SERVED edition only (root ADR-0148), the same one the panel, map and
    # catalog are built from — not every edition held side by side in silver.parcel_boundaries. Reading
    # all editions (the generic read_source) attached sales to parcels a newer, unpromoted edition
    # holds but the served map does not (2026-10-21: 632 PNUs of editions 202609/202610 served under
    # 202606), which the by-PNU serving gate then refused. read_served_parcels is the panel's own
    # served-edition reader, so this kind sees exactly the parcels the host lane serves.
    parcels = panel.read_served_parcels(spark, args, pins, region=False).select("pnu")
    # Read whole: rounds() runs the contract's current_rows over every version to take each round's current row.
    items = read_pinned_iceberg(spark, f"{args.iceberg_catalog_name}.{args.source_iceberg_namespace}.{ITEM_SOURCE.split('.', 1)[1]}",
                                ITEM_SOURCE, pins)
    candidates = panel.read_carry_candidates(spark, args, counters, pins) if args.carry_lineage else None
    merges = None
    if args.carry_lineage:
        merges = (panel.read_source(spark, args, LINEAGE_SOURCE, pins).where(F.col("relation") == F.lit("merge"))
                  .select("predecessor_pnu", "successor_pnu", "grade"))
    usage = None
    if USAGE_CODE_SOURCE in pins:
        usage = panel.read_source(spark, args, USAGE_CODE_SOURCE, pins).select("code", "name")
    round_rows = rounds(items)
    served = served_rounds(round_rows, F.to_timestamp(F.lit(as_of)))
    counters["served_rounds"] = served.count()
    counters["served_rounds_without_pnu"] = served.where(F.col("source_pnu").isNull()).count()
    located = placed(with_usage_names(served, usage), parcels, candidates, merges)
    counters["lineage_missing_rounds"] = located.where(F.col("attached_via") == F.lit(LINEAGE_MISSING)).count()
    documents = document_rows(located)
    return documents.select(
        "pnu", "public_sale_json", "attached_via",
        F.lit(pins[PARCEL_SOURCE]).alias("source_snapshot_id"), F.lit(as_of).alias("published_at_utc"),
    ).withColumn("row_digest", F.sha2(F.to_json(F.struct("pnu", "public_sale_json", "attached_via")), 256))


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
