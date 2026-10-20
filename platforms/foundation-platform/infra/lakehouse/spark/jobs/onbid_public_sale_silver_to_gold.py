#!/usr/bin/env python3
"""Build the 공매 (Onbid public sale) by-PNU kind's Gold from its own Silver (root ADR-0280, ADR-0287).

A kind with its own producer (`by_pnu_kinds.Kind.producer`): served on the parcel lane like a
panel section, but built from `silver.onbid_real_estate_item` and `silver.onbid_item_bid_result`,
not from the parcel panel, so it is neither pinned to nor checked against it.

Which PNU a sale belongs to:

- a round (`cltr_mng_no`, `pbct_cdtn_no`) takes the PNU of its newest item row that has one; its
  removal marker repeats the last row, so a closed round keeps the PNU it was listed under;
- a bid result has no PNU of its own: it takes its round's;
- a sale whose PNU is no longer a current parcel follows the parcel lineage to every current
  successor (`parcel_attribute_carry`, the panel's candidates: identity chains and re-lotting, never
  a merge), in addition to whatever the successor holds itself, with the path in `attached_via`;
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

from typing import Any

JOB_NAME = "onbid_public_sale_silver_to_gold"
ITEM_SOURCE = "silver.onbid_real_estate_item"
RESULT_SOURCE = "silver.onbid_item_bid_result"
PARCEL_SOURCE = "silver.parcel_boundaries"
LINEAGE_SOURCE = "silver.parcel_lineage"
KEY = ("cltr_mng_no", "pbct_cdtn_no")
USAGE_CODE_SOURCE = "reference.onbid_usage_code"
REMOVED = "removed"
LINEAGE_MISSING = "lineage_missing"
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
    seam `gold_rebuild.producer_inputs` reads. The Onbid tables are read whole: a round's PNU comes
    from its whole history, so any change to them rebuilds the kind."""
    del lane
    return ((PARCEL_SOURCE, ITEM_SOURCE, RESULT_SOURCE), {LINEAGE_SOURCE: "--no-carry-lineage"}, PARCEL_SOURCE,
            (ITEM_SOURCE, RESULT_SOURCE))


def rounds(items: Any) -> Any:
    """One row per round: its newest item row (open unless that row is a removal marker) and the PNU
    of its newest row that has one."""
    from pyspark.sql import Window, functions as F  # noqa: PLC0415

    newest = Window.partitionBy(*KEY).orderBy(F.col("ingested_at_utc").desc(), F.col("source_line_number").desc())
    with_pnu = Window.partitionBy(*KEY).orderBy(
        F.col("pnu").isNull().asc(), F.col("ingested_at_utc").desc(), F.col("source_line_number").desc())
    pnus = (items.withColumn("_rank", F.row_number().over(with_pnu)).where(F.col("_rank") == 1)
            .select(*KEY, F.col("pnu").alias("source_pnu")))
    latest = items.withColumn("_rank", F.row_number().over(newest)).where(F.col("_rank") == 1).drop("_rank", "pnu")
    return (latest.join(pnus, list(KEY))
            .withColumn("is_open", F.col("row_state").isNull() | (F.col("row_state") != F.lit(REMOVED))))


def result_rounds(results: Any, round_rows: Any) -> Any:
    """Each round's newest bid result with its round's PNU; a result whose round was never listed
    with a PNU has `source_pnu` null (kept, so the count of unplaced results is visible)."""
    from pyspark.sql import Window, functions as F  # noqa: PLC0415

    newest = Window.partitionBy(*KEY).orderBy(F.col("ingested_at_utc").desc(), F.col("source_line_number").desc())
    latest = results.withColumn("_rank", F.row_number().over(newest)).where(F.col("_rank") == 1).drop("_rank")
    return latest.join(round_rows.select(*KEY, "source_pnu"), list(KEY), "left")


def placed(sales: Any, current_pnus: Any, candidates: Any | None) -> Any:
    """`sales` (rows with `source_pnu`) placed on the PNUs they are served under: `pnu` and
    `attached_via` (null under its own current PNU, the lineage path under a successor,
    `lineage_missing` under a retired PNU no path leaves). Sales without a PNU are not placed."""
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
    return kept.groupBy("pnu").agg(
        F.to_json(F.struct(
            F.array_sort(F.collect_list(F.struct(F.col("bid_ends_at").alias("_sort"), F.col("cltr_mng_no").alias("_k1"),
                                                 F.col("pbct_cdtn_no").alias("_k2"), F.col("_entry").alias("round"))))
            .getField("round").alias("rounds"),
            F.array(*[F.lit(d) for d in SOURCE_DATASETS]).alias("source_datasets"),
        )).alias("public_sale_json"),
        F.count(F.lit(1)).alias("round_count"),
        F.when(F.max(F.col("attached_via").isNull().cast("int")) == 1, F.lit(None).cast("string"))
        .otherwise(F.min("attached_via")).alias("attached_via"),
    )


def main() -> int:
    raise SystemExit(f"{JOB_NAME}: runs once its kind line and document record land (root ADR-0287); "
                     "until then the build is the tested functions above")


if __name__ == "__main__":
    raise SystemExit(main())
