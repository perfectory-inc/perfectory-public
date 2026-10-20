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

The document content each sale contributes is decided in root ADR-0287 (pending); this module holds
the PNU resolution the content is keyed by.
"""
from __future__ import annotations

from typing import Any

JOB_NAME = "onbid_public_sale_silver_to_gold"
ITEM_SOURCE = "silver.onbid_real_estate_item"
RESULT_SOURCE = "silver.onbid_item_bid_result"
PARCEL_SOURCE = "silver.parcel_boundaries"
LINEAGE_SOURCE = "silver.parcel_lineage"
KEY = ("cltr_mng_no", "pbct_cdtn_no")
REMOVED = "removed"
LINEAGE_MISSING = "lineage_missing"


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


def main() -> int:
    raise SystemExit(f"{JOB_NAME}: the document content is pending root ADR-0287; this build is the PNU resolution only")


if __name__ == "__main__":
    raise SystemExit(main())
