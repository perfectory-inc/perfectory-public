"""One row per PNU from a dedup and an ordered collect, reduced map-side (moved out of
parcel_panel_silver_to_gold.py, which is past the line limit)."""

from pyspark.sql import functions as F


def collect_sorted_unique(frame, dedup_keys, order_by, entry, sort_comparator):
    """One row per PNU: for each `dedup_keys` group keep the entry whose `order_by` columns are
    smallest (nulls first, as a `row_number()` asc_nulls_first window picks row 1), then collect the
    kept entries per PNU ordered by `sort_comparator`. Returns `pnu` and `sorted_entries` (an array).

    The dedup is `min()` over a struct of (order_by..., entry): Spark reduces it map-side before the
    shuffle and orders struct fields nulls-first ascending (matching asc_nulls_first). A `row_number()`
    window cannot reduce map-side — it sorts each whole partition on one executor, the spill that made
    the full rebuild time out (measured 2026-10-22: the full parcel-panel rebuild hit its 160-minute
    limit and failed). `dedup_keys` must contain "pnu"; `order_by` must be a total order within a group.
    """
    ordered = F.struct(*(c.alias(f"_o{i}") for i, c in enumerate(order_by)), entry.alias("_entry"))
    chosen = (
        frame.groupBy(*dedup_keys)
        .agg(F.min(ordered).alias("_pick"))
        .select("pnu", F.col("_pick._entry").alias("entry"))
    )
    return chosen.groupBy("pnu").agg(
        F.expr(f"array_sort(collect_list(entry), {sort_comparator})").alias("sorted_entries")
    )
