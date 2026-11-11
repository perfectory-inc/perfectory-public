"""The parcel panel's lineage section (root ADR-0113 §1, ADR-0115 §9): each parcel's lineage edges
as values. Its own module so the panel producer stays under the repository's file length bound."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import legal_dong_code_change_views as views
from lineage_review_queue import STEWARD, steward_resolved
from parcel_lineage import SAME_LOT_PAIRED_DONG

# The values a lineage section entry carries (root ADR-0113 §1): no evidence text, no run identity.
LINEAGE_EDGE_COLUMNS: tuple[str, ...] = ("predecessor_pnu", "successor_pnu", "relation", "cardinality",
                                         "effective_date", "grade")


def drop_outranked_code_changes_frame(lineage: DataFrame) -> DataFrame:
    """`parcel_lineage.drop_outranked_code_changes` over a frame, on the executors (root ADR-0329 §3):
    of one predecessor's `same_lot_paired_dong` links, only those of the best-graded pair source stand.
    A reader that keeps only some successors' rows applies it first, since the replaced link and the
    one in force reach different successors."""

    rank = F.lit(len(views.EVIDENCE_RANKS))
    for prefix, value in reversed(views.EVIDENCE_RANKS):
        rank = F.when(F.coalesce(F.col("evidence_ref"), F.lit("")).startswith(prefix), F.lit(value)).otherwise(rank)
    ranked = (F.coalesce(F.col("evidence_kind"), F.lit("")) == SAME_LOT_PAIRED_DONG) & (
        F.coalesce(F.col("predecessor_pnu"), F.lit("")) != "")
    scored = lineage.withColumn("_ranked", ranked).withColumn("_rank", rank)
    # A grouped minimum, not a window over every predecessor: the pending rows share an empty one.
    best = scored.where("_ranked").groupBy("predecessor_pnu").agg(F.min("_rank").alias("_best"))
    return (scored.join(best, "predecessor_pnu", "left")
            .where(~F.col("_ranked") | (F.col("_rank") == F.col("_best")))
            .drop("_ranked", "_rank", "_best"))


def rows_reaching(lineage: DataFrame, sources: set[str], hops: int) -> list[dict]:
    """The lineage rows a carry walk from `sources` reads (root ADR-0113 §6), collected: every row of
    each successor at most `hops` links forward of a source, with its outranked code changes dropped
    on the executors first (`drop_outranked_code_changes_frame`, which needs the whole table).

    `carry_candidates` walks back from a successor through its incoming links; a candidate whose
    source is in `sources` passes only successors that many hops forward of it, and those successors'
    rows are all here, so the walk over these rows gives the same such candidates as the walk over the
    table. The whole table is millions of code-change rows: on 2026-11-10 the real-transaction Gold
    collected it all beside its 13.5M deals and its Spark container was killed for memory, twice."""

    lineage = drop_outranked_code_changes_frame(lineage.distinct())
    links = (lineage.where((F.coalesce(F.col("predecessor_pnu"), F.lit("")) != "")
                           & (F.col("predecessor_pnu") != F.col("successor_pnu")))
             .select("predecessor_pnu", "successor_pnu").distinct())
    spark = lineage.sparkSession
    reached: set[str] = set()
    frontier = set(sources)
    for _hop in range(hops):
        if not frontier:
            break
        ends = F.broadcast(spark.createDataFrame(sorted((p,) for p in frontier), "predecessor_pnu string"))
        frontier = {r["successor_pnu"] for r in links.join(ends, "predecessor_pnu", "left_semi")
                    .select("successor_pnu").distinct().collect()} - reached
        reached |= frontier
    if not reached:
        return []
    wanted = F.broadcast(spark.createDataFrame(sorted((p,) for p in reached), "successor_pnu string"))
    return [row.asDict() for row in lineage.join(wanted, "successor_pnu", "left_semi").collect()]


def build_lineage(lineage: DataFrame | None, spark: SparkSession) -> DataFrame:
    """Each parcel's lineage edges as values, both directions (root ADR-0113 §1): `lineage_json` of
    a PNU lists every edge it is the predecessor or the successor of.

    Read through the steward's standing decisions (root ADR-0115 §9) without collecting the table:
    only the successors a steward row names are resolved on the driver, by the same function every
    consumer uses; the rest pass as derived. A renumbered sido is millions of code-change edges.
    Edges repeated across derivation runs are one entry; entries are sorted, so the JSON is stable.
    """

    if lineage is None:
        return spark.createDataFrame([], "pnu string, lineage_json string")
    lineage = drop_outranked_code_changes_frame(lineage)
    stewarded = lineage.where(F.col("evidence_kind") == STEWARD).select("successor_pnu").distinct()
    decided = [row.asDict() for row in lineage.join(stewarded, "successor_pnu", "left_semi").collect()]
    plain = lineage.join(stewarded, "successor_pnu", "left_anti").select(*LINEAGE_EDGE_COLUMNS)
    resolved = spark.createDataFrame(
        [tuple(row.get(column) for column in LINEAGE_EDGE_COLUMNS) for row in steward_resolved(decided)],
        plain.schema,
    )
    edges = plain.unionByName(resolved)
    entry = F.struct(*(F.col(column) for column in LINEAGE_EDGE_COLUMNS)).alias("edge")
    ends = edges.select(F.col("successor_pnu").alias("pnu"), entry).unionByName(
        edges.where(F.coalesce(F.col("predecessor_pnu"), F.lit("")) != "")
        .select(F.col("predecessor_pnu").alias("pnu"), entry)
    )
    return ends.groupBy("pnu").agg(F.to_json(F.array_sort(F.collect_set("edge"))).alias("lineage_json"))
