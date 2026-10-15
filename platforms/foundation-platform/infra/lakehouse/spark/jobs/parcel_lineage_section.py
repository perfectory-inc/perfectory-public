"""The parcel panel's lineage section (root ADR-0113 §1, ADR-0115 §9): each parcel's lineage edges
as values. Its own module so the panel producer stays under the repository's file length bound."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lineage_review_queue import STEWARD, steward_resolved

# The values a lineage section entry carries (root ADR-0113 §1): no evidence text, no run identity.
LINEAGE_EDGE_COLUMNS: tuple[str, ...] = ("predecessor_pnu", "successor_pnu", "relation", "cardinality",
                                         "effective_date", "grade")


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
