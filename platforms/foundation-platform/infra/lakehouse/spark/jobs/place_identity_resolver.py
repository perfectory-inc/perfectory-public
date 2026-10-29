"""The one place-identity resolver every parcel source calls at ingestion (root ADR-0288 §1).

A parcel source names a place by a legal-dong CODE (공매 `ltnoPnu`) or by an ADDRESS (경매). This
turns either into the CURRENT pnu, never keeping a superseded code verbatim:

- CODE path (this module): a source code is carried old → current through the pairs already recorded
  in `reference.legal_dong_code_change` (root ADR-0143), so a 분구 (화성시 → 동탄구) that only changed
  the dong code resolves to the current parcel. The current pnu is kept only when it is a current
  parcel (`silver.parcel_boundaries`); a code that splits (one old → many new) or whose current parcel
  we do not hold leaves pnu null for the steward queue (ADR-0288 §4), never a guessed or stale pnu.
- ADDRESS path: `address_pnu` (what 경매 already calls). ADR-0288 §3 converges it into this module;
  until then a source that gives only an address keeps using `address_pnu` directly.

The chain reading is plain Python so it is tested without Spark; `resolve` does the Spark joins.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping

import legal_dong_code_change_views as views

# A via step the pipeline graph guard accepts as "this edge goes through the resolver" (ADR-0288 §1).
RESOLVER_VIA = "platforms/foundation-platform/infra/lakehouse/spark/jobs/place_identity_resolver.py"


def current_code(old: str, pairs: Mapping[str, list[str]], _seen: frozenset[str] = frozenset()) -> tuple[str | None, bool]:
    """Follow the old → new legal-dong pair chain to the current code.

    Returns (current code, ambiguous). ambiguous is True (and the code None) when a code splits into
    more than one successor or the chain cycles: either is a decision for a steward, not a guess.
    """

    if old in _seen:
        return None, True  # a cycle is not a current code
    nexts = pairs.get(old)
    if not nexts:
        return old, False  # no successor: this code is current
    if len(set(nexts)) > 1:
        return None, True  # one old code, many new: a split a steward resolves
    return current_code(nexts[0], pairs, _seen | {old})


def code_crosswalk(pair_rows: Iterable[tuple[str, str]]) -> tuple[dict[str, str], set[str]]:
    """(old → current code for codes that changed, the ambiguous old codes), from (old, new) pairs."""

    pairs: dict[str, list[str]] = {}
    for old, new in pair_rows:
        if old and new and old != new:  # a code recorded as changing to itself is no change
            pairs.setdefault(old, []).append(new)
    remap: dict[str, str] = {}
    ambiguous: set[str] = set()
    for old in pairs:
        current, is_ambiguous = current_code(old, pairs)
        if is_ambiguous:
            ambiguous.add(old)
        elif current != old:
            remap[old] = current
    return remap, ambiguous


def with_current_code(pnu: str, current_legal_code: str) -> str:
    """A 19-digit pnu with its 10-digit legal-dong code replaced by the current one; the lot is kept."""

    return current_legal_code + pnu[10:]


def pairs_from_lakehouse(spark: Any, catalog: str) -> tuple[dict[str, str], set[str]]:
    """The code crosswalk read from `reference.legal_dong_code_change`: the pairs in force, through the
    one view every reader uses, so a pair official evidence superseded is not read (root ADR-0329)."""

    table = f"`{catalog}`.`reference`.`legal_dong_code_change`"
    rows = [row.asDict() for row in spark.sql(f"SELECT kind, old_code, new_code, level, effective_date, source FROM {table}").collect()]
    return code_crosswalk((pair["old_code"], pair["new_code"]) for pair in views.pair_rows(rows))


def resolve(staged: Any, remap: Mapping[str, str], ambiguous: Iterable[str], parcels: Any) -> Any:
    """Replace ``staged.pnu`` (a format-valid source code, or null) with the CURRENT pnu and add
    ``place_identity_grade`` and ``pnu_unresolved_reason``.

    ``remap`` is old → current legal-dong code (``code_crosswalk``); ``ambiguous`` the old codes that
    split; ``parcels`` holds the current parcels' ``pnu`` (``address_pnu.lakehouse_inputs``). A pnu is
    kept only when its current form is a current parcel.
    """

    from pyspark.sql import functions as F  # noqa: PLC0415

    spark = staged.sparkSession
    code10 = F.substring(F.col("pnu"), 1, 10)
    crosswalk = spark.createDataFrame(list(remap.items()), "old_code string, current_code string")
    amb = spark.createDataFrame([(c,) for c in sorted(set(ambiguous))], "amb_code string")
    out = (staged.withColumn("_code10", code10)
           .join(crosswalk.withColumnRenamed("old_code", "_code10"), "_code10", "left")
           .join(amb.withColumnRenamed("amb_code", "_code10").withColumn("_amb", F.lit(True)), "_code10", "left"))
    is_ambiguous = F.coalesce(F.col("_amb"), F.lit(False))
    superseded = F.col("current_code").isNotNull()
    current10 = F.coalesce(F.col("current_code"), F.col("_code10"))
    candidate = F.when(F.col("pnu").isNotNull() & ~is_ambiguous,
                       F.concat(current10, F.substring(F.col("pnu"), 11, 9)))
    out = out.withColumn("_candidate", candidate)
    out = out.join(parcels.select(F.col("pnu").alias("_candidate")).distinct().withColumn("_held", F.lit(True)),
                   "_candidate", "left")
    held = F.coalesce(F.col("_held"), F.lit(False))
    return (out
            .withColumn("place_identity_grade",
                        F.when(~held & superseded & ~is_ambiguous, F.lit("code_superseded_parcel_absent"))
                        .when(held & superseded, F.lit("code_superseded"))
                        .when(held, F.lit("code_exact"))
                        .otherwise(F.lit("unresolved")))
            .withColumn("pnu_unresolved_reason",
                        F.when(held, F.lit(None).cast("string"))
                        .when(F.col("pnu").isNull(), F.lit("no_source_code"))
                        .when(is_ambiguous, F.lit("code_subdivision_ambiguous"))
                        .otherwise(F.lit("parcel_absent")))
            .withColumn("pnu", F.when(held, F.col("_candidate")))
            .drop("_code10", "current_code", "_amb", "_candidate", "_held"))
