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

import re
from typing import Any, Iterable, Mapping, Sequence

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


# --- masked lots: inference by evidence (root ADR-0351) -------------------------------------------
#
# The 실거래가 provider masks the lot of most 공장창고·상업업무·토지·단독다가구 deals (`3**`: 본번 300–399).
# The parcel is inferred only from evidence a contract names, level by level, strongest first, and only
# when exactly one current parcel of the deal's CURRENT 법정동 fits; several or none leave the pnu null
# with the reason. A level whose precision, measured against ground truth, is under the contract's bar
# is not used (`enabled_levels`). The grade says which evidence linked it, so a reader labels it 추정.

MASKED_LOT = re.compile(r"^(산\s*)?(\d+)(\*+)$")
INFERRED_GRADES = ("inferred_ownership_change", "inferred_attributes")
# The evidence each level may name (the contract's `place_inference.levels[].evidence`).
EVIDENCE = frozenset({"lot_range", "zone", "jimok", "land_area", "building_year", "floor_area",
                      "registers_agree", "ownership_change"})


def masked_lot_range(jibun: Any) -> tuple[bool, int, int] | None:
    """(mountain, lowest 본번, highest 본번) a masked lot allows: `3**` is 300–399, `산1*` 10–19, the
    count of digits kept. None for a lot that is not masked or keeps no digit (`*`, `**`)."""

    found = MASKED_LOT.fullmatch(str(jibun or "").strip())
    if not found:
        return None
    digits, stars = found.group(2), len(found.group(3))
    low = int(digits) * 10 ** stars
    return bool(found.group(1)), low, low + 10 ** stars - 1


def zone_name(value: Any) -> str | None:
    """The provider's 용도지역 in the land register's words: `계획관리` → `계획관리지역`."""

    text = str(value or "").strip()
    if not text:
        return None
    return text if text.endswith(("지역", "구역")) else f"{text}지역"


def enabled_levels(inference: Mapping[str, Any], dataset: str) -> list[dict[str, Any]]:
    """The levels a dataset may link by, strongest first: those whose measured precision for it
    reaches the contract's bar on at least its minimum sample. A level never measured for a dataset
    is not used."""

    bar = float(inference["precision_bar"])
    minimum = int(inference["min_measured_rows"])
    levels = []
    for level in inference["levels"]:
        unknown = set(level["evidence"]) - EVIDENCE
        if unknown or level["grade"] not in INFERRED_GRADES:
            raise ValueError(f"place_inference level {level['grade']} names {sorted(unknown) or 'an unknown grade'}")
        measured = (level.get("measured") or {}).get(dataset)
        if measured and float(measured["precision"]) >= bar and int(measured["rows"]) >= minimum:
            levels.append(level)
    return levels


def unique_pick(candidates: Iterable[str]) -> tuple[str | None, str | None, int]:
    """(pnu, reason, count): the one candidate, or why there is none to link."""

    found = sorted(set(candidates))
    if len(found) == 1:
        return found[0], None, 1
    return None, ("inference_ambiguous" if found else "inference_no_candidate"), len(found)


def infer_masked(deals: Any, parcels: Any, titles: Any, ownership: Any | None, levels: Sequence[Mapping[str, Any]],
                 tolerance_m2: float, ownership_window_days: tuple[int, int], ownership_causes: Sequence[str]) -> Any:
    """The inferred pnu of each masked deal, by the first level that leaves exactly one candidate.

    ``deals``: `row_id`, `legal_dong_code` (current), `lot_mountain`, `lot_low`, `lot_high`, `zone`,
    `jimok`, `land_area_m2`, `building_year`, `floor_area_m2`, `deal_date`.
    ``parcels``: the current parcels' `pnu`, `land_category`, `area_m2`, `zone_name_1`.
    ``titles``: the current main buildings' `pnu`, `approval_year`, `floor_area_m2`.
    ``ownership``: the current owners' `pnu`, `ownership_changed_on` (date), `ownership_change_reason`;
    None while that Silver is absent (the level that needs it then links nothing).
    Returns `row_id`, `pnu`, `place_identity_grade`, `pnu_unresolved_reason`, `pnu_candidate_count`.
    """

    from pyspark.sql import Window, functions as F  # noqa: PLC0415

    candidates = (deals.alias("d").join(
        parcels.alias("p"),
        (F.substring("p.pnu", 1, 10) == F.col("d.legal_dong_code"))
        & (F.substring("p.pnu", 11, 1) == F.when(F.col("d.lot_mountain"), F.lit("2")).otherwise(F.lit("1")))
        & F.substring("p.pnu", 12, 4).cast("int").between(F.col("d.lot_low"), F.col("d.lot_high")))
        .select("d.*", F.col("p.pnu").alias("candidate"), "p.land_category", "p.area_m2", "p.zone_name_1"))
    buildings = titles.groupBy("pnu").agg(F.collect_set("approval_year").alias("_years"),
                                          F.collect_list("floor_area_m2").alias("_floors"),
                                          F.sum("floor_area_m2").alias("_floor_sum"))
    candidates = candidates.join(buildings.withColumnRenamed("pnu", "candidate"), "candidate", "left")
    if ownership is not None:
        low, high = ownership_window_days
        sold = ownership.where(F.col("ownership_change_reason").isin(*ownership_causes)).select(
            F.col("pnu").alias("candidate"), F.col("ownership_changed_on").cast("date").alias("_changed"))
        in_window = (candidates.join(sold, "candidate")
                     .where(F.datediff("_changed", "deal_date").between(low, high))
                     .select("row_id", "candidate").distinct().withColumn("_owner_ok", F.lit(True)))
        candidates = candidates.join(in_window, ["row_id", "candidate"], "left").withColumn(
            "_owner_ok", F.coalesce("_owner_ok", F.lit(False)))
    else:
        candidates = candidates.withColumn("_owner_ok", F.lit(False))
    near = lambda a, b: F.abs(F.col(a) - F.col(b)) < F.lit(tolerance_m2)  # noqa: E731
    checks = {
        "lot_range": F.lit(True),
        "zone": F.col("zone").isNull() | (F.col("zone_name_1") == F.col("zone")),
        "jimok": F.col("jimok").isNull() | (F.col("land_category") == F.col("jimok")),
        "land_area": F.col("land_area_m2").isNotNull() & near("area_m2", "land_area_m2"),
        "building_year": F.col("building_year").isNotNull() & F.array_contains(F.coalesce("_years", F.array()), F.col("building_year")),
        "floor_area": F.col("floor_area_m2").isNotNull() & (
            near("_floor_sum", "floor_area_m2")
            | F.exists(F.coalesce("_floors", F.array()), lambda f: F.abs(f - F.col("floor_area_m2")) < F.lit(tolerance_m2))),
        "ownership_change": F.col("_owner_ok"),
    }
    outcomes = []
    for rank, level in enumerate(levels):
        evidence = [e for e in level["evidence"] if e != "registers_agree"]
        fits = F.lit(True)
        for name in evidence:
            fits = fits & checks[name]
        per_row = candidates.where(fits).groupBy("row_id").agg(
            F.collect_set("candidate").alias("_set"))
        if "registers_agree" in level["evidence"]:
            # Two registers kept by different offices must each leave one parcel, the same one: the
            # land register (area, 용도지역) and the building register (사용승인 연도, 연면적).
            land = candidates.where(checks["zone"] & checks["land_area"]).groupBy("row_id").agg(
                F.collect_set("candidate").alias("_land"))
            build = candidates.where(checks["zone"] & checks["building_year"] & checks["floor_area"]).groupBy("row_id").agg(
                F.collect_set("candidate").alias("_build"))
            per_row = per_row.join(land, "row_id", "left").join(build, "row_id", "left").withColumn(
                "_set", F.when((F.size("_land") == 1) & (F.size("_build") == 1) & (F.col("_land") == F.col("_build")),
                               F.col("_set")).otherwise(F.array())).drop("_land", "_build")
        outcomes.append(per_row.select("row_id", F.lit(rank).alias("_rank"), F.lit(level["grade"]).alias("_grade"),
                                       F.size("_set").alias("_count"), F.element_at("_set", 1).alias("_pnu")))
    every = deals.select("row_id")
    if not outcomes:
        return every.select("row_id", F.lit(None).cast("string").alias("pnu"),
                            F.lit("unresolved").alias("place_identity_grade"),
                            F.lit("jibun_masked").alias("pnu_unresolved_reason"),
                            F.lit(None).cast("int").alias("pnu_candidate_count"))
    tried = outcomes[0]
    for frame in outcomes[1:]:
        tried = tried.unionByName(frame)
    linked = (tried.where(F.col("_count") == 1)
              .withColumn("_n", F.row_number().over(Window.partitionBy("row_id").orderBy("_rank")))
              .where(F.col("_n") == 1).drop("_n"))
    # Unlinked: the candidate count of the weakest level tried says ambiguous or none.
    last = tried.where(F.col("_rank") == len(levels) - 1).select("row_id", F.col("_count").alias("_last"))
    return (every.join(linked, "row_id", "left").join(last, "row_id", "left")
            .select("row_id", F.col("_pnu").alias("pnu"),
                    F.coalesce(F.col("_grade"), F.lit("unresolved")).alias("place_identity_grade"),
                    F.when(F.col("_pnu").isNotNull(), F.lit(None).cast("string"))
                    .when(F.coalesce(F.col("_last"), F.lit(0)) > 1, F.lit("inference_ambiguous"))
                    .otherwise(F.lit("inference_no_candidate")).alias("pnu_unresolved_reason"),
                    F.coalesce(F.col("_count"), F.col("_last"), F.lit(0)).cast("int").alias("pnu_candidate_count")))
