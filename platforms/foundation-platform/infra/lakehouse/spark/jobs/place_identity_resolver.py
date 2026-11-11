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


def current_region(sido: str | None, sigungu: str | None, bjdong: str | None, remap: Mapping[str, str],
                   ambiguous: Iterable[str]) -> tuple[str | None, str | None, str | None]:
    """A source's (시도, 시군구, 법정동) codes carried to the current ones (root ADR-0426).

    Each level is read as its 10-digit code (시도 ``xx00000000``, 시군구 ``xxxxx00000``), because the
    change table records the 시도 and 시군구 pairs in that form (2026-07-01: 2900000000 → 1200000000,
    2917000000 → 1230000000). A code whose chain splits keeps no code at that level: the old one is not
    current and the new one is a steward's decision. The finest level that resolves names the levels
    above it (the codes nest), so a 시군구 that moved 시도 on its own pair (군위군 47720 → 27720) takes
    the 시도 with it.
    """

    split = set(ambiguous)

    def carried(code: str | None, digits: int) -> str | None:
        if not code or len(code) != digits:
            return None
        padded = code + "0" * (10 - digits)
        if padded in split:
            return None
        return remap.get(padded, padded)[:digits]

    sido_now, sigungu_now, bjdong_now = carried(sido, 2), carried(sigungu, 5), carried(bjdong, 10)
    if bjdong_now:
        sigungu_now = bjdong_now[:5]
    if sigungu_now:
        sido_now = sigungu_now[:2]
    return sido_now, sigungu_now, bjdong_now


def sido_names(names: Iterable[tuple[str, str, str]]) -> dict[str, str]:
    """{current 2-digit 시도 code: its name}, the first word of the full name of the newest 법정동
    snapshot's current rows under it, the 시도 row (``xx00000000``) first and otherwise the smallest
    code (세종 has no ``3600000000``: its rows start at 3611000000)."""

    out: dict[str, tuple[str, str]] = {}
    for code, full_name, status in names:
        words = (full_name or "").split()
        if status != "존재" or len(code or "") != 10 or not words:
            continue
        rank = ("0" if code[2:] == "00000000" else "1") + code
        if code[:2] not in out or rank < out[code[:2]][0]:
            out[code[:2]] = (rank, words[0])
    return {sido: name for sido, (_, name) in sorted(out.items())}


def with_current_code(pnu: str, current_legal_code: str) -> str:
    """A 19-digit pnu with its 10-digit legal-dong code replaced by the current one; the lot is kept."""

    return current_legal_code + pnu[10:]


def crosswalk_of(rows: Iterable[Mapping[str, Any]]) -> tuple[dict[str, str], set[str]]:
    """The code crosswalk of rows of `reference.legal_dong_code_change`: the pairs in force, through the
    one view every reader uses, so a pair official evidence superseded is not read (root ADR-0329)."""

    return code_crosswalk((pair["old_code"], pair["new_code"]) for pair in views.pair_rows(list(rows)))


def pairs_from_lakehouse(spark: Any, catalog: str) -> tuple[dict[str, str], set[str]]:
    """`crosswalk_of` the table as it is now."""

    table = f"`{catalog}`.`reference`.`legal_dong_code_change`"
    return crosswalk_of(row.asDict() for row in spark.sql(
        f"SELECT kind, old_code, new_code, level, effective_date, source FROM {table}").collect())


def code_lot_pnu(code: str | None, lot: str | None) -> str | None:
    """The 19-digit pnu a source's 10-digit legal-dong code and its lot text name (산 -> ledger 2, else
    1), or None when the code is not ten digits or the lot is prose ("외 6필지", a 블록), never a guess.
    The code is the source's: `resolve` carries it to the current one (root ADR-0382 §1)."""

    import address_pnu  # noqa: PLC0415 - the lot grammar is the address path's, not restated

    if not code or len(code) != 10 or not code.isdigit() or not lot:
        return None
    match = address_pnu.LOT.fullmatch(re.sub(r"\s+", "", lot))
    if not match:
        return None
    key = address_pnu.JibunKey("", "", bool(match.group(1)), int(match.group(2)), int(match.group(3) or 0))
    return address_pnu.jibun_pnu(code, key)


def carried_to_current(frame: Any, remap: Mapping[str, str], column: str = "pnu") -> Any:
    """`frame` with the legal-dong code of `column`'s pnu carried old -> current (``code_crosswalk``),
    its lot kept. Nothing is checked against the parcels: a reader that places rows does that itself
    (the parcel lineage, `servable`), so a pnu read once is carried again on every build after a
    code change (root ADR-0329 §3, ADR-0382 §2)."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    crosswalk = F.broadcast(frame.sparkSession.createDataFrame(sorted(remap.items()), "_old string, _current string"))
    out = frame.join(crosswalk, F.substring(F.col(column), 1, 10) == F.col("_old"), "left")
    return (out.withColumn(column, F.when(F.col("_current").isNotNull(),
                                          F.concat(F.col("_current"), F.substring(F.col(column), 11, 9)))
                           .otherwise(F.col(column)))
            .drop("_old", "_current"))


# The code change rows a Gold build reads and compares (`views.changed_codes`): the pairs, their
# supersessions and the evidence `crosswalk_of` weighs.
CODE_CHANGE_COLUMNS = ("kind", "old_code", "new_code", "level", "effective_date", "source")


def located(items: Any, remap: Mapping[str, str], raw_column: str) -> Any:
    """`items` with `pnu` the PNU each row is placed by on this build: the load's resolved PNU, else the
    source's own code PNU (`raw_column`, when it is 19 digits), carried to the current legal-dong code
    (`carried_to_current`). A Gold build calls it on every version it reads, old and new alike, so a
    link made at load time is never the one served after a code change (root ADR-0329 §3, ADR-0382)."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    raw = F.col(raw_column)
    return carried_to_current(
        items.withColumn("pnu", F.coalesce(F.col("pnu"), F.when(raw.rlike("^[0-9]{19}$"), raw))), remap)


def under_changed_codes(frames: Sequence[Any], before: Iterable[Mapping[str, Any]],
                        after: Iterable[Mapping[str, Any]], key: Sequence[str]) -> Any | None:
    """The `key`s of the rows of `frames` whose `pnu` lies under either code of a code change row that
    differs between `before` and `after` (`views.changed_codes`, root ADR-0273, ADR-0329 §3), or None
    when no row differs. One `isin` per prefix length, as the parcel panel does: a first build after
    the table was added sees every code as changed."""

    from functools import reduce  # noqa: PLC0415

    from pyspark.sql import functions as F  # noqa: PLC0415

    by_digits: dict[int, set[str]] = {}
    for code in views.changed_codes(list(before), list(after), CODE_CHANGE_COLUMNS):
        prefix = views.code_prefix(code)
        by_digits.setdefault(len(prefix), set()).add(prefix)
    if not by_digits or not frames:
        return None
    under = F.lit(False)
    for digits, prefixes in sorted(by_digits.items()):
        under = under | F.substring("pnu", 1, digits).isin(sorted(prefixes))
    return reduce(lambda a, b: a.unionByName(b), [frame.where(under).select(*key) for frame in frames])


def served_forms(remap: Mapping[str, str]) -> list[tuple[str, str, int]]:
    """(code, an earlier form of it, rank) for every code a recorded pair touches (root ADR-0482 §1).

    The served cadastral edition can predate a code change: on 2026-11-11 it was 202606 while the
    2026-07-01 전남광주·인천 pairs were in force, so a parcel the map serves still carried the old
    code (4611010100…) while the resolver asked only for the current one (1211010100…) and left
    6,020 경매 items and 4,122 공매 rounds unplaced. A code's earlier forms are the ones a
    reader may find the served parcel under once its current form is not served: rank 1 is the
    code itself when it is superseded (the source gave an old code), rank 2 every other code the
    pairs carry to the same current code (the source gave the new code, or one sibling of a merge).
    The current form, rank 0, is not listed: every reader tries it first.
    """

    earlier: dict[str, set[str]] = {}
    for old, current in remap.items():
        earlier.setdefault(current, set()).add(old)
    out: list[tuple[str, str, int]] = []
    for code in sorted(set(remap) | set(earlier)):
        current = remap.get(code, code)
        if current != code:
            out.append((code, code, 1))
        out.extend((code, old, 2) for old in sorted(earlier.get(current, ())) if old != code)
    return out


def candidate_pnus(pnus: Iterable[str | None], remap: Mapping[str, str], ambiguous: Iterable[str]) -> set[str]:
    """Every pnu ``resolve`` asks the parcel set about for these source codes: each code carried to its
    current legal-dong code, and its earlier forms (``served_forms``); a code that splits asks about
    nothing."""

    split = set(ambiguous)
    forms: dict[str, list[str]] = {}
    for code, form, _rank in served_forms(remap):
        forms.setdefault(code, []).append(form)
    asked: set[str] = set()
    for pnu in pnus:
        if not pnu or pnu[:10] in split:
            continue
        asked.add(with_current_code(pnu, remap.get(pnu[:10], pnu[:10])))
        asked.update(with_current_code(pnu, form) for form in forms.get(pnu[:10], ()))
    return asked


# The grade of a pnu found under an earlier form of its code because the served edition predates the
# change (root ADR-0482 §1). It is exact about the land: the pair says the two codes are one place.
SERVED_EDITION_GRADE = "code_served_edition"


def held_forms(frame: Any, column: str, remap: Mapping[str, str], parcels: Any) -> Any:
    """``frame`` with ``_served_form``: the served parcel ``column``'s pnu names under an earlier form
    of its legal-dong code (``served_forms``), the lowest rank first, and ``_served_ambiguous`` when
    that rank holds more than one (a merge whose two old dongs both had the lot). Null for a row
    whose code has no earlier form or none is served; the caller tries the current form first.
    Keyed by the pnu itself, so the result does not depend on how ``frame`` is partitioned."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    spark = frame.sparkSession
    forms = F.broadcast(spark.createDataFrame(served_forms(remap), "_code10 string, _form10 string, _rank int"))
    held = parcels.select(F.col("pnu").alias("_form")).distinct()
    asked = frame.select(F.col(column).alias("_asked")).where(F.col("_asked").isNotNull()).distinct()
    found = (asked.join(forms, F.substring("_asked", 1, 10) == F.col("_code10"))
             .withColumn("_form", F.concat(F.col("_form10"), F.substring("_asked", 11, 9)))
             .join(held, "_form", "left_semi")
             .groupBy("_asked", "_rank").agg(F.count("*").alias("_n"), F.min("_form").alias("_form"))
             .groupBy("_asked").agg(F.min(F.struct("_rank", "_n", "_form")).alias("_best"))
             .select("_asked", F.col("_best._form").alias("_served_form"),
                     (F.col("_best._n") > 1).alias("_served_ambiguous")))
    return (frame.join(found, F.col(column) == F.col("_asked"), "left").drop("_asked")
            .withColumn("_served_ambiguous", F.coalesce(F.col("_served_ambiguous"), F.lit(False)))
            .withColumn("_served_form", F.when(~F.col("_served_ambiguous"), F.col("_served_form"))))


def served_form_candidates(spark: Any, sources: Iterable[str], remap: Mapping[str, str], served: Any,
                           lineage: Any | None = None) -> Any:
    """The lineage-candidate rows (``successor_pnu, source_pnu, hop, path``) that place a Gold source
    pnu its current form is not served under on the served parcel an earlier form of its code names
    (``held_forms``; path ``SERVED_EDITION_GRADE``), for the sources ``lineage`` gives no candidate.
    A Gold build carries every pnu to the current code (``located``) and places it on the served
    edition (root ADR-0291 §5); this is the step between the two when the edition is older than the
    pair (root ADR-0482 §1). The rows are few: the sources whose code a pair touches."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    touched = {code for code, _form, _rank in served_forms(remap)}
    asked = sorted({pnu for pnu in sources if pnu and pnu[:10] in touched})
    schema = "successor_pnu string, source_pnu string, hop int, path string"
    if not asked:
        return F.broadcast(spark.createDataFrame([], schema))
    frame = spark.createDataFrame([(pnu,) for pnu in asked], "source_pnu string")
    if lineage is not None:
        frame = frame.join(lineage.select("source_pnu").distinct(), "source_pnu", "left_anti")
    current = served.select(F.col("pnu").alias("source_pnu")).distinct()
    frame = frame.join(current, "source_pnu", "left_anti")
    found = held_forms(frame, "source_pnu", remap, served).where(F.col("_served_form").isNotNull())
    return F.broadcast(found.select(F.col("_served_form").alias("successor_pnu"), "source_pnu",
                                    F.lit(0).alias("hop"), F.lit(SERVED_EDITION_GRADE).alias("path")).cache())


def served_parcels_among(spark: Any, catalog: str, pnus: Iterable[str]) -> Any:
    """The served parcels among ``pnus``, read once and cached: the served edition's rows of
    `silver.parcel_boundaries` (root ADR-0291 §5) are semi-joined to the asked PNUs before anything is
    de-duplicated (root ADR-0390: the whole set's ~40M distinct PNUs, read per 공매 object, outgrew the
    small Spark's 2.5GiB 67 times 2026-10-28..11-03 and took p50 482 s an object)."""

    from pyspark.sql import functions as F  # noqa: PLC0415
    from vworld_parcel_editions import served_rows  # noqa: PLC0415

    asked = spark.createDataFrame([(pnu,) for pnu in sorted(set(pnus))], "pnu string")
    every = served_rows(spark.table(f"`{catalog}`.`silver`.`parcel_boundaries`")).select("pnu")
    return every.join(F.broadcast(asked), "pnu", "left_semi").distinct().cache()


def resolve(staged: Any, remap: Mapping[str, str], ambiguous: Iterable[str], parcels: Any,
            keep_candidate: bool = False) -> Any:
    """Replace ``staged.pnu`` (a format-valid source code, or null) with the CURRENT pnu and add
    ``place_identity_grade`` and ``pnu_unresolved_reason``.

    ``remap`` is old → current legal-dong code (``code_crosswalk``); ``ambiguous`` the old codes that
    split; ``parcels`` holds the served parcels' ``pnu`` (``served_parcels_among``). A pnu is
    kept only when its current form is a current parcel. ``keep_candidate`` also keeps that current
    form as ``pnu_candidate`` when it is not one, so a reader can follow the parcel lineage from it
    (root ADR-0350 §1: a deal on a parcel split or merged since).
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
    # The current form not served: the served edition may hold the parcel under an earlier form of
    # the code (root ADR-0482 §1). Asked by the source pnu, whose code `served_forms` is keyed by.
    out = held_forms(out.withColumn("_source", F.when(~held & ~is_ambiguous, F.col("pnu"))), "_source", remap, parcels)
    served = ~held & F.col("_served_form").isNotNull()
    placed = held | served
    return (out
            .withColumn("place_identity_grade",
                        F.when(served, F.lit(SERVED_EDITION_GRADE))
                        .when(~held & superseded & ~is_ambiguous, F.lit("code_superseded_parcel_absent"))
                        .when(held & superseded, F.lit("code_superseded"))
                        .when(held, F.lit("code_exact"))
                        .otherwise(F.lit("unresolved")))
            .withColumn("pnu_unresolved_reason",
                        F.when(placed, F.lit(None).cast("string"))
                        .when(F.col("pnu").isNull(), F.lit("no_source_code"))
                        .when(is_ambiguous | F.col("_served_ambiguous"), F.lit("code_subdivision_ambiguous"))
                        .otherwise(F.lit("parcel_absent")))
            .withColumn("pnu", F.when(held, F.col("_candidate")).when(served, F.col("_served_form")))
            .transform(lambda frame: frame.withColumn("pnu_candidate", F.when(~placed, F.col("_candidate")))
                       if keep_candidate else frame)
            .drop("_code10", "current_code", "_amb", "_candidate", "_held", "_source", "_served_form",
                  "_served_ambiguous"))


# --- masked lots: inference by evidence (root ADR-0351) -------------------------------------------
#
# The 실거래가 provider masks the lot of most 공장창고·상업업무·토지·단독다가구 deals (`3**`: 본번 300–399).
# The parcel is inferred only from evidence a contract names, level by level, strongest first, and only
# when exactly one current parcel of the deal's CURRENT 법정동 fits; several or none leave the pnu null
# with the reason. A level whose precision, measured against ground truth, is under the contract's bar
# is not used (`enabled_levels`). The grade says which evidence linked it, so a reader labels it 추정.
#
# One level links a deal to a GROUP, not a parcel (root ADR-0405): `inferred_same_main_lot_group`. Its
# evidence must still leave exactly one parcel, but what it claims is "one of the current parcels of
# that parcel's 법정동, 산 flag and 본번" (300, 300-1, 300-2 …: one plant over two lots is its usual
# miss), so the deal carries `pnu_group` and no `pnu`, and every parcel of the group shows it.

MASKED_LOT = re.compile(r"^(산\s*)?(\d+)(\*+)$")
MAIN_LOT_GROUP = "inferred_same_main_lot_group"
INFERRED_GRADES = ("inferred_ownership_change", "inferred_attributes", MAIN_LOT_GROUP)
# The 본번 group level linking a dataset whose measurement is under the bar, only while the owner's one
# switch (`place_inference.below_bar.shown`, root ADR-0483 §4) shows it: the screen says its label
# (`below_bar.label_ko`, 추정 · 정확도 약 <the newest measured %>, below_bar_label) instead of 추정 · 같은 본번.
MAIN_LOT_GROUP_BELOW_BAR = "inferred_same_main_lot_group_below_bar"
GROUP_GRADES = (MAIN_LOT_GROUP, MAIN_LOT_GROUP_BELOW_BAR)
# A PNU's 법정동 (10), 산 flag (1) and 본번 (4): the key of its 본번 필지 묶음.
MAIN_LOT_KEY_LENGTH = 15
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
    reaches the contract's bar on at least its minimum sample, judged on the lower end of its 95%
    Wilson interval (root ADR-0370: a point estimate a hair over the bar on a small sample is not
    a measurement over it). A level never measured for a dataset is not used."""

    bar = float(inference["precision_bar"])
    minimum = int(inference["min_measured_rows"])
    levels = []
    for level in inference["levels"]:
        unknown = set(level["evidence"]) - EVIDENCE
        if unknown or level["grade"] not in INFERRED_GRADES:
            raise ValueError(f"place_inference level {level['grade']} names {sorted(unknown) or 'an unknown grade'}")
        measured = (level.get("measured") or {}).get(dataset)
        if measured and float(measured["precision_wilson_low"]) >= bar and int(measured["rows"]) >= minimum:
            levels.append(level)
    shown = below_bar_level(inference, dataset)
    if shown is not None and not any(level["grade"] == MAIN_LOT_GROUP for level in levels):
        levels.append(shown)
    return levels


def below_bar_level(inference: Mapping[str, Any], dataset: str) -> dict[str, Any] | None:
    """The 본번 group level for a dataset measured under the bar, as `MAIN_LOT_GROUP_BELOW_BAR`, when the
    owner's switch shows it (root ADR-0483 §4); None while it does not, or the dataset has no such
    measurement on at least the minimum sample at or above the switch's `floor`.

    The switch is one value, `place_inference.below_bar.shown`: false is "not shown" (the deals stay
    unlinked, `jibun_masked`), true is "shown as `label_ko`". Nothing else changes between the two."""

    below = inference.get("below_bar") or {}
    measured = (below.get("measured") or {}).get(dataset)
    if not below.get("shown") or not measured:
        return None
    if float(measured["precision_wilson_low"]) < float(below["floor"]) or int(measured["rows"]) < int(inference["min_measured_rows"]):
        return None
    level = next((level for level in inference["levels"] if level["grade"] == MAIN_LOT_GROUP), None)
    if level is None:
        raise ValueError(f"place_inference.below_bar shows {MAIN_LOT_GROUP}, which no level names")
    return {**level, "grade": MAIN_LOT_GROUP_BELOW_BAR}


def latest_below_bar_measurement(inference: Mapping[str, Any], dataset: str) -> Mapping[str, Any]:
    """The newest of a dataset's below-bar measurements: the gating one and every fresh holdout after it,
    by `measured_on` (a later entry wins a tie)."""

    measured = inference["below_bar"]["measured"][dataset]
    every = [measured, *measured.get("holdouts", [])]
    return max(enumerate(every), key=lambda pair: (pair[1]["measured_on"], pair[0]))[1]


def below_bar_label(inference: Mapping[str, Any], dataset: str) -> str:
    """What the screen says for a below-bar link (root ADR-0483 §6): `label_ko_template` with the newest
    measurement's same-parcel precision as a whole percent, so a new measurement changes the words with
    it. The contract's `label_ko` and the web's message must equal this (the tests compare them)."""

    below = inference["below_bar"]
    precision = float(latest_below_bar_measurement(inference, dataset)["precision"])
    return below["label_ko_template"].format(percent=round(precision * 100))


def unique_pick(candidates: Iterable[str]) -> tuple[str | None, str | None, int]:
    """(pnu, reason, count): the one candidate, or why there is none to link."""

    found = sorted(set(candidates))
    if len(found) == 1:
        return found[0], None, 1
    return None, ("inference_ambiguous" if found else "inference_no_candidate"), len(found)


def infer_masked(deals: Any, parcels: Any, titles: Any, ownership: Any | None, levels: Sequence[Mapping[str, Any]],
                 tolerance_m2: float, ownership_window_days: tuple[int, int], ownership_causes: Sequence[str],
                 group_parcels: Any | None = None, remap: Mapping[str, str] | None = None,
                 zone_subdivisions: Mapping[str, Sequence[str]] | None = None) -> Any:
    """The inferred pnu of each masked deal, by the first level that leaves exactly one candidate.

    ``deals``: `row_id`, `legal_dong_code` (current), `lot_mountain`, `lot_low`, `lot_high`, `zone`,
    `jimok`, `land_area_m2`, `building_year`, `floor_area_m2`, `deal_date`.
    ``parcels``: the current parcels' `pnu`, `land_category`, `area_m2`, `zone_name_1`.
    ``titles``: the current main buildings' `pnu`, `approval_year`, `floor_area_m2`.
    ``ownership``: the current owners' `pnu`, `ownership_changed_on` (date), `ownership_change_reason`;
    None while that Silver is absent (the level that needs it then links nothing).
    ``group_parcels``: the current parcels' `pnu` a 본번 group is drawn from (default ``parcels``).
    ``remap``: old → current legal-dong code (``code_crosswalk``). A deal names its CURRENT code, and
    the parcels, their registers and owners may carry an earlier one (the served edition, the land and
    ownership files predate a code change); a parcel is a candidate when its code carried to the
    current one is the deal's, and it stays the pnu its own edition names (root ADR-0482 §1). On
    2026-11-11 the 2026-07-01 전남광주 pairs left 1,547,762 masked 토지 deals (all of 12…) with no
    parcel of their 법정동: the land file and the served edition still name 46…/29….
    ``zone_subdivisions``: a deal's 용도지역 → the ones it was split into since (the contract's
    `place_inference.zone_subdivisions`); a parcel zoned as any of them matches.
    Returns `row_id`, `pnu`, `pnu_group` (the sorted current parcels of the 본번 group, only for
    `MAIN_LOT_GROUP`; `pnu` is then null), `place_identity_grade`, `pnu_unresolved_reason`,
    `pnu_candidate_count`.
    """

    from pyspark.sql import Window, functions as F  # noqa: PLC0415

    spark = deals.sparkSession
    crosswalk = F.broadcast(spark.createDataFrame(sorted((remap or {}).items()), "_old string, _current string"))
    carried = (parcels.withColumn("_old", F.substring("pnu", 1, 10)).join(crosswalk, "_old", "left")
               .withColumn("_dong", F.coalesce("_current", "_old")).drop("_old", "_current"))
    candidates = (deals.alias("d").join(
        carried.alias("p"),
        (F.col("p._dong") == F.col("d.legal_dong_code"))
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
    # "<deal zone>|<parcel zone>" for every zone the deal's has been split into since.
    split_zones = sorted(f"{zone}|{part}" for zone, parts in (zone_subdivisions or {}).items() for part in parts) or [""]
    checks = {
        "lot_range": F.lit(True),
        "zone": F.col("zone").isNull() | (F.col("zone_name_1") == F.col("zone"))
        | F.concat_ws("|", "zone", "zone_name_1").isin(*split_zones),
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
            # size(null) is -1 in Spark: a register that left no candidate counts 0.
            sizes = [F.when(F.col(c).isNull(), F.lit(0)).otherwise(F.size(c)) for c in ("_land", "_build")]
            agreed = (sizes[0] == 1) & (sizes[1] == 1) & (F.col("_land") == F.col("_build"))
            # Two registers naming two parcels are two candidates: ambiguous, not "none".
            disagree = (sizes[0] == 1) & (sizes[1] == 1) & ~agreed
            per_row = (per_row.join(land, "row_id", "left").join(build, "row_id", "left")
                       .withColumn("_count", F.when(agreed, F.size("_set"))
                                   .otherwise(F.greatest(sizes[0], sizes[1], F.when(disagree, F.lit(2)).otherwise(F.lit(0)))))
                       .withColumn("_set", F.when(agreed, F.col("_set")).otherwise(F.array()))
                       .drop("_land", "_build"))
        else:
            per_row = per_row.withColumn("_count", F.size("_set"))
        outcomes.append(per_row.select("row_id", F.lit(rank).alias("_rank"), F.lit(level["grade"]).alias("_grade"),
                                       F.col("_count"), F.element_at("_set", 1).alias("_pnu")))
    every = deals.select("row_id")
    if not outcomes:
        return every.select("row_id", F.lit(None).cast("string").alias("pnu"),
                            F.lit(None).cast("array<string>").alias("pnu_group"),
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
    # A group level's one parcel stands for its 본번 group: every current parcel with its key. The
    # parcels are the caller's share (a Gold build hands only the 법정동s its change reaches), and
    # the groups are not narrowed by `linked` so its plan is not read twice.
    grouped = F.col("_grade").isin(*GROUP_GRADES)
    linked = linked.withColumn("_key", F.when(grouped, F.substring("_pnu", 1, MAIN_LOT_KEY_LENGTH)))
    members = ((parcels if group_parcels is None else group_parcels).select("pnu").distinct()
               .withColumn("_key", F.substring("pnu", 1, MAIN_LOT_KEY_LENGTH))
               .groupBy("_key").agg(F.array_sort(F.collect_set("pnu")).alias("_members")))
    linked = (linked.join(members, "_key", "left")
              # The pick itself is in its group even if `group_parcels` does not hold it.
              .withColumn("_group", F.when(grouped, F.array_sort(F.array_union(
                  F.coalesce("_members", F.array().cast("array<string>")), F.array("_pnu")))))
              .withColumn("_pnu", F.when(~grouped, F.col("_pnu")))
              .drop("_key", "_members"))
    return (every.join(linked, "row_id", "left").join(last, "row_id", "left")
            .select("row_id", F.col("_pnu").alias("pnu"), F.col("_group").alias("pnu_group"),
                    F.coalesce(F.col("_grade"), F.lit("unresolved")).alias("place_identity_grade"),
                    F.when(F.col("_pnu").isNotNull() | F.col("_group").isNotNull(), F.lit(None).cast("string"))
                    .when(F.coalesce(F.col("_last"), F.lit(0)) > 1, F.lit("inference_ambiguous"))
                    .otherwise(F.lit("inference_no_candidate")).alias("pnu_unresolved_reason"),
                    F.coalesce(F.col("_count"), F.col("_last"), F.lit(0)).cast("int").alias("pnu_candidate_count")))
