"""PNU from a Korean address, for sources that give an address but no PNU (root ADR-0280 §4).

Two steps, each naming itself, never geometry:

- `jibun_address`: the 시도 word's code (`sido_prefixes`, derived from the 법정동 code table and the recorded
  code pairs, so an old name such as 강원도 or 전라남도 reads as the current code; root ADR-0403), the remaining place names up to the lot matched against the
  current 법정동 names of `reference.legal_dong_code_snapshot`, then 산 and 본번-부번. The PNU counts only
  when `silver.parcel_boundaries` holds it as a current row.
- `road_address`: (시도 code, 시군구, 도로명, 건물본번, 건물부번) read the same way from the source and from
  `silver.building_register_titles.road_address_raw`; resolved only when every matching title carries
  the same PNU. 읍·면·동·리 words before the road are dropped: the register does not write them there.

The parsing is plain Python so it is tested without Spark; `resolve` does the two joins.
"""

from __future__ import annotations

import re
from typing import Any, Iterable, Mapping, NamedTuple

LOT = re.compile(r"^(산)?(\d+)(?:-(\d+))?(?:번지)?$")
ROAD = re.compile(r"^\S+(?:로|길)$")
BUILDING_NUMBER = re.compile(r"^(\d+)(?:-(\d+))?$")
PLACE_SUFFIX = re.compile(r"(?:읍|면|동|리|가)$")
# The register's road address: 시도, 시군구 words, 도로명, 건물번호, then anything ("(외남면 흔평리)").
REGISTER_ROAD = r"^\S+\s+(.+?)\s+(\S+(?:로|길))\s+(\d+)(?:-(\d+))?"


class JibunKey(NamedTuple):
    sido_code: str
    place: str
    mountain: bool
    bonbun: int
    bubun: int


class RoadKey(NamedTuple):
    sido_code: str
    sigungu: str
    road: str
    main_no: int
    sub_no: int


# The suffixes a 시도 name ends with; what is left is the name an address may write alone (경기, 강원).
SIDO_SUFFIXES = ("특별자치시", "특별자치도", "특별시", "광역시", "직할시", "도")


def sido_aliases(name: str) -> set[str]:
    """The words an address may open with for a 시도 name: the name, the name without its suffix, and
    for a three-syllable stem the first and third syllables (충청북도 -> 충청북, 충북). A rule on the
    name, not a table of codes (root ADR-0103 §2, ADR-0403)."""

    aliases = {name}
    stem = next((name[: -len(suffix)] for suffix in SIDO_SUFFIXES if name.endswith(suffix)), "")
    if len(stem) >= 2:
        aliases.add(stem)
    if len(stem) == 3:
        aliases.add(stem[0] + stem[2])
    return aliases


def current_successors(sido: str, current: set[str], moves: Mapping[str, set[str]]) -> set[str]:
    """The current 시도 codes ``sido`` is, itself or through the recorded moves from it."""

    seen, pending, found = set(), [sido], set()
    while pending:
        code = pending.pop()
        if code in seen:
            continue
        seen.add(code)
        if code in current:
            found.add(code)
        else:
            pending.extend(moves.get(code, ()))
    return found


def sido_prefixes(names: Iterable[tuple[str, str, str]], pairs: Iterable[tuple[str, str]]) -> dict[str, str]:
    """{address word prefix: current 2-digit 시도 code}, from the 법정동 code table and its recorded pairs.

    ``names`` are (10-digit code, full name, status) of every row of the newest snapshot, abolished
    ones too: an address keeps the name a 시도 had (전라남도). ``pairs`` are the (old, new) 시도 codes
    the recorded code changes move between (``legal_dong_code_change_views.region_pairs(rows, 2)``,
    pairs a supersession withdrew left out). A 시도 with current codes is itself. An abolished one
    is where its recorded pairs lead (29, 46 -> 12 on 2026-07-01). One the record does not carry
    (the renumberings before it began, 42 -> 51) is the one current 시도 whose name stem it shares.
    A prefix that would name two current 시도 is left out: guessing would be a rule this has not.
    """

    rows = list(names)
    current = {code[:2] for code, _, status in rows if status == "존재" and len(code or "") == 10}
    sido_names: dict[str, set[str]] = {}
    for code, full_name, _ in rows:
        words = (full_name or "").split()
        if words and len(code or "") == 10:
            sido_names.setdefault(code[:2], set()).add(words[0])
    moves: dict[str, set[str]] = {}
    for old, new in pairs:
        moves.setdefault(old, set()).add(new)

    current_stems = {sido: set().union(*(sido_aliases(n) for n in sido_names.get(sido, ()))) for sido in current}
    claims: dict[str, set[str]] = {}
    for sido, sido_words in sido_names.items():
        targets = current_successors(sido, current, moves)
        for name in sido_words:
            aliases = sido_aliases(name)
            if not targets:
                targets_by_name = {c for c, stems in current_stems.items() if (aliases - {name}) & stems}
                found = targets_by_name
            else:
                found = targets
            for alias in aliases:
                claims.setdefault(alias, set()).update(found)
    return {alias: next(iter(codes)) for alias, codes in sorted(claims.items()) if len(codes) == 1}


def sido_code(word: str, prefixes: Mapping[str, str]) -> str | None:
    """The 2-digit 시도 code a first address word starts with; the longest prefix wins."""

    for prefix in sorted((p for p in prefixes if not p.startswith("$")), key=len, reverse=True):
        if word.startswith(prefix):
            return prefixes[prefix]
    return None


def address_words(address: str) -> list[str]:
    """The words before any parenthesis, comma or 외 (외 1필지), with 산 split from a glued lot."""

    head = re.split(r"[(,]|\s외\s|외\d", address or "", maxsplit=1)[0]
    head = re.sub(r"(?<=\s)산(?=\d)", "산 ", head)
    # 천안시서북구 -> 천안시 서북구
    head = re.sub(r"(?<=\S시)(?=\S+구(?:\s|$))", " ", head)
    return head.split()


def without_gu(place: str) -> str:
    """The place names without a 구 word after a 시 word: an address may omit a 구 its city has
    (a city that gained 구 keeps its old addresses), and the code table always writes it."""

    words = place.split()
    return " ".join(w for i, w in enumerate(words) if not (i > 0 and words[i - 1].endswith("시") and w.endswith("구")))


def jibun_key(address: str, prefixes: Mapping[str, str]) -> JibunKey | None:
    """The jibun reading of an address, or None when it does not read as one."""

    words = address_words(address)
    if len(words) < 3:
        return None
    code = sido_code(words[0], prefixes)
    if code is None:
        return None
    for index in range(2, len(words)):
        mountain = words[index] == "산"
        lot_word = words[index + 1] if mountain and index + 1 < len(words) else words[index]
        match = LOT.fullmatch(lot_word)
        if not match:
            continue
        places = words[1:index]
        if not places or not PLACE_SUFFIX.search(places[-1]):
            return None  # a number after a road name is a building number, not a lot
        return JibunKey(code, " ".join(places), mountain or bool(match.group(1)), int(match.group(2)),
                        int(match.group(3) or 0))
    return None


def road_key(address: str, prefixes: Mapping[str, str]) -> RoadKey | None:
    """The road reading of an address, or None when it does not read as one."""

    words = address_words(address)
    if len(words) < 4:
        return None
    code = sido_code(words[0], prefixes)
    if code is None:
        return None
    for index in range(2, len(words) - 1):
        number = BUILDING_NUMBER.fullmatch(words[index + 1])
        if ROAD.fullmatch(words[index]) and number:
            sigungu = [word for word in words[1:index] if word.endswith(("시", "군", "구"))]
            if not sigungu:
                return None
            return RoadKey(code, " ".join(sigungu), words[index], int(number.group(1)), int(number.group(2) or 0))
    return None


def legal_dong_index(rows: Iterable[tuple[str, str]]) -> dict[tuple[str, str], str]:
    """(시도 code, the names after the 시도) -> 10-digit 법정동 code, from current (code, full name) rows.

    A name two current codes share is left out: guessing between them would be a rule this has not.
    """

    exact: dict[tuple[str, str], set[str]] = {}
    for code, full_name in rows:
        words = (full_name or "").split()
        if len(words) < 2 or not re.fullmatch(r"\d{10}", code or ""):
            continue
        exact.setdefault((code[:2], " ".join(words[1:])), set()).add(code)
    index = {key: next(iter(codes)) for key, codes in exact.items() if len(codes) == 1}
    # The same names without the 구, where that still names one code and no exact name has it.
    loose: dict[tuple[str, str], set[str]] = {}
    for (sido, place), codes in exact.items():
        short = without_gu(place)
        if short != place:
            loose.setdefault((sido, short), set()).update(codes)
    index.update({key: next(iter(codes)) for key, codes in loose.items() if len(codes) == 1 and key not in exact})
    return index


def jibun_pnu(code: str, key: JibunKey) -> str | None:
    if key.bonbun > 9999 or key.bubun > 9999:
        return None
    return f"{code}{'2' if key.mountain else '1'}{key.bonbun:04d}{key.bubun:04d}"


def candidates(address: str, prefixes: Mapping[str, str], dongs: Mapping[tuple[str, str], str]) -> dict[str, Any]:
    """What one address offers the joins: a jibun PNU candidate, a road key, or why neither."""

    if not (address or "").strip():
        return {"jibun_pnu": None, "legal_dong_code": None, "road": None, "reason": "no_address"}
    jibun = jibun_key(address, prefixes)
    if jibun is not None:
        code = dongs.get((jibun.sido_code, jibun.place))
        if code is None:
            return {"jibun_pnu": None, "legal_dong_code": None, "road": None, "reason": "no_legal_dong"}
        return {"jibun_pnu": jibun_pnu(code, jibun), "legal_dong_code": code, "road": None, "reason": None}
    road = road_key(address, prefixes)
    if road is not None:
        return {"jibun_pnu": None, "legal_dong_code": None, "road": road, "reason": None}
    return {"jibun_pnu": None, "legal_dong_code": None, "road": None, "reason": "unparsed"}


def current_legal_dongs(spark: Any, catalog: str) -> list[tuple[str, str]]:
    """(code, full name) of the newest snapshot's current 법정동 codes."""

    from legal_dong_code_snapshot_to_reference import newest_snapshot  # noqa: PLC0415

    rows = (newest_snapshot(spark, f"`{catalog}`.`reference`.`legal_dong_code_snapshot`")
            .where("status = '존재'").select("region_cd", "full_name").collect())
    return [(row["region_cd"], row["full_name"]) for row in rows]


def lakehouse_sido_prefixes(spark: Any, catalog: str) -> dict[str, str]:
    """``sido_prefixes`` from the newest 법정동 snapshot (every status) and the recorded pairs in force."""

    from legal_dong_code_change_views import CHANGE_TABLE, region_pairs  # noqa: PLC0415

    table = f"`{catalog}`.`reference`.`legal_dong_code_snapshot`"
    newest = spark.sql(f"SELECT max(snapshot_date) AS d FROM {table}").collect()[0]["d"]
    names = [(row["region_cd"], row["full_name"], row["status"]) for row in spark.sql(
        f"SELECT region_cd, full_name, status FROM {table} WHERE snapshot_date = DATE '{newest}'").collect()]
    schema, name = CHANGE_TABLE.split(".")
    changes = [row.asDict() for row in spark.sql(
        f"SELECT kind, old_code, new_code, level, effective_date, source FROM `{catalog}`.`{schema}`.`{name}`").collect()]
    return sido_prefixes(names, region_pairs(changes, 2))


def lakehouse_inputs(spark: Any, catalog: str) -> tuple[Any, Any]:
    """The served parcels' PNUs and the building register titles, as ``resolve`` reads them.

    A PNU resolves only to a parcel of the served edition (root ADR-0291 §5): every edition's rows
    are current, and a parcel only a newer edition holds is one the map does not serve."""

    from platform_contracts import current_rows, load_lakehouse_contract  # noqa: PLC0415
    from vworld_parcel_editions import served_rows  # noqa: PLC0415

    parcels = served_rows(spark.table(f"`{catalog}`.`silver`.`parcel_boundaries`")).select("pnu").distinct()
    # The register appends only each release's changed buildings (root ADR-0318): its current rows.
    titles = current_rows(spark.table(f"`{catalog}`.`silver`.`building_register_titles`"),
                          load_lakehouse_contract("silver.building_register_titles")).select("pnu", "road_address_raw")
    return parcels, titles


def resolve(frame: Any, parcels: Any, titles: Any) -> Any:
    """Adds pnu, pnu_basis, pnu_unresolved_reason and pnu_candidate_count to ``frame``.

    ``frame`` carries ``row_id`` and the output of ``candidates`` flattened: ``jibun_pnu``,
    ``road_sido``, ``road_sigungu``, ``road_name``, ``road_main_no``, ``road_sub_no``, ``reason``.
    ``parcels`` holds the current parcels' ``pnu``; ``titles`` the register's ``pnu`` and
    ``road_address_raw`` (``lakehouse_inputs``).
    """

    from pyspark.sql import functions as F  # noqa: PLC0415

    jibun = (frame.where(F.col("jibun_pnu").isNotNull()).select("row_id", "jibun_pnu")
             .join(parcels.withColumnRenamed("pnu", "jibun_pnu").withColumn("_held", F.lit(True)), "jibun_pnu", "left"))
    titles = (titles
              .where(F.col("pnu").isNotNull() & (F.col("road_address_raw") != ""))
              .select(F.substring("pnu", 1, 2).alias("road_sido"),
                      F.regexp_extract("road_address_raw", REGISTER_ROAD, 1).alias("road_sigungu"),
                      F.regexp_extract("road_address_raw", REGISTER_ROAD, 2).alias("road_name"),
                      F.regexp_extract("road_address_raw", REGISTER_ROAD, 3).cast("int").alias("road_main_no"),
                      F.coalesce(F.regexp_extract("road_address_raw", REGISTER_ROAD, 4).cast("int"), F.lit(0)).alias("road_sub_no"),
                      "pnu")
              .groupBy("road_sido", "road_sigungu", "road_name", "road_main_no", "road_sub_no")
              .agg(F.countDistinct("pnu").alias("_pnus"), F.first("pnu").alias("_road_pnu")))
    keys = ["road_sido", "road_sigungu", "road_name", "road_main_no", "road_sub_no"]
    road = frame.where(F.col("road_name").isNotNull()).select("row_id", *keys).join(titles, keys, "left")
    out = (frame.join(jibun.select("row_id", "_held"), "row_id", "left")
           .join(road.select("row_id", "_pnus", "_road_pnu"), "row_id", "left"))
    jibun_ok = F.col("jibun_pnu").isNotNull() & F.col("_held")
    road_ok = F.col("_pnus") == 1
    return (out
            .withColumn("pnu", F.when(jibun_ok, F.col("jibun_pnu")).when(road_ok, F.col("_road_pnu")))
            .withColumn("pnu_basis", F.when(jibun_ok, F.lit("jibun_address")).when(road_ok, F.lit("road_address")))
            .withColumn("pnu_candidate_count", F.when(jibun_ok, F.lit(1))
                        .when(F.col("road_name").isNotNull(), F.coalesce(F.col("_pnus"), F.lit(0)).cast("int")))
            .withColumn("pnu_unresolved_reason",
                        F.when(jibun_ok | road_ok, F.lit(None).cast("string"))
                        .when(F.col("reason").isNotNull(), F.col("reason"))
                        .when(F.col("jibun_pnu").isNotNull(), F.lit("parcel_absent"))
                        .when(F.col("_pnus") > 1, F.lit("road_ambiguous"))
                        .otherwise(F.lit("road_not_found")))
            .drop("_held", "_pnus", "_road_pnu"))
