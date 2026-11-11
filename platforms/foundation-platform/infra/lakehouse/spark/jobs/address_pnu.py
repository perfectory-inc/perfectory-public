"""PNU from a Korean address, for sources that give an address but no PNU (root ADR-0280 §4).

Three steps, each naming itself, never geometry:

- `jibun_address`: the 시도 word's code (`sido_prefixes`, derived from the 법정동 code table and the recorded
  code pairs, so an old name such as 강원도 or 전라남도 reads as the current code; root ADR-0403), the remaining place names up to the lot matched against the
  법정동 names of `reference.legal_dong_code_snapshot` (an abolished name through its recorded code
  pairs, root ADR-0329), then 산 and 본번-부번. The PNU counts only
  when `silver.parcel_boundaries` holds it as a current row.
- `juso_road_address`: (시도, 시군구 codes, 도로명, 건물본번, 건물부번) against the 주소DB (`silver.juso_road_address`,
  root ADR-0495); resolved only when the matching addresses' representative 지번 is one PNU the served
  edition holds.
- `road_address`: (시도 code, 시군구, 도로명, 건물본번, 건물부번) read the same way from the source and from
  `silver.building_register_titles.road_address_raw`; resolved only when every matching title carries
  the same PNU. 읍·면·동·리 words before the road are dropped: the register does not write them there.

The parsing is plain Python so it is tested without Spark; `resolve` does the joins, with the
주소DB road ledger (`juso_road_address`, root ADR-0495) between the jibun and the register.
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
    """The words before any parenthesis, comma or 외 (외 1필지, 999외 9필지), with 산 split from a glued lot."""

    head = re.split(r"[(,]|\s외\s|(?<=\d)외|외(?=\d)", address or "", maxsplit=1)[0]
    head = re.sub(r"(?<=\s)산(?=\d)", "산 ", head)
    # 천안시서북구 -> 천안시 서북구
    head = re.sub(r"(?<=\S시)(?=\S+구(?:\s|$))", " ", head)
    return head.split()


def without_gu(place: str) -> str:
    """The place names without a 구 word first or after a 시 word: an address may omit a 구 its city
    has (a city that gained 구 keeps its old addresses; 광주광역시 금남로1가), and the code table always
    writes it."""

    words = place.split()
    return " ".join(w for i, w in enumerate(words) if not (w.endswith("구") and (i == 0 or words[i - 1].endswith("시"))))


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
    if len(words) < 3:
        return None
    code = sido_code(words[0], prefixes)
    if code is None:
        return None
    for index in range(1, len(words) - 1):
        number = BUILDING_NUMBER.fullmatch(words[index + 1])
        if ROAD.fullmatch(words[index]) and number:
            # No 시군구 word (세종특별자치시 한누리대로 2130): only a road one 시도 holds once can place it.
            sigungu = [word for word in words[1:index] if word.endswith(("시", "군", "구"))]
            return RoadKey(code, " ".join(sigungu), words[index], int(number.group(1)), int(number.group(2) or 0))
    return None


def legal_dong_index(rows: Iterable[tuple[str, str]], former: Iterable[tuple[str, str]] = (),
                     pairs: Iterable[tuple[str, str]] = ()) -> dict[tuple[str, str], str]:
    """(시도 code, the names after the 시도) -> 10-digit 법정동 code, from current (code, full name) rows.

    A name two current codes share is left out: guessing between them would be a rule this has not.
    ``former`` are the (code, full name) rows the table abolished and ``pairs`` the recorded (old, new)
    10-digit code pairs (``legal_dong_code_change_views.region_pairs(rows, 10)``): an address keeps the
    name a place had (인천광역시 서구 청라동 after 2026-07-01), and it reads as the one current code its
    pairs lead to (root ADR-0329). A current name wins over a former one.
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
    current = {code for codes in exact.values() for code in codes}
    moves: dict[str, set[str]] = {}
    for old, new in pairs:
        moves.setdefault(old, set()).add(new)
    claims: dict[tuple[str, str], set[str]] = {}
    for code, full_name in former:
        words = (full_name or "").split()
        if len(words) < 2 or not re.fullmatch(r"\d{10}", code or ""):
            continue
        place = " ".join(words[1:])
        for successor in current_successors(code, current, moves):
            for name in {place, without_gu(place)}:
                claims.setdefault((successor[:2], name), set()).add(successor)
    index.update({key: next(iter(codes)) for key, codes in claims.items()
                  if len(codes) == 1 and key not in exact and key not in loose})
    return index


def sigungu_rows(source: Iterable[tuple[str, str]]) -> list[tuple[str, list[str]]]:
    """(5-digit code, the words after the 시도) of the 시군구 rows (code ending 00000) of ``source``."""

    return [(code[:5], (full_name or "").split()[1:]) for code, full_name in source
            if re.fullmatch(r"\d{5}00000", code or "") and code[2:5] != "000"]


def sigungu_index(rows: Iterable[tuple[str, str]], former: Iterable[tuple[str, str]] = (),
                  pairs: Iterable[tuple[str, str]] = ()) -> dict[tuple[str, str], set[str]]:
    """(시도 code, the 시군구 words an address writes) -> the current 5-digit 시군구 codes they name.

    From the 법정동 code table's 시군구 rows (code ending 00000): ``rows`` current, ``former`` abolished,
    with ``pairs`` the recorded (old, new) 5-digit pairs (``region_pairs(rows, 5)``). A 시 written
    without its 구 (고양시) names every 구 of it; a former name (인천광역시 서구) names the codes its
    pairs lead to (root ADR-0329); no 시군구 word names every 시군구 of the 시도 (세종특별자치시).
    """

    current = sigungu_rows(rows)
    codes = {code for code, _ in current}
    moves: dict[str, set[str]] = {}
    for old, new in pairs:
        moves.setdefault(old, set()).add(new)
    named = {(code[:2], " ".join(words)) for code, words in current}
    claims = [(code, words, {code}) for code, words in current]
    for code, words in sigungu_rows(former):
        for successor in current_successors(code, codes, moves):
            if (successor[:2], " ".join(words)) not in named:
                claims.append((successor, words, {successor}))
    index: dict[tuple[str, str], set[str]] = {}
    for code, words, targets in claims:
        # Every leading run of the words: 고양시 덕양구, 고양시, and none at all.
        for end in range(len(words) + 1):
            index.setdefault((code[:2], " ".join(words[:end])), set()).update(targets)
    return index


class JusoRoads(NamedTuple):
    """The 주소DB road ledger as ``resolve`` reads it (root ADR-0495): ``roads`` holds (sigungu_code,
    road_name, building_main_no, building_sub_no, pnu) of the current above-ground addresses, and
    ``sigungus`` (road_sido, road_sigungu, sigungu_code) of ``sigungu_index``."""

    roads: Any
    sigungus: Any


def lakehouse_juso_roads(spark: Any, catalog: str, road_names: Iterable[str] | None = None) -> JusoRoads | None:
    """The 주소DB ledger, or None before its first load. ``road_names`` cuts it to the roads a run asks
    about (root ADR-0390)."""

    from legal_dong_code_change_views import region_pairs  # noqa: PLC0415
    from platform_contracts import current_rows, load_lakehouse_contract  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415

    table = f"`{catalog}`.`silver`.`juso_road_address`"
    if not spark.catalog.tableExists(f"{catalog}.silver.juso_road_address"):
        return None
    roads = (current_rows(spark.table(table), load_lakehouse_contract("silver.juso_road_address"))
             .where(F.col("pnu").isNotNull() & (F.col("underground_code") == "0")))
    if road_names is not None:
        names = sorted(set(road_names))
        roads = roads.where(F.col("road_name").isin(names) if names else F.lit(False))
    roads = roads.select("sigungu_code", "road_name", "building_main_no", "building_sub_no", "pnu")
    names, changes = code_table(spark, catalog)
    index = sigungu_index([(c, f) for c, f, s in names if s == "존재"], [(c, f) for c, f, s in names if s != "존재"],
                          region_pairs(changes, 5))
    sigungus = spark.createDataFrame([(sido, words, code) for (sido, words), codes in index.items() for code in codes],
                                     "road_sido string, road_sigungu string, sigungu_code string")
    return JusoRoads(roads, sigungus)


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


def code_table(spark: Any, catalog: str) -> tuple[list[tuple[str, str, str]], list[dict[str, Any]]]:
    """(code, full name, status) of every row of the newest 법정동 snapshot, and the recorded code changes."""

    from legal_dong_code_change_views import CHANGE_TABLE  # noqa: PLC0415

    table = f"`{catalog}`.`reference`.`legal_dong_code_snapshot`"
    newest = spark.sql(f"SELECT max(snapshot_date) AS d FROM {table}").collect()[0]["d"]
    names = [(row["region_cd"], row["full_name"], row["status"]) for row in spark.sql(
        f"SELECT region_cd, full_name, status FROM {table} WHERE snapshot_date = DATE '{newest}'").collect()]
    schema, name = CHANGE_TABLE.split(".")
    changes = [row.asDict() for row in spark.sql(
        f"SELECT kind, old_code, new_code, level, effective_date, source FROM `{catalog}`.`{schema}`.`{name}`").collect()]
    return names, changes


def lakehouse_legal_dong_index(spark: Any, catalog: str) -> dict[tuple[str, str], str]:
    """``legal_dong_index`` of the newest snapshot: its current names, and its abolished names through
    the recorded pairs in force."""

    from legal_dong_code_change_views import region_pairs  # noqa: PLC0415

    names, changes = code_table(spark, catalog)
    return legal_dong_index([(code, full) for code, full, status in names if status == "존재"],
                            [(code, full) for code, full, status in names if status != "존재"],
                            region_pairs(changes, 10))


def lakehouse_sido_prefixes(spark: Any, catalog: str) -> dict[str, str]:
    """``sido_prefixes`` from the newest 법정동 snapshot (every status) and the recorded pairs in force."""

    from legal_dong_code_change_views import region_pairs  # noqa: PLC0415

    names, changes = code_table(spark, catalog)
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


def held(candidates: Any, column: str, parcels: Any, remap: Mapping[str, str] | None) -> Any:
    """``candidates`` (``row_id``, ``column``) with ``_held`` true where the served edition holds the PNU,
    itself or, through ``remap``, one earlier form of its code (`place_identity_resolver.held_forms`,
    root ADR-0482 §1), and ``column`` set to the form held."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    found = candidates.join(parcels.withColumnRenamed("pnu", column).withColumn("_held", F.lit(True)), column, "left")
    if not remap:
        return found
    import place_identity_resolver  # noqa: PLC0415 - the one resolver's form search, not restated

    return (place_identity_resolver.held_forms(
        found.withColumn("_absent", F.when(F.col("_held").isNull(), F.col(column))), "_absent", remap, parcels)
        .withColumn("_held", F.when(F.col("_served_form").isNotNull(), F.lit(True)).otherwise(F.col("_held")))
        .withColumn(column, F.coalesce(F.col("_served_form"), F.col(column)))
        .drop("_absent", "_served_form", "_served_ambiguous"))


def resolve(frame: Any, parcels: Any, titles: Any, remap: Mapping[str, str] | None = None,
            juso: JusoRoads | None = None) -> Any:
    """Adds pnu, pnu_basis, pnu_unresolved_reason and pnu_candidate_count to ``frame``.

    ``frame`` carries ``row_id`` and the output of ``candidates`` flattened: ``jibun_pnu``,
    ``road_sido``, ``road_sigungu``, ``road_name``, ``road_main_no``, ``road_sub_no``, ``reason``.
    ``parcels`` holds the current parcels' ``pnu``; ``titles`` the register's ``pnu`` and
    ``road_address_raw`` (``lakehouse_inputs``). ``remap`` (old -> current legal-dong code) lets a
    jibun whose current form the served edition does not hold be found under an earlier form of its
    code (`place_identity_resolver.held_forms`, root ADR-0482 §1): the names are read against the
    current codes, the served edition may still carry the old ones.

    The steps, first that places a row wins: the jibun address; the 주소DB road (``juso``,
    ``lakehouse_juso_roads``, root ADR-0495): the 시군구 words read as current 시군구 codes, then the
    addresses of that road and building number there, placed only when their representative 지번
    is one PNU the served edition holds; the register's road address.
    """

    from pyspark.sql import functions as F  # noqa: PLC0415

    jibun = held(frame.where(F.col("jibun_pnu").isNotNull()).select("row_id", "jibun_pnu"), "jibun_pnu", parcels, remap)
    asked = frame.where(F.col("road_name").isNotNull())
    out = frame.drop("jibun_pnu").join(jibun.select("row_id", "jibun_pnu", "_held"), "row_id", "left")
    if juso is not None:
        picked = (asked.select("row_id", "road_sido", "road_sigungu", "road_name", "road_main_no", "road_sub_no")
                  .join(juso.sigungus, ["road_sido", "road_sigungu"])
                  .join(juso.roads.withColumnRenamed("building_main_no", "road_main_no")
                        .withColumnRenamed("building_sub_no", "road_sub_no"),
                        ["sigungu_code", "road_name", "road_main_no", "road_sub_no"])
                  .groupBy("row_id").agg(F.countDistinct("pnu").alias("_juso_pnus"), F.first("pnu").alias("juso_pnu")))
        juso_held = held(picked.where(F.col("_juso_pnus") == 1).select("row_id", "juso_pnu"), "juso_pnu", parcels, remap)
        out = (out.join(picked.select("row_id", "_juso_pnus"), "row_id", "left")
               .join(juso_held.select("row_id", "juso_pnu", F.col("_held").alias("_juso_held")), "row_id", "left"))
    else:
        out = (out.withColumn("_juso_pnus", F.lit(None).cast("long")).withColumn("juso_pnu", F.lit(None).cast("string"))
               .withColumn("_juso_held", F.lit(None).cast("boolean")))
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
    road = asked.select("row_id", *keys).join(titles, keys, "left")
    out = out.join(road.select("row_id", "_pnus", "_road_pnu"), "row_id", "left")
    jibun_ok = F.col("jibun_pnu").isNotNull() & F.col("_held")
    juso_ok = F.col("juso_pnu").isNotNull() & F.col("_juso_held")
    road_ok = F.col("_pnus") == 1
    return (out
            .withColumn("pnu", F.when(jibun_ok, F.col("jibun_pnu")).when(juso_ok, F.col("juso_pnu"))
                        .when(road_ok, F.col("_road_pnu")))
            .withColumn("pnu_basis", F.when(jibun_ok, F.lit("jibun_address")).when(juso_ok, F.lit("juso_road_address"))
                        .when(road_ok, F.lit("road_address")))
            .withColumn("pnu_candidate_count", F.when(jibun_ok | juso_ok, F.lit(1))
                        .when(F.col("road_name").isNotNull(),
                              F.greatest(F.coalesce(F.col("_pnus"), F.lit(0)), F.coalesce(F.col("_juso_pnus"), F.lit(0)))
                              .cast("int")))
            .withColumn("pnu_unresolved_reason",
                        F.when(jibun_ok | juso_ok | road_ok, F.lit(None).cast("string"))
                        .when(F.col("reason").isNotNull(), F.col("reason"))
                        .when(F.col("jibun_pnu").isNotNull(), F.lit("parcel_absent"))
                        .when((F.col("_pnus") > 1) | (F.col("_juso_pnus") > 1), F.lit("road_ambiguous"))
                        .otherwise(F.lit("road_not_found")))
            .drop("_held", "_pnus", "_road_pnu", "juso_pnu", "_juso_pnus", "_juso_held"))
