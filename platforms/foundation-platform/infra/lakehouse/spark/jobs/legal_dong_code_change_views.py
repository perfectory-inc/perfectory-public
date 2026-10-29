"""Views of `reference.legal_dong_code_change`, the one record of region code changes (root ADR-0145).

The change table holds every old code → new code pair (시도, 시군구, 읍면동, 리), oldest to newest,
with its evidence. Everything else that needs a code pair reads it through one of these views
instead of storing its own copy:

- `sigungu_crosswalk_view`: the 시군구 crosswalk the hub exports read (the projection file is its
  file form, `legal_dong_code_change_pairs.py` writes it);
- `dong_predecessors`: which old 읍면동 each new one came from, for the administrative-boundary
  id (`administrative_boundaries_handoff_to_silver.py`, ADR-0113 §9);
- `leaf_pairs`: the 동·리 pairs the parcel lineage carries lots across
  (`parcel_lineage.dong_pairing_from_changes`);
- `renumbered_into`: which code a parcel attribute keyed under one side of a renumbering is
  served under (`parcel_panel_silver_to_gold.read_inputs`).
- `region_pairs`: which 시도 codes a reorganization joins, for the section coverage bound
  (`section_coverage.py`, root ADR-0272).

Plain Python over plain rows, so the test lane without PySpark exercises every view. A row is a
mapping with at least `kind`, `old_code`, `new_code`, `level`, `effective_date` and `source`, as the
change table's contract names them.

Every view reads the pairs through `pair_rows`, so none of them sees a pair that official evidence
superseded (root ADR-0329): the table holds both rows forever, and the view resolves to the one in
force.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

# The levels parcels are numbered under (the 읍면동 in a city, the 리 in the country).
LEAF_LEVELS = ("eupmyeondong", "ri")
CHANGE_TABLE = "reference.legal_dong_code_change"
PAIR, SUPERSESSION = "pair", "supersession"
STEWARD_SOURCE = "steward:"
OFFICIAL_SOURCE = "official:parcel-history"
# The evidence grade of a pair, by its `source` (the provenance it is recorded under), strongest
# first (root ADR-0329 §1). A steward's pair is a person's decision: no evidence outranks it
# automatically. The official parcel-number history (ADR-0150) outranks every inference.
EVIDENCE_RANKS: tuple[tuple[str, int], ...] = (
    (STEWARD_SOURCE, 0),
    ("official:", 1),
    ("derived:code-go-kr:date+name:", 2),
    ("derived:parcel-jibun:", 3),
    ("derived:children", 4),
)


def evidence_rank(source: str) -> int:
    """The grade of `source` in `EVIDENCE_RANKS` (lower is stronger); anything else ranks last."""

    return next((rank for prefix, rank in EVIDENCE_RANKS if source.startswith(prefix)), len(EVIDENCE_RANKS))


def superseded_pairs(rows: Iterable[Mapping[str, Any]]) -> set[tuple[str, str]]:
    """Every (old, new) a `supersession` row withdraws (root ADR-0329 §1)."""

    return {(str(row["old_code"]), str(row["new_code"])) for row in rows
            if row.get("kind") == SUPERSESSION and row.get("old_code") and row.get("new_code")}


def is_superseded(row: Mapping[str, Any], superseded: set[tuple[str, str]]) -> bool:
    """Whether the pair `row` is withdrawn. A steward's pair never is: a supersession is only ever
    recorded against a derived pair, and a reader without the `source` column (the Gold build reads
    `kind, old_code, new_code, level`) must reach the same answer as one with it."""

    return ((str(row.get("old_code")), str(row.get("new_code"))) in superseded
            and not str(row.get("source") or "").startswith(STEWARD_SOURCE))


def pair_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, str]]:
    """The pairs in force, one per distinct (old, new, source), as plain strings.

    The table is append-only and a pair may be recorded by several runs; the view repeats none. A
    pair a `supersession` row withdraws is left out (root ADR-0329).
    """

    rows = list(rows)
    superseded = superseded_pairs(rows)
    out: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in rows:
        if row.get("kind") != PAIR or not row.get("old_code") or not row.get("new_code"):
            continue
        if is_superseded(row, superseded):
            continue
        key = (str(row["old_code"]), str(row["new_code"]), str(row.get("source") or ""))
        # A dated row of a pair wins over an undated one (a steward pair recorded before it was dated).
        if key in out and (out[key]["effective_date"] or not row.get("effective_date")):
            continue
        out[key] = {"old_code": key[0], "new_code": key[1], "source": key[2], "level": str(row.get("level") or ""),
                    "effective_date": str(row.get("effective_date") or ""), "rule_verdict": str(row.get("rule_verdict") or ""),
                    "detail": str(row.get("detail") or "")}
    return sorted(out.values(), key=lambda pair: (pair["old_code"], pair["new_code"], pair["source"]))


def leaf_pairs(
    rows: Iterable[Mapping[str, Any]], from_date: str | None = None, to_date: str | None = None
) -> dict[str, dict[str, str]]:
    """{old 동·리: {new 동·리: source}} for every changed leaf code.

    With a window, only the pairs whose `effective_date` falls in it, both ends included
    (`YYYY-MM-DD` or `YYYYMMDD`): a change after the later snapshot has not happened to it yet,
    and an undated pair cannot be placed in any window. An old code with more than one new code
    was split; the caller decides what that means for it.
    """

    low, high = (_day(from_date), _day(to_date)) if from_date or to_date else (None, None)
    out: dict[str, dict[str, str]] = {}
    for pair in pair_rows(rows):
        if pair["level"] not in LEAF_LEVELS or pair["old_code"] == pair["new_code"]:
            continue
        if low is not None or high is not None:
            day = _day(pair["effective_date"])
            if not day or (low and day < low) or (high and day > high):
                continue
        out.setdefault(pair["old_code"], {}).setdefault(pair["new_code"], pair["source"])
    return out


def region_pairs(rows: Iterable[Mapping[str, Any]], digits: int) -> list[tuple[str, str]]:
    """Every (old region, new region) a recorded pair moves a code between, a region being the
    code's first `digits` digits (2: the 시도). Pairs inside one region are not listed."""

    return sorted({(pair["old_code"][:digits], pair["new_code"][:digits]) for pair in pair_rows(rows)
                   if pair["old_code"][:digits] != pair["new_code"][:digits]})


def _day(value: str | None) -> str:
    """`YYYYMMDD` from either form the dates come in; blank stays blank."""

    return (value or "").replace("-", "")


def sigungu_crosswalk_view(
    rows: Iterable[Mapping[str, Any]], cadastral_sido: Iterable[str]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """The 시군구 crosswalk of the merged 시도 the cadastral parcel set does not carry yet.

    Old → new, like the table: `sido` entries are `{new_code, old_codes, effective_date}` (2 digits),
    `sigungu` entries `{old_code, new_code, effective_date, source}` (5 digits). A new code carried
    from two old codes, or an old code split into two new ones, is not one entry and goes to the
    review list. Returns (crosswalk, review).
    """

    # Imported here: code_go_kr_legal_dong imports parcel_lineage, which imports this module.
    import code_go_kr_legal_dong as cg  # noqa: PLC0415

    return cg.sigungu_crosswalk(pair_rows(rows), cadastral_sido)


def dong_predecessors(rows: Iterable[Mapping[str, Any]], lots_before: Mapping[str, Sequence[str] | set[str]]) -> dict[str, str]:
    """{new code: old code} for every renumbered 읍면동, and the 리 under it (ADR-0113 §9).

    The same answer the predecessor map this view replaced gave from the dong pairing of two
    cadastral snapshots:

    - an old code the table sends to two new codes was split and is no one's predecessor (the
      pairing left it unpaired);
    - several old codes landing on one new code (dongs merged) leave the id with the one that held
      the most lots in `lots_before` (the 지번 each old code held before the change; the table
      records no lot counts, so they are required), a tie going to the smaller old code;
    - rural parcels are numbered under the 리 while the boundary layer draws the 읍면동
      (`xxxxxxxx00`), so 리 pairs roll up: the old 읍면동 that brought the most lots into a new one
      is its predecessor unless that 읍면동 was paired directly, a tie going to the larger code.
    """

    def weight(code: str) -> int:
        return len(lots_before.get(code, ()))

    pairs = {old: next(iter(news)) for old, news in leaf_pairs(rows).items() if len(news) == 1}
    best: dict[str, tuple[int, str]] = {}
    for old in sorted(pairs):
        new = pairs[old]
        if new not in best or weight(old) > best[new][0]:
            best[new] = (weight(old), old)
    rolled: dict[str, dict[str, int]] = {}
    for old in sorted(pairs):
        new = pairs[old]
        if old[:8] == new[:8]:
            continue
        into = rolled.setdefault(new[:8] + "00", {})
        into[old[:8] + "00"] = into.get(old[:8] + "00", 0) + weight(old)
    for new_emd, olds in rolled.items():
        if new_emd not in best:
            old_emd, carried = max(olds.items(), key=lambda item: (item[1], item[0]))
            best[new_emd] = (carried, old_emd)
    return {new: old for new, (_, old) in sorted(best.items())}


def renumbered_into(rows: Iterable[Mapping[str, Any]], served_codes: Iterable[str]) -> dict[str, str]:
    """{source 동·리 code: the served parcel edition's code for the same place}.

    A Silver lane keys its rows by the codes of its own release, the map by the codes of the served
    parcel edition; across a reorganization the two disagree (2026-07: 29·46 → 12). Only a 1:1
    renumbering is translated, either way: its lots keep their numbers (ADR-0144 §3.1), so the rest
    of the PNU still names the same parcel. A split or merged code, or one changed twice, is not
    one place on both sides and is left for the parcel lineage (ADR-0113). A code the served
    edition holds is never translated.
    """

    served = set(served_codes)
    forward = leaf_pairs(rows)
    backward: dict[str, set[str]] = {}
    for old, news in forward.items():
        for new in news:
            backward.setdefault(new, set()).add(old)
    into: dict[str, set[str]] = {}
    for old, news in forward.items():
        new = next(iter(news))
        if len(news) != 1 or len(backward[new]) != 1:
            continue
        for source, target in ((old, new), (new, old)):
            if target in served and source not in served:
                into.setdefault(source, set()).add(target)
    return {source: next(iter(targets)) for source, targets in sorted(into.items()) if len(targets) == 1}


# How many leading PNU digits a code of each level fixes.
CODE_LEVEL_DIGITS = {"sido": 2, "sigungu": 5, "eupmyeondong": 8, "ri": 10}


def code_prefix(code: str) -> str:
    """The PNU prefix every parcel under `code` starts with: as many digits as its level fixes."""

    import code_go_kr_legal_dong as cg  # noqa: PLC0415 - imported here like `sigungu_crosswalk_view`

    return code[:CODE_LEVEL_DIGITS[cg.code_level(code)]]


def changed_codes(
    before: Iterable[Mapping[str, Any]], after: Iterable[Mapping[str, Any]],
    columns: Sequence[str] = ("kind", "old_code", "new_code", "level"),
) -> set[str]:
    """Both codes of every row that differs between two reads of the table, by `columns` (the Gold
    build's). The parcels under them are the ones a change of the table can reach (root ADR-0273):
    a supersession reaches its old code, the code it withdrew and the code that replaced it, and
    nothing else (root ADR-0329 §3)."""

    def keyed(rows: Iterable[Mapping[str, Any]]) -> set[tuple[str, ...]]:
        return {tuple(str(row.get(column) or "") for column in columns) for row in rows}

    moved = keyed(before) ^ keyed(after)
    at = {column: index for index, column in enumerate(columns)}
    return {row[at[side]] for row in moved for side in ("old_code", "new_code") if len(row[at[side]]) == 10}


def corrected_successors(rows: Iterable[Mapping[str, Any]]) -> dict[str, str]:
    """{the new 동·리 a superseded pair named: the one now in force for the same old code}, where the
    old code has exactly one pair in force (root ADR-0329 §3). An id one inherited from the old code
    belongs to the other (`administrative_boundaries_handoff_to_silver.resolve_unit_ids`)."""

    rows = list(rows)
    current = leaf_pairs(rows)
    out: dict[str, str] = {}
    for old, withdrawn in sorted(superseded_pairs(rows)):
        news = current.get(old, {})
        if len(news) == 1 and withdrawn not in news:
            out[withdrawn] = next(iter(news))
    return out
