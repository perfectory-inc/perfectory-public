"""Views of `reference.legal_dong_code_change`, the one record of region code changes (root ADR-0145).

The change table holds every old code → new code pair (시도, 시군구, 읍면동, 리), oldest to newest,
with its evidence. Everything else that needs a code pair reads it through one of these views
instead of storing its own copy:

- `sigungu_crosswalk_view`: the 시군구 crosswalk the hub exports read (the projection file is its
  file form, `legal_dong_code_change_pairs.py` writes it);
- `dong_predecessors`: which old 읍면동 each new one came from, for the administrative-boundary
  id (`administrative_boundaries_handoff_to_silver.py`, ADR-0113 §9);
- `leaf_pairs`: the 동·리 pairs the parcel lineage carries lots across
  (`parcel_lineage.dong_pairing_from_changes`).

Plain Python over plain rows, so the test lane without PySpark exercises every view. A row is a
mapping with at least `kind`, `old_code`, `new_code`, `level`, `effective_date` and `source`, as the
change table's contract names them.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

# The levels parcels are numbered under (the 읍면동 in a city, the 리 in the country).
LEAF_LEVELS = ("eupmyeondong", "ri")
CHANGE_TABLE = "reference.legal_dong_code_change"


def pair_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, str]]:
    """The recorded pairs, one per distinct (old, new, source), as plain strings.

    The table is append-only and a pair may be recorded by several runs; the view repeats none.
    """

    seen: set[tuple[str, str, str]] = set()
    out: list[dict[str, str]] = []
    for row in rows:
        if row.get("kind") != "pair" or not row.get("old_code") or not row.get("new_code"):
            continue
        key = (str(row["old_code"]), str(row["new_code"]), str(row.get("source") or ""))
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {"old_code": key[0], "new_code": key[1], "source": key[2], "level": str(row.get("level") or ""),
             "effective_date": str(row.get("effective_date") or ""), "rule_verdict": str(row.get("rule_verdict") or ""),
             "detail": str(row.get("detail") or "")}
        )
    return sorted(out, key=lambda pair: (pair["old_code"], pair["new_code"], pair["source"]))


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

    crosswalk, review = cg.sigungu_crosswalk(pair_rows(rows), cadastral_sido)
    return {
        "sido": [
            {"new_code": sido["current_code"], "old_codes": sido["supersedes"], "effective_date": sido["effective_from"]}
            for sido in crosswalk["sido"]
        ],
        "sigungu": [
            {"old_code": entry["superseded_code"], "new_code": entry["current_code"], "effective_date": entry["valid_from"],
             "source": entry["provenance"]}
            for entry in crosswalk["sigungu"]
        ],
    }, review


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
