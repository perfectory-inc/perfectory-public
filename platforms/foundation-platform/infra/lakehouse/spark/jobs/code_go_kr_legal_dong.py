"""Parser and pairing kernels for the 법정동 code change source (root ADR-0143, ADR-0144).

The collector (`collect-code-go-kr-legal-dong` in the outbox publisher) lands the code.go.kr 법정동
전체 표 in Bronze unchanged: one HTML table, every code current and abolished, with its parent and
its 생성일·폐지일. Everything that reads those bytes lives here, in plain Python over plain values, so
the test lane that runs `infra/lakehouse/spark/tests` (no PySpark, standard library only) exercises
every rule:

- the full table, refused whole when its shape changed;
- pairing, from downloaded data only (ADR-0144; the site's 코드변경안내 notice board is
  not a source): the date + name rule, then the official parcel-number history, then the 지번 sets
  of two parcel snapshots matched as the same land (지목 and area; ADR-0113 §5), and everything else
  to the steward;
- the 시군구 crosswalk the hub loaders read, and which merged 시도 it governs.

A response that no longer has the shape the contract names is refused with `SourceFormatError`:
nothing is loaded, and the job fails so the scheduler's failure alert fires (ADR-0143 §6). Polygons
are never evidence (ADR-0113 §4).
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import parcel_lineage as pl
from parcel_land import jimok_of, wkb_area_m2  # noqa: F401 - the 지번 step reads both through this module
from legal_dong_code_change_views import LEAF_LEVELS

CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contracts" / "code-go-kr-legal-dong.contract.json"
EXISTS, ABOLISHED = "존재", "폐지"
CODE_PATTERN = re.compile(r"[0-9]{10}")


class SourceFormatError(ValueError):
    """A code.go.kr response no longer has the shape the contract names. Nothing may load."""


class TableShrunk(ValueError):
    """The full table is smaller than its bounds allow. Nothing may load."""


def load_source_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


# --- HTML tables ------------------------------------------------------------------------------


@dataclass
class _Table:
    head: list[str] | None = None
    rows: list[list[tuple[str, tuple[str, ...]]]] = field(default_factory=list)
    row: list[tuple[str, tuple[str, ...]]] | None = None
    cell: list[str] | None = None
    refs: list[str] = field(default_factory=list)
    in_head: bool = False
    head_rows: list[list[tuple[str, tuple[str, ...]]]] = field(default_factory=list)


class _TableParser(HTMLParser):
    """Every <table> in a page as (header texts, rows of (cell text, link targets)).

    The site leaves `<tr>` unclosed and nests layout tables, so a row ends at the next `<tr>` or at
    the end of its own table, and a cell belongs to the innermost open table only.
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.open: list[_Table] = []
        self.done: list[_Table] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            self.open.append(_Table())
            return
        if not self.open:
            return
        table = self.open[-1]
        if tag == "thead":
            self._end_row(table)
            table.in_head = True
        elif tag == "tr":
            self._end_row(table)
            table.row = []
        elif tag in ("td", "th"):
            self._end_cell(table)
            if table.row is None:
                table.row = []
            table.cell, table.refs = [], []
        elif tag == "a" and table.cell is not None:
            table.refs.append(dict(attrs).get("href") or "")

    def handle_endtag(self, tag: str) -> None:
        if not self.open:
            return
        table = self.open[-1]
        if tag in ("td", "th"):
            self._end_cell(table)
        elif tag == "tr":
            self._end_row(table)
        elif tag == "thead":
            self._end_row(table)
            table.in_head = False
            if table.head_rows:
                table.head = [text for text, _ in table.head_rows[-1]]
        elif tag == "table":
            self._end_row(table)
            self.done.append(self.open.pop())

    def handle_data(self, data: str) -> None:
        if self.open and self.open[-1].cell is not None:
            self.open[-1].cell.append(data)

    def close(self) -> None:
        super().close()
        while self.open:
            table = self.open.pop()
            self._end_row(table)
            self.done.append(table)

    @staticmethod
    def _end_cell(table: _Table) -> None:
        if table.cell is None:
            return
        text = " ".join("".join(table.cell).split())
        if table.row is None:
            table.row = []
        table.row.append((text, tuple(table.refs)))
        table.cell, table.refs = None, []

    def _end_row(self, table: _Table) -> None:
        self._end_cell(table)
        if table.row:
            (table.head_rows if table.in_head else table.rows).append(table.row)
        table.row = None


def html_tables(text: str) -> list[_Table]:
    parser = _TableParser()
    parser.feed(text)
    parser.close()
    return parser.done


def _headed_table(text: str, expected: Sequence[str], what: str) -> _Table:
    """The one table whose header row starts the way the contract says; refuses any other shape.

    A table is found by its first header; its whole header row must then equal the contract's, in
    order. A reordered, renamed, added or dropped column is a changed site, not something to map
    around: the cells would land under the wrong names.
    """

    first = expected[0]
    found = [table for table in html_tables(text) if table.head and table.head[0] == first]
    if not found:
        raise SourceFormatError(f"the {what} page holds no table headed {first!r}")
    if len(found) > 1:
        raise SourceFormatError(f"the {what} page holds {len(found)} tables headed {first!r}")
    table = found[0]
    if table.head != list(expected):
        raise SourceFormatError(
            f"the {what} columns changed: expected {list(expected)}, got {table.head}"
        )
    return table


# --- the full table ---------------------------------------------------------------------------

FULL_TABLE_FIELDS = (
    "region_cd",
    "full_name",
    "status",
    "parent_cd",
    "rank",
    "created_date",
    "abolished_date",
    "last_work_date",
    "lowest_name",
    "jumin_cd",
    "jijuk_cd",
)


def _date_or_blank(value: str, what: str, line: int) -> str:
    if not value:
        return ""
    try:
        datetime.strptime(value, "%Y%m%d")
    except ValueError as error:
        raise SourceFormatError(f"row {line}: {what} {value!r} is not a YYYYMMDD date") from error
    return value


def parse_full_table_html(text: str, contract: Mapping[str, Any]) -> list[dict[str, str]]:
    """The 법정동 전체 표 as one dict per code, in the order the site lists them.

    Fields follow `FULL_TABLE_FIELDS`, one per contract header in the same order. `status` is
    translated to the 존재/폐지 words the snapshot table already uses. Every row must carry every
    column, a 10-digit code, a parent that is blank or digits (the site leaves it blank on most
    long-abolished codes, and short on a few), and dates that are blank or real days; a malformed row or a
    repeated code refuses the whole table rather than skipping a row (a dropped code later reads as
    an abolished one).
    """

    spec = contract["full_table"]
    headers = spec["headers"]
    if len(headers) != len(FULL_TABLE_FIELDS):
        raise ValueError("the contract's full_table.headers must name one column per FULL_TABLE_FIELDS entry")
    status_words = spec["status_words"]
    table = _headed_table(text, headers, "full table")
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for line, cells in enumerate(table.rows, start=1):
        if len(cells) != len(headers):
            raise SourceFormatError(f"row {line}: {len(cells)} cells, expected {len(headers)}")
        row = dict(zip(FULL_TABLE_FIELDS, (text for text, _ in cells)))
        if not CODE_PATTERN.fullmatch(row["region_cd"]):
            raise SourceFormatError(f"row {line}: {row['region_cd']!r} is not a 10-digit 법정동코드")
        if row["parent_cd"] and not row["parent_cd"].isdigit():
            raise SourceFormatError(f"row {line}: parent {row['parent_cd']!r} is not a code")
        if row["status"] not in status_words:
            raise SourceFormatError(f"row {line}: status {row['status']!r} is none of {sorted(status_words)}")
        row["status"] = status_words[row["status"]]
        for name in ("created_date", "abolished_date", "last_work_date"):
            row[name] = _date_or_blank(row[name], name, line)
        for name in ("jumin_cd", "jijuk_cd"):
            if row[name] and not row[name].isdigit():
                raise SourceFormatError(f"row {line}: {name} {row[name]!r} is not a code")
        if row["region_cd"] in seen:
            raise SourceFormatError(f"row {line}: {row['region_cd']} appears twice")
        seen.add(row["region_cd"])
        rows.append(row)
    return rows


def check_table_size(row_count: int, previous_row_count: int | None, contract: Mapping[str, Any]) -> None:
    """Refuses a full table below the contract's floor, or shrunk from the previous one.

    The table lists abolished codes too, so it only grows. A smaller one is a broken response.
    """

    bounds = contract["full_table"]["bounds"]
    if row_count < bounds["min_rows"]:
        raise TableShrunk(f"the full table has {row_count} rows, below the floor of {bounds['min_rows']}")
    if previous_row_count:
        allowed = previous_row_count * (1 - bounds["max_shrink_share"])
        if row_count < allowed:
            raise TableShrunk(
                f"the full table has {row_count} rows, {previous_row_count - row_count} fewer than the "
                f"previous {previous_row_count} (max shrink share {bounds['max_shrink_share']})"
            )


# --- pairing ----------------------------------------------------------------------------------


def code_level(code: str) -> str:
    """sido, sigungu, eupmyeondong or ri, from which parts of the 10-digit code are zero."""

    if code[2:] == "00000000":
        return "sido"
    if code[5:] == "00000":
        return "sigungu"
    if code[8:] == "00":
        return "eupmyeondong"
    return "ri"


LEVELS = ("sido", "sigungu", "eupmyeondong", "ri")


def name_below_sido(full_name: str) -> tuple[str, ...]:
    """The name without its 시도: a merger renames the 시도, never what lies under it (ADR-0113 §5)."""

    return tuple(full_name.split()[1:])


def parent_code(row: Mapping[str, str]) -> str:
    """The row's 상위지역코드, or the code above it by structure where the site leaves it blank."""

    if CODE_PATTERN.fullmatch(row.get("parent_cd") or ""):
        return row["parent_cd"]
    code = row["region_cd"]
    level = code_level(code)
    if level == "sido":
        return "0000000000"
    if level == "sigungu":
        return code[:2] + "00000000"
    if level == "eupmyeondong":
        return code[:5] + "00000"
    return code[:8] + "00"


def _next_day(value: str) -> str:
    return (datetime.strptime(value, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")


def _under(code: str, ancestor: str) -> bool:
    """Whether `code` lies below `ancestor` in the code's own digits."""

    level = code_level(ancestor)
    width = {"sido": 2, "sigungu": 5, "eupmyeondong": 8}.get(level)
    return width is not None and code != ancestor and code[:width] == ancestor[:width]


def _parent_at(code: str, level: str) -> str:
    return code[:2] + "00000000" if level == "sido" else code[:5] + "00000"


Land = tuple[str, float]
"""A parcel as the 지번 step compares it across editions: (지목, area in m²)."""



# A pair from these sources is derived, so official evidence may contradict it (`hold_against_official`);
# a steward's or the official history's own pair is not re-judged.
DERIVED_SOURCES = ("derived:parcel-jibun:", "derived:code-go-kr:date+name:")

BELOW_WAIT = re.compile(r"^\d+ codes below wait: ")
OFFICIAL_HISTORY_MISSING = "the official parcel-number history (필지고유번호변동연혁), not collected"


class PairingConflict(ValueError):
    """Two kinds of evidence settle one code on different new codes. Nothing may be written."""


def is_dong_level_link(pnu: str) -> bool:
    """A 필지고유번호변동연혁 row that moves a whole 법정동 (대장구분 0, 본번·부번 0000): its 19 digits are
    the 10-digit code and nine zeros. The provider writes a renumbered 동 as one such row, not one per
    parcel (measured 2026-10-05: 인천 2026-07-01 44 rows, 화성 2026-02-01 195 rows, all of this kind)."""

    return len(pnu) == 19 and pnu[10:] == "000000000"


@dataclass
class JibunEvidence:
    """The parcels each 법정동 held in two parcel snapshots, one taken before the change and one
    after: {code: {lot (ledger kind + 본번 + 부번, `parcel_lineage.lot`): its `Land`}}. `label` names
    the pair in the source. A lot counts as the same land in both only when its 지목 is the same and
    its area within the contract's `pairing.land_match` tolerance (`same_land`)."""

    before: Mapping[str, Mapping[str, Land]]
    after: Mapping[str, Mapping[str, Land]]
    label: str


def land_match_tolerance(contract: Mapping[str, Any]) -> tuple[float, float]:
    """(relative, absolute m²): the contract's `pairing.land_match`."""

    match = contract["pairing"]["land_match"]
    return float(match["area_relative_tolerance"]), float(match["area_absolute_tolerance_m2"])


def same_land(before: Land, after: Land, tolerance: tuple[float, float]) -> bool:
    """Whether one lot number names the same land in two editions: the same 지목, and an area that
    moved by no more than the larger of the relative and the absolute tolerance. A lot number alone
    is not the land: a renumbered 동 elsewhere can reuse it for a different parcel."""

    (jimok, area), (jimok_after, area_after) = before, after
    relative, absolute = tolerance
    return bool(jimok) and jimok == jimok_after and abs(area - area_after) <= max(relative * area, absolute)


@dataclass
class EditionEvidence:
    """Each abolished 동·리's own `JibunEvidence`: the two parcel editions that bracket its change
    (`vworld_parcel_editions.bracketing`), so each reorganization is read from its own pair. A code
    whose pair is not held yet is in `awaiting`, with the edition it needs named (ADR-0144 §4)."""

    by_code: Mapping[str, JibunEvidence]
    awaiting: Mapping[str, str]


@dataclass
class PairingResult:
    pairs: list[dict[str, str]] = field(default_factory=list)
    review: list[dict[str, Any]] = field(default_factory=list)
    # {old code: how many of its parcels the official parcel rows move without deciding where the 동 went}
    official_partial_moves: dict[str, int] = field(default_factory=dict)


def pair_changes(
    rows: Sequence[Mapping[str, str]],
    floor_date: str,
    decided: Sequence[Mapping[str, str]] = (),
    jibun: JibunEvidence | EditionEvidence | None = None,
    official_links: Iterable[tuple[str, str, str]] | None = None,
    min_share: float = pl.SPLIT_SIGNAL_OVERLAP,
    land_tolerance: tuple[float, float] | None = None,
) -> PairingResult:
    """Old code → new code for every code abolished on or after `floor_date`, from data only.

    The order is the evidence's strength for what it can see (ADR-0144):

    0. A pair the change table already records (`decided`: a steward's `steward:<who>`, or an
       earlier run's) stands: the ledger is append-only, so what one run recorded is not undone by
       a later run that lacks its evidence.
    1. **Date + name** (`derived:code-go-kr:date+name:<day>`): a code created the same day (or the
       day after: 폐지일 = 생성일 − 1 counts as the same day), at the same level, under a related
       parent (the same parent, or one the pairs already carry to it), whose name below the 시도 is
       the same. Exactly one such code is a pair. It runs top-down, so a 시군구 paired here relates
       the 읍면동 under it.
    2. **Official parcel-number history** (`official:parcel-history`, root ADR-0150): the
       필지고유번호변동연혁 links (`official_links`, (old PNU, new PNU, 토지이동일자 YYYYMMDD)). A
       동-level row (`is_dong_level_link`) naming exactly one current code settles it (`detail`
       `dong_level`); otherwise the parcel rows do, when they carry every linked parcel of the old
       code into one new code (`parcel_level`), and a code they carry into several is a split. This is
       how a code whose 지번 were renumbered is settled. Off when `official_links` is None. It decides
       before the 지번 step; where both settle a code they must agree, and a disagreement raises
       `PairingConflict` naming both answers. A pair the change table already records, or the date +
       name rule or the 지번 step settles, from a derived source is held against it too
       (`hold_against_official`), but only against decisive official evidence of the same event:
       동-level rows, or parcel rows moving at least `min_share` of the old code's parcels into one
       code, dated in the old code's change window (its 폐지일 or the day after). Parcel rows moving
       fewer (a boundary adjustment of a few parcels, often months earlier, while the 동 lives on)
       are evidence for those parcels' lineage, never for the 동's pair; they are counted in
       `official_partial_moves` (root ADR-0156).
    3. **지번 sets** (`derived:parcel-jibun:<snapshots>`), for what 1 and 2 cannot settle (a
       renamed, split or merged 동): of the codes at its level that newly hold parcels in the later
       snapshot, lie in its 시도 or one a 시도 pair carries it onto, and were created in its change
       window (its 폐지일 or the day after), the one holding the largest share of the old code's
       parcels — a lot counts only where it is the same land in both editions (`same_land`: 지목
       and area within `land_tolerance`, the contract's `pairing.land_match`) — when that share is
       at least `min_share` (the contract's `pairing.jibun_overlap_min_share`, ADR-0113 §5) and no
       other code holds as many. Off when `jibun` is None. With `EditionEvidence` each code is
       read from the editions that bracket its own change; a code whose editions are not held
       waits for them.
    4. **Roll-up** (`derived:children`): a 시군구 or 시도 none of the above settles, whose leaf codes
       were paired by 2 or 3, pairs with the parent that received at least `min_share` of them
       (weighted by their 지번 where the snapshots are given).

    Steps 1–4 repeat until nothing changes, since each one's pairs relate the next. Whatever is
    left goes on the review list with what each step saw and a `status` (ADR-0144 §3–4):

    - `awaiting_data` — a step that could decide it lacks its data (no parcel snapshot pair, or no
      parcel-number history). Data decides it when it arrives; the name rule does not guess, and a
      steward may not either.
    - `split` — its 지번 went to several new codes, none holding the contract's share. Not one
      pair; `split_into` records how many went where (the parcel lineage links each 지번).
    - `steward` — every step had its data and none settled it: a person decides.
    """

    tolerance = land_tolerance or land_match_tolerance(load_source_contract())
    by_code = {row["region_cd"]: row for row in rows}
    alive ={code for code, row in by_code.items() if row["status"] == EXISTS}
    created: dict[str, list[Mapping[str, str]]] = {}
    for row in rows:
        if row.get("created_date", "") >= floor_date:
            created.setdefault(row["created_date"], []).append(row)
    decided_by_old: dict[str, list[Mapping[str, str]]] = {}
    for pair in decided:
        decided_by_old.setdefault(pair["old_code"], []).append(pair)
    links_by_old: dict[str, dict[str, int]] = {}
    linked_lots: dict[str, set[str]] = {}
    dong_links_by_old: dict[str, set[str]] = {}
    # {old code: [(old lot, or "" for a 동-level row; new code; 토지이동일자)]}, for the same-event test
    dated_by_old: dict[str, list[tuple[str, str, str]]] = {}
    for old_pnu, new_pnu, changed_on in official_links or ():
        if old_pnu[:10] == new_pnu[:10]:
            continue
        whole = is_dong_level_link(old_pnu) and is_dong_level_link(new_pnu)
        dated_by_old.setdefault(old_pnu[:10], []).append(("" if whole else old_pnu[10:], new_pnu[:10], changed_on))
        if whole:
            dong_links_by_old.setdefault(old_pnu[:10], set()).add(new_pnu[:10])
        else:
            news = links_by_old.setdefault(old_pnu[:10], {})
            news[new_pnu[:10]] = news.get(new_pnu[:10], 0) + 1
            linked_lots.setdefault(old_pnu[:10], set()).add(old_pnu[10:])

    abolished = {
        row["region_cd"] for row in rows if row["status"] == ABOLISHED and row.get("abolished_date", "") >= floor_date
    }
    olds = abolished | set(decided_by_old)
    order = lambda code: (LEVELS.index(code_level(code)), code)  # noqa: E731
    successors: dict[str, set[str]] = {}
    result = PairingResult()
    weight: dict[str, int] = {}

    def accept(old: str, new: str, source: str, verdict: str, detail: str = "") -> None:
        successors.setdefault(old, set()).add(new)
        result.pairs.append(
            {"old_code": old, "new_code": new, "level": code_level(old),
             "effective_date": by_code.get(new, {}).get("created_date", ""), "source": source,
             "rule_verdict": verdict, "detail": detail}
        )

    def rule_candidates(old: str) -> list[str]:
        row = by_code.get(old)
        if row is None or row["status"] != ABOLISHED or not row.get("abolished_date"):
            return []
        day = row["abolished_date"]
        parent = parent_code(row)
        parents = successors.get(parent, set()) | {parent}
        leaf = name_below_sido(row["full_name"])
        return sorted(
            {
                new["region_cd"]
                for when in (day, _next_day(day))
                for new in created.get(when, [])
                if new["region_cd"] != old
                and code_level(new["region_cd"]) == code_level(old)
                and parent_code(new) in parents
                and name_below_sido(new["full_name"]) == leaf
            }
        )

    no_parcels = JibunEvidence({}, {}, "")
    newly: dict[int, set[str]] = {}

    def evidence(code: str) -> tuple[JibunEvidence | None, str]:
        """The 지번 evidence `code` is judged on, or (None, why it has none)."""

        if jibun is None:
            return None, "jibun evidence off"
        if isinstance(jibun, JibunEvidence):
            return jibun, ""
        if code in jibun.by_code:
            return jibun.by_code[code], ""
        if code in jibun.awaiting:
            return None, jibun.awaiting[code]
        waiting = sorted(leaf for leaf in jibun.awaiting if _under(leaf, code))
        if waiting:  # a 시군구 or 시도 rolls up from 동 still waiting for their editions
            return None, f"{len(waiting)} codes below wait: {jibun.awaiting[waiting[0]]}"
        return no_parcels, ""

    def newly_held(ev: JibunEvidence) -> set[str]:
        if id(ev) not in newly:
            newly[id(ev)] = {code for code in set(ev.after) - set(ev.before) if code in alive}
        return newly[id(ev)]

    def sido_scope(old: str) -> set[str]:
        """The 시도 a successor of `old` may lie in: its own, and every 시도 the pairs carry its 시도
        onto (a merger's governed successor, read from the sido pairs, never from a list here)."""

        scope: set[str] = set()
        todo = [old[:2]]
        while todo:
            sido = todo.pop()
            if sido not in scope:
                scope.add(sido)
                todo.extend(new[:2] for new in successors.get(sido + "00000000", ()))
        return scope

    def change_window(old: str) -> set[str]:
        """The days `old`'s own change is dated on: its 폐지일 and the day after (폐지일 = 생성일 − 1
        is the same change, as the name rule reads it), or none when the table gives no 폐지일."""

        row = by_code.get(old)
        day = row.get("abolished_date", "") if row else ""
        return {day, _next_day(day)} if day else set()

    def jibun_candidates(old: str, ev: JibunEvidence) -> list[str]:
        """The codes the 지번 step may weigh for `old`: at its level, newly holding parcels, in its
        own or its successor 시도, and created in its change window (its 폐지일 or the day after,
        as the name rule reads it). Without the last two, any code created since the floor whose lot
        range covers `old`'s, a 리 across the country with 본번 1–2000, outscores its real successor."""

        window = change_window(old)
        if not window:
            return []
        scope, level = sido_scope(old), code_level(old)
        return sorted(
            new for new in newly_held(ev)
            if code_level(new) == level and new[:2] in scope and by_code[new].get("created_date", "") in window
        )

    def overlap(old: str, new: str, ev: JibunEvidence) -> int:
        """How many of `old`'s parcels `new` holds as the same land (`same_land`), not merely the
        same lot number."""

        after = ev.after.get(new, {})
        return sum(1 for lot, land in ev.before.get(old, {}).items() if lot in after and same_land(land, after[lot], tolerance))

    def jibun_best(old: str) -> tuple[str, float, bool]:
        """(best new code, its share of the old code's parcels, whether it is the only best)."""

        ev, _ = evidence(old)
        before = ev.before.get(old, {}) if ev is not None else {}
        if not before:
            return "", 0.0, False
        scored = sorted(((overlap(old, new, ev), new) for new in jibun_candidates(old, ev)), reverse=True)
        if not scored or scored[0][0] == 0:
            return "", 0.0, False
        unique = len(scored) == 1 or scored[0][0] > scored[1][0]
        return scored[0][1], scored[0][0] / len(before), unique

    def parcel_coverage(old: str) -> tuple[int, int] | None:
        """(how many of `old`'s parcels in the earlier edition the official parcel rows link, how many
        it held), or None when no edition says what it held."""

        ev, _ = evidence(old)
        before = set(ev.before.get(old, {})) if ev is not None else set()
        return (len(linked_lots.get(old, set()) & before), len(before)) if before else None

    def official_answer(old: str) -> tuple[str, str]:
        """(the one current code the parcel-number history moves `old` into, which rows said so), or
        ("", ""). A 동-level row decides first. The parcel rows decide only where no 동-level row names
        it, all of them go to one current code, and they link at least `min_share` of the parcels the
        old code held in the earlier edition: a few boundary-adjustment rows are evidence of where
        those parcels went, not of where the 동 went."""

        dong = dong_links_by_old.get(old, set())
        if dong:
            return (next(iter(dong)), "dong_level") if len(dong) == 1 and dong <= alive else ("", "")
        news = set(links_by_old.get(old, {}))
        covered = parcel_coverage(old)
        if len(news) == 1 and news <= alive and covered is not None and covered[0] / covered[1] >= min_share:
            return next(iter(news)), "parcel_level"
        return "", ""

    def event_moves(old: str) -> tuple[set[str], str, set[str]]:
        """(the codes decisive official evidence of `old`'s own change moves it into, which rows said
        so, the parcel lots of that decisive move). Decisive: its 동-level rows (one code, a split's
        several, or a chain's next hop), else parcel rows moving at least `min_share` of the parcels
        it held in the earlier edition into one code. Only rows dated in its change window count: a
        move on another day is another event (a boundary adjustment while the 동 lived on)."""

        window = change_window(old)
        rows = [(lot, new) for lot, new, day in dated_by_old.get(old, ()) if day in window]
        whole = {new for lot, new in rows if not lot}
        if whole:
            return whole, "dong_level", set()
        ev, _ = evidence(old)
        before = set(ev.before.get(old, {})) if ev is not None else set()
        moved: dict[str, set[str]] = {}
        for lot, new in rows:
            if lot in before:
                moved.setdefault(new, set()).add(lot)
        decisive = {new: lots for new, lots in moved.items() if len(lots) / len(before) >= min_share}
        return set(decisive), "parcel_level", set().union(*decisive.values())

    def hold_against_official(old: str, new: str, source: str) -> None:
        """A derived pair (recorded, the date + name rule's, or the 지번 step's) must name a code that
        decisive official evidence of the same change (`event_moves`) moves `old` into, whenever that
        evidence names any: a single answer, one of a split, or the next hop of a chain."""

        if code_level(old) not in LEAF_LEVELS or not source.startswith(DERIVED_SOURCES):
            return
        targets, how, _ = event_moves(old)
        if targets and new not in targets:
            raise PairingConflict(
                f"{old}: the official parcel-number history ({how}, dated {'/'.join(sorted(change_window(old)))}) "
                f"moves it into {', '.join(sorted(targets))}, the pair from {source} says {new}; nothing is "
                "written until the evidence is reconciled")

    def partial_moves(old: str) -> int:
        """How many of `old`'s parcels the official parcel rows move outside a decisive move of its own
        change: evidence for those parcels' lineage, not for the 동's pair."""

        _, _, decided = event_moves(old)
        return len(linked_lots.get(old, set()) - decided)

    def jibun_answer(old: str) -> tuple[str, float]:
        """The code the 지번 step settles `old` on and its share, or ("", 0.0). Called after the
        official evidence, and only judged against it (`settle_leaf`)."""

        best, share, unique = jibun_best(old)
        return (best, share) if best and unique and share >= min_share else ("", 0.0)

    def settle_leaf(old: str) -> None:
        """Official parcel-number history first, then the 지번 step; two answers must agree."""

        official, how = official_answer(old)
        by_land, share = jibun_answer(old)
        if official and by_land and official != by_land:
            raise PairingConflict(
                f"{old}: the official parcel-number history ({how}) says {official}, the 지번 step "
                f"({evidence(old)[0].label}, share {share:.4f}) says {by_land}; nothing is written "
                "until the evidence is reconciled")
        if by_land and not official:
            hold_against_official(old, by_land, f"derived:parcel-jibun:{evidence(old)[0].label}")
        if official:  # 2
            accept(old, official, "official:parcel-history", "official_parcel_history", how)
        elif by_land:  # 3
            accept(old, by_land, f"derived:parcel-jibun:{evidence(old)[0].label}", "jibun", f"jibun_share:{share:.4f}")

    def leaf_weight(code: str) -> int:
        if code not in weight:
            ev, _ = evidence(code)
            weight[code] = max(1, len(ev.before.get(code, {}))) if ev is not None else 1
        return weight[code]

    while True:
        before_round = len(result.pairs)
        unsettled = sorted((old for old in olds if old not in successors), key=order)
        for old in unsettled:  # 0 and 1, top-down
            if old in decided_by_old:
                for pair in decided_by_old[old]:
                    hold_against_official(old, pair["new_code"], pair["source"])
                    accept(old, pair["new_code"], pair["source"], pair.get("rule_verdict") or "steward", pair.get("detail") or "")
                continue
            candidates = rule_candidates(old)
            if len(candidates) == 1:
                source = f"derived:code-go-kr:date+name:{by_code[old]['abolished_date']}"
                hold_against_official(old, candidates[0], source)
                accept(old, candidates[0], source, "rule")
        for old in sorted((old for old in olds if old not in successors), key=order, reverse=True):
            level = code_level(old)
            if level in LEAF_LEVELS:
                settle_leaf(old)
                continue
            leaves = [code for code in abolished if code_level(code) in LEAF_LEVELS and _under(code, old)]
            total = sum(leaf_weight(code) for code in leaves)
            received: dict[str, int] = {}
            for code in leaves:
                for new in successors.get(code, ()):
                    received[_parent_at(new, level)] = received.get(_parent_at(new, level), 0) + leaf_weight(code)
            if total and received:  # 4
                parent, got = max(received.items(), key=lambda item: (item[1], item[0]))
                if parent != old and parent in alive and got / total >= min_share:
                    accept(old, parent, "derived:children", "rollup", f"children_share:{got / total:.4f}")
        if len(result.pairs) == before_round:
            break

    for old in sorted((old for old in olds if old not in successors), key=order):
        row = by_code.get(old)
        candidates = rule_candidates(old)
        best, share, unique = jibun_best(old)
        ev, waiting_for = evidence(old)
        before = ev.before.get(old, {}) if ev is not None else {}
        split_into = {
            new: held for new in jibun_candidates(old, ev) if (held := overlap(old, new, ev))
        } if ev is not None else {}
        missing = ""
        if ev is None:
            seen, status = waiting_for, "awaiting_data"
            missing = BELOW_WAIT.sub("", waiting_for)
        elif not before:
            seen, status = "no parcels before", "steward"
        elif not best:
            # Every 지번 left (renumbered), or none is the same land. Only the parcel-number
            # history can say where.
            seen = "no 지번 in any new code"
            status = "awaiting_data" if official_links is None else "steward"
            missing = OFFICIAL_HISTORY_MISSING if official_links is None else ""
        else:
            seen = f"best {best} share {share:.4f}" + ("" if unique else " (tied)")
            status = "split" if len(split_into) > 1 and unique else "steward"
        dong_split = dong_links_by_old.get(old, set())
        official_split = {new: 1 for new in dong_split} if dong_split else dict(links_by_old.get(old, {}))
        if len(official_split) > 1 and status != "split":
            # The official rows carry the old code into several 동 (its 동-level rows name several, or
            # its parcel rows go several ways): not one pair.
            seen, status, split_into, missing = f"{seen}; official history splits it", "split", official_split, ""
        elif official_split and status != "split" and not dong_split:
            # Parcel rows to one code that do not cover the old code's parcels: evidence of where those
            # parcels went, not a decision for the 동. It waits for the rest (or for a 동-level row).
            covered = parcel_coverage(old)
            partial = (f"official partial: {covered[0]} of {covered[1]} parcels linked" if covered
                       else "official partial: no edition says how many parcels it held")
            seen, status, missing = f"{seen}; {partial}", "awaiting_data", partial
        result.review.append(
            {
                "kind": "pair",
                "old_code": old,
                "level": code_level(old),
                "status": status,
                "reason": "ambiguous_name_match" if candidates else "no_candidate",
                "candidates": sorted(set(candidates) | ({best} if best else set())),
                "jibun": seen,
                "split_into": split_into,
                # The evidence an awaiting_data item waits for, one name per kind (which edition,
                # or the official record), for the daily counts.
                "waiting_for": missing,
                "as_of": row.get("abolished_date", "") if row else "",
                "name": row.get("full_name", "") if row else "",
            }
        )
    result.official_partial_moves = {old: n for old in sorted(olds, key=order) if (n := partial_moves(old))}
    return result


def land_sets(parcels: Iterable[tuple[str, str, float]], codes: Iterable[str] | None = None) -> dict[str, dict[str, Land]]:
    """{법정동 code: {lot: (지목, area m²)}} from a parcel snapshot's (PNU, 지목, area) rows, limited to
    `codes` when given; a malformed PNU is not a 지번."""

    wanted = set(codes) if codes is not None else None
    out: dict[str, dict[str, Land]] = {}
    for pnu, jimok, area in parcels:
        if pl.PNU_PATTERN.fullmatch(pnu or "") and (wanted is None or pnu[:10] in wanted):
            out.setdefault(pnu[:10], {})[pl.lot(pnu)] = (jimok or "", float(area))
    return out


# --- the 시군구 crosswalk the loaders read ------------------------------------------------------


def sigungu_crosswalk(
    pairs: Sequence[Mapping[str, str]], cadastral_sido: Iterable[str]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """The 시군구 crosswalk and the merged 시도 it governs, from the pairs, old → new like them.

    A 시도 is governed when a 시도-level pair moves a 시도 the cadastral parcel set still carries
    (`vworld-parcel-source-objects.json`) onto a 시도 it does not: the map keeps the old codes, so
    every hub code under the new 시도 must be carried back. Only 시군구 pairs from a governed 시도
    onto one of the 시도 it supersedes enter the crosswalk. A current code carried onto two old
    codes, or an old code split into two current ones, cannot be one crosswalk entry and goes to the
    steward list instead (both are returned: the crosswalk, and the review items).
    """

    cadastral = set(cadastral_sido)
    supersedes: dict[str, set[str]] = {}
    effective: dict[str, str] = {}
    for pair in pairs:
        if pair["level"] != "sido":
            continue
        old, new = pair["old_code"][:2], pair["new_code"][:2]
        if old in cadastral and new not in cadastral:
            supersedes.setdefault(new, set()).add(old)
            effective[new] = min(filter(None, (effective.get(new), pair["effective_date"])), default="")
    by_current: dict[str, list[Mapping[str, str]]] = {}
    by_old: dict[str, set[str]] = {}
    for pair in pairs:
        if pair["level"] != "sigungu":
            continue
        current, old = pair["new_code"][:5], pair["old_code"][:5]
        if current[:2] in supersedes and old[:2] in supersedes[current[:2]]:
            by_current.setdefault(current, []).append(pair)
            by_old.setdefault(old, set()).add(current)
    entries, review = [], []
    for current, found in sorted(by_current.items()):
        olds = sorted({pair["old_code"][:5] for pair in found})
        if len(olds) != 1 or len(by_old[olds[0]]) != 1:
            review.append({"kind": "crosswalk", "new_code": current, "reason": "not_one_to_one",
                           "old_codes": olds, "split_into": sorted(by_old[olds[0]]) if len(olds) == 1 else []})
            continue
        pair = sorted(found, key=lambda p: (not p["source"].startswith("official"), p["source"]))[0]
        entries.append(
            {"old_code": olds[0], "new_code": current, "effective_date": pair["effective_date"], "source": pair["source"]}
        )
    governed = [
        {"new_code": sido, "old_codes": sorted(olds), "effective_date": effective.get(sido, "")}
        for sido, olds in sorted(supersedes.items())
    ]
    return {"sido": governed, "sigungu": entries}, review


# --- the pending-load handoff -----------------------------------------------------------------


def _manifest_object(manifest: Mapping[str, Any], base: Path, role: str) -> tuple[Mapping[str, Any], bytes]:
    found = [obj for obj in manifest["objects"] if obj["role"] == role]
    if len(found) != 1:
        raise SourceFormatError(f"the collector manifest holds {len(found)} {role} objects, expected one")
    raw = (base / found[0]["local_path"]).read_bytes()
    if hashlib.sha256(raw).hexdigest() != found[0]["checksum_sha256"]:
        raise SourceFormatError(f"{found[0]['local_path']} differs from the sha256 Bronze holds for it")
    return found[0], raw


def stage_handoff(collect_dir: Path, state_dir: Path, contract: Mapping[str, Any], now: datetime) -> dict[str, Any]:
    """Validates a collection run without Spark and hands a changed table to the loader.

    Reads the collector manifest in `collect_dir`, refuses a full table whose shape changed
    (`SourceFormatError`) or that shrank past the contract's bounds from the last staged one
    (`TableShrunk`). When the table's rows differ from the last staged table it writes
    `state_dir/pending/<table object>/`: the manifest, the object, and `handoff.json`. The directory
    appears in one rename and is never rewritten. An unchanged run hands nothing off and says so.

    Either way a run that passed every check records when it did (`accepted.json`,
    `checked_at_utc`): the hub exports refuse a crosswalk whose table no collection has confirmed
    within the contract's `projection.max_age_days`.
    """

    manifest = json.loads((collect_dir / "manifest.json").read_text(encoding="utf-8"))
    table_obj, table_raw = _manifest_object(manifest, collect_dir, "full_table")
    rows = parse_full_table_html(table_raw.decode("utf-8"), contract)
    accepted_path = state_dir / "accepted.json"
    accepted = json.loads(accepted_path.read_text(encoding="utf-8")) if accepted_path.exists() else None
    check_table_size(len(rows), accepted["row_count"] if accepted else None, contract)
    digest = hashlib.sha256(json.dumps(rows, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
    result = {"table_object_key": table_obj["object_key"], "row_count": len(rows), "table_digest": digest,
              "previous_row_count": accepted["row_count"] if accepted else None}
    if accepted and accepted["table_digest"] == digest:
        _write_accepted(state_dir, {**accepted, "checked_at_utc": now.isoformat()})
        return {"status": "unchanged", **result}
    name = Path(table_obj["local_path"]).stem
    pending = state_dir / "pending" / name
    if pending.exists():
        raise FileExistsError(f"{pending} already exists; a handoff is never rewritten")
    staging = state_dir / "pending" / f".{name}.staging"
    shutil.rmtree(staging, ignore_errors=True)
    (staging / "objects").mkdir(parents=True)
    shutil.copy2(collect_dir / "manifest.json", staging / "manifest.json")
    shutil.copy2(collect_dir / table_obj["local_path"], staging / table_obj["local_path"])
    handoff = {"name": name, "snapshot_date": manifest["collection_date"], "staged_at_utc": now.isoformat(), **result}
    (staging / "handoff.json").write_text(json.dumps(handoff, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    staging.rename(pending)
    _write_accepted(state_dir, {"table_digest": digest, "row_count": len(rows), "table_object_key": table_obj["object_key"],
                                "handoff": name, "checked_at_utc": now.isoformat()})
    return {"status": "staged", "handoff": name, **result}


def _write_accepted(state_dir: Path, accepted: Mapping[str, Any]) -> None:
    """`accepted.json`: the last table handed off, and when a collection last confirmed it."""

    tmp = state_dir / ".accepted.json.tmp"
    tmp.write_text(json.dumps(dict(accepted), indent=2) + "\n", encoding="utf-8")
    tmp.replace(state_dir / "accepted.json")


# --- command line -----------------------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    """The collection job's native step (no Spark): `stage-handoff` validates the run and writes a
    pending-load handoff when the table changed (`stage_handoff`)."""

    import argparse  # noqa: PLC0415

    parser = argparse.ArgumentParser(description=main.__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    stage = commands.add_parser("stage-handoff")
    stage.add_argument("--collect-dir", required=True, help="The collector's output directory (manifest.json).")
    stage.add_argument("--state-dir", required=True, help="Holds accepted.json and pending/.")
    args = parser.parse_args(argv)
    result = stage_handoff(Path(args.collect_dir), Path(args.state_dir), load_source_contract(), datetime.now().astimezone())
    print("legal-dong-code-stage-json " + json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
