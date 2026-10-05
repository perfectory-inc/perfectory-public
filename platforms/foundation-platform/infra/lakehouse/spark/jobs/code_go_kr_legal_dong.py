"""Parser and pairing kernels for the 법정동 code change source (root ADR-0143, ADR-0144).

The collector (`collect-code-go-kr-legal-dong` in the outbox publisher) lands the code.go.kr 법정동
전체 표 in Bronze unchanged: one HTML table, every code current and abolished, with its parent and
its 생성일·폐지일. Everything that reads those bytes lives here, in plain Python over plain values, so
the test lane that runs `infra/lakehouse/spark/tests` (no PySpark, standard library only) exercises
every rule:

- the full table, refused whole when its shape changed;
- pairing, from downloaded data only (ADR-0144; the site's 코드변경안내 notice board is
  not a source): the date + name rule, then the 지번 sets of two parcel snapshots (ADR-0113 §5),
  then the official parcel-number history, and everything else to the steward;
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


@dataclass
class JibunEvidence:
    """The 지번 sets (ledger kind + 본번 + 부번, `parcel_lineage.lot`) each 법정동 held in two parcel
    snapshots, one taken before the change and one after; `label` names the pair in the source."""

    before: Mapping[str, set[str]]
    after: Mapping[str, set[str]]
    label: str


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


def pair_changes(
    rows: Sequence[Mapping[str, str]],
    floor_date: str,
    decided: Sequence[Mapping[str, str]] = (),
    jibun: JibunEvidence | EditionEvidence | None = None,
    official_links: Iterable[tuple[str, str]] | None = None,
    min_share: float = pl.SPLIT_SIGNAL_OVERLAP,
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
    2. **지번 sets** (`derived:parcel-jibun:<snapshots>`), for what 1 cannot settle (a renamed,
       split or merged 동): of the codes at its level that newly hold parcels in the later snapshot,
       lie in its 시도 or one a 시도 pair carries it onto, and were created in its change window
       (its 폐지일 or the day after), the one holding the largest share of the old code's 지번, when that share is at least `min_share`
       (the contract's `pairing.jibun_overlap_min_share`, ADR-0113 §5) and no other code holds as
       many. Off when `jibun` is None. With `EditionEvidence` each code is read from the editions
       that bracket its own change; a code whose editions are not held waits for them.
    3. **Official parcel-number history** (`official:parcel-history`): where the
       필지고유번호변동연혁 links (`official_links`, (old PNU, new PNU)) carry every linked parcel of
       the old code into one new code. This is how a code whose 지번 were renumbered is settled.
       Off when `official_links` is None.
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

    by_code = {row["region_cd"]: row for row in rows}
    alive = {code for code, row in by_code.items() if row["status"] == EXISTS}
    created: dict[str, list[Mapping[str, str]]] = {}
    for row in rows:
        if row.get("created_date", "") >= floor_date:
            created.setdefault(row["created_date"], []).append(row)
    decided_by_old: dict[str, list[Mapping[str, str]]] = {}
    for pair in decided:
        decided_by_old.setdefault(pair["old_code"], []).append(pair)
    links_by_old: dict[str, set[str]] = {}
    for old_pnu, new_pnu in official_links or ():
        if old_pnu[:10] != new_pnu[:10]:
            links_by_old.setdefault(old_pnu[:10], set()).add(new_pnu[:10])

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

    def jibun_candidates(old: str, ev: JibunEvidence) -> list[str]:
        """The codes the 지번 step may weigh for `old`: at its level, newly holding parcels, in its
        own or its successor 시도, and created in its change window (its 폐지일 or the day after,
        as the name rule reads it). Without the last two, any code created since the floor whose lot
        range covers `old`'s, a 리 across the country with 본번 1–2000, outscores its real successor."""

        row = by_code.get(old)
        day = row.get("abolished_date", "") if row else ""
        if not day:
            return []
        window, scope, level = {day, _next_day(day)}, sido_scope(old), code_level(old)
        return sorted(
            new for new in newly_held(ev)
            if code_level(new) == level and new[:2] in scope and by_code[new].get("created_date", "") in window
        )

    def jibun_best(old: str) -> tuple[str, float, bool]:
        """(best new code, its share of the old code's 지번, whether it is the only best)."""

        ev, _ = evidence(old)
        before = ev.before.get(old, set()) if ev is not None else set()
        if not before:
            return "", 0.0, False
        scored = sorted(
            ((len(before & ev.after.get(new, set())), new) for new in jibun_candidates(old, ev)),
            reverse=True,
        )
        if not scored or scored[0][0] == 0:
            return "", 0.0, False
        unique = len(scored) == 1 or scored[0][0] > scored[1][0]
        return scored[0][1], scored[0][0] / len(before), unique

    def leaf_weight(code: str) -> int:
        if code not in weight:
            ev, _ = evidence(code)
            weight[code] = max(1, len(ev.before.get(code, set()))) if ev is not None else 1
        return weight[code]

    while True:
        before_round = len(result.pairs)
        unsettled = sorted((old for old in olds if old not in successors), key=order)
        for old in unsettled:  # 0 and 1, top-down
            if old in decided_by_old:
                for pair in decided_by_old[old]:
                    accept(old, pair["new_code"], pair["source"], pair.get("rule_verdict") or "steward", pair.get("detail") or "")
                continue
            candidates = rule_candidates(old)
            if len(candidates) == 1:
                accept(old, candidates[0], f"derived:code-go-kr:date+name:{by_code[old]['abolished_date']}", "rule")
        for old in sorted((old for old in olds if old not in successors), key=order, reverse=True):
            level = code_level(old)
            if level in LEAF_LEVELS:
                best, share, unique = jibun_best(old)
                if best and unique and share >= min_share:  # 2
                    accept(old, best, f"derived:parcel-jibun:{evidence(old)[0].label}", "jibun", f"jibun_share:{share:.4f}")
                    continue
                news = links_by_old.get(old, set())
                if len(news) == 1 and news <= alive:  # 3
                    accept(old, next(iter(news)), "official:parcel-history", "official_parcel_history")
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
        before = ev.before.get(old, set()) if ev is not None else set()
        split_into = {
            new: len(before & ev.after.get(new, set()))
            for new in jibun_candidates(old, ev)
            if before & ev.after.get(new, set())
        } if ev is not None else {}
        if ev is None:
            seen, status = waiting_for, "awaiting_data"
        elif not before:
            seen, status = "no parcels before", "steward"
        elif not best:
            # Every 지번 left: renumbered. Only the parcel-number history can say where.
            seen = "no 지번 in any new code"
            status = "awaiting_data" if official_links is None else "steward"
        else:
            seen = f"best {best} share {share:.4f}" + ("" if unique else " (tied)")
            status = "split" if len(split_into) > 1 and unique else "steward"
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
                "as_of": row.get("abolished_date", "") if row else "",
                "name": row.get("full_name", "") if row else "",
            }
        )
    return result


def jibun_sets(pnus: Iterable[str], codes: Iterable[str] | None = None) -> dict[str, set[str]]:
    """{법정동 code: its 지번 set} from a parcel snapshot's PNUs (`parcel_lineage.lots_by_dong`),
    limited to `codes` when given; a malformed PNU is not a 지번."""

    wanted = set(codes) if codes is not None else None
    good, _ = pl.partition_valid(pnus)
    return {code: lots for code, lots in pl.lots_by_dong(good).items() if wanted is None or code in wanted}


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
