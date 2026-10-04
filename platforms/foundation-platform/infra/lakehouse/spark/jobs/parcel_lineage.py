"""Parcel lineage kernels (root ADR-0113): which old parcel became which new parcel, and on what evidence.

Everything here is pure Python over plain values so the lane that runs `infra/lakehouse/spark/tests`
(no PySpark) exercises every rule; `parcel_lineage_to_silver.py` only feeds these functions from
Silver and writes what they return.

Evidence is codes and official records only. Polygon overlap or containment is never evidence
(ADR-0113 §4): a pair that only the geometry would suggest stays `pending`.
"""

from __future__ import annotations

import collections
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Mapping, Sequence

RULES_VERSION = "parcel-lineage.v1"

# Grades, strongest first (ADR-0113 §4). The effective link of an old parcel is its highest grade.
GRADES = ("official", "code_derived", "evidence_strong", "evidence_weak", "needs_review", "pending")
GRADE_RANK = {grade: rank for rank, grade in enumerate(GRADES)}
RELATIONS = (
    "code_change",
    "jurisdiction_transfer",
    "registration_conversion",
    "merge",
    "split",
    "resurvey",
    "other",
)
# A dong whose lots survive under its new code below this share has been split, not renamed
# (ADR-0113 §5); only its vanished lots go on to the evidence rules. The 법정동 change pairing uses the
# same line to pair a dong by its 지번 set (ADR-0144), so the value lives in its contract.
SPLIT_SIGNAL_OVERLAP = float(
    json.loads(
        (Path(__file__).resolve().parents[2] / "contracts" / "code-go-kr-legal-dong.contract.json").read_text(encoding="utf-8")
    )["pairing"]["jibun_overlap_min_share"]
)
EXISTS, ABOLISHED = "존재", "폐지"
# `evidence_kind` of a link read from the 필지고유번호변동연혁 (ADR-0113 §4; graded `evidence_strong`,
# ADR-0144 §3.3). Nothing writes it yet: the source is not collected. The 법정동 change pairing reads
# only these links, because the history-text links are written through this module's own dong
# pairing and would only echo it.
PARCEL_NUMBER_HISTORY = "parcel_number_history"
PNU_PATTERN = re.compile(r"[0-9]{19}")
LOT_PATTERN = re.compile(r"(산\s*)?([0-9]{1,4})(?:-([0-9]{1,4}))?")


@dataclass(frozen=True)
class Link:
    predecessor_pnu: str
    successor_pnu: str
    relation: str
    grade: str
    evidence_kind: str
    evidence_ref: str = ""
    effective_date: str = ""


@dataclass
class DongPairing:
    """Old legal dong -> new legal dong for one snapshot pair, with how each pair was decided."""

    pairs: dict[str, str] = field(default_factory=dict)
    how: dict[str, str] = field(default_factory=dict)
    lot_overlap: dict[str, float] = field(default_factory=dict)
    unpaired: list[str] = field(default_factory=list)

    def to_new(self, pnu: str) -> str:
        return self.pairs.get(pnu[:10], pnu[:10]) + pnu[10:]

    def to_old(self, pnu: str) -> str:
        """The number the same lot had at the earlier snapshot, when its dong was renamed."""

        reverse = getattr(self, "_reverse", None)
        if reverse is None:
            reverse = {new: old for old, new in self.pairs.items() if new != old}
            self._reverse = reverse
        return reverse.get(pnu[:10], pnu[:10]) + pnu[10:]

    def split_signals(self) -> list[str]:
        return sorted(c for c, share in self.lot_overlap.items() if share < SPLIT_SIGNAL_OVERLAP)


# --- the official code list ------------------------------------------------------------------


def parse_code_list(text: str) -> dict[str, tuple[str, str]]:
    """The 행정표준코드 법정동코드 전체자료 (code<TAB>full name<TAB>존재|폐지) as {code: (name, status)}.

    The file carries no dates and no successor codes; everything about change comes from comparing
    two of these snapshots. A malformed row is refused rather than skipped: a silently dropped code
    later reads as an abolished one.
    """
    rows: dict[str, tuple[str, str]] = {}
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines or not lines[0].startswith("법정동코드"):
        raise ValueError("not a 법정동코드 전체자료 file: the header row is missing")
    for number, line in enumerate(lines[1:], start=2):
        parts = line.split("\t")
        if len(parts) != 3:
            raise ValueError(f"line {number}: expected code, name, status; got {len(parts)} fields")
        code, name, status = (part.strip() for part in parts)
        if not re.fullmatch(r"[0-9]{10}", code):
            raise ValueError(f"line {number}: {code!r} is not a 10-digit 법정동코드")
        if status not in (EXISTS, ABOLISHED):
            raise ValueError(f"line {number}: status {status!r} is neither {EXISTS} nor {ABOLISHED}")
        if code in rows:
            raise ValueError(f"line {number}: {code} appears twice")
        rows[code] = (name, status)
    return rows


def _leaf(full_name: str) -> tuple[str, ...]:
    """The name below the sido: a merger renames the sido, never the sigungu and dong under it."""

    return tuple(full_name.split()[1:])


def pair_legal_dongs(
    codes: Mapping[str, tuple[str, str]],
    old_dongs: Iterable[str],
    lots_before: Mapping[str, set[str]],
    lots_after: Mapping[str, set[str]],
) -> DongPairing:
    """Pair each old legal dong with the dong that carries it now (ADR-0113 §5).

    `codes` is the code list at the later snapshot. A dong that still exists maps to itself. An
    abolished one is matched only among the dongs that newly hold parcels in the later snapshot and
    exist in the code list — dong names repeat across the country, so a nationwide search pairs
    Incheon's 중앙동1가 with Busan's. Among those: same sigungu and dong name under the new sido,
    else the one new dong of that name, else the candidate that holds more of its lot numbers. Lot
    overlap is recorded for every pair so a split shows up as a low share.
    """

    pairing = DongPairing()
    old = sorted(set(old_dongs))
    alive = {c for c, (_, s) in codes.items() if s == EXISTS}
    newly_held = (set(lots_after) - set(lots_before)) & alive
    by_leaf: dict[tuple[str, ...], list[str]] = collections.defaultdict(list)
    by_name: dict[str, list[str]] = collections.defaultdict(list)
    for c in newly_held:
        name = codes[c][0]
        by_leaf[_leaf(name)].append(c)
        by_name[name.split()[-1]].append(c)
    for c in old:
        if c in alive:
            pairing.pairs[c], pairing.how[c] = c, "unchanged"
            continue
        name = codes.get(c, ("", ""))[0]
        candidates = by_leaf.get(_leaf(name), []) if name else []
        how = "sigungu+dong name"
        if len(candidates) != 1:
            candidates = by_name.get(name.split()[-1], []) if name else []
            how = "dong name"
        if len(candidates) > 1:
            before = lots_before.get(c, set())
            scored = sorted(((len(before & lots_after.get(n, set())), n) for n in candidates), reverse=True)
            if scored and scored[0][0] > 0 and (len(scored) == 1 or scored[0][0] > scored[1][0]):
                candidates, how = [scored[0][1]], "lot overlap"
        if len(candidates) != 1:
            pairing.unpaired.append(c)
            continue
        pairing.pairs[c], pairing.how[c] = candidates[0], how
    for c, n in pairing.pairs.items():
        before = lots_before.get(c, set())
        if before:
            pairing.lot_overlap[c] = len(before & lots_after.get(n, set())) / len(before)
    return pairing


# --- parcels that kept their lot number -------------------------------------------------------


def lot(pnu: str) -> str:
    """Everything after the legal dong: ledger kind + main + sub number."""

    return pnu[10:]


def lots_by_dong(pnus: Iterable[str]) -> dict[str, set[str]]:
    out: dict[str, set[str]] = collections.defaultdict(set)
    for p in pnus:
        out[p[:10]].add(lot(p))
    return out


def carry_over(before: set[str], after: set[str], pairing: DongPairing) -> tuple[list[Link], set[str], set[str]]:
    """Links for parcels whose lot survives under the paired dong; returns (links, vanished, appeared).

    A parcel whose dong code did not change is not a lineage event and gets no row: the lineage
    records changes, the registry records presence.
    """

    links: list[Link] = []
    kept: set[str] = set()
    reached: set[str] = set()
    for p in before:
        q = pairing.to_new(p)
        if q in after:
            kept.add(p)
            reached.add(q)
            if q != p:
                links.append(Link(p, q, "code_change", "code_derived", "same_lot_paired_dong", pairing.how.get(p[:10], "")))
    return links, before - kept, after - reached


# --- the land movement history ----------------------------------------------------------------


@dataclass(frozen=True)
class MovementEvent:
    pnu: str
    reason_code: str
    reason: str
    moved_at: str
    erased_at: str
    land_category_code: str = ""
    area_m2: str = ""


def lot_ref_to_pnu(dong: str, ref: str) -> str | None:
    """`산 27-1` / `454-30` / `111` inside a history text, in the dong the text belongs to."""

    m = LOT_PATTERN.fullmatch(ref.strip())
    if not m:
        return None
    ledger = "2" if m.group(1) else "1"
    return f"{dong}{ledger}{int(m.group(2)):04d}{int(m.group(3) or 0):04d}"


MERGED_AWAY = re.compile(r"^(산\s*[0-9]+(?:-[0-9]+)?|[0-9]+(?:-[0-9]+)?)\s*번과\s*합병되어\s*말소")
SPLIT_FROM = re.compile(r"^(산\s*[0-9]+(?:-[0-9]+)?|[0-9]+(?:-[0-9]+)?)\s*번에서\s*분할")
CONVERTED_FROM = re.compile(r"^(산\s*[0-9]+(?:-[0-9]+)?)\s*번에서\s*등록전환")
JURISDICTION_TRANSFER_CODE = "52"


def history_links(
    events: Iterable[MovementEvent],
    since: str,
    until: str,
    pairing: DongPairing | None = None,
) -> list[Link]:
    """Official links the history text states, for events dated in [since, until).

    "62번과 합병되어 말소" on a closed parcel names the lot it merged into; "454-30번에서 분할" on a
    new parcel names its parent; "산 27-1번에서 등록전환" names the mountain-ledger lot it came
    from. The referenced lot is in the same legal dong, so the text is enough to build its PNU.

    `since` is the earlier snapshot's date: an event before it is already in that snapshot and
    would be counted twice. The history is written under the codes of its own time, so with a
    `pairing` each end is written the way the snapshot it belongs to spells it — a predecessor as
    the earlier snapshot's number, a successor as the later one's.
    """

    # One spelling per end, whether or not that parcel happens to be in the snapshot: a lot that was
    # created and closed between the two snapshots is written in the same code system as the rest,
    # so the same event reported under the old and the new code collapses to one link.
    def as_before(pnu: str) -> str:
        return pairing.to_old(pnu) if pairing is not None else pnu

    def as_after(pnu: str) -> str:
        return pairing.to_new(pnu) if pairing is not None else pnu

    links: list[Link] = []
    for e in events:
        if not (since <= e.moved_at < until):
            continue
        dong = e.pnu[:10]
        text = e.reason.strip()
        if m := MERGED_AWAY.match(text):
            target = lot_ref_to_pnu(dong, m.group(1))
            if target and target != e.pnu:
                links.append(Link(as_before(e.pnu), as_after(target), "merge", "official", "history_text", text, e.moved_at))
        elif m := SPLIT_FROM.match(text):
            parent = lot_ref_to_pnu(dong, m.group(1))
            if parent and parent != e.pnu:
                links.append(Link(as_before(parent), as_after(e.pnu), "split", "official", "history_text", text, e.moved_at))
        elif m := CONVERTED_FROM.match(text):
            source = lot_ref_to_pnu(dong, m.group(1))
            if source and source != e.pnu:
                links.append(
                    Link(as_before(source), as_after(e.pnu), "registration_conversion", "official", "history_text", text, e.moved_at)
                )
    return sorted(set(links), key=lambda link: (link.predecessor_pnu, link.successor_pnu, link.relation))


def unexplained(vanished: set[str], links: Sequence[Link]) -> set[str]:
    """Vanished parcels no official or code-derived link accounts for: the attribute-evidence pool."""

    explained = {link.predecessor_pnu for link in links if GRADE_RANK[link.grade] <= GRADE_RANK["code_derived"]}
    return vanished - explained


def partition_valid(pnus: Iterable[str]) -> tuple[set[str], set[str]]:
    """(well-formed 19-digit PNUs, malformed source values) — a malformed value is reported, not paired."""

    good, bad = set(), set()
    for p in pnus:
        (good if PNU_PATTERN.fullmatch(p) else bad).add(p)
    return good, bad


def transferred_in(events: Iterable[MovementEvent], since: str, until: str) -> set[str]:
    """New parcels the history marks as moved in by a jurisdiction change (사유 52) in the window."""

    return {e.pnu for e in events if e.reason_code == JURISDICTION_TRANSFER_CODE and since <= e.moved_at < until}


# --- building register continuity -------------------------------------------------------------


def building_links(before: Mapping[str, str], after: Mapping[str, str], vanished: set[str], appeared: set[str]) -> list[Link]:
    """A building keeps its register key while its lot is renumbered: key -> (old PNU, new PNU).

    Only links a vanished parcel to an appeared one, and only when every building on the old
    parcel points at the same new parcel — two buildings disagreeing means the pair is not known.
    """

    targets: dict[str, set[str]] = collections.defaultdict(set)
    for key, old in before.items():
        new = after.get(key)
        if new and old in vanished and new in appeared:
            targets[old].add(new)
    return [
        Link(old, next(iter(news)), "jurisdiction_transfer", "code_derived", "building_register_key")
        for old, news in sorted(targets.items())
        if len(news) == 1
    ]


# --- attribute evidence -----------------------------------------------------------------------


@dataclass(frozen=True)
class ParcelFacts:
    area_m2: str
    land_category_code: str
    owner_kind: str | None = None
    co_owner_count: str | None = None


def _area(value: str) -> str:
    try:
        return f"{float(value):.1f}"
    except (TypeError, ValueError):
        return ""


def attribute_links(old: Mapping[str, ParcelFacts], new: Mapping[str, ParcelFacts], relation: str) -> list[Link]:
    """Pairs whose official attributes single each other out (ADR-0113 §4).

    evidence_strong: area, category, owner kind and co-owner count all equal, unique on both sides.
    evidence_weak:   area and category unique on both sides, ownership unknown on the old side, or
                     ownership was what broke an area+category tie.
    needs_review:    area and category unique on both sides but ownership differs.
    A new parcel matched by none of these gets a pending row, so it is listed, not forgotten.
    """

    def base(f: ParcelFacts) -> tuple[str, str]:
        return (_area(f.area_m2), f.land_category_code)

    def owner(f: ParcelFacts) -> tuple[str, str] | None:
        if f.owner_kind is None:
            return None
        return (f.owner_kind, (f.co_owner_count or "0").lstrip("0") or "0")

    by_old: dict[tuple[str, str], list[str]] = collections.defaultdict(list)
    by_new: dict[tuple[str, str], list[str]] = collections.defaultdict(list)
    for p, f in old.items():
        if base(f)[0]:
            by_old[base(f)].append(p)
    for p, f in new.items():
        if base(f)[0]:
            by_new[base(f)].append(p)
    links: list[Link] = []
    paired: set[str] = set()
    for key, news in by_new.items():
        olds = by_old.get(key, [])
        if len(news) == 1 and len(olds) == 1:
            n, o = news[0], olds[0]
            on, oo = owner(new[n]), owner(old[o])
            if oo is None or on is None:
                grade, kind = "evidence_weak", "area+category"
            elif on == oo:
                grade, kind = "evidence_strong", "area+category+ownership"
            else:
                grade, kind = "needs_review", "area+category, ownership differs"
            links.append(Link(o, n, relation, grade, kind))
            paired.add(n)
            continue
        for n in news:
            on = owner(new[n])
            if on is None:
                continue
            rivals = [m for m in news if owner(new[m]) == on]
            cands = [o for o in olds if owner(old[o]) == on]
            if len(rivals) == 1 and len(cands) == 1:
                links.append(Link(cands[0], n, relation, "evidence_weak", "area+category, ownership tie-break"))
                paired.add(n)
    for n in sorted(set(new) - paired):
        links.append(Link("", n, relation, "pending", "no unique attribute match"))
    return links


def lot_key(pnu: str) -> tuple[str, str, int, int]:
    """Order of lots inside a dong: ledger kind, main number, sub number."""

    return (pnu[:10], pnu[10], int(pnu[11:15]), int(pnu[15:19]))


def lot_order_links(
    anchors: Mapping[str, str],
    unmatched_new: Iterable[str],
    unmatched_old: Iterable[str],
    new_facts: Mapping[str, ParcelFacts],
    old_facts: Mapping[str, ParcelFacts],
    relation: str,
) -> list[Link]:
    """Pair re-lotted parcels by their place between anchored neighbours (ADR-0113 §4).

    When land is transferred and re-lotted, the new lots are issued in the order of the old ones.
    `anchors` (new -> old, code_derived or evidence_strong) fix points of that order. The rule is
    used for a (new dong, old dong) pair only if the anchors never invert the order; then, inside
    each gap between consecutive anchors:
      - equal counts on both sides whose area and category agree pairwise in order -> all pair;
      - otherwise a new parcel pairs with the one old parcel of the gap sharing its (area, category),
        when that pair of values occurs exactly once on each side.
    Everything paired is area- and category-equal and bounded by official neighbours.
    """

    def facts_key(f: ParcelFacts | None) -> tuple[str, str] | None:
        if f is None or not _area(f.area_m2):
            return None
        return (_area(f.area_m2), f.land_category_code)

    groups: dict[tuple[str, str], list[tuple[str, str]]] = collections.defaultdict(list)
    for new, old in anchors.items():
        groups[(new[:10], old[:10])].append((new, old))
    news_by_dong: dict[str, list[str]] = collections.defaultdict(list)
    for p in unmatched_new:
        news_by_dong[p[:10]].append(p)
    olds_by_dong: dict[str, list[str]] = collections.defaultdict(list)
    for p in unmatched_old:
        olds_by_dong[p[:10]].append(p)
    old_dongs_of_new = collections.defaultdict(set)
    for new_dong, old_dong in groups:
        old_dongs_of_new[new_dong].add(old_dong)

    links: list[Link] = []
    for (new_dong, old_dong), pairs in sorted(groups.items()):
        if len(old_dongs_of_new[new_dong]) != 1:
            continue  # two source dongs feed this dong: position alone cannot say which
        pairs.sort(key=lambda item: lot_key(item[0]))
        olds_in_order = [lot_key(o) for _, o in pairs]
        if any(b < a for a, b in zip(olds_in_order, olds_in_order[1:])):
            continue  # the anchors do not keep the order: the premise fails here
        bounds: list[tuple[str | None, str | None]] = [(None, None), *pairs, (None, None)]
        news = sorted(news_by_dong.get(new_dong, []), key=lot_key)
        olds = sorted(olds_by_dong.get(old_dong, []), key=lot_key)
        for (n0, o0), (n1, o1) in zip(bounds, bounds[1:]):
            gap_new = [p for p in news if (n0 is None or lot_key(p) > lot_key(n0)) and (n1 is None or lot_key(p) < lot_key(n1))]
            gap_old = [p for p in olds if (o0 is None or lot_key(p) > lot_key(o0)) and (o1 is None or lot_key(p) < lot_key(o1))]
            if not gap_new or not gap_old:
                continue
            if len(gap_new) == len(gap_old) and all(
                facts_key(new_facts.get(n)) is not None and facts_key(new_facts.get(n)) == facts_key(old_facts.get(o))
                for n, o in zip(gap_new, gap_old)
            ):
                chosen = list(zip(gap_new, gap_old))
            else:
                new_by_key: dict[tuple[str, str], list[str]] = collections.defaultdict(list)
                old_by_key: dict[tuple[str, str], list[str]] = collections.defaultdict(list)
                for n in gap_new:
                    if (k := facts_key(new_facts.get(n))) is not None:
                        new_by_key[k].append(n)
                for o in gap_old:
                    if (k := facts_key(old_facts.get(o))) is not None:
                        old_by_key[k].append(o)
                chosen = sorted(
                    (new_by_key[k][0], old_by_key[k][0])
                    for k in new_by_key
                    if len(new_by_key[k]) == 1 and len(old_by_key.get(k, [])) == 1
                )
                matched_old = [lot_key(o) for _, o in sorted(chosen, key=lambda item: lot_key(item[0]))]
                if any(b < a for a, b in zip(matched_old, matched_old[1:])):
                    chosen = []  # the unique matches themselves would invert the order: take none
            links.extend(Link(o, n, relation, "evidence_strong", "lot order+area+category") for n, o in chosen)
    return links


# --- effective link and reconciliation ------------------------------------------------------------


# Relations that claim "the same land under another number": a successor has exactly one such
# predecessor. Merges and splits legitimately give a parcel several predecessors or successors.
IDENTITY_RELATIONS = frozenset({"code_change", "jurisdiction_transfer", "registration_conversion"})


def effective_links(links: Sequence[Link]) -> tuple[dict[str, Link], list[tuple[Link, Link]]]:
    """The highest-grade link per successor, and every identity claim that disagrees with a stronger one.

    Two links disagree when both claim the successor is the same land as a different predecessor.
    A merge naming a second predecessor is not a disagreement.
    """

    best: dict[str, Link] = {}
    identity: dict[str, Link] = {}
    conflicts: list[tuple[Link, Link]] = []
    for link in sorted(links, key=lambda item: GRADE_RANK[item.grade]):
        best.setdefault(link.successor_pnu, link)
        if link.relation not in IDENTITY_RELATIONS or not link.predecessor_pnu:
            continue
        stronger = identity.get(link.successor_pnu)
        if stronger is None:
            identity[link.successor_pnu] = link
        elif stronger.predecessor_pnu != link.predecessor_pnu:
            conflicts.append((stronger, link))
    return best, conflicts


@dataclass
class Reconciliation:
    before: int
    after: int
    kept: int
    vanished: int
    appeared: int
    vanished_explained: int
    appeared_explained: int

    @property
    def balanced(self) -> bool:
        return self.before - self.vanished + self.appeared == self.after

    def as_dict(self) -> dict[str, int | bool]:
        return {**self.__dict__, "balanced": self.balanced}


def reconcile(before: set[str], after: set[str], vanished: set[str], appeared: set[str], links: Sequence[Link]) -> Reconciliation:
    """`before − vanished + appeared = after`, and how many of the changes a non-pending link explains."""

    explained_old = {link.predecessor_pnu for link in links if link.grade != "pending" and link.predecessor_pnu}
    explained_new = {link.successor_pnu for link in links if link.grade != "pending"}
    return Reconciliation(
        before=len(before),
        after=len(after),
        kept=len(before) - len(vanished),
        vanished=len(vanished),
        appeared=len(appeared),
        vanished_explained=len(vanished & explained_old),
        appeared_explained=len(appeared & explained_new),
    )


def cardinality(links: Sequence[Link]) -> dict[tuple[str, str], str]:
    """1:1, 1:N, N:1 or N:M for each (predecessor, successor) among the non-pending links."""

    real = [link for link in links if link.grade != "pending" and link.predecessor_pnu]
    outs = collections.Counter(link.predecessor_pnu for link in real)
    ins = collections.Counter(link.successor_pnu for link in real)
    result = {}
    for link in real:
        many_out, many_in = outs[link.predecessor_pnu] > 1, ins[link.successor_pnu] > 1
        result[(link.predecessor_pnu, link.successor_pnu)] = {
            (False, False): "1:1",
            (True, False): "1:N",
            (False, True): "N:1",
            (True, True): "N:M",
        }[(many_out, many_in)]
    return result


def valid_pnu(pnu: str) -> bool:
    return bool(PNU_PATTERN.fullmatch(pnu))
