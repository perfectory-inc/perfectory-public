"""Which older PNU an attribute may come from when the map's PNU holds none (root ADR-0113 §6).

Attributes (price, characteristics, zoning, ...) arrive under the PNU of their own base date. After a
renumbering the map shows the new PNU and the value still sits under the old one. The lineage says
which old PNU is the same land (or where the land came from), so the value can follow it.

For every successor PNU this returns its candidate sources, nearest first, each with the path the
serving row records:

  lineage:<grade>          every hop is an identity relation (code change, jurisdiction transfer,
                           registration conversion) — the same land under an older number;
  origin:<relation>:<grade> the chain crosses a split, resurvey or other re-lotting — the value is the
                           original parcel's, and the screen must say so.

<grade> is the weakest grade along the chain. A merge is never crossed: the merged land has several
originals and none of their values is its value. `needs_review` and `pending` links are never
crossed either (§4: they are not in effect). The own PNU's value always wins; a candidate is used
only for a section the own PNU does not hold. Pure Python; the panel job applies it in Spark.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Iterable

from parcel_lineage import GRADE_RANK, GRADES, IDENTITY_RELATIONS, Link

# The strongest grade that is not in effect (§4); links at or below it are never crossed.
NOT_IN_EFFECT = frozenset({"needs_review", "pending"})
MAX_HOPS = 8


@dataclass(frozen=True)
class Candidate:
    successor_pnu: str
    source_pnu: str
    hop: int
    path: str


def _best_links(links: Iterable[Link]) -> dict[tuple[str, str], Link]:
    """One link per (predecessor, successor): the highest grade. Reruns of the derivation append."""

    best: dict[tuple[str, str], Link] = {}
    for link in links:
        if not link.predecessor_pnu or link.predecessor_pnu == link.successor_pnu:
            continue
        key = (link.predecessor_pnu, link.successor_pnu)
        if key not in best or GRADE_RANK[link.grade] < GRADE_RANK[best[key].grade]:
            best[key] = link
    return best


def carry_candidates(links: Iterable[Link]) -> tuple[list[Candidate], Counter[str]]:
    """Every successor's candidate sources, nearest first, and why chains stopped.

    Reasons: `merged` (several predecessors), `not_in_effect` (needs_review / pending),
    `too_long` (more than MAX_HOPS), `cycle`.
    """

    incoming: dict[str, list[Link]] = defaultdict(list)
    for link in _best_links(links).values():
        incoming[link.successor_pnu].append(link)

    stopped: Counter[str] = Counter()
    candidates: list[Candidate] = []
    for successor in sorted(incoming):
        pnu, weakest, crossed, seen = successor, 0, None, {successor}
        for hop in range(1, MAX_HOPS + 2):
            preds = incoming.get(pnu, [])
            if not preds:
                break
            if hop > MAX_HOPS:
                stopped["too_long"] += 1
                break
            if len({p.predecessor_pnu for p in preds}) > 1 or preds[0].relation == "merge":
                stopped["merged"] += 1
                break
            link = preds[0]
            if link.grade in NOT_IN_EFFECT:
                stopped["not_in_effect"] += 1
                break
            if link.predecessor_pnu in seen:
                stopped["cycle"] += 1
                break
            seen.add(link.predecessor_pnu)
            weakest = max(weakest, GRADE_RANK[link.grade])
            if crossed is None and link.relation not in IDENTITY_RELATIONS:
                crossed = link.relation
            grade = GRADES[weakest]
            path = f"lineage:{grade}" if crossed is None else f"origin:{crossed}:{grade}"
            candidates.append(Candidate(successor, link.predecessor_pnu, hop, path))
            pnu = link.predecessor_pnu
    return candidates, stopped


def attach(
    own: dict[str, object],
    held: dict[str, object],
    candidates: Iterable[Candidate],
) -> dict[str, tuple[object, str]]:
    """Reference semantics of one section, for tests and small runs: map PNU -> (value, path).

    `own` holds the map's PNUs that have a value under their own number; `held` every source PNU
    with a value. The Spark path in the panel job must produce the same result.
    """

    result: dict[str, tuple[object, str]] = {pnu: (value, "same_pnu") for pnu, value in own.items()}
    for c in sorted(candidates, key=lambda c: (c.successor_pnu, c.hop)):
        if c.successor_pnu in result or c.source_pnu not in held:
            continue
        result[c.successor_pnu] = (held[c.source_pnu], c.path)
    return result
