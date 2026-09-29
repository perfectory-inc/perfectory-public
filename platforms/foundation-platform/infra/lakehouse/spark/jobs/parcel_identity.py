"""Stable parcel ids and the bitemporal parcel registry (root ADR-0113 §2·§3).

A parcel is identified by an opaque `parcel_id`; the PNU is an attribute that can change. The id
survives a change that keeps the land (renumbering, jurisdiction transfer, ledger conversion) when
the lineage proves it strongly enough, and is issued afresh when the land itself changes (merge,
split, resurvey) or the proof is weak. A fresh id that a later, stronger link shows to be the same
land is not deleted: it is redirected to the id it should have kept. Ids are never reused.

Pure Python over plain values; `parcel_registry_to_silver.py` feeds it from the lakehouse.
"""

from __future__ import annotations

import collections
import uuid
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from parcel_lineage import GRADE_RANK, IDENTITY_RELATIONS, Link

ID_RULES_VERSION = "parcel-identity.v1"
# The weakest grade that may carry an id across a number change (ADR-0113 §2).
CARRY_MIN_GRADE = "evidence_strong"
CURRENT, HISTORIC, REDIRECTED = "current", "historic", "redirected"


def parcel_id_for(first_pnu: str, first_seen: str) -> str:
    """Deterministic: re-deriving the registry from the same history gives the same ids. The first-
    seen date is part of the seed because a PNU can be issued again after its parcel is closed."""

    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"parcel:{first_pnu}:{first_seen}"))


@dataclass(frozen=True)
class RegistryRow:
    parcel_id: str
    pnu: str
    status: str
    valid_from: str
    valid_to: str | None = None
    redirect_to: str | None = None


def bootstrap(pnus: Iterable[str], snapshot_date: str) -> list[RegistryRow]:
    """The first snapshot: every parcel gets its id, current from that date."""

    return [RegistryRow(parcel_id_for(p, snapshot_date), p, CURRENT, snapshot_date) for p in sorted(set(pnus))]


def carrying_links(effective: Mapping[str, Link]) -> dict[str, str]:
    """successor PNU -> predecessor PNU for the links strong enough to carry an id.

    Only identity relations, only one predecessor per successor and one successor per predecessor
    (a 1:N identity claim is not "the same land"), and only at CARRY_MIN_GRADE or stronger.
    """

    candidates = {
        s: link.predecessor_pnu
        for s, link in effective.items()
        if link.relation in IDENTITY_RELATIONS
        and link.predecessor_pnu
        and GRADE_RANK[link.grade] <= GRADE_RANK[CARRY_MIN_GRADE]
    }
    claims: dict[str, int] = {}
    for p in candidates.values():
        claims[p] = claims.get(p, 0) + 1
    return {s: p for s, p in candidates.items() if claims[p] == 1}


@dataclass
class Transition:
    """What one snapshot step does to the registry: rows to append and a changelog."""

    rows: list[RegistryRow]
    kept: int = 0
    carried: int = 0
    issued: int = 0
    retired: int = 0
    redirected: int = 0


def advance(
    current: Mapping[str, str],
    after: Iterable[str],
    effective: Mapping[str, Link],
    to_date: str,
    issued_by_earlier_runs: Mapping[str, str] | None = None,
) -> Transition:
    """Move the registry from `current` (PNU -> parcel_id at the earlier snapshot) to `after`.

    - a PNU present on both sides keeps its id (no row: nothing changed);
    - a new PNU whose strong identity link points at a current PNU takes that parcel's id: the old
      PNU's row closes (valid_to) and a row with the new PNU opens under the same id;
    - any other new PNU gets a fresh id;
    - a current PNU that is gone and not carried closes as historic.
    `issued_by_earlier_runs` (successor PNU -> id issued before a stronger link existed) turns a
    carry into a redirect of that earlier fresh id instead of issuing again.
    """

    after_set = set(after)
    carry = carrying_links(effective)
    issued_before = dict(issued_by_earlier_runs or {})
    rows: list[RegistryRow] = []
    t = Transition(rows)
    carried_from: set[str] = set()
    for pnu in sorted(after_set):
        if pnu in current:
            t.kept += 1
            continue
        source = carry.get(pnu)
        if source is not None and source in current and source not in after_set:
            pid = current[source]
            rows.append(RegistryRow(pid, source, HISTORIC, "", to_date))
            rows.append(RegistryRow(pid, pnu, CURRENT, to_date))
            carried_from.add(source)
            t.carried += 1
            earlier = issued_before.get(pnu)
            if earlier and earlier != pid:
                rows.append(RegistryRow(earlier, pnu, REDIRECTED, to_date, None, pid))
                t.redirected += 1
            continue
        if pnu in issued_before:
            continue  # issued by an earlier run and still not provably the same land: nothing new
        rows.append(RegistryRow(parcel_id_for(pnu, to_date), pnu, CURRENT, to_date))
        t.issued += 1
    for pnu, pid in sorted(current.items()):
        if pnu not in after_set and pnu not in carried_from:
            rows.append(RegistryRow(pid, pnu, HISTORIC, "", to_date))
            t.retired += 1
    return t


@dataclass
class RegistryState:
    """The registry folded to "now": which id each current PNU carries, and the last id of each
    PNU that is no longer current (what a later, stronger link would carry forward)."""

    current: dict[str, str]
    last_id_of_closed: dict[str, str]


def fold(rows: Iterable[RegistryRow]) -> RegistryState:
    """Replay appended rows in date order. A row opens (current), closes (historic) or redirects."""

    current: dict[str, str] = {}
    closed: dict[str, str] = {}
    ordered = sorted(rows, key=lambda r: (r.valid_from or r.valid_to or "", 0 if r.status != CURRENT else 1))
    for r in ordered:
        if r.status == CURRENT:
            current[r.pnu] = r.parcel_id
            closed.pop(r.pnu, None)
        elif r.status == HISTORIC:
            if current.get(r.pnu) == r.parcel_id:
                del current[r.pnu]
            closed[r.pnu] = r.parcel_id
        elif r.status == REDIRECTED and current.get(r.pnu) == r.parcel_id:
            del current[r.pnu]
    return RegistryState(current, closed)


def upgrade(state: RegistryState, effective: Mapping[str, Link], on_date: str) -> Transition:
    """Re-run after the lineage got stronger: a current parcel whose carrying link now points at a
    closed PNU takes that PNU's id back, and its interim id is redirected to it (never deleted)."""

    rows: list[RegistryRow] = []
    t = Transition(rows)
    for successor, predecessor in sorted(carrying_links(effective).items()):
        interim = state.current.get(successor)
        original = state.last_id_of_closed.get(predecessor)
        if interim and original and interim != original:
            rows.append(RegistryRow(interim, successor, REDIRECTED, on_date, None, original))
            rows.append(RegistryRow(original, successor, CURRENT, on_date))
            t.redirected += 1
    return t


def check_every_parcel_has_one_current_id(current_after: Mapping[str, str], after: Sequence[str]) -> list[str]:
    """The gate's (b): every polygon's PNU resolves to exactly one current id (ADR-0113 §7)."""

    missing = sorted(set(after) - set(current_after))
    counts = collections.Counter(current_after.values())
    duplicated = sorted(i for i, n in counts.items() if n > 1)
    return [f"no current id: {p}" for p in missing[:20]] + [f"id on two current PNUs: {i}" for i in duplicated[:20]]
