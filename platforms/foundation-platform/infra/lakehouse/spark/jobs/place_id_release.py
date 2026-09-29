"""What every map release publishes about its ids (root ADR-0113 §8; Overture GERS registry, bridge
file and changelog).

For a unit (`parcel` or `admin`) and a release:
  - registry:  every id ever issued, with its status now (current / historic / redirected);
  - bridge:    id <-> source code (PNU or legal-dong code) with the period the pair held;
  - changelog: what this release changed against the previous one — added, retired, renumbered
               (same id, new code), redirected (an interim id pointed at the id the place kept).
Consumers holding an old reference (a saved parcel, a shared link) resolve it through the bridge and
follow redirects in the registry. Pure Python; `place_id_release_to_gold.py` writes the result.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

import parcel_identity as pi


@dataclass(frozen=True)
class RegistryEntry:
    place_id: str
    status: str
    current_code: str | None
    redirect_to: str | None


@dataclass(frozen=True)
class BridgeEntry:
    place_id: str
    code: str
    valid_from: str
    valid_to: str | None


@dataclass(frozen=True)
class Change:
    place_id: str
    change: str
    from_code: str | None
    to_code: str | None
    redirect_to: str | None = None


# --- parcels: from the append-only registry rows ------------------------------------------------


def parcel_bridge(rows: Iterable[pi.RegistryRow]) -> list[BridgeEntry]:
    """Every (parcel_id, PNU) period: opened by a current row, closed by the historic row of the
    same pair (or open). A redirected row closes the interim id's period on its date."""

    opened: dict[tuple[str, str], str] = {}
    periods: list[BridgeEntry] = []
    ordered = sorted(rows, key=lambda r: (r.valid_from or r.valid_to or "", 0 if r.status != pi.CURRENT else 1))
    for r in ordered:
        key = (r.parcel_id, r.pnu)
        if r.status == pi.CURRENT:
            opened.setdefault(key, r.valid_from)
        elif key in opened:
            periods.append(BridgeEntry(r.parcel_id, r.pnu, opened.pop(key), r.valid_to or r.valid_from))
    periods.extend(BridgeEntry(pid, pnu, start, None) for (pid, pnu), start in opened.items())
    return sorted(periods, key=lambda b: (b.place_id, b.valid_from))


def parcel_registry(rows: Iterable[pi.RegistryRow]) -> list[RegistryEntry]:
    rows = list(rows)
    state = pi.fold(rows)
    current_by_id = {pid: pnu for pnu, pid in state.current.items()}
    redirect = {r.parcel_id: r.redirect_to for r in rows if r.status == pi.REDIRECTED}
    entries = []
    for pid in sorted({r.parcel_id for r in rows}):
        if pid in current_by_id:
            entries.append(RegistryEntry(pid, pi.CURRENT, current_by_id[pid], None))
        elif pid in redirect:
            entries.append(RegistryEntry(pid, pi.REDIRECTED, None, redirect[pid]))
        else:
            entries.append(RegistryEntry(pid, pi.HISTORIC, None, None))
    return entries


def parcel_changelog(rows: Sequence[pi.RegistryRow], previous_as_of: str, as_of: str) -> list[Change]:
    """Changes between the registry as of `previous_as_of` and as of `as_of` (dates of the rows)."""

    def upto(day: str) -> list[pi.RegistryRow]:
        return [r for r in rows if (r.valid_from or r.valid_to or "") <= day]

    before, after = pi.fold(upto(previous_as_of)).current, pi.fold(upto(as_of)).current
    before_by_id = {pid: pnu for pnu, pid in before.items()}
    after_by_id = {pid: pnu for pnu, pid in after.items()}
    redirects = {r.parcel_id: r.redirect_to for r in rows if r.status == pi.REDIRECTED and previous_as_of < (r.valid_from or "") <= as_of}
    changes = []
    for pid in sorted(set(before_by_id) | set(after_by_id) | set(redirects)):
        old, new = before_by_id.get(pid), after_by_id.get(pid)
        if pid in redirects:
            changes.append(Change(pid, "redirected", old, new, redirects[pid]))
        elif old is None and new is not None:
            changes.append(Change(pid, "added", None, new))
        elif old is not None and new is None:
            changes.append(Change(pid, "retired", old, None))
        elif old != new:
            changes.append(Change(pid, "renumbered", old, new))
    return changes


# --- administrative units: from consecutive Silver snapshots -----------------------------------


def admin_release(
    snapshots: Sequence[tuple[str, Mapping[str, str]]],
) -> tuple[list[RegistryEntry], list[BridgeEntry], list[Change]]:
    """`snapshots` in order: (snapshot label, {administrative_unit_id: canonical_code}).

    The registry is every id any snapshot served, current if the newest serves it; the bridge holds
    each (id, code) from the first snapshot it appeared in until the snapshot that replaced it; the
    changelog compares the newest snapshot with the one before.
    """

    if not snapshots:
        return [], [], []
    newest_label, newest = snapshots[-1]
    all_ids = sorted({i for _, units in snapshots for i in units})
    registry = [
        RegistryEntry(i, pi.CURRENT, newest[i], None) if i in newest else RegistryEntry(i, pi.HISTORIC, None, None)
        for i in all_ids
    ]
    bridge: list[BridgeEntry] = []
    opened: dict[tuple[str, str], str] = {}
    for label, units in snapshots:
        for key in [k for k in opened if units.get(k[0]) != k[1]]:
            bridge.append(BridgeEntry(key[0], key[1], opened.pop(key), label))
        for i, code in units.items():
            opened.setdefault((i, code), label)
    bridge.extend(BridgeEntry(i, code, start, None) for (i, code), start in opened.items())
    changes: list[Change] = []
    if len(snapshots) >= 2:
        _, previous = snapshots[-2]
        for i in sorted(set(previous) | set(newest)):
            old, new = previous.get(i), newest.get(i)
            if old is None:
                changes.append(Change(i, "added", None, new))
            elif new is None:
                changes.append(Change(i, "retired", old, None))
            elif old != new:
                changes.append(Change(i, "renumbered", old, new))
    return registry, sorted(bridge, key=lambda b: (b.place_id, b.valid_from)), changes


def resolve(code: str, bridge: Sequence[BridgeEntry], registry: Sequence[RegistryEntry]) -> str | None:
    """What a consumer does with an old reference: code -> the id that held it -> follow redirects
    -> the id's current code (None when the place no longer exists on the map)."""

    holders = [b.place_id for b in bridge if b.code == code]
    if not holders:
        return None
    by_id = {r.place_id: r for r in registry}
    place = by_id.get(holders[-1])
    seen = set()
    while place is not None and place.status == pi.REDIRECTED and place.place_id not in seen:
        seen.add(place.place_id)
        place = by_id.get(place.redirect_to or "")
    return place.current_code if place is not None and place.status == pi.CURRENT else None
