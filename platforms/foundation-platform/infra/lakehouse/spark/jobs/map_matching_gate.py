"""The matching gate every map bake passes first (root ADR-0113 §7).

A bake is refused when a polygon would reach the map without its place, its identity or an
attribute the sources do hold. Each check returns counts per reason and a sample, so the refusal
says exactly what is wrong; an empty `violations` is a pass. Pure Python over plain values: the
served-Gold jobs and `parcel_matching_gate.py` feed it from the lakehouse.

What is *allowed* to be empty is named, never inferred: a parcel with no official land price has
no row in the source under its number nor under any number its lineage links it to.
"""

from __future__ import annotations

import collections
from dataclasses import dataclass, field
from typing import Iterable, Mapping

EXISTS = "존재"
# An id that disappears between two served snapshots of the administrative layer means a place left
# the map; renumbering never does that (ids are carried). Tolerated: none.
MAX_VANISHED_ADMIN_IDS = 0
SAMPLE = 20


@dataclass
class GateReport:
    checked: int = 0
    violations: dict[str, list[str]] = field(default_factory=lambda: collections.defaultdict(list))
    allowed: dict[str, int] = field(default_factory=lambda: collections.defaultdict(int))

    def refuse(self, reason: str, item: str) -> None:
        self.violations[reason].append(item)

    def allow(self, reason: str) -> None:
        self.allowed[reason] += 1

    @property
    def passed(self) -> bool:
        return not any(self.violations.values())

    def as_dict(self) -> dict:
        return {
            "passed": self.passed,
            "checked": self.checked,
            "violations": {r: {"count": len(v), "sample": sorted(v)[:SAMPLE]} for r, v in sorted(self.violations.items()) if v},
            "allowed": dict(sorted(self.allowed.items())),
        }

    def message(self) -> str:
        parts = [f"{r}: {len(v)} (e.g. {', '.join(sorted(v)[:3])})" for r, v in sorted(self.violations.items()) if v]
        return "matching gate refused the bake — " + "; ".join(parts)


def check_admin_units(
    units: Iterable[tuple[str, str]],
    official: Mapping[str, tuple[str, str]],
    previous: Mapping[str, str] | None = None,
) -> GateReport:
    """(administrative_unit_id, canonical_code) of the served layer against the official code list.

    - every code exists in the official list;
    - no id and no code is served twice;
    - every existing sido has at least one unit;
    - no id served before is missing now (renumbering carries ids, so a vanished id is a lost place).
    """

    report = GateReport()
    ids: collections.Counter[str] = collections.Counter()
    codes: collections.Counter[str] = collections.Counter()
    served_sidos = set()
    for unit_id, code in units:
        report.checked += 1
        ids[unit_id] += 1
        codes[code] += 1
        served_sidos.add(code[:2])
        if official.get(code, ("", ""))[1] != EXISTS:
            report.refuse("code not existing in the official list", code)
    for unit_id, n in ids.items():
        if n > 1:
            report.refuse("id served twice", unit_id)
    for code, n in codes.items():
        if n > 1:
            report.refuse("code served twice", code)
    sidos = {c[:2] for c, (_, s) in official.items() if s == EXISTS and c.endswith("00000000")}
    for sido in sorted(sidos - served_sidos):
        report.refuse("existing sido with no unit", sido)
    if previous:
        vanished = set(previous) - set(ids)
        if len(vanished) > MAX_VANISHED_ADMIN_IDS:
            for unit_id in vanished:
                report.refuse("id served before is gone", f"{unit_id}({previous[unit_id]})")
    return report


def check_parcels(
    pnus: Iterable[str],
    official: Mapping[str, tuple[str, str]],
    admin_codes: set[str],
    current_ids: Mapping[str, str] | None = None,
) -> GateReport:
    """Every parcel polygon has its legal dong, that dong's boundary and one current parcel id.

    `admin_codes` are the served administrative codes (읍면동, `xxxxxxxx00`); a parcel's boundary is
    its 읍면동, since rural parcels carry a 리 in the last two digits. `current_ids` is the folded
    registry (PNU -> parcel_id), or None when the region has no registry yet.
    """

    report = GateReport()
    id_count: collections.Counter[str] = collections.Counter()
    for pnu in pnus:
        report.checked += 1
        if len(pnu) != 19 or not pnu.isdigit():
            report.refuse("malformed PNU", pnu)
            continue
        if official.get(pnu[:10], ("", ""))[1] != EXISTS:
            report.refuse("legal dong not existing in the official list", pnu)
        if pnu[:8] + "00" not in admin_codes:
            report.refuse("no administrative boundary for the parcel's 읍면동", pnu)
        if current_ids is not None:
            pid = current_ids.get(pnu)
            if pid is None:
                report.refuse("no current parcel id", pnu)
            else:
                id_count[pid] += 1
    for pid, n in id_count.items():
        if n > 1:
            report.refuse("parcel id on two parcels", pid)
    return report


def check_attribute(
    name: str,
    pnus: Iterable[str],
    has_value: set[str],
    predecessors: Mapping[str, Iterable[str]],
) -> GateReport:
    """An attribute (e.g. the land price) is attached to every parcel whose sources hold it.

    A parcel with a value under its own number passes. One without passes as "no source value" only
    if no number its lineage links it to holds a value either; if a predecessor does, the value is
    in the sources but not on the map — the renumbering accident this gate exists for.
    """

    report = GateReport()
    for pnu in pnus:
        report.checked += 1
        if pnu in has_value:
            continue
        held = [old for old in predecessors.get(pnu, ()) if old in has_value]
        if held:
            report.refuse(f"{name} held under a predecessor number but not attached", f"{pnu}<-{held[0]}")
        else:
            report.allow(f"{name}: no source value")
    return report
