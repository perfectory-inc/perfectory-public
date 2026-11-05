#!/usr/bin/env python3
"""May this by-PNU lane publish now, or is a promoted edition still being rebuilt? (root ADR-0291 §3, ADR-0381)

    edition_lockstep_gate.py <lane>       prints `ok` or `waiting <reason>`; exits 65 when it cannot tell

A promotion moves `served_edition`; each Gold that reads the parcels then rebuilds, only the PNUs the two
editions differ in (gold_rebuild.plan, gold_incremental), and records the edition it was built from on
its commit (gold_rebuild.SERVED_EDITION_PROPERTY). Those Golds rebuild one after another, so the lanes
serving them must not publish one after another: a parcel panel on the new edition beside a 공매 kind on
the old one is the mixed serving ADR-0291 forbids (2026-10-21: 632 sales under parcels the served map
did not hold). A lane of the served universe therefore publishes only when every such lane's Gold is on
the served edition; until then it waits and keeps serving what it served, and the asset reconciler
starts it again.

The lanes are derived, not listed: the parcel panel and every by-PNU kind whose host panel it is
(by_pnu_kinds). Each Gold's edition is the planner's own reading of its history
(gold_rebuild.recorded_served_edition); a commit from before the record is dated through the promotions
ledger, so on a day without a promotion the gate always opens. Histories are read from the catalog over
REST (iceberg_metadata.py), as lane_follow_gate.py reads them.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Callable, Mapping

PLATFORM = Path(__file__).resolve().parents[2]
for relative in ("infra/lakehouse/spark/jobs", "scripts/ops"):
    sys.path.insert(0, str(PLATFORM / relative))

import by_pnu_kinds  # noqa: E402
import gold_rebuild  # noqa: E402
import iceberg_metadata  # noqa: E402
import vworld_parcel_editions as editions  # noqa: E402

PARCEL_PANEL = "gold.parcel_panel"
PARCEL_LANE = "parcel"


def lockstep_tables() -> dict[str, str]:
    """{by-PNU lane: its Gold table} of every lane built from the served parcels."""

    lanes = {PARCEL_LANE: PARCEL_PANEL}
    lanes.update({name: kind.gold_table for name, kind in by_pnu_kinds.kinds().items() if kind.host_panel == PARCEL_PANEL})
    return lanes


def verdict(lane: str, contract: Mapping[str, Any], history: Callable[[str], dict]) -> str:
    """`ok`, or `waiting <reason>` naming the lanes whose Gold is not yet on the served edition. A lane
    outside the served universe, and a Gold with no pinned commit (nothing of it served), never wait.
    `history(table)` reads a kind's Gold that was never created as one with no commit (`absent_ok`):
    a kind defined before its first build serves nothing, so it holds no barrier."""

    lanes = lockstep_tables()
    if lane not in lanes:
        return "ok"
    served = editions.snapshot_id(contract, editions.served(contract))
    behind = []
    for name, table in sorted(lanes.items()):
        state = history(table)
        if state.get("head") is None:
            continue
        built = gold_rebuild.recorded_served_edition(state["snapshots"], state["head"], dict(contract))
        if built is not None and built != served:
            behind.append(f"{name} ({built})")
    if not behind:
        return "ok"
    return f"waiting the served edition moved to {served}; not rebuilt on it yet: {', '.join(behind)}"


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print("usage: edition_lockstep_gate.py <lane>", file=sys.stderr)
        return 64
    try:
        catalog = iceberg_metadata.Catalog("FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN", "foundation-edition-lockstep-gate")
        # The parcel panel is the universe itself: absent, the catalog is not the one the lanes serve.
        def history(table: str) -> dict:
            return catalog.table(table, absent_ok=table != PARCEL_PANEL)

        print(verdict(argv[0], editions.load(), history))
    except (OSError, KeyError, ValueError, editions.EditionError) as error:
        print(f"edition_lockstep_gate: cannot tell whether {argv[0]} may publish: {type(error).__name__}: {error}",
              file=sys.stderr)
        return 65
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
