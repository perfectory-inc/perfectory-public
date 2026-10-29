#!/usr/bin/env python3
"""May this lane bake this Gold snapshot now, given what the lane it follows serves? (root ADR-0333)

    lane_follow_gate.py <lane> <gold snapshot> <followed lane's state.json>
    lane_follow_gate.py <lane> --follows       the lane it follows this way, or nothing

A lane whose documents follow another's (config/r2-connections.contract.json documents_follow_lane:
the unit price lane follows the building lane, root ADR-0190) places its rows where the followed
lane's documents place them: a unit's price is served under the PNU its building document carries it.
While both lanes baked in one run, the order and "not in a run where the building lane failed" kept
that. Each lane is its own asset and job now, so the rule is a state: the lane's Gold snapshot must be
built from the same snapshots of the pinned inputs (gold-panel-rebuild.contract.json `pins_from`) as
the followed lane's served Gold snapshot. The pins are the source pins each Gold commit records
(gold_rebuild.recorded_pins), read from the catalog over REST (iceberg_metadata.py).

Prints `ok`, or `waiting <followed snapshot> <reason>` (the bake ends `waiting`, and the asset
reconciler starts it again once the followed lane serves another snapshot). Exits 65 when it cannot
tell: the bake then fails rather than publish units under PNUs the building documents do not carry.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

PLATFORM = Path(__file__).resolve().parents[2]
for relative in ("infra/lakehouse/spark/jobs", "scripts/ops"):
    sys.path.insert(0, str(PLATFORM / relative))

import by_pnu_kinds  # noqa: E402
import gold_rebuild  # noqa: E402
import iceberg_metadata  # noqa: E402


def followed_lane(lane: str) -> str:
    """The lane whose served documents this lane's must match, or "": its documents_follow_lane, when its
    Gold pins named inputs to another Gold's build. A kind pinned to its whole host panel bakes inside
    the host's `all` run, and an own-producer kind pins nothing (root ADR-0303 §2)."""
    fields = by_pnu_kinds.lane_fields(lane)
    entry = gold_rebuild.load_contract()["tables"].get(fields["gold_table"], {})
    follows = entry.get("pins_from")
    if not fields["documents_follow_lane"] or not follows or follows["inputs"] == "all":
        return ""
    return fields["documents_follow_lane"]


def served_snapshot(state: dict) -> str | None:
    """The Gold snapshot a lane's state says it serves: its packs' when it serves packs."""
    packs = state.get("section_packs")
    served = packs["reflected_gold_iceberg_snapshot_id"] if packs else state["published"]["reflected_gold_iceberg_snapshot_id"]
    return None if served in (None, "", "-") else str(served)


def verdict(lane: str, snapshot: str, followed_state: dict, table) -> str:
    entry = gold_rebuild.load_contract()["tables"][by_pnu_kinds.lane_fields(lane)["gold_table"]]
    follows = entry.get("pins_from")
    if not follows or follows["inputs"] == "all":
        return "ok"  # nothing is pinned to another Gold: the lanes place their rows independently
    served = served_snapshot(followed_state)
    if served is None:
        return f"waiting - the lane it follows serves no {follows['gold_table']} snapshot yet"
    mine = gold_rebuild.recorded_pins(table(entry_table(lane))["snapshots"], snapshot)
    theirs = gold_rebuild.recorded_pins(table(follows["gold_table"])["snapshots"], served)
    if mine is None or theirs is None:
        raise ValueError(f"no recorded source pins at {snapshot} or at {follows['gold_table']} {served}")
    differ = [name for name in follows["inputs"] if mine.get(name) != theirs.get(name)]
    if not differ:
        return "ok"
    return (f"waiting {served} {follows['gold_table']} {served}, which the lane it follows serves, was built "
            f"from other snapshots of {', '.join(differ)} than Gold {snapshot}")


def entry_table(lane: str) -> str:
    return by_pnu_kinds.lane_fields(lane)["gold_table"]


def main(argv: list[str]) -> int:
    if len(argv) == 2 and argv[1] == "--follows":
        print(followed_lane(argv[0]))
        return 0
    if len(argv) != 3:
        print("usage: lane_follow_gate.py <lane> <gold snapshot> <followed lane's state.json> | <lane> --follows",
              file=sys.stderr)
        return 64
    lane, snapshot, state_path = argv
    try:
        catalog = iceberg_metadata.Catalog("FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN", "foundation-lane-follow-gate")
        print(verdict(lane, snapshot, json.loads(Path(state_path).read_text(encoding="utf-8")), catalog.table))
    except (OSError, KeyError, ValueError) as error:
        print(f"lane_follow_gate: cannot tell whether {lane} may bake {snapshot}: {type(error).__name__}: {error}",
              file=sys.stderr)
        return 65
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
