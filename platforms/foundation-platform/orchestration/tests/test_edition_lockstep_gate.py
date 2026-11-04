"""Every served-universe lane switches to a promoted edition together (root ADR-0291 §3, ADR-0381).

Synthetic editions 209906 → 209910 and synthetic snapshot ids. A promotion's Golds rebuild one after
another; the planted case is the window in between, where before ADR-0381 each lane published its
Gold as soon as it was rebuilt and served mixed editions (the 2026-10-21 공매 class).
"""

import importlib.util
import json
import sys
import unittest
from pathlib import Path

PLATFORM = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "infra/lakehouse/spark/tests"))
spec = importlib.util.spec_from_file_location("edition_lockstep_gate", PLATFORM / "scripts/ops/edition_lockstep_gate.py")
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)

import gold_rebuild  # noqa: E402
import parcel_edition_readiness as readiness  # noqa: E402
from test_parcel_edition_readiness import CANDIDATE, NOW, contract, evidence  # noqa: E402

OLD, NEW = "vworldkr__parcel-209906", "vworldkr__parcel-209910"


def gold(edition: str | None, at: str = "2099-10-21T00:00:00Z") -> dict:
    summary = {gold_rebuild.SOURCE_SNAPSHOTS_PROPERTY: json.dumps({"silver.parcel_boundaries": "11"})}
    if edition is not None:
        summary[gold_rebuild.SERVED_EDITION_PROPERTY] = edition
    return {"head": "1", "snapshots": [{"snapshot_id": "1", "parent_id": None, "committed_at": at,
                                        "operation": "overwrite", "summary": summary}]}


class LockstepGateTest(unittest.TestCase):
    def setUp(self):
        held = contract()
        self.promoted = readiness.promote(held, readiness.readiness(held, evidence(), NOW), CANDIDATE, "op", "why", NOW)
        self.tables = gate.lockstep_tables()

    def test_the_lanes_are_the_parcel_panel_and_its_kinds(self):
        self.assertEqual(self.tables[gate.PARCEL_LANE], gate.PARCEL_PANEL)
        self.assertIn("parcel-public-auction", self.tables, "the 공매 kind reads the served parcels")
        self.assertGreater(len(self.tables), 2)

    def test_no_lane_publishes_until_every_lane_is_rebuilt_then_all_do(self):
        tables = sorted(self.tables.values())
        # The parcel panel is rebuilt on the new edition first; every kind is still on the old one.
        histories = {table: gold(OLD) for table in tables}
        histories[gate.PARCEL_PANEL] = gold(NEW)
        for lane in self.tables:
            with self.subTest(lane=lane, stage="partway"):
                verdict = gate.verdict(lane, self.promoted, histories.__getitem__)
                self.assertTrue(verdict.startswith("waiting"), verdict)
                self.assertNotIn("parcel (", verdict, "the rebuilt lane does not hold the barrier")
        # One kind short: still nobody publishes.
        histories = {table: gold(NEW) for table in tables}
        histories[tables[-1]] = gold(OLD)
        self.assertTrue(gate.verdict(gate.PARCEL_LANE, self.promoted, histories.__getitem__).startswith("waiting"))
        # Every lane on the new edition: every lane publishes, together.
        histories = {table: gold(NEW) for table in tables}
        self.assertEqual({gate.verdict(lane, self.promoted, histories.__getitem__) for lane in self.tables}, {"ok"})

    def test_a_day_without_a_promotion_never_waits(self):
        # Golds committed before the record: the ledger says 209906 was served then, and still is.
        histories = {table: gold(None, "2099-10-01T00:00:00Z") for table in self.tables.values()}
        self.assertEqual(gate.verdict(gate.PARCEL_LANE, contract(), histories.__getitem__), "ok")
        # Before the record but after the promotion's date, the ledger dates it to the new edition.
        histories = {table: gold(None, "2099-10-21T00:00:00Z") for table in self.tables.values()}
        self.assertEqual(gate.verdict(gate.PARCEL_LANE, self.promoted, histories.__getitem__), "ok")

    def test_a_lane_outside_the_served_universe_and_an_absent_gold_never_wait(self):
        self.assertEqual(gate.verdict("building", self.promoted, lambda table: gold(OLD)), "ok")
        histories = {table: {"head": None, "snapshots": []} for table in self.tables.values()}
        self.assertEqual(gate.verdict(gate.PARCEL_LANE, self.promoted, histories.__getitem__), "ok")


if __name__ == "__main__":
    unittest.main()
