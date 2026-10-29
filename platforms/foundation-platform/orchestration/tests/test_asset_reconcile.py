"""The asset reconciler (scripts/ops/asset_reconcile.py, root ADR-0333 §7) against synthetic catalogs.

2026-10-28 21:38: the 공매 Gold committed and recorded asset event 3912, which reached a bake a deploy
held paused; nothing queued the bake and no series said so. These tests replay that, and pin that the
reconciler starts by state: once per set of versions, never while a run is going, never for a
compaction, and that its lag and unscheduled series and its alert threshold follow from the job list.
Snapshot ids and times are synthetic.
"""

import json
import pathlib
import re
import sys
import unittest

ORCHESTRATION = pathlib.Path(__file__).resolve().parents[1]
PLATFORM = ORCHESTRATION.parent
sys.path.insert(0, str(ORCHESTRATION / "dags"))
sys.path.insert(0, str(PLATFORM / "scripts/ops"))

import asset_reconcile  # noqa: E402
import job_specs  # noqa: E402

T0 = 1_800_000_000.0  # a synthetic instant; snapshots are minutes after it


def at(minutes):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(T0 + 60 * minutes, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def snap(snapshot_id, parent, minutes, operation="append", added=1, pins=None):
    summary = {"added-records": str(added)}
    if pins is not None:
        summary["foundation.source-iceberg-snapshots"] = json.dumps(pins)
    return {"snapshot_id": snapshot_id, "parent_id": parent, "committed_at": at(minutes), "operation": operation,
            "summary": summary}


def table(*snapshots):
    return {"head": snapshots[-1]["snapshot_id"], "snapshots": list(snapshots)}


class Reconciler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        jobs = json.loads(job_specs.JOBS.read_text(encoding="utf-8"))
        graph = json.loads(job_specs.GRAPH.read_text(encoding="utf-8"))
        cls.assets = {asset.job: asset for asset in asset_reconcile.assets(jobs, graph, asset_reconcile.gold_rebuild.load_contract())}
        cls.jobs = jobs

    def setUp(self):
        units = {"silver.building_register_titles": "11", "silver.building_register_units": "21"}
        self.catalog = {
            "gold.parcel_public_auction": table(snap("101", None, 0), snap("102", "101", 30)),
            "gold.unit_price": table(snap("301", None, 0, operation="overwrite", pins={**units, "silver.unit_official_price": "41"})),
            "gold.building_panel": table(snap("201", None, 0, operation="overwrite", pins=units)),
            "silver.building_register_titles": table(snap("11", None, 0)),
            "silver.building_register_units": table(snap("21", None, 0)),
            "silver.unit_official_price": table(snap("41", None, 0)),
        }
        self.reads = {"parcel-public-auction": {"read_snapshot_id": "101", "outcome": "served"},
                      "unit-price": {"read_snapshot_id": "301", "outcome": "served"},
                      "building": {"read_snapshot_id": "201", "outcome": "served"}}
        self.running, self.full = set(), set()

    def tick(self, now_minutes=40, state=None):
        return asset_reconcile.tick(T0 + 60 * now_minutes, list(self.assets.values()), self.catalog.__getitem__,
                                    self.reads.get, lambda service: service in self.running,
                                    lambda job: "full" if job in self.full else "", state or {})

    def test_the_assets_are_those_the_reconciler_names_each_one_asset(self):
        self.assertEqual(set(self.assets), {"gold_unit_price_rebuild", "by_pnu_unit_price_bake", "by_pnu_public_auction_bake"})
        price = self.assets["gold_unit_price_rebuild"]
        self.assertEqual(price.uri, "iceberg://gold.unit_price")
        self.assertEqual(price.inputs, ("silver.building_register_titles", "silver.building_register_units",
                                        "silver.unit_official_price"))
        self.assertEqual(price.pins_from["gold_table"], "gold.building_panel")
        lane = self.assets["by_pnu_unit_price_bake"]
        self.assertEqual((lane.lane, lane.lane_table, lane.follows), ("unit-price", "gold.unit_price", "building"))
        auction = self.assets["by_pnu_public_auction_bake"]
        self.assertEqual((auction.lane, auction.lane_table, auction.follows),
                         ("parcel-public-auction", "gold.parcel_public_auction", None))

    def test_event_3912_replayed_the_bake_is_owed_and_unscheduled_from_the_commit(self):
        owed, metrics, state, errors = self.tick()
        self.assertEqual(errors, [])
        [line] = owed
        self.assertRegex(line, r"^foundation-asset-owed by_pnu_public_auction_bake [0-9a-f]{16}$")
        labels = 'asset="perfectory://parcel-public-auction-by-pnu-section-packs",job="by_pnu_public_auction_bake"'
        self.assertIn(f"foundation_asset_lag_seconds{{{labels}}} 1800", metrics)
        self.assertIn(f"foundation_asset_unscheduled_since_timestamp_seconds{{{labels}}} {T0 + 1800:.0f}", metrics)
        self.assertEqual(state["perfectory://parcel-public-auction-by-pnu-section-packs"]["unscheduled_since"], T0 + 1800)
        # The next tick owes the same versions under the same key: the DAG starts it once.
        again, _, state, _ = self.tick(45, state)
        self.assertEqual(again, owed)
        self.assertEqual(state["perfectory://parcel-public-auction-by-pnu-section-packs"]["unscheduled_since"], T0 + 1800)
        # The Gold moves again: a new key.
        self.catalog["gold.parcel_public_auction"] = table(*self.catalog["gold.parcel_public_auction"]["snapshots"],
                                                           snap("103", "102", 42))
        moved, _, _, _ = self.tick(45, state)
        self.assertNotEqual(moved, owed)

    def test_nothing_is_owed_when_caught_up_or_only_compacted(self):
        self.reads["parcel-public-auction"]["read_snapshot_id"] = "102"
        owed, metrics, state, _ = self.tick()
        self.assertEqual(owed, [])
        self.assertFalse(any(line.startswith("foundation_asset_unscheduled_since") for line in metrics))
        self.catalog["gold.parcel_public_auction"] = table(*self.catalog["gold.parcel_public_auction"]["snapshots"],
                                                           snap("103", "102", 35, operation="replace", added=9))
        owed, _, _, _ = self.tick()
        self.assertEqual(owed, [], "a compaction changes no row")

    def test_a_running_job_is_not_started_and_a_full_pool_is_not_unscheduled(self):
        self.running = {"foundation-by-pnu-public-auction-bake.service"}
        owed, metrics, state, _ = self.tick()
        self.assertEqual(owed, [])
        self.assertNotIn("unscheduled_since", state["perfectory://parcel-public-auction-by-pnu-section-packs"])
        self.running, self.full = set(), {"by_pnu_public_auction_bake"}
        owed, metrics, state, _ = self.tick(50, state)
        self.assertEqual(len(owed), 1, "owed while the pool is full: the started run queues for its slot")
        self.assertFalse(any(line.startswith("foundation_asset_unscheduled_since") for line in metrics))
        # Room again: unscheduled counts from the last tick that saw it busy, not from the commit.
        self.full = set()
        _, _, state, _ = self.tick(55, state)
        self.assertEqual(state["perfectory://parcel-public-auction-by-pnu-section-packs"]["unscheduled_since"], T0 + 50 * 60)

    def test_the_unit_price_gold_is_owed_when_the_building_gold_moved_its_registers(self):
        owed, _, _, _ = self.tick()
        self.assertFalse(any("gold_unit_price_rebuild" in line for line in owed))
        self.catalog["silver.building_register_titles"] = table(snap("11", None, 0), snap("12", "11", 10))
        owed, _, _, _ = self.tick()
        self.assertFalse(any("gold_unit_price_rebuild" in line for line in owed),
                         "pinned to the building Gold's registers, not their heads")
        self.catalog["gold.building_panel"] = table(
            *self.catalog["gold.building_panel"]["snapshots"],
            snap("202", "201", 20, operation="overwrite",
                 pins={"silver.building_register_titles": "12", "silver.building_register_units": "21"}))
        owed, metrics, _, _ = self.tick()
        self.assertTrue(any(line.startswith("foundation-asset-owed gold_unit_price_rebuild ") for line in owed))
        self.assertIn('foundation_asset_lag_seconds{asset="iceberg://gold.unit_price",job="gold_unit_price_rebuild"} 600',
                      metrics)
        # A new price vintage at the head of silver.unit_official_price is owed too.
        self.catalog["gold.building_panel"]["head"] = "201"
        self.catalog["silver.unit_official_price"] = table(snap("41", None, 0), snap("42", "41", 5))
        owed, _, _, _ = self.tick()
        self.assertTrue(any(line.startswith("foundation-asset-owed gold_unit_price_rebuild ") for line in owed))

    def test_a_lane_waiting_for_the_lane_it_follows_waits_until_that_lane_moves(self):
        self.catalog["gold.unit_price"] = table(*self.catalog["gold.unit_price"]["snapshots"], snap("302", "301", 20))
        self.reads["unit-price"] = {"read_snapshot_id": "301", "outcome": "waiting", "gold_snapshot_id": "302",
                                    "waiting_on": {"lane": "building", "read_snapshot_id": "201"}}
        owed, metrics, state, _ = self.tick()
        self.assertFalse(any("by_pnu_unit_price_bake" in line for line in owed))
        self.assertNotIn("unscheduled_since", state["perfectory://unit-price-by-pnu-section-packs"])
        self.reads["building"] = {"read_snapshot_id": "202", "outcome": "served"}
        owed, _, _, _ = self.tick()
        self.assertTrue(any(line.startswith("foundation-asset-owed by_pnu_unit_price_bake ") for line in owed))

    def test_an_asset_that_cannot_be_read_does_not_stop_the_others(self):
        del self.catalog["gold.unit_price"]
        owed, metrics, _, errors = self.tick()
        self.assertEqual(len(errors), 2)
        self.assertTrue(any("by_pnu_public_auction_bake" in line for line in owed))
        self.assertIn('foundation_asset_reconcile_error{asset="iceberg://gold.unit_price",job="gold_unit_price_rebuild"} 1',
                      metrics)
        self.assertTrue(metrics[-1].startswith("foundation_asset_reconcile_timestamp_seconds "))

    def test_the_unscheduled_alert_waits_three_reconcile_periods(self):
        # Root ADR-0333 §7: the alert fires past three times the reconciler's period.
        reconciler = next(job for job in self.jobs["jobs"] if job["id"] == asset_reconcile.RECONCILER)
        period = job_specs.shortest_interval_minutes(reconciler["schedule"])
        self.assertEqual(period, 5)
        rules = (PLATFORM / "infra/observability/prometheus/foundation-api.rules.yml").read_text(encoding="utf-8")
        self.assertRegex(rules, rf"asset_unscheduled_seconds > {3 * period * 60}\b")
        for name in ("record: asset_lag_seconds", "record: asset_unscheduled_seconds",
                     "alert: FoundationAssetUnscheduled", "alert: FoundationAssetReconcilerStopped"):
            self.assertIn(name, rules)
        self.assertIn("foundation_asset_reconcile_timestamp_seconds", rules)
        self.assertTrue(re.search(r"absent\(foundation_asset_reconcile_timestamp_seconds\)", rules))


class FollowGate(unittest.TestCase):
    """scripts/ops/lane_follow_gate.py: the unit price lane bakes only a Gold built from the registers
    the building Gold the building lane serves was built from (root ADR-0190, ADR-0333)."""

    def setUp(self):
        import lane_follow_gate
        self.gate = lane_follow_gate
        units = {"silver.building_register_titles": "11", "silver.building_register_units": "21"}
        self.catalog = {
            "gold.building_panel": table(snap("201", None, 0, operation="overwrite", pins=units),
                                         snap("202", "201", 10, operation="overwrite",
                                              pins={**units, "silver.building_register_titles": "12"})),
            "gold.unit_price": table(snap("301", None, 0, operation="overwrite", pins={**units, "silver.unit_official_price": "41"}),
                                     snap("302", "301", 20, operation="overwrite",
                                          pins={**units, "silver.building_register_titles": "12",
                                                "silver.unit_official_price": "41"})),
        }

    def served(self, snapshot):
        return {"published": {"reflected_gold_iceberg_snapshot_id": "1"},
                "section_packs": {"reflected_gold_iceberg_snapshot_id": snapshot}}

    def test_only_the_unit_price_lane_follows_another_this_way(self):
        self.assertEqual(self.gate.followed_lane("unit-price"), "building")
        self.assertEqual(self.gate.followed_lane("parcel-public-auction"), "", "an own-producer kind pins nothing")
        self.assertEqual(self.gate.followed_lane("parcel-zoning"), "", "a section kind bakes inside its host's run")

    def test_it_bakes_what_the_building_lane_serves_and_waits_otherwise(self):
        verdict = lambda snapshot, served: self.gate.verdict("unit-price", snapshot, self.served(served),  # noqa: E731
                                                              self.catalog.__getitem__)
        self.assertEqual(verdict("302", "202"), "ok")
        self.assertEqual(verdict("301", "201"), "ok")
        self.assertTrue(verdict("302", "201").startswith("waiting 201 "), "the building lane is behind")
        self.assertTrue(verdict("301", "202").startswith("waiting 202 "), "the unit price Gold is behind")
        self.assertTrue(verdict("302", None).startswith("waiting - "))


if __name__ == "__main__":
    unittest.main()
