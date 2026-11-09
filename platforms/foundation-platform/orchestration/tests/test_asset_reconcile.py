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
        found = asset_reconcile.assets(jobs, graph, asset_reconcile.gold_rebuild.load_contract())
        # The panel bake serves several lanes, each its own asset; the other jobs make one each.
        cls.lanes = {asset.lane: asset for asset in found if asset.job == "by_pnu_serving_bake"}
        # The panel Gold rebuild makes two panels, each its own asset (root ADR-0333 §7, stage 2).
        cls.panels = {asset.gold: asset for asset in found if asset.job == "gold_panel_rebuild"}
        # The kind Gold rebuild makes every data kind's table, each its own asset pinned to its host panel.
        cls.kinds = {asset.gold: asset for asset in found if asset.job == "gold_kind_rebuild"}
        cls.assets = {asset.job: asset for asset in found
                      if asset.job not in ("by_pnu_serving_bake", "gold_panel_rebuild", "gold_kind_rebuild")}
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
            # The court auction Gold and its lane, caught up (root ADR-0353).
            "gold.parcel_court_auction": table(snap("401", None, 0, operation="overwrite",
                                                    pins={"silver.court_auction_item": "51",
                                                          "silver.court_auction_item_photo": "71",
                                                          "reference.legal_dong_code_change": "61"})),
            "silver.court_auction_item": table(snap("51", None, 0)),
            # Its case photos (root ADR-0384).
            "silver.court_auction_item_photo": table(snap("71", None, 0)),
            # The court auction Gold follows code changes too (root ADR-0382).
            "reference.legal_dong_code_change": table(snap("61", None, 0)),
        }
        self.reads = {"parcel-public-auction": {"read_snapshot_id": "101", "outcome": "served"},
                      "parcel-court-auction": {"read_snapshot_id": "401", "outcome": "served"},
                      "unit-price": {"read_snapshot_id": "301", "outcome": "served"},
                      "building": {"read_snapshot_id": "201", "outcome": "served"}}
        self.running, self.full = set(), set()

    def tick(self, now_minutes=40, state=None, panel=False):
        found = list(self.assets.values()) + (list(self.lanes.values()) if panel else [])
        return asset_reconcile.tick(T0 + 60 * now_minutes, found, self.catalog.__getitem__,
                                    self.reads.get, lambda service: service in self.running,
                                    lambda job: "full" if job in self.full else "", state or {})

    def test_the_assets_are_those_the_reconciler_names_each_one_asset(self):
        # The court auction Gold and its bake join when they are switched on (root ADR-0353, ADR-0386 §3).
        self.assertEqual(set(self.assets), {"gold_unit_price_rebuild", "by_pnu_unit_price_bake", "by_pnu_public_auction_bake"})
        # Every lane the panel bake serves is an asset of its own, fed from its one Gold table; inside
        # the one unit the kinds follow the parcel lane by run order, so none follows across jobs.
        self.assertEqual({lane: (asset.lane_table, asset.follows, asset.uri) for lane, asset in self.lanes.items()},
                         {lane: (table, None, f"perfectory://{lane}-by-pnu-section-packs") for lane, table in {
                             "parcel": "gold.parcel_panel", "building": "gold.building_panel",
                             "parcel-zoning": "gold.parcel_zoning", "parcel-price": "gold.parcel_price",
                             "parcel-characteristics": "gold.parcel_characteristics",
                             "parcel-forest-ledger": "gold.parcel_forest_ledger",
                             "parcel-transfer-history": "gold.parcel_transfer_history",
                             "parcel-land-rights": "gold.parcel_land_rights"}.items()})
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
        self.running = {"foundation-by-pnu-lane-bake@parcel-public-auction.service"}
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

    def test_each_panel_is_owed_when_an_input_its_planner_pins_moved_and_the_job_starts_once(self):
        # Root ADR-0333 §7 stage 2: 2026-11-07 the 08:45 panel rebuild found the rebuild lock held by a lane
        # cut-over and ended `waiting`; only the next day's clock would have started it again.
        parcel, building = self.panels["gold.parcel_panel"], self.panels["gold.building_panel"]
        self.assertEqual((parcel.uri, building.uri), ("iceberg://gold.parcel_panel", "iceberg://gold.building_panel"))
        self.assertIn("silver.land_transfer_history", parcel.inputs)
        self.assertNotIn("silver.parcel_lineage", parcel.inputs, "held back from a real rebuild (unmeasured_inputs)")
        # The unit price join is the job's own output: neither it nor its hub registers are an input the
        # building panel's pins record, so they would leave the asset owed for ever.
        self.assertEqual(building.inputs, ("silver.building_register_floors", "silver.building_register_titles",
                                           "silver.building_register_unit_areas", "silver.building_register_units"))
        pins = {name: "5" for name in parcel.inputs}
        self.catalog["gold.parcel_panel"] = table(snap("601", None, 0, operation="overwrite", pins=pins),
                                                  snap("602", "601", 5, operation="replace"))
        for name in parcel.inputs:
            self.catalog[name] = table(snap("5", None, 0))
        found = [parcel]
        tick = lambda: asset_reconcile.tick(T0 + 2400, found, self.catalog.__getitem__, self.reads.get,
                                            lambda service: False, lambda job: "", {})
        self.assertEqual(tick()[0], [], "a compaction after the build moves nothing")
        self.catalog["silver.land_transfer_history"] = table(snap("5", None, 0), snap("6", "5", 10))
        owed, _, _, _ = tick()
        self.assertEqual(len(owed), 1)
        self.assertTrue(owed[0].startswith("foundation-asset-owed gold_panel_rebuild "), owed)
        self.assertEqual(owed, tick()[0], "the same versions are one key: started once")

    def test_a_gold_start_that_did_not_catch_up_is_started_again_after_a_backoff(self):
        # 2026-11-07 08:45Z the panel rebuild ended `waiting` behind a cut-over's rebuild lock, its key
        # was used, and the same owed versions would never have been started again.
        parcel = self.panels["gold.parcel_panel"]
        pins = {name: "5" for name in parcel.inputs}
        self.catalog["gold.parcel_panel"] = table(snap("601", None, 0, operation="overwrite", pins=pins))
        for name in parcel.inputs:
            self.catalog[name] = table(snap("5", None, 0))
        self.catalog["silver.land_transfer_history"] = table(snap("5", None, 0), snap("6", "5", 10))
        state, keys = {}, []

        def tick(minutes, busy=False):
            nonlocal state
            owed, _, state, _ = asset_reconcile.tick(T0 + 60 * minutes, [parcel], self.catalog.__getitem__,
                                                     self.reads.get, lambda service: busy, lambda job: "", state)
            keys.append(owed[0].split()[-1] if owed else None)

        tick(20)
        tick(25)
        self.assertEqual(keys[0], keys[1], "inside the backoff: the same key, started once")
        tick(31)
        self.assertNotEqual(keys[2], keys[0], "ten minutes on and still owed: a new start")
        for minutes in (35, 60, 160):
            tick(minutes, busy=True)
        self.assertEqual(keys[-1], None, "a running job is owed nothing")
        tick(170)
        self.assertEqual(keys[-1], keys[2], "the backoff counts from when the job last ran: twenty minutes")
        tick(181)
        self.assertNotIn(keys[-1], (keys[0], keys[2]))
        self.catalog["gold.parcel_panel"] = table(snap("601", None, 0, operation="overwrite", pins=pins),
                                                  snap("603", "601", 190, operation="overwrite",
                                                       pins={**pins, "silver.land_transfer_history": "6"}))
        tick(191)
        self.assertEqual((keys[-1], state["iceberg://gold.parcel_panel"].get("attempt")), (None, None),
                         "caught up: nothing owed and the attempts forgotten")

    def test_a_gold_run_its_time_limit_stopped_is_started_again_after_an_hour_whatever_its_attempt(self):
        # 2026-11-07: lock waits and two 160-minute timeouts had doubled the parcel panel's backoff to four
        # hours; its whole build now resumes from staged shards (root ADR-0440), so an hour is enough.
        parcel = self.panels["gold.parcel_panel"]
        pins = {name: "5" for name in parcel.inputs}
        self.catalog["gold.parcel_panel"] = table(snap("601", None, 0, operation="overwrite", pins=pins))
        for name in parcel.inputs:
            self.catalog[name] = table(snap("5", None, 0))
        self.catalog["silver.land_transfer_history"] = table(snap("5", None, 0), snap("6", "5", 10))
        url = "iceberg://gold.parcel_panel"
        state = {url: {"attempt": 5, "attempt_at": T0 + 60 * 20}}

        def key(minutes, timed_out):
            nonlocal state
            state[url].setdefault("attempt_of", asset_reconcile.versions_key({url: asset_reconcile.gold_verdict(
                parcel, self.catalog.__getitem__).versions}))
            owed, _, state, _ = asset_reconcile.tick(T0 + 60 * minutes, [parcel], self.catalog.__getitem__,
                                                     self.reads.get, lambda service: False, lambda job: "", state,
                                                     lambda service: timed_out)
            return owed[0].split()[-1]

        first = key(21, False)
        self.assertEqual(key(81, False), first, "a failure's sixth attempt waits four hours")
        self.assertNotEqual(key(81, True), first, "a time-limit stop waits an hour")

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

    def test_a_gold_not_built_yet_is_owed_its_first_build_and_its_lane_nothing(self):
        # 2026-10-30: a new kind's Gold (the court auction's, root ADR-0353) does not exist until its first
        # build, and the catalog answers 404; the unit price Gold stands in while that kind is off.
        import urllib.error

        def read(name):
            if name == "gold.unit_price":
                raise urllib.error.HTTPError("u", 404, "Not Found", {}, None)
            if name == "boom":
                raise urllib.error.HTTPError("u", 500, "Server Error", {}, None)
            return self.catalog[name]

        self.catalog["gold.unit_price"] = asset_reconcile.absent_is_empty(read, "gold.unit_price")
        self.assertEqual(self.catalog["gold.unit_price"], {"head": None, "snapshots": []})
        with self.assertRaises(urllib.error.HTTPError):
            asset_reconcile.absent_is_empty(read, "boom")
        del self.reads["unit-price"]
        owed, _, _, errors = self.tick()
        self.assertEqual(errors, [])
        self.assertTrue(any(line.startswith("foundation-asset-owed gold_unit_price_rebuild ") for line in owed))
        self.assertFalse(any("by_pnu_unit_price_bake" in line for line in owed))

    def test_an_asset_that_cannot_be_read_does_not_stop_the_others(self):
        del self.catalog["gold.unit_price"]
        owed, metrics, _, errors = self.tick()
        self.assertEqual(len(errors), 2)
        self.assertTrue(any("by_pnu_public_auction_bake" in line for line in owed))
        self.assertIn('foundation_asset_reconcile_error{asset="iceberg://gold.unit_price",job="gold_unit_price_rebuild"} 1',
                      metrics)
        self.assertTrue(metrics[-1].startswith("foundation_asset_reconcile_timestamp_seconds "))

    def panel_caught_up(self):
        for lane, asset in self.lanes.items():
            self.catalog[asset.lane_table] = table(snap(f"5{len(lane)}", None, 0))
            self.reads[lane] = {"read_snapshot_id": f"5{len(lane)}", "outcome": "served"}
        self.catalog["gold.building_panel"] = table(snap("201", None, 0, operation="overwrite",
                                                         pins={"silver.building_register_titles": "11",
                                                               "silver.building_register_units": "21"}))
        self.reads["building"] = {"read_snapshot_id": "201", "outcome": "served"}

    def panel_owed(self, owed):
        return [line for line in owed if line.startswith("foundation-asset-owed by_pnu_serving_bake ")]

    def test_2026_10_29_the_failed_forest_ledger_lane_is_owed_a_start_of_the_panel_bake(self):
        # 07:41Z: the lane failed inside `all` (a container name collision) and recorded no read; the
        # 10:15 start was deferred by takes_turns and ended unchanged. Its Gold is ahead of what it serves.
        self.panel_caught_up()
        self.reads["parcel-forest-ledger"] = None
        owed, metrics, _, errors = self.tick(panel=True)
        self.assertEqual(errors, [])
        [line] = self.panel_owed(owed)
        self.assertIn('foundation_asset_owed{asset="perfectory://parcel-forest-ledger-by-pnu-section-packs",'
                      'job="by_pnu_serving_bake"} 1', metrics)
        self.assertIn('foundation_asset_owed{asset="perfectory://parcel-zoning-by-pnu-section-packs",'
                      'job="by_pnu_serving_bake"} 0', metrics)
        # A second owed lane is the same one start, under a key of both lanes' versions.
        self.catalog["gold.parcel_zoning"] = table(*self.catalog["gold.parcel_zoning"]["snapshots"],
                                                   snap("777", self.catalog["gold.parcel_zoning"]["head"], 20))
        owed, _, _, _ = self.tick(panel=True)
        [both] = self.panel_owed(owed)
        self.assertNotEqual(both, line)
        # While the bake runs it is not started again; the lane lock and the pool stay the bake's.
        self.running = {"foundation-by-pnu-serving-bake.service"}
        owed, _, _, _ = self.tick(panel=True)
        self.assertEqual(self.panel_owed(owed), [])

    def test_a_failed_lane_is_owed_again_after_its_backoff_under_a_new_key(self):
        # 2026-10-29 07:19Z and 07:40Z the unit price bake failed; every tick after owed the key already
        # started, so the trigger skipped it as existing. A failure is now a version of its own.
        self.catalog["gold.unit_price"] = table(*self.catalog["gold.unit_price"]["snapshots"], snap("302", "301", 20))
        owed, _, _, _ = self.tick(30)
        [before] = [line for line in owed if " by_pnu_unit_price_bake " in line]
        self.reads["unit-price"] = {"read_snapshot_id": "301", "outcome": "failed", "at": at(32)[:19] + "Z",
                                    "consecutive_failures": 1}
        owed, metrics, state, _ = self.tick(35)
        self.assertFalse(any(" by_pnu_unit_price_bake " in line for line in owed), "inside its ten-minute backoff")
        self.assertNotIn("unscheduled_since", state["perfectory://unit-price-by-pnu-section-packs"])
        owed, _, _, _ = self.tick(43)
        [first] = [line for line in owed if " by_pnu_unit_price_bake " in line]
        self.assertNotEqual(first, before)
        # The second failure in a row waits twenty minutes, and owes another key.
        self.reads["unit-price"] = {**self.reads["unit-price"], "at": at(50)[:19] + "Z", "consecutive_failures": 2}
        owed, _, _, _ = self.tick(65)
        self.assertFalse(any(" by_pnu_unit_price_bake " in line for line in owed))
        owed, _, _, _ = self.tick(71)
        [second] = [line for line in owed if " by_pnu_unit_price_bake " in line]
        self.assertNotIn(second, (before, first))
        self.assertEqual(asset_reconcile.failure_backoff(9), 4 * 3600)

    def test_a_bake_finished_at_its_pin_is_owed_the_patch_forward(self):
        # Root ADR-0310: a full bake finishes at the snapshot it pinned and the next run patches it to
        # the head; the read moved, so the versions owed did, though the head did not.
        self.catalog["gold.parcel_public_auction"] = table(*self.catalog["gold.parcel_public_auction"]["snapshots"],
                                                           snap("103", "102", 35))
        owed, _, _, _ = self.tick()
        self.reads["parcel-public-auction"]["read_snapshot_id"] = "102"
        forward, _, _, _ = self.tick()
        self.assertEqual(len(forward), 1)
        self.assertNotEqual(forward, owed)

    def behind(self, metrics, lane):
        found = [line for line in metrics if line.startswith(f'foundation_by_pnu_lane_behind_seconds{{lane="{lane}"}} ')]
        return float(found[0].split()[-1]) if found else None

    def test_2026_11_07_a_lane_serving_its_newest_inputs_is_not_behind_however_old_its_manifest(self):
        # Root ADR-0444: zoning served 10-17's packs, which reflect its Gold, built from the newest Silver
        # there was; the parcel lane's Gold was built before a transfer history load it never served.
        self.panel_caught_up()
        self.catalog["silver.land_use_plan"] = table(snap("61", None, 0), snap("62", "61", 5, operation="replace", added=9))
        self.catalog["gold.parcel_zoning"] = table(snap("71", None, 0, operation="overwrite",
                                                        pins={"silver.land_use_plan": "61"}))
        self.reads["parcel-zoning"] = {"read_snapshot_id": "71", "outcome": "served"}
        self.catalog["silver.land_transfer_history"] = table(snap("81", None, 0), snap("82", "81", 10), snap("83", "82", 20))
        self.catalog["gold.parcel_panel"] = table(snap("91", None, 0, operation="overwrite",
                                                       pins={"silver.land_transfer_history": "81"}))
        self.reads["parcel"] = {"read_snapshot_id": "91", "outcome": "served"}
        _, metrics, _, errors = self.tick(100, panel=True)
        self.assertEqual(errors, [])
        self.assertEqual(self.behind(metrics, "parcel-zoning"), 0, "a compaction changes no row")
        self.assertEqual(self.behind(metrics, "parcel"), 90 * 60, "since the first load it does not serve")
        # A Gold built from that load and not yet baked: still behind since the load, not the Gold.
        self.catalog["gold.parcel_panel"] = table(*self.catalog["gold.parcel_panel"]["snapshots"],
                                                  snap("92", "91", 60, operation="overwrite",
                                                       pins={"silver.land_transfer_history": "83"}))
        _, metrics, _, _ = self.tick(100, panel=True)
        self.assertEqual(self.behind(metrics, "parcel"), 90 * 60)
        self.reads["parcel"] = {"read_snapshot_id": "92", "outcome": "served"}
        _, metrics, _, _ = self.tick(100, panel=True)
        self.assertEqual(self.behind(metrics, "parcel"), 0)

    def test_a_lane_whose_data_time_cannot_be_told_has_no_series(self):
        # No read yet, or a Gold that records no pins: the alert falls back to the manifest's age.
        self.panel_caught_up()
        self.reads["parcel-forest-ledger"] = None
        _, metrics, _, _ = self.tick(panel=True)
        self.assertIsNone(self.behind(metrics, "parcel-forest-ledger"))
        self.assertIsNone(self.behind(metrics, "parcel-zoning"), "its synthetic Gold records no pins")
        self.assertEqual(self.behind(metrics, "building"), 0)

    def test_2026_11_08_a_gold_that_only_recorded_new_pins_serves_them(self):
        # 12:32Z gold.building_panel committed an empty append with the Silver pins it was rebuilt from
        # (no PNU's row changed); the lane still served the snapshot below it and read 106 hours behind.
        self.panel_caught_up()
        self.catalog["silver.building_register_titles"] = table(snap("11", None, 0), snap("12", "11", 10))
        _, metrics, _, _ = self.tick(100, panel=True)
        self.assertEqual(self.behind(metrics, "building"), 90 * 60)
        self.catalog["gold.building_panel"] = table(*self.catalog["gold.building_panel"]["snapshots"],
                                                    snap("202", "201", 60, added=0,
                                                         pins={"silver.building_register_titles": "12",
                                                               "silver.building_register_units": "21"}))
        owed, metrics, _, _ = self.tick(100, panel=True)
        self.assertEqual(self.behind(metrics, "building"), 0)
        self.assertEqual(self.panel_owed(owed), [], "no row to bake")

    def test_2026_11_08_a_kind_is_owed_when_its_host_panel_moved_its_pins(self):
        # 08:45Z gold.parcel_panel committed a new land transfer history pin; the kind rebuild's daily
        # start ended `waiting` on the spark pool and nothing started it again (it was not reconciled).
        kind = self.kinds["gold.parcel_transfer_history"]
        self.assertEqual((kind.inputs, kind.pins_from["gold_table"], kind.service),
                         ((), "gold.parcel_panel", "foundation-gold-kind-rebuild.service"))
        self.catalog["silver.land_transfer_history"] = table(snap("81", None, 0), snap("82", "81", 10))
        self.catalog["gold.parcel_panel"] = table(snap("91", None, 0, operation="overwrite",
                                                       pins={"silver.land_transfer_history": "81",
                                                             "silver.land_use_plan": "61"}))
        self.catalog["gold.parcel_transfer_history"] = table(snap("95", None, 0, operation="overwrite",
                                                                  pins={"silver.land_transfer_history": "81"}))
        tick = lambda state=None: asset_reconcile.tick(T0 + 6000, [kind], self.catalog.__getitem__, self.reads.get,
                                                       lambda service: False, lambda job: "", state or {})
        owed, _, _, errors = tick()
        self.assertEqual((owed, errors), ([], []), "its host has not moved: caught up")
        self.catalog["gold.parcel_panel"] = table(*self.catalog["gold.parcel_panel"]["snapshots"],
                                                  snap("92", "91", 60, operation="overwrite",
                                                       pins={"silver.land_transfer_history": "82",
                                                             "silver.land_use_plan": "61"}))
        owed, metrics, _, _ = tick()
        self.assertEqual(len(owed), 1)
        self.assertRegex(owed[0], r"^foundation-asset-owed gold_kind_rebuild ")
        self.assertIn('foundation_asset_owed{asset="iceberg://gold.parcel_transfer_history",job="gold_kind_rebuild"} 1',
                      metrics)

    def test_the_lane_reads_count_the_failures_in_a_row(self):
        import tempfile
        with tempfile.TemporaryDirectory() as root:
            lane = pathlib.Path(root, "parcel-forest-ledger")
            lane.mkdir()
            lines = [{"outcome": "failed"}, {"outcome": "served", "read_snapshot_id": "1"},
                     {"outcome": "failed", "read_snapshot_id": "1"}, {"outcome": "failed", "read_snapshot_id": "1"}]
            (lane / "reads.jsonl").write_text("".join(json.dumps(line) + "\n" for line in lines), encoding="utf-8")
            read = asset_reconcile.last_read(pathlib.Path(root))
            self.assertEqual(read("parcel-forest-ledger")["consecutive_failures"], 2)
            self.assertIsNone(read("parcel-zoning"))

    def test_the_bridge_networks_are_counted_against_the_address_pools(self):
        # 2026-10-29: 23 of 31 taken, 10 by per-run Compose projects whose cleanup failed.
        def docker(networks, pools):
            return lambda command: pools if "info" in command else networks
        text = asset_reconcile.bridge_networks(docker("a\n" * 23, "null\n"))
        self.assertIn("foundation_docker_bridge_networks 23\n", text)
        self.assertIn("foundation_docker_address_pool_networks 31\n", text, "Docker's default pools")
        configured = json.dumps([{"Base": "198.51.100.0/24", "Size": 28}])  # RFC 5737
        self.assertIn("foundation_docker_address_pool_networks 16\n",
                      asset_reconcile.bridge_networks(docker("", configured)))
        rules = (PLATFORM / "infra/observability/prometheus/foundation-api.rules.yml").read_text(encoding="utf-8")
        self.assertIn("alert: FoundationDockerAddressPoolNearlyFull", rules)
        self.assertIn('node_textfile_mtime_seconds{file=~".*/docker-networks.prom"}', rules)

    def test_each_running_container_is_named_by_the_cgroup_cadvisor_reads(self):
        # Root ADR-0385: cAdvisor runs without the engine's socket and labels a container only by
        # its cgroup under the systemd driver; the near-cap alert looks the name up from here.
        text = asset_reconcile.container_names(lambda command: "b2 synthetic-trino\na1 synthetic-spark-run\n")
        self.assertIn('foundation_docker_container_info{id="/system.slice/docker-a1.scope",name="synthetic-spark-run"} 1\n',
                      text)
        self.assertLess(text.index("docker-a1"), text.index("docker-b2"), "sorted, so a tick that changes nothing writes the same file")
        rules = (PLATFORM / "infra/observability/prometheus/foundation-api.rules.yml").read_text(encoding="utf-8")
        self.assertIn('foundation_docker_container_info{id=%q}', rules)

    def test_a_failed_unit_is_failing_from_its_first_failure_until_a_clean_run(self):
        # Root ADR-0445: 2026-11-01..07 nine units stayed failed for days after one notice each; the
        # hourly serving monitors failed every run, so "failed since" must span their runs.
        units = {
            "foundation-x-drill.service": {"Id": "foundation-x-drill.service", "ActiveState": "failed",
                                           "StateChangeTimestamp": "@1000", "FragmentPath": "/units/foundation-x-drill.service",
                                           "Documentation": "file:///opt/foundation-platform/current/docs/runbooks/x.md"},
            "foundation-x-retired.service": {"Id": "foundation-x-retired.service", "ActiveState": "failed",
                                             "StateChangeTimestamp": "@2000", "FragmentPath": "", "Documentation": ""},
        }
        listed = [{"unit": name} for name in units]
        files = {"/units/foundation-x-drill.service": "[Unit]\nX-Foundation-Owner=platform\n"}

        def tick(tracked):
            return asset_reconcile.failed_units(lambda: [entry for entry in listed if units[entry["unit"]]["ActiveState"] == "failed"],
                                                lambda names: {name: units[name] for name in names if name in units},
                                                tracked, read_text=files.__getitem__)

        text, tracked = tick({})
        self.assertIn('foundation_unit_failed_since_seconds{unit="foundation-x-drill.service",owner="platform",'
                      'runbook="docs/runbooks/x.md"} 1000\n', text)
        self.assertIn('{unit="foundation-x-retired.service",owner="",runbook=""} 2000\n', text, "no file, no owner")
        self.assertIn("foundation_units_failed 2\n", text)
        # The drill runs again (activating) and fails again later: still failing since 1000.
        units["foundation-x-drill.service"].update(ActiveState="activating", StateChangeTimestamp="@5000")
        _, tracked = tick(tracked)
        self.assertEqual(tracked["foundation-x-drill.service"], 1000)
        units["foundation-x-drill.service"].update(ActiveState="failed", StateChangeTimestamp="@6000")
        text, tracked = tick(tracked)
        self.assertIn('runbook="docs/runbooks/x.md"} 1000\n', text)
        # A clean run ends it; the next failure starts over.
        units["foundation-x-drill.service"].update(ActiveState="inactive", StateChangeTimestamp="@7000")
        text, tracked = tick(tracked)
        self.assertNotIn("foundation-x-drill.service", tracked)
        self.assertIn("foundation_units_failed 1\n", text)
        rules = (PLATFORM / "infra/observability/prometheus/foundation-api.rules.yml").read_text(encoding="utf-8")
        self.assertIn('node_textfile_mtime_seconds{file=~".*/foundation-units.prom"}', rules)
        self.assertEqual(asset_reconcile.UNITS_METRICS_NAME, "foundation-units.prom")

    def test_a_dagster_location_that_does_not_load_is_said_with_the_last_comparison(self):
        # Root ADR-0431: 2026-11-06 21:16Z..11-09 the code location failed every load, the UI answered,
        # the deploy said "observing" and the ledger got no line. The textfile says the load error, and
        # the last comparison comes from the event log, so it is known while the location is down.
        observing = {"observers": {"dagster": {"enabled": True, "ui": "http://127.0.0.1:19085"}}}
        dag_ids = {"synthetic_b": "foundation_synthetic_b", "synthetic_a": "foundation_synthetic_a"}
        asked = []

        def post(query):
            asked.append(query)
            return {"workspaceOrError": {"locationEntries": [
                        {"name": "definitions.py", "locationOrLoadError": {"__typename": "PythonError"}}]},
                    "j0": [{"evaluation": {"timestamp": 1800000000.5}}], "j1": []}

        text = asset_reconcile.dagster_state(observing, dag_ids, post)
        self.assertIn("foundation_dagster_observing 1\n", text)
        self.assertIn("foundation_dagster_answered 1\n", text)
        self.assertIn('foundation_dagster_code_location_loaded{location="definitions.py"} 0\n', text)
        self.assertIn("foundation_dual_run_jobs 2\n", text)
        self.assertIn('foundation_dual_run_last_comparison_timestamp_seconds{job="synthetic_a"} 1800000000.5\n', text)
        self.assertNotIn('job="synthetic_b"', text, "a job that never compared has no time")
        self.assertIn('j0: assetCheckExecutions(assetKey: {path: ["foundation_airflow", "dag", "foundation_synthetic_a"]},'
                      ' checkName: "dual_run_matches_airflow", limit: 1)', asked[0], "one query, in job order")
        loaded = asset_reconcile.dagster_state(observing, dag_ids, lambda query: {"workspaceOrError": {"locationEntries": [
            {"name": "definitions.py", "locationOrLoadError": {"__typename": "RepositoryLocation"}}]}})
        self.assertIn('{location="definitions.py"} 1\n', loaded)

        def refused(query):
            raise OSError("connection refused")

        self.assertIn("foundation_dagster_answered 0\n", asset_reconcile.dagster_state(observing, dag_ids, refused))
        off = asset_reconcile.dagster_state({"observers": {"dagster": {"enabled": False}}}, dag_ids, refused)
        self.assertIn("foundation_dagster_observing 0\n", off)
        self.assertNotIn("\nfoundation_dagster_answered ", off, "a switched-off Dagster is not asked")
        rules = (PLATFORM / "infra/observability/prometheus/foundation-api.rules.yml").read_text(encoding="utf-8")
        for name in ("alert: FoundationDagsterCodeLocationDown", "alert: FoundationDualRunSilent",
                     'node_textfile_mtime_seconds{file=~".*/dagster.prom"}'):
            self.assertIn(name, rules)
        self.assertEqual(asset_reconcile.DAGSTER_METRICS_NAME, "dagster.prom")

    def test_the_dual_run_check_is_named_as_the_code_location_names_it(self):
        # The host has no Dagster to import dual_run from; its names are read from the file.
        import ast

        tree = ast.parse((ORCHESTRATION / "dagster/dual_run.py").read_text(encoding="utf-8"))
        names = {target.id: node.value.value for node in tree.body if isinstance(node, ast.Assign)
                 for target in node.targets if isinstance(target, ast.Name) and isinstance(node.value, ast.Constant)}
        self.assertEqual(asset_reconcile.DUAL_RUN_CHECK, names["CHECK_NAME"])
        self.assertEqual(asset_reconcile.DUAL_RUN_KEY_PREFIX, (names["INSTANCE_NAME"], "dag"))
        self.assertIn('AssetKey([INSTANCE_NAME, "dag", spec.dag_id])', (ORCHESTRATION / "dagster/dual_run.py").read_text(encoding="utf-8"))

    def test_a_label_value_keeps_systemds_escapes(self):
        self.assertEqual(asset_reconcile.label(r'foundation-x@a\x2db.service'), r'foundation-x@a\\x2db.service')

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


class QueuedStarts(unittest.TestCase):
    """Root ADR-0462, the 2026-11-09 07:00Z replay: ten first Silver loads were refused by the spark pool
    behind a lane cut-over's claim, nothing recorded them, and their next try was the next day's 07:00Z.
    A refused start is queued (job_units.py queue) and the reconciler starts it again."""

    JOB = "silver_refresh_building_permit_site_location"

    @classmethod
    def setUpClass(cls):
        cls.jobs = json.loads(job_specs.JOBS.read_text(encoding="utf-8"))

    def owed(self, record, running=False, full="", now=T0):
        return asset_reconcile.queued_starts(now, self.jobs, {self.JOB: record}, {}, lambda service: running,
                                             lambda job: full)

    def test_a_refused_start_is_started_again_once_its_backoff_passed_and_its_pool_has_room(self):
        record = {"since": T0 - 7200, "attempts": 1, "last_at": T0 - 60, "reason": "kept for claims"}
        owed, metrics = self.owed(record)
        self.assertEqual(owed, [], "asked again before its backoff passed")
        owed, _ = self.owed(record, now=T0 + 300)
        (line,) = owed
        self.assertTrue(line.startswith(f"foundation-asset-owed {self.JOB} "))
        self.assertEqual(job_specs.owed_runs(line, (self.JOB,)), [(self.JOB, line.split()[2])])
        # Each refusal is a new key, so the DAG starts each retry once.
        again, _ = self.owed({**record, "attempts": 2, "last_at": T0 - 600}, now=T0 + 300)
        self.assertNotEqual(again, owed)
        for case in ({"running": True}, {"full": "the spark pool is kept for claims queued before it"}):
            with self.subTest(**case):
                self.assertEqual(self.owed(record, now=T0 + 300, **case)[0], [])
        wait = next(line for line in metrics if line.startswith("foundation_job_queue_wait_seconds{"))
        self.assertIn(f'job="{self.JOB}",pool="spark",priority_class="default"', wait)
        self.assertTrue(wait.endswith(" 7200"))
        self.assertIn(f'foundation_job_queue_wait_budget_seconds{{job="{self.JOB}",pool="spark",priority_class="default"}} 21600',
                      metrics)

    def test_the_backoff_doubles_to_an_hour(self):
        self.assertEqual([asset_reconcile.queue_backoff(n) for n in (1, 2, 3, 5, 9)], [300, 600, 1200, 3600, 3600])

    def test_the_reconciler_may_start_every_enabled_spark_job_airflow_starts(self):
        # The DAG starts only what the reconciler is allowed to: its reconciled jobs and the queued ones.
        (spec,) = [spec for spec in job_specs.load_specs() if spec.reconciles]
        self.assertIn(self.JOB, spec.requeues)
        self.assertIn("data_go_kr_file_data", spec.requeues)
        self.assertFalse(set(spec.requeues) & set(spec.reconciles))
        self.assertFalse([job["id"] for job in self.jobs["jobs"]
                          if job["id"] in spec.requeues and (not job["enabled"] or job["pool"] != "spark")])

    def test_the_wait_alert_compares_each_job_to_its_budget(self):
        rules = (PLATFORM / "infra/observability/prometheus/foundation-api.rules.yml").read_text(encoding="utf-8")
        self.assertIn("alert: FoundationJobQueueWaitOverBudget", rules)
        self.assertRegex(rules, r"foundation_job_queue_wait_seconds > on\(job, pool, priority_class\) "
                                r"foundation_job_queue_wait_budget_seconds")
        for name, entry in self.jobs["priority_classes"].items():
            self.assertIsInstance(entry.get("queue_wait_budget_minutes"), int, name)


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
