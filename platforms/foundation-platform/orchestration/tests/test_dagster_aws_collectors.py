"""The AWS code location makes one asset and one automation sensor per lane, starts a lane only at its beat and
never while it runs, and reports what a run did as Dagster's own events (root ADR-0616 §6, ADR-0623 §1 and §4;
aws/definitions.py).

In process with Dagster 1.13 (skipped where Dagster is not installed), collect_lane.collect replaced:
- a lane is an asset `collect/<job>` with the checks `ready` and `collected`, and its sensor carries the lane's
  run tags; a handoff lane is defined only while its job is off on the host;
- a run that wrote something materializes the asset, a quiet run only passes `collected`, a lane not ready fails
  `ready` and materializes nothing, a failed run records `collected` failed and fails;
- a source that drops what ended has `source_read/<job>` with a FreshnessPolicy at its landed bound, and the run
  materializes it when it read the source to the end;
- the start condition requests a run when the beat's tick has passed, once, and not before;
- a lane the contract pauses has no asset, no sensor and no read check (root ADR-0626 §8 N2).

The lane behaviour is tested with the contract's pauses cleared: a pause is a state of the contract, not
of what a running lane does.
"""

import datetime
import importlib
import pathlib
import sys
import unittest
from unittest import mock

try:
    import dagster
except ImportError:  # pragma: no cover - CI installs it
    dagster = None

AWS = pathlib.Path(__file__).resolve().parents[1] / "dagster" / "aws"


def load(hosted=None, contract=None):
    """aws/definitions.py loaded afresh; `hosted` replaces the host's job set when given, and `contract` the AWS
    contract (by default the real one with every lane's pause cleared)."""
    sys.path.insert(0, str(AWS))
    sys.path.insert(0, str(AWS.parent))
    import collect_lane

    real = collect_lane.contract

    def unpaused():
        platform = real()
        for lane in platform["collect"]["lanes"].values():
            lane.pop("paused", None)
            lane.pop("paused_reason", None)
        return platform

    patches = [mock.patch.object(collect_lane, "contract", contract or unpaused)]
    if hosted is not None:
        patches.append(mock.patch.object(collect_lane, "on_host", return_value=set(hosted)))
    for patch in patches:
        patch.start()
    try:
        sys.modules.pop("aws_collectors_definitions", None)
        spec = importlib.util.spec_from_file_location("aws_collectors_definitions", AWS / "definitions.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module, collect_lane
    finally:
        for patch in patches:
            patch.stop()


@unittest.skipIf(dagster is None, "Dagster is not installed here")
class OneAssetAndOneSensorPerLane(unittest.TestCase):
    def test_overlap_lanes_always_and_handoff_lanes_only_when_off_on_the_host(self):
        module, collect_lane = load(hosted={"rt_molit_real_transaction_collect", "onbid_public_auction_collect"})
        lanes = collect_lane.contract()["collect"]["lanes"]
        keys = {key.to_user_string() for key in module.defs.get_repository_def().asset_graph.get_all_asset_keys()}
        for job_id, lane in lanes.items():
            # An overlap lane always runs, and so does a lane with no host copy (`none`, the RTMS lane).
            self.assertEqual(f"collect/{job_id}" in keys, lane["handover"] in ("overlap", "none"), job_id)
        module, _ = load(hosted=set())
        keys = {key.to_user_string() for key in module.defs.get_repository_def().asset_graph.get_all_asset_keys()}
        self.assertTrue({f"collect/{job_id}" for job_id in lanes} <= keys)
        self.assertIn("source_read/onbid_public_auction_collect", keys)

    def test_each_sensor_carries_its_lane_s_run_tags(self):
        module, collect_lane = load(hosted=set())
        platform = collect_lane.contract()
        sensors = {sensor.name: sensor for sensor in module.defs.get_repository_def().sensor_defs}
        for job_id, lane in platform["collect"]["lanes"].items():
            self.assertEqual(sensors[f"{job_id}_automation"].run_tags, collect_lane.run_tags(platform, job_id, lane))

    def test_a_paused_lane_has_no_asset_no_sensor_and_no_read_check(self):
        _, collect_lane = load(hosted=set())
        real = collect_lane.contract

        def paused():
            platform = real()
            platform["collect"]["lanes"]["onbid_public_auction_collect"]["paused"] = True
            return platform

        module, _ = load(hosted=set(), contract=paused)
        keys = {key.to_user_string() for key in module.defs.get_repository_def().asset_graph.get_all_asset_keys()}
        sensors = {sensor.name for sensor in module.defs.get_repository_def().sensor_defs}
        self.assertNotIn("collect/onbid_public_auction_collect", keys)
        self.assertNotIn("source_read/onbid_public_auction_collect", keys)
        self.assertNotIn("onbid_public_auction_collect_automation", sensors)
        self.assertIn("collect/pension_workplace_collect", keys)

    def test_a_source_that_drops_what_ended_is_held_to_its_landed_bound(self):
        module, collect_lane = load(hosted=set())
        node = module.defs.get_repository_def().asset_graph.get(dagster.AssetKey(["source_read", "onbid_public_auction_collect"]))
        bound = collect_lane.job("onbid_public_auction_collect")["expected_outcome"]["landed_within_hours"]
        self.assertEqual(node.freshness_policy.fail_window.to_timedelta(), datetime.timedelta(hours=bound))


@unittest.skipIf(dagster is None, "Dagster is not installed here")
class WhatARunReports(unittest.TestCase):
    JOB = "onbid_public_auction_collect"

    def run_lane(self, collect):
        module, collect_lane = load(hosted=set())
        asset = next(definition for definition in module.defs.assets if getattr(definition, "node_def", None)
                     and definition.node_def.name == self.JOB)
        with mock.patch.object(collect_lane, "collect", collect), mock.patch.dict(sys.modules, {"boto3": mock.MagicMock()}):
            return dagster.materialize([asset], raise_on_error=False), collect_lane

    def checks(self, result):
        return {event.asset_check_evaluation_data.check_name: event.asset_check_evaluation_data.passed
                for event in result.all_events if event.event_type_value == "ASSET_CHECK_EVALUATION"}

    def materialized(self, result):
        return {event.asset_key.to_user_string() for event in result.get_asset_materialization_events()}

    def test_a_run_that_wrote_materializes_and_passes_both_checks(self):
        result, _ = self.run_lane(lambda *a, **k: {"outcome": "changed", "state_read": "s", "state_written": "w",
                                                  "seconds": 1.0})
        self.assertTrue(result.success)
        self.assertEqual(self.checks(result), {"ready": True, "collected": True})
        self.assertEqual(self.materialized(result), {f"collect/{self.JOB}", f"source_read/{self.JOB}"})

    def test_a_quiet_run_passes_its_check_and_materializes_only_the_read(self):
        result, _ = self.run_lane(lambda *a, **k: {"outcome": "unchanged", "state_read": "s", "state_written": None,
                                                  "seconds": 1.0})
        self.assertTrue(result.success)
        self.assertEqual(self.checks(result), {"ready": True, "collected": True})
        self.assertEqual(self.materialized(result), {f"source_read/{self.JOB}"})

    def test_a_lane_not_ready_fails_ready_and_materializes_nothing(self):
        def not_ready(*args, **kwargs):
            raise collect_lane.NotReady("the source key slot has no value yet")

        _, collect_lane = load(hosted=set())
        result, _ = self.run_lane(not_ready)
        self.assertTrue(result.success)
        self.assertEqual(self.checks(result), {"ready": False, "collected": False})
        self.assertEqual(self.materialized(result), set())

    def test_a_failed_run_records_collected_failed_and_fails(self):
        _, collect_lane = load(hosted=set())

        def failed(*args, **kwargs):
            raise collect_lane.RunFailed("collect-data-go-kr-onbid exited 1 (failed)")

        result, _ = self.run_lane(failed)
        self.assertFalse(result.success)
        self.assertEqual(self.checks(result), {"ready": True, "collected": False})


@unittest.skipIf(dagster is None, "Dagster is not installed here")
class TheBeatStartsIt(unittest.TestCase):
    def test_one_request_after_the_tick_and_none_before(self):
        module, collect_lane = load(hosted=set())
        key = dagster.AssetKey(["collect", "pension_workplace_collect"])
        cron, _ = collect_lane.beat(collect_lane.contract()["collect"]["lanes"]["pension_workplace_collect"])
        minute, hour = (int(part) for part in cron.split()[:2])
        day = datetime.datetime(2026, 11, 27, tzinfo=datetime.timezone.utc)
        before = day + datetime.timedelta(hours=hour, minutes=minute) - datetime.timedelta(minutes=5)
        instance = dagster.DagsterInstance.ephemeral()
        first = dagster.evaluate_automation_conditions(defs=module.defs, instance=instance, asset_selection=dagster.AssetSelection.assets(key),
                                                       evaluation_time=before)
        self.assertEqual(first.total_requested, 0)
        second = dagster.evaluate_automation_conditions(defs=module.defs, instance=instance, asset_selection=dagster.AssetSelection.assets(key),
                                                        evaluation_time=before + datetime.timedelta(minutes=10),
                                                        cursor=first.cursor)
        self.assertEqual(second.total_requested, 1)
        third = dagster.evaluate_automation_conditions(defs=module.defs, instance=instance, asset_selection=dagster.AssetSelection.assets(key),
                                                       evaluation_time=before + datetime.timedelta(minutes=15),
                                                       cursor=second.cursor)
        self.assertEqual(third.total_requested, 0, "a handled tick is not asked again")


if __name__ == "__main__":
    unittest.main()
