"""The AWS code location loads, and each collector lane starts by its condition (root ADR-0623 §1, §3, §4).

orchestration/dagster/aws/definitions.py as Dagster 1.13 resolves it: one asset and one automation sensor per
lane and no schedule; the sensor carries the lane's role, size, the platform's run ceiling and the kind and
lane tags; the read asset has Dagster's own FreshnessPolicy at the job's landed bound. And the start condition
itself, evaluated across a day: nothing before the tick, one request after it, none again once handled, none
while a run of the lane is in progress, and the kept tick after that run ends. Skips where Dagster is not
installed; the observer's image runs it (scripts/verify/foundation-dagster-observe.sh).
"""

import datetime
import importlib.util
import json
import pathlib
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
AWS = PLATFORM / "orchestration" / "dagster" / "aws"
ILIS = "industrial_land_sale_notice_ilis_collect"
UTC = datetime.timezone.utc

try:
    import dagster  # noqa: F401
except ImportError:  # pragma: no cover - the default runner has no Dagster
    dagster = None


@unittest.skipIf(dagster is None, "Dagster is not installed here")
class TheAwsLocationLoads(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location("aws_definitions", AWS / "definitions.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        cls.defs = module.defs
        import collect_lane

        cls.collect_lane = collect_lane
        cls.platform = collect_lane.contract()
        cls.lanes = cls.platform["collect"]["lanes"]
        cls.repository = cls.defs.get_repository_def()

    def test_one_asset_and_one_sensor_per_lane_and_no_schedule(self):
        from dagster import AssetKey

        keys = set(self.repository.asset_graph.get_all_asset_keys())
        for job_id, lane in self.lanes.items():
            self.assertIn(AssetKey(lane["asset_key"]), keys, job_id)
            self.assertIn(AssetKey(["source_read", job_id]), keys, job_id)
        self.assertEqual(len(keys), 2 * len(self.lanes))
        self.assertEqual(self.repository.schedule_defs, [])
        self.assertEqual(sorted(sensor.name for sensor in self.repository.sensor_defs),
                         sorted(f"{job_id}_automation" for job_id in self.lanes))

    def test_the_sensor_carries_the_lane_s_role_size_ceiling_and_tags(self):
        from dagster import DefaultSensorStatus

        for job_id, lane in self.lanes.items():
            sensor = self.defs.resolve_sensor_def(f"{job_id}_automation")
            self.assertEqual(sensor.default_status, DefaultSensorStatus.RUNNING)
            tags = sensor.run_tags
            self.assertEqual(json.loads(tags["ecs/task_overrides"])["taskRoleArn"],
                             self.collect_lane.lane_role_arn(self.platform, lane))
            self.assertIn("/run/", json.loads(tags["ecs/task_overrides"])["taskRoleArn"])
            self.assertEqual(tags["dagster/max_runtime"], str(self.platform["collect"]["run_ceiling_seconds"]))
            self.assertEqual((tags["perfectory-platform/kind"], tags["perfectory-platform/lane"]), ("collect", job_id))

    def test_the_read_asset_is_held_to_the_landed_bound_by_dagster_s_own_policy(self):
        from dagster import AssetKey

        for job_id in self.lanes:
            node = self.repository.asset_graph.get(AssetKey(["source_read", job_id]))
            bound = self.collect_lane.job(job_id)["expected_outcome"]["landed_within_hours"]
            self.assertEqual(node.freshness_policy.fail_window.to_timedelta(), datetime.timedelta(hours=bound))

    def test_the_start_condition_across_a_day(self):
        from dagster import AssetKey, DagsterInstance, DagsterRunStatus, evaluate_automation_conditions
        from dagster._core.events import AssetMaterializationPlannedData, DagsterEvent, DagsterEventType
        from dagster._core.test_utils import create_run_for_test

        lane = self.lanes[ILIS]
        hour = int(self.collect_lane.start_cron(lane).split()[1])
        key = AssetKey(lane["asset_key"])
        instance = DagsterInstance.ephemeral()
        cursor = None

        def requested(day, minute_after_tick):
            nonlocal cursor
            when = datetime.datetime(2026, 11, day, hour, 0, tzinfo=UTC) + datetime.timedelta(minutes=minute_after_tick)
            result = evaluate_automation_conditions(defs=self.defs, instance=instance, evaluation_time=when, cursor=cursor)
            cursor = result.cursor
            return result.get_num_requested(key)

        self.assertEqual(requested(26, -30), 0, "before the tick")
        self.assertEqual(requested(26, 5), 1, "after the tick")
        self.assertEqual(requested(26, 10), 0, "the tick is handled")
        run = create_run_for_test(instance, job_name=ILIS, asset_selection={key}, status=DagsterRunStatus.STARTED)
        instance.report_dagster_event(DagsterEvent(event_type_value=DagsterEventType.ASSET_MATERIALIZATION_PLANNED.value,
                                                   job_name=ILIS, event_specific_data=AssetMaterializationPlannedData(key)),
                                      run_id=run.run_id)
        self.assertEqual(requested(27, 5), 0, "a run of the lane is in progress")
        instance.report_run_failed(run)
        self.assertEqual(requested(27, 60), 1, "the kept tick after that run ended")


if __name__ == "__main__":
    unittest.main()
