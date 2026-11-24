"""A collector of a source whose provider drops what has ended is held to its reads by Dagster's
freshness check (orchestration/dagster/source_read_checks.py, root ADR-0614).

The court auction site drops a listing once its sale ends. 2026-10-21..11-05 its collector was
switched off for 16 nights and nothing judged a job that was off; 11-07 and 11-12 two more nights
landed nothing. Run by the CI `dagster-observe` lane in the code location's image; skipped elsewhere,
where a skip fails that lane.
"""

import datetime
import importlib.util
import pathlib
import sys
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "orchestration" / "dags"))
sys.path.insert(0, str(PLATFORM / "orchestration" / "dagster"))

HAS_DAGSTER = importlib.util.find_spec("dagster") is not None
NEEDS_DAGSTER = "requires dagster (the CI dagster-observe lane runs it)"


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class SourceReadChecksTest(unittest.TestCase):
    def setUp(self):
        import observe_assets
        import source_read_checks

        self.checks = source_read_checks
        self.jobs, self.graph = observe_assets.contracts()

    def test_every_job_that_declares_a_landing_bound_has_a_read_asset_and_its_check(self):
        from dagster import Definitions

        import observe_assets

        bounds = self.checks.bounds(self.jobs)
        self.assertEqual(bounds["court_auction_collect"], 30)
        self.assertEqual(set(bounds), {job["id"] for job in self.jobs["jobs"]
                                        if (job.get("expected_outcome") or {}).get("landed_within_hours")})
        assets, checks = self.checks.assets(self.jobs), self.checks.checks(self.jobs)
        Definitions(assets=[*observe_assets.observed_assets(self.jobs, self.graph), *assets],
                    asset_checks=checks).resolve_asset_graph()
        self.assertEqual({spec.key for spec in assets}, {self.checks.read_key(job_id) for job_id in bounds})
        specs = [spec for check in checks for spec in check.check_specs]
        self.assertEqual({spec.asset_key for spec in specs}, {self.checks.read_key(job_id) for job_id in bounds})
        self.assertEqual(len(specs), len(bounds))

    def test_a_job_that_is_switched_off_keeps_its_check(self):
        # 2026-10-21..11-05: the collector was off. Its check does not read `enabled`.
        jobs = {**self.jobs, "jobs": [{**job, "enabled": False} for job in self.jobs["jobs"]]}
        self.assertEqual(self.checks.bounds(jobs), self.checks.bounds(self.jobs))
        self.assertEqual(len(self.checks.checks(jobs)), len(self.checks.checks(self.jobs)))

    def test_only_a_run_that_read_its_source_to_the_end_says_so(self):
        for outcome in ("changed", "unchanged"):
            event = self.checks.read("court_auction_collect", outcome)
            self.assertEqual(event.asset_key, self.checks.read_key("court_auction_collect"))
        # 11-12: the run failed (out of memory). 11-07: a latch refused it.
        for outcome in ("failed", "waiting", "skipped"):
            self.assertIsNone(self.checks.read("court_auction_collect", outcome))

    def test_the_check_fails_once_the_newest_read_is_older_than_the_bound(self):
        from dagster import AssetMaterialization, DagsterInstance, Definitions
        from dagster._core.test_utils import freeze_time  # the clock Dagster's own freshness tests move
        from dagster._time import get_current_datetime

        job_id = "court_auction_collect"
        key = self.checks.read_key(job_id)
        jobs = {**self.jobs, "jobs": [job for job in self.jobs["jobs"] if job["id"] == job_id]}
        assets, checks = self.checks.assets(jobs), self.checks.checks(jobs)
        check_job, schedule = self.checks.schedule(checks)
        self.assertEqual(schedule.cron_schedule, self.checks.EVERY)
        defs = Definitions(assets=assets, asset_checks=checks, jobs=[check_job], schedules=[schedule])
        instance = DagsterInstance.ephemeral()

        def evaluated():
            result = defs.get_job_def(self.checks.CHECK_JOB).execute_in_process(instance=instance)
            (evaluation,) = result.get_asset_check_evaluations()
            self.assertEqual(evaluation.asset_key, key)
            return evaluation

        night = get_current_datetime()
        # The night's run read the source to the end.
        instance.report_runless_asset_event(self.checks.read(job_id, "changed"))
        self.assertIsInstance(self.checks.read(job_id, "changed"), AssetMaterialization)
        with freeze_time(night + datetime.timedelta(hours=29)):
            self.assertTrue(evaluated().passed, "inside the 30-hour bound")
        # The next night lands nothing (switched off, refused, out of memory): no read is logged.
        with freeze_time(night + datetime.timedelta(hours=31)):
            self.assertFalse(evaluated().passed, "a night unread is reported before the one after it")


if __name__ == "__main__":
    unittest.main()
