"""The daily readiness report (root ADR-0442): what scripts/ops/readiness_report.py decides from what it reads.

2026-11-07 a one-off sweep found 57 of 76 rows not ok, among them an hourly Silver load that had not
succeeded for 51 hours and a daily check failing for a week, each only a unit among thirty-five failed
ones. The report is that sweep, every day, from the definitions; these tests hold its verdicts.
"""

import importlib.util
import json
import pathlib
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
NOW = 1_800_000_000.0


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


report = load(PLATFORM / "scripts/ops/readiness_report.py", "readiness_report")


def event(at, message):
    return {"__REALTIME_TIMESTAMP": str(int(at * 1e6)), "MESSAGE": message}


def job(job_id="silver_refresh_x", schedule="0 7 * * *", limit=90):
    return {"id": job_id, "systemd_service": f"{job_id}.service", "schedule": schedule, "time_limit_minutes": limit}


class Schedules(unittest.TestCase):
    def test_a_schedule_period_is_its_longest_gap(self):
        self.assertEqual(report.cron_period_seconds("0 7 * * *"), 86400)
        self.assertEqual(report.cron_period_seconds("7 * * * *"), 3600)
        self.assertEqual(report.cron_period_seconds("0,30 * * * *"), 1800)
        self.assertIsNone(report.cron_period_seconds("0 7 * * 1"))

    def test_every_job_schedule_is_one_the_report_can_read(self):
        jobs = json.loads((PLATFORM / "orchestration/jobs.v1.json").read_text(encoding="utf-8"))["jobs"]
        self.assertEqual([j["id"] for j in jobs if report.cron_period_seconds(j["schedule"]) is None], [])


class Jobs(unittest.TestCase):
    def test_a_failure_after_the_last_success_is_failing_with_systemds_word(self):
        runs = report.unit_runs([event(NOW - 7200, "x.service: Deactivated successfully."),
                                 event(NOW - 600, "x.service: Failed with result 'timeout'.")])
        row = report.job_row(job(), runs, "Error: boom", [], NOW)
        self.assertEqual((row["state"], row["result"], row["reason"]), ("failing", "timeout", "Error: boom"))

    def test_a_success_older_than_one_period_and_the_limit_is_stale(self):
        runs = report.unit_runs([event(NOW - 86400 - 90 * 60 - 1, "Finished x.")])
        self.assertEqual(report.job_row(job(), runs, None, [], NOW)["state"], "stale")
        runs = report.unit_runs([event(NOW - 86400, "Finished x.")])
        self.assertEqual(report.job_row(job(), runs, None, [], NOW)["state"], "ok")

    def test_the_operations_maps_blocked_problem_marks_an_ok_job(self):
        runs = report.unit_runs([event(NOW - 60, "Finished x.")])
        row = report.job_row(job(), runs, None, [{"kind": "blocked", "summary": "앞 단계가 실패"}], NOW)
        self.assertEqual(row["state"], "blocked")


class Lanes(unittest.TestCase):
    LANE = {"lane": "parcel-x", "serving_nodes": ["parcel-x-by-pnu-section-packs"], "gold_tables": ["gold.parcel_x"],
            "bake_jobs": [{"id": "by_pnu_x_bake"}], "rebuild_jobs": [{"id": "gold_x_rebuild"}]}
    JOBS = {"by_pnu_x_bake": {"id": "by_pnu_x_bake", "enabled": True}, "gold_x_rebuild": {"id": "gold_x_rebuild", "enabled": True}}

    def test_a_served_manifest_past_its_budget_is_stale(self):
        metrics = {("foundation_by_pnu_served_manifest_age_seconds", "parcel-x"): 3 * 86400,
                   ("foundation_by_pnu_serving_freshness_budget_seconds", "parcel-x"): 2 * 86400}
        self.assertEqual(report.lane_row(self.LANE, metrics, "exit-code", self.JOBS)["state"], "stale")

    def test_a_lane_whose_bake_is_off_is_not_served_not_unmeasured(self):
        jobs = {**self.JOBS, "by_pnu_x_bake": {"id": "by_pnu_x_bake", "enabled": False}}
        self.assertEqual(report.lane_row(self.LANE, {}, "", jobs)["state"], "not-served")
        self.assertEqual(report.lane_row(self.LANE, {}, "", self.JOBS)["state"], "unmeasured")

    def test_the_lanes_are_the_lane_index_not_a_list_here(self):
        source = (PLATFORM / "scripts/ops/readiness_report.py").read_text(encoding="utf-8")
        self.assertIn('"by-pnu-lane-index.v1.json"', source)
        self.assertNotIn('"parcel-zoning"', source)


class Order(unittest.TestCase):
    def test_what_users_see_comes_first_and_ok_rows_are_left_out_of_the_table(self):
        rows = [{"kind": "host", "id": "memory", "impact": "host", "state": "low"},
                {"kind": "job", "id": "silver_x", "impact": "silver", "state": "failing"},
                {"kind": "lane", "id": "parcel", "impact": "serving", "state": "stale"},
                {"kind": "job", "id": "ok_job", "impact": "serving", "state": "ok"}]
        table = report.markdown(rows, NOW)
        self.assertLess(table.index("`parcel`"), table.index("`silver_x`"))
        self.assertLess(table.index("`silver_x`"), table.index("`memory`"))
        self.assertNotIn("ok_job", table)
        self.assertIn("blockers 3 of 4 rows", table)

    def test_prometheus_text_is_read_with_its_labels(self):
        found = report.samples('# HELP x y\nfoundation_scheduled_job_problem{job_id="a",kind="failed",summary="b c"} 1\n')
        self.assertEqual(found, [("foundation_scheduled_job_problem", {"job_id": "a", "kind": "failed", "summary": "b c"}, 1.0)])


class Host(unittest.TestCase):
    def test_low_memory_full_disk_and_units_near_their_cap(self):
        meminfo = "MemTotal: 1000 kB\nMemAvailable: 50 kB\nSwapTotal: 10 kB\nSwapFree: 10 kB\n"
        rows = {row["id"]: row for row in report.host_rows(meminfo, {"/data": (100, 90)}, ["a.service"], [],
                                                            [{"unit": "b.service", "peak": 95, "max": 100}])}
        self.assertEqual(rows["memory"]["state"], "low")
        self.assertEqual(rows["disk /data"]["state"], "low")
        self.assertEqual(rows["failed units"]["state"], "failing")
        self.assertEqual(rows["units near their MemoryMax"]["state"], "low")
        self.assertEqual(rows["timers never fired"]["state"], "ok")


class Installed(unittest.TestCase):
    def test_the_host_runs_it_daily_and_lets_it_read_the_journal(self):
        unit = (PLATFORM / "infra/systemd/foundation-readiness-report.service").read_text(encoding="utf-8")
        timer = (PLATFORM / "infra/systemd/foundation-readiness-report.timer").read_text(encoding="utf-8")
        release = (PLATFORM / "scripts/deploy/foundation-release.sh").read_text(encoding="utf-8")
        self.assertIn("ExecStart=/opt/foundation-platform/current/scripts/ops/readiness_report.py\n", unit)
        self.assertIn("SupplementaryGroups=systemd-journal\n", unit)
        self.assertIn(f"ReadWritePaths={report.OUT_DIR.as_posix()} {report.METRICS_DIR.as_posix()}\n", unit)
        self.assertIn("OnCalendar=*-*-* 22:10:00\n", timer)
        self.assertIn("systemctl enable --now foundation-readiness-report.timer\n", release)
        self.assertIn(f"-m 0755 {report.OUT_DIR.as_posix()}\n", release)


if __name__ == "__main__":
    unittest.main()
