"""Dagster decides as Airflow did (orchestration/dagster/dual_run.py, root ADR-0431 §2).

Run by the CI `dagster-observe` lane in the code location's image; skipped elsewhere, where a skip
fails that lane. The recorded production day (fixtures/airflow-recorded-day.json, 2026-10-28 moved to
2099) is replayed through the comparison: every job that ran that day, its runs and the asset events
on its starting inputs. Measured on it: 34 jobs and 178 decisions; the 168 outside the two stretches in
which Airflow started nothing (15:50-18:04 and 19:00-20:47) are all the same, the 10 inside are
reported apart.

A job whose schedule or inputs the contracts changed after the recorded day cannot be compared
against it; it is left out and named, and at least half the day's decisions must stay comparable.
"""

import datetime
import importlib.util
import json
import pathlib
import sys
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "orchestration" / "dags"))
sys.path.insert(0, str(PLATFORM / "orchestration" / "dagster"))

DAY = json.loads((pathlib.Path(__file__).parent / "fixtures" / "airflow-recorded-day.json").read_text(encoding="utf-8"))
HAS_DAGSTER = importlib.util.find_spec("dagster") is not None
NEEDS_DAGSTER = "requires dagster (the CI dagster-observe lane runs it)"
HOUR = 3600.0


def at(text):
    return datetime.datetime.fromisoformat(text).timestamp()


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class RecordedDayTest(unittest.TestCase):
    def replay(self):
        import dual_run
        import observe_assets

        jobs, graph = observe_assets.contracts()
        by_id = {job["id"]: job for job in jobs["jobs"]}
        start = at(DAY["day"] + "T00:00:00+00:00")
        end = start + 24 * HOUR
        ran = {run["dag_id"] for run in DAY["dag_runs"]}
        specs = [spec for spec in dual_run.job_specs.load_specs(jobs, graph) if spec.dag_id in ran]
        out = dual_run.outages([at(run["start_date"]) for run in DAY["dag_runs"]],
                               dual_run.outage_gap_seconds(specs), start, end)
        events = {}
        for event in DAY["asset_events"]:
            events.setdefault(event["uri"].rstrip("/"), []).append(at(event["timestamp"]))
        found = {}
        for spec in specs:
            runs = [dual_run.Run(run["dag_run_id"], run["run_type"], at(run["start_date"]), at(run["end_date"]))
                    for run in DAY["dag_runs"] if run["dag_id"] == spec.dag_id]
            inputs = [t for dataset in spec.starting_inputs for t in events.get(dual_run.job_specs.asset_uri(dataset), [])]
            found[spec.job_id] = dual_run.compare(spec, by_id[spec.job_id], runs, inputs, start, end, out)
        return found, out, start

    def test_dagster_decides_every_tick_and_input_event_as_airflow_did(self):
        found, out, start = self.replay()
        # A drifted job (its contract changed after the day) differs on every decision of its kind.
        drifted = {job for job, comparison in found.items()
                   if comparison.only_dagster and len(comparison.only_dagster) >= comparison.decisions}
        compared = {job: comparison for job, comparison in found.items() if job not in drifted}
        self.assertEqual({job: (c.only_dagster, c.only_airflow) for job, c in compared.items() if not c.matches}, {},
                         f"left out as drifted: {sorted(drifted)}")
        decisions = sum(comparison.decisions for comparison in compared.values())
        self.assertGreaterEqual(decisions, 0.5 * sum(comparison.decisions for comparison in found.values()))
        self.assertGreaterEqual(decisions, 100)
        self.assertEqual([(round((a - start) / 60), round((b - start) / 60)) for a, b in out],
                         [(15 * 60 + 50, 18 * 60 + 4), (19 * 60, 20 * 60 + 47)])

    def test_ticks_inside_an_outage_are_reported_apart(self):
        found, _, _ = self.replay()
        self.assertGreater(sum(len(comparison.airflow_out) for comparison in found.values()), 0)


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class RulesTest(unittest.TestCase):
    def spec(self, **fields):
        import dual_run

        values = {"job_id": "synthetic", "dag_id": "foundation_synthetic", "description": "", "schedule": "50 * * * *",
                  "timeout_minutes": 30, "pool": "default_pool", "pool_slots": 1, "retries": 0, "priority_weight": 1,
                  "systemd_service": "foundation-synthetic.service", "systemd_timer": None, "enabled": True,
                  "inputs": [], "outputs": [], **fields}
        return dual_run.job_specs.JobSpec(**values)

    def test_ticks_while_a_run_is_in_progress_fold_into_the_latest(self):
        import dual_run

        busy = [dual_run.Run("scheduled__x", "scheduled", 1.25 * HOUR, 4.25 * HOUR)]
        ticks = [0.0, 1 * HOUR, 2 * HOUR, 3 * HOUR, 4 * HOUR, 5 * HOUR]
        self.assertEqual(dual_run.due_ticks(ticks, busy), [0.0, 1 * HOUR, 4 * HOUR, 5 * HOUR])
        self.assertEqual(dual_run.due_ticks(ticks, [dual_run.Run("r", "manual", 2.5 * HOUR, None)]), [0.0, 1 * HOUR, 2 * HOUR, 5 * HOUR])

    def test_an_input_event_without_a_run_and_a_run_without_an_event(self):
        import dual_run

        spec = self.spec(schedule="", started_by="inputs")
        runs = [dual_run.Run("asset_triggered__a", "asset_triggered", 2 * HOUR, 2.1 * HOUR),
                dual_run.Run("asset_triggered__b", "asset_triggered", 5 * HOUR, 5.1 * HOUR)]
        found = dual_run.compare(spec, {}, runs, [1.9 * HOUR, 8 * HOUR], 0.0, 24 * HOUR)
        self.assertEqual(found.only_airflow, [("inputs", 5 * HOUR)], "the second run had no input event")
        self.assertEqual(found.only_dagster, [("inputs", 8 * HOUR)], "the last event started no run")
        self.assertEqual(found.decisions, 2)

    def test_an_outage_pushes_an_events_deadline_and_holds_its_ticks_apart(self):
        import dual_run

        spec = self.spec(schedule="", started_by="inputs")
        late = [dual_run.Run("asset_triggered__a", "asset_triggered", 5 * HOUR, 5.1 * HOUR)]
        self.assertFalse(dual_run.compare(spec, {}, late, [1 * HOUR], 0.0, 24 * HOUR).matches)
        self.assertTrue(dual_run.compare(spec, {}, late, [1 * HOUR], 0.0, 24 * HOUR, [(1.2 * HOUR, 5 * HOUR)]).matches)
        hourly = self.spec(schedule="0 * * * *")
        found = dual_run.compare(hourly, {}, [], [], 0.0, 3 * HOUR, [(0.0, 3 * HOUR)])
        self.assertTrue(found.matches)
        self.assertEqual(len(found.airflow_out), found.decisions)

    def test_an_outage_is_a_stretch_with_no_run_of_any_job(self):
        import dual_run

        self.assertEqual(dual_run.outages([0.5 * HOUR, 1 * HOUR, 4 * HOUR], HOUR, 0.0, 5 * HOUR), [(1 * HOUR, 4 * HOUR)])
        self.assertEqual(dual_run.outage_gap_seconds([self.spec(schedule="0,30 * * * *")]), HOUR)


if __name__ == "__main__":
    unittest.main()
