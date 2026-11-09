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
import math
import pathlib
import sys
import tempfile
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
                  "inputs": [], "outputs": [], "dagster_stage": "shadow", **fields}
        return dual_run.job_specs.JobSpec(**values)

    def test_ticks_while_a_run_is_in_progress_fold_into_the_latest(self):
        import dual_run

        busy = [dual_run.Run("scheduled__x", "scheduled", 1.25 * HOUR, 4.25 * HOUR)]
        ticks = [0.0, 1 * HOUR, 2 * HOUR, 3 * HOUR, 4 * HOUR, 5 * HOUR]
        self.assertEqual(dual_run.due_ticks(ticks, busy), [0.0, 1 * HOUR, 4 * HOUR, 5 * HOUR])
        # A run still in progress (a manual one too: one run at a time) holds every tick after it.
        self.assertEqual(dual_run.due_ticks(ticks, [dual_run.Run("r", "manual", 2.5 * HOUR, None)]), [0.0, 1 * HOUR, 2 * HOUR])

    def test_a_pause_holds_its_ticks_and_the_latest_runs_when_it_ends(self):
        import dual_run

        ticks = [0.0, 1 * HOUR, 2 * HOUR, 3 * HOUR, 4 * HOUR]
        paused = [(0.5 * HOUR, 3.2 * HOUR)]
        catch_up = dual_run.Run("scheduled__3", "scheduled", 3.2 * HOUR + 2, 3.3 * HOUR)
        self.assertEqual(dual_run.due_ticks(ticks, [catch_up], paused), [0.0, 3 * HOUR, 4 * HOUR])
        self.assertEqual(dual_run.due_ticks(ticks, [], [(0.5 * HOUR, math.inf)]), [0.0], "a pause still open holds them all")

    def test_a_tick_within_the_schedulers_lag_of_the_end_runs_after_the_catch_up(self):
        import dual_run

        # map_edit_fold_admin 2026-11-03: the 11:35 run ended 4 s after the 13:35 tick; 12:35 and 13:35 both ran.
        ticks = [0.0, 1 * HOUR, 2 * HOUR, 3 * HOUR]
        runs = [dual_run.Run("scheduled__0", "scheduled", 2, 2 * HOUR + 4),
                dual_run.Run("scheduled__1", "scheduled", 2 * HOUR + 7, 2 * HOUR + 27),
                dual_run.Run("scheduled__2", "scheduled", 2 * HOUR + 30, 2 * HOUR + 49)]
        self.assertEqual(dual_run.due_ticks(ticks, runs), [0.0, 1 * HOUR, 2 * HOUR, 3 * HOUR])

    def test_an_operators_run_that_takes_the_job_first_keeps_the_ticks_waiting(self):
        import dual_run

        # onbid_public_auction_silver 2026-11-04 08:27: a manual run 3 s after an unpause.
        ticks = [0.0, 1 * HOUR, 3 * HOUR, 4 * HOUR]
        manual = dual_run.Run("manual__x", "manual", 2 * HOUR + 3, 3.5 * HOUR)
        catch_up = dual_run.Run("scheduled__3", "scheduled", 3.5 * HOUR + 2, 3.6 * HOUR)
        self.assertEqual(dual_run.due_ticks(ticks, [manual, catch_up], [(0.5 * HOUR, 2 * HOUR)]), [0.0, 3 * HOUR, 4 * HOUR])

    def test_a_pause_is_read_from_the_last_event_before_it_never_the_next(self):
        import dual_run

        # Both events repeat: every deploy unpauses every enabled DAG, paused or not.
        events = [(1 * HOUR, False), (2 * HOUR, True), (2.5 * HOUR, True), (3 * HOUR, False), (4 * HOUR, False)]
        self.assertEqual(dual_run.paused_spans(events, 0.0, 5 * HOUR), [(2 * HOUR, 3 * HOUR)])
        self.assertEqual(dual_run.paused_spans(events, 0.0, 5 * HOUR, paused_at_start=True),
                         [(0.0, 1 * HOUR), (2 * HOUR, 3 * HOUR)])
        self.assertEqual(dual_run.paused_spans([(1 * HOUR, True)], 0.0, 5 * HOUR), [(1 * HOUR, math.inf)])

    def test_ticks_and_input_events_while_paused_are_reported_apart(self):
        import dual_run

        hourly = self.spec(schedule="0 * * * *", started_by="inputs")
        found = dual_run.compare(hourly, {}, [], [0.75 * HOUR], 0.0, 3 * HOUR, paused=[(0.0, math.inf)])
        self.assertTrue(found.matches)
        self.assertEqual(found.paused, [("schedule", 0.0), ("schedule", 1 * HOUR), ("schedule", 2 * HOUR), ("inputs", 0.75 * HOUR)])

    def test_an_input_event_without_a_run_and_a_run_without_an_event(self):
        import dual_run

        spec = self.spec(schedule="", started_by="inputs")
        runs = [dual_run.Run("asset_triggered__a", "asset_triggered", 2 * HOUR, 2.1 * HOUR),
                dual_run.Run("asset_triggered__b", "asset_triggered", 5 * HOUR, 5.1 * HOUR)]
        found = dual_run.compare(spec, {}, runs, [1.9 * HOUR, 8 * HOUR], 0.0, 24 * HOUR)
        self.assertEqual(found.only_airflow, [("inputs", 5 * HOUR)], "the second run had no input event")
        self.assertEqual(found.only_dagster, [("inputs", 8 * HOUR)], "the last event started no run")
        self.assertEqual(found.decisions, 2)
        self.assertEqual(found.agreed, [("inputs", 2 * HOUR)])

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


def synthetic_spec(**fields):
    return RulesTest.spec(None, **fields)


class EventLogs:
    """Airflow 3.3.2's `GET /api/v2/eventLogs` over a list of audit-log records."""

    def __init__(self, test, records):
        self.test, self.records, self.calls = test, records, []

    def get(self, url, params=None, **_):
        from test_dagster_observe import Answer

        self.calls.append(params)
        self.test.assertTrue(url.endswith("/api/v2/eventLogs"))
        found = [record for record in self.records if record["dag_id"] == params["dag_id"]
                 and record["event"] in params["included_events"]
                 and ("after" not in params or record["when"] >= params["after"])
                 and ("before" not in params or record["when"] < params["before"])]
        found.sort(key=lambda record: record["when"], reverse=params["order_by"] == "-when")
        page = found[params["offset"]:params["offset"] + params["limit"]]
        return Answer({"event_logs": page, "total_entries": len(found)})


def audit(at, event, dag="foundation_synthetic"):
    return {"dag_id": dag, "event": event, "when": datetime.datetime.fromtimestamp(at, datetime.UTC).isoformat()}


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class LiveCheckTest(unittest.TestCase):
    def airflow(self, records):
        from airflow3 import Airflow3Instance
        from dagster_airlift.core.airflow_instance import AirflowAuthBackend

        server = EventLogs(self, records)

        class Backend(AirflowAuthBackend):
            def get_session(self):
                return server

            def get_webserver_url(self):
                return "http://airflow.invalid"

        return Airflow3Instance(auth_backend=Backend(), name="foundation_airflow"), server

    def test_pauses_are_read_from_airflows_audit_log(self):
        import dual_run

        airflow, server = self.airflow([audit(0.5 * HOUR, "cli_dag_pause"), audit(2 * HOUR, "cli_dag_unpause"),
                                        audit(3 * HOUR, "cli_dag_pause"), audit(3.5 * HOUR, "cli_dag_unpause"),
                                        audit(3.2 * HOUR, "cli_dag_pause", dag="foundation_other")])
        self.assertEqual(dual_run.airflow_pauses(airflow, "foundation_synthetic", 1 * HOUR, 5 * HOUR),
                         [(1 * HOUR, 2 * HOUR), (3 * HOUR, 3.5 * HOUR)])
        self.assertEqual(server.calls[0]["included_events"], ["cli_dag_pause", "cli_dag_unpause"])
        # No event at all before the window: Airflow made the DAG paused.
        self.assertEqual(dual_run.airflow_pauses(airflow, "foundation_new", 1 * HOUR, 5 * HOUR), [(1 * HOUR, math.inf)])

    def test_every_comparison_is_appended_to_the_ledger(self):
        import dual_run

        spec = synthetic_spec(schedule="0 * * * *")
        with tempfile.TemporaryDirectory() as directory:
            ledger = pathlib.Path(directory) / "dual-run" / "comparisons.jsonl"
            differs = dual_run.Comparison(decisions=3, agreed=[("schedule", HOUR)], only_dagster=[("schedule", 2 * HOUR)])
            dual_run.append(ledger, dual_run.ledger_line(spec, differs, 0.0, 24 * HOUR, "run-1", "release-a"))
            dual_run.append(ledger, dual_run.ledger_line(spec, dual_run.Comparison(decisions=1), HOUR, 25 * HOUR,
                                                         "run-2", "release-a"))
            lines = [json.loads(text) for text in ledger.read_text(encoding="utf-8").splitlines()]
        self.assertEqual([line["dagster_run_id"] for line in lines], ["run-1", "run-2"])
        self.assertEqual(lines[0]["only_dagster"], [["schedule", "1970-01-01T02:00:00+00:00"]])
        self.assertEqual(lines[0]["agreed"], [["schedule", "1970-01-01T01:00:00+00:00"]])
        self.assertEqual([line["matches"] for line in lines], [False, True])
        self.assertEqual({line["stage"] for line in lines}, {"shadow"})

    def test_the_gate_counts_distinct_agreeing_decisions_of_every_kind_since_the_last_difference(self):
        import dual_run

        def line(hour, agreed=(), job="synthetic", differs=(), paused=()):
            def stamped(items):
                return [[kind, f"2099-11-{1 + at // 24:02d}T{at % 24:02d}:00:00+00:00"] for kind, at in items]

            return {"evaluated_at": stamped([("", hour)])[0][1], "job": job, "matches": not differs,
                    "decisions": len(agreed) + len(differs), "agreed": stamped(agreed), "only_dagster": stamped(differs),
                    "only_airflow": [], "airflow_out": [], "paused": stamped(paused)}

        hourly = synthetic_spec(schedule="0 * * * *")
        both = synthetic_spec(job_id="both", schedule="0 7 * * *", started_by="inputs")
        weekly = synthetic_spec(job_id="weekly", schedule="0 3 * * 1")
        # Each check's window repeats the decisions before it: three lines, three distinct ticks.
        lines = [line(1, [("schedule", 0)]), line(2, [("schedule", 0), ("schedule", 1)]),
                 line(3, [("schedule", 1), ("schedule", 2)], paused=[("schedule", 3)])]
        lines += [line(30, [("schedule", 7), ("inputs", 8), ("inputs", 9)], job="both")]
        lines += [line(10, [("schedule", 5)], job="weekly"), line(11, job="weekly", differs=[("schedule", 6)]),
                  line(12, [("schedule", 5), ("schedule", 7)], job="weekly")]
        found = dual_run.gate(lines, [hourly, both, weekly, synthetic_spec(job_id="absent")])
        self.assertEqual(found["synthetic"], {"agreeing": 3, "kinds": ["daily"], "missing_kinds": [], "ready": True,
                                              "reset_at": None, "apart": [("schedule", "2099-11-01T03:00:00+00:00")]})
        self.assertEqual((found["both"]["agreeing"], found["both"]["kinds"], found["both"]["ready"]),
                         (3, ["daily", "event"], True))
        # The difference at 06:00 resets the count: 05:00, agreed again by a later window, is not evidence.
        self.assertEqual((found["weekly"]["agreeing"], found["weekly"]["kinds"], found["weekly"]["reset_at"]),
                         (1, ["weekly"], "2099-11-01T06:00:00+00:00"))
        self.assertFalse(found["weekly"]["ready"])
        self.assertEqual((found["absent"]["agreeing"], found["absent"]["missing_kinds"], found["absent"]["ready"]),
                         (0, ["daily"], False))
        # Three decisions of one kind do not move a job that makes another kind it never showed.
        only_ticks = dual_run.gate([line(30, [("schedule", 7), ("schedule", 31), ("schedule", 55)], job="both")], [both])
        self.assertEqual((only_ticks["both"]["missing_kinds"], only_ticks["both"]["ready"]), (["event"], False))
        # Moved to `proxied`, the job counts again from its first comparison there (root ADR-0458 §4): the
        # shadow lines that made it ready are not evidence of Dagster running it.
        proxied = synthetic_spec(schedule="0 * * * *", dagster_stage="proxied")
        moved = lines[:3] + [{**line(40, [("schedule", 39)]), "stage": "proxied"}]
        self.assertEqual((dual_run.gate(moved, [proxied])["synthetic"]["agreeing"],
                          dual_run.gate(moved, [hourly])["synthetic"]["agreeing"]), (1, 3))

    def test_a_schedules_cadence_is_its_kind(self):
        import dual_run

        self.assertEqual([dual_run.schedule_kind(cron) for cron in ("5 * * * *", "0 7 * * *", "0 3 * * 1", "0 3 1 * *")],
                         ["daily", "daily", "weekly", "monthly"])

    def test_a_difference_pages_through_alertmanager(self):
        from alertmanager_stub import AlertmanagerStub

        import dual_run

        stub = AlertmanagerStub()
        try:
            differs = dual_run.Comparison(decisions=2, only_airflow=[("schedule", 2 * HOUR)])
            self.assertTrue(dual_run.page(synthetic_spec(), differs, f"http://127.0.0.1:{stub.port}"))
        finally:
            stub.close()
        (alert,) = stub.named("FoundationDualRunDisagreed")
        self.assertEqual((alert["labels"]["service"], alert["labels"]["job"]), ("dual-run", "synthetic"))
        self.assertIn("1970-01-01T02:00:00+00:00", alert["annotations"]["summary"])
        # No Alertmanager: said, not raised (the ledger line is already written).
        self.assertFalse(dual_run.page(synthetic_spec(), differs, "http://127.0.0.1:9"))

    def test_a_check_compares_appends_and_reports(self):
        from dagster import DagsterInstance, build_asset_check_context

        import dual_run

        spec = synthetic_spec(schedule="0 * * * *")
        airflow, _ = self.airflow([])
        original = dual_run.airflow_runs
        dual_run.airflow_runs = lambda *_: {}
        try:
            with tempfile.TemporaryDirectory() as directory, DagsterInstance.ephemeral() as instance:
                ledger = pathlib.Path(directory) / "comparisons.jsonl"
                evaluate = dual_run._evaluator(airflow, spec, {}, [spec.dag_id], HOUR, ledger)
                reported = evaluate(build_asset_check_context(instance=instance))
                (line,) = [json.loads(text) for text in ledger.read_text(encoding="utf-8").splitlines()]
        finally:
            dual_run.airflow_runs = original
        # No pause or unpause ever: Airflow created the DAG paused and it stayed so; nothing differs.
        self.assertTrue(reported.passed)
        self.assertEqual((line["job"], line["only_dagster"], line["dagster_run_id"]), ("synthetic", [], None))
        self.assertGreater(len(line["paused"]), 20)


if __name__ == "__main__":
    unittest.main()
