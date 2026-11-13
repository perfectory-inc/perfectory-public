"""Dagster decides a job at `dagster_stage: dagster` (orchestration/dagster/decide.py, root ADR-0431 §3).

The decisions are replayed, one evaluation every 30 s as the sensor runs: the 2026-11-05 unpause that ran
a past tick (root ADR-0425), the recorded day's onbid Silver fold (ADR-0431's table), a daemon outage, and
input events. The fold is held to the comparator's model of Airflow (dual_run.due_ticks), the rule the dual
run proved. Run by the CI `dagster-observe` lane in the code location's image; skipped elsewhere, where a
skip fails that lane.
"""

import copy
import datetime
import importlib.util
import json
import pathlib
import sys
import unittest
from unittest import mock

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "orchestration" / "dags"))
sys.path.insert(0, str(PLATFORM / "orchestration" / "dagster"))

HAS_DAGSTER = importlib.util.find_spec("dagster") is not None
NEEDS_DAGSTER = "requires dagster (the CI dagster-observe lane runs it)"
STEP = 30.0


def at(text):
    return datetime.datetime.fromisoformat(text).replace(tzinfo=datetime.UTC).timestamp()


def spec_of(job_id):
    import observe_assets

    return next(spec for spec in observe_assets.job_specs.load_specs() if spec.job_id == job_id)


def limit_of(job_id):
    import observe_assets

    jobs, _ = observe_assets.contracts()
    return 60 * observe_assets.job_specs.run_limit_minutes(next(job for job in jobs["jobs"] if job["id"] == job_id))


def replay(spec, start, end, run_seconds, held=(), down=(), events=()):
    """Evaluate the job's sensor every STEP seconds over [start, end): its requested runs, as (start, end, key).

    A requested run holds the job for `run_seconds`; `held` are stretches another run held it (an operator's),
    `down` stretches nothing evaluated (the daemon down), `events` input materialization times.
    """
    import decide

    cursor, runs, now, seen = None, [], start, 0
    while now < end:
        if not any(a <= now < b for a, b in down):
            open_run = any(a <= now < b for a, b, _ in runs) or any(a <= now < b for a, b in held)
            new = [(index + 1, event) for index, event in enumerate(events) if index + 1 > seen and event <= now]
            requests, cursor = decide.decide(spec, limit_of(spec.job_id), cursor, now, open_run,
                                             new if cursor is not None else [], 0 if cursor is not None else len(
                                                 [event for event in events if event <= now]))
            seen = cursor.event_id
            for request in requests:
                runs.append((now, now + run_seconds, request.key))
        now += STEP
    return runs


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class TheDecisionsTest(unittest.TestCase):
    def test_the_first_look_never_runs_a_past_tick_2026_11_05(self):
        # 11-05 23:46:32 the deploy unpaused the new migration DAG and Airflow ran that day's 18:45 tick at
        # once; it held the Spark pool into the owner's cut-over (root ADR-0425). Dagster's sensor first
        # looks at that moment: nothing runs until the next day's 18:45.
        spec = spec_of("unit_price_versions_migration")
        runs = replay(spec, at("2026-11-05T23:46:32"), at("2026-11-06T19:00:00"), 3600)
        self.assertEqual([key for _, _, key in runs], ["tick:2026-11-06T18:45:00+00:00"])
        self.assertGreaterEqual(runs[0][0], at("2026-11-06T18:45:00"))

    def test_ticks_while_a_run_is_open_fold_into_the_latest_as_airflow_did(self):
        # The recorded day (2026-10-28): onbid Silver's 17:50 run took 18:05-21:05; 18:50 and 19:50 folded
        # and 20:50 ran next (ADR-0431's table). Here an operator's run holds the job over the same stretch.
        import dual_run

        spec = spec_of("onbid_public_auction_silver")
        held = [(at("2026-10-28T18:05:00"), at("2026-10-28T21:05:00"))]
        start, end = at("2026-10-28T17:00:00"), at("2026-10-28T23:00:00")
        runs = replay(spec, start, end, 600, held=held)
        keys = [key for _, _, key in runs]
        self.assertIn("tick:2026-10-28T20:50:00+00:00", keys)
        self.assertNotIn("tick:2026-10-28T18:50:00+00:00", keys)
        self.assertNotIn("tick:2026-10-28T19:50:00+00:00", keys)
        ticks = dual_run.schedule_ticks(spec.schedule, start, end)
        model = dual_run.due_ticks(ticks, [dual_run.Run("held", "manual", a, b) for a, b in held]
                                   + [dual_run.Run(key, "scheduled", a, b) for a, b, key in runs])
        self.assertEqual([datetime.datetime.fromtimestamp(tick, datetime.UTC).isoformat() for tick in model],
                         [key.removeprefix("tick:") for key in keys], "the comparator's model of Airflow decides the same")

    def test_a_tick_missed_while_down_runs_late_only_if_it_ends_before_the_next(self):
        # The fold of the map edits (hourly, 130 min limit) ends after its next tick: a late start waits for it.
        fold = spec_of("map_edit_fold_admin")
        runs = replay(fold, at("2026-11-09T12:00:00"), at("2026-11-09T13:40:00"), 300,
                      down=[(at("2026-11-09T12:30:00"), at("2026-11-09T12:45:00"))])
        self.assertEqual([key for _, _, key in runs], ["tick:2026-11-09T13:35:00+00:00"])
        # onbid Silver (hourly, 45 min limit) missed at :50 and back at :55 still ends before the next tick.
        silver = spec_of("onbid_public_auction_silver")
        runs = replay(silver, at("2026-11-09T12:00:00"), at("2026-11-09T13:00:00"), 300,
                      down=[(at("2026-11-09T12:45:00"), at("2026-11-09T12:55:00"))])
        self.assertEqual([key for _, _, key in runs], ["tick:2026-11-09T12:50:00+00:00"])
        self.assertGreaterEqual(runs[0][0], at("2026-11-09T12:55:00"))

    def test_input_events_start_one_run_and_fold_while_one_is_open(self):
        spec = spec_of("gold_unit_price_rebuild")
        self.assertEqual(spec.started_by, "inputs")
        events = [at("2026-11-09T10:00:10"), at("2026-11-09T10:05:00"), at("2026-11-09T10:10:00")]
        runs = replay(spec, at("2026-11-09T10:00:00"), at("2026-11-09T11:30:00"), 1200, events=events)
        # 10:00:10 starts a run (to 10:20:30); 10:05 and 10:10 fold into one run when it ends.
        self.assertEqual(len(runs), 2)
        self.assertTrue(all(key.startswith("inputs:") for _, _, key in runs))
        self.assertEqual(runs[1][2], "inputs:2026-11-09T10:10:00+00:00")
        self.assertGreaterEqual(runs[1][0], runs[0][1])

    def test_an_input_event_missed_while_down_still_starts_one_run(self):
        spec = spec_of("gold_unit_price_rebuild")
        events = [at("2026-11-09T10:10:00")]
        runs = replay(spec, at("2026-11-09T10:00:00"), at("2026-11-09T11:00:00"), 600, events=events,
                      down=[(at("2026-11-09T10:05:00"), at("2026-11-09T10:40:00"))])
        self.assertEqual([key for _, _, key in runs], ["inputs:2026-11-09T10:10:00+00:00"])


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class ARunCoversWhatCameBeforeItTest(unittest.TestCase):
    """Root ADR-0565 §5: a run, whoever started it, covers every tick and input event from before it began."""

    def decide(self, cursor, now, **kwargs):
        import decide

        spec = spec_of("gold_unit_price_rebuild")
        return decide.decide(spec, limit_of(spec.job_id), cursor, now, kwargs.pop("held", False),
                             kwargs.pop("events", []), kwargs.pop("newest_event_id", 0), (), **kwargs)

    def test_a_sensor_stopped_for_weeks_does_not_replay_what_runs_it_did_not_start_did(self):
        # Demoted to proxied and promoted again (review M2): Airflow's runs of the same Dagster job
        # covered the ticks and events of the weeks the sensor was away.
        import decide

        left = at("2026-11-01T00:00:00")
        _, cursor = self.decide(None, left)
        back = at("2026-11-20T10:00:00")
        requests, _ = self.decide(cursor, back, events=[(9, back - 3600)], last_run_at=back - 600)
        self.assertEqual(requests, [])
        # Without such a run the outage rules apply as before: one run (a missed tick that still fits, or
        # the event), never one per thing missed.
        requests, _ = self.decide(cursor, back, events=[(9, back - 3600)])
        self.assertEqual(len(requests), 1)
        self.assertIsInstance(cursor, decide.Cursor)

    def test_a_run_the_deploy_or_an_operator_started_covers_the_tick_waiting_behind_it(self):
        now = at("2026-11-09T10:00:00")
        _, cursor = self.decide(None, now)
        spec = spec_of("gold_unit_price_rebuild")
        import dual_run

        tick = dual_run.schedule_ticks(spec.schedule, now, now + 2 * 86400)[0]
        _, cursor = self.decide(cursor, tick + STEP, held=True)
        self.assertEqual(cursor.waiting, [tick])
        self.assertEqual(self.decide(cursor, tick + 2 * STEP, last_run_at=tick + 1)[0], [])
        self.assertEqual(len(self.decide(cursor, tick + 2 * STEP, last_run_at=tick - 1)[0]), 1)

    def test_the_first_look_owes_one_run_for_an_input_newer_than_the_jobs_newest_run(self):
        # The deploy that moves a job here paused its DAG; Airflow dropped the event (review M3, root ADR-0337).
        now = at("2026-11-09T10:00:00")
        _, cursor = self.decide(None, now, newest_event_id=7, newest_event_at=now - 60, last_run_at=now - 3600)
        requests, _ = self.decide(cursor, now + STEP)
        self.assertEqual([(request.key.split(":")[0], request.kind) for request in requests], [("inputs", "triggered")])
        for last_run_at in (now - 30, None):
            with self.subTest(last_run_at=last_run_at):
                _, cursor = self.decide(None, now, newest_event_id=7, newest_event_at=now - 60, last_run_at=last_run_at)
                self.assertEqual(self.decide(cursor, now + STEP)[0], [], "covered, or never run here: nothing owed")


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class TheOwedRunsTest(unittest.TestCase):
    """Root ADR-0565 §4: what the reconciler's run says a job is owed starts the job from the job's own
    sensor, as the reconciler's DAG tasks `select` and `start` did for a job with a DAG."""

    def decide(self, cursor, now, held=False, events=(), owed=()):
        import decide

        spec = spec_of("gold_unit_price_rebuild")
        return decide.decide(spec, limit_of(spec.job_id), cursor, now, held, list(events), 0, list(owed))

    def test_an_owed_key_starts_one_triggered_run_named_by_the_key(self):
        now = at("2026-11-09T10:00:00")
        _, cursor = self.decide(None, now, owed=["0123456789abcdef"])
        self.assertEqual(self.decide(None, now, owed=["0123456789abcdef"])[0], [], "the first look starts nothing")
        requests, cursor = self.decide(cursor, now + STEP, owed=["0123456789abcdef", "fedcba9876543210"])
        self.assertEqual([(request.key, request.kind) for request in requests], [("reconcile:fedcba9876543210", "triggered")])

    def test_an_open_run_or_a_run_starting_now_covers_what_is_owed(self):
        now = at("2026-11-09T10:00:00")
        _, cursor = self.decide(None, now)
        # Held: Airflow's `select` kept only the jobs with no run queued or running.
        self.assertEqual(self.decide(cursor, now + STEP, held=True, owed=["0123456789abcdef"])[0], [])
        # An input event starts the run now; that run reads the inputs as they are.
        requests, _ = self.decide(cursor, now + STEP, events=[(1, now + 1)], owed=["0123456789abcdef"])
        self.assertEqual(len(requests), 1)
        self.assertTrue(requests[0].key.startswith("inputs:"))

    def test_the_sensor_reads_the_owed_runs_from_the_reconcilers_dagster_runs(self):
        import observe_assets
        from dagster import DagsterInstance, RunRequest, SkipReason, build_sensor_context, job, op

        import decide

        job_specs = observe_assets.job_specs
        spec = spec_of("gold_unit_price_rebuild")
        reconciler = next(found for found in job_specs.load_specs() if spec.job_id in found.reconciles)

        @op
        def run():
            pass

        @job(name=reconciler.dagster_job)
        def reconcile():
            run()

        def reconciled(instance, owed, outcome="unchanged"):
            reconcile.execute_in_process(instance=instance, tags={
                job_specs.OUTCOME_TAG: outcome, job_specs.OWED_TAG: json.dumps(owed)})

        clock = [at("2026-11-09T10:00:00")]
        evaluate = decide.evaluator(spec, limit_of(spec.job_id), clock=lambda: clock[0], reconcilers=[reconciler])
        with DagsterInstance.ephemeral() as instance:
            context = build_sensor_context(instance=instance)
            self.assertIsInstance(evaluate(context), SkipReason)
            # A reconciler whose run was skipped (`waiting`) started nothing through Airflow either.
            reconciled(instance, [[spec.job_id, "aaaaaaaaaaaaaaaa"]], outcome="waiting")
            reconciled(instance, [["by_pnu_unit_price_bake", "bbbbbbbbbbbbbbbb"]])
            clock[0] += STEP
            context = build_sensor_context(instance=instance, cursor=context.cursor)
            self.assertIsInstance(evaluate(context), SkipReason, "nothing is owed to this job")
            reconciled(instance, [[spec.job_id, "0123456789abcdef"]])
            clock[0] += STEP
            context = build_sensor_context(instance=instance, cursor=context.cursor)
            (request,) = evaluate(context)
            self.assertIsInstance(request, RunRequest)
            self.assertEqual(request.run_key, "reconcile:0123456789abcdef")
            self.assertEqual(request.tags["foundation/run-kind"], "triggered")

    def test_a_job_s_sensor_knows_the_reconciler_only_once_dagster_runs_it(self):
        import observe_assets

        import decide

        jobs, graph = (copy.deepcopy(value) for value in observe_assets.contracts())
        by_id = {job["id"]: job for job in jobs["jobs"]}
        # The bake is the end of its chain: nothing started by its outputs has to move before it.
        by_id["by_pnu_unit_price_bake"]["dagster_stage"] = "dagster"
        reconciler = next(job for job in jobs["jobs"] if job.get("reconciles"))
        stages = (*observe_assets.job_specs.IMPLEMENTED_DAGSTER_STAGES, "dagster")
        seen = []
        with mock.patch.object(observe_assets.job_specs, "IMPLEMENTED_DAGSTER_STAGES", stages), \
                mock.patch.object(decide, "evaluator", lambda spec, limit, reconcilers=(): seen.append(
                    [found.job_id for found in reconcilers]) or (lambda context: None)):
            reconciler["dagster_stage"] = "proxied"
            decide.sensors(jobs, graph)
            # The order rule refuses the other arrangement: a job at dagster under a reconciler Airflow runs.
            reconciler["dagster_stage"] = "shadow"
            with self.assertRaises(observe_assets.job_specs.JobListError):
                decide.sensors(jobs, graph)
        self.assertEqual(seen, [[reconciler["id"]]])


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class TheSensorTest(unittest.TestCase):
    """The sensor a job at `dagster` gets from the job list, against a Dagster instance."""

    def defs(self):
        import observe_assets
        import run_jobs

        import decide

        jobs, graph = (copy.deepcopy(value) for value in observe_assets.contracts())
        next(job for job in jobs["jobs"] if job["id"] == "gold_unit_price_rebuild")["dagster_stage"] = "dagster"
        stages = (*observe_assets.job_specs.IMPLEMENTED_DAGSTER_STAGES, "dagster")
        with mock.patch.object(observe_assets.job_specs, "IMPLEMENTED_DAGSTER_STAGES", stages), \
                mock.patch.object(observe_assets.job_specs, "dagster_order", lambda *_: []):
            return decide.sensors(jobs, graph), run_jobs.run_jobs(jobs, graph), jobs, graph

    def test_one_sensor_per_dagster_job_starts_its_dagster_job_with_its_priority(self):
        import observe_assets
        from dagster import AssetMaterialization, DagsterInstance, RunRequest, SkipReason, build_sensor_context

        import decide

        sensors, jobs_defs, _, _ = self.defs()
        (sensor,) = sensors
        self.assertEqual((sensor.name, sensor.job_name), ("foundation_decide__gold_unit_price_rebuild",
                                                         "foundation_job__gold_unit_price_rebuild"))
        self.assertIn(sensor.job_name, {job.name for job in jobs_defs})
        spec = spec_of("gold_unit_price_rebuild")
        clock = [at("2026-11-09T10:00:00")]
        evaluate = decide.evaluator(spec, limit_of(spec.job_id), clock=lambda: clock[0])
        with DagsterInstance.ephemeral() as instance:
            key = observe_assets.asset_key(spec.starting_inputs[0])
            instance.report_runless_asset_event(AssetMaterialization(key))  # before the first look: not its
            context = build_sensor_context(instance=instance)
            self.assertIsInstance(evaluate(context), SkipReason)
            instance.report_runless_asset_event(AssetMaterialization(key))
            clock[0] += STEP
            context = build_sensor_context(instance=instance, cursor=context.cursor)
            (request,) = evaluate(context)
            self.assertIsInstance(request, RunRequest)
            self.assertTrue(request.run_key.startswith("inputs:"))
            self.assertEqual(request.tags["foundation/run-kind"], "triggered")
            self.assertEqual(request.tags["dagster/priority"], str(spec.priority))
            clock[0] += STEP
            context = build_sensor_context(instance=instance, cursor=context.cursor)
            self.assertIsInstance(evaluate(context), SkipReason, "the same event never starts a second run")


if __name__ == "__main__":
    unittest.main()
