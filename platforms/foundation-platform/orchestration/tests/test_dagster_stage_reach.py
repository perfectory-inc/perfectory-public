"""Root ADR-0565: what the `dagster` stage asks of the job list before a job may be at it.

The reconciler and the jobs it starts move in an order in which each can reach the other, a job Dagster
decides is still started again from the start queue, and the deploy asks no "owed by inputs" for it.
Pure Python over the real job list (the default CI runs it; so does the `dagster-observe` lane).
"""

import copy
import dataclasses
import datetime
import json
import pathlib
import sys
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "orchestration" / "dags"))

import job_specs  # noqa: E402


def real_inputs():
    return (json.loads(job_specs.JOBS.read_text(encoding="utf-8")), json.loads(job_specs.GRAPH.read_text(encoding="utf-8")))


class TheOrderOfTheReconcilerAndWhatItStarts(unittest.TestCase):
    def test_a_reconciler_is_proxied_before_the_first_job_it_starts_moves_and_dagster_after_the_last(self):
        # Root ADR-0565 §4. The reconciler says which jobs are owed a run; a job Dagster decides reads that
        # from the reconciler's Dagster run (so Dagster must run the reconciler), and a reconciler Dagster
        # decides has no DAG whose `start` task could trigger the DAG of a job Airflow decides.
        jobs, graph = (copy.deepcopy(value) for value in real_inputs())
        feeders = job_specs.producers(jobs, graph)
        by_id = {job["id"]: job for job in jobs["jobs"]}
        reconciler = next(job for job in jobs["jobs"] if job.get("reconciles"))
        started = job_specs.started_for(jobs, reconciler)
        self.assertTrue(set(reconciler["reconciles"]) <= set(started))
        self.assertTrue(set(job_specs.requeued(jobs, reconciler)) <= set(started))
        first = started[0]

        def problems():
            return [problem for problem in job_specs.dagster_order(jobs, feeders)
                    if reconciler["id"] in problem and "owed a run" in problem or problem.startswith(reconciler["id"])]

        reconciler["dagster_stage"] = "shadow"
        by_id[first]["dagster_stage"] = "dagster"
        self.assertEqual([problem.split(":")[0] for problem in problems()], [first])
        reconciler["dagster_stage"] = "proxied"
        self.assertEqual(problems(), [])
        reconciler["dagster_stage"] = "dagster"
        still_airflow = [job_id for job_id in started if job_id != first]
        self.assertEqual(len(problems()), len(still_airflow))
        for job_id in still_airflow:
            by_id[job_id]["dagster_stage"] = "dagster"
        self.assertEqual(problems(), [])

    def test_a_job_dagster_decides_is_still_requeued_and_misses_no_input_event(self):
        # Before root ADR-0565 a `dagster` job was left out of the reconciler's requeue list ("no DAG to
        # start") and would have had its refused starts dropped; the deploy's "owed by inputs" question
        # (root ADR-0337) is Airflow's, and such a job's sensor keeps the events itself.
        jobs, graph = (copy.deepcopy(value) for value in real_inputs())
        reconciler = next(job for job in jobs["jobs"] if job.get("requeues_refused_starts_in"))
        requeued = job_specs.requeued(jobs, reconciler)
        self.assertTrue(requeued)
        next(job for job in jobs["jobs"] if job["id"] == requeued[0])["dagster_stage"] = "dagster"
        self.assertEqual(job_specs.requeued(jobs, reconciler), requeued)

        spec = next(spec for spec in job_specs.load_specs() if spec.started_by == "inputs" and spec.enabled)
        changed = {job_specs.asset_uri(dataset): datetime.datetime(2099, 1, 1, tzinfo=datetime.UTC)
                   for dataset in spec.starting_inputs}
        self.assertEqual(job_specs.owed_by_inputs([spec], changed, {}, set()), [spec.job_id])
        moved = dataclasses.replace(spec, dagster_stage="dagster")
        self.assertEqual(job_specs.owed_by_inputs([moved], changed, {}, set()), [])


if __name__ == "__main__":
    unittest.main()
