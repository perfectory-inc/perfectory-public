"""Root ADR-0565: what the `dagster` stage asks of the job list before a job may be at it.

The reconciler and the jobs it starts move in an order in which each can reach the other, a job Dagster
decides is still started again from the start queue, and the deploy asks no "owed by inputs" for it.
Pure Python over the real job list (the default CI runs it; so does the `dagster-observe` lane).
"""

import copy
import dataclasses
import datetime
import json
import itertools
import pathlib
import re
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
        # The walk starts from the list before any group switched (root ADR-0583 §1: Gold already has).
        for job_id in started:
            if by_id[job_id].get("dagster_stage") == "dagster":
                by_id[job_id]["dagster_stage"] = "shadow"
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
        # The list before any group switched (root ADR-0583 §1), so the first requeued job is Airflow's.
        for job in jobs["jobs"]:
            if job.get("dagster_stage") == "dagster":
                job["dagster_stage"] = "shadow"
        reconciler = next(job for job in jobs["jobs"] if job.get("requeues_refused_starts_in"))
        requeued = job_specs.requeued(jobs, reconciler)
        self.assertTrue(requeued)
        next(job for job in jobs["jobs"] if job["id"] == requeued[0])["dagster_stage"] = "dagster"
        self.assertEqual(job_specs.requeued(jobs, reconciler), requeued)

        spec = next(dataclasses.replace(spec, dagster_stage="shadow") for spec in job_specs.load_specs()
                    if spec.started_by == "inputs" and spec.enabled)
        changed = {job_specs.asset_uri(dataset): datetime.datetime(2099, 1, 1, tzinfo=datetime.UTC)
                   for dataset in spec.starting_inputs}
        self.assertEqual(job_specs.owed_by_inputs([spec], changed, {}, set()), [spec.job_id])
        moved = dataclasses.replace(spec, dagster_stage="dagster")
        self.assertEqual(job_specs.owed_by_inputs([moved], changed, {}, set()), [])


class OneRuleForAMissedTick(unittest.TestCase):
    """Review M1: the reconciler's missed-start catch-up would start, 15 minutes after any slot, a job whose
    own sensor decided not to run that missed tick. A job Dagster decides has its sensor's rule only."""

    def test_the_reconciler_does_not_catch_up_a_job_dagster_decides(self):
        sys.path.insert(0, str(PLATFORM / "scripts" / "ops"))
        sys.path.insert(0, str(PLATFORM / "scripts" / "deploy"))
        import asset_reconcile

        jobs, _ = (copy.deepcopy(value) for value in real_inputs())
        reconciler = next(job for job in jobs["jobs"] if job.get("requeues_refused_starts_in"))
        by_id = {job["id"]: job for job in jobs["jobs"]}
        requeued = [job_id for job_id in job_specs.requeued(jobs, reconciler) if not by_id[job_id].get("takes_turns")]
        candidates = [job_id for job_id in requeued if by_id[job_id].get("dagster_stage") != "dagster"]
        now = datetime.datetime(2099, 1, 2, tzinfo=datetime.UTC).timestamp()

        def owed():
            lines, _ = asset_reconcile.missed_starts(now, jobs, reconciler, {}, {}, lambda job_id: False,
                                                     lambda service: False)
            return {line.split()[1] for line in lines}

        self.assertIn(candidates[0], owed())
        # The Gold group is decided by Dagster (root ADR-0583): none of its jobs is caught up here.
        self.assertFalse(owed() & {job_id for job_id in requeued if by_id[job_id].get("dagster_stage") == "dagster"})
        by_id[candidates[0]]["dagster_stage"] = "dagster"
        self.assertNotIn(candidates[0], owed())


class ARunOutlivesADeploy(unittest.TestCase):
    """Root ADR-0565 §9: the code location is its own server, a deploy reloads it and restarts nothing, and a
    run whose process died anyway is failed by run monitoring at its max runtime. 2026-11-14 03:12Z a deploy
    restarted the daemon under an asset-check run, which stayed STARTED four days."""

    @staticmethod
    def service(compose, name):
        """The lines of one compose service, up to the next service."""
        lines = compose.split(f"\n  {name}:\n", 1)[1].splitlines()
        return "\n".join(itertools.takewhile(lambda line: not re.match(r"^  [a-z]", line), lines))

    def test_the_daemon_and_the_webserver_read_the_code_location_from_its_server(self):
        workspace = (PLATFORM / "orchestration/dagster/workspace.yaml").read_text(encoding="utf-8")
        self.assertIn("grpc_server:", workspace)
        self.assertNotIn("python_file:", workspace)
        compose = (PLATFORM / "compose.orchestration.yml").read_text(encoding="utf-8")
        server = self.service(compose, "dagster-code-server")
        self.assertIn('"code-server", "start"', server)
        self.assertIn("FOUNDATION_HOST_CONNECTION", server, "runs are the code server's processes")
        self.assertNotIn("FOUNDATION_HOST_CONNECTION", self.service(compose, "dagster-daemon"),
                         "the daemon runs nothing on the host")

    def test_a_deploy_reloads_the_location_and_restarts_nothing(self):
        runtime = (PLATFORM / "scripts/deploy/airflow-runtime.sh").read_text(encoding="utf-8")
        sync = runtime.split("sync_dagster() {", 1)[1].split("\n}\n", 1)[0]
        self.assertNotIn("compose restart", sync)
        # Through the writable webserver's gate, from inside its container (root ADR-0565 §8).
        self.assertIn("dagster_ask reload", sync)
        self.assertIn("reloadRepositoryLocation", (PLATFORM / "orchestration/dags/dagster_writes.py").read_text(encoding="utf-8"))
        self.assertNotIn("19085/graphql", runtime, "no host port reaches the writable webserver")

    def test_only_the_read_only_webserver_has_a_host_port(self):
        compose = (PLATFORM / "compose.orchestration.yml").read_text(encoding="utf-8")
        for name in ("dagster-webserver", "dagster-code-server", "dagster-daemon"):
            self.assertNotIn("ports:", self.service(compose, name), name)
        read_only = self.service(compose, "dagster-webserver-read-only")
        self.assertIn('"--read-only"]', read_only)
        self.assertIn('"127.0.0.1:19085:3000"', read_only)
        writable = self.service(compose, "dagster-webserver")
        self.assertIn("/opt/dagster/gate/writer_gate.py", writable)
        self.assertNotIn('"dagster-webserver",', writable, "the writable webserver runs only behind its gate")

    def test_every_run_has_a_max_runtime(self):
        queue = (PLATFORM / "orchestration/dagster/dagster.yaml").read_text(encoding="utf-8")
        self.assertRegex(queue, r"run_monitoring:\n  enabled: true\n  max_runtime_seconds: \d+")


if __name__ == "__main__":
    unittest.main()
