"""A run Dagster decides tells the data catalog of itself as its DAG did (orchestration/dagster/lineage.py,
root ADR-0565 §7).

Airflow's OpenLineage provider reports a DAG run as job `foundation_<id>` and its `run` task as
`foundation_<id>.run` with the DAG run as parent, in namespace `perfectory`; DataHub files both under the
flow of the DAG's id, named by the event's processing engine (production 2026-11-18:
`urn:li:dataFlow:(airflow,foundation_real_estate_broker_lookup_load,perfectory)` with children
`foundation_real_estate_broker_lookup_load` and `foundation_real_estate_broker_lookup_load.run`). The op of a job
at `dagster` sends the same names; foundation-api operations.rs joins the two engines' flows by id and
job name (its test `a_job_moved_to_dagster_keeps_one_run_history_across_the_two_engines_flows`).

Run by the CI `dagster-observe` lane in the code location's image, which holds openlineage-python;
skipped elsewhere, where a skip fails that lane.
"""

import dataclasses
import importlib.util
import pathlib
import sys
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "orchestration" / "dags"))
sys.path.insert(0, str(PLATFORM / "orchestration" / "dagster"))
sys.path.insert(0, str(PLATFORM / "orchestration" / "tests"))

HAS_DAGSTER = importlib.util.find_spec("dagster") is not None and importlib.util.find_spec("openlineage") is not None
NEEDS_DAGSTER = "requires dagster and openlineage-python (the CI dagster-observe lane runs it)"


class Recorder:
    """An OpenLineage client that keeps what it was given, or refuses every event."""

    def __init__(self, refuses=False):
        self.events, self.refuses = [], refuses

    def emit(self, event):
        if self.refuses:
            raise ConnectionError("the catalog is down")
        self.events.append(event)


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class ARunDagsterDecidesReportsItself(unittest.TestCase):
    def spec(self, stage="dagster", retries=0):
        import observe_assets

        spec = next(spec for spec in observe_assets.job_specs.load_specs()
                    if spec.enabled and spec.inputs and spec.outputs)
        return dataclasses.replace(spec, dagster_stage=stage, retries=retries, check_service=None)

    def execute(self, spec, lines, status=0, client=None):
        import run_jobs
        from dagster import DagsterInstance
        from test_dagster_systemd_pipes import Stand

        client = client or Recorder()
        instance = DagsterInstance.ephemeral()
        self.addCleanup(instance.dispose)
        result = run_jobs.run_job(spec, transport=Stand(lines, status), lineage_client=client).execute_in_process(
            instance=instance, raise_on_error=False)
        return result, client.events

    @staticmethod
    def shape(events):
        return [(event.job.name, str(event.eventType.value)) for event in events]

    def test_a_run_reports_the_dag_and_its_run_task_by_the_names_airflow_used(self):
        spec = self.spec()
        result, events = self.execute(spec, ["foundation-job-outcome changed"])
        self.assertTrue(result.success)
        run_task = f"{spec.dag_id}.run"
        self.assertEqual(self.shape(events), [(spec.dag_id, "START"), (run_task, "START"),
                                              (run_task, "COMPLETE"), (spec.dag_id, "COMPLETE")])
        self.assertEqual({event.job.namespace for event in events}, {"perfectory"})
        dag_start, task_start = events[0], events[1]
        self.assertEqual(dag_start.run.runId, result.run_id, "the run is the Dagster run")
        parent = task_start.run.facets["parent"]
        self.assertEqual((parent.run.runId, parent.job.namespace, parent.job.name),
                         (result.run_id, "perfectory", spec.dag_id))
        self.assertEqual(task_start.run.facets["processing_engine"].name, "Dagster",
                         "DataHub names the flow by the engine that ran it; its endpoint refuses an event without one")
        self.assertEqual([(dataset.namespace, dataset.name) for dataset in task_start.inputs], list(spec.inputs))
        self.assertEqual([(dataset.namespace, dataset.name) for dataset in task_start.outputs], list(spec.outputs))
        self.assertEqual((dag_start.inputs, dag_start.outputs), ([], []), "the datasets are the task's, as Airflow sent them")

    def test_a_run_with_nothing_to_do_completes_as_airflow_s_skipped_task_did(self):
        for outcome in ("unchanged", "waiting held by the deploy"):
            with self.subTest(outcome):
                result, events = self.execute(self.spec(), [f"foundation-job-outcome {outcome}"])
                self.assertTrue(result.success)
                self.assertEqual([state for _, state in self.shape(events)], ["START", "START", "COMPLETE", "COMPLETE"])

    def test_a_failed_last_try_fails_the_run_too(self):
        result, events = self.execute(self.spec(), ["foundation-job-outcome failed systemd result=timeout"], 1)
        self.assertFalse(result.success)
        self.assertEqual([state for _, state in self.shape(events)], ["START", "START", "FAIL", "FAIL"])

    def test_a_catalog_that_does_not_answer_never_fails_the_run(self):
        result, events = self.execute(self.spec(), ["foundation-job-outcome changed"], client=Recorder(refuses=True))
        self.assertTrue(result.success)

    def test_a_proxied_job_s_run_is_reported_by_its_dag_alone(self):
        result, events = self.execute(self.spec(stage="proxied"), ["foundation-job-outcome changed"])
        self.assertTrue(result.success)
        self.assertEqual(events, [], "Airflow's provider reports it; a second report would count it twice")

    def test_each_try_is_its_own_run_of_the_run_task_and_a_resend_names_the_same_one(self):
        import lineage

        run_id = "01a1edc6-ef40-7b67-ae96-3cccbcbe2a6a"
        self.assertEqual(lineage.try_run_id(run_id, 0), lineage.try_run_id(run_id, 0))
        self.assertNotEqual(lineage.try_run_id(run_id, 0), lineage.try_run_id(run_id, 1))

    def test_the_code_server_reports_where_airflow_does(self):
        compose = (PLATFORM / "compose.orchestration.yml").read_text(encoding="utf-8")
        self.assertIn("AIRFLOW__OPENLINEAGE__TRANSPORT: *openlineage-transport", compose)
        self.assertIn("FOUNDATION_OPENLINEAGE_TRANSPORT: *openlineage-transport", compose)


if __name__ == "__main__":
    unittest.main()
