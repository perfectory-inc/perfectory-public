"""A proxied job's `run` task in the release's own Airflow image (orchestration/airflow/Dockerfile, root ADR-0431 §5).

test_job_dags.py checks the DAG file against stand-ins; this runs it against apache/airflow 3.3.2 and the
dagster-airlift wheel the image installs, with Dagster's webserver replaced by a GraphQL server on the
loopback that answers as Dagster 1.13 does: the proxied task launches the job's Dagster job by name, with
how the run was started, waits for it, and hands its outcome to `publish_outputs` as before. Run by the CI
`dagster-observe` lane (scripts/verify/foundation-dagster-observe.sh) in that image; skipped elsewhere,
where a skip fails that lane.
"""

import dataclasses
import http.server
import importlib.util
import json
import os
import pathlib
import sys
import threading
import types
import unittest
from unittest import mock

ORCHESTRATION = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ORCHESTRATION / "dags"))

import job_specs  # noqa: E402

HAS_AIRFLOW = all(importlib.util.find_spec(name) is not None for name in ("airflow", "dagster_airlift"))
NEEDS_AIRFLOW = "requires the release's Airflow image (the CI dagster-observe lane runs it)"
JOB = "onbid_public_auction_collect"


class Dagster(http.server.BaseHTTPRequestHandler):
    """Dagster's /graphql as the proxied task uses it; every query is kept."""

    asked, ended = [], ("SUCCESS", {})

    def do_POST(self):  # noqa: N802 - http.server's name
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        Dagster.asked.append(body)
        query = body["query"]
        if "launchPipelineExecution" in query:
            data = {"launchPipelineExecution": {"__typename": "LaunchRunSuccess", "run": {"id": "run-1"}}}
        elif "runOrError" in query:
            status, tags = Dagster.ended
            data = {"runOrError": {"__typename": "Run", "id": "run-1", "status": status,
                                   "tags": [{"key": key, "value": value} for key, value in tags.items()]}}
        else:
            data = {"version": "1.13.27"}
        answer = json.dumps({"data": data}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(answer)))
        self.end_headers()
        self.wfile.write(answer)

    def log_message(self, *args):
        pass


@unittest.skipUnless(HAS_AIRFLOW, NEEDS_AIRFLOW)
class TheProxiedRunInAirflow(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Dagster)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        real = job_specs.load_specs

        def load(*args, **kwargs):
            return [dataclasses.replace(spec, dagster_stage="proxied") if spec.job_id == JOB else spec
                    for spec in real(*args, **kwargs)]

        url = f"http://127.0.0.1:{cls.server.server_address[1]}"
        with mock.patch.object(job_specs, "load_specs", load), mock.patch.dict(os.environ, {"FOUNDATION_DAGSTER_URL": url}):
            spec = importlib.util.spec_from_file_location("foundation_jobs_proxied", ORCHESTRATION / "dags" / "foundation_jobs.py")
            cls.module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(cls.module)
        cls.spec = next(spec for spec in cls.module.SPECS if spec.job_id == JOB)
        cls.dag = getattr(cls.module, cls.spec.dag_id)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def execute(self, ended, run_type="scheduled"):
        Dagster.asked.clear()
        Dagster.ended = ended
        task = self.dag.get_task("run")
        ti = mock.Mock()
        context = {"ti": ti, "task": task, "dag": self.dag, "logical_date": None,
                   "dag_run": types.SimpleNamespace(dag_id=self.dag.dag_id, run_id="scheduled__2026-11-09T00:00:00+00:00",
                                                    run_type=run_type, conf={})}
        return task, ti, context

    def test_the_run_task_is_airlifts_proxied_task_in_the_jobs_pool(self):
        from dagster_airlift.in_airflow import BaseProxyTaskToDagsterOperator

        task = self.dag.get_task("run")
        self.assertIsInstance(task, BaseProxyTaskToDagsterOperator)
        self.assertEqual((task.pool, task.pool_slots, task.priority_weight), (self.spec.pool, self.spec.pool_slots,
                                                                            self.spec.priority_weight))
        self.assertEqual(sorted(self.dag.task_dict), sorted(["check", "run", "publish_outputs"]
                                                            if self.spec.check_service else ["run", "publish_outputs"]))

    def test_it_launches_the_jobs_dagster_job_and_passes_its_outcome_on(self):
        task, ti, context = self.execute(("SUCCESS", {"foundation/job-outcome": "changed"}), run_type="asset_triggered")
        task.execute(context)
        (launch,) = [body for body in Dagster.asked if "launchPipelineExecution" in body["query"]]
        params = launch["variables"]["executionParams"]
        self.assertEqual(params["selector"]["pipelineName"], f"foundation_job__{JOB}")
        tags = {tag["key"]: tag["value"] for tag in params["executionMetadata"]["tags"]}
        self.assertEqual(tags["foundation/run-kind"], "triggered")
        self.assertEqual(tags["dagster-airlift/airflow-task-id"], "run")
        ti.xcom_push.assert_called_once_with(key="outcome", value="changed")

    def test_a_waiting_run_is_skipped_and_a_failed_one_fails(self):
        from airflow.sdk.exceptions import AirflowSkipException

        task, ti, context = self.execute(("SUCCESS", {"foundation/job-outcome": "waiting"}))
        with self.assertRaises(AirflowSkipException):
            task.execute(context)
        task, ti, context = self.execute(("FAILURE", {"foundation/job-outcome": "failed"}))
        with self.assertRaisesRegex(RuntimeError, "FAILURE"):
            task.execute(context)

    def test_the_dag_serializes_with_its_proxied_task(self):
        from airflow.serialization.serialized_objects import DagSerialization

        restored = DagSerialization.from_dict(DagSerialization.to_dict(self.dag))
        self.assertEqual(set(restored.task_dict), set(self.dag.task_dict))


if __name__ == "__main__":
    unittest.main()
