"""The writable Dagster webserver answers only its two callers (root ADR-0565 §8).

dagster_writes.py holds what each caller may send; orchestration/dagster/writer_gate.py puts it in front
of Dagster's own webserver. The rules are plain Python and run on every runner; Airflow's documents are
dagster-airlift's own, so their cases run where it is installed (the CI `dagster-observe` lane, in the
Dagster image the gate ships in, where a skip fails the lane). What the deploy, the run-now and a
proxied task really send passes these rules in test_dagster_job_orchestrator.py and
test_airflow_proxied_run.py.
"""

import asyncio
import importlib.util
import json
import pathlib
import sys
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "orchestration" / "dags"))
sys.path.insert(0, str(PLATFORM / "orchestration" / "dagster"))

import dagster_writes  # noqa: E402
import writer_gate  # noqa: E402

HAS_AIRLIFT = importlib.util.find_spec("dagster_airlift") is not None
NEEDS_AIRLIFT = "requires dagster-airlift (the CI dagster-observe lane runs it)"
# Dagster 1.13.27 gives a sensor state the compound id `<origin id>::<selector id>` (CompoundID).
STATE = "0f" * 20 + "::" + "a1" * 20
RUN = "6f1c2a3b-4d5e-4f60-8a71-b82c93d4e5f6"


def body(query, variables=None, **extra):
    return json.dumps({"query": query, "variables": variables, **extra}).encode()


def launch_params(job="foundation_job__synthetic_nightly", **selector):
    return {"mode": "default", "executionMetadata": {"tags": [{"key": "foundation/run-kind", "value": "triggered"}]},
            "runConfigData": "{}", "selector": {**dagster_writes.REPOSITORY, "pipelineName": job, **selector}}


def host(raw, method="POST", path="/graphql"):
    return dagster_writes.refused(dagster_writes.HOST, method, path, raw, ready=True)


class WhatTheHostMaySend(unittest.TestCase):
    def test_the_deploys_and_the_run_nows_documents(self):
        for raw in (body(dagster_writes.SENSORS, {"repository": dagster_writes.REPOSITORY}),
                    body(dagster_writes.STOP_SENSOR, {"id": STATE}),
                    body(dagster_writes.START_SENSOR, {"sensor": {**dagster_writes.REPOSITORY,
                                                                   "sensorName": "foundation_decide__synthetic_nightly"}}),
                    body(dagster_writes.LAUNCH_RUN, {"params": launch_params()}),
                    body(dagster_writes.RELOAD_LOCATION),
                    body(dagster_writes.LOCATION_ERRORS),
                    # Whitespace is not a different document.
                    body("  " + dagster_writes.RELOAD_LOCATION.replace("\n", "\n\t\t") + "\n")):
            with self.subTest(raw[:60]):
                self.assertIsNone(host(raw))
        self.assertIsNone(host(b"", "GET", "/server_info"))
        self.assertIsNotNone(dagster_writes.refused(dagster_writes.HOST, "GET", "/server_info", b""),
                             "the readiness route is the host's only")

    def test_any_other_document_or_shape_is_refused(self):
        cases = {
            "another mutation": body("mutation { terminateRuns(runIds: []) { __typename } }"),
            "wiping": body('mutation { wipeAssets(assetPartitionRanges: []) { __typename } }'),
            "a launch elsewhere": body(dagster_writes.LAUNCH_RUN, {"params": launch_params(repositoryLocationName="other")}),
            "a launch of no job": body(dagster_writes.LAUNCH_RUN, {"params": launch_params(job="__ASSET_JOB")}),
            "a launch with config": body(dagster_writes.LAUNCH_RUN, {"params": {**launch_params(), "runConfigData": "ops: {}"}}),
            "a launch of a subset": body(dagster_writes.LAUNCH_RUN, {"params": {**launch_params(), "stepKeys": ["x"]}}),
            "another sensor": body(dagster_writes.START_SENSOR, {"sensor": {**dagster_writes.REPOSITORY, "sensorName": "other"}}),
            "a stop of no state": body(dagster_writes.STOP_SENSOR, {"id": "x; drop"}),
            "a stop of a state with a third part": body(dagster_writes.STOP_SENSOR, {"id": STATE + "::ff"}),
            "variables where none": body(dagster_writes.RELOAD_LOCATION, {"x": 1}),
            "a batch": json.dumps([{"query": dagster_writes.RELOAD_LOCATION}]).encode(),
            "extra fields": body(dagster_writes.RELOAD_LOCATION, extensions={}),
            "not JSON": b"query=mutation",
            "too large": body(dagster_writes.LOCATION_ERRORS, {"pad": "x" * dagster_writes.MAX_BODY_BYTES}),
        }
        for name, raw in cases.items():
            with self.subTest(name):
                self.assertIsNotNone(host(raw))
        for method, path in (("GET", "/graphql"), ("GET", "/"), ("POST", "/report_asset_check/x"), ("GET", "/graphiql")):
            with self.subTest(path):
                self.assertIsNotNone(host(body(dagster_writes.RELOAD_LOCATION), method, path))


@unittest.skipUnless(HAS_AIRLIFT, NEEDS_AIRLIFT)
class WhatAirflowMaySend(unittest.TestCase):
    def test_airlifts_documents_and_the_terminate_and_nothing_of_the_hosts(self):
        from dagster_airlift.in_airflow.gql_queries import RUNS_QUERY, TRIGGER_ASSETS_MUTATION, VERIFICATION_QUERY

        allowed = dagster_writes.airflow_allowed()
        for raw in (body(VERIFICATION_QUERY), body(TRIGGER_ASSETS_MUTATION, {"executionParams": launch_params()}),
                    body(RUNS_QUERY, {"runId": RUN}), body(dagster_writes.TERMINATE_RUN, {"runId": RUN})):
            with self.subTest(raw[:60]):
                self.assertIsNone(dagster_writes.refused(allowed, "POST", "/graphql", raw))
        for raw in (body(dagster_writes.RELOAD_LOCATION), body(dagster_writes.STOP_SENSOR, {"id": STATE}),
                    body(TRIGGER_ASSETS_MUTATION, {"executionParams": launch_params(job="__ASSET_JOB")})):
            with self.subTest(raw[:60]):
                self.assertIsNotNone(dagster_writes.refused(allowed, "POST", "/graphql", raw))
        self.assertIsNotNone(dagster_writes.refused(allowed, "GET", "/server_info", b""))


class App:
    """An ASGI app recording what reached it."""

    def __init__(self):
        self.seen = []

    async def __call__(self, scope, receive, send):
        message = await receive() if scope["type"] == "http" else None
        self.seen.append((scope["type"], message and message["body"]))
        if scope["type"] == "http":
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"{}"})


def call(gate, scope, chunks):
    sent, queue = [], [{"type": "http.request", "body": chunk, "more_body": n < len(chunks) - 1}
                        for n, chunk in enumerate(chunks)]

    async def receive():
        return queue.pop(0) if queue else {"type": "http.disconnect"}

    async def send(message):
        sent.append(message)

    asyncio.run(gate(scope, receive, send))
    return sent


class TheGate(unittest.TestCase):
    def setUp(self):
        self.app = App()
        self.gate = writer_gate.Gate(self.app, dagster_writes.HOST, "host", ready=True)

    def test_an_allowed_request_reaches_dagster_whole_and_a_refused_one_never_does(self):
        raw = body(dagster_writes.RELOAD_LOCATION)
        sent = call(self.gate, {"type": "http", "method": "POST", "path": "/graphql"}, [raw[:10], raw[10:]])
        self.assertEqual(self.app.seen, [("http", raw)])
        self.assertEqual(sent[0]["status"], 200)
        sent = call(self.gate, {"type": "http", "method": "POST", "path": "/graphql"},
                    [body("mutation { terminateRuns(runIds: []) { __typename } }")])
        self.assertEqual(len(self.app.seen), 1)
        self.assertEqual(sent[0]["status"], 403)
        self.assertIn(b"refused by the writer gate", sent[1]["body"])

    def test_the_ui_websocket_is_closed_and_the_lifespan_passes(self):
        sent = call(self.gate, {"type": "websocket", "path": "/graphql"}, [])
        self.assertEqual(sent, [{"type": "websocket.close", "code": 1008}])
        call(self.gate, {"type": "lifespan"}, [])
        self.assertEqual(self.app.seen, [("lifespan", None)])

    def test_the_deploys_questions_are_the_ones_the_host_may_ask(self):
        for document in writer_gate.ASKS.values():
            self.assertIsNone(host(body(document)))
        self.assertEqual(writer_gate.main(["ask", "anything"]), 64)


if __name__ == "__main__":
    unittest.main()
