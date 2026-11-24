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
                    body(dagster_writes.WAITING_RUN, {"job": "foundation_job__synthetic_nightly"}),
                    body(dagster_writes.RELOAD_LOCATION),
                    body(dagster_writes.LOCATION_ERRORS),
                    # Whitespace is not a different document.
                    body("  " + dagster_writes.RELOAD_LOCATION.replace("\n", "\n\t\t") + "\n")):
            with self.subTest(raw[:60]):
                self.assertIsNone(host(raw))
        self.assertIsNone(host(b"", "GET", "/server_info"))
        self.assertIsNotNone(dagster_writes.refused(dagster_writes.HOST, "GET", "/server_info", b""),
                             "the readiness route is the host's only")

    def test_the_waiting_run_is_asked_of_one_job_and_nothing_else(self):
        for variables in ({"job": "__ASSET_JOB"}, {"job": "foundation_job__"}, {"job": 7}, {},
                          {"job": "foundation_job__synthetic_nightly", "limit": 500}):
            with self.subTest(variables):
                self.assertIsNotNone(host(body(dagster_writes.WAITING_RUN, variables)))

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
        q = dagster_writes.airlift_queries()
        RUNS_QUERY, TRIGGER_ASSETS_MUTATION, VERIFICATION_QUERY = q.RUNS_QUERY, q.TRIGGER_ASSETS_MUTATION, q.VERIFICATION_QUERY
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


class DagstersPoolLimitsAreTheJobLists(unittest.TestCase):
    """Root ADR-0605: the deploy sets Dagster's own pool limits from jobs.v1.json pools and reads them back."""

    def test_the_gate_sets_every_limit_the_job_list_may_declare(self):
        sys.path.insert(0, str(PLATFORM / "orchestration" / "dags"))
        import job_specs  # noqa: PLC0415
        self.assertEqual(dagster_writes.MAX_POOL_LIMIT, job_specs.MAX_POOL_SLOTS)

    def test_the_host_may_read_set_and_delete_a_pool_limit_and_nothing_wider(self):
        self.assertIsNone(host(body(dagster_writes.POOL_LIMITS)))
        self.assertIsNone(host(body(dagster_writes.SET_POOL_LIMIT, {"concurrencyKey": "duckdb", "limit": 2})))
        self.assertIsNone(host(body(dagster_writes.DELETE_POOL_LIMIT, {"concurrencyKey": "documents"})))
        for variables in ({"concurrencyKey": "duckdb", "limit": 0},  # 0 would stop every run of the pool
                          {"concurrencyKey": "duckdb", "limit": dagster_writes.MAX_POOL_LIMIT + 1},
                          {"concurrencyKey": "Duck-DB", "limit": 2},
                          {"concurrencyKey": "duckdb", "limit": "2"},
                          {"concurrencyKey": "duckdb", "limit": 2, "extra": 1}):
            with self.subTest(variables):
                self.assertIsNotNone(host(body(dagster_writes.SET_POOL_LIMIT, variables)))
        self.assertIsNotNone(host(body(dagster_writes.DELETE_POOL_LIMIT, {"concurrencyKey": "../x"})))
        self.assertIsNotNone(host(body(dagster_writes.POOL_LIMITS, {"x": 1})))

    def dagster(self, held, refuse_set=False):
        """A stand-in for the writer socket: each document must pass the host's gate, as the real one does."""
        told = []

        def graphql(document, variables=None):
            self.assertIsNone(host(body(document, variables)))
            told.append((document, variables))
            if document == dagster_writes.POOL_LIMITS:
                return {"instance": {"concurrencyLimits": [
                    {"concurrencyKey": key, "limit": limit, "usingDefaultLimit": limit is None} for key, limit in held.items()]}}
            if document == dagster_writes.SET_POOL_LIMIT and not refuse_set:
                held[variables["concurrencyKey"]] = variables["limit"]
            if document == dagster_writes.DELETE_POOL_LIMIT:
                held.pop(variables["concurrencyKey"])
            return {}
        return graphql, told

    def test_the_list_is_set_an_undeclared_pool_deleted_and_both_read_back(self):
        held = {"spark": 3, "documents": 1, "duckdb": None}
        graphql, told = self.dagster(held)
        original, writer_gate.graphql = writer_gate.graphql, graphql
        try:
            # Before the reload: the list's pools are set, an undeclared one stays (a loaded op may still name it).
            self.assertEqual(writer_gate.pools({"spark": 1, "duckdb": 2, "gpu": 1}), 0)
            self.assertEqual(held, {"spark": 1, "documents": 1, "duckdb": 2, "gpu": 1})
            # After it: pruned, and read back exactly.
            self.assertEqual(writer_gate.pools({"spark": 1, "duckdb": 2, "gpu": 1}, prune=True), 0)
        finally:
            writer_gate.graphql = original
        self.assertEqual(held, {"spark": 1, "duckdb": 2, "gpu": 1})
        sets = [variables for document, variables in told if document == dagster_writes.SET_POOL_LIMIT]
        # A pool Dagster gave its default limit is set too: the default is not the list's.
        self.assertEqual(sorted(sets, key=lambda v: v["concurrencyKey"]),
                         [{"concurrencyKey": "duckdb", "limit": 2}, {"concurrencyKey": "gpu", "limit": 1},
                          {"concurrencyKey": "spark", "limit": 1}])
        # Already the list's: nothing is written.
        graphql, told = self.dagster(held)
        original, writer_gate.graphql = writer_gate.graphql, graphql
        try:
            self.assertEqual(writer_gate.pools({"spark": 1, "duckdb": 2, "gpu": 1}), 0)
        finally:
            writer_gate.graphql = original
        self.assertEqual([document for document, _ in told], [dagster_writes.POOL_LIMITS] * 2)

    def test_the_rollback_prunes_every_pool(self):
        # Root ADR-0605 §8: before a revert deploys, `echo '{}' | ... ask pools --prune` leaves Dagster no limit.
        held = {"spark": 1, "duckdb": 2, "gpu": 1}
        graphql, _ = self.dagster(held)
        original, writer_gate.graphql = writer_gate.graphql, graphql
        try:
            self.assertEqual(writer_gate.pools({}, prune=True), 0)
        finally:
            writer_gate.graphql = original
        self.assertEqual(held, {})

    def test_a_limit_dagster_does_not_hold_after_the_deploy_or_a_bad_list_fails(self):
        graphql, _ = self.dagster({"spark": 3}, refuse_set=True)
        original, writer_gate.graphql = writer_gate.graphql, graphql
        try:
            self.assertEqual(writer_gate.pools({"spark": 1}), 1)
            for declared in ({"spark": 0}, {"Spark": 1}, ["spark"], {"spark": "1"}):
                with self.subTest(declared):
                    self.assertEqual(writer_gate.pools(declared), 1)
        finally:
            writer_gate.graphql = original


if __name__ == "__main__":
    unittest.main()
