"""Dagster's observe view of Airflow (root ADR-0333 stage 0) yields the materializations Airflow recorded.

The recorded day (fixtures/airflow-recorded-day.json) is production's: every job DAG run that
ended on 2026-10-28 UTC, its publish task and the asset events those runs recorded, its times moved
to 2099 (the public fixtures' reserved namespace).

- ContractParityTest (no Dagster needed): the job specs both orchestrators are generated from say a
  successful publish task records exactly its job's outputs, run for run.
- AirliftParityTest (needs dagster and dagster-airlift; skipped elsewhere, run by the CI
  `dagster-observe` lane in the code location's own image, where a skip fails the lane): the
  generated code location, through dagster-airlift's own polling sensor and our Airflow 3 client,
  serving the day from a stand-in for Airflow 3.3.2's REST API, materializes exactly the assets
  Airflow recorded events for, run for run.

A DAG whose outputs the contracts changed after the recorded day cannot be compared against it; it
is left out and named, and at least half the recorded events must stay comparable. Re-record a day
(the query is in the fixture's `recorded`) when that fails, and after a deploy brings production to
the contracts a merged change declared: main can declare outputs (the onbid Silver tables, #943) a
day before production runs them, so no recorded day matches main exactly in between.
"""

import datetime
import importlib.util
import json
import pathlib
import sys
import unittest
from collections import defaultdict

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "orchestration" / "dags"))
sys.path.insert(0, str(PLATFORM / "orchestration" / "dagster"))

import job_specs  # noqa: E402

DAY = json.loads((pathlib.Path(__file__).parent / "fixtures" / "airflow-recorded-day.json").read_text(encoding="utf-8"))
HAS_DAGSTER = all(importlib.util.find_spec(name) is not None for name in ("dagster", "dagster_airlift"))
NEEDS_DAGSTER = "requires dagster and dagster-airlift (the CI dagster-observe lane runs it)"
COMPARABLE_SHARE = 0.5


def uri(text):
    """Airflow stores an asset URI with a trailing slash after the authority; the contract's has none."""
    return text.rstrip("/")


def recorded_events():
    """(dag_id, run_id) -> the asset URIs Airflow recorded events on."""
    found = defaultdict(set)
    for event in DAY["asset_events"]:
        found[(event["source_dag_id"], event["source_run_id"])].add(uri(event["uri"]))
    return found


def contract_outputs():
    """dag_id -> the asset URIs its job's publish task records, from today's contracts."""
    return {spec.dag_id: {job_specs.asset_uri(dataset) for dataset in spec.outputs} for spec in job_specs.load_specs()}


def comparable(recorded, outputs):
    """The (dag_id, run_id) whose DAG records today what it recorded that day, and the DAGs that do not."""
    drifted = {dag for (dag, _), uris in recorded.items() if outputs.get(dag) != uris}
    return {key for key in recorded if key[0] not in drifted}, drifted


class ContractParityTest(unittest.TestCase):
    def test_a_successful_publish_task_records_its_jobs_outputs(self):
        recorded, outputs = recorded_events(), contract_outputs()
        keep, drifted = comparable(recorded, outputs)
        expected = {
            (ti["dag_id"], ti["dag_run_id"]): outputs[ti["dag_id"]]
            for ti in DAY["publish_task_instances"]
            if ti["state"] == "success" and ti["dag_id"] not in drifted and outputs.get(ti["dag_id"])
        }
        self.assertEqual({key: recorded[key] for key in keep}, expected)
        events = sum(len(uris) for uris in recorded.values())
        kept = sum(len(recorded[key]) for key in keep)
        self.assertGreaterEqual(kept / events, COMPARABLE_SHARE, f"contracts changed for {sorted(drifted)}; re-record a day")

    def test_the_day_is_the_shape_the_parity_needs(self):
        states = {ti["state"] for ti in DAY["publish_task_instances"]}
        # A day with only successes would not prove that a skipped or failed publish materializes nothing.
        self.assertTrue({"success", "skipped", "upstream_failed"} <= states, states)
        self.assertTrue(any(run["logical_date"] is None for run in DAY["dag_runs"]),
                        "Airflow 3 runs without a logical date are what the client must handle")
        self.assertEqual({event["source_task_id"] for event in DAY["asset_events"]}, {job_specs.PUBLISH_TASK_ID})


# The request bodies Airflow 3.3.2 accepts (its v2-rest-api-generated.yaml, `additionalProperties: false`).
DAG_RUNS_BATCH_BODY = {
    "conf_contains", "dag_ids", "duration_gt", "duration_gte", "duration_lt", "duration_lte", "end_date_gt",
    "end_date_gte", "end_date_lt", "end_date_lte", "logical_date_gt", "logical_date_gte", "logical_date_lt",
    "logical_date_lte", "order_by", "page_limit", "page_offset", "run_after_gt", "run_after_gte", "run_after_lt",
    "run_after_lte", "start_date_gt", "start_date_gte", "start_date_lt", "start_date_lte", "states",
}
TASK_INSTANCES_BATCH_BODY = {
    "dag_ids", "dag_run_ids", "duration_gt", "duration_gte", "duration_lt", "duration_lte", "end_date_gt",
    "end_date_gte", "end_date_lt", "end_date_lte", "executor", "logical_date_gt", "logical_date_gte",
    "logical_date_lt", "logical_date_lte", "order_by", "page_limit", "page_offset", "pool", "queue",
    "run_after_gt", "run_after_gte", "run_after_lt", "run_after_lte", "start_date_gt", "start_date_gte",
    "start_date_lt", "start_date_lte", "state", "task_ids",
}
BASE = "http://airflow.test"


def instant(text):
    return datetime.datetime.fromisoformat(text)


class Answer:
    def __init__(self, body, status_code=200):
        self.status_code = status_code
        self._body = body
        self.text = json.dumps(body)

    def json(self):
        return self._body


class RecordedAirflow:
    """Airflow 3.3.2's `/api/v2`, answering from the recorded day; anything else is a 404."""

    def __init__(self, test):
        self.test = test
        self.calls = []

    def get(self, url, params=None, **_):
        self.calls.append(("GET", url))
        path = url.removeprefix(f"{BASE}/api/v2")
        if path == "/dags":
            dags = sorted(spec.dag_id for spec in job_specs.load_specs())
            offset, limit = int(params["offset"]), int(params["limit"])
            return Answer({"dags": [{"dag_id": dag, "file_token": dag, "is_paused": False, "tags": []}
                                    for dag in dags[offset:offset + limit]], "total_entries": len(dags)})
        if path.startswith("/dags/") and path.endswith("/tasks"):
            spec = {spec.dag_id: spec for spec in job_specs.load_specs()}[path.split("/")[2]]
            tasks = [{"task_id": job_specs.RUN_TASK_ID, "downstream_task_ids": [job_specs.PUBLISH_TASK_ID] if spec.outputs else []}]
            if spec.outputs:
                tasks.append({"task_id": job_specs.PUBLISH_TASK_ID, "downstream_task_ids": []})
            return Answer({"tasks": tasks, "total_entries": len(tasks)})
        return Answer({"detail": "Not Found"}, 404)

    def post(self, url, json=None, **_):  # noqa: A002 - requests' own keyword
        self.calls.append(("POST", url))
        path = url.removeprefix(f"{BASE}/api/v2")
        if path == "/dags/~/dagRuns/list":
            self.test.assertLessEqual(set(json), DAG_RUNS_BATCH_BODY)
            runs = [run for run in DAY["dag_runs"] if run["dag_id"] in json["dag_ids"] and run["state"] in json["states"]
                    and instant(json["end_date_gte"]) <= instant(run["end_date"]) <= instant(json["end_date_lte"])]
            runs.sort(key=lambda run: run["end_date"])
            page = runs[json["page_offset"]:json["page_offset"] + json["page_limit"]]
            return Answer({"dag_runs": [{**run, "conf": {}, "note": None} for run in page], "total_entries": len(runs)})
        if path == "/dags/~/dagRuns/~/taskInstances/list":
            self.test.assertLessEqual(set(json), TASK_INSTANCES_BATCH_BODY)
            found = [ti for ti in DAY["publish_task_instances"] if ti["dag_id"] in json["dag_ids"]
                     and ti["dag_run_id"] in json["dag_run_ids"] and ti["task_id"] in json["task_ids"]]
            return Answer({"task_instances": found, "total_entries": len(found)})
        return Answer({"detail": "Not Found"}, 404)


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class AirliftParityTest(unittest.TestCase):
    def setUp(self):
        from airflow3 import Airflow3Instance, AirflowTokenAuthBackend
        from dagster_airlift.core.airflow_instance import AirflowAuthBackend

        server = RecordedAirflow(self)
        self.server = server

        class Backend(AirflowAuthBackend):
            def get_session(self):
                return server

            def get_webserver_url(self):
                return BASE

        self.assertTrue(issubclass(AirflowTokenAuthBackend, AirflowAuthBackend))
        self.instance = Airflow3Instance(auth_backend=Backend(), name="foundation_airflow")

    def observe_the_day(self):
        """Every materialization airlift's sensor reports over the recorded day, as (dag_id, run_id, asset URI)."""
        from dagster import AssetMaterialization, DagsterInstance, build_sensor_context
        from dagster._serdes import serialize_value
        from dagster_airlift.core.sensor.sensor_builder import AirflowPollingSensorCursor

        import definitions
        import observe_assets

        defs = definitions.build(self.instance)
        repository = defs.get_repository_def()
        (sensor,) = [sensor for sensor in repository.sensor_defs if sensor.name.endswith("__airflow_dag_status_sensor")]
        dataset_of = {spec.key: spec.metadata["dataset"] for spec in observe_assets.observed_assets()}
        start = instant(DAY["day"] + "T00:00:00+00:00").timestamp()
        cursor = serialize_value(AirflowPollingSensorCursor(end_date_gte=start, end_date_lte=start + 86400, dag_query_offset=0))
        seen = set()
        with DagsterInstance.ephemeral() as instance:
            for _ in range(50):
                context = build_sensor_context(instance=instance, repository_def=repository, cursor=cursor)
                result = sensor(context)
                for event in result.asset_events:
                    if isinstance(event, AssetMaterialization) and event.asset_key in dataset_of:
                        run = event.metadata["Run Metadata (raw)"].value
                        seen.add((run["dag_id"], run["dag_run_id"], dataset_of[event.asset_key]))
                cursor = context.cursor
                from dagster._serdes import deserialize_value

                if deserialize_value(cursor, AirflowPollingSensorCursor).end_date_lte is None:
                    return seen
        self.fail("the sensor did not finish the recorded day in 50 ticks")

    def test_dagster_materializes_what_airflow_recorded(self):
        recorded, outputs = recorded_events(), contract_outputs()
        keep, drifted = comparable(recorded, outputs)
        seen = {item for item in self.observe_the_day() if item[0] not in drifted}
        expected = {(dag, run, asset) for (dag, run), uris in recorded.items() if (dag, run) in keep for asset in uris}
        self.assertEqual(seen, expected)
        self.assertTrue(any(call == ("POST", f"{BASE}/api/v2/dags/~/dagRuns/list") for call in self.server.calls))
        self.assertFalse(any("/api/v1/" in url for _, url in self.server.calls))


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class CodeLocationTest(unittest.TestCase):
    def test_every_graph_node_is_one_asset_and_the_unit_price_checks_attach(self):
        import observe_assets
        import unit_price_checks
        from dagster import AssetKey, Definitions

        jobs, graph = observe_assets.contracts()
        assets = observe_assets.observed_assets(jobs, graph)
        self.assertEqual(len({spec.key for spec in assets}), len(graph["nodes"]))
        Definitions(assets=assets, asset_checks=unit_price_checks.CHECKS).resolve_asset_graph()
        checked = {check.asset_key for checks in unit_price_checks.CHECKS for check in checks.check_specs}
        self.assertEqual(checked, {AssetKey(["iceberg", "gold", "unit_price"]),
                                   AssetKey(["perfectory", "unit-price-by-pnu-section-packs"])})
        self.assertFalse(any(spec.blocking for checks in unit_price_checks.CHECKS for spec in checks.check_specs),
                         "stage 0 checks report only")

    def test_rows_check_names_the_failing_sido(self):
        import unit_price_checks

        result = unit_price_checks.rows_result([("11", 10, 0, 0, 0), ("26", 5, 1, 0, 0), ("41", 7, 0, 2, 0)])
        self.assertFalse(result.passed)
        self.assertEqual(result.metadata["failing_sido"].value, ["26", "41"])
        self.assertTrue(unit_price_checks.rows_result([("11", 10, 0, 0, 0)]).passed)
        self.assertFalse(unit_price_checks.rows_result([]).passed, "an empty table is not a passing one")
        self.assertIn("FROM r2.gold.unit_price GROUP BY 1", unit_price_checks.per_sido_sql("gold.unit_price"))

    def test_lane_check_measures_lag_against_the_freshness_budget(self):
        import unit_price_checks

        hour = 3600
        self.assertTrue(unit_price_checks.lag_result(100 * hour, 101 * hour, 200 * hour, 48).passed)
        behind = unit_price_checks.lag_result(100 * hour, 99 * hour, 149 * hour, 48)
        self.assertFalse(behind.passed)
        self.assertEqual(behind.metadata["asset_lag_seconds"].value, 49 * hour)
        self.assertTrue(unit_price_checks.lag_result(100 * hour, None, 120 * hour, 48).passed)
        self.assertTrue(unit_price_checks.lag_result(None, None, 120 * hour, 48).passed)

    def test_airflow_3_runs_without_a_logical_date_take_their_run_after(self):
        from airflow3 import _with_logical_date

        due, ended = "2099-10-28T01:00:00+00:00", "2099-10-28T02:00:00+00:00"
        self.assertEqual(_with_logical_date({"logical_date": None, "run_after": due, "start_date": due, "end_date": ended})["logical_date"], due)
        self.assertEqual(_with_logical_date({"logical_date": "a", "run_after": "b", "start_date": "c", "end_date": "d"})["logical_date"], "a")
        # Marked success by an operator before it started: no start_date, and a task may have no end_date.
        marked = _with_logical_date({"logical_date": None, "run_after": due, "start_date": None, "end_date": ended})
        self.assertEqual((marked["start_date"], marked["end_date"]), (ended, ended))
        task = _with_logical_date({"logical_date": None, "run_after": due, "start_date": None, "end_date": None})
        self.assertEqual((task["start_date"], task["end_date"]), (due, due))


if __name__ == "__main__":
    unittest.main()
