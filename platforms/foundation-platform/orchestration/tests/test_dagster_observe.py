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

    def test_the_code_location_builds_after_its_loader_restores_the_path(self):
        # Dagster's code server imports definitions.py with its directory on sys.path, restores sys.path,
        # and only then calls defs(): from 2026-11-06 21:16Z to 11-09 every load failed on an import
        # first made in defs() (`import publish_checks`), and the dual run recorded no comparison.
        import os
        import tempfile

        from dagster._core.code_pointer import load_python_file

        here = PLATFORM / "orchestration" / "dagster"

        def ours(module):
            return str(PLATFORM) in (getattr(module, "__file__", None) or "")

        saved_path, saved_modules, cwd = list(sys.path), dict(sys.modules), os.getcwd()
        try:
            sys.path[:] = [entry for entry in sys.path if entry and str(PLATFORM) not in entry]
            for name in [name for name, module in sys.modules.items() if ours(module)]:
                del sys.modules[name]
            with tempfile.TemporaryDirectory() as elsewhere:
                os.chdir(elsewhere)
                module = load_python_file(str(here / "definitions.py"), str(here))
                self.assertNotIn(str(here), sys.path)
                self.assertTrue(module.build(self.instance).resolve_asset_graph().asset_check_keys)
                os.chdir(cwd)
        finally:
            os.chdir(cwd)
            sys.path[:] = saved_path
            for name in [name for name, module in sys.modules.items() if ours(module)]:
                del sys.modules[name]
            sys.modules.update({name: module for name, module in saved_modules.items() if ours(module)})

    def test_the_dual_run_checks_attach_to_the_shadow_jobs_dags(self):
        import definitions
        import dual_run

        graph = definitions.build(self.instance).resolve_asset_graph()
        checked = {key.asset_key for key in graph.asset_check_keys if key.name == dual_run.CHECK_NAME}
        shadow = {dual_run.dag_key(spec) for spec, _ in dual_run.shadow_jobs(*dual_run.observe_assets.contracts())}
        self.assertTrue(shadow)
        self.assertEqual(checked, shadow)
        self.assertTrue(all(key in graph.get_all_asset_keys() for key in shadow))

    def test_a_proxied_job_is_a_dagster_job_and_airlift_no_longer_speaks_for_its_outputs(self):
        # Root ADR-0431 §5: the collector's Dagster run logs its outputs itself (run_jobs.py); its Silver
        # refresh, still Airflow's, keeps its mapping to the same tables.
        import copy

        import definitions
        import observe_assets
        from dagster_airlift.constants import TASK_MAPPING_METADATA_KEY

        jobs, graph = (copy.deepcopy(value) for value in observe_assets.contracts())
        # The list before any group switched (root ADR-0583 §1), so the collector can be proxied alone.
        for job in jobs["jobs"]:
            if job.get("dagster_stage") == "dagster":
                job["dagster_stage"] = "shadow"
        by_id = {job["id"]: job for job in jobs["jobs"]}
        # The collector here as it was on the host before it moved to AWS (root ADR-0616 §7, W101c): on.
        onbid = by_id["onbid_public_auction_collect"]
        onbid.update(enabled=True, enabled_since="2026-10-20T01:26:10Z")
        onbid.pop("disabled_reason", None)
        onbid["dagster_stage"] = "proxied"
        prefix = jobs["dag_id_prefix"]
        defs = definitions.build(self.instance, jobs, graph)
        repository = defs.get_repository_def()
        self.assertTrue(repository.has_job("foundation_job__onbid_public_auction_collect"))
        mapped = {}
        for spec in observe_assets.observed_assets(jobs, graph):
            for handle in spec.metadata.get(TASK_MAPPING_METADATA_KEY, []):
                mapped.setdefault(handle["dag_id"], set()).add(spec.key)
        self.assertNotIn(prefix + "onbid_public_auction_collect", mapped)
        (collector,) = [spec for spec in observe_assets.job_specs.load_specs(jobs, graph)
                        if spec.job_id == "onbid_public_auction_collect"]
        collected = {observe_assets.asset_key(dataset) for dataset in collector.outputs}
        self.assertFalse(repository.has_job("foundation_job__onbid_public_auction_silver"))
        self.assertTrue(collected & mapped[prefix + "onbid_public_auction_silver"])
        # Its decisions are still compared: Airflow decides a proxied job (root ADR-0458 §4).
        checked = {key.asset_key for key in defs.resolve_asset_graph().asset_check_keys}
        self.assertIn(definitions.dual_run.dag_key(collector), checked)

    def test_dagster_materializes_what_airflow_recorded(self):
        recorded, outputs = recorded_events(), contract_outputs()
        keep, drifted = comparable(recorded, outputs)
        seen = {item for item in self.observe_the_day() if item[0] not in drifted}
        # A job Dagster decides has no DAG, so airlift observes none of its recorded day (root ADR-0565, ADR-0583).
        decided = {spec.dag_id for spec in job_specs.load_specs() if spec.dagster_stage == "dagster"}
        expected = {(dag, run, asset) for (dag, run), uris in recorded.items()
                    if (dag, run) in keep and dag not in decided for asset in uris}
        self.assertTrue(expected)
        self.assertFalse({item for item in seen if item[0] in decided})
        self.assertEqual(seen, expected)
        self.assertTrue(any(call == ("POST", f"{BASE}/api/v2/dags/~/dagRuns/list") for call in self.server.calls))
        self.assertFalse(any("/api/v1/" in url for _, url in self.server.calls))


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class TheInstanceLoads(unittest.TestCase):
    def test_dagster_loads_the_instance_config_the_daemon_and_webservers_read(self):
        # 2026-11-23 from about 01:35Z the daemon and both webservers restarted in a loop: dagster.yaml had a
        # `concurrency` section and the run queue's limits still in run_coordinator's config, which Dagster
        # refuses when it loads the instance. Every test read the file as text; this loads it as they do.
        import os
        from unittest import mock

        from dagster._core.instance.config import dagster_instance_config

        directory = PLATFORM / "orchestration" / "dagster"
        environment = {"DAGSTER_PG_HOST": "127.0.0.1", "DAGSTER_PG_PORT": "5432", "DAGSTER_PG_PASSWORD": "synthetic"}
        with mock.patch.dict(os.environ, environment):
            config, _ = dagster_instance_config(str(directory), config_filename="dagster.yaml")
        runs = config["concurrency"]["runs"]
        self.assertEqual(runs["max_concurrent_runs"], 7)
        self.assertNotIn("tag_concurrency_limits", runs)
        self.assertNotIn("max_concurrent_runs", config["run_coordinator"]["config"])
        self.assertNotIn("tag_concurrency_limits", config["run_coordinator"]["config"])


def instance_concurrency():
    """The `concurrency` section of the instance config, as Dagster loads it."""
    import os
    from unittest import mock

    from dagster._core.instance.config import dagster_instance_config

    directory = PLATFORM / "orchestration" / "dagster"
    environment = {"DAGSTER_PG_HOST": "127.0.0.1", "DAGSTER_PG_PORT": "5432", "DAGSTER_PG_PASSWORD": "synthetic"}
    with mock.patch.dict(os.environ, environment):
        config, _ = dagster_instance_config(str(directory), config_filename="dagster.yaml")
    return config["concurrency"]


def dequeued(concurrency):
    """What Dagster's own dequeue (QueuedRunCoordinatorDaemon._get_runs_to_dequeue) starts from the queue of
    2026-11-23 06:40Z in small: spark's one slot taken, Gold's spark runs queued above a Silver refresh, two
    duckdb slots free (the limits the deploy sets from jobs.v1.json pools, writer_gate.py ask pools)."""
    import warnings

    from dagster import DagsterRunStatus, job, op
    from dagster._core.remote_origin import RegisteredCodeLocationOrigin, RemoteJobOrigin, RemoteRepositoryOrigin
    from dagster._core.test_utils import instance_for_test
    from dagster._daemon.run_coordinator.queued_run_coordinator_daemon import QueuedRunCoordinatorDaemon

    @op(pool="spark")
    def spark_op():
        pass

    @op(pool="duckdb")
    def duckdb_op():
        pass

    @job
    def synthetic_spark():
        spark_op()

    @job
    def synthetic_duckdb():
        duckdb_op()

    repository = RemoteRepositoryOrigin(RegisteredCodeLocationOrigin("foundation"), "__repository__")
    overrides = {"run_coordinator": {"module": "dagster._core.run_coordinator", "class": "QueuedRunCoordinator"},
                 "concurrency": concurrency}
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with instance_for_test(overrides=overrides) as instance:
            instance.event_log_storage.set_concurrency_slots("spark", 1)
            instance.event_log_storage.set_concurrency_slots("duckdb", 2)

            def run(job_def, status, priority):
                tags = {"foundation/run": "job", "foundation/decided-by": "dagster", "dagster/priority": str(priority)}
                return instance.create_run_for_job(job_def, tags=tags, status=status,
                                                   remote_job_origin=RemoteJobOrigin(repository, job_def.name))

            run(synthetic_spark, DagsterRunStatus.STARTED, 1_000_030)
            run(synthetic_duckdb, DagsterRunStatus.STARTED, 100_021)
            for _ in range(6):
                run(synthetic_spark, DagsterRunStatus.QUEUED, 1_000_030)
            run(synthetic_duckdb, DagsterRunStatus.QUEUED, 100_021)
            daemon = QueuedRunCoordinatorDaemon(interval_seconds=1)
            batch = daemon._get_runs_to_dequeue(instance, instance.get_concurrency_config(), fixed_iteration_time=None)
            return [queued.job_name for queued in batch]


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class TheQueueDequeues(unittest.TestCase):
    """Root ADR-0612: a run its pool admits starts, whatever runs a full pool holds back above it."""

    def test_a_run_its_pool_admits_starts_behind_runs_their_full_pool_holds_back(self):
        self.assertEqual(dequeued(instance_concurrency()), ["synthetic_duckdb"])

    def test_a_tag_limit_the_held_back_runs_carry_still_starves_it(self):
        # Dagster's own behaviour, pinned: it counts a run against its tag limits before it asks the run's
        # pool. When this fails, Dagster has changed that, and root ADR-0612 can be read again.
        concurrency = instance_concurrency()
        concurrency["runs"]["tag_concurrency_limits"] = [
            {"key": "foundation/decided-by", "value": "dagster", "limit": 5}]
        self.assertEqual(dequeued(concurrency), [])


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class CodeLocationTest(unittest.TestCase):
    def test_an_asset_a_job_writes_carries_that_jobs_code_version(self):
        # Root ADR-0499: Dagster's own code_version, from the version the reconciler compares a failed run with.
        import job_code_version
        import observe_assets

        jobs, graph = observe_assets.contracts()
        specs = observe_assets.job_specs.load_specs(jobs, graph)
        (spec,) = [spec for spec in specs if spec.job_id == "data_go_kr_file_data"]
        job = next(job for job in jobs["jobs"] if job["id"] == "data_go_kr_file_data")
        by_key = {asset.key: asset for asset in observe_assets.observed_assets(jobs, graph)}
        # An asset several jobs write carries the digest of all of theirs (code_versions); its own Silver tables are its alone.
        shared = {dataset for other in specs if other.job_id != spec.job_id for dataset in other.outputs}
        written = [by_key[observe_assets.asset_key(dataset)] for dataset in spec.outputs if dataset not in shared]
        self.assertTrue(written)
        self.assertEqual({asset.code_version for asset in written}, {job_code_version.version(job)})

    def test_every_graph_node_is_one_asset_in_its_definitions_group(self):
        import observe_assets

        jobs, graph = observe_assets.contracts()
        assets = observe_assets.observed_assets(jobs, graph)
        self.assertEqual(len({spec.key for spec in assets}), len(graph["nodes"]))
        groups = observe_assets.asset_groups()
        self.assertEqual({spec.metadata["pipeline_graph_node"]: spec.group_name for spec in assets}, groups)
        self.assertEqual(groups["gold-unit-price-panel"], "gold_unit_price_rebuild")

    def test_a_ledger_its_collector_schedules_from_is_feedback_and_any_other_cycle_is_refused(self):
        import observe_assets

        nodes = [{"id": "src", "type": "source_group"}, {"id": "ledger", "type": "silver_table"}]
        feeds = {"id": "src-to-ledger", "from": "src", "to": "ledger", "relation": "feeds"}
        back = {"id": "ledger-to-src", "from": "ledger", "to": "src", "relation": "schedules"}
        self.assertEqual(observe_assets.feedback_edges({"nodes": nodes, "edges": [feeds, back]}), {"ledger-to-src"})
        with self.assertRaises(observe_assets.job_specs.JobListError):
            observe_assets.feedback_edges({"nodes": nodes, "edges": [feeds, {**back, "relation": "feeds"}]})

    def test_the_declared_checks_attach_and_the_unit_price_ones_only_report(self):
        import asset_checks
        import observe_assets
        from dagster import AssetKey, Definitions

        jobs, graph = observe_assets.contracts()
        checks = asset_checks.build(graph)
        Definitions(assets=observe_assets.observed_assets(jobs, graph), asset_checks=checks).resolve_asset_graph()
        specs = {(spec.asset_key, spec.name): spec for check in checks for spec in check.check_specs}
        import contract_checks

        keys = observe_assets.node_keys(graph)
        declared = {(keys[c["asset"]], c["check"]) for group in asset_checks.definitions().values() for c in group}
        declared |= {(item.key(), item.check) for item in contract_checks.declared(graph, keys)}
        self.assertEqual(set(specs), declared, "the code location carries exactly the declared checks and the contracts' rules")
        unit_price = {(AssetKey(["iceberg", "gold", "unit_price"]), "rows_valid_per_sido"),
                      (AssetKey(["perfectory", "unit-price-by-pnu-section-packs"]), "serves_the_newest_gold")}
        self.assertLessEqual(unit_price, set(specs))
        self.assertFalse(any(specs[key].blocking for key in unit_price), "stage 0 checks report only")

    def test_a_check_on_an_unknown_node_or_of_an_unknown_kind_is_refused(self):
        import asset_checks
        import observe_assets

        _, graph = observe_assets.contracts()
        with self.assertRaisesRegex(observe_assets.job_specs.JobListError, "nodes"):
            asset_checks.build(graph, {"g": [{"asset": "no-such-node", "check": "rows_valid_per_sido", "blocking": False}]})
        with self.assertRaisesRegex(observe_assets.job_specs.JobListError, "implements"):
            asset_checks.build(graph, {"g": [{"asset": "gold-unit-price-panel", "check": "no_such", "blocking": True}]})

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
