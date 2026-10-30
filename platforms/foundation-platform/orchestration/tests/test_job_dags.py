"""The DAGs foundation_jobs.py builds from the job list (root ADR-0122, ADR-0171).

The repository's checks run without Airflow, so the DAG file is loaded against stand-ins for the
few Airflow names it imports. They record what the file asks for and refuse an argument the real
classes do not take; whether Airflow itself accepts the result (the asset-or-time timetable, the
assets, the serialized DAG) is `WithAirflow`, which runs where Airflow is installed.
"""

import importlib.util
import pathlib
import sys
import types
import unittest
from unittest import mock

import jinja2

ORCHESTRATION = pathlib.Path(__file__).resolve().parents[1]
DAGS = ORCHESTRATION / "dags"
sys.path.insert(0, str(DAGS))

import job_specs  # noqa: E402

OPERATOR_ARGUMENTS = {"task_id", "pool", "pool_slots", "priority_weight", "weight_rule", "retries", "retry_delay",
                      "execution_timeout", "inlets", "outlets", "do_xcom_push"}
SSH_ARGUMENTS = {"ssh_conn_id", "command", "cmd_timeout"}


class SkipRun(Exception):
    pass


def stand_ins():
    """Modules standing in for what foundation_jobs.py imports, recording what it builds."""
    built = {"dags": []}

    class DAG:
        current = None

        def __init__(self, **kwargs):
            self.kwargs, self.tasks = kwargs, []
            self.dag_id = kwargs["dag_id"]

        def __enter__(self):
            DAG.current = self
            built["dags"].append(self)
            return self

        def __exit__(self, *exc):
            DAG.current = None

    class BaseOperator:
        allowed = OPERATOR_ARGUMENTS

        def __init__(self, **kwargs):
            unknown = set(kwargs) - self.allowed
            if unknown:
                raise TypeError(f"{type(self).__name__} takes no {sorted(unknown)}")
            self.kwargs = kwargs
            self.task_id = kwargs["task_id"]
            self.outlets = list(kwargs.get("outlets", []))
            self.inlets = list(kwargs.get("inlets", []))
            self.downstream, self.log = [], mock.Mock()
            DAG.current.tasks.append(self)

        def __rshift__(self, other):
            self.downstream.append(other.task_id)
            return other

        @property
        def output(self):
            return ("XComArg", self.task_id)

    class SSHOperator(BaseOperator):
        allowed = OPERATOR_ARGUMENTS | SSH_ARGUMENTS
        output = b""
        error = None  # what connecting or the command raises, as SSHOperator.execute would

        def run_ssh_client_command(self, ssh_client, command, context=None):
            return self.output

        def execute(self, context):
            if self.error:
                raise self.error
            return self.run_ssh_client_command(None, self.kwargs["command"], context=context)

    class TriggerDagRunOperator(BaseOperator):
        allowed = OPERATOR_ARGUMENTS | {"trigger_dag_id", "trigger_run_id", "skip_when_already_exists",
                                        "wait_for_completion", "conf"}

        class Partial:
            def __init__(self, kwargs):
                self.kwargs = kwargs

            def expand_kwargs(self, arguments):
                task = TriggerDagRunOperator(**self.kwargs)
                task.expanded = arguments
                # An XComArg makes its producer upstream of the mapped task, as Airflow does.
                producer = next(other for other in DAG.current.tasks if other.task_id == arguments[1])
                producer.downstream.append(task.task_id)
                return task

        @classmethod
        def partial(cls, **kwargs):
            return cls.Partial(kwargs)

    def record(kind):
        def make(*args, **kwargs):
            return (kind, args, tuple(sorted(kwargs.items())))
        return make

    def asset(*, uri, name):
        return ("Asset", uri, name)

    def lineage_policy(obj, **flags):
        obj.lineage_flags = flags
        return obj

    sdk = types.ModuleType("airflow.sdk")
    sdk.DAG, sdk.BaseOperator, sdk.Asset = DAG, BaseOperator, asset
    sdk.AssetAny, sdk.AssetOrTimeSchedule = record("AssetAny"), record("AssetOrTimeSchedule")
    sdk.CronTriggerTimetable = record("CronTriggerTimetable")
    exceptions = types.ModuleType("airflow.sdk.exceptions")
    exceptions.AirflowSkipException = SkipRun
    ssh = types.ModuleType("airflow.providers.ssh.operators.ssh")
    ssh.SSHOperator = SSHOperator
    policy = types.ModuleType("airflow.providers.openlineage.api.emission_policy")
    policy.extend_global_openlineage_emission_policy = lineage_policy
    lineage = types.ModuleType("openlineage.client.event_v2")
    lineage.Dataset = lambda namespace, name: ("Dataset", namespace, name)
    trigger = types.ModuleType("airflow.providers.standard.operators.trigger_dagrun")
    trigger.TriggerDagRunOperator = TriggerDagRunOperator
    pendulum = types.ModuleType("pendulum")
    pendulum.datetime = record("datetime")
    pendulum.duration = record("duration")
    modules = {"airflow.sdk": sdk, "airflow.sdk.exceptions": exceptions, "airflow.providers.ssh.operators.ssh": ssh,
               "airflow.providers.openlineage.api.emission_policy": policy, "openlineage.client.event_v2": lineage,
               "pendulum": pendulum, "airflow.providers.standard.operators.trigger_dagrun": trigger}
    for package in ["airflow", "airflow.providers", "airflow.providers.ssh", "airflow.providers.ssh.operators",
                    "airflow.providers.openlineage", "airflow.providers.openlineage.api", "openlineage",
                    "airflow.providers.standard", "airflow.providers.standard.operators",
                    "openlineage.client"]:
        modules[package] = types.ModuleType(package)
    return modules, built


def load_dag_file(modules):
    spec = importlib.util.spec_from_file_location("foundation_jobs_under_test", DAGS / "foundation_jobs.py")
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, modules):
        spec.loader.exec_module(module)
    return module


def asset(dataset):
    return ("Asset", job_specs.asset_uri(dataset), job_specs.asset_uri(dataset))


class TheDagsTheJobListBuilds(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        modules, built = stand_ins()
        cls.module = load_dag_file(modules)
        cls.dags = {dag.dag_id: dag for dag in built["dags"]}
        cls.specs = job_specs.load_specs()

    def tasks(self, spec):
        return {task.task_id: task for task in self.dags[spec.dag_id].tasks}

    def test_one_dag_per_job_runs_it_and_publishes_what_it_changed(self):
        self.assertEqual(set(self.dags), {spec.dag_id for spec in self.specs})
        for spec in self.specs:
            with self.subTest(spec.dag_id):
                tasks = self.tasks(spec)
                run = tasks["run"]
                # The job id, and ` triggered` for a run the clock did not start (root ADR-0179), or
                # ` requested <id>` for an operator's run-now (root ADR-0282), rendered by Jinja.
                template = jinja2.Template(run.kwargs["command"])
                for run_type, conf, expected in (
                        ("scheduled", {}, spec.job_id),
                        ("asset_triggered", {}, f"{spec.job_id} triggered"),
                        ("manual", None, f"{spec.job_id} triggered"),
                        ("manual", {"operator_request": "1" * 8 + "-1111-4111-8111-" + "1" * 12},
                         f"{spec.job_id} requested 11111111-1111-4111-8111-111111111111")):
                    dag_run = types.SimpleNamespace(run_type=run_type, conf=conf)
                    self.assertEqual(template.render(dag_run=dag_run), expected, (run_type, conf))
                self.assertIs(run.kwargs["do_xcom_push"], False, "the journal is not kept as an XCom")
                tasks.pop("check", None)  # its own test below
                if spec.reconciles:  # the asset reconciler, its own test below
                    self.assertEqual(set(tasks), {"run", "select", "start"})
                    self.assertEqual(run.downstream, ["select"])
                    continue
                if spec.outputs:
                    self.assertEqual(set(tasks), {"run", "publish_outputs"})
                    self.assertEqual(run.downstream, ["publish_outputs"])
                    publish = tasks["publish_outputs"]
                    self.assertEqual(publish.outlets, [asset(dataset) for dataset in spec.outputs])
                    self.assertEqual(publish.lineage_flags, {"emit_task_events": False})
                    self.assertEqual(publish.kwargs["retries"], 0)
                else:
                    self.assertEqual(set(tasks), {"run"})

    def test_the_run_still_reports_its_lineage_as_before(self):
        for spec in self.specs:
            with self.subTest(spec.dag_id):
                run = self.tasks(spec)["run"]
                self.assertEqual(run.inlets, [("Dataset", *dataset) for dataset in spec.inputs])
                self.assertEqual(run.outlets, [("Dataset", *dataset) for dataset in spec.outputs])

    def test_a_job_starts_by_its_schedule_or_by_its_inputs_with_the_schedule_as_fallback(self):
        for spec in self.specs:
            with self.subTest(spec.dag_id):
                schedule = self.dags[spec.dag_id].kwargs["schedule"]
                if spec.started_by == "schedule":
                    self.assertEqual(schedule, spec.schedule)
                    continue
                kind, _, arguments = schedule
                arguments = dict(arguments)
                self.assertEqual(kind, "AssetOrTimeSchedule")
                self.assertEqual(arguments["timetable"],
                                 ("CronTriggerTimetable", (spec.schedule,), (("timezone", "UTC"),)))
                self.assertEqual(arguments["assets"],
                                 ("AssetAny", tuple(asset(dataset) for dataset in spec.starting_inputs), ()))

    def test_what_a_job_publishes_is_what_the_jobs_it_feeds_wait_for(self):
        published = {spec.job_id: set(self.tasks(spec)["publish_outputs"].outlets)
                     for spec in self.specs if spec.outputs}
        for spec in self.specs:
            if spec.started_by != "inputs":
                continue
            waited = set(dict(self.dags[spec.dag_id].kwargs["schedule"][2])["assets"][1])
            for producer in spec.producers:
                with self.subTest(job=spec.job_id, producer=producer):
                    self.assertTrue(published[producer] & waited)

    def test_the_run_keeps_only_its_outcome(self):
        spec = next(spec for spec in self.specs if spec.job_id == "gold_panel_rebuild")
        run = self.tasks(spec)["run"]
        for output, expected in [(b"x\nfoundation-job-outcome unchanged\nresult=success\n", "unchanged"),
                                 (b"x\nfoundation-job-outcome changed\n", "changed"),
                                 (b"an older job that does not say\n", "changed")]:
            with self.subTest(expected=expected, output=output):
                ti = mock.Mock()
                run.output = output
                self.assertEqual(run.run_ssh_client_command(None, spec.job_id, context={"ti": ti}), b"")
                ti.xcom_push.assert_called_once_with(key="outcome", value=expected)

    def test_a_run_that_ended_waiting_is_skipped_not_failed_nor_succeeded(self):
        # Root ADR-0260: skipped is what the data flow map reads as waiting, with the reason from the log.
        spec = next(spec for spec in self.specs if spec.job_id == "map_edit_fold_complex")
        run = self.tasks(spec)["run"]
        ti = mock.Mock()
        run.output = b"foundation-job-outcome waiting a release build is running; this job starts after it finishes\n"
        with self.assertRaises(SkipRun):
            run.run_ssh_client_command(None, spec.job_id, context={"ti": ti})
        ti.xcom_push.assert_called_once_with(key="outcome", value="waiting")

    def test_an_unchanged_run_publishes_nothing(self):
        spec = next(spec for spec in self.specs if spec.job_id == "gold_panel_rebuild")
        publish = self.tasks(spec)["publish_outputs"]
        for outcome, skipped in [("unchanged", True), ("waiting", True), ("failed", True), ("changed", False),
                                 (None, False)]:
            with self.subTest(outcome=outcome):
                ti = mock.Mock()
                ti.xcom_pull.return_value = outcome
                if skipped:
                    with self.assertRaises(SkipRun):
                        publish.execute({"ti": ti})
                else:
                    publish.execute({"ti": ti})
                ti.xcom_pull.assert_called_once_with(task_ids="run", key="outcome")


class TheReconcilerStartsWhatIsOwed(unittest.TestCase):
    """Root ADR-0333 §7: the reconciler's run names what each reconciled job owes; `select` keeps the
    jobs with no run queued or running and `start` triggers each once per key. 2026-10-28: the 공매
    Gold's asset event 3912 reached a paused bake and nothing started it for 13 hours."""

    @classmethod
    def setUpClass(cls):
        modules, built = stand_ins()
        load_dag_file(modules)
        cls.dags = {dag.dag_id: dag for dag in built["dags"]}
        [cls.spec] = [spec for spec in job_specs.load_specs() if spec.reconciles]
        cls.tasks = {task.task_id: task for task in cls.dags[cls.spec.dag_id].tasks}

    def test_it_covers_every_by_pnu_bake_and_the_unit_price_assets(self):
        self.assertEqual(set(self.spec.reconciles),
                         {"by_pnu_public_auction_bake", "gold_unit_price_rebuild", "by_pnu_unit_price_bake",
                          "gold_court_auction_rebuild", "by_pnu_court_auction_bake", "by_pnu_serving_bake"})
        # Every job that serves a by-PNU lane is reconciled (2026-10-29: the panel bake was not).
        lanes = {spec.job_id for spec in job_specs.load_specs()
                 if any(job_specs.by_pnu_lane_of(name) for _, name in spec.outputs)}
        self.assertLessEqual(lanes, set(self.spec.reconciles))
        start = self.tasks["start"]
        self.assertEqual(start.expanded, ("XComArg", "select"))
        self.assertIs(start.kwargs["skip_when_already_exists"], True)
        self.assertIs(start.kwargs["wait_for_completion"], False)
        self.assertEqual(self.tasks["select"].downstream, ["start"])
        for task in (self.tasks["select"], start):
            self.assertEqual(task.lineage_flags, {"emit_task_events": False})

    def test_the_run_keeps_the_owed_runs_of_the_jobs_it_reconciles_only(self):
        run = self.tasks["run"]
        run.output = (b"asset iceberg://gold.unit_price: owed\n"
                      b"foundation-asset-owed gold_unit_price_rebuild 0123456789abcdef\n"
                      b"foundation-asset-owed gold_panel_rebuild 0123456789abcdef\n"
                      b"foundation-asset-owed by_pnu_public_auction_bake fedcba9876543210\n"
                      b"foundation-job-outcome unchanged\n")
        ti = mock.Mock()
        run.run_ssh_client_command(None, self.spec.job_id, context={"ti": ti})
        ti.xcom_push.assert_any_call(key="outcome", value="unchanged")
        ti.xcom_push.assert_any_call(key="owed", value=[["gold_unit_price_rebuild", "0123456789abcdef"],
                                                       ["by_pnu_public_auction_bake", "fedcba9876543210"]])

    def test_select_starts_a_job_once_per_key_unless_a_run_of_it_is_queued_or_running(self):
        select = self.tasks["select"]
        ti = mock.Mock()
        ti.xcom_pull.return_value = [["gold_unit_price_rebuild", "0123456789abcdef"],
                                     ["by_pnu_public_auction_bake", "fedcba9876543210"]]
        counts = {"foundation_gold_unit_price_rebuild": 1, "foundation_by_pnu_public_auction_bake": 0}
        runtime = types.ModuleType("airflow.sdk.execution_time.task_runner")
        runtime.RuntimeTaskInstance = mock.Mock()
        runtime.RuntimeTaskInstance.get_dr_count.side_effect = lambda dag_id, states: counts[dag_id]
        with mock.patch.dict(sys.modules, {"airflow.sdk.execution_time.task_runner": runtime}):
            starts = select.execute({"ti": ti})
        ti.xcom_pull.assert_called_once_with(task_ids="run", key="owed")
        self.assertEqual(starts, [{"trigger_dag_id": "foundation_by_pnu_public_auction_bake",
                                   "trigger_run_id": "reconcile__by_pnu_public_auction_bake__fedcba9876543210"}])
        runtime.RuntimeTaskInstance.get_dr_count.assert_any_call(
            dag_id="foundation_gold_unit_price_rebuild", states=["queued", "running"])


class TheCheckBeforeThePool(unittest.TestCase):
    """Root ADR-0277: a job with a check_service asks it, in default_pool, before its run waits for a pool."""

    @classmethod
    def setUpClass(cls):
        modules, built = stand_ins()
        load_dag_file(modules)
        cls.dags = {dag.dag_id: dag for dag in built["dags"]}
        cls.specs = job_specs.load_specs()

    def check_of(self, spec):
        return next((task for task in self.dags[spec.dag_id].tasks if task.task_id == "check"), None)

    def test_every_silver_lane_checks_first_and_nothing_else_does(self):
        checked = {spec.job_id for spec in self.specs if self.check_of(spec)}
        self.assertEqual(checked, {spec.job_id for spec in self.specs if spec.check_service})
        self.assertEqual(checked, {spec.job_id for spec in self.specs if spec.job_id.startswith("silver_refresh_")})
        for spec in self.specs:
            check = self.check_of(spec)
            if check is None:
                continue
            with self.subTest(spec.job_id):
                self.assertEqual(check.kwargs["command"], f"{spec.job_id} check")
                self.assertEqual(check.kwargs["pool"], "default_pool")
                self.assertNotIn("pool_slots", check.kwargs)
                self.assertEqual(check.kwargs["retries"], 0)
                self.assertEqual(check.downstream, ["run"])
                self.assertEqual(check.lineage_flags, {"emit_task_events": False})

    def test_only_unchanged_skips_the_run(self):
        spec = next(spec for spec in self.specs if spec.check_service)
        check = self.check_of(spec)
        check.output = b"silver-refresh-plan lane=x outcome=unchanged\nfoundation-job-check unchanged\n"
        with self.assertRaises(SkipRun):
            check.execute({})
        for output in [b"foundation-job-check would_change\n", b"", b"foundation-job-check unchanged but more\n",
                       b"note: foundation-job-check unchanged\n",
                       b"foundation-job-check unchanged\nfoundation-job-check would_change\n"]:
            with self.subTest(output=output):
                check.output = output
                self.assertIsNone(check.execute({}))

    def test_a_check_that_cannot_answer_lets_the_run_decide(self):
        spec = next(spec for spec in self.specs if spec.check_service)
        check = self.check_of(spec)
        check.error = RuntimeError("SSH command timed out")
        try:
            self.assertIsNone(check.execute({}))
        finally:
            check.error = None


def airflow_installed():
    try:
        import airflow.providers.openlineage.api.emission_policy  # noqa: F401
        import airflow.providers.ssh.operators.ssh  # noqa: F401
        import airflow.sdk  # noqa: F401
    except Exception:  # noqa: BLE001 -- any failure to import means Airflow is not usable here
        return False
    return True


@unittest.skipUnless(airflow_installed(), "Airflow is not installed here (the scheduler image has it)")
class WithAirflow(unittest.TestCase):
    """The same file against Airflow itself: what the scheduler parses and stores."""

    def test_the_dags_build_and_serialize(self):
        from airflow.sdk import DAG
        from airflow.serialization.serialized_objects import DagSerialization

        module = load_dag_file({})
        dags = {value.dag_id: value for value in vars(module).values() if isinstance(value, DAG)}
        specs = {spec.dag_id: spec for spec in job_specs.load_specs()}
        self.assertEqual(set(dags), set(specs))
        for dag_id, dag in dags.items():
            with self.subTest(dag_id):
                spec = specs[dag_id]
                restored = DagSerialization.from_dict(DagSerialization.to_dict(dag))
                if spec.started_by == "inputs":
                    self.assertEqual(type(restored.timetable).__name__, "AssetOrTimeSchedule")
                    uris = {asset.uri.rstrip("/") for asset in dag.timetable.asset_condition.objects}
                    self.assertEqual(uris, {job_specs.asset_uri(dataset) for dataset in spec.inputs})
                else:
                    self.assertEqual(type(restored.timetable).__name__, "CronTriggerTimetable")
                if spec.outputs:
                    publish = dag.get_task("publish_outputs")
                    self.assertEqual({outlet.name for outlet in publish.outlets},
                                     {job_specs.asset_uri(dataset) for dataset in spec.outputs})


if __name__ == "__main__":
    unittest.main()
