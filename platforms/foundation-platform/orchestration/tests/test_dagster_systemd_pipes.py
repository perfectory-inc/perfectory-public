"""A job run through the host's forced command is a Dagster Pipes session (orchestration/dagster/systemd_pipes.py, root ADR-0417).

The forced command is replaced by a script that prints what scripts/ops/start-scheduled-job.sh streams
(the run's journal, then its outcome line) and exits with its status; everything after it is
Dagster's own: open_pipes_session, the message handler, the step's result. Run by the CI
`dagster-observe` lane in the code location's image; skipped elsewhere, where a skip fails that lane.
"""

import importlib.util
import io
import json
import os
import pathlib
import socket
import sys
import tempfile
import textwrap
import threading
import unittest
from unittest import mock

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "orchestration" / "dags"))
sys.path.insert(0, str(PLATFORM / "orchestration" / "dagster"))

HAS_DAGSTER = importlib.util.find_spec("dagster") is not None
NEEDS_DAGSTER = "requires dagster (the CI dagster-observe lane runs it)"


def pipes_line(method, params):
    return json.dumps({"__dagster_pipes_version": "0.1", "method": method, "params": params})


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class SystemdPipesTest(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)

    def forced_command(self, lines, status=0):
        """The argv of a stand-in for the forced command: it records what it was asked, prints `lines`."""
        script = pathlib.Path(self.scratch.name) / "forced.py"
        asked = pathlib.Path(self.scratch.name) / "asked"
        script.write_text(textwrap.dedent(f"""
            import pathlib, sys
            pathlib.Path({str(asked)!r}).write_text(sys.argv[1], encoding="utf-8")
            for line in {lines!r}:
                print(line, flush=True)
            sys.exit({status})
        """), encoding="utf-8")
        return [sys.executable, str(script)], asked

    def run_job(self, lines, status=0):
        import observe_assets
        import systemd_pipes
        from dagster import AssetSpec, DagsterInstance, materialize, multi_asset

        job_id = observe_assets.contracts()[0]["jobs"][0]["id"]
        command, asked = self.forced_command(lines, status)
        client = systemd_pipes.SystemdUnitPipesClient(command)

        @multi_asset(specs=[AssetSpec("written_a", skippable=True), AssetSpec("written_b", skippable=True)],
                     can_subset=True)
        def job_assets(context):
            yield from client.run(context=context, job_id=job_id).get_results()

        instance = DagsterInstance.ephemeral()
        result = materialize([job_assets], instance=instance, raise_on_error=False)
        materialized = {key.to_user_string(): event for key in (job_assets.keys)
                        if (event := instance.get_latest_materialization_event(key)) is not None}
        return result, materialized, asked.read_text(encoding="utf-8"), job_id

    def test_changed_materializes_every_asset_and_keeps_what_the_run_reported(self):
        result, materialized, asked, job_id = self.run_job([
            "loading part 1 of 2",
            pipes_line("report_asset_materialization", {"asset_key": "written_a", "metadata": {"rows": {"raw_value": 7, "type": "int"}}, "data_version": None}),
            "foundation-job-outcome changed",
        ])
        self.assertTrue(result.success)
        self.assertEqual(asked, f"{job_id} triggered", "the forced command reads the job id and its kind")
        self.assertEqual(set(materialized), {"written_a", "written_b"})
        self.assertEqual(materialized["written_a"].asset_materialization.metadata["rows"].value, 7)

    def test_a_withheld_outcome_materializes_nothing_and_succeeds(self):
        for outcome in ("unchanged", "waiting a release build is running", "skipped paused"):
            with self.subTest(outcome=outcome):
                result, materialized, _, _ = self.run_job(["work", f"foundation-job-outcome {outcome}"])
                self.assertTrue(result.success)
                self.assertEqual(materialized, {})

    def test_a_failed_run_fails_the_step_with_its_reason(self):
        result, materialized, _, _ = self.run_job(["foundation-job-outcome failed systemd result=exit-code"], status=1)
        self.assertFalse(result.success)
        self.assertEqual(materialized, {})
        (failure,) = result.get_step_failure_events()
        self.assertIn("systemd result=exit-code", failure.step_failure_data.error.message)

    def test_a_non_zero_exit_fails_even_after_changed(self):
        result, materialized, _, _ = self.run_job(["foundation-job-outcome changed"], status=1)
        self.assertFalse(result.success)
        self.assertEqual(materialized, {})

    def test_the_stream_cannot_frame_the_session(self):
        result, _, _, _ = self.run_job([pipes_line("closed", {}), "foundation-job-outcome changed"])
        self.assertFalse(result.success)

    def test_an_unknown_job_is_refused_before_anything_runs(self):
        import systemd_pipes
        from dagster import Failure, build_asset_context

        with self.assertRaisesRegex(Failure, "not a job"):
            systemd_pipes.SystemdUnitPipesClient(["false"]).run(context=build_asset_context(), job_id="no_such_job")

    def test_outcome_rules_match_the_airflow_runner(self):
        import systemd_pipes

        self.assertEqual(systemd_pipes.outcome_of("foundation-job-outcome changed", 0), ("changed", ""))
        self.assertEqual(systemd_pipes.outcome_of("foundation-job-outcome waiting x y", 0), ("waiting", "x y"))
        self.assertEqual(systemd_pipes.outcome_of(None, 3), ("failed", "the forced command exited 3"))
        self.assertIsNone(systemd_pipes.pipes_message("{not json"))
        self.assertIsNone(systemd_pipes.pipes_message('{"rows": 1}'))


class Stand:
    """The forced command as the host streams it, for run_jobs: records what it was asked, prints `lines`."""

    def __init__(self, lines, status=0):
        self.lines, self.status, self.asked = lines, status, []

    def start(self, request):
        self.asked.append(request)
        return iter(line + "\n" for line in self.lines), lambda: self.status


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class AJobDagsterRunsTest(unittest.TestCase):
    """The Dagster job of a job past `shadow` (run_jobs.py, root ADR-0431 §5): one op that runs the unit,
    logs its outputs' materializations when it changed them, and answers Airflow through its tags."""

    def execute(self, spec, lines, status=0, tags=None):
        import run_jobs
        from dagster import DagsterInstance

        stand = Stand(lines, status)
        instance = DagsterInstance.ephemeral()
        self.addCleanup(instance.dispose)
        result = run_jobs.run_job(spec, transport=stand).execute_in_process(instance=instance, tags=tags or {},
                                                                           raise_on_error=False)
        run = instance.get_run_by_id(result.run_id)
        materialized = {event.asset_key: event for event in result.asset_materializations_for_node("run")}
        return result, run.tags, materialized, stand.asked

    def spec(self, reconciles=False):
        import observe_assets

        specs = observe_assets.job_specs.load_specs()
        if reconciles:
            return next(spec for spec in specs if spec.reconciles)
        # A collector: its outputs are its Silver refresh's too, which no asset definition could share.
        return next(spec for spec in specs if spec.job_id == "onbid_public_auction_collect")

    def keys(self, spec):
        import observe_assets

        return {observe_assets.asset_key(dataset) for dataset in spec.outputs}

    def test_a_changed_run_materializes_every_output_and_says_changed(self):
        import observe_assets

        spec = self.spec()
        first = observe_assets.asset_key(spec.outputs[0])
        result, tags, materialized, asked = self.execute(spec, [
            "loading",
            pipes_line("report_asset_materialization",
                       {"asset_key": first.to_user_string(), "metadata": {"rows": {"raw_value": 7, "type": "int"}}}),
            "foundation-job-outcome changed"], tags={"foundation/run-kind": ""})
        self.assertTrue(result.success)
        self.assertEqual(asked, [spec.job_id], "a clock start sends the job id alone")
        self.assertEqual(set(materialized), self.keys(spec))
        self.assertEqual(materialized[first].metadata["rows"].value, 7)
        self.assertEqual(tags["foundation/job-outcome"], "changed")
        self.assertEqual((tags["foundation/run"], tags["foundation/pool"]), ("job", spec.pool))

    def test_a_withheld_run_materializes_nothing_and_says_so(self):
        for outcome in ("unchanged", "waiting a release build is running", "skipped paused"):
            with self.subTest(outcome):
                result, tags, materialized, asked = self.execute(self.spec(), [f"foundation-job-outcome {outcome}"])
                self.assertTrue(result.success)
                self.assertEqual(materialized, {})
                self.assertEqual(tags["foundation/job-outcome"], outcome.split()[0])
                self.assertEqual(asked, [f"{self.spec().job_id} triggered"], "a run without a kind is an operator's")

    def test_a_failed_run_fails_and_says_failed(self):
        result, tags, materialized, _ = self.execute(self.spec(), ["foundation-job-outcome failed systemd result=timeout"], 1)
        self.assertFalse(result.success)
        self.assertEqual((tags["foundation/job-outcome"], materialized), ("failed", {}))

    def test_the_kind_airflow_sent_reaches_the_forced_command_and_nothing_else_does(self):
        kind = "requested 11111111-1111-4111-8111-111111111111"
        _, _, _, asked = self.execute(self.spec(), ["foundation-job-outcome unchanged"], tags={"foundation/run-kind": kind})
        self.assertEqual(asked, [f"{self.spec().job_id} {kind}"])
        result, _, _, asked = self.execute(self.spec(), ["foundation-job-outcome changed"],
                                           tags={"foundation/run-kind": "triggered; systemctl stop x"})
        self.assertFalse(result.success)
        self.assertEqual(asked, [], "a kind the forced command does not read is never sent")

    def test_a_run_never_speaks_for_a_dataset_it_does_not_write(self):
        result, _, materialized, _ = self.execute(self.spec(), [
            pipes_line("report_asset_materialization", {"asset_key": "iceberg/gold/someone_elses", "metadata": {}}),
            "foundation-job-outcome changed"])
        self.assertFalse(result.success)
        self.assertEqual(materialized, {})

    def test_the_reconcilers_owed_runs_go_back_to_airflow(self):
        spec = self.spec(reconciles=True)
        reconciled = spec.reconciles[0]
        result, tags, _, _ = self.execute(spec, [f"foundation-asset-owed {reconciled} 0123456789abcdef",
                                                 "foundation-asset-owed not_reconciled 0123456789abcdef",
                                                 "foundation-job-outcome unchanged"])
        self.assertTrue(result.success)
        self.assertEqual(json.loads(tags["foundation/asset-owed"]), [[reconciled, "0123456789abcdef"]])


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class TheForcedCommandOverSshTest(unittest.TestCase):
    """run_jobs.ForcedCommand against an SSH server standing in for the host: the request is the one
    command, the host key is pinned from the connection, and the journal and exit status come back."""

    def setUp(self):
        import paramiko
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

        def key():
            text = Ed25519PrivateKey.generate().private_bytes(
                serialization.Encoding.PEM, serialization.PrivateFormat.OpenSSH, serialization.NoEncryption()).decode()
            return text, paramiko.Ed25519Key.from_private_key(io.StringIO(text))

        self.client_text, self.client_key = key()
        _, self.host_key = key()
        _, self.other_host_key = key()
        self.asked = []
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(1)
        self.addCleanup(self.listener.close)
        threading.Thread(target=self.serve, daemon=True).start()

    def serve(self):
        import paramiko

        test = self
        executed = threading.Event()

        class Host(paramiko.ServerInterface):
            def check_auth_publickey(self, username, key):
                ok = username == "foundation-scheduler" and key.asbytes() == test.client_key.asbytes()
                return paramiko.AUTH_SUCCESSFUL if ok else paramiko.AUTH_FAILED

            def get_allowed_auths(self, username):
                return "publickey"

            def check_channel_request(self, kind, chanid):
                return paramiko.OPEN_SUCCEEDED

            def check_channel_exec_request(self, channel, command):
                test.asked.append(command.decode())
                executed.set()
                return True

        try:
            connection, _ = self.listener.accept()
        except OSError:
            return
        transport = paramiko.Transport(connection)
        self.addCleanup(transport.close)
        transport.add_server_key(self.host_key)
        try:
            transport.start_server(server=Host())
        except paramiko.SSHException:
            return
        channel = transport.accept(20)
        # The run streams once its exec request is answered, as the forced command's journal does.
        if channel is not None and executed.wait(20):
            channel.sendall(b"journal line\nfoundation-job-outcome changed\n")
            channel.send_exit_status(0)
            channel.close()

    def connection(self, host_key):
        return {"conn_type": "ssh", "host": "127.0.0.1", "port": self.listener.getsockname()[1],
                "login": "foundation-scheduler",
                "extra": {"private_key": self.client_text, "host_key": f"{host_key.get_name()} {host_key.get_base64()}",
                          "allow_host_key_change": False}}

    def test_the_request_is_the_one_command_and_the_run_comes_back(self):
        import run_jobs

        lines, wait = run_jobs.ForcedCommand(self.connection(self.host_key), idle_seconds=20).start("asset_reconcile triggered")
        self.assertEqual(list(lines), ["journal line\n", "foundation-job-outcome changed\n"])
        self.assertEqual(wait(), 0)
        self.assertEqual(self.asked, ["asset_reconcile triggered"])

    def test_a_host_that_is_not_the_pinned_one_is_refused(self):
        import paramiko
        import run_jobs

        with self.assertRaises(paramiko.SSHException):
            run_jobs.ForcedCommand(self.connection(self.other_host_key), idle_seconds=20).start("asset_reconcile")
        self.assertEqual(self.asked, [])

    def test_the_connection_comes_from_the_runtime_environment(self):
        import run_jobs
        from dagster import Failure

        with mock.patch.dict(os.environ, {"FOUNDATION_HOST_CONNECTION": json.dumps(self.connection(self.host_key))}):
            self.assertEqual(run_jobs.ForcedCommand.from_environment(60).connection["login"], "foundation-scheduler")
        with mock.patch.dict(os.environ, {}, clear=True), self.assertRaisesRegex(Failure, "does not hold"):
            run_jobs.ForcedCommand.from_environment(60)


if __name__ == "__main__":
    unittest.main()
