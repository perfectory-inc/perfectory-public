"""A job run through the host's forced command is a Dagster Pipes session (orchestration/dagster/systemd_pipes.py, root ADR-0417).

The forced command is replaced by a script that prints what scripts/ops/start-scheduled-job.sh streams
(the run's journal, then its outcome line) and exits with its status; everything after it is
Dagster's own: open_pipes_session, the message handler, the step's result. Run by the CI
`dagster-observe` lane in the code location's image; skipped elsewhere, where a skip fails that lane.
"""

import importlib.util
import json
import pathlib
import sys
import tempfile
import textwrap
import unittest

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


if __name__ == "__main__":
    unittest.main()
