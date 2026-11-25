"""The 국토교통부 실거래가 Silver load as Dagster assets with no host under it
(orchestration/dagster/rt_molit_lane.py on publisher_lane.silver_load, root ADR-0616 §8).

The publisher is a stand-in that answers its two commands as the real one does. Run by the CI
`dagster-observe` lane in the code location's image; the Dagster tests skip elsewhere, where a skip
fails that lane.
"""

import importlib.util
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest import mock

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "orchestration" / "dags"))
sys.path.insert(0, str(PLATFORM / "orchestration" / "dagster"))

HAS_DAGSTER = importlib.util.find_spec("dagster") is not None
NEEDS_DAGSTER = "requires dagster (the CI dagster-observe lane runs it)"
DAGSTER = PLATFORM / "orchestration" / "dagster"
SPARK_JOB = PLATFORM / "infra" / "lakehouse" / "spark" / "jobs" / "rt_molit_real_transaction.py"


def pipes_line(method, params):
    return json.dumps({"__dagster_pipes_version": "0.1", "method": method, "params": params})


class NoHostTest(unittest.TestCase):
    def test_no_lane_module_names_a_host_path_unit_or_journal(self):
        for module in ("publisher_lane.py", "rt_molit_lane.py", "court_auction_lane.py"):
            text = (DAGSTER / module).read_text(encoding="utf-8")
            code = "\n".join(line for line in text.split('"""')[2].splitlines() if not line.lstrip().startswith("#"))
            for forbidden in ("/var/lib", "/opt/", "/etc/", "/data/", "systemctl", "journal", ".service", "ssh"):
                self.assertNotIn(forbidden, code, f"{module} names {forbidden}")

    @unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
    def test_the_plan_is_the_spark_jobs(self):
        # Until the Spark job is deleted the two must plan the same table and sources.
        import rt_molit_lane as lane

        made = subprocess.run([sys.executable, "-I", str(SPARK_JOB), "tables"], capture_output=True, text=True,
                              encoding="utf-8", check=True)
        table, slugs = made.stdout.split()
        self.assertEqual([(t, s) for _name, t, s in lane.plans()], [(table, slugs)])
        contract = json.loads(lane.CONTRACT.read_text(encoding="utf-8"))
        self.assertEqual(lane.window(), (contract["silver_load"]["window_days"], contract["silver_load"]["max_objects"]))


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class SilverLaneTest(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)

    def publisher(self, objects, load_lines=(), load_status=0):
        script = pathlib.Path(self.scratch.name) / "publisher.py"
        calls = pathlib.Path(self.scratch.name) / "calls.jsonl"
        script.write_text(textwrap.dedent(f"""
            import json, os, pathlib, sys
            command = sys.argv[1]
            with open({str(calls)!r}, "a", encoding="utf-8") as handle:
                handle.write(json.dumps({{"command": command, "env": {{k: v for k, v in os.environ.items()
                                                                   if k.startswith("FOUNDATION_PLATFORM_")}}}}) + "\\n")
            if command == "plan-silver-load-objects":
                out = pathlib.Path(os.environ["FOUNDATION_PLATFORM_SILVER_LOAD_OUTPUT_DIR"])
                out.mkdir(parents=True, exist_ok=True)
                plan = {{"objects": [f"bronze/synthetic/{{i}}.csv" for i in range({objects})], "selection": {{}}}}
                (out / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
            elif command == "load-rt-molit-real-transaction-silver":
                assert pathlib.Path(os.environ["FOUNDATION_PLATFORM_RT_MOLIT_PLAN_ROOT"], "real_transaction", "plan.json").exists()
                for line in {list(load_lines)!r}:
                    print(line, flush=True)
                sys.exit({load_status})
            else:
                sys.exit(64)
        """), encoding="utf-8")
        launcher = pathlib.Path(self.scratch.name) / ("publisher.cmd" if os.name == "nt" else "publisher")
        if os.name == "nt":
            launcher.write_text(f'@"{sys.executable}" "{script}" %*\r\n', encoding="utf-8")
        else:
            launcher.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{script}" "$@"\n', encoding="utf-8")
            launcher.chmod(0o755)
        return str(launcher), calls

    def materialized(self, program):
        import rt_molit_lane as lane
        from dagster import DagsterInstance, materialize

        with mock.patch.dict(os.environ, {"FOUNDATION_PLATFORM_PUBLISHER_BIN": program}):
            result = materialize([lane.silver()], instance=DagsterInstance.ephemeral(), raise_on_error=False)
        return lane, result

    def test_a_load_materializes_both_tables_and_logs_the_checks_it_reported(self):
        import rt_molit_lane as lane

        rows = "iceberg/silver/real_transaction"
        program, calls = self.publisher(objects=2, load_lines=[
            pipes_line("report_asset_check", {"asset_key": rows, "check_name": "silver_load_rows", "passed": True,
                                              "metadata": {}}),
            lane.SUMMARY_PREFIX + json.dumps({"objects": []}),
        ])
        lane, result = self.materialized(program)
        self.assertTrue(result.success)
        keys = {key.to_user_string() for key in lane.output_keys()}
        self.assertEqual(keys, {rows, "iceberg/silver/real_transaction_rejected"})
        self.assertEqual({e.asset_key.to_user_string() for e in result.get_asset_materialization_events()}, keys)
        self.assertEqual({(e.asset_key.to_user_string(), e.check_name) for e in result.get_asset_check_evaluations()},
                         {(rows, "silver_load_rows")})
        made = [json.loads(line) for line in calls.read_text(encoding="utf-8").splitlines()]
        self.assertEqual([call["command"] for call in made], ["plan-silver-load-objects", lane.LOAD_COMMAND])
        self.assertEqual(made[0]["env"]["FOUNDATION_PLATFORM_SILVER_LOAD_SOURCE_SLUGS"], lane.plans()[0][2])

    def test_nothing_owed_starts_no_load(self):
        program, calls = self.publisher(objects=0)
        lane, result = self.materialized(program)
        self.assertTrue(result.success)
        self.assertEqual(result.get_asset_materialization_events(), [])
        self.assertNotIn(lane.LOAD_COMMAND, calls.read_text(encoding="utf-8"))

    def test_a_load_that_fails_materializes_nothing(self):
        program, _calls = self.publisher(objects=1, load_lines=["Error: the export is refused"], load_status=1)
        _lane, result = self.materialized(program)
        self.assertFalse(result.success)
        self.assertEqual(result.get_asset_materialization_events(), [])

    def test_the_lane_is_its_own_definitions_and_the_code_location_does_not_load_it(self):
        import rt_molit_lane as lane

        lane.lane_definitions().resolve_asset_graph()
        self.assertNotIn("rt_molit_lane", (DAGSTER / "definitions.py").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
