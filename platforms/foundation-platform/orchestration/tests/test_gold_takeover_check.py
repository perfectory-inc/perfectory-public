"""The shadow build's unit reports its takeover verdict as the declared asset check (scripts/ops/gold_takeover_check.py).

What the unit asks: which committed run is a shadow whose definition declares `takeover_equal` and
for which 시도, and the Pipes line the op that ran the unit logs as the check's evaluation
(orchestration/dagster/takeover_checks.py, root ADR-0564 §6-3).
"""

import importlib.util
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts" / "ops" / "gold_takeover_check.py"
SPEC = importlib.util.spec_from_file_location("gold_takeover_check", SCRIPT)
check = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(check)

TABLE = "gold.parcel_register_masters_versions"
DEFINITION = {
    "assets": [{"id": "gold-parcel-register-masters-versions", "table_name": TABLE}],
    "asset_checks": [{"asset": "gold-parcel-register-masters-versions", "check": "takeover_equal",
                      "blocking": True, "sido": "11"}],
}


def shadow_commit(**changes):
    return {"outcome": "committed", "table": TABLE, "shadow_of": {"table": TABLE[:-9], "build": 1}, **changes}


class Declared(unittest.TestCase):
    def test_a_committed_shadow_run_is_judged_in_the_sido_its_check_declares(self):
        self.assertEqual(check.declared(shadow_commit(), [DEFINITION]), (TABLE, "11"))

    def test_the_sido_defaults_to_the_largest(self):
        definition = json.loads(json.dumps(DEFINITION))
        del definition["asset_checks"][0]["sido"]
        self.assertEqual(check.declared(shadow_commit(), [definition]), (TABLE, "41"))

    def test_no_verdict_without_a_shadow_commit_or_a_declared_check(self):
        for summary in (shadow_commit(outcome="unchanged"), shadow_commit(shadow_of=None)):
            with self.subTest(summary=summary):
                self.assertIsNone(check.declared(summary, [DEFINITION]))
        self.assertIsNone(check.declared(shadow_commit(), [{"assets": DEFINITION["assets"]}]))
        self.assertIsNone(check.declared(shadow_commit(table="gold.parcel_nearby_versions"), [DEFINITION]))

    def test_the_repository_s_shadow_lanes_declare_the_check(self):
        definitions = [json.loads(path.read_text(encoding="utf-8"))
                       for path in sorted((PLATFORM / "definitions").glob("*.definition.json"))]
        for table in ("gold.parcel_register_masters_versions", "gold.parcel_demolitions_versions",
                      "gold.parcel_energy_versions"):
            with self.subTest(table=table):
                self.assertEqual(check.declared(shadow_commit(table=table), definitions), (table, "41"))


class Report(unittest.TestCase):
    def test_an_equal_verdict_passes_and_carries_its_scope(self):
        verdict = {"reference_rows": 9, "same": 9, "different": 0, "only_reference": 0, "only_candidate": 0,
                   "scope": {"sido_rows": 9, "rows_nationally": 40}}
        line = check.report(TABLE, "41", 0, verdict, {"failed": []})
        self.assertEqual(line["method"], "report_asset_check")
        params = line["params"]
        self.assertEqual((params["asset_key"], params["check_name"], params["passed"], params["severity"]),
                         ("iceberg/gold/parcel_register_masters_versions", "takeover_equal", True, "ERROR"))
        metadata = {name: value["raw_value"] for name, value in params["metadata"].items()}
        self.assertEqual((metadata["outcome"], metadata["sido"], metadata["rows_compared"], metadata["rows_nationally"]),
                         ("equal", "41", 9, 40))
        self.assertEqual(metadata["national_checks_failed"], "[]")

    def test_anything_but_equal_fails_and_says_why(self):
        for status, outcome in ((1, "not equal"), (3, "a national check failed"), (2, "the verdict did not run (exit 2)")):
            with self.subTest(status=status):
                params = check.report(TABLE, "41", status, {}, {})["params"]
                self.assertFalse(params["passed"])
                self.assertEqual(params["metadata"]["outcome"]["raw_value"], outcome)
                self.assertNotIn("rows_compared", params["metadata"], "a verdict that did not run has no counts")

    def test_the_command_line_prints_one_pipes_line(self):
        with tempfile.TemporaryDirectory() as work:
            pathlib.Path(work, "verdict.json").write_text(json.dumps({"reference_rows": 1, "same": 1}), encoding="utf-8")
            done = subprocess.run([sys.executable, "-I", "-B", str(SCRIPT), "report", TABLE, "41", "0", work],
                                  capture_output=True, text=True, check=True)
        message = json.loads(done.stdout)
        self.assertEqual(message["__dagster_pipes_version"], "0.1")
        self.assertTrue(message["params"]["passed"])


if __name__ == "__main__":
    unittest.main()
