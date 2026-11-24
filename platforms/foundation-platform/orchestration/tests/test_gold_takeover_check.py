"""The shadow build's unit reports its takeover verdict as the declared asset check (scripts/ops/gold_takeover_check.py).

What the unit asks: whether this run's shadow head wants a verdict and in which 시도 (a commit always,
a run that kept the head until one verdict of it ran to its end), and the Pipes line the op that ran
the unit logs as the check's evaluation (orchestration/dagster/takeover_checks.py, root ADR-0564 §6-3).
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
EQUAL = {"reference_rows": 9, "same": 9, "different": 0, "only_reference": 0, "only_candidate": 0,
         "reference_snapshot": "1", "candidate_snapshot": "7", "scope": {"sido_rows": 9, "rows_nationally": 40}}


def run(outcome="committed", **changes):
    return {"outcome": outcome, "table": TABLE, "shadow_of": {"table": TABLE[:-9], "build": 1}, **changes}


def cli(*argv):
    return subprocess.run([sys.executable, "-I", "-B", str(SCRIPT), *argv], capture_output=True, text=True, check=True)


class Declared(unittest.TestCase):
    def test_a_shadow_commit_is_judged_in_the_sido_its_check_declares(self):
        self.assertEqual(check.declared(run(), [DEFINITION], {}), (TABLE, "11"))

    def test_a_run_that_kept_the_head_judges_it_until_one_verdict_ran_to_its_end(self):
        # Lane 1's first shadow commit landed before the check existed; a verdict that could not run
        # (a timeout, an R2 error) is run again: the inputs of a shadow may never move again.
        self.assertEqual(check.declared(run("unchanged"), [DEFINITION], {}), (TABLE, "11"))
        self.assertEqual(check.declared(run("unchanged"), [DEFINITION], {"completed": False}), (TABLE, "11"))
        self.assertIsNone(check.declared(run("unchanged"), [DEFINITION], {"completed": True}))
        self.assertEqual(check.declared(run(), [DEFINITION], {"completed": True}), (TABLE, "11"),
                         "a commit is a new head")

    def test_a_kept_head_is_judged_again_when_the_build_it_shadows_moved(self):
        # The unit prices' shadow writes nothing: its head stays while Spark appends a newer batch.
        done = {"completed": True, "reference_snapshot": "1"}
        self.assertIsNone(check.declared(run("unchanged"), [DEFINITION], done))
        moved = run("unchanged", shadow_of={"table": TABLE[:-9], "build": "price:3|exclusive:4|vintage:209912"})
        self.assertEqual(check.declared(moved, [DEFINITION], done), (TABLE, "11"))
        self.assertIsNone(check.declared(moved, [DEFINITION], {"completed": True}),
                          "a record that names no reference stands for the build it judged")

    def test_no_verdict_without_a_shadow_or_a_declared_check(self):
        self.assertIsNone(check.declared(run(shadow_of=None), [DEFINITION], {}))
        self.assertIsNone(check.declared(run(), [{"assets": DEFINITION["assets"]}], {}))
        self.assertIsNone(check.declared(run(table="gold.parcel_nearby_versions"), [DEFINITION], {}))

    def test_a_check_must_name_its_sido(self):
        definition = json.loads(json.dumps(DEFINITION))
        del definition["asset_checks"][0]["sido"]
        with self.assertRaises(ValueError):
            check.declared(run(), [definition], {})

    def test_the_repository_s_shadow_lanes_declare_the_check(self):
        definitions = [json.loads(path.read_text(encoding="utf-8"))
                       for path in sorted((PLATFORM / "definitions").glob("*.definition.json"))]
        for table in ("gold.parcel_sub_parcels_versions", "gold.parcel_boundary_served_versions",
                      "gold.parcel_panel_versions", "gold.unit_price_versions",
                      "silver.unit_official_price_versions"):
            with self.subTest(table=table):
                self.assertEqual(check.declared(run(table=table), definitions, {}), (table, "41"))
        # A lane that took its table over shadows nothing and declares the check no more (root ADR-0564 §6-4).
        for table in ("gold.parcel_energy_versions", "gold.parcel_register_masters_versions",
                      "gold.parcel_demolitions_versions"):
            with self.subTest(taken_over=table):
                self.assertIsNone(check.declared(run(table=table, shadow_of=None), definitions, {}))
                self.assertIsNone(check.declared(run(table=table), definitions, {}))

    def test_a_commit_clears_the_last_head_s_record(self):
        with tempfile.TemporaryDirectory() as work:
            work = pathlib.Path(work)
            (work / "summary.json").write_text(json.dumps(run(table="gold.parcel_sub_parcels_versions")), encoding="utf-8")
            state = work / "state.json"
            state.write_text(json.dumps({"completed": True}), encoding="utf-8")
            done = cli("declared", str(work / "summary.json"), str(PLATFORM / "definitions"), str(state))
            self.assertEqual(done.stdout.split(), ["gold.parcel_sub_parcels_versions", "41"])
            self.assertFalse(state.exists())


class Report(unittest.TestCase):
    def test_an_equal_verdict_passes_and_carries_its_scope(self):
        documents = {"lane": "parcel-register-masters", "rendered": 9, "refused": 0, "samples": []}
        params = check.report(TABLE, "41", 0, EQUAL, {"failed": [], "documents": documents})["params"]
        self.assertEqual((params["asset_key"], params["check_name"], params["passed"]),
                         ("iceberg/gold/parcel_register_masters_versions", "takeover_equal", True))
        self.assertNotIn("severity", params, "the op gives the declared severity")
        metadata = {name: value["raw_value"] for name, value in params["metadata"].items()}
        self.assertEqual((metadata["outcome"], metadata["sido"], metadata["rows_compared"], metadata["rows_nationally"]),
                         ("equal", "41", 9, 40))
        self.assertEqual(metadata["result_checks_failed"], "[]")
        # Every document of the 시도 was rendered and held to its schema (gold_takeover_documents).
        self.assertEqual((metadata["documents_rendered"], metadata["documents_refused"]), (9, 0))

    def test_a_verdict_that_differs_fails_and_one_that_crashed_says_it_did_not_run(self):
        self.assertEqual(check.report(TABLE, "41", 1, {**EQUAL, "same": 8, "different": 1}, {})["params"]["metadata"]
                         ["outcome"]["raw_value"], "not equal")
        self.assertEqual(check.report(TABLE, "41", 3, {}, {"failed": ["rows"]})["params"]["metadata"]
                         ["outcome"]["raw_value"], "a result check failed")
        # An uncaught error also exits 1, and leaves no comparison: not a data difference.
        for status in (1, 2, 124):
            with self.subTest(status=status):
                params = check.report(TABLE, "41", status, {}, {})["params"]
                self.assertFalse(params["passed"])
                self.assertTrue(params["metadata"]["outcome"]["raw_value"].startswith("the verdict did not run"))
                self.assertNotIn("rows_compared", params["metadata"])

    def test_the_command_line_prints_one_pipes_line_and_records_whether_it_ran(self):
        with tempfile.TemporaryDirectory() as work:
            work = pathlib.Path(work)
            (work / "verdict.json").write_text(json.dumps(EQUAL), encoding="utf-8")
            done = cli("report", TABLE, "41", "0", str(work), str(work / "state.json"))
            state = json.loads((work / "state.json").read_text(encoding="utf-8"))
            crashed = cli("report", TABLE, "41", "1", str(work / "absent"), str(work / "crashed.json"))
            crashed_state = json.loads((work / "crashed.json").read_text(encoding="utf-8"))
        message = json.loads(done.stdout)
        self.assertEqual(message["__dagster_pipes_version"], "0.1")
        self.assertTrue(message["params"]["passed"])
        self.assertEqual((state["completed"], state["reference_snapshot"], state["candidate_snapshot"]),
                         (True, "1", "7"))
        self.assertFalse(json.loads(crashed.stdout)["params"]["passed"])
        self.assertFalse(crashed_state["completed"], "the next run judges the head again")


if __name__ == "__main__":
    unittest.main()
