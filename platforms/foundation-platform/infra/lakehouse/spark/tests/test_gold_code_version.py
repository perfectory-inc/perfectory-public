"""A Gold commit records the code that built it, and a code change rebuilds (root ADR-0321).

The code version covers the producer, the modules it imports at any depth and the contracts they
name, but not a comment, a docstring, or a module the contract declares as plumbing. The planner
rebuilds in full when the version recorded beside the current Gold's pins differs. Fixture modules
are synthetic.
"""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
import test_industrial_complex_gold_schema_evolution  # noqa: F401  Import-only PySpark stub.
import gold_code_version as code  # noqa: E402
import gold_rebuild as rebuild  # noqa: E402

ENTRY = {"producer": "fixture", "producer_arguments": [], "max_row_loss_fraction": 0.01,
         "incremental": {"max_changed_key_fraction": 0.3, "parity_sample_keys": 10, "reason": "fixture"}}
INPUTS = (("silver.a",), {}, "silver.a")
PINS = {"silver.a": "11"}


def snap(snapshot_id, parent, operation="append", added=1, **summary):
    return {"snapshot_id": snapshot_id, "parent_id": parent, "committed_at": "2026-01-01T00:00:00Z",
            "operation": operation, "summary": {"added-records": str(added), **summary}}


def gold(version):
    summary = {"total-records": "1000", rebuild.SOURCE_SNAPSHOTS_PROPERTY: json.dumps(PINS)}
    if version is not None:
        summary[code.PROPERTY] = version
    # A compaction after the build carries no record; the plan reads through it.
    return {"head": "91", "snapshots": [snap("90", None, "overwrite", 10, **summary),
                                        snap("91", "90", "replace", 10)], "row_count": 1000}


def silver(*ids):
    return {"silver.a": {"head": ids[-1], "snapshots": [snap(i, ids[n - 1] if n else None)
                                                        for n, i in enumerate(ids)]}}


class Tree:
    """A synthetic jobs directory and contracts directory."""

    def __init__(self):
        self.dir = tempfile.TemporaryDirectory()
        self.root = Path(self.dir.name)
        self.jobs, self.contracts = self.root / "jobs", self.root / "contracts"
        self.jobs.mkdir()
        self.contracts.mkdir()

    def write(self, path, text):
        (self.root / path).write_text(text, encoding="utf-8")
        return self

    def version(self, plumbing=None, scoped=None, table="gold.t", inputs=("silver.a",)):
        return code.value("producer", {"plumbing": plumbing or {}, "scoped": scoped or {}}, table, inputs,
                          jobs=self.jobs, contract_dirs=(self.contracts,), root=self.root)


def fixture_tree():
    return (Tree()
            .write("jobs/producer.py", '"""Doc."""\nimport helper\n\ndef build():\n    from deep import f\n    return f()\n')
            .write("jobs/helper.py", 'CONTRACT = "rules.contract.json"\n')
            .write("jobs/deep.py", "def f():\n    return 1\n")
            .write("jobs/planner.py", "X = 1\n")
            .write("jobs/unrelated.py", "Y = 1\n")
            .write("contracts/rules.contract.json", '{"a": 1, "b": 2}'))


class Version(unittest.TestCase):
    def test_it_covers_transitive_and_deferred_imports_and_named_contracts_only(self):
        tree = fixture_tree()
        self.addCleanup(tree.dir.cleanup)
        self.assertEqual(sorted(json.loads(tree.version())["files"]),
                         ["contracts/rules.contract.json", "jobs/deep.py", "jobs/helper.py", "jobs/producer.py"])

    def test_a_comment_docstring_or_formatting_change_is_not_a_code_change(self):
        tree = fixture_tree()
        self.addCleanup(tree.dir.cleanup)
        before = tree.version()
        tree.write("jobs/producer.py", '"""Other doc."""\nimport helper  # why\n\n\ndef build():\n'
                                       '    """Says what."""\n    from deep import f\n    return f()\n')
        tree.write("contracts/rules.contract.json", '{\n  "b": 2,\n  "a": 1\n}\n')
        self.assertTrue(code.same(before, tree.version()))

    def test_a_change_at_any_depth_or_in_a_contract_changes_the_version_and_names_the_file(self):
        for path, text, changed in (("jobs/deep.py", "def f():\n    return 2\n", "jobs/deep.py"),
                                    ("contracts/rules.contract.json", '{"a": 1, "b": 3}', "contracts/rules.contract.json")):
            tree = fixture_tree()
            self.addCleanup(tree.dir.cleanup)
            before = tree.version()
            tree.write(path, text)
            after = tree.version()
            self.assertFalse(code.same(before, after))
            self.assertEqual(code.changed_files(before, after), [changed])

    def test_a_plumbing_module_is_left_out_and_its_change_rebuilds_nothing(self):
        tree = fixture_tree().write("jobs/helper.py", 'import planner\nCONTRACT = "rules.contract.json"\n')
        self.addCleanup(tree.dir.cleanup)
        plumbing = {"planner": "decides; does not build rows"}
        before = tree.version(plumbing)
        self.assertNotIn("jobs/planner.py", json.loads(before)["files"])
        tree.write("jobs/planner.py", "X = 2\n")
        self.assertTrue(code.same(before, tree.version(plumbing)))
        self.assertFalse(code.same(before, tree.version()))

    def test_a_shared_registry_counts_only_at_the_entries_the_table_reads(self):
        registry = {"contracts": {"gold.t": 1, "silver.a": 2, "silver.other": 3}, "domains": 4, "prose": 5}
        lanes = {"by_pnu_lanes": {"t": {"x": 1}, "u": {"host": "parcel_by_pnu_gateway", "x": 2},
                                  "w": {"gold_table": "gold.w"}}}
        scoped = {"rules.contract.json": {"reason": "fixture", "keep": ["domains"], "entries": {"contracts": "tables"}},
                  "lanes.json": {"reason": "fixture", "entries": {"by_pnu_lanes": "lanes"}}}
        tree = fixture_tree().write("jobs/helper.py", 'A = "rules.contract.json"\nB = "lanes.json"\n')
        self.addCleanup(tree.dir.cleanup)
        tree.write("contracts/rules.contract.json", json.dumps(registry)).write("contracts/lanes.json", json.dumps(lanes))
        before = tree.version(scoped=scoped)
        for path, document in (("contracts/rules.contract.json", {**registry, "prose": 6,
                                                                   "contracts": {**registry["contracts"], "silver.other": 9}}),
                               ("contracts/lanes.json", {"by_pnu_lanes": {**lanes["by_pnu_lanes"], "w": {"gold_table": "gold.w", "y": 1}}})):
            tree.write(path, json.dumps(document))
            self.assertTrue(code.same(before, tree.version(scoped=scoped)), path)
        tree.write("contracts/rules.contract.json", json.dumps(registry))
        tree.write("contracts/lanes.json", json.dumps(lanes))
        for path, document in (("contracts/rules.contract.json", {**registry, "contracts": {**registry["contracts"], "silver.a": 7}}),
                               ("contracts/rules.contract.json", {**registry, "domains": 8}),
                               ("contracts/lanes.json", {"by_pnu_lanes": {**lanes["by_pnu_lanes"], "t": {"x": 9}}})):
            tree.write(path, json.dumps(document))
            self.assertFalse(code.same(before, tree.version(scoped=scoped)), (path, document))
            tree.write("contracts/rules.contract.json", json.dumps(registry))
            tree.write("contracts/lanes.json", json.dumps(lanes))
        # A lane hosted on the panel is the panel's: gold.parcel_panel hosts every parcel_by_pnu_gateway kind.
        panel = tree.version(scoped=scoped, table="gold.parcel_panel")
        tree.write("contracts/lanes.json", json.dumps({"by_pnu_lanes": {**lanes["by_pnu_lanes"], "u": {"host": "parcel_by_pnu_gateway"}}}))
        self.assertFalse(code.same(panel, tree.version(scoped=scoped, table="gold.parcel_panel")))

    def test_the_contract_names_only_existing_plumbing_modules_and_known_selectors_with_reasons(self):
        declared = code.settings(rebuild.load_contract())
        self.assertIn("gold_rebuild", declared["plumbing"])
        self.assertIn("r2-connections.contract.json", declared["scoped"])
        for bad in ({"plumbing_modules": {"no_such_module": "why"}}, {"plumbing_modules": {"gold_rebuild": " "}},
                    {"scoped_contracts": {"x.json": {"reason": "r", "entries": {"a": "everything"}}}},
                    {"scoped_contracts": {"x.json": {"entries": {"a": "tables"}}}}):
            with self.assertRaises(ValueError):
                code.settings({"code_version": bad})

    def test_every_producer_of_the_contract_has_a_version_covering_its_own_script(self):
        contract = rebuild.load_contract()
        config = code.settings(contract)
        for table, entry in contract["tables"].items():
            files = json.loads(code.value(entry["producer"], config, table, ["silver.x"]))["files"]
            self.assertIn(f"infra/lakehouse/spark/jobs/{entry['producer']}.py", files)
            self.assertFalse(set(config["plumbing"]) & {Path(f).stem for f in files}, entry["producer"])

    def test_a_test_runner_is_not_a_producer_and_records_no_version(self):
        self.assertIsNone(code.of_running_producer("gold.parcel_panel", PINS))
        with mock.patch.object(code.sys.modules["__main__"], "__file__",
                               str(code.JOBS / "parcel_panel_silver_to_gold.py"), create=True):
            written = code.of_running_producer("`r2`.`gold`.`parcel_panel`", PINS)
        self.assertTrue(code.same(written, code.value("parcel_panel_silver_to_gold",
                                                      code.settings(rebuild.load_contract()),
                                                      "gold.parcel_panel", PINS)))


class Plan(unittest.TestCase):
    V1 = json.dumps({"sha256": "a", "files": {"jobs/p.py": "1", "jobs/q.py": "2"}})
    V2 = json.dumps({"sha256": "b", "files": {"jobs/p.py": "1", "jobs/q.py": "3"}})

    def test_same_code_and_pins_is_nothing_to_do(self):
        decision = rebuild.plan("gold.x", ENTRY, INPUTS, gold(self.V1), silver("11"), code_version=lambda inputs: self.V1)
        self.assertEqual(decision["action"], "nothing_to_do", decision["reasons"])
        self.assertTrue(decision["code_version_recorded"])

    def test_changed_code_with_unchanged_pins_rebuilds_in_full_and_names_the_file(self):
        decision = rebuild.plan("gold.x", ENTRY, INPUTS, gold(self.V1), silver("11"), code_version=lambda inputs: self.V2)
        self.assertEqual((decision["action"], decision["mode"]), ("rebuild", "full"), decision)
        self.assertEqual(decision["code_changed_files"], ["jobs/q.py"])
        self.assertIn("jobs/q.py", decision["mode_reasons"][0])
        self.assertIsNone(decision["previous_source_snapshots"])

    def test_changed_code_turns_an_incremental_input_change_into_a_full_rebuild(self):
        self.assertEqual(rebuild.plan("gold.x", ENTRY, INPUTS, gold(self.V1), silver("11", "12"),
                                      code_version=lambda inputs: self.V1)["mode"], "incremental")
        decision = rebuild.plan("gold.x", ENTRY, INPUTS, gold(self.V1), silver("11", "12"), code_version=lambda inputs: self.V2)
        self.assertEqual(decision["mode"], "full")

    def test_a_gold_built_before_the_record_adopts_the_current_code(self):
        decision = rebuild.plan("gold.x", ENTRY, INPUTS, gold(None), silver("11"), code_version=lambda inputs: self.V2)
        self.assertEqual(decision["action"], "nothing_to_do", decision["reasons"])
        self.assertFalse(decision["code_version_recorded"])


class Commit(unittest.TestCase):
    def test_a_producer_commit_records_the_code_beside_the_pins(self):
        frame = mock.MagicMock()
        with mock.patch.object(code, "of_running_producer", return_value="V"):
            rebuild.write_gold_snapshot(frame, "t", "append", PINS, "ALL")
            self.assertEqual(rebuild.pins_properties("t", PINS)[code.PROPERTY], "V")
        options = dict(call.args for call in frame.mock_calls if call[0].endswith("option"))
        self.assertEqual(options[rebuild.SNAPSHOT_PROPERTY_PREFIX + code.PROPERTY], "V")


if __name__ == "__main__":
    unittest.main()
