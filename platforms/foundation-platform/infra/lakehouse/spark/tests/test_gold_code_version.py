"""A Gold commit records the code that built it, and a code change is published by key (root ADR-0321).

The code version covers the symbols the producer's call graph reaches, at any depth, and the
contracts they name, but not a comment, a docstring, an annotation, a function the producer never
reaches, or a module the contract declares as plumbing. The planner rebuilds when the version
recorded beside the current Gold's pins differs, and publishes that build by its changed keys.
Fixture modules are synthetic.
"""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
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


PRODUCER = '''"""Doc."""
import helper


def main():
    from deep import f
    return f() + helper.LIMIT


def cli_only():
    return helper.other()


if __name__ == "__main__":
    main()
'''
HELPER = 'CONTRACT = "rules.contract.json"\nLIMIT = len(CONTRACT)\n\n\ndef other():\n    return 3\n'
DEEP = "def f():\n    return g()\n\n\ndef g():\n    return 1\n\n\ndef unused():\n    return 9\n"


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

    def parts(self, **kwargs):
        return set(json.loads(self.version(**kwargs))["parts"])


def fixture_tree(test):
    tree = (Tree().write("jobs/producer.py", PRODUCER).write("jobs/helper.py", HELPER).write("jobs/deep.py", DEEP)
            .write("jobs/planner.py", "X = 1\n").write("jobs/unrelated.py", "Y = 1\n")
            .write("contracts/rules.contract.json", '{"a": 1, "b": 2}'))
    test.addCleanup(tree.dir.cleanup)
    return tree


class Version(unittest.TestCase):
    def test_it_covers_the_reached_symbols_at_any_depth_and_the_contracts_they_name(self):
        parts = fixture_tree(self).parts()
        for reached in ("jobs/producer.py::<module>", "jobs/producer.py::main", "jobs/deep.py::f", "jobs/deep.py::g",
                        "jobs/helper.py::LIMIT", "jobs/helper.py::CONTRACT", "contracts/rules.contract.json"):
            self.assertIn(reached, parts)
        for unreached in ("jobs/producer.py::cli_only", "jobs/helper.py::other", "jobs/deep.py::unused"):
            self.assertNotIn(unreached, parts)
        self.assertFalse({p for p in parts if p.startswith(("jobs/unrelated.py", "jobs/planner.py"))})

    def test_a_refactor_that_cannot_change_a_row_is_not_a_code_change(self):
        tree = fixture_tree(self)
        before = tree.version()
        # Comments, docstrings, formatting, annotations, key order, a function nothing reaches, a new
        # function, and the body of another entry point of the same modules.
        tree.write("jobs/producer.py", PRODUCER.replace('"""Doc."""', '"""Other doc."""\n# why')
                   .replace("def main():", "def main() -> int:\n    \"\"\"Says what.\"\"\"")
                   .replace("return helper.other()", "return helper.other() + 1"))
        tree.write("jobs/helper.py", HELPER.replace("return 3", "return 4") + "\n\ndef added(x: int) -> int:\n    return x\n")
        tree.write("jobs/deep.py", DEEP.replace("return 9", "return 10").replace("def g():", "def g(  ):  # same"))
        tree.write("contracts/rules.contract.json", '{\n  "b": 2,\n  "a": 1\n}\n')
        self.assertTrue(code.same(before, tree.version()), code.changed_parts(before, tree.version()))
        decision = rebuild.plan("gold.x", ENTRY, INPUTS, gold(before), silver("11"),
                                code_version=lambda inputs: tree.version())
        self.assertEqual(decision["action"], "nothing_to_do", decision["reasons"])

    def test_a_change_at_any_depth_or_in_a_contract_changes_the_version_and_names_the_part(self):
        for path, text, changed in (("jobs/deep.py", DEEP.replace("return 1", "return 2"), "jobs/deep.py::g"),
                                    ("jobs/helper.py", HELPER.replace("len(CONTRACT)", "len(CONTRACT) + 1"),
                                     "jobs/helper.py::LIMIT"),
                                    ("contracts/rules.contract.json", '{"a": 1, "b": 3}', "contracts/rules.contract.json")):
            tree = fixture_tree(self)
            before = tree.version()
            tree.write(path, text)
            after = tree.version()
            self.assertFalse(code.same(before, after))
            self.assertEqual(code.changed_parts(before, after), [changed])

    def test_a_reached_class_counts_whole_and_a_module_used_as_a_value_counts_whole(self):
        tree = fixture_tree(self)
        tree.write("jobs/deep.py", DEEP + "\n\nclass Lane:\n    def build(self):\n        return 1\n\n"
                   "    def report(self):\n        return 2\n")
        tree.write("jobs/producer.py", PRODUCER.replace("return f()", "import unrelated\n    getattr(unrelated, 'Y')\n"
                                                                     "    from deep import Lane\n    return Lane().build() + f()"))
        before = tree.version()
        self.assertIn("jobs/unrelated.py::Y", json.loads(before)["parts"])
        tree.write("jobs/deep.py", DEEP + "\n\nclass Lane:\n    def build(self):\n        return 1\n\n"
                   "    def report(self):\n        return 3\n")
        self.assertEqual(code.changed_parts(before, tree.version()), ["jobs/deep.py::Lane"])

    def test_a_plumbing_module_is_left_out_and_its_change_rebuilds_nothing(self):
        tree = fixture_tree(self).write("jobs/helper.py", HELPER.replace("LIMIT = len(CONTRACT)",
                                                                         "import planner\nLIMIT = planner.X"))
        plumbing = {"planner": "decides; does not build rows"}
        self.assertFalse({p for p in tree.parts(plumbing=plumbing) if p.startswith("jobs/planner.py")})
        before = tree.version(plumbing)
        tree.write("jobs/planner.py", "X = 2\n")
        self.assertTrue(code.same(before, tree.version(plumbing)))
        self.assertIn("jobs/planner.py::X", tree.parts())

    def test_a_shared_registry_counts_only_at_the_entries_the_table_reads(self):
        registry = {"contracts": {"gold.t": 1, "silver.a": 2, "silver.other": 3}, "domains": 4, "prose": 5}
        lanes = {"by_pnu_lanes": {"t": {"x": 1}, "u": {"host": "parcel_by_pnu_gateway", "x": 2},
                                  "w": {"gold_table": "gold.w"}}}
        scoped = {"rules.contract.json": {"reason": "fixture", "keep": ["domains"], "entries": {"contracts": "tables"}},
                  "lanes.json": {"reason": "fixture", "entries": {"by_pnu_lanes": "lanes"}}}
        tree = fixture_tree(self).write("jobs/helper.py", HELPER.replace('"rules.contract.json"',
                                                                         '("rules.contract.json", "lanes.json")'))
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

    def test_every_producer_of_the_contract_has_a_version_starting_at_its_own_script(self):
        contract = rebuild.load_contract()
        config = code.settings(contract)
        for table, entry in contract["tables"].items():
            parts = json.loads(code.value(entry["producer"], config, table, ["silver.x"]))["parts"]
            self.assertIn(f"infra/lakehouse/spark/jobs/{entry['producer']}.py::<module>", parts)
            self.assertIn(f"infra/lakehouse/spark/jobs/{entry['producer']}.py::main", parts)
            self.assertFalse(set(config["plumbing"]) & {Path(p.split("::")[0]).stem for p in parts}, entry["producer"])

    def test_a_test_runner_is_not_a_producer_and_records_no_version(self):
        self.assertIsNone(code.of_running_producer("gold.parcel_panel", PINS))
        with mock.patch.object(code.sys.modules["__main__"], "__file__",
                               str(code.JOBS / "parcel_panel_silver_to_gold.py"), create=True):
            written = code.of_running_producer("`r2`.`gold`.`parcel_panel`", PINS)
        self.assertTrue(code.same(written, code.value("parcel_panel_silver_to_gold",
                                                      code.settings(rebuild.load_contract()),
                                                      "gold.parcel_panel", PINS)))


class Plan(unittest.TestCase):
    V1 = json.dumps({"sha256": "a", "parts": {"jobs/p.py::f": "1", "jobs/q.py::g": "2"}})
    V2 = json.dumps({"sha256": "b", "parts": {"jobs/p.py::f": "1", "jobs/q.py::g": "3"}})

    def test_same_code_and_pins_is_nothing_to_do(self):
        decision = rebuild.plan("gold.x", ENTRY, INPUTS, gold(self.V1), silver("11"), code_version=lambda inputs: self.V1)
        self.assertEqual(decision["action"], "nothing_to_do", decision["reasons"])
        self.assertTrue(decision["code_version_recorded"])
        self.assertIsNone(decision["publish"])

    def test_changed_code_recomputes_every_row_and_publishes_by_changed_keys(self):
        decision = rebuild.plan("gold.x", ENTRY, INPUTS, gold(self.V1), silver("11"), code_version=lambda inputs: self.V2)
        self.assertEqual((decision["action"], decision["mode"], decision["publish"]),
                         ("rebuild", "full", "changed_keys"), decision)
        self.assertEqual(decision["code_changed_parts"], ["jobs/q.py::g"])
        self.assertIn("jobs/q.py::g", decision["mode_reasons"][0])
        self.assertIsNone(decision["previous_source_snapshots"])
        flag = decision["producer_arguments"].index("--publish-changed-keys")
        self.assertEqual(decision["producer_arguments"][flag + 1], "0.3")

    def test_changed_code_with_an_input_change_is_published_by_key_too(self):
        self.assertEqual(rebuild.plan("gold.x", ENTRY, INPUTS, gold(self.V1), silver("11", "12"),
                                      code_version=lambda inputs: self.V1)["mode"], "incremental")
        decision = rebuild.plan("gold.x", ENTRY, INPUTS, gold(self.V1), silver("11", "12"), code_version=lambda inputs: self.V2)
        self.assertEqual((decision["mode"], decision["publish"]), ("full", "changed_keys"))

    def test_a_table_without_an_incremental_bound_merges_any_share_of_changed_keys(self):
        entry = {k: v for k, v in ENTRY.items() if k != "incremental"} | {"full_reason": "fixture"}
        decision = rebuild.plan("gold.x", entry, INPUTS, gold(self.V1), silver("11"), code_version=lambda inputs: self.V2)
        flag = decision["producer_arguments"].index("--publish-changed-keys")
        self.assertEqual(decision["producer_arguments"][flag + 1], "1.0")

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

    def test_a_shadow_write_goes_to_the_branch_and_records_the_tables_code(self):
        frame = mock.MagicMock()
        with mock.patch.object(code, "of_running_producer", return_value="V") as running:
            rebuild.write_gold_snapshot(frame, "`r2`.`gold`.`t`", "overwrite", PINS, "ALL", branch="audit")
        frame.writeTo.assert_called_once_with("`r2`.`gold`.`t`.`branch_audit`")
        running.assert_called_once_with("`r2`.`gold`.`t`", PINS)


class Arguments(unittest.TestCase):
    def args(self, **overrides):
        values = {"publish_changed_keys": 0.3, "incremental_from_snapshots": None, "input_mode": "iceberg",
                  "write_mode": "iceberg", "iceberg_write_mode": "overwrite", "region_prefix": None}
        return SimpleNamespace(**{**values, **overrides})

    def test_publishing_by_key_is_a_whole_iceberg_overwrite_only(self):
        import gold_incremental

        self.assertIsNone(gold_incremental.validate_arguments(self.args(), ("silver.a",)))
        for bad in ({"publish_changed_keys": 0.0}, {"publish_changed_keys": 1.5},
                    {"incremental_from_snapshots": "pins.json"}, {"iceberg_write_mode": "append"},
                    {"region_prefix": "99999"}):
            with self.assertRaises(ValueError, msg=bad):
                gold_incremental.validate_arguments(self.args(**bad), ("silver.a",))


if __name__ == "__main__":
    unittest.main()
