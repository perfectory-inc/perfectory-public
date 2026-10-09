"""The deploy's lakehouse migration decides what it may change by itself (root ADR-0124)."""

from __future__ import annotations

import sys
import ast
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import lakehouse_schema_migrate as migrate  # noqa: E402


def contract(*columns: tuple[str, bool]) -> dict:
    return {"columns": [{"name": name, "logical_type": "string", "required": required} for name, required in columns]}


class PlanTable(unittest.TestCase):
    def test_planning_imports_without_spark_even_in_a_fresh_process(self) -> None:
        result = subprocess.run(
            [sys.executable, "-c", "import lakehouse_schema_migrate"],
            cwd=JOBS_DIR, capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_a_table_that_matches_needs_nothing(self) -> None:
        plan = migrate.plan_table(contract(("a", True), ("b", False)), ("a", "b"), True, "gold.t")
        self.assertEqual(plan, {"add": [], "backfill": [], "reorder": False, "write_order": False})

    def test_an_optional_column_is_added_without_a_backfill(self) -> None:
        plan = migrate.plan_table(contract(("a", True), ("b", False)), ("a",), True, "gold.t")
        self.assertEqual(plan["add"], ["b"])
        self.assertEqual(plan["backfill"], [])

    def test_contract_columns_out_of_order_are_reordered_not_refused(self) -> None:
        plan = migrate.plan_table(contract(("a", True), ("b", True)), ("b", "a"), True, "gold.t")
        self.assertTrue(plan["reorder"])

    def test_a_column_the_contract_does_not_declare_stops_the_deploy(self) -> None:
        with self.assertRaises(migrate.MigrationBlocked):
            migrate.plan_table(contract(("a", True),), ("a", "unmerged"), True, "gold.t")

    def test_a_required_column_on_a_table_with_rows_needs_a_registered_backfill(self) -> None:
        with self.assertRaises(migrate.MigrationBlocked):
            migrate.plan_table(contract(("a", True), ("b", True)), ("a",), True, "gold.t")

    def test_a_required_column_on_an_empty_table_needs_no_backfill(self) -> None:
        plan = migrate.plan_table(contract(("a", True), ("b", True)), ("a",), False, "gold.t")
        self.assertEqual(plan["backfill"], [])

    def test_a_registered_backfill_is_planned(self) -> None:
        plan = migrate.plan_table(
            contract(("pnu", True), ("row_digest", True)), ("pnu",), True, "gold.parcel_panel"
        )
        self.assertEqual(plan["backfill"], ["row_digest"])

    def test_a_failed_backfill_is_planned_again_after_the_column_was_added(self) -> None:
        plan = migrate.plan_table(
            contract(("pnu", True), ("row_digest", True)), ("pnu", "row_digest"),
            True, "gold.parcel_panel", needs_backfill=("row_digest",),
        )
        self.assertEqual(plan["add"], [])
        self.assertEqual(plan["backfill"], ["row_digest"])

    def test_a_wrong_type_is_blocked_even_when_column_names_match(self) -> None:
        with self.assertRaisesRegex(migrate.MigrationBlocked, "type"):
            migrate.plan_table(contract(("a", True)), ("a",), True, "gold.t", actual_types={"a": "int"})


class WriteOrder(unittest.TestCase):
    """A range-distributed contract puts its sort order on the live table (root ADR-0164)."""

    ORDERED = {"write.distribution-mode": "range", "sort-order": "pnu ASC NULLS FIRST"}

    @staticmethod
    def ranged(distribution: str = "range") -> dict:
        return {**contract(("pnu", True), ("b", False)), "table_name": "gold.t",
                "sort_order": ["pnu"], "write_distribution": distribution}

    def test_a_range_table_without_the_order_is_planned_and_one_with_it_is_not(self) -> None:
        for properties, expected in (
            ({}, True),
            ({"write.distribution-mode": "hash"}, True),
            ({"write.distribution-mode": "range"}, True),
            ({"write.distribution-mode": "hash", "sort-order": "pnu ASC NULLS FIRST"}, True),
            ({"write.distribution-mode": "range", "sort-order": "pnu DESC NULLS LAST"}, True),
            ({"write.distribution-mode": "range", "sort-order": "b ASC NULLS FIRST"}, True),
            ({"write.distribution-mode": "range", "sort-order": "pnu ASC NULLS FIRST, b ASC NULLS FIRST"}, True),
            (self.ORDERED, False),
        ):
            with self.subTest(properties=properties):
                plan = migrate.plan_table(self.ranged(), ("pnu", "b"), True, "gold.t",
                                          actual_properties=properties)
                self.assertIs(plan["write_order"], expected)

    def test_a_hash_table_is_left_to_its_writer(self) -> None:
        plan = migrate.plan_table(self.ranged("hash"), ("pnu", "b"), True, "gold.t", actual_properties={})
        self.assertFalse(plan["write_order"])

    def test_an_undeclared_distribution_is_refused_rather_than_defaulted(self) -> None:
        undeclared = {k: v for k, v in self.ranged().items() if k != "write_distribution"}
        with self.assertRaisesRegex(ValueError, "write_distribution"):
            migrate.plan_table(undeclared, ("pnu", "b"), True, "gold.t", actual_properties=self.ORDERED)

    def test_both_panel_gold_tables_are_range_ordered_by_pnu(self) -> None:
        from platform_contracts import load_lakehouse_contract, sort_order, write_distribution_mode

        for table in ("gold.parcel_panel", "gold.building_panel"):
            with self.subTest(table=table):
                declared = load_lakehouse_contract(table)
                self.assertEqual(write_distribution_mode(declared), "range")
                # The by-PNU bake prunes files on the bounds of the leading sort column.
                self.assertEqual(sort_order(declared)[0], "pnu")


class TemporaryExtraColumns(unittest.TestCase):
    TABLE = "silver.building_register_units"
    EXTRA = ("building_link_source_record_id", "building_link_input_sha256", "building_link_reason")

    @classmethod
    def fixture(cls) -> dict:
        return {cls.TABLE: {"since": "2026-10-02", "reason": "pending contract", "closes_when": "merged",
                            "extra_columns": [{"name": name, "logical_type": "string", "nullable": True}
                                              for name in cls.EXTRA]}}

    def setUp(self) -> None:
        # Exercise exception behavior even after the production entry is retired.
        self.registry = patch.object(migrate, "load_known_drift", return_value=self.fixture())
        self.registry.start()
        self.addCleanup(self.registry.stop)

    def test_only_the_three_named_columns_are_retained_and_reported(self) -> None:
        for extras in (self.EXTRA, self.EXTRA[:1]):
            with self.subTest(extras=extras):
                plan = migrate.plan_table(contract(("a", True)), ("a", *extras), True, self.TABLE)
                self.assertEqual(plan["temporary_extra_columns"], list(extras))
                self.assertFalse(plan["reorder"])

    def test_another_extra_column_on_the_same_table_still_blocks(self) -> None:
        with self.assertRaises(migrate.MigrationBlocked):
            migrate.plan_table(contract(("a", True)), ("a", *self.EXTRA, "unexpected"), True, self.TABLE)

    def test_the_named_columns_on_another_table_still_block(self) -> None:
        with self.assertRaises(migrate.MigrationBlocked):
            migrate.plan_table(contract(("a", True)), ("a", *self.EXTRA), True, "silver.other")

    def test_the_exception_does_not_hide_an_unregistered_required_backfill(self) -> None:
        with self.assertRaisesRegex(migrate.MigrationBlocked, "backfill"):
            migrate.plan_table(contract(("a", True), ("required_new", True)), ("a", *self.EXTRA), True, self.TABLE)

    def test_the_exception_does_not_hide_wrong_types_in_any_column(self) -> None:
        for wrong in ("a", *self.EXTRA):
            with self.subTest(column=wrong), self.assertRaisesRegex(migrate.MigrationBlocked, "type"):
                types = dict.fromkeys(("a", *self.EXTRA), "string")
                types[wrong] = "int"
                migrate.plan_table(contract(("a", True)), tuple(types), True, self.TABLE, actual_types=types)

    def test_contract_columns_are_reordered_ahead_of_retained_extras(self) -> None:
        plan = migrate.plan_table(contract(("a", True), ("b", False)), (*self.EXTRA, "b", "a"), True, self.TABLE)
        self.assertTrue(plan["reorder"])

    def test_the_exception_must_be_removed_when_the_parent_link_contract_arrives(self) -> None:
        with self.assertRaisesRegex(migrate.MigrationBlocked, "remove.*temporary"):
            migrate.plan_table(contract(("a", True), (self.EXTRA[0], False)), ("a", *self.EXTRA), True, self.TABLE)

    def test_effective_contract_keeps_extras_optional_without_changing_the_source_contract(self) -> None:
        source = contract(("a", True), ("b", False))
        effective = migrate.migration_contract(source, ("a", *self.EXTRA), self.TABLE)
        self.assertEqual([c["name"] for c in source["columns"]], ["a", "b"])
        self.assertEqual([c["name"] for c in effective["columns"]], ["a", "b", *self.EXTRA])
        self.assertTrue(all(c["logical_type"] == "string" and not c["required"] for c in effective["columns"][2:]))

    def test_a_restored_table_requires_removing_its_exception(self) -> None:
        with self.assertRaisesRegex(migrate.MigrationBlocked, "remove"):
            migrate.plan_table(contract(("a", True)), ("a",), True, self.TABLE)

    def test_retained_columns_must_have_the_declared_nullability(self) -> None:
        for wrong in self.EXTRA:
            with self.subTest(column=wrong), self.assertRaisesRegex(migrate.MigrationBlocked, "nullable"):
                nullable = dict.fromkeys(("a", *self.EXTRA), True)
                nullable[wrong] = False
                migrate.plan_table(contract(("a", True)), tuple(nullable), True, self.TABLE,
                                   actual_nullability=nullable)


class Backfills(unittest.TestCase):
    """A backfilled value must equal a rebuilt one: each registered backfill is the build's own function."""

    def test_every_backfill_names_a_function_in_the_build_that_writes_the_table(self) -> None:
        for (table, column), (module, function) in migrate.BACKFILLS.items():
            with self.subTest(f"{table}.{column}"):
                # CI has no Spark runtime: inspect the actual build, without making discovery
                # depend on another test having installed a global PySpark stub first.
                tree = ast.parse((JOBS_DIR / f"{module}.py").read_text(encoding="utf-8"))
                names = {node.name for node in tree.body if isinstance(node, ast.FunctionDef)}
                self.assertIn(function, names)
                tables = [ast.literal_eval(node.value) for node in tree.body if isinstance(node, ast.Assign)
                          and any(isinstance(target, ast.Name) and target.id == "GOLD_CONTRACT_NAME" for target in node.targets)]
                self.assertEqual(tables, [table])

    def test_every_backfill_names_a_required_contract_column(self) -> None:
        from platform_contracts import load_lakehouse_contract

        for table, column in migrate.BACKFILLS:
            with self.subTest(f"{table}.{column}"):
                declared = {c["name"]: c["required"] for c in load_lakehouse_contract(table)["columns"]}
                self.assertTrue(declared.get(column), f"{table}.{column} is not a required contract column")


class KnownDrift(unittest.TestCase):
    def test_registry_grants_only_the_three_optional_string_columns(self) -> None:
        entries = migrate.load_known_drift()
        self.assertLessEqual(set(entries), {TemporaryExtraColumns.TABLE})
        if TemporaryExtraColumns.TABLE in entries:
            self.assertEqual(entries[TemporaryExtraColumns.TABLE]["extra_columns"],
                             TemporaryExtraColumns.fixture()[TemporaryExtraColumns.TABLE]["extra_columns"])

    def test_the_published_contract_has_not_outlived_the_temporary_exception(self) -> None:
        from platform_contracts import load_lakehouse_contract

        for table in migrate.load_known_drift():
            migrate.known_drift_for_contract(load_lakehouse_contract(table), table)

    def test_a_table_wide_or_invalid_column_exception_is_refused(self) -> None:
        import json
        import tempfile

        valid = {"name": "kept", "logical_type": "string", "nullable": True}
        for columns in (None, [], [valid, valid], [{**valid, "nullable": False}],
                        [{**valid, "name": "*"}], [{**valid, "logical_type": "invalid"}]):
            with self.subTest(columns=columns), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp, "drift.json")
                path.write_text(json.dumps({"schema_version": migrate.KNOWN_DRIFT_SCHEMA, "tables": {
                    "gold.t": {"since": "2026-10-02", "reason": "pending contract", "closes_when": "merged",
                               "extra_columns": columns},
                }}), encoding="utf-8")
                with self.assertRaises(ValueError):
                    migrate.load_known_drift(path)

    def test_every_entry_states_why_and_when_it_closes(self) -> None:
        for table, entry in migrate.load_known_drift().items():
            with self.subTest(table):
                self.assertTrue(entry["reason"] and entry["since"] and entry["closes_when"])

    def test_an_entry_without_a_closing_condition_is_refused(self) -> None:
        import json
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "drift.json")
            path.write_text(json.dumps({
                "schema_version": migrate.KNOWN_DRIFT_SCHEMA,
                "tables": {"gold.t": {"since": "2026-10-02", "reason": "x", "closes_when": ""}},
            }))
            with self.assertRaises(ValueError):
                migrate.load_known_drift(path)

    def test_every_entry_names_a_contract_table(self) -> None:
        from platform_contracts import load_lakehouse_artifact

        contracts = load_lakehouse_artifact()["contracts"]
        for table in migrate.load_known_drift():
            self.assertIn(table, contracts)


if __name__ == "__main__":
    unittest.main()
