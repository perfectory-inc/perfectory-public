"""The deploy's lakehouse migration decides what it may change by itself (root ADR-0124)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import lakehouse_schema_migrate as migrate  # noqa: E402


def contract(*columns: tuple[str, bool]) -> dict:
    return {"columns": [{"name": name, "logical_type": "string", "required": required} for name, required in columns]}


class PlanTable(unittest.TestCase):
    def test_a_table_that_matches_needs_nothing(self) -> None:
        plan = migrate.plan_table(contract(("a", True), ("b", False)), ("a", "b"), True, "gold.t")
        self.assertEqual(plan, {"add": [], "backfill": [], "reorder": False})

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


class Backfills(unittest.TestCase):
    """A backfilled value must equal a rebuilt one: each registered backfill is the build's own function."""

    def test_every_backfill_is_the_function_of_the_build_that_writes_the_table(self) -> None:
        import importlib

        for (table, column), (module, function) in migrate.BACKFILLS.items():
            with self.subTest(f"{table}.{column}"):
                build = importlib.import_module(module)
                self.assertEqual(build.GOLD_CONTRACT_NAME, table, f"{module} does not build {table}")
                self.assertIs(migrate.backfill_function(table, column), getattr(build, function))

    def test_every_backfill_names_a_required_contract_column(self) -> None:
        from platform_contracts import load_lakehouse_contract

        for table, column in migrate.BACKFILLS:
            with self.subTest(f"{table}.{column}"):
                declared = {c["name"]: c["required"] for c in load_lakehouse_contract(table)["columns"]}
                self.assertTrue(declared.get(column), f"{table}.{column} is not a required contract column")


if __name__ == "__main__":
    unittest.main()
