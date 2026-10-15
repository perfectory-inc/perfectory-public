"""The deploy's lakehouse migration accepts every table as production holds it (root ADR-0259).

`lakehouse-production-shape.json` is production's catalog: each table's columns, types,
nullability, whether it holds rows and the earlier column sets its data files were written under.
The migration's own `plan_table` runs on it here, in every pull request, so a contract change the
deploy would refuse fails before the merge instead of stopping the deploy and every DAG after it.
"""

from __future__ import annotations

import copy
import json
import sys
import unittest
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(TESTS_DIR.parent / "jobs"))

import lakehouse_schema_migrate as migrate  # noqa: E402

REPLAYS = json.loads((TESTS_DIR / "fixtures" / "production_shape_replays.json").read_text(encoding="utf-8"))


class ProductionShape(unittest.TestCase):
    def setUp(self) -> None:
        self.contracts = migrate.load_lakehouse_artifact()["contracts"]
        self.shapes = migrate.load_production_shape()

    def test_the_deploy_migration_accepts_every_production_table(self) -> None:
        self.assertEqual(migrate.production_shape_refusals(self.contracts, self.shapes), [])

    def test_the_ledger_holds_the_contract_tables_production_holds(self) -> None:
        # Not only contract tables: a contract added for a table production already holds (an old
        # name revived, say) is gated by that table's live shape.
        self.assertIn("reference.legal_dong_code_snapshot", self.shapes)
        self.assertTrue(set(self.shapes) - set(self.contracts))

    def test_a_required_column_planted_on_a_table_with_rows_is_refused(self) -> None:
        table = "reference.legal_dong_code_snapshot"
        self.assertTrue(self.shapes[table]["has_rows"])
        contracts = copy.deepcopy(self.contracts)
        contracts[table]["columns"].append({"name": "planted", "logical_type": "string", "required": True})
        refusals = migrate.production_shape_refusals(contracts, self.shapes)
        self.assertEqual(len(refusals), 1)
        self.assertIn("['planted'] with no registered backfill", refusals[0])

    def test_a_type_the_live_table_does_not_hold_is_refused(self) -> None:
        table = "reference.legal_dong_code_snapshot"
        contracts = copy.deepcopy(self.contracts)
        next(column for column in contracts[table]["columns"] if column["name"] == "snapshot_date")[
            "logical_type"] = "string"
        self.assertRegex(" ".join(migrate.production_shape_refusals(contracts, self.shapes)), "snapshot_date type")

    def test_a_table_production_does_not_hold_is_left_to_its_load(self) -> None:
        contracts = copy.deepcopy(self.contracts)
        contracts["gold.not_yet_loaded"] = {
            "columns": [{"name": "planted", "logical_type": "string", "required": True}]}
        self.assertNotIn("gold.not_yet_loaded", self.shapes)
        self.assertEqual(migrate.production_shape_refusals(contracts, self.shapes), [])


class LedgerDrift(unittest.TestCase):
    """The deploy reports a table it found other than the ledger CI gated the release on."""

    SHAPE = {"has_rows": True, "fields": [[1, "a", "string", True], [2, "b", "string", True]],
             "columns": [1, 2], "earlier": []}

    def test_the_shape_ci_checked_is_no_drift(self) -> None:
        self.assertEqual(migrate.shape_ledger_drift("gold.t", self.SHAPE, ("a", "b")), [])
        self.assertEqual(migrate.shape_ledger_drift("gold.t", None, None), [])

    def test_columns_presence_and_absence_the_ledger_does_not_know_are_drift(self) -> None:
        self.assertEqual(len(migrate.shape_ledger_drift("gold.t", self.SHAPE, ("a", "b", "c"))), 1)
        self.assertIn("present", migrate.shape_ledger_drift("gold.t", None, ("a",))[0])
        self.assertIn("absent", migrate.shape_ledger_drift("gold.t", self.SHAPE, None)[0])


class Replays(unittest.TestCase):
    """Merged contracts that stopped a production deploy, against the shape production held."""

    def test_pr469_the_unit_price_grain_change_is_refused_as_the_deploy_refused_it(self) -> None:
        # 2026-10-13: gold.unit_price_panel gained a required building_id over live rows; every
        # deploy stopped in its migrate step with the DAGs paused until the revert (#498).
        replay = REPLAYS["pr469"]
        refusals = migrate.production_shape_refusals(replay["contracts"], replay["shapes"])
        self.assertEqual(len(refusals), 1)
        self.assertIn(replay["deploy_refusal"], refusals[0])

    def test_pr469_on_an_empty_table_would_have_passed(self) -> None:
        # The refusal comes from the rows production held: the shape, not the contract alone.
        replay = copy.deepcopy(REPLAYS["pr469"])
        replay["shapes"]["gold.unit_price_panel"]["has_rows"] = False
        self.assertEqual(migrate.production_shape_refusals(replay["contracts"], replay["shapes"]), [])


if __name__ == "__main__":
    unittest.main()
