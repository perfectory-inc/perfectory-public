"""The deploy's lakehouse migration accepts every table as production holds it (root ADR-0259).

`lakehouse-production-shape.json` is production's catalog: each table's columns, types,
nullability, whether it holds rows and the earlier column sets its data files were written under.
The migration's own `plan_table` runs on it here, in every pull request, so a contract change the
deploy would refuse fails before the merge instead of stopping the deploy and every DAG after it.
"""

from __future__ import annotations

import copy
import importlib.util
import json
import sys
import unittest
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(TESTS_DIR.parent / "jobs"))

import lakehouse_schema_migrate as migrate  # noqa: E402



def replay_pr469() -> dict:
    """#469 (merge commit 3f4ae2cd): its gold.unit_price_panel contract columns, and the shape the
    production catalog held for that table, which it still holds (one row per PNU, with rows)."""
    columns = ("pnu", "building_id", "units_json", "row_digest", "source_snapshot_id", "published_at_utc")
    live = ("pnu", "units_json", "row_digest", "source_snapshot_id", "published_at_utc")
    return {
        "contracts": {"gold.unit_price_panel": {"columns": [
            {"name": column, "logical_type": "string", "required": True} for column in columns]}},
        "shapes": {"gold.unit_price_panel": {
            "has_rows": True,
            "fields": [[index, column, "string", True] for index, column in enumerate(live, start=1)],
            "columns": list(range(1, len(live) + 1)),
            "earlier": [],
        }},
    }


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

    def test_the_comparing_land_tables_gain_their_removal_column_without_a_backfill(self) -> None:
        # Root ADR-0275, ADR-0311: a lane's first compare needs `row_state` on the table production
        # holds; every row there is a source row, which a null says.
        for table in ("silver.land_transfer_history", "silver.land_characteristic", "silver.land_forest_ledger",
                      "silver.land_individual_price", "silver.land_right_registration"):
            with self.subTest(table=table):
                shape = self.shapes[table]
                self.assertTrue(shape["has_rows"])
                fields = {field[0]: field for field in shape["fields"]}
                live = [fields[field_id] for field_id in shape["columns"]]
                plan = migrate.plan_table(
                    self.contracts[table], tuple(name for _, name, _, _ in live), True, table,
                    actual_types={name: migrate.live_spark_type(kind) for _, name, kind, _ in live},
                    actual_nullability={name: nullable for _, name, _, nullable in live},
                )
                # The transfer history gained it with #728 (ledger of 10-18); the other four gain it now.
                live_names = {name for _, name, _, _ in live}
                self.assertEqual(plan["add"], [] if "row_state" in live_names else ["row_state"])
                self.assertEqual((plan["backfill"], plan["reorder"]), ([], False))

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


def load_capture():
    path = TESTS_DIR.parents[3] / "scripts" / "ops" / "lakehouse-production-shape.py"
    spec = importlib.util.spec_from_file_location("lakehouse_production_shape", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Capture(unittest.TestCase):
    """The capture reads a shape from the catalog's metadata and the live files' column ids."""

    METADATA = {
        "current-schema-id": 1,
        "current-snapshot-id": 20,
        # The current snapshot is named by id, not by its place in the list.
        "snapshots": [{"snapshot-id": 20, "summary": {"total-records": "3"}},
                      {"snapshot-id": 10, "summary": {"total-records": "0"}}],
        "schemas": [
            {"schema-id": 0, "fields": [{"id": 1, "name": "a", "type": "string", "required": False},
                                        {"id": 2, "name": "b", "type": "timestamptz", "required": True}]},
            {"schema-id": 1, "fields": [
                {"id": 1, "name": "a", "type": "string", "required": False},
                {"id": 2, "name": "b", "type": "timestamptz", "required": True},
                {"id": 3, "name": "c", "type": {"type": "list", "element-id": 4, "element": "string",
                                                "element-required": False}, "required": False}]},
        ],
    }

    def test_old_files_are_the_shapes_and_a_list_is_named_by_its_column(self) -> None:
        shape = load_capture().table_shape(self.METADATA, [[1, 2], [1, 2, 4]])
        self.assertEqual(shape, {
            "has_rows": True,
            "fields": [[1, "a", "string", True], [2, "b", "timestamp", False], [3, "c", "array<string>", True]],
            "columns": [1, 2, 3],
            "earlier": [[1, 2]],
        })

    def test_a_schema_no_live_file_was_written_under_is_no_shape(self) -> None:
        # A backfill rewrote every file: schema 0 is in the metadata, no file of it is live.
        self.assertEqual(load_capture().table_shape(self.METADATA, [[1, 2, 4]])["earlier"], [])

    def test_a_file_column_no_schema_names_stops_the_capture(self) -> None:
        with self.assertRaisesRegex(ValueError, r"\[9\]"):
            load_capture().table_shape(self.METADATA, [[1, 9]])

    def test_the_rendered_ledger_is_the_committed_one_read_back(self) -> None:
        path = migrate.PRODUCTION_SHAPE_PATH
        document = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(load_capture().render(document), path.read_text(encoding="utf-8"))


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
        replay = replay_pr469()
        refusals = migrate.production_shape_refusals(replay["contracts"], replay["shapes"])
        self.assertEqual(len(refusals), 1)
        # The deploy's words on 2026-10-13.
        self.assertIn("would gain required columns ['building_id'] with no registered backfill", refusals[0])

    def test_pr469_the_replay_is_the_shape_production_holds(self) -> None:
        self.assertEqual(replay_pr469()["shapes"]["gold.unit_price_panel"],
                         migrate.load_production_shape()["gold.unit_price_panel"])

    def test_pr469_on_an_empty_table_would_have_passed(self) -> None:
        # The refusal comes from the rows production held: the shape, not the contract alone.
        replay = replay_pr469()
        replay["shapes"]["gold.unit_price_panel"]["has_rows"] = False
        self.assertEqual(migrate.production_shape_refusals(replay["contracts"], replay["shapes"]), [])


if __name__ == "__main__":
    unittest.main()
