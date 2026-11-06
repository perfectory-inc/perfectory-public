"""The panel Gold producers read every Silver input through the contract's current rows (root ADR-0267,
ADR-0275), on a Spark runtime.

While a lane overwrites, its table holds one row per key, so reading it under a declared
newest-per-key rule gives the very rows the overwrite left: the move is safe on its own. Once a lane
appends versions, the same read keeps the newest one per key. A reader that takes appends or chosen
batches of a versioned table is refused: the newest row of a key needs all its versions.

Skipped where pyspark is missing; the CI spark-runtime lane runs it. PNUs are synthetic (`99999…`).
"""
import copy
import importlib.util
import sys
import tempfile
import unittest
from argparse import Namespace
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))


def has_pyspark():
    try:
        return importlib.util.find_spec("pyspark") is not None
    except ValueError:  # another test module left a stand-in `pyspark` in sys.modules
        return False


def pnu(lot: int) -> str:
    return f"99999000001{lot:04d}0000"


def versioned(contract: dict, key: list[str]) -> dict:
    """`contract` with a newest-per-key rule, as a versioned lane will declare it."""
    rule = copy.deepcopy(contract)
    rule["current_row_latest"] = {"key": key, "newest_first": ["ingested_at_utc"]}
    return rule


@unittest.skipUnless(has_pyspark(), "requires pyspark")
class PanelReadersCurrentRowsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (SparkSession.builder.master("local[2]").appName("panel-current-rows")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.directory = tempfile.TemporaryDirectory()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def write(self, module, name, rows):
        """`rows` as the parquet input of `name`, typed by the contract, every column present."""
        from platform_contracts import column_names, create_table_columns_sql, load_lakehouse_contract

        contract = load_lakehouse_contract(name)
        schema = " ".join(create_table_columns_sql(contract, indent=0).split())
        frame = self.spark.createDataFrame([tuple(row.get(c) for c in column_names(contract)) for row in rows], schema)
        root = Path(self.directory.name) / module
        frame.write.mode("overwrite").parquet(str(root / name.split(".", 1)[1]))
        return contract, root

    def read(self, module, name, contract, root, **extra):
        job = __import__(module)
        args = Namespace(input_mode="parquet", input_root=str(root), price_vintage=None,
                         iceberg_catalog_name="lakehouse", source_iceberg_namespace="silver")
        with mock.patch.object(job, "load_lakehouse_contract", side_effect=lambda _: contract):
            return job.read_source(self.spark, args, name, {}, **extra)

    def rows(self, frame, columns):
        return sorted(tuple(row[c] for c in columns) for row in frame.select(*columns).collect())

    def test_the_parcel_panel_reads_the_overwritten_table_unchanged_and_the_newest_version_of_a_versioned_one(self):
        name = "silver.land_transfer_history"
        old = datetime(2026, 7, 1, tzinfo=timezone.utc)
        new = datetime(2026, 10, 1, tzinfo=timezone.utc)
        once = [{"pnu": pnu(i), "transfer_history_seq": 1, "reason": "split", "ingested_at_utc": old,
                 "source_snapshot_id": "release-0630"} for i in range(1, 6)]
        contract, root = self.write("parcel_panel_silver_to_gold", name, once)
        columns = ["pnu", "transfer_history_seq", "reason"]
        plain = self.read("parcel_panel_silver_to_gold", name, contract, root)
        keyed = self.read("parcel_panel_silver_to_gold", name, versioned(contract, ["pnu", "transfer_history_seq"]), root)
        self.assertEqual(self.rows(keyed, columns), self.rows(plain, columns), "one row per key: the same rows")

        # A later release appended a new version of lot 2: the read keeps only that one.
        appended = once + [{**once[1], "reason": "merge", "ingested_at_utc": new, "source_snapshot_id": "release-0930"}]
        contract, root = self.write("parcel_panel_silver_to_gold", name, appended)
        keyed = self.read("parcel_panel_silver_to_gold", name, versioned(contract, ["pnu", "transfer_history_seq"]), root)
        reasons = dict((row.pnu, row.reason) for row in keyed.select("pnu", "reason").collect())
        self.assertEqual(len(reasons), 5)
        self.assertEqual(reasons[pnu(2)], "merge")

    def test_the_building_panel_reads_through_the_same_rule_and_refuses_appends_of_a_versioned_table(self):
        name = "silver.building_register_units"
        at = datetime(2026, 9, 1, tzinfo=timezone.utc)
        units = [{"mgm_bldrgst_pk": f"unit-{i}", "pnu": pnu(i), "ingested_at_utc": at,
                  "source_snapshot_id": "release-202609"} for i in range(1, 4)]
        contract, root = self.write("building_panel_silver_to_gold", name, units)
        rule = versioned(contract, ["mgm_bldrgst_pk"])
        columns = ["mgm_bldrgst_pk", "pnu"]
        self.assertEqual(self.rows(self.read("building_panel_silver_to_gold", name, rule, root), columns),
                         self.rows(self.read("building_panel_silver_to_gold", name, contract, root), columns))
        with self.assertRaisesRegex(ValueError, "keeps versions"):
            self.read("building_panel_silver_to_gold", name, rule, root, batches=("release-202609",))

    def test_the_transfer_history_is_read_as_its_contract_states_and_named_by_its_newest_release(self):
        # Root ADR-0275: releases append their changed keys; a key's release is a set of rows (ADR-0090).
        import parcel_panel_silver_to_gold as job

        name = "silver.land_transfer_history"
        june, september = "vworldkr-land-transfer-history:20990601", "vworldkr-land-transfer-history:20990901"
        at = datetime(2099, 9, 1, tzinfo=timezone.utc)
        rows = [
            {"pnu": pnu(1), "transfer_history_seq": 1, "reason": "split", "source_snapshot_id": june},
            {"pnu": pnu(2), "transfer_history_seq": 1, "reason": "split", "source_snapshot_id": june},
            # Lot 2 changed in September and repeats its row there, as the source does.
            {"pnu": pnu(2), "transfer_history_seq": 1, "reason": "merge", "source_snapshot_id": september},
            {"pnu": pnu(2), "transfer_history_seq": 1, "reason": "merge", "source_snapshot_id": september},
            {"pnu": pnu(3), "transfer_history_seq": 1, "reason": "gone", "source_snapshot_id": june},
            {"pnu": pnu(3), "transfer_history_seq": 1, "reason": "gone", "source_snapshot_id": september,
             "row_state": "removed"},
        ]
        contract, root = self.write("parcel_panel_silver_to_gold", name,
                                    [{**row, "ingested_at_utc": at, "source_record_id": "bronze/x.zip"} for row in rows])
        current = self.read("parcel_panel_silver_to_gold", name, contract, root)
        self.assertEqual(self.rows(current, ["pnu", "reason"]),
                         [(pnu(1), "split"), (pnu(2), "merge"), (pnu(2), "merge")])
        self.assertEqual(job.assert_single_snapshot(current, name), september)


if __name__ == "__main__":
    unittest.main()
