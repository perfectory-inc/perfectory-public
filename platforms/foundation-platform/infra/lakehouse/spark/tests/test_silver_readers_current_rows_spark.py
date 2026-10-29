"""The lineage, the matching gate and the unit proposal export read a versioned Silver table's current
rows (root ADR-0267, ADR-0275), on a Spark runtime.

While a lane overwrites, its table holds one row per key and its contract declares no versions: each
reader reads it exactly as before. Under a declared newest-per-key rule a table that still holds one
row per key reads the same, and once a later release appends a version of a key, only that newest
version is read. An unversioned table's predicate is not applied by these readers.

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


OLD = datetime(2026, 7, 1, tzinfo=timezone.utc)
NEW = datetime(2026, 10, 1, tzinfo=timezone.utc)


def versioned(contract: dict, key: list[str]) -> dict:
    rule = copy.deepcopy(contract)
    rule["current_row_latest"] = {"key": key, "newest_first": ["ingested_at_utc"]}
    return rule


@unittest.skipUnless(has_pyspark(), "requires pyspark")
class SilverReadersCurrentRowsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[2]").appName("silver-readers-current-rows")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.warehouse.dir", str(Path(cls.directory.name) / "warehouse"))
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sql("CREATE DATABASE IF NOT EXISTS silver")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def frame(self, name, rows):
        """`rows` typed by `name`'s contract, every contract column present."""
        from platform_contracts import column_names, create_table_columns_sql, load_lakehouse_contract

        contract = load_lakehouse_contract(name)
        schema = " ".join(create_table_columns_sql(contract, indent=0).split())
        return contract, self.spark.createDataFrame(
            [tuple(row.get(c) for c in column_names(contract)) for row in rows], schema)

    def table(self, name, rows):
        contract, frame = self.frame(name, rows)
        frame.write.mode("overwrite").saveAsTable(name)
        return contract

    def test_the_matching_gate_holds_the_current_attribute_rows(self):
        import parcel_matching_gate as job

        name = "silver.land_individual_price"
        once = [{"pnu": pnu(i), "base_year": "2026", "ingested_at_utc": OLD} for i in range(1, 4)]
        contract = self.table(name, once)
        spec = f"{name}:base_year=2026"

        def held(rule):
            with mock.patch("platform_contracts.load_lakehouse_contract", side_effect=lambda _: rule):
                return sorted(r.pnu for r in job.attribute_frame(self.spark, "spark_catalog", spec, "99")[1].collect())

        self.assertEqual(held(versioned(contract, ["pnu"])), held(contract), "one row per key: the same parcels")
        # Lot 2's newest version is of another year: under versions it no longer holds the attribute.
        self.table(name, once + [{**once[1], "base_year": "2025", "ingested_at_utc": NEW}])
        self.assertEqual(held(versioned(contract, ["pnu"])), [pnu(1), pnu(3)])
        # The table's own contract keeps versions (root ADR-0311); without a rule it is read as it is.
        # Before ADR-0328 its rule tied these rows on a null release id and read them all, too.
        unversioned = {**contract, "current_row_latest": None}
        self.assertEqual(held(unversioned), [pnu(1), pnu(2), pnu(3)], "an unversioned table is read as it is")

    def test_the_lineage_binds_a_versioned_table_to_its_current_rows(self):
        import parcel_lineage_inputs as inputs_module

        name = "silver.land_transfer_history"
        once = [{"pnu": pnu(i), "transfer_history_seq": 1, "reason": "split", "ingested_at_utc": OLD} for i in range(1, 4)]
        contract, before = self.frame(name, once)
        _, after = self.frame(name, once + [{**once[0], "reason": "merge", "ingested_at_utc": NEW}])
        inputs = inputs_module.LineageInputs({"history": {"table": name, "snapshot_id": "1"}},
                                             inputs_module.OwnershipInput({}, None), inputs_module.OwnershipInput({}, None))

        def bound(frame, rule):
            with mock.patch.object(inputs_module, "read_pinned_iceberg", return_value=frame), \
                    mock.patch("platform_contracts.load_lakehouse_contract", side_effect=lambda _: rule):
                view = inputs_module.bind_input_views(self.spark, "lakehouse", inputs)["history"]
            return sorted((r.pnu, r.reason) for r in self.spark.table(view).select("pnu", "reason").collect())

        key = ["pnu", "transfer_history_seq"]
        self.assertEqual(bound(before, versioned(contract, key)), bound(before, contract))
        self.assertEqual(bound(after, versioned(contract, key)), [(pnu(1), "merge"), (pnu(2), "split"), (pnu(3), "split")])

    def test_the_unit_proposal_export_reads_the_current_units(self):
        import building_register_unit_proposal_context_export as job

        name = "silver.building_register_units"
        once = [{"mgm_bldrgst_pk": f"unit-{i}", "pnu": pnu(i), "ingested_at_utc": OLD} for i in range(1, 4)]
        contract = self.table(name, once)
        args = Namespace(input_parquet=None, catalog="spark_catalog", namespace="silver", table="building_register_units")

        def units(rule):
            with mock.patch("platform_contracts.load_lakehouse_contract", side_effect=lambda _: rule):
                return sorted((r.mgm_bldrgst_pk, r.pnu) for r in job.read_source_frame(self.spark, args)[0].collect())

        self.assertEqual(units(versioned(contract, ["mgm_bldrgst_pk"])), units(contract))
        self.table(name, once + [{**once[2], "pnu": pnu(9), "ingested_at_utc": NEW}])
        self.assertEqual(units(versioned(contract, ["mgm_bldrgst_pk"])),
                         [("unit-1", pnu(1)), ("unit-2", pnu(2)), ("unit-3", pnu(9))])


if __name__ == "__main__":
    unittest.main()
