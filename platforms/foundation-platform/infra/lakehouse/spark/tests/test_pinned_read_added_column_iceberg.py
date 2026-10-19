"""A pinned read of a snapshot taken before a nullable column was added (2026-10-19 regression).

`silver.land_transfer_history` gained `row_state` in the deploy of #728; the parcel panel then read its
pin, a snapshot from before, and Iceberg's time travel by snapshot id gave that snapshot's schema
without the column: "input is missing columns: row_state". Runs on a real local Iceberg catalog
(RUN_ICEBERG_TESTS=1). Rows are synthetic.
"""
import os
import sys
import tempfile
import unittest
from argparse import Namespace
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

NAME = "silver.land_transfer_history"


def lot(n):
    return f"99999101001{n:04d}0000"


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class PinnedReadAddedColumnIcebergTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        from platform_contracts import column_names, columns, load_lakehouse_contract, spark_sql_type

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[2]").appName("pinned-read-added-column")
                     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                     .config("spark.sql.catalog.proof", "org.apache.iceberg.spark.SparkCatalog")
                     .config("spark.sql.catalog.proof.type", "hadoop")
                     .config("spark.sql.catalog.proof.warehouse", cls.directory.name)
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.ui.enabled", "false")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        contract = load_lakehouse_contract(NAME)
        before = [column for column in columns(contract) if column["name"] != "row_state"]
        schema = ", ".join(f"{c['name']} {spark_sql_type(c['logical_type'])}" for c in before)
        cls.spark.sql("CREATE NAMESPACE IF NOT EXISTS proof.silver")
        cls.spark.sql(f"CREATE TABLE proof.{NAME} ({schema}) USING iceberg")
        at = datetime(2099, 9, 1, tzinfo=timezone.utc)
        rows = [{"pnu": lot(n), "transfer_history_seq": 1, "reason": "split", "source_record_id": "bronze/x.zip",
                 "source_snapshot_id": "vworldkr-land-transfer-history:20990901", "ingested_at_utc": at}
                for n in (1, 2, 2)]
        names = [c["name"] for c in before]
        cls.spark.createDataFrame([tuple(r.get(n) for n in names) for r in rows], schema) \
            .writeTo(f"proof.{NAME}").append()
        cls.pin = str(cls.spark.sql(f"SELECT snapshot_id FROM proof.{NAME}.refs WHERE name = 'main'").first()[0])
        # The deploy's migration adds the nullable column after the pin was taken.
        cls.spark.sql(f"ALTER TABLE proof.{NAME} ADD COLUMN row_state string")
        assert "row_state" in column_names(contract)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def test_the_parcel_panel_reads_its_pin_with_the_added_column_as_null(self):
        import parcel_panel_silver_to_gold as job

        args = Namespace(input_mode="iceberg", iceberg_catalog_name="proof", source_iceberg_namespace="silver")
        frame = job.read_source(self.spark, args, NAME, {NAME: self.pin})
        rows = sorted((row.pnu, row.row_state) for row in frame.select("pnu", "row_state").collect())
        # Lot 2's two rows are one release's repeat (root ADR-0090): both current, none removed.
        self.assertEqual(rows, [(lot(1), None), (lot(2), None), (lot(2), None)])

    def test_the_lineage_binds_its_pin_with_the_added_column_as_null(self):
        from lakehouse_snapshot_pins import read_pinned_iceberg
        from platform_contracts import current_rows_of_versioned

        pinned = read_pinned_iceberg(self.spark, f"proof.{NAME}", NAME, {NAME: self.pin})
        self.assertNotIn("row_state", pinned.columns, "time travel reads the snapshot's own schema")
        self.assertEqual(current_rows_of_versioned(pinned, NAME).count(), 3)


class RequiredColumnsStayRequired(unittest.TestCase):
    def test_a_missing_required_column_is_refused_before_spark_is_needed(self):
        from platform_contracts import load_lakehouse_contract, with_contract_columns

        class Frame:
            columns = ["transfer_history_seq"]

        with self.assertRaisesRegex(ValueError, "missing columns: .*pnu"):
            with_contract_columns(Frame(), load_lakehouse_contract(NAME))


if __name__ == "__main__":
    unittest.main()
