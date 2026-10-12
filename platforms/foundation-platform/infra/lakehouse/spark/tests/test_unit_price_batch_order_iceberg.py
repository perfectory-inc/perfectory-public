"""The panel serves the last batch appended for the newest vintage (root ADR-0182, 2026-10-11).

On a real local Iceberg catalog (RUN_ICEBERG_TESTS=1): the order is the ingest registry's commit
order, which only Iceberg's snapshot metadata can show.
"""
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class UnitPriceBatchOrderIcebergTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[2]").appName("unit-price-batch-order")
                     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                     .config("spark.sql.catalog.proof", "org.apache.iceberg.spark.SparkCatalog")
                     .config("spark.sql.catalog.proof.type", "hadoop")
                     .config("spark.sql.catalog.proof.warehouse", cls.directory.name)
                     .config("spark.ui.enabled", "false")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sql("CREATE NAMESPACE IF NOT EXISTS proof.silver")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def test_a_vintage_joined_again_is_served_from_its_later_batch(self):
        import unit_official_price as silver
        from lakehouse_ingest import append_batch_once

        table = "`proof`.`silver`.`unit_official_price`"
        self.spark.sql(f"CREATE TABLE {table} (source_record_id string, source_snapshot_id string) USING iceberg")
        older = silver.batch_source_snapshot_id(1, 2, "209908")
        partial = silver.batch_source_snapshot_id(3, 4, "209909")
        full = silver.batch_source_snapshot_id(5, 6, "209909")
        for batch in (older, partial, full):
            frame = self.spark.createDataFrame(
                [(f"unit-official-price/base-date-v2/{sido}/{batch}", batch) for sido in ("98", "99")],
                "source_record_id string, source_snapshot_id string")
            append_batch_once(self.spark, frame, ["source_record_id", "source_snapshot_id"], table,
                              "silver.unit_official_price")
        order = silver.batch_commit_order(self.spark, "proof.silver.unit_official_price")
        self.assertEqual(order, [older, partial, full])
        self.assertEqual(silver.newest_batch([older, partial, full], order), full)


if __name__ == "__main__":
    unittest.main()
