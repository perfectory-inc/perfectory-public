"""A release loaded in several batches reads back only the batch just written (Spark).

Run with RUN_SPARK_TESTS=1 in the pinned Spark 3.5.6 container.
"""
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class ReadbackCountsItsOwnBatch(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (SparkSession.builder.master("local[2]").appName("silver-readback-batch")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.ui.enabled", "false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_an_earlier_batch_of_the_same_release_is_not_counted(self):
        # 2026-10-11: the hub price lane's second 40M-row batch read 80M rows back, because every
        # batch of one release shares its (source_snapshot_id, ingested_at_utc) pair.
        import silver_scalar_handoff_to_lakehouse as loader

        # The load column is not first, as in the contracts: the readback keeps the table's order.
        schema = "price_won long, source_part_id string, source_snapshot_id string"
        table = self.spark.createDataFrame(
            [(1, "part-0001", "release"), (2, "part-0001", "release"),
             (3, "part-0002", "release"), (4, "part-0003", "release")], schema)
        batch = self.spark.createDataFrame([(3, "part-0002", "release"), (4, "part-0003", "release")], schema)
        persisted = loader.restrict_to_batch_loads(table, batch, "silver.building_register_apartment_price")
        self.assertEqual(sorted(row.price_won for row in persisted.collect()), [3, 4])
        self.assertEqual(persisted.columns, table.columns)


if __name__ == "__main__":
    unittest.main()
