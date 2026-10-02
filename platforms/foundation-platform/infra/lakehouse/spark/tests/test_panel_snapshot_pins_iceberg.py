"""Real snapshot isolation with the pinned Spark/Iceberg runtime; no R2 credentials needed."""
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
from lakehouse_snapshot_pins import read_pinned_iceberg


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class PanelSnapshotIcebergTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[2]").appName("panel-snapshot-pins")
            .config("spark.sql.catalog.pinproof", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.pinproof.type", "hadoop")
            .config("spark.sql.catalog.pinproof.warehouse", cls.directory.name)
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sql("CREATE NAMESPACE pinproof.silver")
        cls.spark.sql("CREATE TABLE pinproof.silver.source (id BIGINT, value STRING) USING iceberg")
        cls.spark.sql("INSERT INTO pinproof.silver.source VALUES (1, 'before')")
        cls.original = str(cls.spark.sql(
            "SELECT snapshot_id FROM pinproof.silver.source.refs WHERE name='main'").first()[0])
        cls.spark.sql("INSERT OVERWRITE pinproof.silver.source VALUES (1, 'after')")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def test_existing_head_can_change_without_changing_the_pinned_value(self):
        self.assertEqual(self.spark.table("pinproof.silver.source").first().value, "after")
        for _ in range(2):
            frame = read_pinned_iceberg(self.spark, "pinproof.silver.source", "silver.source",
                                      {"silver.source": self.original})
            self.assertEqual([(row.id, row.value) for row in frame.collect()], [(1, "before")])

    def test_missing_snapshot_is_not_replaced_with_current_head(self):
        with self.assertRaises(Exception) as caught:
            read_pinned_iceberg(self.spark, "pinproof.silver.source", "silver.source",
                                {"silver.source": "1"}).collect()
        self.assertIn("snapshot", str(caught.exception).lower())
        self.assertEqual(self.spark.table("pinproof.silver.source").first().value, "after")


if __name__ == "__main__":
    unittest.main()
