"""A release staged on its load branch is invisible on main until published (root ADR-0183).

Runs on a real local Iceberg catalog (RUN_ICEBERG_TESTS=1, the pinned runtime): the branch DDL, the
`branch_<name>` write identifier and the lineage reads are Iceberg's, so a fake would prove nothing.
The publish itself is the lane's catalog commit (Rust, `publish_branch`); here Iceberg's own
fast-forward stands in for it to show what main holds afterwards.
"""
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class LoadBranchIcebergTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[2]").appName("load-branch")
                     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                     .config("spark.sql.catalog.proof", "org.apache.iceberg.spark.SparkCatalog")
                     .config("spark.sql.catalog.proof.type", "hadoop")
                     .config("spark.sql.catalog.proof.warehouse", cls.directory.name)
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.ui.enabled", "false")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sql("CREATE NAMESPACE IF NOT EXISTS proof.silver")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def test_batches_staged_on_the_branch_reach_main_only_together(self):
        import lakehouse_ingest as ingest

        table, unquoted = "`proof`.`silver`.`building_register_apartment_price`", "proof.silver.building_register_apartment_price"
        self.spark.sql(f"CREATE TABLE {table} (source_part_id string, price_won bigint) USING iceberg")
        schema = "source_part_id string, price_won bigint"
        columns = ["source_part_id", "price_won"]
        contract = "silver.building_register_apartment_price"
        previous = self.spark.createDataFrame([("old-part", 1)], schema)
        ingest.append_batch_once(self.spark, previous, columns, table, contract)

        first = self.spark.createDataFrame([("part-0001", 10), ("part-0001", 11)], schema)
        second = self.spark.createDataFrame([("part-0002", 20)], schema)
        for batch in (first, second):
            outcome = ingest.append_batch_once(self.spark, batch, columns, table, contract, branch="load_fixture")
            self.assertTrue(outcome["appended"])
            self.assertEqual(outcome["ref"], "load_fixture")
        # Readers see none of the release while it is staged.
        self.assertEqual(self.spark.table(table).count(), 1)
        self.assertEqual(set(ingest.read_ingested_objects(self.spark, table, unquoted)), {"old-part"})
        staged = set(ingest.read_ingested_objects(self.spark, table, unquoted, "load_fixture"))
        self.assertEqual(staged, {"old-part", "part-0001", "part-0002"})
        # A retry of a staged batch is recognised on the branch and not written twice.
        again = ingest.append_batch_once(self.spark, first, columns, table, contract, branch="load_fixture")
        self.assertFalse(again["appended"])
        self.assertEqual(self.spark.table(f"{table}.`branch_load_fixture`").count(), 4)

        self.spark.sql(f"CALL proof.system.fast_forward('{unquoted.split('.', 1)[1]}', 'main', 'load_fixture')")
        self.assertEqual(self.spark.table(table).count(), 4)
        self.assertEqual(set(ingest.read_ingested_objects(self.spark, table, unquoted)), staged)


if __name__ == "__main__":
    unittest.main()
