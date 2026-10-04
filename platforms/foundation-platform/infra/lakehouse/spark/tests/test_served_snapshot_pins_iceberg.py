"""A tagged Gold snapshot survives snapshot expiry; an untagged one does not (root ADR-0146 §2).

The by-PNU publish tags the Gold snapshot each serving manifest reflects, and releases the older
tag only once the new manifest is live. This proves, against the pinned Iceberg runtime and the
maintenance job's own expiry call, the two facts that design rests on: expiry keeps every snapshot
a tag names, and a released tag no longer keeps it. A real local Iceberg catalog; no R2.
"""
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
import lakehouse_maintenance as maintenance  # noqa: E402

TABLE = "gold.parcel_panel"
TAG = "served-parcel-by-pnu-20260101T000000Z-1"


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class ServedSnapshotPinsIceberg(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[2]").appName("served-snapshot-pins")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.proof", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.proof.type", "hadoop")
            .config("spark.sql.catalog.proof.warehouse", cls.directory.name)
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sql("CREATE NAMESPACE proof.gold")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def snapshots(self):
        return {str(row.snapshot_id) for row in
                self.spark.sql(f"SELECT snapshot_id FROM proof.{TABLE}.snapshots").collect()}

    def head(self):
        return str(self.spark.sql(f"SELECT snapshot_id FROM proof.{TABLE}.refs WHERE name = 'main'").first()[0])

    def expire_everything_allowed(self):
        """The maintenance job's call with a cutoff after every snapshot: only refs keep any."""
        self.spark.sql(maintenance.expire_snapshots_sql("proof", TABLE, "2999-01-01 00:00:00.000", 1)).collect()

    def test_a_tag_survives_expiry_and_a_released_tag_does_not(self):
        self.spark.sql(f"CREATE TABLE proof.{TABLE} (pnu STRING, row_digest STRING) USING iceberg")
        self.spark.sql(f"INSERT INTO proof.{TABLE} VALUES ('9999900000100000001', 'a')")
        served = self.head()
        self.spark.sql(f"INSERT OVERWRITE proof.{TABLE} VALUES ('9999900000100000001', 'b')")
        unserved = self.head()
        self.spark.sql(f"INSERT OVERWRITE proof.{TABLE} VALUES ('9999900000100000001', 'c')")
        current = self.head()
        # What the publish commits through the REST catalog (`set-snapshot-ref`, type tag).
        self.spark.sql(f"ALTER TABLE proof.{TABLE} CREATE TAG `{TAG}` AS OF VERSION {served}")

        self.expire_everything_allowed()
        self.assertEqual(self.snapshots(), {served, current},
                         "expiry removed the tagged snapshot, or kept an untagged old one")
        self.assertNotIn(unserved, self.snapshots())
        # The served snapshot is still readable: the next change set can be computed against it.
        rows = self.spark.sql(f"SELECT row_digest FROM proof.{TABLE} VERSION AS OF {served}").collect()
        self.assertEqual([row.row_digest for row in rows], ["a"])

        # Released (the next manifest is live and pins another snapshot): the next expiry frees it.
        self.spark.sql(f"ALTER TABLE proof.{TABLE} DROP TAG `{TAG}`")
        self.expire_everything_allowed()
        self.assertEqual(self.snapshots(), {current})


if __name__ == "__main__":
    unittest.main()
