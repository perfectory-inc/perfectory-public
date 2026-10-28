"""silver.parcel_boundaries moves to one partition per edition, range ordered by PNU (root ADR-0316).

On 2026-10-28 the table held two editions (79.7M rows, 14.93 GB) in 29 files of one unpartitioned
spec. Every file spanned the whole country in `pnu`, and its `source_snapshot_id` bounds, truncated
to 16 characters, could not tell the editions apart, so a read of the served edition opened all 137
splits. Here a real local Iceberg table starts in that shape and goes through what production will:
the deploy's `apply_write_order`, the next edition's append, and the one controlled rewrite. Files
written before the evolution keep their spec until that rewrite; after it every file holds one
edition and the files of an edition do not overlap in `pnu`.
"""
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

ROWS = 20_000
CONTRACT = {
    "table_name": "silver.parcel_layout_probe",
    "current_row_predicate": "valid_to_utc IS NULL",
    "columns": [
        {"name": "pnu", "logical_type": "string", "required": True},
        {"name": "source_snapshot_id", "logical_type": "string", "required": True},
        {"name": "valid_from_utc", "logical_type": "string", "required": True},
    ],
    "partition_spec": ["source_snapshot_id"],
    "sort_order": ["pnu", "valid_from_utc"],
    "write_distribution": "range",
}
# Longer than the 16-character metrics truncation and equal through it, as the real ids are.
EDITIONS = ("synthetic__parcel-209901", "synthetic__parcel-209902", "synthetic__parcel-209903")


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class ParcelBoundariesLayoutIcebergTest(unittest.TestCase):
    TABLE = "`proof`.`silver`.`parcel_layout_probe`"

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[4]").appName("parcel-boundaries-layout")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.proof", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.proof.type", "hadoop")
            .config("spark.sql.catalog.proof.warehouse", cls.directory.name)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "65536")
            .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sql("CREATE NAMESPACE proof.silver")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def frame(self, edition: str):
        """ROWS synthetic 99999 PNUs of one edition, scattered over 8 input partitions."""
        from pyspark.sql import functions as F

        return (self.spark.range(ROWS).repartition(8).select(
            F.concat(F.lit("99999"), F.lpad(F.col("id").cast("string"), 11, "0"), F.lit("000")).alias("pnu"),
            F.lit(edition).alias("source_snapshot_id"),
            F.lit("2099-01-01T00:00:00Z").alias("valid_from_utc")))

    def files(self) -> list:
        return self.spark.sql(
            f"SELECT spec_id, partition.source_snapshot_id AS edition, "
            f"readable_metrics.pnu.lower_bound AS lo, readable_metrics.pnu.upper_bound AS hi "
            f"FROM {self.TABLE}.files").collect()

    def editions_per_file(self) -> list[int]:
        rows = self.spark.sql(
            f"SELECT input_file_name() AS f, count(DISTINCT source_snapshot_id) AS n "
            f"FROM {self.TABLE} GROUP BY 1").collect()
        return [row.n for row in rows]

    def test_the_edition_becomes_the_partition_and_pnu_ranges_split_its_files(self):
        from platform_contracts import apply_write_order, table_partition_fields

        # Production's shape: unpartitioned, hash, both editions mixed in the same files.
        self.spark.sql(f"CREATE TABLE {self.TABLE} (pnu string, source_snapshot_id string, "
                       "valid_from_utc string) USING iceberg "
                       "TBLPROPERTIES ('format-version' = '2', 'write.distribution-mode' = 'hash')")
        self.frame(EDITIONS[0]).unionByName(self.frame(EDITIONS[1])).repartition(4) \
            .writeTo(self.TABLE).append()
        self.assertIn(2, self.editions_per_file(), "the fixture must mix editions in a file")
        self.assertEqual(table_partition_fields(self.spark, self.TABLE), ())

        self.assertTrue(apply_write_order(self.spark, self.TABLE, CONTRACT))
        self.assertEqual(table_partition_fields(self.spark, self.TABLE), ("source_snapshot_id",))
        self.assertFalse(apply_write_order(self.spark, self.TABLE, CONTRACT), "a second run changes nothing")
        self.spark.sql(f"ALTER TABLE {self.TABLE} SET TBLPROPERTIES "
                       "('write.spark.advisory-partition-size-bytes' = '65536')")

        # The next edition's append lands in the new spec; the files already there keep the old one.
        self.frame(EDITIONS[2]).writeTo(self.TABLE).append()
        files = self.files()
        self.assertTrue(all(row.edition is None for row in files if row.spec_id == 0))
        self.assertTrue(all(row.edition == EDITIONS[2] for row in files if row.spec_id != 0))

        # The one controlled rewrite (ADR-0316 §re-layout) puts every file in the new spec.
        self.spark.sql("CALL proof.system.rewrite_data_files(table => 'silver.parcel_layout_probe', "
                       "strategy => 'sort', options => map('rewrite-all', 'true', "
                       "'target-file-size-bytes', '262144'))").collect()
        files = self.files()
        self.assertTrue(all(row.spec_id != 0 and row.edition in EDITIONS for row in files), files)
        self.assertEqual(set(self.editions_per_file()), {1})
        for edition in EDITIONS:
            bounds = sorted((row.lo, row.hi) for row in files if row.edition == edition)
            for (_, previous_hi), (lo, _) in zip(bounds, bounds[1:]):
                self.assertLessEqual(previous_hi, lo, f"{edition}: data files overlap in pnu: {bounds}")
        self.assertEqual(self.spark.table(self.TABLE).count(), 3 * ROWS)
        self.assertEqual(
            self.spark.table(self.TABLE).where(f"source_snapshot_id = '{EDITIONS[1]}'").count(), ROWS)


if __name__ == "__main__":
    unittest.main()
