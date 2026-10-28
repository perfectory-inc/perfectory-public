"""A range-ordered Gold keeps disjoint PNU ranges per data file through every write (root ADR-0307).

The by-PNU bake skips a data file only when its `pnu` bounds exclude the shard's prefix (root
ADR-0164). On 2026-10-28 every file of gold.parcel_panel spanned the whole country, so each of
~252 shards downloaded the whole 4 GB table: the full write was range-ordered, but the incremental
rebuild's MERGE hash-clustered by the (single-valued) partition and the maintenance compaction
bin-packed the files together. Here a real local Iceberg table goes through all three — the
shared writer's overwrite, the incremental MERGE and the maintenance compaction — and after each
the data files' `pnu` bounds must not overlap.
"""
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

ROWS = 100_000
CONTRACT = {
    "table_name": "gold.layout_probe",
    "current_row_predicate": None,
    "columns": [
        {"name": "pnu", "logical_type": "string", "required": True},
        {"name": "row_digest", "logical_type": "string", "required": True},
        {"name": "source_snapshot_id", "logical_type": "string", "required": True},
    ],
    # One partition value, as in production: the partition does not separate PNUs, the order must.
    "partition_spec": ["source_snapshot_id"],
    "sort_order": ["pnu"],
    "write_distribution": "range",
}


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class GoldRangeLayoutIcebergTest(unittest.TestCase):
    TABLE = "`proof`.`gold`.`layout_probe`"

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[4]").appName("gold-range-layout")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.proof", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.proof.type", "hadoop")
            .config("spark.sql.catalog.proof.warehouse", cls.directory.name)
            # The production session's adaptive execution (lakehouse_engine.py).
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            # Small write tasks so a few thousand rows make several files, as 40M rows do in production.
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "65536")
            .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sql("CREATE NAMESPACE proof.gold")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def frame(self, version: str, parity: int = 0):
        """ROWS synthetic PNUs (16-character unique prefix, under the bounds' truncation), even or
        odd, scattered over 8 input partitions so no input task holds one range."""
        from pyspark.sql import functions as F

        number = F.col("id") * 2 + parity
        return (self.spark.range(ROWS).repartition(8).select(
            F.concat(F.lit("99999"), F.lpad(number.cast("string"), 11, "0"), F.lit("000")).alias("pnu"),
            F.sha2(F.concat(F.col("id").cast("string"), F.lit(version)), 256).alias("row_digest"),
            F.lit("probe").alias("source_snapshot_id")))

    def bounds(self) -> list[tuple[str, str]]:
        rows = self.spark.sql(
            f"SELECT readable_metrics.pnu.lower_bound AS lo, readable_metrics.pnu.upper_bound AS hi "
            f"FROM {self.TABLE}.files").collect()
        return sorted((row.lo, row.hi) for row in rows)

    def assert_disjoint(self, step: str) -> None:
        bounds = self.bounds()
        self.assertGreater(len(bounds), 1, f"{step}: one file proves nothing about the layout")
        for (_, previous_hi), (lo, _) in zip(bounds, bounds[1:]):
            # A truncated upper bound rounds up to the next file's first prefix, hence <=.
            self.assertLessEqual(previous_hi, lo, f"{step}: data files overlap in pnu: {bounds}")

    def test_every_gold_write_keeps_disjoint_pnu_ranges(self):
        from pyspark.sql import functions as F

        import gold_incremental
        import lakehouse_maintenance
        from gold_rebuild import write_gold_snapshot
        from platform_contracts import ensure_contract_table

        ensure_contract_table(self.spark, self.TABLE, CONTRACT)
        # Iceberg sizes write tasks itself; small ones make several files here, as 40M rows do in production.
        self.spark.sql(f"ALTER TABLE {self.TABLE} SET TBLPROPERTIES "
                       "('write.spark.advisory-partition-size-bytes' = '262144')")

        write_gold_snapshot(self.frame("v1"), self.TABLE, "overwrite", {}, F.lit(True))
        self.assert_disjoint("overwrite")

        changed = self.frame("v2").withColumn(gold_incremental.DELETE_FLAG, F.lit(False))
        self.assertTrue(gold_incremental.merge_into_gold(self.spark, self.TABLE, changed, {}))
        self.assert_disjoint("incremental merge")

        # An append of the odd PNUs interleaves with every existing file; a compaction must not keep
        # (bin-pack would) the overlap, it must sort the files it rewrites back into ranges.
        write_gold_snapshot(self.frame("v1", parity=1), self.TABLE, "append", {}, F.lit(True))
        compaction = {**lakehouse_maintenance.load_maintenance_contract()["compaction"],
                      "target_file_bytes": 4_000_000}
        ordered = lakehouse_maintenance.has_sort_order(self.spark, self.TABLE)
        self.assertTrue(ordered)
        before = len(self.bounds())
        self.spark.sql(lakehouse_maintenance.rewrite_data_files_sql(
            "proof", "gold.layout_probe", compaction, ordered)).collect()
        self.assertLess(len(self.bounds()), before, "the compaction did not merge the small files")
        self.assert_disjoint("compaction")
        self.assertEqual(self.spark.table(self.TABLE).count(), 2 * ROWS)


if __name__ == "__main__":
    unittest.main()
