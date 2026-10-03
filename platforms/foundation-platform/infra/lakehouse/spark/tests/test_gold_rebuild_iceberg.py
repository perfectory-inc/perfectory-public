"""The Gold rebuild's commit and plan against the pinned Spark/Iceberg runtime (root ADR-0139).

A real local Iceberg catalog, no R2 credentials: a commit through write_gold_snapshot records the
Silver pins in the snapshot summary and replaces every row in one new snapshot while the old one
stays in the history; the planner reads that record back from the `.snapshots` metadata table.
"""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
import gold_rebuild as rebuild  # noqa: E402

ENTRY = {"producer": "fixture", "producer_arguments": [], "max_row_loss_fraction": 0.01}


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class GoldRebuildIcebergTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[2]").appName("gold-rebuild")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.proof", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.proof.type", "hadoop")
            .config("spark.sql.catalog.proof.warehouse", cls.directory.name)
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        for namespace in ("silver", "gold"):
            cls.spark.sql(f"CREATE NAMESPACE proof.{namespace}")
        cls.spark.sql("CREATE TABLE proof.silver.source (pnu STRING) USING iceberg")
        cls.spark.sql("INSERT INTO proof.silver.source VALUES ('9999900000000000001'), ('9999900000000000002')")
        cls.spark.sql("CREATE TABLE proof.gold.panel (pnu STRING) USING iceberg")
        cls.spark.sql("INSERT INTO proof.gold.panel VALUES ('stale')")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def state(self, table):
        return {"head": str(self.spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name='main'").first()[0]),
                "snapshots": rebuild._snapshots(self.spark, table)}

    def plan(self):
        silver = self.state("proof.silver.source")
        gold = self.state("proof.gold.panel")
        head = next(s for s in gold["snapshots"] if s["snapshot_id"] == gold["head"])
        gold["row_count"] = int(head["summary"]["total-records"])
        gold["published_at_utc"] = "2000-01-01T00:00:00Z"
        return rebuild.plan("gold.panel", ENTRY, (("silver.source",), {}, "silver.source"), gold,
                            {"silver.source": silver})

    def test_a_commit_records_its_pins_and_the_next_plan_reads_them(self):
        from pyspark.sql import functions as F
        before = self.state("proof.gold.panel")
        self.assertEqual(self.plan()["action"], "rebuild", "a Gold without a record is read by publish time")
        pins = {"silver.source": self.state("proof.silver.source")["head"]}
        frame = self.spark.table("proof.silver.source")
        rebuild.write_gold_snapshot(frame, "`proof`.`gold`.`panel`", "overwrite", pins, F.lit(True))

        after = self.state("proof.gold.panel")
        head = next(s for s in after["snapshots"] if s["snapshot_id"] == after["head"])
        self.assertEqual(json.loads(head["summary"][rebuild.SOURCE_SNAPSHOTS_PROPERTY]), pins)
        self.assertEqual(head["operation"], "overwrite")
        self.assertEqual(head["parent_id"], before["head"], "the old snapshot stays in the history")
        self.assertEqual(sorted(r.pnu for r in self.spark.table("proof.gold.panel").collect()),
                         ["9999900000000000001", "9999900000000000002"])
        self.assertEqual(self.plan()["action"], "nothing_to_do")

        self.spark.sql("CALL proof.system.rewrite_data_files(table => 'silver.source', options => map('min-input-files','1'))")
        self.assertEqual(self.plan()["action"], "nothing_to_do", "a compaction changes no rows")
        self.spark.sql("INSERT INTO proof.silver.source VALUES ('9999900000000000003')")
        decision = self.plan()
        self.assertEqual(decision["action"], "rebuild")
        self.assertEqual(decision["minimum_row_count"], 2)


if __name__ == "__main__":
    unittest.main()
