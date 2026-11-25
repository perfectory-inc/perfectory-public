"""The map edit pin and the stale list on real Iceberg (root ADR-0621 §3, §5).

`resolve_pins` reads two snapshots by id: the served Gold of the release an edit was saved over, and
the Silver snapshot that Gold was built from. `record_outcomes` reads the newest outcome per edit
from `gold.map_edit_stale` and appends only the ones that changed. Both run in production only when
a complex or admin edit is folded, so their Spark SQL (the snapshots metadata read, the time travel,
the window over the stale list) is proved here before that. Ids are synthetic.
"""
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

SILVER = "`proof`.`silver`.`boundaries_proof`"
SERVED = "`proof`.`gold`.`served_proof`"
P, Q, R = "99999-p", "99999-q", "99999-r"
OLD, NEW = "a" * 64, "b" * 64


def session(name):
    from pyspark.sql import SparkSession

    directory = tempfile.TemporaryDirectory()
    spark = (SparkSession.builder.master("local[2]").appName(name)
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.proof", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.proof.type", "hadoop")
             .config("spark.sql.catalog.proof.warehouse", directory.name)
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    for namespace in ("proof.silver", "proof.gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    return spark, directory


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class MapEditPinAndStaleListIcebergTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import served_gold_common as common

        cls.common = common
        cls.spark, cls.directory = session("map-edit-pin-and-stale-list")
        spark = cls.spark
        # The Silver the release was built from, then the source redrawing P.
        spark.sql(f"CREATE TABLE {SILVER} (id string, geometry_checksum_sha256 string) USING iceberg")
        spark.sql(f"INSERT INTO {SILVER} VALUES ('{P}', '{OLD}'), ('{Q}', '{OLD}')")
        cls.silver_at_release = common.current_snapshot(spark, SILVER)
        spark.sql(f"CREATE TABLE {SERVED} (id string, silver_iceberg_snapshot_id string) USING iceberg")
        spark.sql(f"INSERT INTO {SERVED} VALUES ('{P}', '{cls.silver_at_release}'), ('{Q}', '{cls.silver_at_release}')")
        cls.release_gold = common.current_snapshot(spark, SERVED)
        spark.sql(f"INSERT OVERWRITE {SILVER} VALUES ('{P}', '{NEW}'), ('{Q}', '{OLD}')")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def test_a_pin_is_the_target_as_the_release_saw_it_and_an_unknown_release_leaves_it_empty(self):
        def checksums(frame):
            return {row["id"]: row["geometry_checksum_sha256"] for row in frame.collect()}

        fresh = [
            {"change_seq": 1, "feature_id": P, "base_served_snapshot_id": self.release_gold},
            {"change_seq": 2, "feature_id": R, "base_served_snapshot_id": self.release_gold},
            {"change_seq": 3, "feature_id": Q, "base_served_snapshot_id": "123456789"},
            {"change_seq": 4, "feature_id": Q, "base_served_snapshot_id": None},
        ]
        pinned = self.common.resolve_pins(self.spark, served_table=SERVED, silver_table=SILVER, fresh=fresh,
                                          base_checksums=checksums)
        self.assertEqual(
            [(row["base_source_snapshot_id"], row["base_geometry_checksum_sha256"]) for row in pinned],
            [
                (self.silver_at_release, OLD),  # the Silver of the release, not its redrawn head
                (self.silver_at_release, None),  # the source had no R
                (None, None),  # a Gold snapshot that does not exist
                (None, None),  # no release named
            ],
        )
        # The pin it read and the current source differ: the build would judge the edit stale.
        applied, stale = self.common.judge(pinned[:1], {P: NEW})
        self.assertEqual((applied, stale), ([], {1: "source_changed"}))

    def test_the_stale_list_gets_a_row_only_when_an_outcome_changes(self):
        common = self.common
        table = common.ensure_table(self.spark, "`proof`.`gold`.`map_edit_stale`", "proof", "gold",
                                    common.STALE_CONTRACT)
        ledger = [{"change_seq": 1, "feature_id": P, "op": "upsert"},
                  {"change_seq": 2, "feature_id": Q, "op": "delete"}]

        def judged(stale, batch, second):
            return common.record_outcomes(self.spark, table, "complex", ledger, stale, self.silver_at_release,
                                          batch, datetime(2099, 1, 1, 0, 0, second, tzinfo=timezone.utc))

        first = judged({1: "source_changed"}, "batch-1", 1)
        self.assertEqual([(row["change_seq"], row["outcome"]) for row in first], [(1, "stale")])
        self.assertEqual(judged({1: "source_changed"}, "batch-2", 2), [], "still stale: no row")
        back = judged({}, "batch-3", 3)
        self.assertEqual([(row["change_seq"], row["outcome"]) for row in back], [(1, "applied")])
        rows = self.spark.sql(f"SELECT change_seq, outcome, reason, export_batch_id FROM {table} ORDER BY judged_at_utc").collect()
        self.assertEqual([(r["change_seq"], r["outcome"], r["reason"], r["export_batch_id"]) for r in rows],
                         [(1, "stale", "source_changed", "batch-1"), (1, "applied", None, "batch-3")])


if __name__ == "__main__":
    unittest.main()
