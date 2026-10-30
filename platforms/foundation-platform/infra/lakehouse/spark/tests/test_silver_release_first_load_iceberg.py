"""A comparing lane's first release into a new table reaches main in one commit, on real Iceberg.

silver.land_ownership and silver.land_use_zone_area (2026-10-30) were new tables: the first load
batch had no main snapshot to make its branch from, so it was written on main and the rest staged,
and the compare refused main's part of the release ("a release is one commit"). Now every batch
stages (an empty commit gives the branch its base), and a table the old path left that way is
compared, not refused. Rows are synthetic.
"""
import json
import os
import sys
import tempfile
import unittest
from argparse import Namespace
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

CONTRACT = "silver.land_transfer_history"
TABLE, UNQUOTED = "`proof`.`silver`.`land_transfer_history`", "proof.silver.land_transfer_history"
LANE = Path(__file__).resolve().parents[2] / "contracts" / "vworld-land-transfer-history-source-objects.json"
CONTENT = json.loads(LANE.read_text(encoding="utf-8"))["silver_refresh"]["compare"]["content_columns"]
RELEASE = "vworldkr-land-transfer-history:20990901"
FIRST, SECOND = [(1, "split"), (2, "merge")], [(3, "once"), (4, "new")]


def lot(n):
    return f"99999101001{n:04d}0000"


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class FirstReleaseIcebergTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        from platform_contracts import column_names, create_table_columns_sql, load_lakehouse_contract

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[2]").appName("silver-release-first-load")
                     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                     .config("spark.sql.catalog.proof", "org.apache.iceberg.spark.SparkCatalog")
                     .config("spark.sql.catalog.proof.type", "hadoop")
                     .config("spark.sql.catalog.proof.warehouse", cls.directory.name)
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.ui.enabled", "false")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.contract = load_lakehouse_contract(CONTRACT)
        cls.columns = column_names(cls.contract)
        cls.schema = " ".join(create_table_columns_sql(cls.contract, indent=0).split())
        cls.spark.sql("CREATE NAMESPACE IF NOT EXISTS proof.silver")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def setUp(self):
        from platform_contracts import create_table_columns_sql

        self.spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
        self.spark.sql(f"CREATE TABLE {TABLE} ({create_table_columns_sql(self.contract)}) USING iceberg")

    def frame(self, obj, events):
        at = datetime(2020, 9, 1, tzinfo=timezone.utc)
        rows = [{"pnu": lot(n), "transfer_history_seq": 1, "reason": reason, "source_record_id": obj,
                 "source_snapshot_id": RELEASE, "ingested_at_utc": at, "data_reference_date": "20990901"}
                for n, reason in events]
        return self.spark.createDataFrame([tuple(row.get(c) for c in self.columns) for row in rows], self.schema)

    def load(self, obj, events, write_mode, branch):
        import lakehouse_ingest as ingest

        return ingest.append_batch_once(self.spark, self.frame(obj, events), self.columns, TABLE, CONTRACT,
                                        write_mode=write_mode, branch=branch)

    def compare_and_publish(self):
        import silver_release_compare as job

        args = Namespace(contract=CONTRACT, branch="load_first", source_snapshot_id=RELEASE,
                         objects="bronze/a.zip,bronze/b.zip", content_columns=",".join(CONTENT),
                         changes_branch="changes_load_first", bucket_prefix_length=0, deadline_utc=None,
                         iceberg_catalog_name="proof")
        summary = job.compare_and_append(self.spark, args)
        self.assertEqual(summary["outcome"], "staged")
        self.spark.sql("CALL proof.system.fast_forward('silver.land_transfer_history', 'main', 'changes_load_first')")
        return summary

    def recorded_on_main(self):
        import lakehouse_ingest as ingest

        return set(ingest.read_ingested_objects(self.spark, TABLE, UNQUOTED))

    def assert_main_holds_the_release_once(self):
        from platform_contracts import current_rows

        self.assertEqual(self.recorded_on_main(), {"bronze/a.zip", "bronze/b.zip"})
        rows = current_rows(self.spark.table(UNQUOTED), self.contract).collect()
        self.assertEqual(Counter(row.pnu for row in rows), Counter(lot(n) for n, _ in FIRST + SECOND))
        self.assertEqual(self.spark.table(UNQUOTED).count(), len(FIRST) + len(SECOND), "no row twice")

    def test_every_batch_of_a_new_tables_first_release_stages_on_its_branch(self):
        first = self.load("bronze/a.zip", FIRST, "overwrite", "load_first")
        second = self.load("bronze/b.zip", SECOND, "append", "load_first")
        self.assertEqual((first["ref"], second["ref"]), ("load_first", "load_first"))
        # Readers see none of the release while it is staged; main records nothing of it.
        self.assertEqual(self.spark.table(UNQUOTED).count(), 0)
        self.assertEqual(self.recorded_on_main(), set())
        summary = self.compare_and_publish()
        self.assertEqual(summary["changed_key_rows"], len(FIRST) + len(SECOND))
        self.assert_main_holds_the_release_once()

    def test_a_first_batch_the_old_path_left_on_main_is_compared_not_refused(self):
        import lakehouse_ingest as ingest

        # The production state of 2026-10-30: batch 0 on main, the branch made from it, the rest staged.
        self.assertTrue(self.load("bronze/a.zip", FIRST, "overwrite", None)["appended"])
        self.spark.sql(f"ALTER TABLE {TABLE} CREATE BRANCH load_first")
        self.assertEqual(self.load("bronze/b.zip", SECOND, "append", "load_first")["ref"], "load_first")
        self.assertEqual(self.recorded_on_main(), {"bronze/a.zip"})
        summary = self.compare_and_publish()
        # main's part is the branch's own rows, so only the rest is appended.
        self.assertEqual((summary["changed_key_rows"], summary["removed_keys"]), (len(SECOND), 0))
        self.assert_main_holds_the_release_once()
        self.assertEqual(set(ingest.read_ingested_objects(self.spark, TABLE, UNQUOTED)),
                         {"bronze/a.zip", "bronze/b.zip"})


if __name__ == "__main__":
    unittest.main()
