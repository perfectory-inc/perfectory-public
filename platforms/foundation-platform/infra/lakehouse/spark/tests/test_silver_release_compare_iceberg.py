"""A staged release appended to main as its changes (root ADR-0275), on a real local Iceberg catalog.

The release is staged on its load branch the way the lane stages it (`append_batch_once` with a
branch, the first batch overwriting), then `silver_release_compare` appends its changes to main.
After it, main's current rows are the new release, row for row, repeats included (root ADR-0090);
main still holds every row it held before. Rows are synthetic.
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


def release(vintage):
    return f"vworldkr-land-transfer-history:{vintage}"


def lot(n):
    return f"99999101001{n:04d}0000"


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class SilverReleaseCompareIcebergTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        from platform_contracts import column_names, create_table_columns_sql, load_lakehouse_contract

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[2]").appName("silver-release-compare")
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
        cls.spark.sql(f"CREATE TABLE {TABLE} ({create_table_columns_sql(cls.contract)}) USING iceberg")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def frame(self, vintage, events):
        """One release's rows: `events` are (lot, seq, reason); every other content column is fixed."""
        at = datetime(2099, int(vintage[4:6]), 1, tzinfo=timezone.utc)
        rows = [{"pnu": lot(n), "transfer_history_seq": seq, "reason": reason, "source_record_id": f"bronze/{vintage}.zip",
                 "source_snapshot_id": release(vintage), "ingested_at_utc": at, "data_reference_date": vintage}
                for n, seq, reason in events]
        return self.spark.createDataFrame([tuple(row.get(c) for c in self.columns) for row in rows], self.schema)

    def stage(self, vintage, events, branch):
        import lakehouse_ingest as ingest

        outcome = ingest.append_batch_once(self.spark, self.frame(vintage, events), self.columns, TABLE, CONTRACT,
                                           write_mode="overwrite", branch=branch)
        self.assertEqual(outcome["ref"], branch)

    def compare(self, vintage, branch):
        import silver_release_compare as job

        args = Namespace(contract=CONTRACT, branch=branch, source_snapshot_id=release(vintage),
                         objects=f"bronze/{vintage}.zip", content_columns=",".join(CONTENT),
                         iceberg_catalog_name="proof")
        return job.compare_and_append(self.spark, args)

    def current(self):
        from platform_contracts import current_rows

        rows = current_rows(self.spark.table(UNQUOTED), self.contract).collect()
        return Counter((row.pnu, row.transfer_history_seq, row.reason) for row in rows)

    def test_a_release_appends_only_its_changed_keys_and_marks_the_vanished(self):
        import lakehouse_ingest as ingest

        june = [(1, 1, "split"), (2, 1, "dup"), (2, 1, "dup"), (3, 1, "merge"), (4, 1, "once"), (5, 1, "gone")]
        ingest.append_batch_once(self.spark, self.frame("20990601", june), self.columns, TABLE, CONTRACT)
        september = [(1, 1, "split"), (2, 1, "dup"), (2, 1, "dup"), (3, 1, "merge, worded again"),
                     (4, 1, "once"), (4, 1, "once"), (6, 1, "new")]
        self.stage("20990901", september, "load_september")
        self.assertEqual(self.current(), Counter((lot(n), s, r) for n, s, r in june), "staged is not published")

        summary = self.compare("20990901", "load_september")

        # Lot 3 changed (1 row), lot 4 gained a repeat (its 2 rows), lot 6 is new; lot 5 vanished.
        self.assertEqual((summary["outcome"], summary["changed_key_rows"], summary["removed_keys"]), ("appended", 4, 1))
        self.assertEqual(self.current(), Counter((lot(n), s, r) for n, s, r in september))
        self.assertEqual(self.spark.table(UNQUOTED).count(), len(june) + 4 + 1, "no row of main was rewritten")
        recorded = ingest.read_ingested_objects(self.spark, TABLE, UNQUOTED)
        self.assertIn("bronze/20990901.zip", recorded)
        self.assertEqual(self.compare("20990901", "load_september")["outcome"], "already_appended")

        # An unchanged release still commits, so the lane reads it as loaded; it adds no row.
        self.stage("20991201", september, "load_december")
        rows = self.spark.table(UNQUOTED).count()
        summary = self.compare("20991201", "load_december")
        self.assertEqual((summary["changed_key_rows"], summary["removed_keys"]), (0, 0))
        self.assertEqual(self.spark.table(UNQUOTED).count(), rows)
        self.assertIn("bronze/20991201.zip", ingest.read_ingested_objects(self.spark, TABLE, UNQUOTED))
        self.assertEqual(self.current(), Counter((lot(n), s, r) for n, s, r in september))


if __name__ == "__main__":
    unittest.main()
