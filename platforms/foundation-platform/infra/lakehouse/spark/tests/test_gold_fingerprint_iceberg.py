"""An unchanged Gold is not rewritten, and the bake does not compare it (root ADR-0274 §1).

On a real local Iceberg catalog: a full build whose content equals the current Gold commits an empty
append that carries the new Silver pins and the same content fingerprint; a changed build rewrites
the table under a new fingerprint; and the bake's delta job reflects two snapshots with the same
fingerprint without reading their rows. PNUs are synthetic (`99999…`); no R2 credentials are needed.
"""
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

TABLE = "proof.gold.fingerprint_panel"
ROWS = [("9999900000100010000", "a", "0123456789abcdef0123"), ("9999900000100020000", "b", "fedcba9876543210fedc")]


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class UnchangedGoldIcebergTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[2]").appName("gold-fingerprint")
                     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                     .config("spark.sql.catalog.proof", "org.apache.iceberg.spark.SparkCatalog")
                     .config("spark.sql.catalog.proof.type", "hadoop")
                     .config("spark.sql.catalog.proof.warehouse", cls.directory.name)
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.ui.enabled", "false")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sql("CREATE NAMESPACE IF NOT EXISTS proof.gold")
        cls.spark.sql(f"CREATE TABLE {TABLE} (pnu string, payload string, row_digest string) USING iceberg")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def frame(self, rows):
        return self.spark.createDataFrame(rows, "pnu string, payload string, row_digest string")

    def commit(self, rows, pin):
        import gold_incremental as incremental

        outcome = incremental.Outcome(mode="full", row_count=len(rows), metrics={}, gold=self.frame(rows))
        outcome.commit(self.spark, TABLE, {"silver.example": pin}, "overwrite", contract_table="gold.parcel_panel")
        head = self.spark.sql(f"SELECT snapshot_id, operation, summary FROM {TABLE}.snapshots "
                              "ORDER BY committed_at DESC LIMIT 1").first()
        return outcome, str(head.snapshot_id), head.operation, dict(head.summary)

    def test_an_unchanged_build_records_new_pins_without_rewriting_and_the_bake_skips_the_compare(self):
        import by_pnu_panel_delta as delta
        import gold_fingerprint
        from gold_rebuild import SOURCE_SNAPSHOTS_PROPERTY

        _, first, operation, summary = self.commit(ROWS, "11")
        self.assertEqual(operation, "overwrite")
        fingerprint = summary[gold_fingerprint.PROPERTY]
        self.assertTrue(fingerprint.startswith("rows:2;"), fingerprint)

        # The same content in another order, built from newer Silver pins.
        outcome, second, operation, summary = self.commit(list(reversed(ROWS)), "12")
        self.assertTrue(outcome.content_unchanged)
        self.assertEqual((operation, summary.get("added-records", "0")), ("append", "0"))
        self.assertEqual(summary[gold_fingerprint.PROPERTY], fingerprint)
        self.assertIn('"12"', summary[SOURCE_SNAPSHOTS_PROPERTY])
        self.assertEqual(self.spark.table(TABLE).count(), 2)

        changed = [ROWS[0], ("9999900000100020000", "b2", "00000000000000000001")]
        outcome, third, operation, summary = self.commit(changed, "13")
        self.assertFalse(outcome.content_unchanged)
        self.assertEqual(operation, "overwrite")
        self.assertNotEqual(summary[gold_fingerprint.PROPERTY], fingerprint)

        # The bake: equal fingerprints answer without reading a row; a different one is compared.
        self.assertEqual(delta.same_content(self.spark, TABLE, first, second), 2)
        self.assertIsNone(delta.same_content(self.spark, TABLE, second, third))
        work = Path(self.directory.name) / "delta"
        args = SimpleNamespace(unit="parcel", iceberg_catalog_name="proof", baseline_snapshot_id=first,
                               current_snapshot_id=second, max_delta_fraction=0.5,
                               upsert_output=str(work / "upserts.txt"), delete_output=str(work / "deletes.txt"))
        tables, scan = dict(delta.GOLD_TABLES), delta.snapshot_frame
        delta.GOLD_TABLES["parcel"] = TABLE.split(".", 1)[1]
        delta.snapshot_frame = lambda *_args, **_kwargs: self.fail("an unchanged snapshot was scanned")
        try:
            metrics = delta.compare(self.spark, args)
        finally:
            delta.GOLD_TABLES.clear()
            delta.GOLD_TABLES.update(tables)
            delta.snapshot_frame = scan
        self.assertEqual((metrics["upsert_count"], metrics["delete_count"], metrics["unchanged_count"]), (0, 0, 2))
        self.assertEqual((work / "upserts.txt").read_text(), "")

    def test_a_gold_changed_outside_the_producer_is_rewritten_even_when_its_digests_still_match(self):
        # A write that is not the producer's (a backfill, a hand fix) can change rows and keep their
        # row_digest; only the fingerprint the producer recorded on the current rows is trusted.
        table = "proof.gold.fingerprint_tampered"
        self.spark.sql(f"CREATE TABLE {table} (pnu string, payload string, row_digest string) USING iceberg")
        import gold_incremental as incremental

        def commit(pin):
            outcome = incremental.Outcome(mode="full", row_count=len(ROWS), metrics={}, gold=self.frame(ROWS))
            outcome.commit(self.spark, table, {"silver.example": pin}, "overwrite", contract_table="gold.parcel_panel")
            return outcome

        commit("11")
        self.spark.sql(f"UPDATE {table} SET payload = 'tampered' WHERE pnu = '{ROWS[0][0]}'")
        self.assertFalse(commit("12").content_unchanged)
        payloads = sorted(row.payload for row in self.spark.table(table).collect())
        self.assertEqual(payloads, ["a", "b"])
        # Once the producer recorded it again, the same build is not rewritten.
        self.assertTrue(commit("13").content_unchanged)


if __name__ == "__main__":
    unittest.main()
