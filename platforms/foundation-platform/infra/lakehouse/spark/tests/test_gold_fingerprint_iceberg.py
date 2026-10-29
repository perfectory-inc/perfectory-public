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
        outcome.commit(self.spark, TABLE, {"silver.example": pin}, "overwrite")
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
            outcome.commit(self.spark, table, {"silver.example": pin}, "overwrite")
            return outcome

        commit("11")
        self.spark.sql(f"UPDATE {table} SET payload = 'tampered' WHERE pnu = '{ROWS[0][0]}'")
        self.assertFalse(commit("12").content_unchanged)
        payloads = sorted(row.payload for row in self.spark.table(table).collect())
        self.assertEqual(payloads, ["a", "b"])
        # Once the producer recorded it again, the same build is not rewritten.
        self.assertTrue(commit("13").content_unchanged)


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class RecordedChangesIcebergTest(unittest.TestCase):
    """Root ADR-0312 §1: an incremental merge records the PNUs it changed, and the bake's delta compares
    only those, with the counts the whole-snapshot join gives; a full overwrite records the keys whose
    row_digest it changed (root ADR-0347), and the delta reads them with the same counts."""

    TABLE = "proof.gold.recorded_panel"

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[2]").appName("gold-recorded-changes")
                     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                     .config("spark.sql.catalog.proof", "org.apache.iceberg.spark.SparkCatalog")
                     .config("spark.sql.catalog.proof.type", "hadoop")
                     .config("spark.sql.catalog.proof.warehouse", cls.directory.name)
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.ui.enabled", "false")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sql("CREATE NAMESPACE IF NOT EXISTS proof.gold")
        cls.spark.sql(f"CREATE TABLE {cls.TABLE} (pnu string, payload string, row_digest string) USING iceberg")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def head(self):
        return str(self.spark.sql(f"SELECT snapshot_id FROM {self.TABLE}.refs WHERE name = 'main'").first()[0])

    def delta(self, baseline, current, recorded=True):
        import by_pnu_panel_delta as delta
        import gold_incremental

        work = Path(self.directory.name) / f"delta-{baseline}-{current}-{recorded}"
        args = SimpleNamespace(unit="parcel", iceberg_catalog_name="proof", baseline_snapshot_id=baseline,
                               current_snapshot_id=current, max_delta_fraction=1.0,
                               upsert_output=str(work / "upserts.txt"), delete_output=str(work / "deletes.txt"))
        tables, reader = dict(delta.GOLD_TABLES), gold_incremental.recorded_changes
        delta.GOLD_TABLES["parcel"] = self.TABLE.split(".", 1)[1]
        if not recorded:
            gold_incremental.recorded_changes = lambda *_args: None
        try:
            metrics = delta.compare(self.spark, args)
        finally:
            delta.GOLD_TABLES.clear()
            delta.GOLD_TABLES.update(tables)
            gold_incremental.recorded_changes = reader
        return metrics, (work / "upserts.txt").read_text().split(), (work / "deletes.txt").read_text().split()

    def test_the_delta_reads_the_recorded_pnus_and_agrees_with_the_join(self):
        from pyspark.sql import functions as F

        import gold_incremental as incremental

        p = [f"99999000001{n:04d}0000" for n in range(6)]
        v1 = [(p[0], "a", "d0"), (p[1], "b", "d1"), (p[2], "c", "d2"), (p[3], "d", "d3")]
        frame = self.spark.createDataFrame(v1, "pnu string, payload string, row_digest string")
        incremental.Outcome(mode="full", row_count=4, metrics={}, gold=frame).commit(
            self.spark, self.TABLE, {"silver.example": "11"}, "overwrite")
        first = self.head()
        # One changed, one new, one deleted, one untouched: the merge source of plan_merge.
        source = self.spark.createDataFrame(
            [(p[1], "b2", "d1b", False), (p[4], "e", "d4", False), (p[2], None, None, True)],
            f"pnu string, payload string, row_digest string, {incremental.DELETE_FLAG} boolean")
        counts = {"inserted_pnus": 1, "updated_pnus": 1, "deleted_pnus": 1}
        merge = {"source": source, "row_key": ("pnu",), "counts": counts, "persisted": [], "stages": None}
        incremental.Outcome(mode="incremental", row_count=4, metrics={}, merge=merge).commit(
            self.spark, self.TABLE, {"silver.example": "12"}, "overwrite")
        second = self.head()
        summary = self.spark.sql(f"SELECT summary FROM {self.TABLE}.snapshots WHERE snapshot_id = {second}").first()[0]
        self.assertEqual(summary[incremental.KEYS_CHANGED_PROPERTY], "3")
        recorded = {r.pnu for r in incremental.recorded_changes(self.spark, self.TABLE, first, second).collect()}
        self.assertEqual(recorded, {p[1], p[2], p[4]})
        # An empty merge records its step too, so the walk passes it.
        empty = self.spark.createDataFrame([], f"pnu string, payload string, row_digest string, {incremental.DELETE_FLAG} boolean")
        incremental.Outcome(mode="incremental", row_count=4, metrics={},
                            merge={**merge, "source": empty, "counts": dict.fromkeys(counts, 0)}).commit(self.spark, self.TABLE, {"silver.example": "13"}, "overwrite")
        third = self.head()
        self.spark.sql(f"CALL proof.system.rewrite_data_files(table => 'gold.recorded_panel', options => map('rewrite-all', 'true'))")
        compacted = self.head()

        via_record, upserts, deletes = self.delta(first, compacted)
        via_join, joined_upserts, joined_deletes = self.delta(first, compacted, recorded=False)
        self.assertEqual((via_record["change_source"], via_join["change_source"]), ("recorded", "diff"))
        counts = ("changed_count", "new_count", "deleted_count", "unchanged_count", "current_total")
        self.assertEqual({k: via_record[k] for k in counts}, {k: via_join[k] for k in counts})
        self.assertEqual({k: via_record[k] for k in counts},
                         {"changed_count": 1, "new_count": 1, "deleted_count": 1, "unchanged_count": 2, "current_total": 4})
        self.assertEqual((upserts, deletes), (joined_upserts, joined_deletes))
        import by_pnu_panel_delta as delta
        pushdown, delta.PUSHDOWN_PNUS = delta.PUSHDOWN_PNUS, 0
        try:
            via_join_of_record = self.delta(first, compacted)[0]
        finally:
            delta.PUSHDOWN_PNUS = pushdown
        self.assertEqual({k: via_join_of_record[k] for k in counts}, {k: via_join[k] for k in counts})
        self.assertEqual(self.delta(second, third)[0]["upsert_count"], 0)

        # Root ADR-0347: a full overwrite records the keys whose row_digest it changed, like a merge.
        # Incident 2026-10-29: two whole overwrites of gold.building_panel recorded nothing, and the
        # bake could not tell 0.17% real change from 99.8% rewritten ids.
        changed = F.col("pnu") == p[0]
        rewritten = (self.spark.table(self.TABLE)
                     .withColumn("payload", F.when(changed, F.concat("payload", F.lit("!"))).otherwise(F.col("payload")))
                     .withColumn("row_digest", F.when(changed, F.lit("d0b")).otherwise(F.col("row_digest")))
                     .where(F.col("pnu") != p[3]).cache())
        rewritten.count()
        outcome = incremental.Outcome(mode="full", row_count=3, metrics={}, gold=rewritten)
        outcome.commit(self.spark, self.TABLE, {"silver.example": "14"}, "overwrite")
        overwritten = self.head()
        summary = self.spark.sql(f"SELECT operation, summary FROM {self.TABLE}.snapshots "
                                 f"WHERE snapshot_id = {overwritten}").first()
        self.assertEqual(summary.operation, "overwrite")
        self.assertEqual(summary.summary[incremental.KEYS_CHANGED_PROPERTY], "2")
        self.assertEqual(outcome.report()["overwrite_changes"],
                         {"affected_pnus": 2, "inserted_pnus": 0, "updated_pnus": 1, "deleted_pnus": 1})
        recorded = {r.pnu for r in incremental.recorded_changes(self.spark, self.TABLE, compacted, overwritten).collect()}
        self.assertEqual(recorded, {p[0], p[3]})
        via_record, upserts, deletes = self.delta(compacted, overwritten)
        via_join, joined_upserts, joined_deletes = self.delta(compacted, overwritten, recorded=False)
        self.assertEqual((via_record["change_source"], via_join["change_source"]), ("recorded", "diff"))
        self.assertEqual({k: via_record[k] for k in counts}, {k: via_join[k] for k in counts})
        self.assertEqual((upserts, deletes), (joined_upserts, joined_deletes))


if __name__ == "__main__":
    unittest.main()
