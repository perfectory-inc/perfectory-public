"""A producer code change is published by its changed keys (root ADR-0321 §5), on a real Iceberg catalog.

The full build of a code change is written to a shadow branch, compared with main by `row_digest`,
and only the keys that differ reach main, recorded in `gold.<t>__changes` (root ADR-0312 §1). A build
that alters K PNUs merges exactly K and leaves the files of the other PNUs alone; one that alters
none adopts the new code version without writing a data file; past the bound, main is fast-forwarded
to the branch and the K keys are still recorded. PNUs are synthetic (`99999…`); no R2 credentials.
"""
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

SCHEMA = "pnu string, payload string, row_digest string"
PNUS = [f"99999000001{n:04d}0000" for n in range(20)]
V1 = [(p, f"v{n}", f"{n:020x}") for n, p in enumerate(PNUS)]
PINS = {"silver.example": "11"}


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class CodeChangePublishIcebergTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[2]").appName("gold-code-change-publish")
                     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                     .config("spark.sql.catalog.proof", "org.apache.iceberg.spark.SparkCatalog")
                     .config("spark.sql.catalog.proof.type", "hadoop")
                     .config("spark.sql.catalog.proof.warehouse", cls.directory.name)
                     # Its own: Stages clears sibling stage directories, and other files run beside this one.
                     .config("spark.local.dir", str(Path(cls.directory.name) / "local"))
                     .config("spark.sql.shuffle.partitions", "4")
                     # Four range files, so a merge has files it must leave alone.
                     .config("spark.sql.adaptive.enabled", "false")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.ui.enabled", "false")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sql("CREATE NAMESPACE IF NOT EXISTS proof.gold")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def table_with_v1(self, name):
        """A Gold built by code V1: four files by PNU range, pins, fingerprint and code version."""
        import gold_incremental as incremental

        table = f"proof.gold.{name}"
        self.spark.sql(f"CREATE TABLE {table} ({SCHEMA}) USING iceberg")
        with mock.patch("gold_code_version.of_running_producer", return_value="V1"):
            incremental.Outcome(mode="full", row_count=len(V1), metrics={},
                                gold=self.frame(V1).repartitionByRange(4, "pnu")).commit(self.spark, table, PINS, "overwrite")
        return table

    def frame(self, rows):
        return self.spark.createDataFrame(rows, SCHEMA)

    def publish(self, table, rows, fraction):
        import gold_incremental as incremental

        outcome = incremental.Outcome(mode="full", row_count=len(rows), metrics={}, gold=self.frame(rows).cache(),
                                      publish_changed_keys=fraction)
        with mock.patch("gold_code_version.of_running_producer", return_value="V2"):
            outcome.commit(self.spark, table, {"silver.example": "12"}, "overwrite")
        return outcome

    def head(self, table):
        row = self.spark.sql(f"SELECT snapshot_id, parent_id, operation, summary FROM {table}.snapshots "
                             f"WHERE snapshot_id = (SELECT snapshot_id FROM {table}.refs WHERE name = 'main')").first()
        return str(row.snapshot_id), str(row.parent_id), row.operation, dict(row.summary)

    def rows(self, table):
        return sorted(tuple(r) for r in self.spark.table(table).select("pnu", "payload", "row_digest").collect())

    def assert_published(self, table, before, expected, changed):
        import gold_code_version
        import gold_incremental as incremental

        head, parent, _, summary = self.head(table)
        self.assertEqual(parent, before)
        self.assertEqual(summary[gold_code_version.PROPERTY], "V2")
        self.assertIn('"12"', summary["foundation.source-iceberg-snapshots"])
        self.assertEqual(self.rows(table), sorted(expected))
        recorded = incremental.recorded_changes(self.spark, table, before, head)
        self.assertEqual(sorted(r.pnu for r in recorded.collect()), sorted(changed))
        refs = {r.name for r in self.spark.sql(f"SELECT name FROM {table}.refs").collect()}
        self.assertEqual(refs, {"main"}, "the shadow branch is dropped once main has the result")
        return summary

    def changed_build(self):
        """Code V2's build: PNU 0 changes, PNU 1 disappears, a new PNU appears; 17 PNUs as before."""
        new = "99999000009999990000"
        rows = [(PNUS[0], "v0-new", "f" * 20), *V1[2:], (new, "new", "e" * 20)]
        return rows, [PNUS[0], PNUS[1], new]

    def test_a_change_that_alters_k_pnus_merges_exactly_k_and_records_them(self):
        import gold_incremental as incremental

        table = self.table_with_v1("publish_k")
        before, _, _, prior = self.head(table)
        rows, changed = self.changed_build()
        outcome = self.publish(table, rows, 0.5)
        summary = self.assert_published(table, before, rows, changed)
        self.assertEqual(summary[incremental.KEYS_CHANGED_PROPERTY], str(len(changed)))
        # Merge-on-read (root ADR-0344): no data file rewritten, the two replaced rows deleted by position.
        self.assertEqual(summary.get("deleted-data-files", "0"), "0")
        self.assertEqual(summary.get("added-position-deletes"), "2")
        self.assertEqual(outcome.report()["publish"], {"to": "changed_keys", "by": "merge"})
        self.assertEqual(outcome.report()["counts"]["affected_pnus"], len(changed))

    def test_a_change_that_alters_no_row_adopts_the_code_without_writing_a_data_file(self):
        table = self.table_with_v1("publish_none")
        before, _, _, _ = self.head(table)
        outcome = self.publish(table, list(reversed(V1)), 0.5)
        self.assert_published(table, before, V1, [])
        _, _, operation, summary = self.head(table)
        self.assertEqual((operation, summary.get("added-data-files", "0"), summary.get("deleted-data-files", "0")),
                         ("append", "0", "0"))
        self.assertEqual((outcome.merged, outcome.report()["publish"]["by"]), (False, "metadata"))

    def test_with_no_recorded_fingerprint_the_shadow_diff_finds_no_row_and_writes_none(self):
        # A head whose fingerprint is unknown cannot be judged equal up front: the shadow branch is
        # written and compared, and the empty difference publishes metadata only.
        import gold_code_version

        table = "proof.gold.publish_unfingerprinted"
        self.spark.sql(f"CREATE TABLE {table} ({SCHEMA}) USING iceberg")
        (self.frame(V1).writeTo(table)
         .option("snapshot-property.foundation.source-iceberg-snapshots", '{"silver.example":"11"}')
         .option(f"snapshot-property.{gold_code_version.PROPERTY}", "V1").append())
        before, _, _, _ = self.head(table)
        outcome = self.publish(table, V1, 0.5)
        self.assert_published(table, before, V1, [])
        _, _, operation, summary = self.head(table)
        self.assertEqual((operation, summary.get("added-data-files", "0")), ("append", "0"))
        self.assertEqual(outcome.report()["publish"]["by"], "metadata")

    def test_past_the_bound_main_is_fast_forwarded_to_the_shadow_build_and_the_keys_are_still_recorded(self):
        table = self.table_with_v1("publish_fast_forward")
        before, _, _, _ = self.head(table)
        rows, changed = self.changed_build()
        outcome = self.publish(table, rows, 0.1)
        self.assert_published(table, before, rows, changed)
        self.assertEqual(outcome.report()["publish"]["by"], "fast_forward")


if __name__ == "__main__":
    unittest.main()
