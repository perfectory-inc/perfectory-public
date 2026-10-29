"""The by-PNU change set (root ADR-0141 §3): what it names and what it refuses.

The verdict and the refusals are plain functions, so the lane without PySpark checks them. The
Spark join itself runs with `RUN_SPARK_TESTS=1` in the pinned Spark image. PNUs are synthetic.
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import by_pnu_panel_delta as delta  # noqa: E402


class SnapshotIds(unittest.TestCase):
    def test_no_comparison_snapshot_is_a_refusal_not_no_change(self):
        for baseline in ("", "   "):
            with self.subTest(baseline=baseline):
                with self.assertRaises(delta.Refusal) as refused:
                    delta.check_snapshot_ids(baseline, "202")
                self.assertEqual(refused.exception.exit_code, delta.EXIT_NO_COMPARISON)

    def test_the_same_snapshot_on_both_sides_is_refused(self):
        with self.assertRaises(delta.Refusal):
            delta.check_snapshot_ids("202", "202")

    def test_a_snapshot_id_that_is_not_a_number_is_refused(self):
        with self.assertRaises(delta.Refusal):
            delta.check_snapshot_ids("101; DROP", "202")

    def test_the_main_entry_refuses_before_starting_spark(self):
        with tempfile.TemporaryDirectory() as work:
            code = delta.main([
                "--unit", "building", "--current-snapshot-id", "202", "--max-delta-fraction", "0.5",
                "--upsert-output", f"{work}/u", "--delete-output", f"{work}/d",
                "--summary-output", f"{work}/s.json",
            ])
            self.assertEqual(code, delta.EXIT_NO_COMPARISON)
            self.assertFalse(Path(work, "s.json").exists())

    def test_every_lane_the_contract_names_is_a_unit(self):
        # Root ADR-0190: a pack-only lane's Gold panel is compared like the parcel and building ones.
        import json
        lanes = json.loads(delta.R2_CONTRACT_PATH.read_text(encoding="utf-8"))["by_pnu_lanes"]
        self.assertTrue(lanes)
        import by_pnu_kinds
        kinds = by_pnu_kinds.kinds()
        for name, lane in lanes.items():
            self.assertEqual(delta.GOLD_TABLES[name], kinds[name].gold_table if name in kinds else lane["gold_table"])
            self.assertEqual(delta.parse_args([
                "--unit", name, "--current-snapshot-id", "202", "--max-delta-fraction", "0.5",
                "--upsert-output", "u", "--delete-output", "d", "--summary-output", "s.json",
            ]).unit, name)


class RefusalCodes(unittest.TestCase):
    def test_the_two_cannot_compare_refusals_have_their_own_exit_codes(self):
        # The bake tells an expired snapshot (3) from a digest-less one (5) by these codes.
        self.assertEqual(
            sorted({delta.EXIT_NO_COMPARISON, delta.EXIT_NOT_A_DELTA, delta.EXIT_NO_DIGEST}),
            [3, 4, 5],
        )


class Judge(unittest.TestCase):
    def test_counts_name_upserts_and_tombstones(self):
        metrics = delta.judge({"changed": 2, "new": 1, "deleted": 1, "same": 96}, 0.5)
        self.assertEqual(metrics["upsert_count"], 3)
        self.assertEqual(metrics["delete_count"], 1)
        self.assertEqual(metrics["current_total"], 99)

    def test_more_than_the_contract_fraction_is_not_a_delta(self):
        with self.assertRaises(delta.Refusal) as refused:
            delta.judge({"changed": 40, "new": 5, "deleted": 6, "same": 55}, 0.5)
        self.assertEqual(refused.exception.exit_code, delta.EXIT_NOT_A_DELTA)
        # At the bound exactly it is still a delta.
        delta.judge({"changed": 50, "same": 50}, 0.5)

    def test_an_empty_current_table_is_refused(self):
        with self.assertRaises(delta.Refusal) as refused:
            delta.judge({"deleted": 3}, 0.5)
        # Not a large change a record could explain: an empty Gold is never baked.
        self.assertNotIsInstance(refused.exception, delta.NotADelta)

    def test_an_identical_snapshot_pair_is_an_empty_change_set(self):
        metrics = delta.judge({"same": 10}, 0.5)
        self.assertEqual((metrics["upsert_count"], metrics["delete_count"]), (0, 0))

    def test_the_fraction_must_be_a_fraction(self):
        for value in (0, -0.1, 1.5):
            with self.subTest(value=value), self.assertRaises(ValueError):
                delta.check_delta_fraction(value)


class LargeChange(unittest.TestCase):
    """Root ADR-0346: over max_delta_fraction, a record that explains the change makes it a full bake
    (exit 6); none is a refusal (exit 4). Named for the 2026-10-29 refusals of the building lane
    (unexplained: two whole overwrites, no record) and parcel-forest-ledger (a v1 -> v2 schema)."""

    ACCEPTED = delta.accepted_explanations(delta._CONTRACT)
    CODE = '{"sha256": "%s", "parts": {}}'

    def over(self):
        with self.assertRaises(delta.NotADelta) as refused:
            delta.judge({"changed": 5933490, "new": 6029, "deleted": 6487, "same": 5790}, 0.5)
        return refused.exception

    def test_the_contract_accepts_the_three_records_with_its_reason(self):
        self.assertEqual(self.ACCEPTED, ("recorded_change_set", "code_version", "document_schema"))
        for broken in ({"explained_by": ["the size of the change"], "reason": "x"},
                       {"explained_by": ["code_version"], "reason": " "}):
            with self.subTest(broken=broken), self.assertRaises(ValueError):
                delta.accepted_explanations({"by_pnu_serving_patches": {"over_max_delta_fraction": broken}})

    def test_the_building_refusal_of_2026_10_29_stays_a_refusal(self):
        # Neither overwrite recorded its keys or a code version, and the documents kept their schema.
        large = self.over()
        large.explain(delta.explanations(None, (None, None), "", self.ACCEPTED))
        self.assertEqual((large.exit_code, large.explained_by), (delta.EXIT_NOT_A_DELTA, {}))
        self.assertEqual(large.counts, {"changed": 5933490, "new": 6029, "deleted": 6487, "same": 5790})

    def test_a_document_schema_revision_explains_it(self):
        # parcel-forest-ledger, 2026-10-29: v1 -> v2 re-rendered almost every document.
        large = self.over()
        large.explain(delta.explanations(None, (None, None), "doc.v1 -> doc.v2", self.ACCEPTED))
        self.assertEqual(large.exit_code, delta.EXIT_EXPLAINED_FULL)
        self.assertEqual(list(large.explained_by), ["document_schema"])

    def test_recorded_keys_explain_it(self):
        found = delta.explanations(5945309, (None, None), "", self.ACCEPTED)
        self.assertEqual(list(found), ["recorded_change_set"])
        self.assertIn("5945309", found["recorded_change_set"])

    def test_only_a_code_version_both_commits_record_and_that_differs_explains_it(self):
        old, new = self.CODE % ("a" * 64), self.CODE % ("b" * 64)
        self.assertEqual(list(delta.explanations(None, (old, new), "", self.ACCEPTED)), ["code_version"])
        # The same code, or a commit from before ADR-0321 that records none, says nothing.
        for versions in ((old, old), (None, new), (old, None)):
            with self.subTest(versions=versions):
                self.assertEqual(delta.explanations(None, versions, "", self.ACCEPTED), {})
        # The parts may differ in order; the whole sha256 decides.
        self.assertEqual(delta.code_sha(self.CODE % ("c" * 64)), "c" * 64)

    def test_an_explanation_the_contract_does_not_accept_is_not_used(self):
        self.assertEqual(delta.explanations(1, (None, None), "doc.v1 -> doc.v2", ("code_version",)), {})

    def test_the_evidence_is_written_for_the_bake(self):
        large = self.over()
        large.explain(delta.explanations(None, (None, None), "", self.ACCEPTED))
        with tempfile.TemporaryDirectory() as work:
            args = delta.parse_args([
                "--unit", "building", "--baseline-snapshot-id", "101", "--current-snapshot-id", "202",
                "--max-delta-fraction", "0.5", "--upsert-output", f"{work}/u", "--delete-output", f"{work}/d",
                "--summary-output", f"{work}/s.json"])
            delta.write_large_change(args, large)
            import json
            summary = json.loads(Path(work, "s.json").read_text(encoding="utf-8"))
        self.assertEqual(summary["refused"], "not_a_delta")
        self.assertEqual(summary["counts"]["changed"], 5933490)
        self.assertEqual((summary["explained_by"], summary["explanations_checked"]), ({}, list(self.ACCEPTED)))


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class ClassifySpark(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("by-pnu-delta").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, rows):
        from pyspark.sql import functions as F

        return self.spark.createDataFrame(rows, "pnu string, row_digest string").withColumn(
            "present", F.lit(True))

    def test_each_pnu_gets_one_verdict(self):
        baseline = self.frame([("9999900000100000001", "a"), ("9999900000100000002", "b"),
                               ("9999900000100000003", "c")])
        current = self.frame([("9999900000100000001", "a"), ("9999900000100000002", "B"),
                              ("9999900000100000004", "d")])
        verdicts = {row.pnu: row.verdict for row in delta.classify(baseline, current).collect()}
        self.assertEqual(verdicts, {
            "9999900000100000001": "same", "9999900000100000002": "changed",
            "9999900000100000003": "deleted", "9999900000100000004": "new"})


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class ComparisonSnapshotIceberg(unittest.TestCase):
    """The two "cannot compare" refusals are told apart (root ADR-0146): a snapshot Iceberg no
    longer holds is exit 3, a snapshot written before `row_digest` existed is exit 5."""

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[2]").appName("by-pnu-delta-iceberg")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.proof", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.proof.type", "hadoop")
            .config("spark.sql.catalog.proof.warehouse", cls.directory.name)
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sql("CREATE NAMESPACE proof.gold")
        cls.spark.sql("CREATE TABLE proof.gold.parcel_panel (pnu STRING) USING iceberg")
        cls.spark.sql("INSERT INTO proof.gold.parcel_panel VALUES ('9999900000100000001')")
        cls.digestless = cls.head()
        cls.spark.sql("ALTER TABLE proof.gold.parcel_panel ADD COLUMN row_digest STRING")
        cls.spark.sql("INSERT OVERWRITE proof.gold.parcel_panel VALUES ('9999900000100000001', 'a')")
        cls.digested = cls.head()

    @classmethod
    def head(cls):
        return str(cls.spark.sql(
            "SELECT snapshot_id FROM proof.gold.parcel_panel.refs WHERE name = 'main'").first()[0])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def refusal(self, snapshot_id):
        with self.assertRaises(delta.Refusal) as refused:
            delta.snapshot_frame(self.spark, "proof", "gold.parcel_panel", snapshot_id)
        return refused.exception

    def test_a_snapshot_without_row_digest_is_exit_5(self):
        refusal = self.refusal(self.digestless)
        self.assertEqual(refusal.exit_code, delta.EXIT_NO_DIGEST)
        self.assertIn("carries no row_digest", str(refusal))

    def test_a_snapshot_iceberg_no_longer_holds_is_exit_3(self):
        refusal = self.refusal("999990000000000009")
        self.assertEqual(refusal.exit_code, delta.EXIT_NO_COMPARISON)
        self.assertIn("no longer has snapshot", str(refusal))

    def test_a_snapshot_with_row_digest_is_compared(self):
        frame = delta.snapshot_frame(self.spark, "proof", "gold.parcel_panel", self.digested)
        self.assertEqual([row.row_digest for row in frame.collect()], ["a"])

    def test_a_lane_keyed_narrower_than_a_pnu_compares_and_lists_keys(self):
        # Root ADR-0204: the unit-price Gold is one row per (pnu, building_id); a building gone from
        # a PNU is a deleted key, not a changed PNU, so the bake tombstones exactly that document.
        self.assertEqual(delta.KEY_SUFFIX_COLUMNS.get("unit-price"), "building_id")
        self.spark.sql("CREATE TABLE proof.gold.keyed (pnu STRING, building_id STRING, row_digest STRING) USING iceberg")
        pnu = "9999900000100000001"
        self.spark.sql(f"INSERT INTO proof.gold.keyed VALUES ('{pnu}', 'b1', 'x'), ('{pnu}', 'b2', 'y')")
        baseline = str(self.spark.sql("SELECT snapshot_id FROM proof.gold.keyed.refs WHERE name = 'main'").first()[0])
        self.spark.sql(f"INSERT OVERWRITE proof.gold.keyed VALUES ('{pnu}', 'b1', 'z')")
        current = str(self.spark.sql("SELECT snapshot_id FROM proof.gold.keyed.refs WHERE name = 'main'").first()[0])
        verdicts = {row.pnu: row.verdict for row in delta.classify(
            delta.snapshot_frame(self.spark, "proof", "gold.keyed", baseline, "building_id"),
            delta.snapshot_frame(self.spark, "proof", "gold.keyed", current, "building_id")).collect()}
        self.assertEqual(verdicts, {f"{pnu}b1": "changed", f"{pnu}b2": "deleted"})


class RecordedChangeSet(unittest.TestCase):
    """Root ADR-0312 §1: with recorded PNUs only they are compared; every other current row is the same."""

    def test_the_rows_outside_the_recorded_pnus_count_as_unchanged(self):
        counts = {"changed": 3, "new": 2, "deleted": 1, "same": 4}
        self.assertEqual(delta.unchanged_rest(100, counts), 95)
        self.assertEqual(delta.judge({**counts, "same": delta.unchanged_rest(100, counts)}, 0.1)["unchanged_count"], 95)

    def test_a_row_count_below_the_recorded_pnus_is_refused(self):
        with self.assertRaises(ValueError):
            delta.unchanged_rest(5, {"changed": 3, "new": 2, "same": 1})

    def test_a_merge_on_read_snapshot_holds_its_records_less_its_position_deletes(self):
        # Root ADR-0344: total-records counts a deleted row until compaction folds the delete in.
        self.assertEqual(delta.live_rows("100", "7", "0"), 93)
        self.assertEqual(delta.live_rows("100", None, None), 100)
        with self.assertRaises(ValueError):
            delta.live_rows("100", "0", "3")


if __name__ == "__main__":
    unittest.main()
