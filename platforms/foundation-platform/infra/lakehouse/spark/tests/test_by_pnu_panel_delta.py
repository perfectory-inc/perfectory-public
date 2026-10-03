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
        with self.assertRaises(delta.Refusal):
            delta.judge({"deleted": 3}, 0.5)

    def test_an_identical_snapshot_pair_is_an_empty_change_set(self):
        metrics = delta.judge({"same": 10}, 0.5)
        self.assertEqual((metrics["upsert_count"], metrics["delete_count"]), (0, 0))

    def test_the_fraction_must_be_a_fraction(self):
        for value in (0, -0.1, 1.5):
            with self.subTest(value=value), self.assertRaises(ValueError):
                delta.check_delta_fraction(value)


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


if __name__ == "__main__":
    unittest.main()
