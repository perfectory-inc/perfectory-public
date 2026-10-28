"""`collect_sorted_unique` dedups with a map-side `min(struct(...))` instead of a `row_number()`
window (the whole-partition sort that timed the full parcel-panel rebuild out, measured 2026-10-22).

The window picked, per group, the row smallest under `orderBy(col.asc_nulls_first())`. `min` over a
struct of the same columns must pick the identical row: Spark orders struct fields nulls-first
ascending, so a null leading field is smallest, exactly as `asc_nulls_first` made it. This test holds
that equivalence against a literal `row_number()` reference over adversarial input — nulls in the
order columns, ties, empty-string keys, one and many rows per PNU — and then holds the two sections
that moved onto the helper (`build_transfer_history`, `build_land_rights`) to the same reference, so a
wrong key, order, entry struct or comparator cannot pass.
"""

from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class CollectSortedUniqueSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("collect-sorted-unique").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @staticmethod
    def _row_number_reference(frame, dedup_keys, order_by, entry, comparator):
        """The semantics `collect_sorted_unique` replaces: keep row 1 of an asc_nulls_first window
        per group, then array_sort the collected entries per PNU by the same comparator."""
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        dedup = Window.partitionBy(*dedup_keys).orderBy(*[column.asc_nulls_first() for column in order_by])
        unique = (
            frame.withColumn("_rn", F.row_number().over(dedup)).where(F.col("_rn") == 1).drop("_rn")
        )
        entries = unique.select("pnu", entry.alias("entry"))
        return entries.groupBy("pnu").agg(
            F.expr(f"array_sort(collect_list(entry), {comparator})").alias("sorted_entries")
        )

    @staticmethod
    def _as_dict(frame):
        return {row["pnu"]: [r.asDict(recursive=True) for r in row["sorted_entries"]] for row in frame.collect()}

    def test_helper_matches_the_window_over_adversarial_input(self):
        import parcel_panel_silver_to_gold as job
        from pyspark.sql import functions as F
        from pyspark.sql import types as T

        # k is a dedup key (empty string is a real value, ADR-0093); o1/o2 are the order columns with
        # nulls and ties; payload is carried out in the entry but takes no part in the pick.
        schema = T.StructType(
            [
                T.StructField("pnu", T.StringType()),
                T.StructField("k", T.StringType()),
                T.StructField("o1", T.StringType()),
                T.StructField("o2", T.IntegerType()),
                T.StructField("payload", T.StringType()),
            ]
        )
        rows = [
            # One PNU, one key group, order columns a total order including a null that must win.
            ("A", "k1", None, 1, "null-o1-wins"),
            ("A", "k1", "b", 2, "loser"),
            ("A", "k1", "a", 9, "loser"),
            # Second key group in the same PNU: two rows tie on o1, o2 breaks the tie nulls-first.
            ("A", "k2", "z", None, "null-o2-wins"),
            ("A", "k2", "z", 5, "loser"),
            # Empty-string key is its own group, single row.
            ("A", "", "m", 3, "empty-key"),
            # A different PNU, single row.
            ("B", "k1", "x", 7, "only"),
        ]
        frame = self.spark.createDataFrame(rows, schema)
        dedup_keys = ["pnu", "k"]
        order_by = [F.col("o1"), F.col("o2")]
        entry = F.struct(F.col("k"), F.col("o1"), F.col("o2"), F.col("payload"))
        # A total order on the surviving entries so the comparison is of the pick, not of a sort tie.
        comparator = """(l, r) -> CASE
            WHEN l.k < r.k THEN -1 WHEN l.k > r.k THEN 1
            WHEN l.payload < r.payload THEN -1 WHEN l.payload > r.payload THEN 1
            ELSE 0 END"""

        new = self._as_dict(job.collect_sorted_unique(frame, dedup_keys, order_by, entry, comparator))
        reference = self._as_dict(self._row_number_reference(frame, dedup_keys, order_by, entry, comparator))
        self.assertEqual(new, reference)
        # The null-first picks are the ones a NULLS LAST min would get wrong.
        picks = {(e["k"], e["payload"]) for e in new["A"]}
        self.assertIn(("k1", "null-o1-wins"), picks)
        self.assertIn(("k2", "null-o2-wins"), picks)
        self.assertNotIn(("k1", "loser"), picks)

    def test_build_transfer_history_matches_the_window(self):
        import parcel_panel_silver_to_gold as job
        from pyspark.sql import functions as F

        schema = (
            "pnu string, transfer_history_seq string, parcel_history_seq string, reason_code string, "
            "reason string, moved_at string, erased_at string, land_category string, area_m2 string, "
            "closure_seq string"
        )
        rows = [
            # Distinct identities on one PNU: newest moved_at first, null moved_at last.
            ("A", "2", "1", "10", "분할", "2020-01-01", None, "대", "100", "1"),
            ("A", "1", "1", "20", "합병", "2019-01-01", None, "전", "200", "1"),
            ("A", "3", "1", "30", "지목변경", None, None, "답", "300", "1"),
            # An exact duplicate identity (same three-part key, same payload): collapses to one.
            ("A", "1", "1", "20", "합병", "2019-01-01", None, "전", "200", "1"),
            # history_seq breaks a moved_at tie, DESC.
            ("A", "5", "1", "40", "분할", "2019-01-01", None, "대", "100", "1"),
            ("B", "1", "1", "50", "등록", "2021-06-01", None, "대", "10", "1"),
        ]
        frame = self.spark.createDataFrame(rows, schema)

        new = {r["pnu"]: r["transfer_history_json"] for r in job.build_transfer_history(frame).collect()}
        entry = F.struct(
            F.col("reason"),
            F.col("reason_code"),
            F.col("moved_at"),
            F.col("erased_at"),
            F.col("land_category"),
            F.col("area_m2"),
            F.col("transfer_history_seq").alias("history_seq"),
            F.col("parcel_history_seq"),
            F.col("closure_seq"),
        )
        comparator = """(l, r) -> CASE
            WHEN l.moved_at IS NULL AND r.moved_at IS NOT NULL THEN 1
            WHEN l.moved_at IS NOT NULL AND r.moved_at IS NULL THEN -1
            WHEN l.moved_at > r.moved_at THEN -1
            WHEN l.moved_at < r.moved_at THEN 1
            WHEN l.history_seq > r.history_seq THEN -1
            WHEN l.history_seq < r.history_seq THEN 1
            WHEN l.parcel_history_seq > r.parcel_history_seq THEN -1
            WHEN l.parcel_history_seq < r.parcel_history_seq THEN 1
            ELSE 0 END"""
        reference = self._row_number_reference(
            frame,
            ["pnu", "transfer_history_seq", "parcel_history_seq"],
            [F.col(c) for c in ("reason_code", "reason", "moved_at", "erased_at", "land_category", "area_m2", "closure_seq")],
            entry,
            comparator,
        )
        expected = {
            row["pnu"]: row["json"]
            for row in reference.select("pnu", F.to_json(F.col("sorted_entries")).alias("json")).collect()
        }
        self.assertEqual(new, expected)
        # The duplicate identity collapsed; the three distinct ones survived in moved_at/seq order.
        history = json.loads(new["A"])
        self.assertEqual([e["history_seq"] for e in history], ["2", "5", "1", "3"])

    def test_build_land_rights_matches_the_window_and_keeps_the_page_bound(self):
        import parcel_panel_silver_to_gold as job
        from pyspark.sql import functions as F

        schema = (
            "pnu string, right_serial_no string, building_name string, dong_name string, "
            "floor_name string, ho_name string, room_name string, right_ratio string, "
            "closure_kind_name string, closure_kind_code string"
        )
        # More distinct rights than the page bound on one PNU, plus null unit designations (ADR-0093
        # coalesces them to '') and an exact duplicate that must collapse.
        bound = job.LAND_RIGHT_PAGE_BOUND
        rows = []
        for i in range(bound + 3):
            rows.append(("A", f"{i:04d}", "빌딩", None, None, None, None, "1/2", "말소", "01"))
        rows.append(("A", "0000", "빌딩", None, None, None, None, "1/2", "말소", "01"))  # duplicate of i=0
        rows.append(("B", "0001", "집", "101", "3", "301", "", "1/1", None, None))
        frame = self.spark.createDataFrame(rows, schema)

        built = {r["pnu"]: r for r in job.build_land_rights(frame).collect()}
        self.assertEqual(built["A"]["land_right_total"], bound + 3)  # duplicate collapsed, rest counted
        self.assertEqual(len(json.loads(built["A"]["land_rights_json"])), bound)  # page bound honored

        normalized = frame.select(
            "pnu",
            "right_serial_no",
            "building_name",
            F.coalesce(F.col("dong_name"), F.lit("")).alias("dong_name"),
            F.coalesce(F.col("floor_name"), F.lit("")).alias("floor_name"),
            F.coalesce(F.col("ho_name"), F.lit("")).alias("ho_name"),
            F.coalesce(F.col("room_name"), F.lit("")).alias("room_name"),
            "right_ratio",
            F.col("closure_kind_name").alias("closure_kind"),
            "closure_kind_code",
        )
        entry = F.struct(
            F.col("right_serial_no"),
            F.col("building_name"),
            F.col("dong_name"),
            F.col("floor_name"),
            F.col("ho_name"),
            F.col("room_name"),
            F.col("right_ratio"),
            F.col("closure_kind"),
            F.col("closure_kind_code"),
        )
        comparator = """(l, r) -> CASE
            WHEN l.right_serial_no < r.right_serial_no THEN -1
            WHEN l.right_serial_no > r.right_serial_no THEN 1
            WHEN l.dong_name < r.dong_name THEN -1
            WHEN l.dong_name > r.dong_name THEN 1
            WHEN l.floor_name < r.floor_name THEN -1
            WHEN l.floor_name > r.floor_name THEN 1
            WHEN l.ho_name < r.ho_name THEN -1
            WHEN l.ho_name > r.ho_name THEN 1
            WHEN l.room_name < r.room_name THEN -1
            WHEN l.room_name > r.room_name THEN 1
            ELSE 0 END"""
        reference = self._row_number_reference(
            normalized,
            ["pnu", "right_serial_no", "dong_name", "floor_name", "ho_name", "room_name"],
            [F.col(c) for c in ("building_name", "right_ratio", "closure_kind_code", "closure_kind")],
            entry,
            comparator,
        )
        expected_total = {
            row["pnu"]: row["total"]
            for row in reference.select("pnu", F.size(F.col("sorted_entries")).alias("total")).collect()
        }
        expected_page = {
            row["pnu"]: row["json"]
            for row in reference.select(
                "pnu", F.to_json(F.slice(F.col("sorted_entries"), 1, bound)).alias("json")
            ).collect()
        }
        for pnu, row in built.items():
            self.assertEqual(row["land_right_total"], expected_total[pnu], pnu)
            self.assertEqual(json.loads(row["land_rights_json"]), json.loads(expected_page[pnu]), pnu)


if __name__ == "__main__":
    unittest.main()
