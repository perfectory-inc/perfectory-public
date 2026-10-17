"""The section coverage bound against real Spark frames (root ADR-0272).

`measure` counts a section non-empty only when its column holds a value the panel does not write for
an absent section, and the full build refuses before it returns an outcome to write. The keys are
not PNUs: only their first two characters, the region `measure` groups by, matter here, and the
repository's reserved PNU range (99999…) has one region.
"""
import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

ROWS = [("97-a", '[{"zone":"a"}]', "[]"), ("97-b", "[]", None), ("97-c", None, '{"price":1}'),
        ("98-a", '[{"zone":"b"}]', " [] ")]
SECTIONS = {"zonings": "zonings_json", "price": "price_json"}


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class SectionCoverageSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("section-coverage").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, rows=ROWS):
        return self.spark.createDataFrame(rows, "pnu string, zonings_json string, price_json string")

    def test_measure_counts_present_sections_per_region(self):
        import section_coverage as coverage

        self.assertEqual(coverage.measure(self.frame(), SECTIONS), {
            ("zonings", "97"): (1, 3), ("price", "97"): (1, 3),
            ("zonings", "98"): (1, 1), ("price", "98"): (0, 1),
        })

    def test_the_full_build_refuses_a_section_emptied_for_one_region_before_it_returns(self):
        import gold_incremental as incremental
        import section_coverage as coverage

        served = {("zonings", "97"): (1_000, 3_000), ("zonings", "98"): (1_000, 1_000)}
        guard = coverage.Guard("gold.x", lambda frame: coverage.measure(frame, SECTIONS), served, [],
                               coverage.Bound(0.05, 1, shares=True))
        built = self.frame([(f"98-{n}", "[]", None) for n in range(10)])
        lane = incremental.Lane(build=lambda frames: built, restrict=None, affected=None, validate=None,
                                content_columns=("zonings_json",), gold_columns=("pnu", "zonings_json", "price_json"))
        args = SimpleNamespace(expected_count=None, minimum_count=None)
        with self.assertRaisesRegex(ValueError, "zonings in 시도 98"):
            incremental.build_or_merge(self.spark, args, lane, {}, {}, None, "gold.x",
                                       lambda gold, expected: (gold.count(), {}), lambda count, minimum: None, guard)


if __name__ == "__main__":
    unittest.main()
