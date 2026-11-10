"""실제 Spark 에서 필지 단위 place_id 발행(root ADR-0481)을 합성 행으로 확인한다.

PNU 는 99999 대역의 지어낸 것이다. 지역은 처음 등록된 PNU 의 시도(앞 두 자리)라 모두 99 여서, 지역이 둘인
시험은 둘째 지역의 열을 직접 붙인다.
"""

import datetime
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

try:
    from pyspark.sql import SparkSession
    HAS_SPARK = hasattr(SparkSession, "builder")
except ImportError:
    HAS_SPARK = False

ROWS = "parcel_id string, pnu string, status string, valid_from string, valid_to string, redirect_to string, recorded_at_utc timestamp"
T1 = datetime.datetime(2026, 6, 1)
T2 = datetime.datetime(2026, 9, 1)


@unittest.skipUnless(HAS_SPARK, "requires the pinned Spark runtime")
class ParcelPlaceIdReleaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import parcel_place_id_release
        cls.release = parcel_place_id_release
        cls.spark = (SparkSession.builder.master("local[1]").appName("synthetic-parcel-place-id")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def rows(self):
        return self.release.with_scope(self.spark.createDataFrame([
            ("a", "9999910200100070000", "current", "2026-06-01", None, None, T1),     # kept
            ("b", "9999910200100080000", "current", "2026-06-01", None, None, T1),     # renumbered
            ("b", "9999910200100080000", "historic", "2026-06-01", "2026-09-01", None, T2),
            ("b", "9999910200100090000", "current", "2026-09-01", None, None, T2),
            ("c", "9999910300100070000", "current", "2026-06-01", None, None, T1),     # merge sibling
            ("d", "9999910400100070000", "current", "2026-06-01", None, None, T1),     # merge sibling
            ("e", "9999910500100010000", "current", "2026-06-01", None, None, T1),     # redirected
            ("e", "9999910500100010000", "redirected", "2026-09-01", None, "a", T2),
        ], ROWS))

    REMAP = {"9999910200": "9999920200", "9999910300": "9999920300", "9999910400": "9999920300"}

    def test_the_registry_is_each_ids_newest_row(self):
        registry = {r.place_id: r for r in self.release.registry_of(self.rows()).collect()}
        self.assertEqual({r.scope for r in registry.values()}, {"99"})
        self.assertEqual((registry["b"].status, registry["b"].current_code), ("current", "9999910200100090000"))
        self.assertEqual((registry["e"].status, registry["e"].current_code, registry["e"].redirect_to),
                         ("redirected", None, "a"))

    def test_the_bridge_closes_a_period_and_carries_an_open_pnu_under_its_current_code(self):
        bridge = {(r.place_id, r.code): r for r in self.release.bridge_of(self.rows(), self.REMAP).collect()}
        self.assertEqual(bridge[("b", "9999910200100080000")].valid_to, "2026-09-01")
        self.assertIsNone(bridge[("b", "9999910200100090000")].valid_to)
        # The new code of an open PNU: the same id, marked as recovered from the pair.
        carried = bridge[("a", "9999920200100070000")]
        self.assertEqual((carried.valid_from, carried.valid_to), (self.release.PAIR_SOURCE, self.release.PAIR_SOURCE))
        self.assertIn(("b", "9999920200100090000"), bridge)
        self.assertNotIn(("b", "9999920200100080000"), bridge)  # a closed period is not carried
        # Two merged old dongs with the same lot would give one new code two ids: neither is bridged.
        self.assertNotIn("9999920300100070000", {code for _, code in bridge})

    def test_the_changelog_names_what_moved_against_the_published_registry(self):
        published = self.spark.createDataFrame([
            ("99", "a", "current", "9999910200100070000", None),
            ("99", "b", "current", "9999910200100080000", None),
            ("99", "e", "current", "9999910500100010000", None),
            ("99", "z", "current", "9999910900100010000", None),
        ], "scope string, place_id string, status string, current_code string, redirect_to string")
        changes = {r.place_id: r for r in self.release.changelog_of(
            published, self.release.registry_of(self.rows())).collect()}
        self.assertEqual(changes["b"].change, "renumbered")
        self.assertEqual((changes["e"].change, changes["e"].redirect_to), ("redirected", "a"))
        self.assertEqual(changes["c"].change, "added")
        self.assertEqual(changes["z"].change, "retired")
        self.assertNotIn("a", changes)

    def test_only_regions_whose_inputs_moved_are_stale(self):
        from pyspark.sql import functions as F

        other = self.spark.createDataFrame([("x", "9999990100100010000", "current", "2026-06-01", None, None, T1)], ROWS)
        rows = self.rows().unionByName(other.withColumn("scope", F.lit("98")))  # another region, synthetic
        published = self.spark.createDataFrame([("99", datetime.datetime(2026, 10, 1)),
                                                ("98", datetime.datetime(2026, 10, 1))],
                                               "scope string, published_at_utc timestamp")
        self.assertEqual(self.release.stale_scopes(rows, published, set()), [])
        self.assertEqual(self.release.stale_scopes(rows, published, {"99"}), [("99", 5)])
        never = self.spark.createDataFrame([("99", datetime.datetime(2026, 10, 1))], "scope string, published_at_utc timestamp")
        self.assertEqual(self.release.stale_scopes(rows, never, set()), [("98", 1)])
        early = self.spark.createDataFrame([("99", datetime.datetime(2026, 7, 1)), ("98", datetime.datetime(2026, 10, 1))],
                                           "scope string, published_at_utc timestamp")
        self.assertEqual(self.release.stale_scopes(rows, early, set()), [("99", 5)])

    def test_a_code_change_moves_only_the_regions_holding_a_pnu_under_it(self):
        rows = self.rows()
        self.assertEqual(self.release.code_moved_scopes(rows, {"9999910200"}), {"99"})
        self.assertEqual(self.release.code_moved_scopes(rows, {"9999970000"}), set())
        self.assertEqual(self.release.code_moved_scopes(rows, set()), set())
        self.assertEqual(self.release.code_moved_scopes(rows, None), {"99"})


if __name__ == "__main__":
    unittest.main()
