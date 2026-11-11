"""실제 Spark 에서 주소 → PNU 두 단계(root ADR-0280 §4)의 결과를 합성 행으로 확인한다.

PNU·주소는 지어낸 것이다(시도 99). 단계마다 거부해야 할 것을 심는다: 현재 필지에 없는 지번 PNU, 두 PNU 에
걸친 도로명 주소, 표제부에 없는 도로명 주소, 원래 이유가 있는 행.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

try:
    from pyspark.sql import SparkSession
    HAS_SPARK = hasattr(SparkSession, "builder")
except ImportError:
    HAS_SPARK = False

OFFERED = ("row_id int, jibun_pnu string, reason string, road_sido string, road_sigungu string, road_name string, "
           "road_main_no int, road_sub_no int")


@unittest.skipUnless(HAS_SPARK, "requires the pinned Spark runtime")
class ResolveTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import address_pnu
        cls.address_pnu = address_pnu
        cls.spark = (SparkSession.builder.master("local[1]").appName("synthetic-address-pnu")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_each_row_names_the_step_that_found_its_pnu_or_why_none_did(self):
        offered = self.spark.createDataFrame([
            (1, "9999910100100070000", None, None, None, None, None, None),   # jibun, parcel held
            (2, "9999910100100080000", None, None, None, None, None, None),   # jibun, parcel absent
            (3, None, None, "99", "가상시", "가나대로", 3, 0),                    # road, one PNU
            (4, None, None, "99", "가상시", "다라로", 5, 1),                      # road, two PNUs
            (5, None, None, "99", "가상시", "없는로", 9, 0),                      # road, no title
            (6, None, "no_legal_dong", None, None, None, None, None),          # reason kept
        ], OFFERED)
        parcels = self.spark.createDataFrame([("9999910100100070000",)], "pnu string")
        titles = self.spark.createDataFrame([
            ("9999910100100010000", "합성도 가상시 가나대로 3 (가나동)"),
            ("9999910100100010000", "합성도 가상시 가나대로 3"),
            ("9999910100100020000", "합성도 가상시 다라로 5-1 (가나동)"),
            ("9999910100100030000", "합성도 가상시 다라로 5-1"),
            (None, "합성도 가상시 없는로 9"),
        ], "pnu string, road_address_raw string")
        rows = {row.row_id: row for row in self.address_pnu.resolve(offered, parcels, titles).collect()}
        self.assertEqual((rows[1].pnu, rows[1].pnu_basis, rows[1].pnu_candidate_count), ("9999910100100070000", "jibun_address", 1))
        self.assertEqual((rows[2].pnu, rows[2].pnu_unresolved_reason), (None, "parcel_absent"))
        self.assertEqual((rows[3].pnu, rows[3].pnu_basis), ("9999910100100010000", "road_address"))
        self.assertEqual((rows[4].pnu, rows[4].pnu_unresolved_reason, rows[4].pnu_candidate_count), (None, "road_ambiguous", 2))
        self.assertEqual((rows[5].pnu, rows[5].pnu_unresolved_reason, rows[5].pnu_candidate_count), (None, "road_not_found", 0))
        self.assertEqual((rows[6].pnu, rows[6].pnu_unresolved_reason), (None, "no_legal_dong"))
        for row in rows.values():
            self.assertEqual(row.pnu is None, row.pnu_basis is None)
            self.assertEqual(row.pnu is None, row.pnu_unresolved_reason is not None)

    def test_the_juso_road_places_a_road_its_sigungu_codes_name_once_after_the_jibun(self):
        """Root ADR-0495: between the jibun and the register, the 주소DB road; its 시군구 is matched by code
        (a former name through its pair), and two PNUs, an unserved PNU or another 시군구 do not place."""

        offered = self.spark.createDataFrame([
            (1, None, None, "99", "가상시", "가나대로", 3, 0),       # one address, served: juso
            (2, None, None, "99", "옛구", "새길", 7, 0),            # former 시군구 name, its successor's road
            (3, None, None, "99", "가상시", "겹로", 1, 0),          # two addresses, two PNUs
            (4, None, None, "99", "가상시", "먼로", 2, 0),          # its PNU not served
            (5, None, None, "99", "다른시", "가나대로", 3, 0),       # same road number in another 시군구
            (6, "9999910100100070000", None, "99", "가상시", "가나대로", 3, 0),   # the jibun wins
            (7, None, None, "99", "", "홀로", 4, 0),                # no 시군구 word, one in the 시도
        ], OFFERED)
        parcels = self.spark.createDataFrame([("9999910100100070000",), ("9999910100100010000",),
                                              ("9999920100100020000",), ("9999910100100050000",),
                                              ("9999910100100060000",)], "pnu string")
        titles = self.spark.createDataFrame([], "pnu string, road_address_raw string")
        roads = self.spark.createDataFrame([
            ("99101", "가나대로", 3, 0, "9999910100100010000"),
            ("99201", "새길", 7, 0, "9999920100100020000"),
            ("99101", "겹로", 1, 0, "9999910100100050000"),
            ("99101", "겹로", 1, 0, "9999910100100060000"),
            ("99101", "먼로", 2, 0, "9999910100100099999"),
            ("99101", "홀로", 4, 0, "9999910100100060000"),
        ], "sigungu_code string, road_name string, building_main_no int, building_sub_no int, pnu string")
        index = self.address_pnu.sigungu_index(
            [("9910100000", "합성도 가상시"), ("9920100000", "합성도 새구"), ("9930100000", "합성도 다른시")],
            [("9919900000", "합성도 옛구")], [("99199", "99201")])
        sigungus = self.spark.createDataFrame([(s, w, c) for (s, w), codes in index.items() for c in codes],
                                              "road_sido string, road_sigungu string, sigungu_code string")
        juso = self.address_pnu.JusoRoads(roads, sigungus)
        rows = {row.row_id: row for row in self.address_pnu.resolve(offered, parcels, titles, None, juso).collect()}
        self.assertEqual((rows[1].pnu, rows[1].pnu_basis), ("9999910100100010000", "juso_road_address"))
        self.assertEqual((rows[2].pnu, rows[2].pnu_basis), ("9999920100100020000", "juso_road_address"))
        self.assertEqual((rows[3].pnu, rows[3].pnu_unresolved_reason, rows[3].pnu_candidate_count),
                         (None, "road_ambiguous", 2))
        self.assertEqual((rows[4].pnu, rows[4].pnu_unresolved_reason), (None, "road_not_found"))
        self.assertEqual((rows[5].pnu, rows[5].pnu_unresolved_reason), (None, "road_not_found"))
        self.assertEqual((rows[6].pnu, rows[6].pnu_basis), ("9999910100100070000", "jibun_address"))
        self.assertEqual((rows[7].pnu, rows[7].pnu_basis), ("9999910100100060000", "juso_road_address"))
        for row in rows.values():
            self.assertEqual(row.pnu is None, row.pnu_basis is None)
            self.assertEqual(row.pnu is None, row.pnu_unresolved_reason is not None)

    # Root ADR-0482 §1. Synthetic: 99999102 dongs became 99999202 on a pair the served edition predates, so
    # the served parcel still carries the old code. 99999103 and 99999104 merged into 99999203; both had lot 7.
    REMAP = {"9999910200": "9999920200", "9999910300": "9999920300", "9999910400": "9999920300"}

    def test_a_jibun_the_served_edition_holds_under_its_old_code_is_placed_there(self):
        offered = self.spark.createDataFrame([
            (1, "9999920200100070000", None, None, None, None, None, None),   # current names, served old
            (2, "9999920300100070000", None, None, None, None, None, None),   # merged: two old forms hold it
        ], OFFERED)
        parcels = self.spark.createDataFrame([("9999910200100070000",), ("9999910300100070000",),
                                              ("9999910400100070000",)], "pnu string")
        titles = self.spark.createDataFrame([], "pnu string, road_address_raw string")
        rows = {row.row_id: row for row in self.address_pnu.resolve(offered, parcels, titles, self.REMAP).collect()}
        self.assertEqual((rows[1].pnu, rows[1].pnu_basis), ("9999910200100070000", "jibun_address"))
        self.assertEqual((rows[2].pnu, rows[2].pnu_unresolved_reason), (None, "parcel_absent"))
        # Without the pairs the served form is never asked: the old behaviour.
        plain = {row.row_id: row for row in self.address_pnu.resolve(offered, parcels, titles).collect()}
        self.assertIsNone(plain[1].pnu)

    def test_the_code_path_places_on_the_served_form_and_grades_it(self):
        import place_identity_resolver as resolver

        staged = self.spark.createDataFrame([
            ("a", "9999910200100070000"),   # the source gave the old code: served verbatim
            ("b", "9999920200100070000"),   # the source gave the new code: served under the old
            ("c", "9999920200100080000"),   # no form served
            ("d", "9999920300100070000"),   # two old forms served: a steward's
            ("e", "9999930100100010000"),   # no pair, served as it is
        ], "k string, pnu string")
        parcels = self.spark.createDataFrame([("9999910200100070000",), ("9999910300100070000",),
                                              ("9999910400100070000",), ("9999930100100010000",)], "pnu string")
        rows = {row.k: row for row in resolver.resolve(staged, self.REMAP, set(), parcels).collect()}
        self.assertEqual((rows["a"].pnu, rows["a"].place_identity_grade), ("9999910200100070000", "code_served_edition"))
        self.assertEqual((rows["b"].pnu, rows["b"].place_identity_grade), ("9999910200100070000", "code_served_edition"))
        self.assertEqual((rows["c"].pnu, rows["c"].pnu_unresolved_reason), (None, "parcel_absent"))
        self.assertEqual((rows["d"].pnu, rows["d"].pnu_unresolved_reason), (None, "code_subdivision_ambiguous"))
        self.assertEqual((rows["e"].pnu, rows["e"].place_identity_grade), ("9999930100100010000", "code_exact"))

    def test_the_current_form_wins_once_the_served_edition_holds_it(self):
        import place_identity_resolver as resolver

        staged = self.spark.createDataFrame([("a", "9999910200100070000")], "k string, pnu string")
        parcels = self.spark.createDataFrame([("9999910200100070000",), ("9999920200100070000",)], "pnu string")
        row = resolver.resolve(staged, self.REMAP, set(), parcels).collect()[0]
        self.assertEqual((row.pnu, row.place_identity_grade), ("9999920200100070000", "code_superseded"))

    def test_a_gold_source_not_served_gets_its_served_form_as_a_candidate(self):
        import place_identity_resolver as resolver

        served = self.spark.createDataFrame([("9999910200100070000",), ("9999930100100010000",)], "pnu string")
        lineage = self.spark.createDataFrame([("9999999999100010000", "9999920200100090000", 1, "split:x")],
                                             "successor_pnu string, source_pnu string, hop int, path string")
        rows = resolver.served_form_candidates(
            self.spark, ["9999920200100070000", "9999930100100010000", "9999920200100090000", None],
            self.REMAP, served, lineage).collect()
        self.assertEqual([(r.source_pnu, r.successor_pnu, r.path) for r in rows],
                         [("9999920200100070000", "9999910200100070000", "code_served_edition")])


@unittest.skipUnless(HAS_SPARK, "requires the pinned Spark runtime")
class OnbidPlacementTest(unittest.TestCase):
    """Root ADR-0482 §2: 공매 is placed by its code first, else by its item name's address."""

    @classmethod
    def setUpClass(cls):
        import data_go_kr_onbid
        cls.job = data_go_kr_onbid
        cls.spark = (SparkSession.builder.master("local[1]").appName("synthetic-onbid-placement")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_code_first_then_jibun_then_road_and_each_says_how(self):
        rows = [
            {"k": "a", "pnu": "9999910100100070000", "item_name": "합성도 가상시 가나동 8"},   # code wins
            {"k": "b", "pnu": None, "item_name": "합성도 가상시 가나동 8, 101호 근린생활시설"},
            {"k": "c", "pnu": None, "item_name": "합성도 가상시 가나대로 3 상가"},
            {"k": "d", "pnu": None, "item_name": "합성도 가상시 가나동 9구역9블록"},             # prose
            {"k": "e", "pnu": "9999910100100090000", "item_name": "합성도 가상시 가나동 9"},     # neither held
        ]
        frame = self.spark.createDataFrame(
            [(r["k"], r["pnu"], r["item_name"], None, None, n) for n, r in enumerate(rows)],
            "k string, pnu string, item_name string, pnu_basis string, pnu_unresolved_reason string, _row int")
        prefixes = {"합성도": "99", "합성": "99"}
        dongs = {("99", "가상시 가나동"): "9999910100"}
        parcels = self.spark.createDataFrame([("9999910100100070000",), ("9999910100100080000",)], "pnu string")
        titles = self.spark.createDataFrame([("9999910100100070000", "합성도 가상시 가나대로 3")],
                                            "pnu string, road_address_raw string")
        out = {r.k: r for r in self.job.placed_items(frame, rows, {}, set(), parcels, titles, prefixes, dongs).collect()}
        self.assertEqual((out["a"].pnu, out["a"].pnu_basis), ("9999910100100070000", "legal_dong_code"))
        self.assertEqual((out["b"].pnu, out["b"].pnu_basis), ("9999910100100080000", "jibun_address"))
        self.assertEqual((out["c"].pnu, out["c"].pnu_basis), ("9999910100100070000", "road_address"))
        self.assertEqual((out["d"].pnu, out["d"].pnu_unresolved_reason), (None, "no_source_code"))
        self.assertEqual((out["e"].pnu, out["e"].pnu_unresolved_reason), (None, "parcel_absent"))
        for row in out.values():
            self.assertEqual(row.pnu is None, row.pnu_basis is None)
            self.assertEqual(row.pnu is None, row.pnu_unresolved_reason is not None)


if __name__ == "__main__":
    unittest.main()
