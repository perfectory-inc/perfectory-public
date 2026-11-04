"""가린 지번의 필지 추정(root ADR-0351)을 합성 자료로 본다.

PNU 는 합성 시도 99 를 쓴다. 관문마다 거부해야 할 것을 심는다. 숫자를 남기지 않은 가림, 후보 둘, 후보 없음, 대장 둘이
엇갈리는 필지, 소유권 변동이 창 밖인 필지, 계약이 기준 아래로 잰 수준이다.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from datetime import date
from pathlib import Path

JOBS = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS))

import place_identity_resolver as r  # noqa: E402

CONTRACT = json.loads((JOBS.parents[1] / "contracts" / "rt-molit-real-transaction.contract.json").read_text(encoding="utf-8"))
INFERENCE = CONTRACT["place_inference"]


def has_pyspark() -> bool:
    try:
        return importlib.util.find_spec("pyspark") is not None
    except ValueError:
        return False


class MaskedLots(unittest.TestCase):
    def test_a_masked_lot_names_its_range_by_the_digits_kept(self) -> None:
        self.assertEqual(r.masked_lot_range("3**"), (False, 300, 399))
        self.assertEqual(r.masked_lot_range("1*"), (False, 10, 19))
        self.assertEqual(r.masked_lot_range("산1**"), (True, 100, 199))
        self.assertEqual(r.masked_lot_range("산 4*"), (True, 40, 49))
        # Nothing kept, or nothing masked: no range to search.
        self.assertIsNone(r.masked_lot_range("**"))
        self.assertIsNone(r.masked_lot_range("12-3"))
        self.assertIsNone(r.masked_lot_range(""))

    def test_zones_read_in_the_land_registers_words(self) -> None:
        self.assertEqual(r.zone_name("계획관리"), "계획관리지역")
        self.assertEqual(r.zone_name("일반상업"), "일반상업지역")
        self.assertEqual(r.zone_name("개발제한구역"), "개발제한구역")
        self.assertIsNone(r.zone_name(" "))

    def test_one_candidate_links_and_several_or_none_say_why(self) -> None:
        self.assertEqual(r.unique_pick(["9999933021103000000"]), ("9999933021103000000", None, 1))
        self.assertEqual(r.unique_pick(["a", "b", "a"]), (None, "inference_ambiguous", 2))
        self.assertEqual(r.unique_pick([]), (None, "inference_no_candidate", 0))


class TheBarIsInTheContract(unittest.TestCase):
    def test_the_bar_is_ninety_nine_percent(self) -> None:
        self.assertGreaterEqual(float(INFERENCE["precision_bar"]), 0.99)

    def test_a_level_measured_under_the_bar_is_not_used(self) -> None:
        inference = {"precision_bar": 0.99, "min_measured_rows": 200, "levels": [
            {"grade": "inferred_ownership_change", "evidence": ["lot_range", "ownership_change"],
             "measured": {"real_transaction_land_trade": {"precision": 0.985, "precision_wilson_low": 0.98, "rows": 5000},
                          # Over the bar as a point, under it at the interval's low end: not used (ADR-0370).
                          "real_transaction_commercial_trade": {"precision": 0.9908, "precision_wilson_low": 0.9894,
                                                                "rows": 18778}}},
            {"grade": "inferred_attributes", "evidence": ["lot_range", "land_area", "registers_agree"],
             "measured": {"real_transaction_land_trade": {"precision": 0.995, "precision_wilson_low": 0.99, "rows": 150},
                          "real_transaction_industrial_trade": {"precision": 0.995, "precision_wilson_low": 0.991,
                                                                "rows": 900}}},
        ]}
        self.assertEqual(r.enabled_levels(inference, "real_transaction_land_trade"), [])
        self.assertEqual([lv["grade"] for lv in r.enabled_levels(inference, "real_transaction_industrial_trade")],
                         ["inferred_attributes"])
        self.assertEqual(r.enabled_levels(inference, "real_transaction_commercial_trade"), [])

    def test_the_ownership_cause_is_the_files_own_word(self) -> None:
        # 2026-11-03: AL_D161 writes 소유권이전 for a sale; 매매 is not one of its causes (ADR-0370).
        self.assertEqual(INFERENCE["ownership_causes"], ["소유권이전"])

    def test_a_level_naming_unknown_evidence_is_refused(self) -> None:
        bad = {"precision_bar": 0.99, "min_measured_rows": 1,
               "levels": [{"grade": "inferred_attributes", "evidence": ["lot_range", "guess"]}]}
        with self.assertRaises(ValueError):
            r.enabled_levels(bad, "real_transaction_land_trade")

    def test_the_main_lot_group_level_is_on_only_where_its_lower_bound_clears_the_bar(self) -> None:
        # 2026-11-04 (ADR-0405): the 본번 group's Wilson low is 99.53 / 99.08 / 99.33%; 토지 is unmeasured.
        on = {dataset: [lv["grade"] for lv in r.enabled_levels(INFERENCE, dataset)]
              for dataset in ("real_transaction_industrial_trade", "real_transaction_commercial_trade",
                              "real_transaction_detached_house_trade", "real_transaction_land_trade")}
        self.assertEqual(on, {"real_transaction_industrial_trade": [r.MAIN_LOT_GROUP],
                              "real_transaction_commercial_trade": [r.MAIN_LOT_GROUP],
                              "real_transaction_detached_house_trade": [r.MAIN_LOT_GROUP],
                              "real_transaction_land_trade": []})
        group = next(lv for lv in INFERENCE["levels"] if lv["grade"] == r.MAIN_LOT_GROUP)
        ownership = next(lv for lv in INFERENCE["levels"] if lv["grade"] == "inferred_ownership_change")
        # The same evidence as the ownership level: only the claim of the link differs.
        self.assertEqual(group["evidence"], ownership["evidence"])
        for dataset, measured in group["measured"].items():
            check = INFERENCE["evidence_checks"][dataset]["lot_group"]
            self.assertEqual((measured["rows"], measured["right"], measured["precision_wilson_low"]),
                             (check["rows"], check["right"], check["precision_wilson_low"]), dataset)

    def test_every_enabled_level_carries_its_measurement(self) -> None:
        for level in INFERENCE["levels"]:
            for dataset, measured in (level.get("measured") or {}).items():
                for field in ("precision", "precision_wilson_low", "rows", "coverage", "ambiguous", "method", "measured_on"):
                    self.assertIn(field, measured, f"{level['grade']} {dataset}")


@unittest.skipUnless(has_pyspark(), "requires pyspark")
class InfersInSpark(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from pyspark.sql import SparkSession

        cls.spark = (SparkSession.builder.master("local[1]").appName("masked-lot-inference")
                     .config("spark.sql.shuffle.partitions", "1").getOrCreate())

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def frames(self):
        deals = self.spark.createDataFrame([
            # 1: one parcel fits on both registers.
            (1, "9999933021", False, 300, 399, "계획관리지역", None, 2050.0, 2007, 684.0, date(2098, 1, 10)),
            # 2: two parcels share the area and the building facts.
            (2, "9999933021", False, 100, 199, "계획관리지역", None, 500.0, 2010, 200.0, date(2098, 1, 10)),
            # 3: the land register names one parcel, the building register another.
            (3, "9999933021", False, 400, 499, "계획관리지역", None, 800.0, 2015, 300.0, date(2098, 1, 10)),
            # 4: nothing in range.
            (4, "9999933021", False, 900, 999, "계획관리지역", None, 1.0, 2000, 1.0, date(2098, 1, 10)),
        ], "row_id int, legal_dong_code string, lot_mountain boolean, lot_low int, lot_high int, zone string, "
           "jimok string, land_area_m2 double, building_year int, floor_area_m2 double, deal_date date")
        parcels = self.spark.createDataFrame([
            ("9999933021103010000", "공장용지", 2050.0, "계획관리지역"),
            ("9999933021103020000", "공장용지", 1000.0, "계획관리지역"),
            ("9999933021101010000", "공장용지", 500.0, "계획관리지역"),
            ("9999933021101020000", "공장용지", 500.0, "계획관리지역"),
            # 102-1: the same 본번 as 102, another area (the evidence never picks it).
            ("9999933021101020001", "공장용지", 900.0, "계획관리지역"),
            ("9999933021104010000", "공장용지", 800.0, "계획관리지역"),
            ("9999933021104020000", "공장용지", 700.0, "계획관리지역"),
        ], "pnu string, land_category string, area_m2 double, zone_name_1 string")
        titles = self.spark.createDataFrame([
            ("9999933021103010000", 2007, 684.0),
            ("9999933021101010000", 2010, 200.0),
            ("9999933021101020000", 2010, 200.0),
            ("9999933021104020000", 2015, 300.0),
        ], "pnu string, approval_year int, floor_area_m2 double")
        ownership = self.spark.createDataFrame([
            ("9999933021101020000", "2098-02-20", "소유권이전"),
            ("9999933021101010000", "2099-06-01", "소유권이전"),
        ], "pnu string, ownership_changed_on string, ownership_change_reason string")
        return deals, parcels, titles, ownership

    def run_levels(self, levels, ownership=True, group_parcels=None):
        deals, parcels, titles, owners = self.frames()
        out = r.infer_masked(deals, parcels, titles, owners if ownership else None, levels, 0.05, (0, 120),
                             INFERENCE["ownership_causes"], group_parcels)
        return {row["row_id"]: row.asDict() for row in out.collect()}

    def test_registers_must_agree_on_one_parcel(self) -> None:
        level = {"grade": "inferred_attributes", "evidence": ["lot_range", "zone", "land_area", "registers_agree"]}
        got = self.run_levels([level])
        self.assertEqual((got[1]["pnu"], got[1]["place_identity_grade"]), ("9999933021103010000", "inferred_attributes"))
        self.assertEqual((got[2]["pnu"], got[2]["pnu_unresolved_reason"]), (None, "inference_ambiguous"))
        self.assertIsNone(got[3]["pnu"])  # land says 104-01, building says 104-02
        self.assertEqual((got[4]["pnu"], got[4]["pnu_unresolved_reason"]), (None, "inference_no_candidate"))

    def test_an_ownership_change_in_the_window_breaks_a_tie(self) -> None:
        levels = [{"grade": "inferred_ownership_change", "evidence": ["lot_range", "zone", "land_area", "ownership_change"]},
                  {"grade": "inferred_attributes", "evidence": ["lot_range", "zone", "land_area", "registers_agree"]}]
        got = self.run_levels(levels)
        # 101-02 changed hands 41 days after the contract; 101-01 more than a year later.
        self.assertEqual((got[2]["pnu"], got[2]["place_identity_grade"]), ("9999933021101020000", "inferred_ownership_change"))
        self.assertEqual(got[1]["place_identity_grade"], "inferred_attributes")
        # Without the ownership Silver, that level links nothing and the tie stays.
        self.assertIsNone(self.run_levels(levels, ownership=False)[2]["pnu"])

    def test_the_group_level_links_a_deal_to_every_parcel_of_its_main_lot(self) -> None:
        level = {"grade": r.MAIN_LOT_GROUP, "evidence": ["lot_range", "zone", "land_area", "ownership_change"]}
        got = self.run_levels([level])
        # The evidence leaves 101-02 (102); the deal claims the 본번 102 group, not one PNU.
        self.assertEqual((got[2]["pnu"], got[2]["place_identity_grade"], got[2]["pnu_unresolved_reason"]),
                         (None, r.MAIN_LOT_GROUP, None))
        self.assertEqual(got[2]["pnu_group"], ["9999933021101020000", "9999933021101020001"])
        # Evidence leaving no parcel links no group.
        self.assertEqual((got[4]["pnu_group"], got[4]["pnu_unresolved_reason"]), (None, "inference_no_candidate"))

    def test_the_group_is_drawn_from_the_served_parcels(self) -> None:
        level = {"grade": r.MAIN_LOT_GROUP, "evidence": ["lot_range", "zone", "land_area", "ownership_change"]}
        served = self.spark.createDataFrame([("9999933021101020000",), ("9999933021101020002",),
                                             ("9999933021201020000",)], "pnu string")
        # 102-2 has no land row but is a current parcel; 산 102 is another group.
        got = self.run_levels([level], group_parcels=served)
        self.assertEqual(got[2]["pnu_group"], ["9999933021101020000", "9999933021101020002"])

    def test_a_single_parcel_level_still_comes_before_the_group(self) -> None:
        levels = [{"grade": "inferred_ownership_change", "evidence": ["lot_range", "zone", "land_area", "ownership_change"]},
                  {"grade": r.MAIN_LOT_GROUP, "evidence": ["lot_range", "zone", "land_area", "ownership_change"]}]
        got = self.run_levels(levels)
        self.assertEqual((got[2]["pnu"], got[2]["pnu_group"]), ("9999933021101020000", None))


if __name__ == "__main__":
    unittest.main()
