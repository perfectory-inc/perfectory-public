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
            (1, "9911010100100070000", None, None, None, None, None, None),   # jibun, parcel held
            (2, "9911010100100080000", None, None, None, None, None, None),   # jibun, parcel absent
            (3, None, None, "99", "가상시", "가나대로", 3, 0),                    # road, one PNU
            (4, None, None, "99", "가상시", "다라로", 5, 1),                      # road, two PNUs
            (5, None, None, "99", "가상시", "없는로", 9, 0),                      # road, no title
            (6, None, "no_legal_dong", None, None, None, None, None),          # reason kept
        ], OFFERED)
        parcels = self.spark.createDataFrame([("9911010100100070000",)], "pnu string")
        titles = self.spark.createDataFrame([
            ("9911010100100010000", "합성도 가상시 가나대로 3 (가나동)"),
            ("9911010100100010000", "합성도 가상시 가나대로 3"),
            ("9911010100100020000", "합성도 가상시 다라로 5-1 (가나동)"),
            ("9911010100100030000", "합성도 가상시 다라로 5-1"),
            (None, "합성도 가상시 없는로 9"),
        ], "pnu string, road_address_raw string")
        rows = {row.row_id: row for row in self.address_pnu.resolve(offered, parcels, titles).collect()}
        self.assertEqual((rows[1].pnu, rows[1].pnu_basis, rows[1].pnu_candidate_count), ("9911010100100070000", "jibun_address", 1))
        self.assertEqual((rows[2].pnu, rows[2].pnu_unresolved_reason), (None, "parcel_absent"))
        self.assertEqual((rows[3].pnu, rows[3].pnu_basis), ("9911010100100010000", "road_address"))
        self.assertEqual((rows[4].pnu, rows[4].pnu_unresolved_reason, rows[4].pnu_candidate_count), (None, "road_ambiguous", 2))
        self.assertEqual((rows[5].pnu, rows[5].pnu_unresolved_reason, rows[5].pnu_candidate_count), (None, "road_not_found", 0))
        self.assertEqual((rows[6].pnu, rows[6].pnu_unresolved_reason), (None, "no_legal_dong"))
        for row in rows.values():
            self.assertEqual(row.pnu is None, row.pnu_basis is None)
            self.assertEqual(row.pnu is None, row.pnu_unresolved_reason is not None)


if __name__ == "__main__":
    unittest.main()
