"""주소에서 PNU 찾기(root ADR-0280 §4)의 판독부를 합성 지명으로 본다.

지명은 지어낸 것이다(합성도·가상시 등, 시도 코드 99). 각 관문은 거부해야 할 것을 심어서 확인한다: 도로명 뒤
숫자를 지번으로 읽지 않기, 두 코드가 나눠 가진 이름은 고르지 않기, 지번이 9999 를 넘으면 PNU 를 만들지 않기,
구가 빠진 주소는 구 없는 이름이 하나일 때만 찾기.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import address_pnu as a  # noqa: E402

PREFIXES = {"$comment": "synthetic", "합성": "99", "합성특별": "98"}
DONGS = a.legal_dong_index([
    ("9999910100", "합성도 가상시 가나동"),
    ("9999925021", "합성도 가상시 다라면 마바리"),
    ("9999931101", "합성도 새시 동녘구 사아동"),
    ("9999950101", "합성도 겹시 같은동"),
    ("9999950201", "합성도 겹시 같은동"),
])


class ParseTest(unittest.TestCase):
    def test_a_jibun_address_reads_its_places_and_lot(self) -> None:
        self.assertEqual(a.jibun_key("합성도 가상시 다라면 마바리 산 12-3번지 외 2필지", PREFIXES),
                         a.JibunKey("99", "가상시 다라면 마바리", True, 12, 3))
        self.assertEqual(a.jibun_key("합성도 가상시 가나동 7", PREFIXES), a.JibunKey("99", "가상시 가나동", False, 7, 0))
        self.assertEqual(a.jibun_key("합성도 가상시 다라면 마바리 708-3외1번지", PREFIXES).bubun, 3)

    def test_a_building_number_after_a_road_is_not_a_lot(self) -> None:
        self.assertIsNone(a.jibun_key("합성도 가상시 가나대로 12 (가나동)", PREFIXES))
        self.assertEqual(a.road_key("합성도 가상시 다라면 가나대로413번길 6-1, 3층", PREFIXES),
                         a.RoadKey("99", "가상시", "가나대로413번길", 6, 1))

    def test_a_glued_city_and_district_split(self) -> None:
        self.assertEqual(a.address_words("합성도 새시동녘구 사아로 5")[1:3], ["새시", "동녘구"])

    def test_the_longest_province_prefix_wins(self) -> None:
        self.assertEqual(a.sido_code("합성특별자치도", PREFIXES), "98")
        self.assertIsNone(a.sido_code("다른도", PREFIXES))


class SidoPrefixTest(unittest.TestCase):
    """Root ADR-0403: the 시도 prefixes come from the 법정동 code table and its recorded pairs.

    Synthetic 시도 (codes 9x) replay the three cases the real table holds: two 시도 merged into a
    new code by recorded pairs (광주·전남 -> 12, 2026-07-01), a renumbering before the record began
    (강원 42 -> 51), and a three-syllable stem's short form (충청북 -> 충북)."""

    NAMES = [
        ("9100000000", "가상남도", "폐지"), ("9110110100", "가상남도 가상군 가나면", "폐지"),
        ("9200000000", "가상광역시", "폐지"),
        ("9300000000", "가상통합특별시", "존재"), ("9311010100", "가상통합특별시 가상군 가나면", "존재"),
        ("9400000000", "합성도", "폐지"),
        ("9500000000", "합성특별자치도", "존재"),
        ("9600000000", "나다북도", "존재"),
        ("9711010100", "세모특별자치시 세모읍", "존재"),
    ]
    PAIRS = [("91", "93"), ("92", "93")]

    def test_a_merged_province_reads_as_the_code_its_pairs_lead_to(self) -> None:
        prefixes = a.sido_prefixes(self.NAMES, self.PAIRS)
        for word in ("가상남도", "가상남", "가상광역시", "가상", "가상통합특별시"):
            self.assertEqual(a.sido_code(word, prefixes), "93", word)
        key = a.jibun_key("가상남도 가상군 가나면 7", prefixes)
        self.assertEqual(key.sido_code, "93")

    def test_a_renumbering_the_record_lacks_follows_the_shared_stem(self) -> None:
        prefixes = a.sido_prefixes(self.NAMES, self.PAIRS)
        self.assertEqual(a.sido_code("합성도", prefixes), "95")
        self.assertEqual(a.sido_code("합성", prefixes), "95")

    def test_a_three_syllable_stem_reads_in_its_short_form(self) -> None:
        prefixes = a.sido_prefixes(self.NAMES, self.PAIRS)
        self.assertEqual((a.sido_code("나다북도", prefixes), a.sido_code("나북", prefixes)), ("96", "96"))
        # A 시도 the table names only through its 시군구 rows still reads.
        self.assertEqual(a.sido_code("세모", prefixes), "97")

    def test_without_its_pairs_a_merged_province_is_not_guessed(self) -> None:
        prefixes = a.sido_prefixes(self.NAMES, [])
        self.assertIsNone(a.sido_code("가상남도", prefixes))

    def test_a_stem_two_current_provinces_share_is_left_out(self) -> None:
        names = [*self.NAMES, ("9800000000", "합성광역시", "존재")]
        self.assertIsNone(a.sido_code("합성", a.sido_prefixes(names, self.PAIRS)))


class CandidateTest(unittest.TestCase):
    def test_a_jibun_address_offers_its_pnu(self) -> None:
        offer = a.candidates("합성도 가상시 다라면 마바리 산 12-3", PREFIXES, DONGS)
        self.assertEqual((offer["jibun_pnu"], offer["legal_dong_code"]), ("9999925021200120003", "9999925021"))

    def test_a_name_two_codes_share_is_not_guessed(self) -> None:
        self.assertEqual(a.candidates("합성도 겹시 같은동 1", PREFIXES, DONGS)["reason"], "no_legal_dong")

    def test_an_address_without_its_district_finds_the_one_code(self) -> None:
        self.assertEqual(a.candidates("합성도 새시 사아동 4-1", PREFIXES, DONGS)["jibun_pnu"], "9999931101100040001")

    def test_what_neither_reading_takes_says_why(self) -> None:
        self.assertEqual(a.candidates("", PREFIXES, DONGS)["reason"], "no_address")
        self.assertEqual(a.candidates("다른도 어딘가", PREFIXES, DONGS)["reason"], "unparsed")
        self.assertIsNone(a.jibun_pnu("9999910100", a.JibunKey("99", "가상시 가나동", False, 10000, 0)))


if __name__ == "__main__":
    unittest.main()
