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
    ("9911010100", "합성도 가상시 가나동"),
    ("9911025021", "합성도 가상시 다라면 마바리"),
    ("9913110100", "합성도 새시 동녘구 사아동"),
    ("9915010100", "합성도 겹시 같은동"),
    ("9915020100", "합성도 겹시 같은동"),
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


class CandidateTest(unittest.TestCase):
    def test_a_jibun_address_offers_its_pnu(self) -> None:
        offer = a.candidates("합성도 가상시 다라면 마바리 산 12-3", PREFIXES, DONGS)
        self.assertEqual((offer["jibun_pnu"], offer["legal_dong_code"]), ("9911025021200120003", "9911025021"))

    def test_a_name_two_codes_share_is_not_guessed(self) -> None:
        self.assertEqual(a.candidates("합성도 겹시 같은동 1", PREFIXES, DONGS)["reason"], "no_legal_dong")

    def test_an_address_without_its_district_finds_the_one_code(self) -> None:
        self.assertEqual(a.candidates("합성도 새시 사아동 4-1", PREFIXES, DONGS)["jibun_pnu"], "9913110100100040001")

    def test_what_neither_reading_takes_says_why(self) -> None:
        self.assertEqual(a.candidates("", PREFIXES, DONGS)["reason"], "no_address")
        self.assertEqual(a.candidates("다른도 어딘가", PREFIXES, DONGS)["reason"], "unparsed")
        self.assertIsNone(a.jibun_pnu("9911010100", a.JibunKey("99", "가상시 가나동", False, 10000, 0)))


if __name__ == "__main__":
    unittest.main()
