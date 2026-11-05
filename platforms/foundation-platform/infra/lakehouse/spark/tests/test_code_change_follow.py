"""법정동 코드 변경 따라가기(root ADR-0329 §3, ADR-0416)의 기록과 다시 잇기를 합성 행으로 본다.

지명·코드는 지어낸 것이다(시도 99). 심는 것: 따라가기 기록이 없는 표(처음 한 번은 현재 행 전부), 기록 순서가 스냅숏 id
순서와 다른 경우, 바뀐 코드 아래가 아닌 행은 다시 잇지 않기, 주소는 그대로 두고 연결만 새로 만들기.
"""

from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import address_pnu  # noqa: E402
import code_change_follow as follow  # noqa: E402
import courtauction_go_kr as court  # noqa: E402
import data_go_kr_registered_factory as factory  # noqa: E402

NOW = datetime(2099, 1, 2, 3, 4, 5)
NAMES = [("9100000000", "가상남도", "폐지"), ("9110110100", "가상남도 가상군 가나면", "폐지"),
         ("9300000000", "가상통합특별시", "존재"), ("9311010100", "가상통합특별시 가상군 가나면", "존재")]
PREFIXES = address_pnu.sido_prefixes(NAMES, [("91", "93")])
DONGS = address_pnu.legal_dong_index([(c, n) for c, n, s in NAMES if s == "존재"])


class RecordTest(unittest.TestCase):
    def test_the_newest_record_by_instant_names_the_snapshot(self) -> None:
        first = follow.follow_record(9, datetime(2099, 1, 1))
        later = follow.follow_record(3, datetime(2099, 1, 2))
        self.assertEqual(follow.last_followed([later, "bronze/source=x/y.gz", first]), 3)

    def test_a_table_without_a_record_has_never_followed(self) -> None:
        # None makes the first follow re-link every current row (changed_prefixes returns None).
        self.assertIsNone(follow.last_followed(["bronze/source=x/y.gz"]))
        self.assertIsNone(follow.changed_prefixes(None, "c", None, 7, "job"))

    def test_only_the_codes_that_changed_are_followed(self) -> None:
        before = [{"kind": "pair", "old_code": "9110110100", "new_code": "9311010100", "level": "eupmyeondong"}]
        after = [*before, {"kind": "pair", "old_code": "9110120100", "new_code": "9311020100", "level": "eupmyeondong"}]
        import legal_dong_code_change_views as views  # noqa: PLC0415

        self.assertEqual(follow.prefixes_from_changes(before, after),
                         {views.code_prefix("9110120100"), views.code_prefix("9311020100")})
        self.assertEqual(follow.prefixes_from_changes(before, before), set())


class RelinkTest(unittest.TestCase):
    def test_a_court_row_keeps_its_values_and_gets_a_new_link(self) -> None:
        row = {"court_office_code": "B999999", "case_no": "1", "property_seq": 1, "item_seq": 1,
               "address_sido_name": "가상남도", "address_sigungu_name": "가상군", "address_dong_name": "가나면",
               "address_ri_name": None, "representative_lot_raw": "7", "road_name": None, "building_no_raw": None,
               "appraisal_amount_krw": 1, "legal_dong_code": None, "pnu": None, "source_record_id": "bronze/old.gz",
               "source_snapshot_id": "20990101T000000Z"}
        [again] = court.relinked_rows([row], "follow:x", "20990102T030405Z", NOW, PREFIXES, DONGS)
        self.assertEqual(again["jibun_pnu"], "9311010100100070000")
        self.assertEqual(again["legal_dong_code"], "9311010100")
        self.assertEqual(again["appraisal_amount_krw"], 1)
        self.assertEqual((again["source_record_id"], again["source_snapshot_id"]), ("follow:x", "20990102T030405Z"))

    def test_a_factory_line_keeps_its_values_and_gets_a_new_link(self) -> None:
        row = {"serial_no": 1, "company_name": "합성공업", "complex_name": None, "products": "부품",
               "factory_address_raw": "가상남도 가상군 가나면 7", "source_line_number": 2}
        [again] = factory.relinked_offers([row], PREFIXES, DONGS)
        self.assertEqual(again["jibun_pnu"], "9311010100100070000")
        self.assertEqual((again["company_name"], again["source_line_number"]), ("합성공업", 2))
        built = set(again) | {"pnu", "pnu_basis", "pnu_unresolved_reason", "pnu_candidate_count",
                              "source_record_id", "source_snapshot_id", "ingested_at_utc"}
        from platform_contracts import load_lakehouse_contract  # noqa: PLC0415

        self.assertLessEqual({c["name"] for c in load_lakehouse_contract(factory.TABLE)["columns"]}, built)


if __name__ == "__main__":
    unittest.main()
