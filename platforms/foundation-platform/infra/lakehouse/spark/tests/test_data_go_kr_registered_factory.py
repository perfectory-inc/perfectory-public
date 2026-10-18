"""전국등록공장현황 Silver 적재(root ADR-0280 §4)의 판독부를 합성 파일로 본다.

회사명·주소는 지어낸 것이다(시도 코드 99 의 합성 지명). 거부해야 할 것을 심는다: 데이터 멤버가 둘인 zip,
머리글이 바뀐 파일, CP949 로 읽을 수 없는 바이트, 바닥보다 적은 행, 직전 판보다 한계 넘게 줄어든 판.
"""

from __future__ import annotations

import io
import sys
import unittest
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import address_pnu  # noqa: E402
import data_go_kr_registered_factory as job  # noqa: E402
from platform_contracts import load_lakehouse_contract  # noqa: E402

CONTRACT = job.load_source_contract()
HEADER = "순번,회사명,단지명,생산품,공장주소"
MEMBER = "합성자료/합성_등록공장현황_20991231.csv"


def zipped(lines, members=(MEMBER,), encoding="cp949"):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name in members:
            archive.writestr(name, "\r\n".join(lines).encode(encoding))
        archive.writestr("합성자료/데이터사전.csv", "a,b".encode("utf-8"))
    return buffer.getvalue()


class MemberTest(unittest.TestCase):
    def test_the_member_gives_its_date_and_rows(self) -> None:
        snapshot, rows = job.read_member(zipped([HEADER, "1,합성공업,,부품,합성도 가상시 가나동 7", ""]), CONTRACT)
        self.assertEqual((snapshot, len(rows)), ("20991231", 1))

    def test_what_is_not_the_contract_file_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            job.read_member(zipped([HEADER], members=(MEMBER, MEMBER.replace("2099", "2098"))), CONTRACT)
        with self.assertRaises(ValueError):
            job.read_member(zipped(["순번,회사명,공장주소"]), CONTRACT)
        broken = io.BytesIO()
        with zipfile.ZipFile(broken, "w") as archive:
            archive.writestr(MEMBER, HEADER.encode("cp949") + b"\r\n1,\xff\xff,,,\r\n")
        with self.assertRaises(UnicodeDecodeError):
            job.read_member(broken.getvalue(), CONTRACT)

    def test_an_edition_under_the_floor_or_shrunk_is_refused(self) -> None:
        floor = CONTRACT["member"]["bounds"]["min_rows"]
        job.check_bounds(floor, None, CONTRACT)
        with self.assertRaises(ValueError):
            job.check_bounds(floor - 1, None, CONTRACT)
        with self.assertRaises(ValueError):
            job.check_bounds(floor, floor * 2, CONTRACT)


class RowTest(unittest.TestCase):
    def test_rows_carry_what_the_address_offers(self) -> None:
        prefixes = {**CONTRACT["pnu_resolution"]["sido_prefixes"], "합성": "99"}
        contract = {**CONTRACT, "pnu_resolution": {**CONTRACT["pnu_resolution"], "sido_prefixes": prefixes}}
        dongs = address_pnu.legal_dong_index([("9911010100", "합성도 가상시 가나동")])
        rows = job.factory_rows([["1", "합성공업", "", "부품", "합성도 가상시 가나동 7"],
                                 ["2", "합성상사", "합성단지", "", "합성도 가상시 가나대로 3 (가나동)"],
                                 ["3", "", "", "", ""]], contract, dongs)
        self.assertEqual([r["source_line_number"] for r in rows], [2, 3, 4])
        self.assertEqual(rows[0]["jibun_pnu"], "9911010100100070000")
        self.assertEqual((rows[1]["road_name"], rows[1]["road_main_no"]), ("가나대로", 3))
        self.assertEqual(rows[2]["reason"], "no_address")

    def test_the_summary_says_the_match_rate(self) -> None:
        counts = job.summary_of([{"pnu_basis": "jibun_address", "pnu_unresolved_reason": None},
                                 {"pnu_basis": "road_address", "pnu_unresolved_reason": None},
                                 {"pnu_basis": None, "pnu_unresolved_reason": "road_ambiguous"},
                                 {"pnu_basis": None, "pnu_unresolved_reason": "no_address"}])
        self.assertEqual(counts["match_rate"], 0.5)
        self.assertEqual(counts["unresolved_reasons"], {"road_ambiguous": 1, "no_address": 1})

    def test_the_load_writes_every_table_column(self) -> None:
        names = {c["name"] for c in load_lakehouse_contract(job.TABLE)["columns"]}
        built = set(job.factory_rows([["1", "", "", "", ""]], CONTRACT, {})[0])
        resolved = {"pnu", "pnu_basis", "pnu_unresolved_reason", "pnu_candidate_count"}
        loaded = {"source_record_id", "source_snapshot_id", "ingested_at_utc"}
        self.assertLessEqual(names, built | resolved | loaded)


if __name__ == "__main__":
    unittest.main()
