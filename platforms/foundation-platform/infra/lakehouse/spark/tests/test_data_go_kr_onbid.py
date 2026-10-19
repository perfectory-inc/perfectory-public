"""온비드 공매 Silver 적재(root ADR-0280)의 판독부를 합성 자료로 본다.

PNU 는 합성 지역 99 를 쓰고 물건관리번호는 지어낸 것이다. 각 관문은 거부해야 할 것을 심어서 확인한다:
대장구분 숫자가 틀린 PNU, 19자리가 아닌 PNU, 숫자가 아닌 최저입찰가, 03·429 답, 두 쪽에 걸친 같은 회차,
키가 없는 행, 다른 형식의 매니페스트.
"""

from __future__ import annotations

import gzip
import json
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

SPARK_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK_DIR / "jobs"))

import data_go_kr_onbid as job  # noqa: E402
from platform_contracts import load_lakehouse_contract  # noqa: E402

CONTRACT = job.load_source_contract()
NOW = datetime(2099, 1, 2, 3, 4, 5)


def page(items, code="00", status=200):
    body = {"header": {"resultCode": code}, "body": {"items": {"item": items}, "totalCount": len(items)}}
    if code != "00":
        body = {"result": {"resultCode": code}}
    return {"operation": "getRlstCltrList2", "request": {"pageNo": "1"}, "http_status": status,
            "body": json.dumps(body, ensure_ascii=False)}


def item(cltr="X-0001", condition=1, **extra):
    base = {"cltrMngNo": cltr, "pbctCdtnNo": condition, "ltnoPnu": "9999999999000010000", "prptDivCd": "0007",
            "pvctTrgtYn": "N", "pbctStatCd": "0001", "apslEvlAmt": 31190000, "lowstBidPrcIndctCont": "28071000",
            "cltrBidBgngDt": "209901051000", "cltrBidEndDt": "209901071700", "mdfcnDt": "20990101093000"}
    base.update(extra)
    return base


class PnuTest(unittest.TestCase):
    def test_the_provider_ledger_digit_is_read_in_the_hub_dialect(self) -> None:
        # ltnoPnu writes 0 for ordinary land and 1 for mountain land (measured on the first load).
        self.assertEqual(job.valid_pnu("9999999999000010000", CONTRACT), "9999999999100010000")
        self.assertEqual(job.valid_pnu("9999999999100010000", CONTRACT), "9999999999200010000")
        self.assertIsNone(job.valid_pnu("9999999999200010000", CONTRACT))  # a block lot has no PNU
        self.assertIsNone(job.valid_pnu("999999999900001000", CONTRACT))  # 18 digits
        self.assertIsNone(job.valid_pnu("99999999990000100٠0", CONTRACT))  # a non-ASCII digit
        self.assertIsNone(job.valid_pnu(None, CONTRACT))

    def test_a_row_without_a_valid_pnu_keeps_the_raw_text(self) -> None:
        row = job.item_row(item(ltnoPnu="99999-bad"), CONTRACT)
        self.assertIsNone(row["pnu"])
        self.assertEqual(row["ltno_pnu_raw"], "99999-bad")
        self.assertEqual(job.item_row(item(), CONTRACT)["ltno_pnu_raw"], "9999999999000010000")


class OwnershipTest(unittest.TestCase):
    def test_the_job_only_reads_the_handoffs(self) -> None:
        # The host script owns pending/ and moves a handoff after the load; the Spark container runs
        # as another user and could not (2026-10-19 first load: PermissionError on rmdir).
        source = Path(job.__file__).read_text(encoding="utf-8")
        for call in ("shutil.move", "os.rename", ".rename(", "rmdir", "unlink("):
            self.assertNotIn(call, source)


class ValueTest(unittest.TestCase):
    def test_provider_minutes_are_korean_time(self) -> None:
        self.assertEqual(job.kst_minute("209901051000"), datetime(2099, 1, 5, 1, 0))
        self.assertEqual(job.kst_minute("20990101093000"), datetime(2099, 1, 1, 0, 30))
        self.assertIsNone(job.kst_minute("2099-01-05"))

    def test_a_price_that_is_not_a_whole_number_stays_raw_only(self) -> None:
        row = job.item_row(item(lowstBidPrcIndctCont="협의"), CONTRACT)
        self.assertEqual(row["lowest_bid_price_raw"], "협의")
        self.assertIsNone(row["lowest_bid_price_krw"])
        self.assertEqual(job.item_row(item(), CONTRACT)["lowest_bid_price_krw"], 28071000)


class AnswerTest(unittest.TestCase):
    def test_empty_quota_and_single_answers(self) -> None:
        self.assertEqual(job.answer_items(page([], code="03")), [])
        self.assertEqual(job.answer_items({**page([item()]), "http_status": 429}), [])
        one = {**page([]), "body": json.dumps({"header": {"resultCode": "00"}, "body": {"items": {"item": item()}}})}
        self.assertEqual(len(job.answer_items(one)), 1)

    def test_a_condition_on_two_pages_is_one_row_from_its_later_answer(self) -> None:
        lines = [page([item(), item("X-0002")]), page([item(pbctStatCd="0002")])]
        rows = job.rows_of(lines, lambda i: job.item_row(i, CONTRACT), "bronze/k.gz", "20990102T030405Z", NOW)
        self.assertEqual(len(rows), 2)
        first = next(r for r in rows if r["cltr_mng_no"] == "X-0001")
        self.assertEqual((first["pbct_stat_cd"], first["source_line_number"]), ("0002", 2))

    def test_an_item_without_its_key_is_refused(self) -> None:
        with self.assertRaises((ValueError, KeyError)):
            job.rows_of([page([{"cltrMngNo": "X-0001"}])], job.result_row, "bronze/k.gz", "s", NOW)


class ContractTest(unittest.TestCase):
    def test_every_built_column_is_a_table_column_and_back(self) -> None:
        for table, build in ((job.ITEM_TABLE, lambda i: job.item_row(i, CONTRACT)), (job.RESULT_TABLE, job.result_row)):
            names = {c["name"] for c in load_lakehouse_contract(table)["columns"]}
            built = set(build(item())) | {"source_record_id", "source_line_number", "source_snapshot_id", "ingested_at_utc"}
            self.assertEqual(built | ({"row_state"} & names), names, table)

    def test_content_leaves_out_the_key_and_what_the_load_writes(self) -> None:
        content = job.content_columns(load_lakehouse_contract(job.ITEM_TABLE))
        for name in (*job.KEY, *job.LOAD_COLUMNS):
            self.assertNotIn(name, content)
        self.assertIn("pbct_stat_cd", content)


class HandoffTest(unittest.TestCase):
    def write(self, root: Path, complete: bool, schema: str = job.MANIFEST_SCHEMA) -> Path:
        handoff = root / "pending" / "20990102T030405Z"
        (handoff / "objects").mkdir(parents=True)
        objects = []
        for role, lines in (("item_list", [page([item()])]), ("item_bid_result", [page([item("X-0009", 3, scfbAmt="30000000")])]),
                            ("announcement_list", [page([], code="03")])):
            path = handoff / "objects" / f"{role}.gz"
            with gzip.open(path, "wt", encoding="utf-8") as out:
                out.write("".join(json.dumps(line, ensure_ascii=False) + "\n" for line in lines))
            objects.append({"role": role, "object_key": f"bronze/source=x/{role}.gz", "local_path": f"objects/{role}.gz"})
        (handoff / "manifest.json").write_text(json.dumps({
            "schema_version": schema, "source_snapshot_id": "20990102T030405Z",
            "report": {"item_pass_complete": complete}, "objects": objects}), encoding="utf-8")
        return handoff

    def test_a_handoff_gives_item_and_result_rows_and_says_whether_the_pass_was_complete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            handoff = self.write(Path(tmp), complete=False)
            loads = job.read_handoff(handoff, CONTRACT, NOW)
            self.assertFalse(loads["item_pass_complete"])
            key, rows = loads["items"]
            self.assertEqual((key, len(rows), rows[0]["pnu"]), ("bronze/source=x/item_list.gz", 1, "9999999999100010000"))
            self.assertEqual(loads["results"][1][0]["winning_bid_amount_krw"], 30000000)
            self.assertEqual(job.pending_handoffs(Path(tmp)), [handoff])

    def test_a_manifest_of_another_kind_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            handoff = self.write(Path(tmp), complete=True, schema="something.else.v1")
            with self.assertRaises(ValueError):
                job.read_handoff(handoff, CONTRACT, NOW)


if __name__ == "__main__":
    unittest.main()
