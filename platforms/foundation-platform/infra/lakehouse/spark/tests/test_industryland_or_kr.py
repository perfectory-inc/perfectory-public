"""산업단지 고시 읽은 기록·첨부 색인 Silver 적재(root ADR-0484)를 합성 줄로 본다.

고시 번호·단지 코드·파일 id 는 지어낸 것이다(99999). 거부해야 할 것을 심는다: 번호가 없는 줄, 상세를 못 읽은 줄,
저장소에 없는 첨부(사라짐·내림·상한 넘음·실패), 한 고시에 같은 내용 두 번, 다른 형식의 계획, 이 적재가 맡지 않은 원천.
"""

from __future__ import annotations

import gzip
import json
import sys
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import industryland_or_kr as job  # noqa: E402
from platform_contracts import column_names, load_lakehouse_contract  # noqa: E402

CONTRACT = job.load_source_contract()
NOW = datetime(2099, 1, 2, 3, 4, 5)
KEY = "bronze/source=industrylandorkr__industrial_complex_notice_detail/industrial-complex-notice-detail-20990102t030405z-000.gz"
SHA = "a" * 64


def file(name, outcome, **extra):
    return {"file_name": f"고시문 {name}", "real_file_name": name, "file_path": "/smUpload/209901",
            "file_extension": ".pdf", "file_size": 1234, "use_yn": "Y", "store_outcome": outcome,
            "file_sequence": extra.pop("seq", 1), **extra}


def line(notice, outcome="read", files=None, body=True):
    result = {"ref_seq": notice, "danji_cd": "999990", "noti_title": "합성 산업단지계획(변경) 승인 고시",
              "noti_num": "합성시 고시 제2099-1호", "noti_dtm": "2099-01-01", "noti_type": "2", "noti_type_nm": "변경",
              "noti_dantype": "2", "noti_dantype_nm": "일반", "saveFiles": files or []}
    return {"notice_sequence": notice, "requested_at_utc": "2099-01-02T03:04:05+00:00", "http_status": 200 if body else None,
            "read_outcome": outcome, "body": {"code": 200, "result": result} if body else None}


class RowsTest(unittest.TestCase):
    def test_reads_every_line_and_indexes_only_files_in_the_store(self):
        lines = [
            line(99001, files=[
                file("f1", "stored", seq=1, sha256=SHA, media_format="pdf", size_bytes=1234,
                     source_url="https://example.invalid/d?x=1", fetched_at_utc="2099-01-02T03:04:06+00:00"),
                file("f2", "deduplicated", seq=2, sha256="b" * 64, media_format="hwp", size_bytes=99),
                file("f3", "gone", seq=3), file("f4", "withdrawn", seq=4), file("f5", "oversize", seq=5, sha256="c" * 64,
                                                                               media_format="pdf"),
                file("f6", "stored", seq=6, sha256=SHA, media_format="pdf", size_bytes=1234),
            ]),
            line(99002, outcome="failed", body=False),
            {"requested_at_utc": "2099-01-02T03:04:05+00:00", "read_outcome": "read"},
        ]
        rows = job.rows_of(lines, CONTRACT, KEY, "20990102T030405Z", NOW)
        reads = rows["rows"]
        self.assertEqual([r["notice_sequence"] for r in reads], [99001, 99002])
        self.assertEqual((reads[0]["file_count"], reads[0]["files_stored"]), (6, 3))
        self.assertEqual(reads[0]["notice_date"], date(2099, 1, 1))
        self.assertEqual(reads[0]["official_complex_code"], "999990")
        self.assertEqual((reads[1]["read_outcome"], reads[1]["file_count"], reads[1]["http_status"]), ("failed", None, None))
        attachments = rows["attachment_rows"]
        # f6 repeats f1's bytes: one row per (notice, hash), the later listing winning.
        self.assertEqual(sorted((a["sha256"][0], a["file_sequence"]) for a in attachments), [("a", 6), ("b", 2)])
        first = next(a for a in attachments if a["media_format"] == "hwp")
        self.assertEqual((first["provider_file_id"], first["provider_file_path"]), ("f2", "/smUpload/209901"))
        self.assertEqual(first["fetched_at_utc"], datetime(2099, 1, 2, 3, 4, 5))
        for table, sample in ((job.DETAIL_TABLE, reads[0]), (job.ATTACHMENT_TABLE, attachments[0])):
            contract = load_lakehouse_contract(table)
            self.assertEqual(set(sample), set(column_names(contract)), table)
            for column in contract["columns"]:
                if column["required"]:
                    self.assertIsNotNone(sample[column["name"]], f"{table}.{column['name']}")

    def test_plans_name_the_run_and_refuse_what_is_not_theirs(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan_dir = Path(tmp)
            (plan_dir / "objects").mkdir()
            local = plan_dir / "objects" / "a.gz"
            with gzip.open(local, "wt", encoding="utf-8") as handle:
                handle.write(json.dumps(line(99001)) + "\n")
            plan = {"schema_version": job.PLAN_SCHEMA, "objects": [
                {"object_key": KEY, "source_slug": job.source_slug(CONTRACT), "local_path": "objects/a.gz"}]}
            (plan_dir / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
            objects = job.planned_objects([plan_dir], CONTRACT)
            self.assertEqual(objects[0]["source_snapshot_id"], "20990102T030405Z")
            self.assertEqual(len(job.read_object(objects[0], CONTRACT, NOW)["rows"]), 1)
            plan["objects"][0]["source_slug"] = "industrylandorkr__industrial_complex_list"
            (plan_dir / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
            with self.assertRaises(ValueError):
                job.planned_objects([plan_dir], CONTRACT)
            (plan_dir / "plan.json").write_text(json.dumps({"schema_version": "other", "objects": []}), encoding="utf-8")
            with self.assertRaises(ValueError):
                job.planned_objects([plan_dir], CONTRACT)
        with self.assertRaises(ValueError):
            job.snapshot_id("bronze/source=x/no-run-id.gz")


if __name__ == "__main__":
    unittest.main()
