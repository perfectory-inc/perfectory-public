"""row_digest 는 내용 칼럼 전부를, 계보 칼럼은 하나도 지문에 넣지 않는다 (root ADR-0099).

지문 대상은 계약에서 빼기로 유도된다: 나중에 문서 칼럼이 하나 늘면 지문에도 자동으로
들어가고, 빠뜨리는 실수는 이 시험이 아니라 유도식 자체가 막는다. 이 시험이 지키는 것은
"계보 3종만 빠진다"는 경계다 — 계보가 지문에 들어가면 매일 전량이 '변경'으로 읽혀
델타 파이프라인이 무의미해진다.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import parcel_panel_silver_to_gold as job  # noqa: E402


class RowDigestColumnsTest(unittest.TestCase):
    def test_the_contract_carries_the_digest(self) -> None:
        self.assertIn("row_digest", job.GOLD_COLUMNS)

    def test_only_the_lineage_trio_stays_out_of_the_fingerprint(self) -> None:
        excluded = set(job.GOLD_COLUMNS) - set(job.CONTENT_DIGEST_COLUMNS)
        self.assertEqual(
            excluded,
            {"row_digest", "source_snapshot_id", "published_at_utc"},
        )

    def test_the_fingerprint_preserves_contract_order(self) -> None:
        ordered = tuple(
            column
            for column in job.GOLD_COLUMNS
            if column in set(job.CONTENT_DIGEST_COLUMNS)
        )
        self.assertEqual(job.CONTENT_DIGEST_COLUMNS, ordered)


if __name__ == "__main__":
    unittest.main()
