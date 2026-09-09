"""validate-only 는 덮어쓰기 승인 없이 지나간다 (root ADR-0096 서울 실증의 교훈).

첫 검증 실행이 덮어쓰기 가드에 걸려 죽었다: 아무것도 쓰지 않는 실행에
--allow-non-smoke-overwrite 를 요구했다. 검증이 승인 플래그를 의례처럼 들고 다니면
그 플래그는 실제 덮어쓰기 자리에서 아무것도 못 막는다. 가드는 쓰기가 일어날
실행에서만 선다.
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest import mock

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import parcel_panel_silver_to_gold as job  # noqa: E402
from lakehouse_engine import required_catalog_env  # noqa: E402


def parse(*extra: str):
    argv = [
        "parcel_panel_silver_to_gold.py",
        "--input-mode",
        "iceberg",
        "--write-mode",
        "iceberg",
        "--iceberg-snapshot-id",
        "1",
        *extra,
    ]
    with mock.patch.object(sys, "argv", argv):
        return job.parse_args()


class NonSmokeOverwriteGuardTest(unittest.TestCase):
    def setUp(self) -> None:
        catalog = parse().iceberg_catalog_name
        env = {name: "probe" for name in required_catalog_env(catalog)}
        patcher = mock.patch.dict(os.environ, env)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_a_write_without_approval_is_refused(self) -> None:
        with self.assertRaisesRegex(ValueError, "allow-non-smoke-overwrite"):
            job.validate_args(parse())

    def test_a_write_with_approval_passes(self) -> None:
        job.validate_args(parse("--allow-non-smoke-overwrite"))

    def test_validate_only_needs_no_approval(self) -> None:
        job.validate_args(parse("--validate-only"))


if __name__ == "__main__":
    unittest.main()
