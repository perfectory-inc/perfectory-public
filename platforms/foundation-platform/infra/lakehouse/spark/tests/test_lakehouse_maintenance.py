"""유지보수 세 단계의 순서와 안전창을 고정한다.

이 저장소는 2026-08-30 까지 세 가지를 한 번도 안 돌렸다. 그 결과 데이터 파일 3,160개가
평균 5 MB 였고, 스냅숏이 한 번도 만료된 적 없어 R2 58 GB 가 17 GB 를 서빙하고 있었다.
Iceberg 는 셋 다 갖고 있었고, 우리가 부르지 않았을 뿐이다.

순서는 바꿀 수 없다. 합치기가 밀려난 파일을 만들고, 만료가 그것을 붙들던 스냅숏을 놓아 주고,
고아 청소만이 **어느 스냅숏도 가리킨 적 없는** 파일에 닿는다 — 작업이 쓰다가 죽었을 때 남는
잔해다. 만료는 그것을 보지 못하므로, 청소를 먼저 하면 잔해가 다음 회차로 밀리고 아예 안 하면
영원히 남는다.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

from lakehouse_maintenance import (  # noqa: E402
    MINIMUM_ORPHAN_SAFETY_HOURS,
    load_maintenance_contract,
    should_compact,
    timestamp_before,
    validate_maintenance_contract,
)


def sound_contract() -> dict:
    return {
        "schema_version": 1,
        "order": ["compaction", "snapshot_expiry", "orphan_cleanup"],
        "compaction": {"target_file_bytes": 1, "min_input_files": 2,
                       "trigger_small_file_count": 10, "trigger_small_file_fraction": 0.25},
        "snapshot_expiry": {"retain_days": 7, "retain_last": 1},
        "orphan_cleanup": {"safety_days": 3},
    }


class OrderTest(unittest.TestCase):
    def test_the_shipped_contract_is_sound(self) -> None:
        contract = load_maintenance_contract()
        self.assertEqual(
            contract["order"], ["compaction", "snapshot_expiry", "orphan_cleanup"]
        )

    def test_cleanup_before_expiry_is_refused(self) -> None:
        """청소를 먼저 하면 만료가 만들어 낼 잔해를 못 본다."""
        contract = sound_contract()
        contract["order"] = ["compaction", "orphan_cleanup", "snapshot_expiry"]

        with self.assertRaises(ValueError) as caught:
            validate_maintenance_contract(contract)
        self.assertIn("orphan_cleanup", str(caught.exception))

    def test_compaction_last_is_refused(self) -> None:
        """합치기가 마지막이면 그것이 만든 밀려난 파일을 아무도 안 치운다."""
        contract = sound_contract()
        contract["order"] = ["snapshot_expiry", "orphan_cleanup", "compaction"]

        with self.assertRaises(ValueError):
            validate_maintenance_contract(contract)


class SafetyWindowTest(unittest.TestCase):
    def test_a_window_under_a_day_is_refused_with_the_reason(self) -> None:
        """돌고 있는 쓰기의 출력을 지우면 그 쓰기가 하려던 커밋이 깨진다.

        Iceberg 도 24시간 미만을 거부하지만, 그때는 표를 이미 붙든 뒤다. 계약에서 먼저 막고
        이유를 말한다.
        """
        contract = sound_contract()
        contract["orphan_cleanup"]["safety_days"] = 0

        with self.assertRaises(ValueError) as caught:
            validate_maintenance_contract(contract)
        message = str(caught.exception)
        self.assertIn(str(MINIMUM_ORPHAN_SAFETY_HOURS), message)
        self.assertIn("corrupt", message.lower())

    def test_the_shipped_window_clears_icebergs_floor(self) -> None:
        contract = load_maintenance_contract()
        self.assertGreaterEqual(
            contract["orphan_cleanup"]["safety_days"] * 24, MINIMUM_ORPHAN_SAFETY_HOURS
        )

    def test_expiry_always_keeps_the_current_snapshot(self) -> None:
        """모든 스냅숏이 창 밖이어도 표는 현재 상태를 잃지 않아야 한다."""
        contract = sound_contract()
        contract["snapshot_expiry"]["retain_last"] = 0

        with self.assertRaises(ValueError):
            validate_maintenance_contract(contract)

    def test_a_window_is_a_past_timestamp_not_an_interval(self) -> None:
        """Iceberg 절차는 간격이 아니라 시각 문자열을 받는다."""
        stamp = timestamp_before(3)
        self.assertRegex(stamp, r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$")
        self.assertLess(stamp, timestamp_before(0), "과거여야 한다")


class CompactionTriggerTest(unittest.TestCase):
    def test_a_healthy_table_is_left_alone(self) -> None:
        """합치기는 읽은 바이트를 전부 다시 쓴다. 멀쩡한 표에 돌리면 돈만 든다."""
        self.assertFalse(should_compact(small_files=0, total_files=14, trigger_count=10))
        self.assertFalse(should_compact(small_files=9, total_files=14, trigger_count=10))

    def test_a_fragmented_table_is_compacted(self) -> None:
        self.assertTrue(should_compact(small_files=257, total_files=257, trigger_count=10))

    def test_a_single_file_table_is_never_compacted(self) -> None:
        """파일이 하나뿐이면 합칠 상대가 없다. 작아도 그렇다.

        산업단지 경계가 5.30 MB 파일 하나다 — 목표의 1%지만 더 줄일 방법이 없다.
        """
        self.assertFalse(should_compact(small_files=1, total_files=1, trigger_count=1))


if __name__ == "__main__":
    unittest.main()
