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
from datetime import datetime, timezone
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

from lakehouse_maintenance import (  # noqa: E402
    MINIMUM_ORPHAN_SAFETY_HOURS,
    create_tag_sql,
    drop_tag_sql,
    load_maintenance_contract,
    month_end_snapshots,
    month_of_tag,
    retention_tags,
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
        "snapshot_expiry": {"retain_days": 7, "retain_last": 1,
                            "month_end_tag": {"name_format": "monthly-%Y-%m", "keep_months": 12},
                            "year_end_tag": {"name_format": "yearly-%Y"}},
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


def at(year: int, month: int, day: int, hour: int = 0) -> datetime:
    return datetime(year, month, day, hour, tzinfo=timezone.utc)


def gfs(keep_months: int = 12) -> dict:
    return {"month_end_tag": {"name_format": "monthly-%Y-%m", "keep_months": keep_months},
            "year_end_tag": {"name_format": "yearly-%Y"}}


class MonthEndTagTest(unittest.TestCase):
    """30일 + 달 끝 12개월 + 해 끝 영구 (ADR-0237). 달·해 끝은 그때 `main` 이 가리키던 스냅숏."""

    def test_each_finished_month_ends_on_its_last_current_snapshot(self) -> None:
        history = [(at(2026, 8, 3), 1), (at(2026, 8, 30), 2), (at(2026, 9, 10), 3)]

        self.assertEqual(
            month_end_snapshots(history, now=at(2026, 10, 14)),
            [(2026, 8, 2), (2026, 9, 3)],
        )

    def test_the_running_month_is_not_tagged_yet(self) -> None:
        """끝나지 않은 달의 마지막 스냅숏은 아직 모른다."""
        history = [(at(2026, 10, 1), 7), (at(2026, 10, 13), 8)]

        self.assertEqual(month_end_snapshots(history, now=at(2026, 10, 14)), [])

    def test_a_month_without_a_commit_ends_on_the_previous_snapshot(self) -> None:
        history = [(at(2026, 7, 20), 4), (at(2026, 9, 2), 5)]

        self.assertEqual(
            month_end_snapshots(history, now=at(2026, 9, 30)),
            [(2026, 7, 4), (2026, 8, 4)],
        )

    def test_a_commit_at_the_boundary_belongs_to_the_next_month(self) -> None:
        history = [(at(2026, 8, 15), 1), (at(2026, 9, 1), 2)]

        self.assertEqual(month_end_snapshots(history, now=at(2026, 9, 2)), [(2026, 8, 1)])

    def test_the_year_rolls_over(self) -> None:
        history = [(at(2026, 12, 31, 23), 9)]

        self.assertEqual(month_end_snapshots(history, now=at(2027, 1, 1)), [(2026, 12, 9)])

    def test_an_existing_tag_is_never_moved(self) -> None:
        """이미 있는 태그는 다시 만들지 않는다. 옮기면 그 달의 상태가 바뀐다."""
        history = [(at(2026, 8, 3), 1), (at(2026, 9, 10), 3)]

        create, _ = retention_tags(history, {"main", "monthly-2026-08"}, gfs(), at(2026, 10, 14))

        self.assertEqual(create, [("monthly-2026-09", 3)])

    def test_december_is_also_the_year_end(self) -> None:
        history = [(at(2025, 12, 20), 5), (at(2026, 1, 5), 6)]

        create, _ = retention_tags(history, {"main"}, gfs(), at(2026, 2, 3))

        self.assertEqual(
            create, [("monthly-2025-12", 5), ("yearly-2025", 5), ("monthly-2026-01", 6)]
        )

    def test_a_month_end_tag_past_the_window_is_dropped_and_a_year_end_tag_is_not(self) -> None:
        """12개월 창은 태그가 이름한 달로 센다: 13개월 전 달 끝 태그는 지우고, 해 끝 태그는 남긴다."""
        refs = {"main", "monthly-2025-09", "monthly-2025-10", "yearly-2024", "served-parcel-x"}

        _, drop = retention_tags([], refs, gfs(), at(2026, 10, 14))

        self.assertEqual(drop, ["monthly-2025-09"])

    def test_a_month_already_past_the_window_is_not_tagged(self) -> None:
        """첫 실행이 오래된 history 를 만나도, 창 밖 달은 태그를 만들었다 지우지 않는다."""
        history = [(at(2025, 8, 3), 1)]

        create, _ = retention_tags(history, {"main"}, gfs(), at(2026, 10, 14))

        self.assertNotIn("monthly-2025-08", [name for name, _ in create])
        self.assertNotIn("monthly-2025-09", [name for name, _ in create])
        self.assertIn(("monthly-2025-10", 1), create)
        self.assertIn(("yearly-2025", 1), create)

    def test_only_a_month_end_name_reads_as_a_month(self) -> None:
        self.assertEqual(month_of_tag("monthly-2026-09", "monthly-%Y-%m"), (2026, 9))
        self.assertIsNone(month_of_tag("yearly-2026", "monthly-%Y-%m"))
        self.assertIsNone(month_of_tag("monthly-2026-9", "monthly-%Y-%m"))
        self.assertIsNone(month_of_tag("served-parcel-by-pnu-20260101T000000Z-1", "monthly-%Y-%m"))

    def test_a_tag_is_dropped_by_name(self) -> None:
        self.assertEqual(
            drop_tag_sql("r2", "silver.parcel_boundaries", "monthly-2025-09"),
            "ALTER TABLE r2.silver.parcel_boundaries DROP TAG IF EXISTS `monthly-2025-09`",
        )

    def test_a_year_end_name_that_reads_as_a_month_is_refused(self) -> None:
        """해 끝 이름이 달 끝 이름으로 읽히면 12개월 창이 영구 태그를 지운다."""
        contract = sound_contract()
        contract["snapshot_expiry"]["year_end_tag"]["name_format"] = "monthly-%Y-12"

        with self.assertRaises(ValueError):
            validate_maintenance_contract(contract)

    def test_an_empty_monthly_window_is_refused(self) -> None:
        contract = sound_contract()
        contract["snapshot_expiry"]["month_end_tag"]["keep_months"] = 0

        with self.assertRaises(ValueError):
            validate_maintenance_contract(contract)

    def test_a_tag_is_created_without_an_age_limit(self) -> None:
        """`RETAIN` 을 붙이면 `max-ref-age-ms` 가 생기고, 스냅숏 커밋 시각부터 세어 만료가 지운다."""
        sql = create_tag_sql("r2", "silver.parcel_boundaries", "monthly-2026-09", 42)

        self.assertIn("CREATE TAG IF NOT EXISTS `monthly-2026-09` AS OF VERSION 42", sql)
        self.assertNotIn("RETAIN", sql.upper())

    def test_a_name_two_months_share_is_refused(self) -> None:
        """이름에 연·월이 없으면 둘째 달부터 태그가 안 생기고 그 스냅숏이 만료된다."""
        contract = sound_contract()
        contract["snapshot_expiry"]["month_end_tag"]["name_format"] = "monthly-%Y"

        with self.assertRaises(ValueError) as caught:
            validate_maintenance_contract(contract)
        self.assertIn("%m", str(caught.exception))

    def test_the_shipped_name_is_unique_per_month(self) -> None:
        name_format = load_maintenance_contract()["snapshot_expiry"]["month_end_tag"]["name_format"]
        names = {
            at(year, month, 1).strftime(name_format)
            for year in (2026, 2027)
            for month in range(1, 13)
        }
        self.assertEqual(len(names), 24)


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
