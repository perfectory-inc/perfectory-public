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

import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest import mock
from datetime import datetime, timezone
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

from lakehouse_maintenance import (  # noqa: E402
    MINIMUM_ORPHAN_SAFETY_HOURS,
    create_tag_sql,
    drop_tag_sql,
    expiry_floor_millis,
    ingest_record_properties,
    millis_text,
    load_maintenance_contract,
    month_end_snapshots,
    month_of_tag,
    protected_snapshots,
    retention_tags,
    rewrite_data_files_sql,
    set_properties_sql,
    read_rotation,
    rotation_order,
    should_compact,
    timestamp_before,
    validate_maintenance_contract,
    write_rotation,
)


def sound_contract() -> dict:
    return {
        "schema_version": 1,
        "order": ["compaction", "snapshot_expiry", "orphan_cleanup"],
        "compaction": {"target_file_bytes": 1, "min_input_files": 2,
                       "trigger_small_file_count": 10, "trigger_small_file_fraction": 0.25,
                       "partial_progress_max_commits": 10,
                       "fold_deletes": {"max_deleted_row_ratio": 0.01, "max_deleted_row_ratio_reason": "r",
                                        "max_delete_byte_ratio": 0.002, "max_delete_byte_ratio_reason": "r",
                                        "max_delete_bytes": 16777216, "max_delete_bytes_reason": "r",
                                        "after_days": 7}},
        "snapshot_expiry": {"retain_days": 7, "retain_last": 1,
                            "month_end_tag": {"name_format": "monthly-%Y-%m", "keep_months": 12},
                            "year_end_tag": {"name_format": "yearly-%Y"}},
        "orphan_cleanup": {"safety_days": 3},
        "scheduled": {"deadline_minutes": 140, "skip": ["orphan_cleanup"],
                      "spark": {"master": "local[8,8]", "driver_memory": "16g"}},
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


INGEST = "foundation.ingest-batch-objects"
PINS = "foundation.source-iceberg-snapshots"


def snapshot(parent: int | None, ms: int, **summary: str) -> dict:
    return {"parent_id": parent, "committed_ms": ms, "summary": summary}


class ReadBackFloorTest(unittest.TestCase):
    """만료는 이 레이크하우스가 메타데이터에서 다시 읽는 스냅숏을 지우지 않는다."""

    def test_every_ingest_record_in_main_is_protected(self) -> None:
        """부분 레인은 모든 부분의 기록이 남아 있어야 적재됐다고 본다. 하나라도 지우면 그 부분이 두 번 적재된다."""
        snapshots = {
            1: snapshot(None, 100, **{INGEST: "part-1"}),
            2: snapshot(1, 200, **{INGEST: "part-2"}),
            3: snapshot(2, 300, operation="replace"),
        }

        self.assertEqual(protected_snapshots(snapshots, 3), {1, 2})

    def test_only_the_newest_gold_pins_are_protected(self) -> None:
        """Gold 재구축은 가장 새 pin 만 읽는다. 옛 pin 스냅숏까지 묶으면 Gold 는 영영 만료되지 않는다."""
        snapshots = {
            1: snapshot(None, 100, **{PINS: "{}"}),
            2: snapshot(1, 200, **{PINS: "{}"}),
            3: snapshot(2, 300),
        }

        self.assertEqual(protected_snapshots(snapshots, 3), {2})

    def test_a_record_outside_main_is_not_protected(self) -> None:
        snapshots = {1: snapshot(None, 100), 2: snapshot(1, 200), 9: snapshot(1, 150, **{INGEST: "staged"})}

        self.assertEqual(protected_snapshots(snapshots, 2), set())

    def test_records_past_the_window_get_a_durable_copy_once(self) -> None:
        """창 밖의 적재 기록만, 아직 속성이 없는 것만 옮긴다(root ADR-0332). 창 안은 요약이 지킨다."""
        snapshots = {
            1: snapshot(None, 100, **{INGEST: "a.zip,b.zip"}),
            2: snapshot(1, 200, **{INGEST: "c.zip"}),
            3: snapshot(2, 300, **{INGEST: "d.zip"}),
            4: snapshot(3, 400, operation="replace"),
        }
        held = {f"{INGEST}.2": "c.zip"}

        self.assertEqual(ingest_record_properties(snapshots, [4, 3, 2, 1], 350, held),
                         {f"{INGEST}.1": "a.zip,b.zip", f"{INGEST}.3": "d.zip"})
        self.assertEqual(ingest_record_properties(snapshots, [4, 3, 2, 1], 50, held), {})

    def test_property_values_are_quoted_as_spark_literals(self) -> None:
        sql = set_properties_sql("cat.silver.t", {f"{INGEST}.7": "bronze/it's.zip"})

        self.assertEqual(sql, f"ALTER TABLE cat.silver.t SET TBLPROPERTIES ('{INGEST}.7' = 'bronze/it\\'s.zip')")

    def test_the_floor_moves_the_window_back_to_the_oldest_protected_snapshot(self) -> None:
        snapshots = {1: snapshot(None, 100), 2: snapshot(1, 200)}

        self.assertEqual(expiry_floor_millis(500, snapshots, {2}), 200)
        self.assertEqual(expiry_floor_millis(500, snapshots, set()), 500)
        self.assertEqual(expiry_floor_millis(150, snapshots, {2}), 150)

    def test_the_floor_is_written_to_the_millisecond(self) -> None:
        """Iceberg 는 older_than 보다 엄격히 이전인 스냅숏만 지운다. 초로 자르면 보호한 스냅숏이 지워진다."""
        self.assertEqual(millis_text(1_760_000_000_123), "2025-10-09 08:53:20.123")


class ScheduledRunTest(unittest.TestCase):
    def test_tables_never_maintained_come_first_then_the_oldest(self) -> None:
        last = {"gold.a": "2026-10-13T01:00:00+00:00", "silver.b": "2026-10-12T01:00:00+00:00"}

        self.assertEqual(
            rotation_order(["gold.a", "silver.b", "silver.c"], last), ["silver.c", "silver.b", "gold.a"]
        )

    def test_an_empty_or_broken_rotation_file_starts_the_order_over(self) -> None:
        """2026-10-15: a run that hit a full disk left rotation.json empty, and every run after it
        failed reading it. The file only orders tables; it must not stop the job."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rotation.json"
            for text in ("", "{", "[]"):
                path.write_text(text, encoding="utf-8")
                out = io.StringIO()
                with redirect_stdout(out):
                    self.assertEqual(read_rotation(str(path)), {})
                self.assertIn("maintenance-rotation-reset", out.getvalue())

    def test_a_failed_write_keeps_the_previous_rotation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "rotation.json")
            write_rotation(path, {"gold.a": "2026-10-13T01:00:00+00:00"})
            with mock.patch("lakehouse_maintenance.os.fsync", side_effect=OSError(28, "No space left on device")):
                with self.assertRaises(OSError):
                    write_rotation(path, {"gold.a": "2026-10-16T01:00:00+00:00"})
            self.assertEqual(read_rotation(path), {"gold.a": "2026-10-13T01:00:00+00:00"})

    def test_a_table_rule_names_a_known_step_and_a_reason(self) -> None:
        contract = sound_contract()
        contract["tables"] = {"silver.x": {"skip": ["vacuum"], "reason": "r"}}
        with self.assertRaises(ValueError):
            validate_maintenance_contract(contract)
        contract["tables"] = {"silver.x": {"skip": ["compaction"]}}
        with self.assertRaises(ValueError):
            validate_maintenance_contract(contract)

    def test_every_table_rule_names_a_lakehouse_table(self) -> None:
        """규칙이 없는 표 이름을 가리키면 그 표의 실제 이름은 규칙 없이 돈다."""
        from platform_contracts import load_lakehouse_artifact

        tables = set(load_lakehouse_artifact()["contracts"])
        self.assertLessEqual(set(load_maintenance_contract().get("tables", {})), tables)

    def test_the_deadline_leaves_the_unit_time_to_finish_a_table(self) -> None:
        """마감 뒤에는 새 표를 시작하지 않는다. 그 마감이 유닛 상한 안이어야 진행 중인 표가 끝난다."""
        import re

        unit = (JOBS_DIR.parents[2] / "systemd" / "foundation-lakehouse-maintenance.service").read_text(
            encoding="utf-8")
        limit = int(re.search(r"^TimeoutStartSec=(\d+)m$", unit, re.M).group(1))
        self.assertLess(load_maintenance_contract()["scheduled"]["deadline_minutes"], limit)


class R2ResetsTest(unittest.TestCase):
    """2026-10-15: one R2 connection reset aborted silver.parcel_boundaries's whole compaction."""

    def test_a_master_without_task_retries_is_refused(self):
        """Plain local[N] ignores spark.task.maxFailures: one reset aborts the job (root ADR-0102)."""
        for master in ("local[8]", "local[8,1]", "local[*]"):
            contract = sound_contract()
            contract["scheduled"]["spark"]["master"] = master
            with self.assertRaises(ValueError, msg=master):
                validate_maintenance_contract(contract)

    def test_the_shipped_master_retries_tasks(self):
        master = load_maintenance_contract()["scheduled"]["spark"]["master"]
        self.assertRegex(master, r"^local\[\d+,\s*([2-9]|\d{2,})\]$")

    def test_a_compaction_keeps_the_groups_it_already_rewrote(self):
        sql = rewrite_data_files_sql("r2", "silver.parcel_boundaries", sound_contract()["compaction"])
        self.assertIn("'partial-progress.enabled','true'", sql)
        self.assertIn("'partial-progress.max-commits','10'", sql)
        self.assertIn("'min-input-files','2'", sql)
        self.assertNotIn("strategy", sql)

    def test_a_sorted_table_is_compacted_in_its_sort_order(self):
        """Bin-pack mixed gold.parcel_panel's PNU ranges into whole-country files (root ADR-0307)."""
        sql = rewrite_data_files_sql("r2", "gold.parcel_panel", sound_contract()["compaction"], ordered=True)
        self.assertIn("strategy => 'sort'", sql)
        self.assertIn("'partial-progress.enabled','true'", sql)

    def test_each_reason_adds_only_its_own_options(self):
        """Root ADR-0344 §5: deletes fold only the files carrying them; out of order rewrites every file."""
        import lakehouse_maintenance as maintenance

        compaction = sound_contract()["compaction"]
        small = rewrite_data_files_sql("r2", "gold.t", compaction, True)
        self.assertNotIn("delete-file-threshold", small)
        self.assertNotIn("rewrite-all", small)
        fold = rewrite_data_files_sql("r2", "gold.t", compaction, True, (maintenance.FOLD_DELETES,))
        self.assertIn("'delete-file-threshold','1'", fold)
        self.assertIn("'remove-dangling-deletes','true'", fold)
        self.assertNotIn("rewrite-all", fold)
        whole = rewrite_data_files_sql("r2", "gold.t", compaction, True, (maintenance.OUT_OF_ORDER,))
        self.assertIn("'rewrite-all','true'", whole)


def shape(position_deletes=48_477, delete_bytes=400_000, oldest_days=1.0):
    """gold.parcel_panel after one national vintage merged on read (2026-10-26's counts)."""
    return {"records": 39_861_511, "position_deletes": position_deletes, "data_bytes": 4_061_289_766,
            "delete_bytes": delete_bytes, "oldest_days": oldest_days}


class CompactionReasonTest(unittest.TestCase):
    """Root ADR-0344 §5: small files, deletes past any fold bound, or files out of order."""

    def reasons(self, small=0, deletes=None, share=None):
        import lakehouse_maintenance as maintenance

        return maintenance.compaction_reasons(small, 14, sound_contract()["compaction"], deletes, share)

    def test_a_healthy_table_with_one_vintage_of_deletes_is_left_alone(self):
        self.assertEqual(self.reasons(small=3, deletes=shape(), share=0.3), ())
        self.assertEqual(self.reasons(), ())

    def test_each_reason_is_named_when_it_holds(self):
        import lakehouse_maintenance as maintenance

        self.assertEqual(self.reasons(small=10), (maintenance.SMALL_FILES,))
        self.assertEqual(self.reasons(deletes=shape(oldest_days=7.0)), (maintenance.FOLD_DELETES,))
        # The three Gold tables the shape-ledger check failed on 2026-10-29 read 100% per region.
        self.assertEqual(self.reasons(share=1.0), (maintenance.OUT_OF_ORDER,))
        self.assertEqual(self.reasons(small=12, deletes=shape(oldest_days=30), share=0.71),
                         (maintenance.SMALL_FILES, maintenance.FOLD_DELETES, maintenance.OUT_OF_ORDER))

    def test_any_one_fold_bound_folds_and_names_itself(self):
        import lakehouse_maintenance as maintenance

        fold = sound_contract()["compaction"]["fold_deletes"]
        self.assertEqual(maintenance.fold_due(shape(), fold), [])
        self.assertEqual(maintenance.fold_due(shape(position_deletes=400_000), fold), ["deleted_row_ratio"])
        self.assertEqual(maintenance.fold_due(shape(delete_bytes=9_000_000), fold), ["delete_byte_ratio"])
        small_table = {**shape(position_deletes=0, delete_bytes=17_000_000), "data_bytes": 10**13}
        self.assertEqual(maintenance.fold_due(small_table, fold), ["delete_bytes"])
        self.assertEqual(maintenance.fold_due(shape(oldest_days=8), fold), ["age"])
        self.assertEqual(maintenance.fold_due(None, fold), [])

    def test_a_fold_cadence_past_the_expiry_window_is_refused(self):
        for days in (None, 0, 8):
            contract = sound_contract()
            contract["compaction"]["fold_deletes"]["after_days"] = days
            with self.assertRaises(ValueError, msg=days):
                validate_maintenance_contract(contract)

    def test_a_fold_bound_without_its_value_or_reason_is_refused(self):
        for bound in ("max_deleted_row_ratio", "max_delete_byte_ratio", "max_delete_bytes"):
            for broken in ({bound: 0}, {f"{bound}_reason": ""}):
                contract = sound_contract()
                contract["compaction"]["fold_deletes"].update(broken)
                with self.assertRaises(ValueError, msg=(bound, broken)):
                    validate_maintenance_contract(contract)


class DeleteMetricsTest(unittest.TestCase):
    """Root ADR-0344 §5: both ratios per table, for tuning the bounds; an unreached table keeps its last."""

    def test_the_textfile_carries_both_ratios_and_keeps_tables_this_run_did_not_reach(self):
        import lakehouse_maintenance as maintenance

        log = ["noise", maintenance.delete_metric_line("gold.a", shape()), maintenance.delete_metric_line("gold.b", None)]
        previous = {"gold.c": {"records": 10, "position_deletes": 1, "data_bytes": 100, "delete_bytes": 5}}
        state, text = maintenance.render_delete_metrics(log, previous)
        self.assertEqual(sorted(state), ["gold.a", "gold.b", "gold.c"])
        self.assertIn('foundation_lakehouse_deleted_row_ratio{table="gold.a"} 0.00121614', text)
        self.assertIn('foundation_lakehouse_delete_byte_ratio{table="gold.a"} 9.84909e-05', text)
        self.assertIn('foundation_lakehouse_deleted_row_ratio{table="gold.b"} 0', text)
        self.assertIn('foundation_lakehouse_delete_byte_ratio{table="gold.c"} 0.05', text)
        self.assertTrue(text.endswith("\n"))


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
