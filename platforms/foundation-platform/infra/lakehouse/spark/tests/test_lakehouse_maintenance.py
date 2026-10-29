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
    held_by_pins,
    ingest_record_properties,
    millis_text,
    load_maintenance_contract,
    month_end_snapshots,
    missing_reader_pins,
    month_of_tag,
    read_keep_snapshots,
    retention_tags,
    rewrite_data_files_sql,
    set_properties_sql,
    read_rotation,
    rotation_order,
    should_compact,
    timestamp_before,
    validate_maintenance_contract,
    would_expire,
    write_rotation,
)


def sound_contract() -> dict:
    return {
        "schema_version": 1,
        "order": ["compaction", "snapshot_expiry", "orphan_cleanup"],
        "compaction": {"target_file_bytes": 1, "min_input_files": 2,
                       "trigger_small_file_count": 10, "trigger_small_file_fraction": 0.25,
                       "partial_progress_max_commits": 10},
        "snapshot_expiry": {"retain_days": 7, "retain_last": 1,
                            "month_end_tag": {"name_format": "monthly-%Y-%m", "keep_months": 12},
                            "year_end_tag": {"name_format": "yearly-%Y"},
                            "reader_pins": {"tags": {}, "readers": []}},
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


def tag(snapshot_id: int, age: int | None = None) -> dict:
    return {"type": "TAG", "snapshot_id": snapshot_id, "max_reference_age_in_ms": age}


def branch(snapshot_id: int) -> dict:
    return {"type": "BRANCH", "snapshot_id": snapshot_id}


def shipped_expiry() -> dict:
    return load_maintenance_contract()["snapshot_expiry"]


class ReaderPinTest(unittest.TestCase):
    """30일이 지나면 태그가 없는 스냅숏은 만료된다. 옛 스냅숏을 읽는 독자는 자기 태그로 고정한다(root ADR-0338).

    2026-10-29: 적재 기록 보호가 표 29개의 만료를 첫 적재 뒤로 막아, 30일 넘은 스냅숏 161개(데이터 약 24 GB)가
    남아 있었다.
    """

    def test_an_ingest_record_no_longer_holds_its_snapshot(self) -> None:
        """기록은 표 속성에 옮겨졌다(root ADR-0332). 기록이 있다는 이유만으로는 남지 않는다."""
        snapshots = {
            1: snapshot(None, 100, **{INGEST: "part-1"}),
            2: snapshot(1, 200, **{INGEST: "part-2"}),
            3: snapshot(2, 300, operation="replace"),
        }
        refs = {"main": branch(3)}

        self.assertEqual(expiry_floor_millis(250, snapshots, 3, refs, shipped_expiry()), 250)
        self.assertEqual(would_expire(snapshots, refs, 250, 1, 400), {1, 2})

    def test_the_gold_build_tag_keeps_the_chain_its_planner_walks(self) -> None:
        """Gold 계획은 머리에서 가장 새 pin 커밋까지 걷는다. 태그는 그 커밋을, 바닥은 그 사이 압축 커밋을 지킨다."""
        snapshots = {
            1: snapshot(None, 100, **{PINS: "{}"}),
            2: snapshot(1, 200, **{PINS: "{}"}),
            3: snapshot(2, 300, operation="replace"),
            4: snapshot(3, 400, operation="replace"),
        }
        refs = {"main": branch(4), "foundation-gold-build": tag(2)}
        floor = expiry_floor_millis(450, snapshots, 4, refs, shipped_expiry())

        self.assertEqual(floor, 200)
        self.assertEqual(would_expire(snapshots, refs, floor, 1, 500), {1})

    def test_a_table_waits_for_the_reader_pin_it_needs(self) -> None:
        """태그가 아직 없으면 그 표는 아무것도 만료하지 않는다. 다음 생산자 커밋이나 FLOOR 실행이 태그를 건다."""
        expiry = shipped_expiry()
        gold = {1: snapshot(None, 100, **{PINS: "{}"}), 2: snapshot(1, 200, operation="replace")}
        plain = {1: snapshot(None, 100), 2: snapshot(1, 200)}

        self.assertEqual(missing_reader_pins("gold.parcel_panel", gold, 2, {"main": branch(2)}, expiry),
                         ["foundation-gold-build"])
        self.assertEqual(missing_reader_pins("gold.parcel_panel", gold, 2,
                                             {"main": branch(2), "foundation-gold-build": tag(1)}, expiry), [])
        self.assertEqual(missing_reader_pins("silver.building_register_floors", plain, 2, {"main": branch(2)}, expiry),
                         ["foundation-floor-history-witness"])
        self.assertEqual(missing_reader_pins("silver.land_use_plan", plain, 2, {"main": branch(2)}, expiry), [])

    def test_a_tag_keeps_its_snapshot_and_a_tag_past_its_age_does_not(self) -> None:
        snapshots = {1: snapshot(None, 100), 2: snapshot(1, 200), 3: snapshot(2, 300)}

        self.assertEqual(would_expire(snapshots, {"main": branch(3), "served-x": tag(1)}, 250, 1, 400), {2})
        self.assertEqual(would_expire(snapshots, {"main": branch(3), "old": tag(1, age=50)}, 250, 1, 400), {1, 2})

    def test_a_snapshot_off_every_branch_is_kept_inside_the_window(self) -> None:
        """Iceberg 는 어느 브랜치에도 없는 스냅숏을 창 안이면 남긴다(예: 지운 브랜치에 커밋한 것)."""
        snapshots = {1: snapshot(None, 100), 2: snapshot(1, 200), 9: snapshot(1, 260), 8: snapshot(1, 150)}

        self.assertEqual(would_expire(snapshots, {"main": branch(2)}, 250, 1, 400), {1, 8})

    def test_retain_last_keeps_the_head_however_old(self) -> None:
        snapshots = {1: snapshot(None, 100), 2: snapshot(1, 200)}

        self.assertEqual(would_expire(snapshots, {"main": branch(2)}, 999, 1, 1000), {1})

    def test_a_pin_outside_the_catalog_refuses_the_expiry(self) -> None:
        """진행 중인 굽기의 고정값(in-progress.json)이 만료될 스냅숏이면 거부한다."""
        self.assertEqual(held_by_pins({1, 2}, {2: "the bake pin", 7: "served"}), {2: "the bake pin"})
        self.assertEqual(held_by_pins({1}, {2: "the bake pin"}), {})

    def test_the_keep_file_maps_snapshot_ids_to_reasons(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "keep.json"
            path.write_text('{"9999900000000000001": "the bake pin"}', encoding="utf-8")
            self.assertEqual(read_keep_snapshots(str(path)), {9999900000000000001: "the bake pin"})
            path.write_text('["9999900000000000001"]', encoding="utf-8")
            with self.assertRaises(ValueError):
                read_keep_snapshots(str(path))
        self.assertEqual(read_keep_snapshots(None), {})

    def test_a_reader_pin_says_who_makes_and_releases_it(self) -> None:
        contract = sound_contract()
        contract["snapshot_expiry"]["reader_pins"]["tags"] = {"x-": {"prefix": True, "made_by": "a", "reader": "c"}}
        with self.assertRaises(ValueError):
            validate_maintenance_contract(contract)
        contract["snapshot_expiry"]["reader_pins"]["tags"] = {
            "x": {"made_by": "a", "released_by": "b", "reader": "c", "walks_main_back_to_it": True}}
        with self.assertRaises(ValueError):
            validate_maintenance_contract(contract)

    def test_a_reader_is_kept_by_a_declared_pin_or_says_why_not(self) -> None:
        contract = sound_contract()
        contract["snapshot_expiry"]["reader_pins"]["readers"] = [{"path": "p", "reads": 1, "kept_by": "nope", "reason": "r"}]
        with self.assertRaises(ValueError):
            validate_maintenance_contract(contract)
        contract["snapshot_expiry"]["reader_pins"]["readers"] = [{"path": "p", "reads": 1, "kept_by": None, "reason": ""}]
        with self.assertRaises(ValueError):
            validate_maintenance_contract(contract)


class IngestRecordCopyTest(unittest.TestCase):
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
