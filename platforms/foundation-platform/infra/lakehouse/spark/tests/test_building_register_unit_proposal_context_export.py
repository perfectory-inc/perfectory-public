import sys
import unittest
from datetime import datetime
from pathlib import Path


JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

from building_register_unit_proposal_context_export import (  # noqa: E402
    SCHEMA_VERSION,
    build_context_pack_value,
    build_proposal_source_sql,
    build_run_summary,
    qualified_table,
    row_to_context_json_line,
    validate_args,
)


class BuildingRegisterUnitProposalContextExportTest(unittest.TestCase):
    def test_build_context_pack_uses_silver_unit_identity_and_human_review_policy(self) -> None:
        row = {
            "unit_row_id": "building-register-unit:line-000123",
            "mgm_bldrgst_pk": "unit-pk-1",
            "pnu": "9999900401100890004",
            "register_parcel_key": "9999900401000890004",
            "dong_join_name": "102동",
            "dong_name_raw": "102동",
            "unit_name_raw": "호",
            "unit_number": None,
            "floor_kind": "above_ground",
            "floor_index": 6,
            "floor_number": 6,
            "building_mgm_bldrgst_pk": "building-pk-1",
            "building_link_method": "canonical_dong",
            "normalization_status": "proposal_required",
            "normalization_reason": "no_unit_number",
            "source_snapshot_id": "snapshot-1",
            "bronze_object_key": "bronze/source=hubgokr__building_register_exclusive_unit/file.zip",
            "source_line_number": 123,
            "valid_from_utc": "2026-06-20T00:00:00Z",
            "ingested_at_utc": "2026-07-06T00:00:00Z",
            "row_checksum_sha256": "a" * 64,
        }
        scope_summary = {
            "scope_key": "9999900401000890004|102동|6",
            "accepted_unit_count": 3,
            "min_unit_number": 601,
            "max_unit_number": 603,
            "distinct_unit_number_count": 3,
        }

        pack = build_context_pack_value(row, scope_summary)

        self.assertEqual(pack["schema_version"], SCHEMA_VERSION)
        self.assertEqual(pack["source_system"], "foundation-platform.silver.building_register_units")
        self.assertTrue(pack["context_pack_id"].startswith("unit-context-pack:"))
        self.assertEqual(pack["target"]["target_kind"], "building_register_unit")
        self.assertEqual(pack["target"]["silver_row_id"], "building-register-unit:line-000123")
        self.assertEqual(pack["unit_identity_candidate"]["pnu"], "9999900401100890004")
        self.assertEqual(pack["unit_identity_candidate"]["unit_name_raw"], "호")
        self.assertEqual(pack["current_deterministic_normalization"]["status"], "proposal_required")
        self.assertEqual(pack["current_deterministic_normalization"]["reason"], "no_unit_number")
        self.assertEqual(pack["same_scope_unit_summary"]["accepted_unit_count"], 3)
        self.assertEqual(pack["policy_context"]["decision_owner"], "foundation-platform")
        self.assertEqual(pack["policy_context"]["ai_role"], "proposal_only")
        self.assertEqual(pack["allowed_output_contract"]["required_locale"], "ko-KR")
        self.assertIn("review_message_ko", pack["allowed_output_contract"]["localized_fields"])

    def test_build_context_pack_launch_v1_includes_entity_context_and_second_pass_decision(
        self,
    ) -> None:
        row = sample_row(
            unit_name_raw=".",
            accepted_unit_count=12,
            min_unit_number=3,
            max_unit_number=16,
            distinct_unit_number_count=12,
            same_building_accepted_unit_count=120,
            neighbor_unit_examples=[
                {"floor_index": 1, "unit_number": 3, "unit_name_raw": "3호"},
                {"floor_index": 1, "unit_number": 4, "unit_name_raw": "4호"},
            ],
        )
        scope_summary = scope_summary_from_row(row)

        pack = build_context_pack_value(row, scope_summary)

        self.assertEqual(pack["schema_version"], "foundation-platform.unit_entity_context_pack.v1")
        # entity_context_key는 내부 키(register_parcel_key) 기반 — ADR 0023.
        self.assertEqual(
            pack["entity_context"]["entity_context_key"],
            "9999900401000890004|building-pk-1|102동|1",
        )
        self.assertEqual(pack["entity_context"]["same_scope_accepted_unit_count"], 12)
        self.assertEqual(pack["entity_context"]["same_building_accepted_unit_count"], 120)
        self.assertEqual(len(pack["entity_context"]["neighbor_unit_examples"]), 2)
        self.assertEqual(pack["second_pass_decision"]["status"], "manual_review_required")
        self.assertEqual(
            pack["second_pass_decision"]["reason"],
            "scope_sequence_but_no_numeric_unit_name",
        )
        self.assertFalse(pack["second_pass_decision"]["ai_required"])

    def test_build_context_pack_marks_numeric_with_scope_as_ai_required(self) -> None:
        row = sample_row(
            unit_name_raw="지층+J11864001호",
            accepted_unit_count=29,
            min_unit_number=102,
            max_unit_number=308,
            distinct_unit_number_count=29,
        )

        pack = build_context_pack_value(row, scope_summary_from_row(row))

        self.assertEqual(pack["second_pass_decision"]["status"], "ai_required")
        self.assertEqual(pack["second_pass_decision"]["reason"], "numeric_unit_name_with_context")
        self.assertTrue(pack["second_pass_decision"]["ai_required"])

    def test_build_context_pack_keeps_numeric_without_scope_as_manual_review(self) -> None:
        row = sample_row(unit_name_raw="201201", accepted_unit_count=0)

        pack = build_context_pack_value(row, scope_summary_from_row(row))

        self.assertEqual(pack["second_pass_decision"]["status"], "manual_review_required")
        self.assertEqual(pack["second_pass_decision"]["reason"], "no_scope_sequence")
        self.assertFalse(pack["second_pass_decision"]["ai_required"])

    def test_build_context_pack_carries_unit_label_contract(self) -> None:
        row = sample_row(unit_name_raw="가호", unit_label_ko="가호")

        pack = build_context_pack_value(row, scope_summary_from_row(row))

        self.assertEqual(pack["unit_identity_candidate"]["unit_label_ko"], "가호")
        self.assertEqual(
            pack["current_deterministic_normalization"]["unit_label_ko"], "가호"
        )
        self.assertIn("unit_label_ko", pack["allowed_output_contract"]["machine_fields"])

    def test_build_context_pack_routes_annex_building_empty_row(self) -> None:
        row = sample_row(
            unit_name_raw="",
            building_main_or_annex="부속건축물",
            building_title_unit_count=0,
            building_row_total=1,
            building_empty_row_total=1,
        )

        pack = build_context_pack_value(row, scope_summary_from_row(row))

        self.assertEqual(
            pack["second_pass_decision"]["reason"], "non_unit_annex_building_row"
        )
        self.assertEqual(
            pack["second_pass_decision"]["status"], "manual_review_required"
        )
        self.assertFalse(pack["second_pass_decision"]["ai_required"])

    def test_build_context_pack_keeps_annex_with_title_units_in_baseline(self) -> None:
        # An annex building whose card claims units is NOT safe to write off:
        # the census T2 rule is gated on card unit-count 0.
        row = sample_row(
            unit_name_raw="",
            building_main_or_annex="부속건축물",
            building_title_unit_count=1,
            building_row_total=1,
            building_empty_row_total=1,
        )

        pack = build_context_pack_value(row, scope_summary_from_row(row))

        self.assertEqual(
            pack["second_pass_decision"]["reason"],
            "empty_unit_name_and_no_scope_sequence",
        )

    def test_build_context_pack_marks_single_unit_building_candidate(self) -> None:
        row = sample_row(
            unit_name_raw="",
            building_main_or_annex="주건축물",
            building_title_unit_count=1,
            building_row_total=1,
            building_empty_row_total=1,
        )

        pack = build_context_pack_value(row, scope_summary_from_row(row))

        self.assertEqual(
            pack["second_pass_decision"]["reason"], "single_unit_building_candidate"
        )
        self.assertFalse(pack["second_pass_decision"]["ai_required"])

    def test_build_context_pack_confirms_unnamed_unit_group(self) -> None:
        row = sample_row(
            unit_name_raw="",
            building_main_or_annex="주건축물",
            building_title_unit_count=3,
            building_row_total=3,
            building_empty_row_total=3,
        )

        pack = build_context_pack_value(row, scope_summary_from_row(row))

        self.assertEqual(
            pack["second_pass_decision"]["reason"], "unnamed_units_count_confirmed"
        )
        self.assertFalse(pack["second_pass_decision"]["ai_required"])

    def test_build_context_pack_keeps_plain_empty_reason_without_title_evidence(
        self,
    ) -> None:
        # No title attrs joined (e.g. unresolved building): baseline reason stays.
        row = sample_row(unit_name_raw="")

        pack = build_context_pack_value(row, scope_summary_from_row(row))

        self.assertEqual(
            pack["second_pass_decision"]["reason"],
            "empty_unit_name_and_no_scope_sequence",
        )

    def test_proposal_source_sql_joins_building_row_totals(self) -> None:
        sql = build_proposal_source_sql("`r2`.`silver`.`building_register_units`")

        self.assertIn("building_row_total", sql)
        self.assertIn("building_empty_row_total", sql)
        self.assertIn("GROUP BY building_mgm_bldrgst_pk", sql)

    def test_qualified_table_quotes_catalog_namespace_and_table(self) -> None:
        self.assertEqual(
            qualified_table("r2", "silver", "building_register_units"),
            "`r2`.`silver`.`building_register_units`",
        )

    def test_proposal_source_sql_filters_only_proposal_required_with_scope_stats(self) -> None:
        sql = build_proposal_source_sql("`r2`.`silver`.`building_register_units`")

        self.assertIn("normalization_status = 'proposal_required'", sql)
        self.assertIn("normalization_application_id IS NULL", sql)
        self.assertIn("normalization_status = 'accepted'", sql)
        self.assertIn("GROUP BY __scope_key", sql)
        # scope/building 키는 내부 키(register_parcel_key)로 조립 — pnu는 블록에서 null (ADR 0023)
        self.assertIn("register_parcel_key,", sql)
        self.assertIn("accepted_unit_count", sql)
        self.assertIn("distinct_unit_number_count", sql)
        self.assertIn("__building_key", sql)
        self.assertIn("same_building_accepted_unit_count", sql)
        self.assertIn("neighbor_unit_examples", sql)
        self.assertIn("__example_rank <= 5", sql)
        self.assertNotIn("INSERT", sql.upper())
        self.assertNotIn("CREATE TABLE", sql.upper())

    def test_run_summary_records_proposal_artifact_not_canonical_write(self) -> None:
        summary = build_run_summary(
            output_path="/workspace/target/remote-lakehouse/ai/building_register_unit_proposals-full",
            proposal_count=29259,
            source_table="r2.silver.building_register_units",
        )

        self.assertEqual(
            summary["schema_version"],
            "foundation-platform.building_register_unit_proposal_context_export_summary.v1",
        )
        self.assertEqual(summary["proposal_count"], 29259)
        self.assertEqual(summary["target"]["kind"], "ai_proposal_input_jsonl")
        self.assertEqual(summary["canonical_write"], False)

    def test_row_to_context_json_line_uses_joined_scope_stats(self) -> None:
        row = FakeSparkRow(
            unit_row_id="building-register-unit:line-000124",
            mgm_bldrgst_pk="unit-pk-2",
            pnu="9999900401100890004",
            register_parcel_key="9999900401000890004",
            dong_join_name=None,
            dong_name_raw="",
            unit_name_raw="",
            unit_number=None,
            floor_kind="above_ground",
            floor_index=1,
            floor_number=1,
            building_mgm_bldrgst_pk=None,
            building_link_method="unresolved",
            normalization_status="proposal_required",
            normalization_reason="empty_unit_name",
            source_snapshot_id="snapshot-1",
            bronze_object_key="bronze/source=hubgokr__building_register_exclusive_unit/file.zip",
            source_line_number=124,
            valid_from_utc="2026-06-20T00:00:00Z",
            ingested_at_utc="2026-07-06T00:00:00Z",
            row_checksum_sha256="b" * 64,
            __scope_key="9999900401000890004||1",
            accepted_unit_count=0,
            min_unit_number=None,
            max_unit_number=None,
            distinct_unit_number_count=0,
        )

        line = row_to_context_json_line(row)

        self.assertNotIn("\n", line)
        self.assertIn('"schema_version":"foundation-platform.unit_entity_context_pack.v1"', line)
        self.assertIn('"second_pass_decision"', line)
        self.assertIn('"entity_context"', line)
        self.assertIn('"scope_key":"9999900401000890004||1"', line)
        self.assertIn('"canonical_write_path":"proposal_inbox_human_review_then_command"', line)

    def test_row_to_context_json_line_serializes_naive_spark_timestamps_as_utc(self) -> None:
        row = FakeSparkRow(
            unit_row_id="building-register-unit:line-000125",
            mgm_bldrgst_pk="unit-pk-3",
            pnu="9999900401100890004",
            register_parcel_key="9999900401000890004",
            dong_join_name=None,
            dong_name_raw="",
            unit_name_raw="",
            unit_number=None,
            floor_kind="above_ground",
            floor_index=1,
            floor_number=1,
            building_mgm_bldrgst_pk=None,
            building_link_method="unresolved",
            normalization_status="proposal_required",
            normalization_reason="empty_unit_name",
            source_snapshot_id="snapshot-1",
            bronze_object_key="bronze/source=hubgokr__building_register_exclusive_unit/file.zip",
            source_line_number=125,
            valid_from_utc=datetime(2026, 6, 20, 0, 0, 0),
            ingested_at_utc=datetime(2026, 7, 6, 0, 0, 0),
            row_checksum_sha256="c" * 64,
            __scope_key="9999900401000890004||1",
            accepted_unit_count=0,
            min_unit_number=None,
            max_unit_number=None,
            distinct_unit_number_count=0,
        )

        line = row_to_context_json_line(row)

        self.assertIn('"valid_from_utc":"2026-06-20T00:00:00Z"', line)
        self.assertIn('"ingested_at_utc":"2026-07-06T00:00:00Z"', line)

    def test_validate_args_requires_positive_output_partitions(self) -> None:
        with self.assertRaisesRegex(ValueError, "output partitions"):
            validate_args(FakeArgs(output="/tmp/out", output_partitions=0))

    def test_validate_args_requires_output_path(self) -> None:
        with self.assertRaisesRegex(ValueError, "output"):
            validate_args(FakeArgs(output=""))

    def test_validate_args_allows_parquet_source_without_iceberg_env(self) -> None:
        validate_args(
            FakeArgs(input_parquet="/workspace/target/lakehouse/silver_handoff/units")
        )


class FakeSparkRow:
    def __init__(self, **values: object) -> None:
        self.values = values

    def asDict(self) -> dict[str, object]:
        return dict(self.values)


class FakeArgs:
    def __init__(self, **values: object) -> None:
        self.catalog = "r2"
        self.namespace = "silver"
        self.table = "building_register_units"
        self.output = "/tmp/building-register-unit-proposals"
        self.summary_output = None
        self.output_partitions = 1
        self.expected_proposal_count = None
        self.input_parquet = None
        self.__dict__.update(values)


def sample_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "unit_row_id": "building-register-unit:line-000123",
        "mgm_bldrgst_pk": "unit-pk-1",
        "pnu": "9999900401100890004",
        "register_parcel_key": "9999900401000890004",
        "dong_join_name": "102동",
        "dong_name_raw": "102동",
        "unit_name_raw": "",
        "unit_number": None,
        "unit_label_ko": None,
        "floor_kind": "above_ground",
        "floor_index": 1,
        "floor_number": 1,
        "building_mgm_bldrgst_pk": "building-pk-1",
        "building_link_method": "canonical_dong",
        "normalization_status": "proposal_required",
        "normalization_reason": "no_unit_number",
        "source_snapshot_id": "snapshot-1",
        "bronze_object_key": "bronze/source=hubgokr__building_register_exclusive_unit/file.zip",
        "source_line_number": 123,
        "valid_from_utc": "2026-06-20T00:00:00Z",
        "ingested_at_utc": "2026-07-06T00:00:00Z",
        "row_checksum_sha256": "a" * 64,
        "__scope_key": "9999900401000890004|102동|1",
        "accepted_unit_count": 0,
        "min_unit_number": None,
        "max_unit_number": None,
        "distinct_unit_number_count": 0,
        "same_building_accepted_unit_count": 0,
        "neighbor_unit_examples": [],
    }
    row.update(overrides)
    return row


def scope_summary_from_row(row: dict[str, object]) -> dict[str, object]:
    return {
        "scope_key": row["__scope_key"],
        "accepted_unit_count": row["accepted_unit_count"],
        "min_unit_number": row["min_unit_number"],
        "max_unit_number": row["max_unit_number"],
        "distinct_unit_number_count": row["distinct_unit_number_count"],
        "same_building_accepted_unit_count": row["same_building_accepted_unit_count"],
        "neighbor_unit_examples": row["neighbor_unit_examples"],
    }


if __name__ == "__main__":
    unittest.main()
