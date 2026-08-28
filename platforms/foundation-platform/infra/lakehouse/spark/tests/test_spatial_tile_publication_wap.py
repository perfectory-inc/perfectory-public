import copy
import json
import io
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch


SPARK_DIR = Path(__file__).resolve().parents[1]
JOBS_DIR = SPARK_DIR / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import spatial_tile_publication_wap as wap  # noqa: E402

from platform_contracts import (  # noqa: E402
    load_lakehouse_contract,
    partition_spec_sql,
)

from spatial_tile_publication_wap import (  # noqa: E402
    ICEBERG_PACKAGES,
    LOGICAL_CONTRACT,
    PROBE_CATALOG_BUCKET,
    PROBE_NAMESPACE,
    PROBE_TABLE,
    SparkWapRuntime,
    WapEvidence,
    branch_name_for_release,
    branch_table,
    build_create_table_sql,
    build_create_branch_sql,
    build_fast_forward_sql,
    build_prepare_statements,
    build_probe_fixture,
    configure_spark_builder,
    current_parcel_predicate,
    execute_command,
    historical_branch_name_for_release,
    live_success_line,
    offline_capability_line,
    parse_args,
    parse_positive_snapshot,
    redact_secret_values,
    validate_candidate_invariants,
    validate_closed_history_rows,
    validate_evidence,
    validate_geometry_rows,
    validate_identifier,
    validate_probe_target,
)


class FakeSparkBuilder:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.calls: list[tuple[str, str]] = []

    def config(self, key: str, value: str) -> "FakeSparkBuilder":
        self.values[key] = value
        self.calls.append((key, value))
        return self


class FakeWapRuntime:
    physical_table = f"r2.{PROBE_NAMESPACE}.{PROBE_TABLE}"
    catalog_bucket = PROBE_CATALOG_BUCKET

    def __init__(self) -> None:
        self.calls: list[str] = []

    def ensure_probe_table(self) -> None:
        self.calls.append("ensure_probe_table")

    def seed_baseline(self, _fixture: object) -> int:
        self.calls.append("seed_baseline")
        return 40

    def advance_main(self, _fixture: object, historical_base_snapshot: int) -> int:
        self.calls.append(f"advance-main:{historical_base_snapshot}")
        return 41

    def prepare_candidate(
        self,
        _fixture: object,
        _historical_branch: str,
        _branch: str,
        historical_base_snapshot: int,
        base_snapshot: int,
    ) -> tuple[int, int]:
        self.calls.append(f"prepare:{historical_base_snapshot}:{base_snapshot}")
        return (40, 42)

    def validate_candidate(
        self,
        _fixture: object,
        _historical_branch: str,
        _branch: str,
        historical_base_snapshot: int,
        base_snapshot: int,
    ) -> tuple[int, int]:
        self.calls.append(f"validate:{historical_base_snapshot}:{base_snapshot}")
        return (40, 42)

    def fast_forward_candidate(
        self,
        _fixture: object,
        _historical_branch: str,
        _branch: str,
        historical_base_snapshot: int,
        base_snapshot: int,
    ) -> tuple[int, int]:
        self.calls.append(f"fast-forward:{historical_base_snapshot}:{base_snapshot}")
        return (40, 42)


class RecordingSpark:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def sql(self, statement: str) -> object:
        self.statements.append(statement)
        return object()


class SpatialTilePublicationWapTest(unittest.TestCase):
    def test_identifiers_snapshots_and_release_branch_are_strict(self) -> None:
        validate_identifier("namespace", "tiles_slice_proof")
        with self.assertRaisesRegex(ValueError, "simple identifier"):
            validate_identifier("namespace", "silver; DROP TABLE parcels")

        self.assertEqual(parse_positive_snapshot("41"), 41)
        for invalid in ("0", "-1", "not-a-snapshot"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                parse_positive_snapshot(invalid)

        self.assertEqual(
            branch_name_for_release("018f1111-1111-7111-8111-111111111111"),
            "tile_018f1111111171118111111111111111",
        )
        self.assertEqual(
            historical_branch_name_for_release(
                "018f1111-1111-7111-8111-111111111111"
            ),
            "history_018f1111111171118111111111111111",
        )
        with self.assertRaisesRegex(ValueError, "UUID"):
            branch_name_for_release("release-one")
        with self.assertRaisesRegex(ValueError, "nil"):
            branch_name_for_release("00000000-0000-0000-0000-000000000000")

    def test_branch_sql_is_exact_snapshot_retained_and_branch_qualified(self) -> None:
        branch = branch_name_for_release("018f1111-1111-7111-8111-111111111111")
        self.assertEqual(
            build_create_branch_sql("r2", "tiles_slice_proof", "parcels", branch, 41),
            "ALTER TABLE r2.tiles_slice_proof.parcels "
            "CREATE BRANCH `tile_018f1111111171118111111111111111` "
            "AS OF VERSION 41 RETAIN 7 DAYS",
        )
        self.assertEqual(
            branch_table("r2", "tiles_slice_proof", "parcels", branch),
            "r2.tiles_slice_proof.parcels."
            "branch_tile_018f1111111171118111111111111111",
        )
        self.assertEqual(
            build_fast_forward_sql("r2", "tiles_slice_proof", "parcels", branch),
            "CALL r2.system.fast_forward("
            "'tiles_slice_proof.parcels', 'main', "
            "'tile_018f1111111171118111111111111111')",
        )

    def test_provider_branch_metadata_proves_historical_base_and_retention(self) -> None:
        expected_retention_ms = 7 * 24 * 60 * 60 * 1000
        valid_reference = [
            {
                "snapshot_id": 41,
                "max_reference_age_in_ms": expected_retention_ms,
            }
        ]
        try:
            self.assertEqual(
                wap.validate_branch_reference(
                    valid_reference,
                    expected_snapshot=41,
                ),
                41,
            )
            wap.validate_historical_branch_invariants(
                valid_reference,
                historical_base_snapshot=41,
                publication_base_snapshot=42,
                sentinel_current_count=0,
            )
        except AttributeError as error:
            self.fail(f"branch metadata proof helper is missing: {error}")

        ignored_as_of = [
            {
                "snapshot_id": 42,
                "max_reference_age_in_ms": expected_retention_ms,
            }
        ]
        with self.assertRaisesRegex(ValueError, "AS OF"):
            wap.validate_historical_branch_invariants(
                ignored_as_of,
                historical_base_snapshot=41,
                publication_base_snapshot=42,
                sentinel_current_count=0,
            )

        for retention in (None, 0, expected_retention_ms - 1):
            provider_rows = [
                {
                    "snapshot_id": 41,
                    "max_reference_age_in_ms": retention,
                }
            ]
            with self.subTest(retention=retention), self.assertRaisesRegex(
                ValueError,
                "retention",
            ):
                wap.validate_branch_reference(
                    provider_rows,
                    expected_snapshot=41,
                )

        with self.assertRaisesRegex(ValueError, "sentinel"):
            wap.validate_historical_branch_invariants(
                valid_reference,
                historical_base_snapshot=41,
                publication_base_snapshot=42,
                sentinel_current_count=1,
            )
        with self.assertRaisesRegex(ValueError, "older"):
            wap.validate_historical_branch_invariants(
                valid_reference,
                historical_base_snapshot=41,
                publication_base_snapshot=41,
                sentinel_current_count=0,
            )

    def test_probe_is_restricted_to_the_dedicated_physical_table(self) -> None:
        validate_probe_target("r2", PROBE_NAMESPACE, PROBE_TABLE)
        for catalog, namespace, table in (
            ("other", PROBE_NAMESPACE, PROBE_TABLE),
            ("r2", "silver", PROBE_TABLE),
            ("r2", PROBE_NAMESPACE, "parcel_boundaries"),
            ("r2", "gold", "complex_catalog"),
        ):
            with self.subTest(catalog=catalog, namespace=namespace, table=table):
                with self.assertRaisesRegex(ValueError, "dedicated"):
                    validate_probe_target(catalog, namespace, table)

    def test_every_command_rejects_non_probe_targets(self) -> None:
        release = "018f1111-1111-7111-8111-111111111111"
        for command in ("prepare", "validate", "fast-forward", "probe"):
            arguments = [
                command,
                "--namespace",
                "silver",
                "--table",
                "parcel_boundaries",
                "--release-id",
                release,
            ]
            if command != "probe":
                arguments.extend(
                    (
                        "--historical-base-snapshot",
                        "40",
                        "--base-snapshot",
                        "41",
                    )
                )
            with self.subTest(command=command):
                with self.assertRaisesRegex(ValueError, "dedicated"):
                    parse_args(arguments)

    def test_every_command_rejects_a_non_r2_catalog(self) -> None:
        release = "018f1111-1111-7111-8111-111111111111"
        for command in ("prepare", "validate", "fast-forward", "probe"):
            arguments = [
                command,
                "--catalog",
                "other",
                "--namespace",
                PROBE_NAMESPACE,
                "--table",
                PROBE_TABLE,
                "--release-id",
                release,
            ]
            if command != "probe":
                arguments.extend(
                    (
                        "--historical-base-snapshot",
                        "40",
                        "--base-snapshot",
                        "41",
                    )
                )
            with self.subTest(command=command):
                with self.assertRaisesRegex(ValueError, "dedicated"):
                    parse_args(arguments)

    def test_current_predicate_comes_from_the_rust_derived_contract(self) -> None:
        self.assertEqual(LOGICAL_CONTRACT, "silver.parcel_boundaries")
        self.assertEqual(current_parcel_predicate(), "valid_to_utc IS NULL")

        source = (JOBS_DIR / "spatial_tile_publication_wap.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("valid_to_utc IS NULL", source)
        self.assertIn("current_row_predicate", source)

    def test_candidate_invariants_reject_wrong_cardinality_or_history_leak(self) -> None:
        validate_candidate_invariants(
            before={"add": 0, "replace": 1, "delete": 1},
            after={"add": 1, "replace": 1, "delete": 0},
            duplicate_current_pnu_count=0,
            superseded_current_row_count=0,
            invalid_geometry_count=0,
        )
        invalid_cases = (
            ({"add": 1, "replace": 1, "delete": 1}, 0, 0, 0),
            ({"add": 0, "replace": 2, "delete": 1}, 0, 0, 0),
            ({"add": 0, "replace": 1, "delete": 0}, 0, 0, 0),
            ({"add": 0, "replace": 1, "delete": 1}, 1, 0, 0),
            ({"add": 0, "replace": 1, "delete": 1}, 0, 1, 0),
            ({"add": 0, "replace": 1, "delete": 1}, 0, 0, 1),
        )
        for before, duplicates, leaked, invalid_geometry in invalid_cases:
            with self.subTest(
                before=before,
                duplicates=duplicates,
                leaked=leaked,
                invalid_geometry=invalid_geometry,
            ):
                with self.assertRaises(ValueError):
                    validate_candidate_invariants(
                        before=before,
                        after={"add": 1, "replace": 1, "delete": 0},
                        duplicate_current_pnu_count=duplicates,
                        superseded_current_row_count=leaked,
                        invalid_geometry_count=invalid_geometry,
                    )

    def test_evidence_is_strict_expected_and_secret_free(self) -> None:
        branch = branch_name_for_release("018f1111-1111-7111-8111-111111111111")
        payload = {
            "schema_version": "foundation-platform.spatial_tile_wap_evidence.v1",
            "logical_contract": LOGICAL_CONTRACT,
            "physical_table": f"r2.{PROBE_NAMESPACE}.{PROBE_TABLE}",
            "catalog_bucket": "perfectory-tiles-slice-proof",
            "historical_base_snapshot": 40,
            "base_snapshot": 41,
            "historical_branch_snapshot": 40,
            "branch_snapshot": 42,
            "historical_branch_name": (
                "history_018f1111111171118111111111111111"
            ),
            "branch_name": branch,
            "result": "probe_ok",
            "provider": "cloudflare-r2-data-catalog",
            "historical_base_isolation": "ok",
            "branch_isolation": "ok",
            "retention": "ok",
            "fast_forward": "ok",
        }
        raw = json.dumps(payload, sort_keys=True)
        self.assertNotIn("catalog_token", raw.lower())
        try:
            expected = validate_evidence(
                raw,
                expected_table=payload["physical_table"],
                expected_base_snapshot=41,
                expected_branch=branch,
                expected_result="probe_ok",
                expected_fast_forward="ok",
            )
        except ValueError as error:
            self.fail(f"dedicated-bucket evidence must validate: {error}")
        self.assertEqual(expected.to_dict(), payload)

        for field, value in (
            (
                "schema_version",
                "foundation-platform.spatial_tile_wap_evidence.v2",
            ),
            ("physical_table", "r2.silver.parcel_boundaries"),
            ("catalog_bucket", "foundation-platform-lakehouse-prod"),
            ("historical_base_snapshot", 41),
            ("historical_branch_snapshot", 41),
            ("historical_branch_name", "history_wrong"),
            ("base_snapshot", 99),
            ("branch_snapshot", 40),
            ("branch_name", "tile_wrong"),
            ("result", "failed"),
            ("historical_base_isolation", "not_proven"),
            ("branch_isolation", "not_proven"),
            ("retention", "not_proven"),
        ):
            mismatch = expected.to_dict()
            mismatch[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_evidence(
                    json.dumps(mismatch),
                    expected_table=expected.physical_table,
                    expected_base_snapshot=41,
                    expected_branch=branch,
                    expected_result="probe_ok",
                    expected_fast_forward="ok",
                )

        unexpected = expected.to_dict()
        unexpected["catalog_token"] = "must-never-be-recorded"
        with self.assertRaises(ValueError):
            validate_evidence(
                json.dumps(unexpected),
                expected_table=expected.physical_table,
                expected_base_snapshot=41,
                expected_branch=branch,
                expected_result="probe_ok",
                expected_fast_forward="ok",
            )

        for field in (
            "schema_version",
            "catalog_bucket",
            "historical_base_snapshot",
            "historical_branch_snapshot",
            "historical_branch_name",
            "historical_base_isolation",
            "retention",
        ):
            legacy = expected.to_dict()
            legacy.pop(field)
            with self.subTest(legacy_missing=field), self.assertRaisesRegex(
                ValueError,
                "strict schema",
            ):
                validate_evidence(
                    json.dumps(legacy),
                    expected_table=expected.physical_table,
                    expected_base_snapshot=41,
                    expected_branch=branch,
                    expected_result="probe_ok",
                    expected_fast_forward="ok",
                )

        for field, value in (
            ("historical_base_snapshot", 40.0),
            ("historical_base_snapshot", 40.9),
            ("historical_base_snapshot", "40"),
            ("historical_base_snapshot", True),
            ("base_snapshot", 41.0),
            ("base_snapshot", 41.9),
            ("base_snapshot", "41"),
            ("base_snapshot", True),
            ("historical_branch_snapshot", 40.0),
            ("historical_branch_snapshot", 40.9),
            ("historical_branch_snapshot", "40"),
            ("historical_branch_snapshot", True),
            ("branch_snapshot", 42.0),
            ("branch_snapshot", 42.9),
            ("branch_snapshot", "42"),
            ("branch_snapshot", True),
        ):
            malformed_snapshot = expected.to_dict()
            malformed_snapshot[field] = value
            with self.subTest(field=field, value=value), self.assertRaisesRegex(
                ValueError,
                "snapshot",
            ):
                validate_evidence(
                    json.dumps(malformed_snapshot),
                    expected_table=expected.physical_table,
                    expected_base_snapshot=41,
                    expected_branch=branch,
                    expected_result="probe_ok",
                    expected_fast_forward="ok",
                )

    def test_evidence_rejects_impossible_or_unexpected_result_status_pairs(self) -> None:
        branch = branch_name_for_release("018f1111-1111-7111-8111-111111111111")
        base = WapEvidence(
            schema_version="foundation-platform.spatial_tile_wap_evidence.v1",
            logical_contract=LOGICAL_CONTRACT,
            physical_table=f"r2.{PROBE_NAMESPACE}.{PROBE_TABLE}",
            catalog_bucket=PROBE_CATALOG_BUCKET,
            historical_base_snapshot=40,
            base_snapshot=41,
            historical_branch_snapshot=40,
            branch_snapshot=42,
            historical_branch_name=(
                "history_018f1111111171118111111111111111"
            ),
            branch_name=branch,
            result="probe_ok",
            provider="cloudflare-r2-data-catalog",
            historical_base_isolation="ok",
            branch_isolation="ok",
            retention="ok",
            fast_forward="ok",
        )
        cases = (
            ("prepared", "ok", "prepared", "not_requested"),
            ("validated", "ok", "validated", "not_requested"),
            ("fast_forwarded", "not_requested", "fast_forwarded", "ok"),
            ("probe_ok", "not_requested", "probe_ok", "ok"),
            ("probe_ok", "ok", "fast_forwarded", "ok"),
            ("probe_ok", "ok", "probe_ok", "not_requested"),
        )

        for result, fast_forward, expected_result, expected_fast_forward in cases:
            payload = {
                **base.to_dict(),
                "result": result,
                "fast_forward": fast_forward,
            }
            with self.subTest(
                result=result,
                fast_forward=fast_forward,
                expected_result=expected_result,
                expected_fast_forward=expected_fast_forward,
            ), self.assertRaises(ValueError):
                validate_evidence(
                    json.dumps(payload),
                    expected_table=base.physical_table,
                    expected_base_snapshot=41,
                    expected_branch=branch,
                    expected_result=expected_result,
                    expected_fast_forward=expected_fast_forward,
                )

    def test_evidence_contract_fails_closed_on_unknown_custom_rule(self) -> None:
        branch = branch_name_for_release("018f1111-1111-7111-8111-111111111111")
        evidence = WapEvidence(
            schema_version="foundation-platform.spatial_tile_wap_evidence.v1",
            logical_contract=LOGICAL_CONTRACT,
            physical_table=f"r2.{PROBE_NAMESPACE}.{PROBE_TABLE}",
            catalog_bucket=PROBE_CATALOG_BUCKET,
            historical_base_snapshot=40,
            base_snapshot=41,
            historical_branch_snapshot=40,
            branch_snapshot=42,
            historical_branch_name=(
                "history_018f1111111171118111111111111111"
            ),
            branch_name=branch,
            result="probe_ok",
            provider="cloudflare-r2-data-catalog",
            historical_base_isolation="ok",
            branch_isolation="ok",
            retention="ok",
            fast_forward="ok",
        )
        contract = copy.deepcopy(wap.EVIDENCE_CONTRACT)
        contract["x-perfectory-cross-field-invariants"].append(
            {"op": "invented", "fields": ["base_snapshot"]}
        )
        with self.assertRaisesRegex(ValueError, "unsupported.*invented"):
            wap.validate_evidence_payload(evidence.to_dict(), contract)

    def test_evidence_contract_rejects_malformed_required_fields(self) -> None:
        contract = copy.deepcopy(wap.EVIDENCE_CONTRACT)
        contract["required"].remove("schema_version")
        with self.assertRaisesRegex(ValueError, "required.*properties"):
            wap.validate_evidence_contract(contract)

    def test_evidence_contract_fails_closed_on_malformed_rule_shapes(self) -> None:
        mutations = (
            lambda contract: contract.update({"title": 7}),
            lambda contract: contract["allOf"][0]["if"].update(
                {"description": "must not be ignored"}
            ),
            lambda contract: contract["allOf"][0]["if"]["properties"][
                "result"
            ].update({"minimum": 1}),
            lambda contract: contract["allOf"][0]["then"]["properties"][
                "fast_forward"
            ].update({"enum": ["not_requested"]}),
            lambda contract: contract["properties"][
                "historical_base_snapshot"
            ].update({"const": True}),
            lambda contract: contract["x-perfectory-branch-pair"][
                "historical"
            ].update({"field": "base_snapshot"}),
            lambda contract: contract[
                "x-perfectory-cross-field-invariants"
            ][0].update(
                {"fields": ["branch_name", "historical_base_snapshot"]}
            ),
        )
        for mutate in mutations:
            contract = copy.deepcopy(wap.EVIDENCE_CONTRACT)
            mutate(contract)
            with self.subTest(contract=contract), self.assertRaises(ValueError):
                wap.validate_evidence_contract(contract)

    def test_evidence_write_is_atomic_create_only_and_cleans_failed_temp(self) -> None:
        runtime = FakeWapRuntime()
        evidence = execute_command(
            parse_args(
                [
                    "probe",
                    "--namespace",
                    PROBE_NAMESPACE,
                    "--table",
                    PROBE_TABLE,
                    "--release-id",
                    "018f1111-1111-7111-8111-111111111111",
                ]
            ),
            runtime,
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            final = root / "evidence.json"
            wap.write_evidence_create_new_atomic(final, evidence)
            original = final.read_bytes()
            with self.assertRaises(FileExistsError):
                wap.write_evidence_create_new_atomic(final, evidence)
            self.assertEqual(final.read_bytes(), original)
            self.assertEqual(list(root.glob(".evidence.json.*.tmp")), [])

            failed = root / "failed.json"
            with patch(
                "spatial_tile_wap_evidence.os.link",
                side_effect=OSError("install failed"),
            ), self.assertRaisesRegex(OSError, "install failed"):
                wap.write_evidence_create_new_atomic(failed, evidence)
            self.assertFalse(failed.exists())
            self.assertEqual(list(root.glob(".failed.json.*.tmp")), [])

    def test_offline_tests_never_claim_provider_capability(self) -> None:
        self.assertEqual(
            offline_capability_line(),
            "provider_capability=not_proven_offline",
        )

    def test_live_success_line_is_emitted_only_for_complete_probe(self) -> None:
        runtime = FakeWapRuntime()
        args = parse_args(
            [
                "probe",
                "--namespace",
                PROBE_NAMESPACE,
                "--table",
                PROBE_TABLE,
                "--release-id",
                "018f1111-1111-7111-8111-111111111111",
            ]
        )
        evidence = execute_command(args, runtime)
        self.assertEqual(
            live_success_line(evidence),
            "provider=cloudflare-r2-data-catalog "
            "historical_base_isolation=ok branch_isolation=ok "
            "retention=ok fast_forward=ok",
        )
        self.assertEqual(evidence.catalog_bucket, PROBE_CATALOG_BUCKET)
        prepared = WapEvidence(
            **{**evidence.to_dict(), "result": "prepared", "fast_forward": "not_requested"}
        )
        with self.assertRaisesRegex(ValueError, "probe_ok"):
            live_success_line(prepared)
        for field in (
            "historical_base_isolation",
            "branch_isolation",
            "retention",
        ):
            incomplete = WapEvidence(
                **{**evidence.to_dict(), field: "not_proven"}
            )
            with self.subTest(field=field), self.assertRaisesRegex(
                ValueError,
                "probe_ok",
            ):
                live_success_line(incomplete)

    def test_probe_table_ddl_is_derived_from_the_logical_contract(self) -> None:
        sql = build_create_table_sql("r2", PROBE_NAMESPACE, PROBE_TABLE)
        compact = " ".join(sql.split())
        self.assertIn(
            "CREATE TABLE IF NOT EXISTS "
            f"r2.{PROBE_NAMESPACE}.{PROBE_TABLE}",
            compact,
        )
        self.assertIn("boundary_id STRING", compact)
        self.assertIn("geometry_wkb BINARY", compact)
        self.assertIn("valid_to_utc TIMESTAMP", compact)
        # Read off the contract rather than spelled here. This assertion used to carry its own
        # copy of the partition spec, so changing the contract left a test insisting on the old
        # layout — the check that claims the DDL comes from the contract was the one place that
        # did not (root ADR-0063).
        contract = load_lakehouse_contract(wap.LOGICAL_CONTRACT)
        self.assertIn(
            f"PARTITIONED BY ({partition_spec_sql(contract)})",
            compact,
        )
        self.assertNotIn("DROP ", compact.upper())
        self.assertNotIn("DELETE FROM", compact.upper())

    def test_spark_runtime_creates_only_the_dedicated_contract_table(self) -> None:
        spark = RecordingSpark()
        runtime = SparkWapRuntime(spark, "r2", PROBE_NAMESPACE, PROBE_TABLE)
        runtime.ensure_probe_table()
        rendered = "\n".join(spark.statements)
        self.assertEqual(len(spark.statements), 2)
        self.assertIn(
            f"CREATE NAMESPACE IF NOT EXISTS r2.{PROBE_NAMESPACE}",
            rendered,
        )
        self.assertIn(
            f"CREATE TABLE IF NOT EXISTS r2.{PROBE_NAMESPACE}.{PROBE_TABLE}",
            " ".join(rendered.split()),
        )
        self.assertNotIn("DROP ", rendered.upper())
        self.assertNotIn("DELETE FROM", rendered.upper())

    def test_fixture_is_unique_scd2_and_has_valid_polygon_evidence(self) -> None:
        fixture = build_probe_fixture("018f1111-1111-7111-8111-111111111111")
        self.assertEqual(len(set(fixture.pnus)), 3)
        self.assertEqual(
            len(set((*fixture.pnus, fixture.main_advance_row.pnu))),
            4,
        )
        self.assertEqual(len(fixture.baseline_rows), 3)
        self.assertIsNotNone(fixture.baseline_rows[0].valid_to_utc)
        self.assertIsNone(fixture.replacement_row.valid_to_utc)
        self.assertNotEqual(
            fixture.replacement_row.geometry_checksum_sha256,
            fixture.replace_active_row.geometry_checksum_sha256,
        )
        fixture_rows = (
            *fixture.baseline_rows,
            fixture.add_row,
            fixture.replacement_row,
            fixture.main_advance_row,
        )
        for row in fixture_rows:
            self.assertGreaterEqual(row.bbox_min_x, 127.123)
            self.assertLess(row.bbox_max_x, 127.1239)
            self.assertGreaterEqual(row.bbox_min_y, 36.123)
            self.assertLess(row.bbox_max_y, 36.1239)
        validate_geometry_rows(
            [
                fixture.add_row.to_dict(),
                fixture.replacement_row.to_dict(),
                fixture.main_advance_row.to_dict(),
            ]
        )
        corrupted = fixture.add_row.to_dict()
        corrupted["geometry_checksum_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "geometry"):
            validate_geometry_rows([corrupted])

    def test_closed_history_requires_exact_scd2_intervals(self) -> None:
        fixture = build_probe_fixture("018f1111-1111-7111-8111-111111111111")
        historical, replace_active, delete_active = fixture.baseline_rows
        rows = [
            {
                "boundary_id": historical.boundary_id,
                "valid_from_utc": historical.valid_from_utc,
                "valid_to_utc": historical.valid_to_utc,
            },
            {
                "boundary_id": replace_active.boundary_id,
                "valid_from_utc": replace_active.valid_from_utc,
                "valid_to_utc": fixture.transition_utc,
            },
            {
                "boundary_id": delete_active.boundary_id,
                "valid_from_utc": delete_active.valid_from_utc,
                "valid_to_utc": fixture.transition_utc,
            },
        ]
        validate_closed_history_rows(rows, fixture)

        rows[1]["valid_to_utc"] = "2099-01-02T00:00:01Z"
        with self.assertRaisesRegex(ValueError, "SCD2"):
            validate_closed_history_rows(rows, fixture)

    def test_prepare_statements_only_mutate_the_candidate_branch(self) -> None:
        release = "018f1111-1111-7111-8111-111111111111"
        fixture = build_probe_fixture(release)
        branch = branch_name_for_release(release)
        candidate = branch_table("r2", PROBE_NAMESPACE, PROBE_TABLE, branch)
        statements = build_prepare_statements(
            "r2", PROBE_NAMESPACE, PROBE_TABLE, branch, fixture
        )
        rendered = "\n".join(statements)
        self.assertEqual(len(statements), 4)
        self.assertTrue(all(candidate in statement for statement in statements))
        self.assertIn(f"INSERT INTO {candidate}", statements[0])
        self.assertIn(f"UPDATE {candidate}", statements[1])
        self.assertIn(f"INSERT INTO {candidate}", statements[2])
        self.assertIn(f"UPDATE {candidate}", statements[3])
        self.assertNotIn("DELETE FROM", rendered.upper())
        self.assertIn(current_parcel_predicate(), rendered)

    def test_commands_are_separate_and_probe_guard_is_not_optional(self) -> None:
        common = [
            "--namespace",
            PROBE_NAMESPACE,
            "--table",
            PROBE_TABLE,
            "--release-id",
            "018f1111-1111-7111-8111-111111111111",
        ]
        snapshots = [
            "--historical-base-snapshot",
            "40",
            "--base-snapshot",
            "41",
        ]
        prepare = parse_args(["prepare", *common, *snapshots])
        validate = parse_args(["validate", *common, *snapshots])
        promote = parse_args(["fast-forward", *common, *snapshots])
        probe = parse_args(["probe", *common])
        self.assertEqual(
            (prepare.command, validate.command, promote.command, probe.command),
            ("prepare", "validate", "fast-forward", "probe"),
        )
        self.assertEqual(prepare.historical_base_snapshot, 40)
        self.assertEqual(prepare.base_snapshot, 41)
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parse_args(["prepare", *common])

    def test_spark_builder_uses_official_rest_vended_credentials_config(self) -> None:
        self.assertEqual(
            PROBE_CATALOG_BUCKET,
            "perfectory-tiles-slice-proof",
        )
        self.assertEqual(
            wap.EVIDENCE_CONTRACT["properties"]["catalog_bucket"]["const"],
            PROBE_CATALOG_BUCKET,
        )
        builder = FakeSparkBuilder()
        values = {
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI": (
                "https://catalog.cloudflarestorage.com/"
                "0123456789abcdef0123456789abcdef/"
                "perfectory-tiles-slice-proof"
            ),
            "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE": "warehouse-id",
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN": "sentinel-secret",
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER": "r2_data_catalog",
        }
        configured = configure_spark_builder(
            builder, "r2", lambda name: values.get(name)
        )
        self.assertIs(configured, builder)
        self.assertEqual(
            builder.values["spark.sql.catalog.r2"],
            "org.apache.iceberg.spark.SparkCatalog",
        )
        self.assertEqual(builder.values["spark.sql.catalog.r2.type"], "rest")
        self.assertEqual(
            builder.values[
                "spark.sql.catalog.r2.header.X-Iceberg-Access-Delegation"
            ],
            "vended-credentials",
        )
        self.assertEqual(
            builder.values["spark.sql.catalog.r2.s3.remote-signing-enabled"],
            "false",
        )
        redaction_index = next(
            index
            for index, (key, _value) in enumerate(builder.calls)
            if key == "spark.redaction.regex"
        )
        token_index = next(
            index
            for index, (key, _value) in enumerate(builder.calls)
            if key == "spark.sql.catalog.r2.token"
        )
        self.assertLess(redaction_index, token_index)
        self.assertEqual(
            builder.values["spark.redaction.regex"],
            "(?i)secret|password|token|credential|access[.]?key",
        )
        self.assertFalse(
            any("oauth2-server-uri" in key for key in builder.values),
            "Cloudflare does not publish an OAuth endpoint for this static-token mode",
        )
        self.assertEqual(
            ICEBERG_PACKAGES,
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
            "org.apache.iceberg:iceberg-aws-bundle:1.6.1",
        )

    def test_spark_builder_rejects_non_cloudflare_and_ambiguous_catalog_uris(self) -> None:
        account = "0123456789abcdef0123456789abcdef"
        invalid_uris = (
            (
                "https://catalog.cloudflarestorage.com/"
                f"{account}/foundation-platform-lakehouse-prod"
            ),
            (
                "https://catalog.cloudflarestorage.com/"
                f"{account}/perfectory-tiles-slice-proof-spoof"
            ),
            (
                "https://catalog.cloudflarestorage.com/"
                f"{account}/Perfectory-tiles-slice-proof"
            ),
            (
                "https://catalog.cloudflarestorage.com/"
                f"{account}/perfectory-tiles-slice-proof/"
            ),
            f"https://localhost/{account}/foundation-lakehouse",
            f"https://catalog.example.test/{account}/foundation-lakehouse",
            (
                "https://catalog.cloudflarestorage.com.evil.example/"
                f"{account}/foundation-lakehouse"
            ),
            (
                "http://catalog.cloudflarestorage.com/"
                f"{account}/foundation-lakehouse"
            ),
            (
                "https://catalog.cloudflarestorage.com/"
                f"{account}/foundation-lakehouse?provider=cloudflare"
            ),
            (
                "https://user@catalog.cloudflarestorage.com/"
                f"{account}/foundation-lakehouse"
            ),
            (
                "https://catalog.cloudflarestorage.com:8443/"
                f"{account}/foundation-lakehouse"
            ),
            (
                "https://catalog.cloudflarestorage.com/"
                f"{account}/foundation-lakehouse#fragment"
            ),
            "https://catalog.cloudflarestorage.com/not-an-account/bucket",
            f"https://catalog.cloudflarestorage.com/{account}/invalid_bucket",
        )
        base_values = {
            "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE": "warehouse-id",
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN": "sentinel-secret",
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER": "r2_data_catalog",
        }

        for catalog_uri in invalid_uris:
            values = {
                **base_values,
                "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI": catalog_uri,
            }
            with self.subTest(catalog_uri=catalog_uri), self.assertRaisesRegex(
                ValueError,
                "dedicated perfectory-tiles-slice-proof bucket",
            ):
                configure_spark_builder(
                    FakeSparkBuilder(), "r2", lambda name: values.get(name)
                )

    def test_probe_execution_keeps_observed_base_separate_from_evidence_json(self) -> None:
        args = parse_args(
            [
                "probe",
                "--namespace",
                PROBE_NAMESPACE,
                "--table",
                PROBE_TABLE,
                "--release-id",
                "018f1111-1111-7111-8111-111111111111",
            ]
        )
        outcome = wap.execute_command_with_outcome(args, FakeWapRuntime())

        self.assertEqual(outcome.observed_historical_base_snapshot, 40)
        self.assertEqual(outcome.observed_base_snapshot, 41)
        self.assertEqual(outcome.evidence.historical_base_snapshot, 40)
        self.assertEqual(outcome.evidence.base_snapshot, 41)
        self.assertEqual(
            wap.expected_evidence_for_command("probe"),
            ("probe_ok", "ok"),
        )

    def test_secret_redaction_never_echoes_catalog_token(self) -> None:
        message = (
            "catalog failed token=sentinel-secret "
            "uri=https://catalog.example.test"
        )
        redacted = redact_secret_values(message, ["sentinel-secret"])
        self.assertNotIn("sentinel-secret", redacted)
        self.assertIn("[redacted]", redacted)

    def test_cli_failure_never_echoes_catalog_token(self) -> None:
        token = "sentinel-cli-catalog-token-secret"
        stderr = io.StringIO()
        with patch.dict(
            os.environ,
            {"FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN": token},
            clear=False,
        ), patch.object(
            wap,
            "run",
            side_effect=RuntimeError(f"provider rejected {token}"),
        ), redirect_stderr(stderr):
            self.assertEqual(wap.cli(["probe"]), 1)
        self.assertNotIn(token, stderr.getvalue())
        self.assertIn("[redacted]", stderr.getvalue())

    def test_probe_orchestrates_prepare_validate_and_explicit_fast_forward(self) -> None:
        args = parse_args(
            [
                "probe",
                "--namespace",
                PROBE_NAMESPACE,
                "--table",
                PROBE_TABLE,
                "--release-id",
                "018f1111-1111-7111-8111-111111111111",
            ]
        )
        runtime = FakeWapRuntime()
        evidence = execute_command(args, runtime)
        self.assertEqual(
            runtime.calls,
            [
                "ensure_probe_table",
                "seed_baseline",
                "advance-main:40",
                "prepare:40:41",
                "validate:40:41",
                "fast-forward:40:41",
            ],
        )
        self.assertEqual(evidence.result, "probe_ok")
        self.assertEqual(evidence.historical_base_snapshot, 40)
        self.assertEqual(evidence.historical_branch_snapshot, 40)
        self.assertEqual(evidence.base_snapshot, 41)
        self.assertEqual(evidence.branch_snapshot, 42)
        self.assertEqual(evidence.historical_base_isolation, "ok")
        self.assertEqual(evidence.retention, "ok")
        self.assertEqual(evidence.fast_forward, "ok")

    def test_runtime_snapshot_results_require_exact_integers(self) -> None:
        args = parse_args(
            [
                "probe",
                "--namespace",
                PROBE_NAMESPACE,
                "--table",
                PROBE_TABLE,
                "--release-id",
                "018f1111-1111-7111-8111-111111111111",
            ]
        )
        float_base = FakeWapRuntime()
        float_base.seed_baseline = lambda _fixture: 40.0  # type: ignore[method-assign]
        with self.assertRaisesRegex(ValueError, "JSON integer"):
            execute_command(args, float_base)

        float_history = FakeWapRuntime()
        float_history.prepare_candidate = (  # type: ignore[method-assign]
            lambda *_args: (40.0, 42)
        )
        with self.assertRaisesRegex(ValueError, "JSON integer"):
            execute_command(args, float_history)

        fractional_candidate = FakeWapRuntime()
        fractional_candidate.prepare_candidate = (  # type: ignore[method-assign]
            lambda *_args: (40, 42.5)
        )
        with self.assertRaisesRegex(ValueError, "JSON integer"):
            execute_command(args, fractional_candidate)

    def test_separate_commands_do_not_publish_implicitly(self) -> None:
        common = [
            "--namespace",
            PROBE_NAMESPACE,
            "--table",
            PROBE_TABLE,
            "--release-id",
            "018f1111-1111-7111-8111-111111111111",
            "--historical-base-snapshot",
            "40",
            "--base-snapshot",
            "41",
        ]
        cases = (
            ("prepare", ["prepare:40:41"], "prepared", "not_requested"),
            ("validate", ["validate:40:41"], "validated", "not_requested"),
            (
                "fast-forward",
                ["validate:40:41", "fast-forward:40:41"],
                "fast_forwarded",
                "ok",
            ),
        )
        for command, expected_calls, result, fast_forward in cases:
            runtime = FakeWapRuntime()
            evidence = execute_command(parse_args([command, *common]), runtime)
            self.assertEqual(
                wap.expected_evidence_for_command(command),
                (result, fast_forward),
            )
            self.assertEqual(runtime.calls, expected_calls)
            self.assertEqual(evidence.result, result)
            self.assertEqual(evidence.fast_forward, fast_forward)


if __name__ == "__main__":
    print("provider_capability=not_proven_offline")
    unittest.main()
