import ast
import json
import os
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


SPARK_DIR = Path(__file__).resolve().parents[1]
JOBS_DIR = SPARK_DIR / "jobs"
sys.path.insert(0, str(JOBS_DIR))

from platform_contracts import (  # noqa: E402
    CONTRACTS_PATH_ENV,
    column_names,
    current_row_predicate,
    jsonl_transport_columns,
    load_lakehouse_contract,
    value_domain,
)

BRONZE_INDUSTRIAL_COMPLEXES_RAW_JSONL = "bronze.industrial_complexes_raw_jsonl"
SILVER_INDUSTRIAL_COMPLEXES = "silver.industrial_complexes"


class PlatformContractsTest(unittest.TestCase):
    def test_parcel_contract_exposes_rust_owned_current_row_predicate(self) -> None:
        contract = load_lakehouse_contract("silver.parcel_boundaries")

        self.assertEqual(
            current_row_predicate(contract),
            "valid_to_utc IS NULL",
        )

    def test_current_row_predicate_requires_the_artifact_key(self) -> None:
        with self.assertRaisesRegex(ValueError, "current_row_predicate key"):
            current_row_predicate({"table_name": "silver.parcel_boundaries"})

    def test_current_row_predicate_accepts_only_none_or_nonblank_string(self) -> None:
        self.assertIsNone(
            current_row_predicate(
                {
                    "table_name": "silver.industrial_complexes",
                    "current_row_predicate": None,
                }
            )
        )

        for invalid in ("", "   ", 7, False, ["valid_to_utc IS NULL"]):
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(
                    ValueError,
                    "must be null or a nonblank string",
                ):
                    current_row_predicate(
                        {
                            "table_name": "silver.parcel_boundaries",
                            "current_row_predicate": invalid,
                        }
                    )

    def test_loader_rejects_a_missing_predicate_key(self) -> None:
        artifact = {
            "schema_version": "foundation-platform.lakehouse_contracts.v1",
            "contracts": {
                "silver.parcel_boundaries": {
                    "table_name": "silver.parcel_boundaries"
                }
            },
        }
        with TemporaryDirectory() as directory:
            path = Path(directory) / "contracts.json"
            path.write_text(json.dumps(artifact), encoding="utf-8")
            with patch.dict(os.environ, {CONTRACTS_PATH_ENV: str(path)}):
                with self.assertRaisesRegex(ValueError, "current_row_predicate key"):
                    load_lakehouse_contract("silver.parcel_boundaries")

    def test_loader_rejects_malformed_predicate_values(self) -> None:
        for invalid in ("", "   ", 7, False, ["valid_to_utc IS NULL"]):
            with self.subTest(invalid=invalid), TemporaryDirectory() as directory:
                artifact = {
                    "schema_version": "foundation-platform.lakehouse_contracts.v1",
                    "contracts": {
                        "silver.parcel_boundaries": {
                            "table_name": "silver.parcel_boundaries",
                            "current_row_predicate": invalid,
                        }
                    },
                }
                path = Path(directory) / "contracts.json"
                path.write_text(json.dumps(artifact), encoding="utf-8")
                with patch.dict(os.environ, {CONTRACTS_PATH_ENV: str(path)}):
                    with self.assertRaisesRegex(
                        ValueError,
                        "must be null or a nonblank string",
                    ):
                        load_lakehouse_contract("silver.parcel_boundaries")

    def test_parcel_producer_consumes_the_central_predicate_helper(self) -> None:
        producer_path = JOBS_DIR / "vworld_parcel_boundaries_handoff_to_silver.py"
        source = producer_path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
        ]

        self.assertTrue(
            any(
                isinstance(call.func, ast.Name)
                and call.func.id == "current_row_predicate"
                and any(
                    isinstance(argument, ast.Name)
                    and argument.id == "TABLE_CONTRACT"
                    for argument in call.args
                )
                for call in calls
            )
        )
        self.assertTrue(
            any(
                isinstance(call.func, ast.Attribute)
                and call.func.attr == "expr"
                and any(
                    isinstance(argument, ast.Name)
                    and argument.id == "CURRENT_ROW_PREDICATE"
                    for argument in call.args
                )
                for call in calls
            )
        )


class JsonlTransportContractTest(unittest.TestCase):
    def test_bronze_transport_is_the_silver_contract_minus_job_derived_columns(self) -> None:
        transport = jsonl_transport_columns(BRONZE_INDUSTRIAL_COMPLEXES_RAW_JSONL)
        silver = column_names(load_lakehouse_contract(SILVER_INDUSTRIAL_COMPLEXES))
        derived = ("complex_id", "complex_name_normalized", "valid_to_utc", "row_checksum_sha256")

        self.assertEqual(transport, tuple(name for name in silver if name not in derived))
        self.assertIn("address_text", transport)
        self.assertIn("primary_bjdong_code", transport)

    def test_value_domains_are_exported_for_the_gated_columns(self) -> None:
        self.assertEqual(
            value_domain(SILVER_INDUSTRIAL_COMPLEXES, "complex_kind"),
            ("national", "general", "agricultural", "urban_high_tech"),
        )
        self.assertEqual(
            value_domain(SILVER_INDUSTRIAL_COMPLEXES, "status"),
            ("planned", "developing", "operating", "changed", "abolished", "unknown"),
        )

    def test_transport_and_domain_lookups_reject_a_stripped_artifact(self) -> None:
        artifact = {
            "schema_version": "foundation-platform.lakehouse_contracts.v1",
            "contracts": {},
        }
        with TemporaryDirectory() as directory:
            path = Path(directory) / "contracts.json"
            path.write_text(json.dumps(artifact), encoding="utf-8")
            with patch.dict(os.environ, {CONTRACTS_PATH_ENV: str(path)}):
                with self.assertRaisesRegex(ValueError, "jsonl_transports"):
                    jsonl_transport_columns(BRONZE_INDUSTRIAL_COMPLEXES_RAW_JSONL)
                with self.assertRaisesRegex(ValueError, "value_domains"):
                    value_domain(SILVER_INDUSTRIAL_COMPLEXES, "status")

    def test_transport_lookup_rejects_an_unknown_or_empty_dataset(self) -> None:
        artifact = {
            "schema_version": "foundation-platform.lakehouse_contracts.v1",
            "contracts": {},
            "jsonl_transports": {BRONZE_INDUSTRIAL_COMPLEXES_RAW_JSONL: {"columns": []}},
            "value_domains": {SILVER_INDUSTRIAL_COMPLEXES: {"status": []}},
        }
        with TemporaryDirectory() as directory:
            path = Path(directory) / "contracts.json"
            path.write_text(json.dumps(artifact), encoding="utf-8")
            with patch.dict(os.environ, {CONTRACTS_PATH_ENV: str(path)}):
                with self.assertRaisesRegex(ValueError, "has no columns"):
                    jsonl_transport_columns(BRONZE_INDUSTRIAL_COMPLEXES_RAW_JSONL)
                with self.assertRaisesRegex(ValueError, "missing jsonl transport"):
                    jsonl_transport_columns("bronze.absent_jsonl")
                with self.assertRaisesRegex(ValueError, "is empty"):
                    value_domain(SILVER_INDUSTRIAL_COMPLEXES, "status")

    def test_bronze_to_silver_job_reads_the_exported_contract_instead_of_its_own_lists(
        self,
    ) -> None:
        job_path = JOBS_DIR / "industrial_complex_bronze_to_silver.py"
        tree = ast.parse(job_path.read_text(encoding="utf-8"))
        assignments = {
            target.id: node.value
            for node in ast.walk(tree)
            if isinstance(node, (ast.Assign, ast.AnnAssign))
            for target in (node.targets if isinstance(node, ast.Assign) else [node.target])
            if isinstance(target, ast.Name)
        }

        for name, helper in (
            ("INPUT_COLUMNS", "jsonl_transport_columns"),
            ("ALLOWED_COMPLEX_KINDS", "value_domain"),
            ("ALLOWED_STATUSES", "value_domain"),
        ):
            with self.subTest(name=name):
                value = assignments[name]
                self.assertIsInstance(value, ast.Call)
                self.assertIsInstance(value.func, ast.Name)
                self.assertEqual(value.func.id, helper)


if __name__ == "__main__":
    unittest.main()
