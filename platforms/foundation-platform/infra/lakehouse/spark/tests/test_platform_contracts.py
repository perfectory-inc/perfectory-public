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
    declared_geometry_srid,
    jsonl_transport_columns,
    load_lakehouse_contract,
    partition_column_names,
    partition_spec,
    required_string_column_names,
    sort_order,
    string_column_names,
    value_domain,
)

BRONZE_INDUSTRIAL_COMPLEXES_RAW_JSONL = "bronze.industrial_complexes_raw_jsonl"
SILVER_INDUSTRIAL_COMPLEXES = "silver.industrial_complexes"
GOLD_COMPLEX_CATALOG = "gold.complex_catalog"


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


class RegionOptionalContractTest(unittest.TestCase):
    """The industrial-complex tables no longer require a region, and must not partition on one."""

    def test_region_columns_are_optional_on_both_industrial_complex_tables(self) -> None:
        for table in (SILVER_INDUSTRIAL_COMPLEXES, GOLD_COMPLEX_CATALOG):
            with self.subTest(table=table):
                required = required_string_column_names(load_lakehouse_contract(table))
                self.assertNotIn("sido_code", required)
                self.assertNotIn("sigungu_code", required)

    def test_neither_industrial_complex_table_partitions_or_sorts_on_a_region(self) -> None:
        for table in (SILVER_INDUSTRIAL_COMPLEXES, GOLD_COMPLEX_CATALOG):
            with self.subTest(table=table):
                contract = load_lakehouse_contract(table)
                # A partition key cannot be null, so an optional column must not be one. This is
                # the check that would have caught the old `sido_code` partitioning.
                region = ("sido_code", "sigungu_code", "primary_bjdong_code")
                for entry in (*partition_spec(contract), *sort_order(contract)):
                    self.assertFalse(
                        any(name in entry for name in region),
                        f"{table} still partitions or sorts on a region: {entry}",
                    )
                self.assertEqual(partition_spec(contract), ("source_snapshot_id",))

    def test_partition_column_names_drops_transform_entries(self) -> None:
        contract = {
            "table_name": "silver.example",
            "current_row_predicate": None,
            "columns": [
                {"name": "complex_id", "logical_type": "string", "required": True},
                {"name": "source_snapshot_id", "logical_type": "string", "required": True},
            ],
            "partition_spec": ["source_snapshot_id", "bucket(32, complex_id)"],
        }

        # A local Parquet writer can partition by a column but not by an Iceberg transform.
        self.assertEqual(partition_column_names(contract), ("source_snapshot_id",))

    def test_string_column_names_covers_optional_columns_too(self) -> None:
        contract = load_lakehouse_contract(SILVER_INDUSTRIAL_COMPLEXES)
        gated = string_column_names(contract)

        self.assertIn("sido_code", gated)
        self.assertIn("sigungu_code", gated)
        self.assertIn("primary_bjdong_code", gated)
        for name in required_string_column_names(contract):
            self.assertIn(name, gated)
        # Only string columns; a timestamp has no empty-string failure mode.
        self.assertNotIn("valid_from_utc", gated)

    def test_industrial_complex_jobs_read_partitioning_from_the_contract(self) -> None:
        for job, partition_name, sort_name in (
            ("industrial_complex_bronze_to_silver.py", "PARTITION_COLUMNS", "SORT_COLUMNS"),
            (
                "industrial_complex_silver_to_gold.py",
                "GOLD_PARTITION_COLUMNS",
                "GOLD_SORT_COLUMNS",
            ),
        ):
            with self.subTest(job=job):
                tree = ast.parse((JOBS_DIR / job).read_text(encoding="utf-8"))
                assignments = {
                    target.id: node.value
                    for node in ast.walk(tree)
                    if isinstance(node, (ast.Assign, ast.AnnAssign))
                    for target in (
                        node.targets if isinstance(node, ast.Assign) else [node.target]
                    )
                    if isinstance(target, ast.Name)
                }
                for name, helper in (
                    (partition_name, "partition_column_names"),
                    (sort_name, "sort_order"),
                ):
                    value = assignments[name]
                    self.assertIsInstance(value, ast.Call)
                    self.assertIsInstance(value.func, ast.Name)
                    self.assertEqual(value.func.id, helper)


class DeclaredGeometrySridTest(unittest.TestCase):
    """The CRS a geometry table carries is declared per table (root ADR-0042).

    Reading it off the contract is what keeps a Spark job from holding a second copy of the answer;
    the job that holds the copy is the one that drifts when a source changes projection.
    """

    def test_each_geometry_table_declares_its_own_srid(self) -> None:
        self.assertEqual(
            declared_geometry_srid(
                load_lakehouse_contract("silver.industrial_complex_boundaries")
            ),
            5186,
        )
        self.assertEqual(
            declared_geometry_srid(load_lakehouse_contract("silver.parcel_boundaries")),
            4326,
        )

    def test_a_table_without_the_gate_is_refused(self) -> None:
        with self.assertRaisesRegex(ValueError, "geometry_srid"):
            declared_geometry_srid(load_lakehouse_contract(SILVER_INDUSTRIAL_COMPLEXES))

    def test_two_srid_gates_are_refused(self) -> None:
        contract = {
            "table_name": "silver.confused",
            "quality_gates": ["geometry_srid = 4326", "geometry_srid = 5186"],
        }

        with self.assertRaisesRegex(ValueError, "exactly one"):
            declared_geometry_srid(contract)


if __name__ == "__main__":
    unittest.main()
