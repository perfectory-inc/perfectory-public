import ast
import json
import os
import re
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
    current_row_latest,
    current_row_predicate,
    declared_geometry_srid,
    jsonl_transport_columns,
    load_lakehouse_artifact,
    load_identity_column,
    load_identity_from_value,
    load_lakehouse_contract,
    load_unit,
    partition_column_names,
    partition_spec,
    row_versions,
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


PIPELINE_GRAPH = SPARK_DIR.parents[2] / "docs" / "catalog" / "pipeline-graph.v1.json"
CURRENT_ROW_READS = {"current_rows", "current_rows_of_versioned"}


def versioned_tables() -> set[str]:
    """Every table that keeps versions per key: its contract declares the newest-per-key rule."""
    return {
        name for name, contract in load_lakehouse_artifact()["contracts"].items()
        if current_row_latest(contract) is not None
    }


def producers(graph: dict, tables: set[str]) -> set[tuple[str, str]]:
    """(job file name, table) for each job the pipeline graph says writes a versioned table."""
    nodes = {node["id"]: node.get("table_name") for node in graph["nodes"]}
    return {
        (Path(via).name, nodes[edge["to"]])
        for edge in graph["edges"] if nodes.get(edge["to"]) in tables
        for via in edge.get("via", [])
    }


def named_tables(source: str, tables: set[str]) -> set[str]:
    """The tables a module names in a string other than a docstring, as `silver.x` or the bare `x`
    an Iceberg argument defaults to."""
    tree = ast.parse(source)
    docstrings = {
        id(node.body[0].value) for node in ast.walk(tree)
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        and node.body and isinstance(node.body[0], ast.Expr) and isinstance(node.body[0].value, ast.Constant)
    }
    strings = [
        node.value for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstrings
    ]
    return {
        table for table in tables
        if any(re.search(rf"(?<!\w)`?{re.escape(table.split('.', 1)[1])}`?(?!\w)",
                         text) for text in strings)
    }


def reaches_current_rows(path: Path, seen: set[Path]) -> bool:
    """Whether the module, or a job module it imports, calls the contract's current-row selection.

    shortcut: a module counts once any module it imports selects current rows, even for another
    table; upgrade to a per-table data-flow check if a helper module ever reads two versioned tables.
    """
    if path in seen or not path.exists() or path.name == "platform_contracts.py":
        return False
    seen.add(path)
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            called = node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, "attr", None)
            if called in CURRENT_ROW_READS:
                return True
    imported = [
        alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
    ] + [node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module]
    return any(reaches_current_rows(path.parent / f"{module}.py", seen) for module in imported)


def readers_bypassing_current_rows(jobs: Path, graph: dict, tables: set[str]) -> list[tuple[str, str]]:
    """(job, table) for each job that names a versioned table it does not write, yet never selects
    that table's current rows through the contract: it would read every version (root ADR-0267)."""
    written = producers(graph, tables)
    found = []
    for path in sorted(jobs.glob("*.py")):
        if path.name == "platform_contracts.py":
            continue
        for table in sorted(named_tables(path.read_text(encoding="utf-8"), tables)):
            if (path.name, table) not in written and not reaches_current_rows(path, set()):
                found.append((path.name, table))
    return found


class CurrentRowRuleTest(unittest.TestCase):
    """A versioned table's contract says which row is current (root ADR-0267, ADR-0275), and every
    reader takes it through `current_rows`, not a filter of its own.

    Modelled on dbt-project-evaluator's rule that only one layer may touch a source directly: every
    other reader goes through the one access point. Who is checked is derived from the contracts
    and the pipeline graph, never listed here: on 2026-10-16 the complex boundary job read the
    complexes table without a rule and failed in production, and the guard added afterwards named
    three readers by hand.
    """

    def test_complexes_contract_declares_the_newest_row_per_code(self) -> None:
        contract = load_lakehouse_contract(SILVER_INDUSTRIAL_COMPLEXES)
        self.assertEqual(current_row_predicate(contract), "valid_to_utc IS NULL")
        latest = current_row_latest(contract)
        self.assertIsNotNone(latest)
        self.assertEqual(latest.key, ("official_complex_code",))
        self.assertEqual(latest.newest_first, ("valid_from_utc", "ingested_at_utc"))

    def test_an_unversioned_table_has_no_latest_rule(self) -> None:
        self.assertIsNone(current_row_latest(load_lakehouse_contract("silver.parcel_boundaries")))

    def test_the_rule_is_refused_when_missing_or_naming_a_foreign_column(self) -> None:
        contract = dict(load_lakehouse_contract(SILVER_INDUSTRIAL_COMPLEXES))
        missing = {key: value for key, value in contract.items() if key != "current_row_latest"}
        with self.assertRaisesRegex(ValueError, "current_row_latest key"):
            current_row_latest(missing)
        for broken in (
            {"key": ["official_complex_code"], "newest_first": ["no_such_column"]},
            {"key": [], "newest_first": ["ingested_at_utc"]},
            {"key": ["official_complex_code"]},
            {"key": ["official_complex_code"], "newest_first": ["ingested_at_utc"], "removal_column": "no_such_column"},
            {"key": ["official_complex_code"], "newest_first": ["ingested_at_utc"], "keep_ties": "yes"},
            "ingested_at_utc",
        ):
            with self.subTest(broken=broken):
                with self.assertRaisesRegex(ValueError, "current_row_latest"):
                    current_row_latest({**contract, "current_row_latest": broken})

    def test_every_reader_of_a_versioned_table_selects_current_rows_through_the_contract(self) -> None:
        graph = json.loads(PIPELINE_GRAPH.read_text(encoding="utf-8"))
        tables = versioned_tables()
        self.assertIn(SILVER_INDUSTRIAL_COMPLEXES, tables)
        self.assertEqual(readers_bypassing_current_rows(JOBS_DIR, graph, tables), [])

    def test_a_complex_reader_keeps_no_rule_of_its_own(self) -> None:
        for path in sorted(JOBS_DIR.glob("industrial_complex*.py")):
            source = path.read_text(encoding="utf-8")
            if SILVER_INDUSTRIAL_COMPLEXES not in named_tables(source, {SILVER_INDUSTRIAL_COMPLEXES}):
                continue
            with self.subTest(reader=path.name):
                # A private window or a bare `valid_to_utc` filter is a second current-row rule.
                self.assertNotIn("Window.partitionBy", source)
                self.assertNotIn("complexes.where(", source)

    def test_the_guard_refuses_a_reader_that_skips_the_rule(self) -> None:
        tables = {SILVER_INDUSTRIAL_COMPLEXES}
        graph = {
            "nodes": [{"id": "complexes", "table_name": SILVER_INDUSTRIAL_COMPLEXES}, {"id": "source"}],
            "edges": [{"from": "source", "to": "complexes", "via": ["jobs/writer.py"]}],
        }
        jobs = {
            "writer.py": 'TABLE = "silver.industrial_complexes"\n',
            "planted.py": 'frame = spark.table("silver.industrial_complexes")\n',
            "planted_default.py": 'parser.add_argument("--complexes-iceberg-table", default="industrial_complexes")\n',
            "planted_sql.py": 'spark.sql(f"SELECT * FROM {cat}.`silver`.`industrial_complexes`")\n',
            "shared.py":"def read(frame, contract):\n    return current_rows(frame, contract)\n",
            "via_shared.py": 'from shared import read\nTABLE = "industrial_complexes"\n',
            "attribute.py": 'import platform_contracts as pc\nrows = pc.current_rows(spark.table("silver.industrial_complexes"), c)\n',
            "prose.py": '"""Reads silver.industrial_complexes."""\nTABLE = "silver.industrial_complex_boundaries"\n',
        }
        with TemporaryDirectory() as tmp:
            for name, text in jobs.items():
                (Path(tmp) / name).write_text(text, encoding="utf-8")
            self.assertEqual(
                readers_bypassing_current_rows(Path(tmp), graph, tables),
                [(name, SILVER_INDUSTRIAL_COMPLEXES) for name in ("planted.py", "planted_default.py", "planted_sql.py")],
            )


class RowVersionsTest(unittest.TestCase):
    """Root ADR-0286: every contract says how its rows pile up."""

    def test_every_contract_declares_valid_row_versions(self) -> None:
        for name, contract in load_lakehouse_artifact()["contracts"].items():
            with self.subTest(table=name):
                row_versions(contract)

    def test_a_canonical_reader_is_a_function_of_a_job_module(self) -> None:
        for name, contract in load_lakehouse_artifact()["contracts"].items():
            reader = row_versions(contract).get("canonical_reader")
            if reader is None:
                continue
            with self.subTest(table=name):
                module, function = reader.split(".")
                tree = ast.parse((JOBS_DIR / f"{module}.py").read_text(encoding="utf-8"))
                self.assertIn(function, {node.name for node in tree.body if isinstance(node, ast.FunctionDef)})

    def test_a_contract_without_row_versions_or_naming_a_foreign_column_is_refused(self) -> None:
        contract = dict(load_lakehouse_contract("silver.parcel_number_change_history"))
        missing = {key: value for key, value in contract.items() if key != "row_versions"}
        with self.assertRaisesRegex(ValueError, "must declare row_versions"):
            row_versions(missing)
        for broken in (
            {"kind": "editions", "edition_column": "no_such_column", "newest_first": ["provider_updated_on"],
             "per": [], "read": "newest"},
            {"kind": "editions", "edition_column": "source_snapshot_id", "newest_first": ["provider_updated_on"],
             "per": ["no_such_column"], "read": "newest"},
            {"kind": "editions", "edition_column": "source_snapshot_id", "newest_first": ["provider_updated_on"],
             "per": [], "read": "oldest"},
            {"kind": "editions", "edition_column": "source_snapshot_id", "newest_first": [], "per": [], "read": "newest"},
            {"kind": "editions", "edition_column": "source_snapshot_id", "newest_first": ["no_such_column"], "per": [],
             "read": "newest"},
            {"kind": "replaced", "key": ["no_such_column"]},
            {"kind": "latest_per_key"},
            {"kind": "scd3"},
        ):
            with self.subTest(broken=broken), self.assertRaises(ValueError):
                row_versions({**contract, "row_versions": broken})


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


class LoadUnitTest(unittest.TestCase):
    """표마다 "한 번의 적재가 무엇인가"가 다르다 (root ADR-0069).

    2026-09-01 실측: 살아 있는 표 6개가 세 가지를 쓰고 있었다 — 객체 키, 수집 실행,
    그리고 아무것도. 한 가지로 읽어 다섯 표가 안전장치에 아무것도 못 남겼다.
    """

    def test_every_table_declares_what_a_load_carries(self) -> None:
        """선언 안 한 표가 있으면 그 표는 조용히 기본값을 쓰게 된다."""
        artifact = json.loads(
            (
                SPARK_DIR.parent / "contracts" / "industrial_complex_lakehouse_contracts.json"
            ).read_text(encoding="utf-8")
        )
        undeclared = [
            name for name, spec in artifact["contracts"].items() if not spec.get("load", {}).get("unit")
        ]

        self.assertEqual(undeclared, [], "적재 단위를 선언하지 않은 표가 있다")

    def test_the_column_comes_from_the_contract_not_from_a_default(self) -> None:
        self.assertEqual(
            load_identity_column("silver.parcel_boundaries"), "source_record_id"
        )
        self.assertEqual(
            load_identity_column("silver.building_register_units"), "source_snapshot_id"
        )
        # 파생 표는 비교할 것이 없다. 없는 것을 "안 실렸다"로 읽으면 안 된다.
        self.assertIsNone(load_identity_column("gold.complex_catalog"))

    def test_a_wrapped_object_key_is_cut_back_to_the_object(self) -> None:
        """값 하나에 객체와 단지 코드가 같이 들어 있다.

        통째로 비교하면 1,442개 객체가 있다고 보고, 한 묶음 상한 64개를 넘겨 적재가
        거부된다 — 보호가 아니라 차단이다.
        """
        value = (
            "foundation-platform:bronze:"
            "bronze/source=vworldkr__sandan_profile/20991231DS99991-1.zip#247930"
        )

        self.assertEqual(
            load_identity_from_value("silver.industrial_complexes", value),
            "bronze/source=vworldkr__sandan_profile/20991231DS99991-1.zip",
        )

    def test_a_value_that_does_not_match_its_declaration_is_an_error(self) -> None:
        """모양이 다른 값을 조용히 통과시키면 그것이 곧 다른 객체로 기록된다."""
        with self.assertRaisesRegex(ValueError, "declared prefix"):
            load_identity_from_value("silver.industrial_complexes", "30138-6.zip#247930")

    def test_a_plain_object_value_is_left_alone(self) -> None:
        key = "bronze/source=vworldkr__sandan_boundary/20991231DS99992-1.zip"

        self.assertEqual(
            load_identity_from_value("silver.industrial_complex_boundaries", key), key
        )

    def test_a_table_with_no_declaration_is_refused(self) -> None:
        """기본값으로 넘어가면 그 표는 안전장치가 꺼진 채로 돈다."""
        with TemporaryDirectory() as root:
            path = Path(root) / "contracts.json"
            artifact = json.loads(
                (
                    SPARK_DIR.parent / "contracts" / "industrial_complex_lakehouse_contracts.json"
                ).read_text(encoding="utf-8")
            )
            artifact["contracts"]["silver.parcel_boundaries"].pop("load")
            path.write_text(json.dumps(artifact), encoding="utf-8")
            with patch.dict(os.environ, {CONTRACTS_PATH_ENV: str(path)}):
                with self.assertRaisesRegex(ValueError, "does not declare"):
                    load_unit("silver.parcel_boundaries")


class WriteDistributionTest(unittest.TestCase):
    """How a write spreads rows over files is the contract's, not each writer's (root ADR-0164)."""

    @staticmethod
    def ranged(**overrides) -> dict:
        return {
            "table_name": "gold.example",
            "current_row_predicate": None,
            "columns": [
                {"name": "pnu", "logical_type": "string", "required": True},
                {"name": "source_snapshot_id", "logical_type": "string", "required": True},
            ],
            "partition_spec": ["source_snapshot_id"],
            "sort_order": ["pnu"],
            "write_distribution": "range",
            **overrides,
        }

    def test_every_table_declares_a_known_distribution(self) -> None:
        from platform_contracts import load_lakehouse_artifact, write_distribution_mode

        for name, contract in load_lakehouse_artifact()["contracts"].items():
            with self.subTest(table=name):
                self.assertIn(write_distribution_mode(contract), ("hash", "range"))

    def test_a_range_table_must_range_over_its_own_columns(self) -> None:
        from platform_contracts import write_distribution_mode

        self.assertEqual(write_distribution_mode(self.ranged()), "range")
        for overrides in ({"sort_order": []}, {"sort_order": ["bucket(16, pnu)"]},
                          {"sort_order": ["absent"]}, {"write_distribution": "none"}):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                write_distribution_mode(self.ranged(**overrides))

    def test_the_table_is_created_and_ordered_from_the_contract(self) -> None:
        from unittest.mock import MagicMock

        import platform_contracts

        spark = MagicMock()
        live = [{}, {"write.distribution-mode": "range", "sort-order": "pnu ASC NULLS FIRST"}]
        with patch.object(platform_contracts, "evolve_iceberg_table_to_contract", return_value=()), \
                patch.object(platform_contracts, "table_properties", side_effect=live):
            platform_contracts.ensure_contract_table(spark, "`c`.`gold`.`example`", self.ranged())
        statements = [call.args[0] for call in spark.sql.call_args_list]
        self.assertIn("'write.distribution-mode' = 'range'", statements[0])
        self.assertEqual(statements[1], "ALTER TABLE `c`.`gold`.`example` WRITE ORDERED BY pnu")
        self.assertEqual(len(statements), 2)

    def test_an_order_the_table_did_not_take_is_an_error(self) -> None:
        from unittest.mock import MagicMock

        import platform_contracts

        with patch.object(platform_contracts, "table_properties", return_value={}), \
                self.assertRaisesRegex(ValueError, "did not take"):
            platform_contracts.apply_write_order(MagicMock(), "t", self.ranged())

    def test_a_hash_table_is_created_as_before_and_not_reordered(self) -> None:
        from unittest.mock import MagicMock

        import platform_contracts

        spark = MagicMock()
        with patch.object(platform_contracts, "evolve_iceberg_table_to_contract", return_value=()), \
                patch.object(platform_contracts, "table_properties", return_value={}):
            platform_contracts.ensure_contract_table(
                spark, "t", self.ranged(write_distribution="hash"))
        statements = [call.args[0] for call in spark.sql.call_args_list]
        self.assertEqual(len(statements), 1)
        self.assertIn("'write.distribution-mode' = 'hash'", statements[0])


if __name__ == "__main__":
    unittest.main()
