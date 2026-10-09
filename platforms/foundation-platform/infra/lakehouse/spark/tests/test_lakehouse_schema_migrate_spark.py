"""Secretless Spark/Iceberg migration proofs; opt in with RUN_SPARK_TESTS=1.

Run in the pinned Spark image with the contract's Iceberg runtime jar. All tables use a
temporary local Hadoop catalog. No production catalog, credentials, or input data are needed.
"""

import importlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
import lakehouse_schema_migrate as migrate
from platform_contracts import column_names, create_table_columns_sql, load_lakehouse_contract
from lakehouse_engine import load_engine_contract
import test_lakehouse_schema_migrate as migration_fixtures


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark and Iceberg runtime")
class MigrationSpark(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.workspace = tempfile.TemporaryDirectory(prefix="migration-proof-")
        cls.spark = (SparkSession.builder.master("local[2]").appName("lakehouse-migration-proof")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                     .config("spark.sql.catalog.proof", "org.apache.iceberg.spark.SparkCatalog")
                     .config("spark.sql.catalog.proof.type", "hadoop")
                     .config("spark.sql.catalog.proof.warehouse", cls.workspace.name).getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        for namespace in ("silver", "gold"):
            cls.spark.sql(f"CREATE NAMESPACE proof.{namespace}")

    def test_runtime_versions_match_the_engine_contract(self):
        engine = load_engine_contract()
        self.assertEqual(self.spark.version, engine["spark"]["version"])
        self.assertEqual(self.spark._jvm.org.apache.iceberg.IcebergBuild.version(), engine["iceberg"]["version"])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.workspace.cleanup()

    def table(self, name, columns):
        self.spark.sql(f"DROP TABLE IF EXISTS proof.{name}")
        self.spark.sql(f"CREATE TABLE proof.{name} ({columns}) USING iceberg")
        return f"proof.{name}"

    def scalar_fixture(self, name, batch):
        from datetime import datetime, timedelta
        from pyspark.sql import functions as F

        # 컬럼 계약은 그대로 쓰되, 한 행 증명에 대규모 bucket shuffle은 필요하지 않다.
        contract = dict(load_lakehouse_contract(name), partition_spec=[], sort_order=[])
        expressions = []
        for index, column in enumerate(contract["columns"]):
            logical_type = column["logical_type"]
            if logical_type == "timestamp":
                value = datetime(2099, 1, 1) + timedelta(seconds=index)
            elif logical_type in ("int", "long"):
                value = 100 + index
            elif logical_type == "double":
                value = 200.25 + index
            else:
                value = f"synthetic-{batch}-{column['name']}"
            expressions.append(F.lit(value).cast(migrate.spark_sql_type(logical_type)).alias(column["name"]))
        return contract, self.spark.range(1).select(*expressions)

    def scalar_args(self, name, mode):
        from argparse import Namespace

        namespace, table = name.split(".")
        return Namespace(contract=name, iceberg_catalog_name="proof", iceberg_namespace=namespace,
                         iceberg_table=table, iceberg_write_mode=mode, derivation=None)

    def test_scalar_unit_tables_keep_named_values_before_and_after_schema_reorder(self):
        from pyspark.sql import functions as F
        import silver_scalar_handoff_to_lakehouse as scalar

        evidence = {"building_link_source_record_id", "building_link_input_sha256", "building_link_reason"}
        for name in ("silver.building_register_units", "silver.building_register_unit_areas"):
            for mode in ("append", "overwrite"):
                with self.subTest(table=name, mode=mode):
                    contract, first = self.scalar_fixture(name, "first")
                    columns = contract["columns"]
                    # UNIT 근거 3개는 과거 ADD COLUMN처럼 끝에 둔다. 나머지도 순서를 뒤집어
                    # 같은 문자열 타입끼리의 위치 교환까지 값 비교로 검출한다.
                    shuffled = [c for c in reversed(columns) if c["name"] not in evidence]
                    shuffled.extend(c for c in columns if c["name"] in evidence)
                    table = self.table(name, create_table_columns_sql(dict(contract, columns=shuffled)))
                    first_expected = first.first().asDict()
                    outcome = scalar.write_silver_iceberg(
                        self.spark, first.select(*reversed(first.columns)), self.scalar_args(name, mode), contract, F)
                    self.assertTrue(outcome["appended"])
                    self.assertEqual(self.spark.table(table).first().asDict(), first_expected)
                    self.assertEqual(self.spark.table(table).columns, [c["name"] for c in shuffled])

                    plan = migrate.inspect_table(self.spark, table, contract, name)
                    self.assertTrue(plan["reorder"])
                    self.assertEqual(plan["add"], [])
                    self.assertEqual(plan["backfill"], [])
                    migrate.apply_table(self.spark, "proof", name, contract, plan)
                    self.assertEqual(self.spark.table(table).columns, list(column_names(contract)))
                    self.assertEqual(self.spark.table(table).first().asDict(), first_expected)

                    _, second = self.scalar_fixture(name, "second")
                    second_expected = second.first().asDict()
                    scalar.write_silver_iceberg(
                        self.spark, second.select(*reversed(second.columns)), self.scalar_args(name, mode), contract, F)
                    rows = self.spark.table(table).collect()
                    expected = [first_expected, second_expected] if mode == "append" else [second_expected]
                    self.assertCountEqual([row.asDict() for row in rows], expected)

    def test_scalar_unit_tables_reject_missing_input_columns_without_committing(self):
        from pyspark.errors import AnalysisException
        from pyspark.sql import functions as F
        import silver_scalar_handoff_to_lakehouse as scalar

        for name, missing in (("silver.building_register_units", "building_link_input_sha256"),
                              ("silver.building_register_unit_areas", "area_m2_raw")):
            with self.subTest(table=name, missing=missing):
                contract, frame = self.scalar_fixture(name, "present")
                table = self.table(name, create_table_columns_sql(contract))
                scalar.write_silver_iceberg(self.spark, frame, self.scalar_args(name, "append"), contract, F)
                before = self.spark.table(table).first().asDict()
                snapshot_query = f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'"
                snapshot = self.spark.sql(snapshot_query).first()[0]
                _, incomplete = self.scalar_fixture(name, "missing")
                handoff = str(Path(self.workspace.name) / f"{name}-missing-handoff")
                incomplete.drop(missing).write.parquet(handoff)
                with self.assertRaisesRegex(ValueError, missing):
                    scalar.read_handoff_parquet(self.spark, handoff, contract)
                with self.assertRaisesRegex(AnalysisException, missing):
                    scalar.write_silver_iceberg(
                        self.spark, incomplete.drop(missing), self.scalar_args(name, "append"), contract, F)
                self.assertEqual(self.spark.sql(snapshot_query).first()[0], snapshot)
                self.assertEqual(self.spark.table(table).count(), 1)
                self.assertEqual(self.spark.table(table).first().asDict(), before)

    def test_each_registered_backfill_matches_a_rebuild_and_retries_after_add_column(self):
        from pyspark.sql import functions as F

        for (name, column), (module, function) in migrate.BACKFILLS.items():
            with self.subTest(table=name):
                contract = load_lakehouse_contract(name)
                # Simulate failure after ADD COLUMN: required row_digest already exists as NULL.
                table = self.table(name, create_table_columns_sql(contract))
                values = [F.lit(None if c["name"] == column else
                                (1 if c["logical_type"] == "long" else "synthetic"))
                          .cast(migrate.spark_sql_type(c["logical_type"])).alias(c["name"])
                          for c in contract["columns"]]
                self.spark.range(2).select(*values).writeTo(table).append()
                plan = migrate.inspect_table(self.spark, table, contract, name)
                self.assertEqual(plan["add"], [])
                self.assertEqual(plan["backfill"], [column])
                build = importlib.import_module(module)
                self.assertIs(migrate.backfill_function(name, column), getattr(build, function))
                result = migrate.apply_table(self.spark, "proof", name, contract, plan)
                self.assertEqual(result["backfilled"]["rows"], 2)
                persisted = self.spark.table(table)
                self.assertEqual(persisted.where(F.col(column) != getattr(build, function)()).count(), 0)
                self.assertEqual(migrate.inspect_table(self.spark, table, contract, name)["backfill"], [])

    @patch.object(migrate, "load_known_drift", return_value=migration_fixtures.TemporaryExtraColumns.fixture())
    def test_retained_extras_survive_reorder_addition_and_backfill(self, _registry):
        from pyspark.sql import functions as F

        name = "silver.building_register_units"
        extras = tuple(c["name"] for c in migrate.load_known_drift()[name]["extra_columns"])
        contract = {"write_distribution": "hash", "columns": [
            {"name": "a", "logical_type": "string", "required": True},
            {"name": "filled", "logical_type": "string", "required": True},
        ]}
        table = self.table(name, ", ".join(f"{c} STRING" for c in (*extras, "a")))
        self.spark.sql(f"INSERT INTO {table} VALUES ('source', 'digest', 'reason', 'value')")
        with patch.dict(migrate.BACKFILLS, {(name, "filled"): ("unused", "unused")}), patch.object(
            migrate, "backfill_function", return_value=lambda: F.upper(F.col("a"))
        ):
            plan = migrate.inspect_table(self.spark, table, contract, name)
            self.assertTrue(plan["reorder"])
            self.assertEqual(plan["backfill"], ["filled"])
            migrate.apply_table(self.spark, "proof", name, contract, plan)
        self.assertEqual(self.spark.table(table).columns, [*column_names(contract), *extras])
        self.assertEqual(tuple(self.spark.table(table).first()), ("value", "VALUE", "source", "digest", "reason"))
        self.spark.sql(f"ALTER TABLE {table} ADD COLUMN unexpected STRING")
        with self.assertRaises(migrate.MigrationBlocked):
            migrate.inspect_table(self.spark, table, contract, name)

    def test_failed_backfill_is_retryable_and_whitespace_is_not_a_required_value(self):
        from pyspark.sql import functions as F

        name = "gold.retry"
        contract = {"write_distribution": "hash", "columns": [
            {"name": "a", "logical_type": "string", "required": True},
            {"name": "filled", "logical_type": "string", "required": True},
        ]}
        table = self.table(name, "a STRING")
        self.spark.sql(f"INSERT INTO {table} VALUES ('value')")
        with patch.dict(migrate.BACKFILLS, {(name, "filled"): ("unused", "unused")}):
            plan = migrate.inspect_table(self.spark, table, contract, name)
            with patch.object(migrate, "backfill_function", return_value=lambda: F.lit(" ")):
                with self.assertRaisesRegex(migrate.MigrationBlocked, "empty"):
                    migrate.apply_table(self.spark, "proof", name, contract, plan)
            retry = migrate.inspect_table(self.spark, table, contract, name)
            self.assertEqual(retry["add"], [])
            self.assertEqual(retry["backfill"], ["filled"])
            with patch.object(migrate, "backfill_function", return_value=lambda: F.col("a")):
                migrate.apply_table(self.spark, "proof", name, contract, retry)
        self.assertEqual(tuple(self.spark.table(table).first()), ("value", "value"))

    def test_backfill_reads_current_main_after_rollback_not_the_newest_snapshot(self):
        from pyspark.sql import functions as F

        name = "gold.rollback"
        contract = {"write_distribution": "hash", "columns": [{"name": c, "logical_type": "string", "required": True} for c in ("a", "filled")]}
        table = self.table(name, "a STRING, filled STRING")
        self.spark.sql(f"INSERT INTO {table} VALUES ('current', NULL)")
        snapshot = self.spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name='main'").first()[0]
        self.spark.sql(f"INSERT INTO {table} VALUES ('rolled-back', NULL)")
        self.spark.sql(f"CALL proof.system.rollback_to_snapshot('{name}', {snapshot})")
        with patch.object(migrate, "backfill_function", return_value=lambda: F.col("a")):
            result = migrate.backfill(self.spark, "proof", name, contract, ["filled"])
        self.assertEqual(result["from_snapshot"], str(snapshot))
        self.assertEqual([tuple(r) for r in self.spark.table(table).collect()], [("current", "current")])

    def test_concurrent_append_is_preserved_and_blocks_the_backfill_commit(self):
        from pyspark.sql import functions as F

        name = "gold.concurrent"
        contract = {"write_distribution": "hash", "columns": [{"name": c, "logical_type": "string", "required": True} for c in ("a", "filled")]}
        table = self.table(name, "a STRING, filled STRING")
        self.spark.sql(f"INSERT INTO {table} VALUES ('original', NULL)")

        def concurrent_fill():
            self.spark.sql(f"INSERT INTO {table} VALUES ('concurrent', NULL)")
            return F.col("a")

        with patch.object(migrate, "backfill_function", return_value=concurrent_fill):
            with self.assertRaisesRegex(Exception, "conflict|Conflicting"):
                migrate.backfill(self.spark, "proof", name, contract, ["filled"])
        self.assertEqual(sorted(r.a for r in self.spark.table(table).collect()), ["concurrent", "original"])

    @patch.object(migrate, "load_known_drift", return_value=migration_fixtures.TemporaryExtraColumns.fixture())
    def test_z_main_does_not_waive_a_listed_tables_missing_required_backfill(self, _registry):
        # Keep this last: the real CLI shuts down its Spark session when it completes.
        name = migration_fixtures.TemporaryExtraColumns.TABLE
        extras = migration_fixtures.TemporaryExtraColumns.EXTRA
        table = self.table(name, ", ".join(f"{c} STRING" for c in ("a", *extras)))
        self.spark.sql(f"INSERT INTO {table} VALUES ('value', 'source', 'digest', 'reason')")
        contract = {"write_distribution": "hash", "columns": [{"name": c, "logical_type": "string", "required": True}
                                for c in ("a", "unregistered_required")]}
        report = str(Path(self.workspace.name) / "main-summary.json")
        with patch.object(sys, "argv", ["migrate", "--mode", "apply", "--iceberg-catalog-name", "proof",
                                        "--summary-output", report]), patch.object(
            migrate, "load_lakehouse_artifact", return_value={"contracts": {name: contract}}
        ), patch.object(migrate, "apply_catalog_settings", side_effect=lambda builder, _catalog: builder), patch.object(
            # Deployment checks require remote-catalog classes. This proof uses the local Hadoop
            # catalog already exercised above, without provider credentials or the AWS bundle.
            migrate, "assert_iceberg_runtime_loaded"
        ):
            self.assertEqual(migrate.main(), 1)
        summary = json.loads(Path(report).read_text(encoding="utf-8"))
        self.assertEqual(summary["tables"][name]["state"], "blocked")
        self.assertIn("backfill", summary["tables"][name]["reason"])


if __name__ == "__main__":
    unittest.main()
