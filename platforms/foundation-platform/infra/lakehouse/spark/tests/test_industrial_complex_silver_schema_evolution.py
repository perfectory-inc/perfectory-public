"""Contract tests for the Silver table's contract-driven schema evolution.

`silver.industrial_complexes` is a published table: 1,442 rows were written to it under the
previous contract. `CREATE TABLE IF NOT EXISTS` only ever describes a table that does not exist, so
a contract that gains ten columns reaches that table through `ALTER TABLE ... ADD COLUMN` or not at
all — and the `INSERT` that follows writes one value per contract column, which a narrower table has
nowhere to put.

The step itself is shared with the Silver-to-Gold job. These tests exercise it against the Silver
contract, because a helper that is right for one contract and wrong for another is exactly what
sharing it is supposed to make impossible.

The session is a fake. Whether Iceberg accepts the statements is not a question a fake can answer;
`test_industrial_complex_gold_schema_evolution` records how that question was answered against a
real Iceberg table for the same step.
"""

import sys
import types
import unittest
from pathlib import Path


SPARK_DIR = Path(__file__).resolve().parents[1]
JOBS_DIR = SPARK_DIR / "jobs"
sys.path.insert(0, str(JOBS_DIR))


def _install_pyspark_stub() -> None:
    """Make the job importable where PySpark is not installed, which is the CI runner."""

    if "pyspark" in sys.modules:
        return
    for name in ("pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
                 "pyspark.storagelevel"):
        sys.modules.setdefault(name, types.ModuleType(name))
    sql = sys.modules["pyspark.sql"]
    for attribute in ("DataFrame", "SparkSession", "Window"):
        setattr(sql, attribute, type(attribute, (), {}))
    sql.functions = sys.modules["pyspark.sql.functions"]
    sql.types = sys.modules["pyspark.sql.types"]
    sys.modules["pyspark.storagelevel"].StorageLevel = type("StorageLevel", (), {})


_install_pyspark_stub()

import industrial_complex_bronze_to_silver as job  # noqa: E402

TABLE = "`r2`.`silver`.`industrial_complexes`"

# The columns the canonical table gained after it was published. Spelled out rather than derived: a
# later contract edit has to state its intent here too, and deriving this from the contract would
# make every test below pass vacuously.
ADDED_COLUMNS = (
    "construction_start_date",
    "development_progress_percent",
    "lot_sales_status",
    "business_period_raw",
    "business_period_start_month",
    "business_period_end_month",
    "designation_basis_law_raw",
    "development_method_raw",
    "development_purpose_raw",
    "invited_industries_raw",
)


class FakeSchema:
    def __init__(self, names):
        self.fields = [type("Field", (), {"name": name})() for name in names]


class FakeTable:
    def __init__(self, schema):
        self.schema = schema


class FakeSpark:
    """Records SQL and answers `table()` from a mutable column list."""

    def __init__(self, columns):
        self.columns = list(columns)
        self.statements: list[str] = []

    def table(self, name):
        assert name == TABLE, name
        return FakeTable(FakeSchema(self.columns))

    def sql(self, statement):
        self.statements.append(statement)
        parts = statement.split()
        # ALTER TABLE <t> ADD COLUMN <name> <type> [FIRST|AFTER <col>]
        assert parts[:2] == ["ALTER", "TABLE"], statement
        assert parts[3:5] == ["ADD", "COLUMN"], statement
        name = parts[5]
        if parts[-2] == "AFTER":
            self.columns.insert(self.columns.index(parts[-1]) + 1, name)
        elif parts[-1] == "FIRST":
            self.columns.insert(0, name)
        else:
            self.columns.append(name)
        return None


def columns_before_this_change() -> list[str]:
    """The contract's columns minus the ones this change added — the live table's shape."""

    return [name for name in job.SILVER_COLUMNS if name not in ADDED_COLUMNS]


class SilverSchemaEvolutionTest(unittest.TestCase):
    def test_the_contract_actually_declares_the_added_columns(self) -> None:
        # Guards the rest of this file: if the contract lost these, `columns_before_this_change`
        # would silently equal the contract and every test below would pass vacuously.
        for column in ADDED_COLUMNS:
            self.assertIn(column, job.SILVER_COLUMNS)

    def test_a_narrower_table_gains_exactly_the_missing_columns(self) -> None:
        spark = FakeSpark(columns_before_this_change())

        added = job.evolve_silver_iceberg_table_to_contract(spark, TABLE)

        self.assertEqual(added, ADDED_COLUMNS)
        self.assertEqual(len(spark.statements), len(ADDED_COLUMNS))

    def test_evolution_leaves_the_table_in_contract_order(self) -> None:
        spark = FakeSpark(columns_before_this_change())

        job.evolve_silver_iceberg_table_to_contract(spark, TABLE)

        # Not merely "contains the same names": the INSERT is positional, so a table holding the
        # contract's columns in another order would take every value into the wrong column.
        self.assertEqual(tuple(spark.columns), job.SILVER_COLUMNS)

    def test_new_columns_are_placed_rather_than_appended(self) -> None:
        spark = FakeSpark(columns_before_this_change())

        job.evolve_silver_iceberg_table_to_contract(spark, TABLE)

        for statement in spark.statements:
            self.assertTrue(
                statement.rstrip().split()[-2] == "AFTER"
                or statement.rstrip().endswith("FIRST"),
                f"column added without a position: {statement}",
            )

    def test_the_new_columns_carry_their_contract_types(self) -> None:
        # `development_progress_percent` is the one that would silently degrade: a DECIMAL(5,2)
        # written as DOUBLE holds no exact 59.9, and nothing downstream would notice.
        spark = FakeSpark(columns_before_this_change())

        job.evolve_silver_iceberg_table_to_contract(spark, TABLE)

        types_by_column = {
            statement.split()[5]: statement.split()[6] for statement in spark.statements
        }
        self.assertEqual(types_by_column["development_progress_percent"], "DECIMAL(5,2)")
        self.assertEqual(types_by_column["construction_start_date"], "DATE")
        self.assertEqual(types_by_column["business_period_raw"], "STRING")

    def test_a_matching_table_is_left_alone(self) -> None:
        spark = FakeSpark(job.SILVER_COLUMNS)

        added = job.evolve_silver_iceberg_table_to_contract(spark, TABLE)

        self.assertEqual(added, ())
        self.assertEqual(spark.statements, [])

    def test_evolution_is_idempotent(self) -> None:
        spark = FakeSpark(columns_before_this_change())

        job.evolve_silver_iceberg_table_to_contract(spark, TABLE)
        statements_after_first = len(spark.statements)
        added = job.evolve_silver_iceberg_table_to_contract(spark, TABLE)

        self.assertEqual(added, ())
        self.assertEqual(len(spark.statements), statements_after_first)

    def test_a_column_the_contract_does_not_declare_is_refused(self) -> None:
        spark = FakeSpark([*job.SILVER_COLUMNS, "column_from_somewhere_else"])

        with self.assertRaisesRegex(ValueError, "contract does not declare"):
            job.evolve_silver_iceberg_table_to_contract(spark, TABLE)

        self.assertEqual(spark.statements, [])

    def test_a_table_that_cannot_be_brought_into_contract_order_is_refused(self) -> None:
        # A session whose ALTER does not place the column where it was asked to. The step must not
        # accept the result, because a positional INSERT into it would corrupt every row.
        class MisplacingSpark(FakeSpark):
            def sql(self, statement):
                self.statements.append(statement)
                self.columns.append(statement.split()[5])
                return None

        spark = MisplacingSpark(columns_before_this_change())

        with self.assertRaisesRegex(ValueError, "do not match the contract"):
            job.evolve_silver_iceberg_table_to_contract(spark, TABLE)


class SilverProjectionColumnTest(unittest.TestCase):
    def test_every_silver_column_has_a_lineage_entry(self) -> None:
        lineage = {entry["output_column"] for entry in job.column_lineage()}

        self.assertEqual(lineage, set(job.SILVER_COLUMNS))

    def test_the_added_columns_are_sourced_from_the_bronze_transport(self) -> None:
        lineage = {entry["output_column"]: entry["inputs"] for entry in job.column_lineage()}

        for column in ADDED_COLUMNS:
            with self.subTest(column=column):
                inputs = lineage[column]
                self.assertEqual(len(inputs), 1)
                self.assertEqual(inputs[0]["dataset"], job.BRONZE_DATASET_NAME)
                self.assertEqual(inputs[0]["column"], column)

    def test_the_transport_supplies_every_added_column(self) -> None:
        # The producer and this job read one exported list. A column the contract declares and the
        # transport does not would fail at run time with a missing-column error on real input.
        for column in ADDED_COLUMNS:
            self.assertIn(column, job.INPUT_COLUMNS)


if __name__ == "__main__":
    unittest.main()
