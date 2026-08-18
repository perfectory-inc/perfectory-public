"""Contract tests for the Gold table's contract-driven schema evolution.

`create_gold_iceberg_table_if_missing` only ever describes a table that does not exist, so a
widened contract reaches an already-published table through `ALTER TABLE ... ADD COLUMN` or not at
all. These tests pin the three properties that make that step safe to run against the live table:

1. it adds exactly the columns the contract has and the table does not,
2. the resulting column order matches the contract — the INSERT resolves by position, so an order
   mismatch would write every value into the wrong column without erroring, and
3. it is a no-op once the table already matches.

The session is a fake. Whether Iceberg itself accepts the statements is not a question a fake can
answer, so it was answered separately by running the real thing:
`target/schema-evolution-proof/prove_schema_evolution.py` builds a pre-widening Iceberg table,
shows the widened INSERT failing with `INSERT_COLUMN_ARITY_MISMATCH`, runs this step, and shows the
same INSERT succeeding.
"""

import sys
import types
import unittest
from pathlib import Path


SPARK_DIR = Path(__file__).resolve().parents[1]
JOBS_DIR = SPARK_DIR / "jobs"
sys.path.insert(0, str(JOBS_DIR))


def _install_pyspark_stub() -> None:
    """Make the job importable where PySpark is not installed, which is the CI runner.

    The job imports PySpark at module scope, and the function under test never calls into it — it
    issues SQL through whatever session it is handed. Stubbing the import is therefore not a way of
    avoiding the real thing; it is how a pure-logic test reaches pure logic. `from __future__ import
    annotations` means the job's PySpark type hints are never evaluated either.

    Whether Iceberg accepts the SQL this step emits is a different question, and a stub could not
    answer it. `target/schema-evolution-proof/prove_schema_evolution.py` answers it against a real
    Iceberg table.
    """

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

import industrial_complex_silver_to_gold as job  # noqa: E402

TABLE = "`r2`.`gold`.`complex_catalog`"

# The columns this change added to the Gold projection. Spelled out here rather than derived, so a
# later contract edit has to state its intent in the test too.
ADDED_COLUMNS = (
    "management_agency_name",
    "developer_name",
    "designated_date",
    "completion_date",
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

    return [name for name in job.GOLD_COLUMNS if name not in ADDED_COLUMNS]


class GoldSchemaEvolutionTest(unittest.TestCase):
    def test_the_contract_actually_declares_the_added_columns(self) -> None:
        # Guards the rest of this file: if the contract lost these, `columns_before_this_change`
        # would silently equal the contract and every test below would pass vacuously.
        for column in ADDED_COLUMNS:
            self.assertIn(column, job.GOLD_COLUMNS)

    def test_a_narrower_table_gains_exactly_the_missing_columns(self) -> None:
        spark = FakeSpark(columns_before_this_change())

        added = job.evolve_gold_iceberg_table_to_contract(spark, TABLE)

        self.assertEqual(added, ADDED_COLUMNS)
        self.assertEqual(len(spark.statements), len(ADDED_COLUMNS))

    def test_evolution_leaves_the_table_in_contract_order(self) -> None:
        spark = FakeSpark(columns_before_this_change())

        job.evolve_gold_iceberg_table_to_contract(spark, TABLE)

        # Not merely "contains the same names": the INSERT is positional.
        self.assertEqual(tuple(spark.columns), job.GOLD_COLUMNS)

    def test_new_columns_are_placed_rather_than_appended(self) -> None:
        spark = FakeSpark(columns_before_this_change())

        job.evolve_gold_iceberg_table_to_contract(spark, TABLE)

        for statement in spark.statements:
            self.assertTrue(
                statement.rstrip().split()[-2] == "AFTER"
                or statement.rstrip().endswith("FIRST"),
                f"column added without a position: {statement}",
            )

    def test_a_matching_table_is_left_alone(self) -> None:
        spark = FakeSpark(job.GOLD_COLUMNS)

        added = job.evolve_gold_iceberg_table_to_contract(spark, TABLE)

        self.assertEqual(added, ())
        self.assertEqual(spark.statements, [])

    def test_evolution_is_idempotent(self) -> None:
        spark = FakeSpark(columns_before_this_change())

        job.evolve_gold_iceberg_table_to_contract(spark, TABLE)
        statements_after_first = len(spark.statements)
        added = job.evolve_gold_iceberg_table_to_contract(spark, TABLE)

        self.assertEqual(added, ())
        self.assertEqual(len(spark.statements), statements_after_first)

    def test_a_column_the_contract_does_not_declare_is_refused(self) -> None:
        spark = FakeSpark([*job.GOLD_COLUMNS, "column_from_somewhere_else"])

        with self.assertRaisesRegex(ValueError, "contract does not declare"):
            job.evolve_gold_iceberg_table_to_contract(spark, TABLE)

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
            job.evolve_gold_iceberg_table_to_contract(spark, TABLE)


class GoldProjectionColumnTest(unittest.TestCase):
    def test_every_gold_column_has_a_lineage_entry(self) -> None:
        lineage = {entry["output_column"] for entry in job.column_lineage()}

        self.assertEqual(lineage, set(job.GOLD_COLUMNS))

    def test_the_added_columns_are_sourced_from_silver(self) -> None:
        lineage = {entry["output_column"]: entry["inputs"] for entry in job.column_lineage()}

        for column in ADDED_COLUMNS:
            with self.subTest(column=column):
                inputs = lineage[column]
                self.assertEqual(len(inputs), 1)
                self.assertEqual(inputs[0]["dataset"], job.SILVER_CONTRACT_NAME)
                self.assertEqual(inputs[0]["column"], column)

    def test_the_projection_does_not_carry_a_legal_dong_code(self) -> None:
        # Zero of Silver's 1,442 rows fill `primary_bjdong_code`. Projecting it would add a Gold
        # column no producer fills, which root ADR-0040 exists to refuse.
        self.assertNotIn("primary_bjdong_code", job.GOLD_COLUMNS)


if __name__ == "__main__":
    unittest.main()
