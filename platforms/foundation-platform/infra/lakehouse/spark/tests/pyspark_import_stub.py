"""Make the Spark jobs importable where PySpark is not installed, which is the CI runner.

Imported for its effect: the jobs directory goes on the import path, and a module-level stub of
PySpark is installed unless the real one is present. A job imports PySpark at module scope while the
pure-logic functions these tests call never call into it (they issue SQL through whatever session
they are handed, and `from __future__ import annotations` keeps the type hints unevaluated).
Stubbing the import is how a pure-logic test reaches pure logic; whether Spark or Iceberg accepts
the SQL is a question for a real session, which a stub cannot answer.

It was the head of the catalog's schema-evolution test until that job's takeover (root ADR-0564 §6,
W103) deleted the job; the tests that imported that test for this effect import this instead.
"""

import sys
import types
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
if str(JOBS_DIR) not in sys.path:
    sys.path.insert(0, str(JOBS_DIR))


def _install_pyspark_stub() -> None:
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
