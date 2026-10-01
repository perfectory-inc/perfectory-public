"""Bring every published lakehouse table to its contract (root ADR-0124).

Run by the deploy's `migrate` step after the database migrations, the way those bring Postgres to
the release's schema. For each contract table the lakehouse holds:

1. Columns the live table has and the contract does not stop the run. Contract columns present in
   another order are put in contract order: an `INSERT` resolves columns by position and would
   write values into the wrong ones, and Iceberg reorders by metadata alone (rows are read by field
   id), so no data is rewritten.
2. Contract columns the table lacks are planned. A required one on a table with rows needs a
   backfill registered in BACKFILLS -- the function the table's own build uses for that column --
   or the run stops before changing anything.
3. `--mode apply` adds the columns (contract order) and runs the registered backfills: rows read
   from the snapshot current at start, pinned by id; the filled column checked for the same row
   count and no NULL; the overwrite read back. Iceberg keeps the previous snapshot.

A table the lakehouse does not hold yet is left to the load that creates it. Prints one line per
table and exits 1 on anything that stops the deploy.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
from typing import Callable

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

from lakehouse_engine import apply_catalog_settings, assert_iceberg_runtime_loaded, iceberg_packages
from platform_contracts import (
    column_names,
    evolve_iceberg_table_to_contract,
    iceberg_table_columns,
    load_lakehouse_artifact,
    spark_sql_type,
)


# (table, column) -> (the module that builds the table, its function computing that column from the
# row). The backfill calls the build's own function, so a backfilled value equals a rebuilt one.
BACKFILLS: dict[tuple[str, str], tuple[str, str]] = {
    ("gold.parcel_panel", "row_digest"): ("parcel_panel_silver_to_gold", "row_digest_column"),
    ("gold.building_panel", "row_digest"): ("building_panel_silver_to_gold", "row_digest_column"),
}


def backfill_function(table_name: str, column: str) -> Callable[[], Column]:
    module, function = BACKFILLS[(table_name, column)]
    return getattr(importlib.import_module(module), function)


class MigrationBlocked(ValueError):
    pass


def plan_table(contract: dict, actual: tuple[str, ...], has_rows: bool, table_name: str) -> dict:
    """What bringing one table to its contract means, or why it cannot be done automatically."""

    expected = column_names(contract)
    unknown = [name for name in actual if name not in expected]
    if unknown:
        raise MigrationBlocked(f"{table_name} carries columns the contract does not declare: {unknown}")
    present_in_contract_order = [name for name in expected if name in actual]
    reorder = list(actual) != present_in_contract_order
    required = {column["name"] for column in contract["columns"] if column["required"]}
    missing = [name for name in expected if name not in actual]
    backfills = [name for name in missing if name in required and has_rows]
    unregistered = [name for name in backfills if (table_name, name) not in BACKFILLS]
    if unregistered:
        raise MigrationBlocked(
            f"{table_name} would gain required columns {unregistered} with no registered backfill; "
            "register the build's function in lakehouse_schema_migrate.BACKFILLS"
        )
    return {"add": missing, "backfill": backfills, "reorder": reorder}


def reorder_to_contract(spark: SparkSession, quoted: str, contract: dict, actual: tuple[str, ...]) -> None:
    """Put the present contract columns in contract order: a metadata change, no rows rewritten."""

    present = [name for name in column_names(contract) if name in actual]
    for index, name in enumerate(present):
        position = "FIRST" if index == 0 else f"AFTER {present[index - 1]}"
        spark.sql(f"ALTER TABLE {quoted} ALTER COLUMN {name} {position}")
    if [name for name in iceberg_table_columns(spark, quoted) if name in present] != present:
        raise MigrationBlocked(f"{quoted} did not take the contract order")


def backfill(spark: SparkSession, catalog: str, table_name: str, contract: dict, columns: list[str]) -> dict:
    identifier = f"{catalog}.{table_name}"
    quoted = ".".join(f"`{part}`" for part in identifier.split("."))
    snapshot = spark.sql(
        f"SELECT snapshot_id FROM {quoted}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).first()[0]
    frame = spark.read.format("iceberg").option("snapshot-id", snapshot).load(identifier)
    expected = column_names(contract)
    types = {column["name"]: spark_sql_type(column["logical_type"]) for column in contract["columns"]}
    for name in expected:
        if name not in frame.columns:
            frame = frame.withColumn(name, F.lit(None).cast(types[name]))
    for name in columns:
        frame = frame.withColumn(name, backfill_function(table_name, name)())
    frame = frame.select(*expected)
    rows = frame.count()
    for name in columns:
        empty = frame.where(F.col(name).isNull()).count()
        if empty:
            raise MigrationBlocked(f"{table_name}.{name} backfill left {empty} of {rows} rows empty")
    frame.createOrReplaceTempView("lakehouse_migrate_backfill")
    spark.sql(f"INSERT OVERWRITE {quoted} SELECT {', '.join(expected)} FROM lakehouse_migrate_backfill")
    persisted = spark.table(quoted)
    persisted_rows = persisted.count()
    still_empty = {name: persisted.where(F.col(name).isNull()).count() for name in columns}
    if persisted_rows != rows or any(still_empty.values()):
        raise MigrationBlocked(
            f"{table_name} backfill did not hold: rows {rows}->{persisted_rows}, empty {still_empty}"
        )
    return {"rows": rows, "from_snapshot": str(snapshot)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mode", choices=("plan", "apply"), required=True)
    parser.add_argument(
        "--iceberg-catalog-name", default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_CATALOG_NAME", "r2")
    )
    parser.add_argument(
        "--iceberg-packages", default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_PACKAGES", iceberg_packages())
    )
    parser.add_argument("--summary-output")
    args = parser.parse_args()

    builder = SparkSession.builder.appName("foundation-platform-lakehouse-schema-migrate").config(
        "spark.sql.session.timeZone", "UTC"
    )
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    assert_iceberg_runtime_loaded(spark, args.iceberg_packages)
    catalog = args.iceberg_catalog_name
    report, blocked = {}, []
    try:
        contracts = load_lakehouse_artifact()["contracts"]
        for table_name, contract in sorted(contracts.items()):
            quoted = ".".join(f"`{part}`" for part in f"{catalog}.{table_name}".split("."))
            if not spark.catalog.tableExists(f"{catalog}.{table_name}"):
                report[table_name] = {"state": "absent"}
                print(f"lakehouse-migrate {table_name}: absent (its load creates it)")
                continue
            actual = iceberg_table_columns(spark, quoted)
            has_rows = spark.table(quoted).limit(1).count() > 0
            try:
                plan = plan_table(contract, actual, has_rows, table_name)
            except MigrationBlocked as error:
                blocked.append(str(error))
                report[table_name] = {"state": "blocked", "reason": str(error)}
                print(f"lakehouse-migrate {table_name}: BLOCKED {error}")
                continue
            if not plan["add"] and not plan["reorder"]:
                report[table_name] = {"state": "matches"}
                print(f"lakehouse-migrate {table_name}: matches")
                continue
            entry = {"state": "planned" if args.mode == "plan" else "applied", **plan}
            if args.mode == "apply":
                if plan["reorder"]:
                    reorder_to_contract(spark, quoted, contract, actual)
                added = evolve_iceberg_table_to_contract(spark, quoted, contract)
                entry["added"] = list(added)
                if plan["backfill"]:
                    entry["backfilled"] = backfill(spark, catalog, table_name, contract, plan["backfill"])
            report[table_name] = entry
            print(f"lakehouse-migrate {table_name}: {json.dumps(entry, ensure_ascii=False)}")
    finally:
        if args.summary_output:
            with open(args.summary_output, "w", encoding="utf-8") as handle:
                json.dump({"mode": args.mode, "tables": report, "blocked": blocked}, handle, ensure_ascii=False, indent=2)
        spark.stop()
    counts = {}
    for entry in report.values():
        counts[entry["state"]] = counts.get(entry["state"], 0) + 1
    print(f"lakehouse-migrate {args.mode}: {json.dumps(counts)}")
    return 1 if blocked else 0


if __name__ == "__main__":
    raise SystemExit(main())
