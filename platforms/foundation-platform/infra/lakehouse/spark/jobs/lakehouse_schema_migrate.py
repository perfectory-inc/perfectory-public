"""Bring every published lakehouse table to its contract (root ADR-0124).

Run by the deploy's `migrate` step after the database migrations, the way those bring Postgres to
the release's schema. For each contract table the lakehouse holds:

1. Undeclared columns stop the run except for the exact known-drift columns. All present column types
   must match. Contract columns in another order are put in contract order: an `INSERT` resolves columns by position and would
   write values into the wrong ones, and Iceberg reorders by metadata alone (rows are read by field
   id), so no data is rewritten.
2. Contract columns the table lacks are planned. A required one on a table with rows needs a
   backfill registered in BACKFILLS -- the function the table's own build uses for that column --
   or the run stops before changing anything.
3. `--mode apply` adds the columns (contract order) and runs the registered backfills: rows read
   from the current main snapshot, pinned by id; the filled column checked for the same row
   count and no empty value; the overwrite rejects concurrent changes and is read back. A required
   registered column left empty after a failed migration is filled on retry.
4. A table the contract declares range distributed is given the contract's sort order as its
   Iceberg write order (`WRITE ORDERED BY`, root ADR-0164). A metadata commit: rows already
   written stay where they are, and the next write lays them out in order.

A table the lakehouse does not hold yet is left to the load that creates it. Prints one line per
table and exits 1 on anything that stops the deploy.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from pyspark.sql import Column, SparkSession

from lakehouse_engine import apply_catalog_settings, assert_iceberg_runtime_loaded, iceberg_packages
from platform_contracts import (
    IDENTIFIER_PATTERN,
    apply_write_order,
    column_names,
    evolve_iceberg_table_to_contract,
    iceberg_table_columns,
    load_lakehouse_artifact,
    spark_sql_type,
    table_properties,
    write_order_drift,
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


KNOWN_DRIFT_PATH = Path(__file__).resolve().parents[2] / "contracts" / "lakehouse-known-drift.json"
KNOWN_DRIFT_SCHEMA = "foundation-platform.lakehouse_known_drift.v1"


def load_known_drift(path: Path = KNOWN_DRIFT_PATH) -> dict[str, dict]:
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("schema_version") != KNOWN_DRIFT_SCHEMA:
        raise ValueError(f"{path.name} schema_version is not {KNOWN_DRIFT_SCHEMA}")
    entries = document.get("tables")
    if not isinstance(entries, dict):
        raise ValueError(f"{path.name} must declare a tables object")
    for table, entry in entries.items():
        if not isinstance(entry, dict):
            raise ValueError(f"{path.name}: invalid entry for {table}")
        missing = [field for field in ("since", "reason", "closes_when")
                   if not isinstance(entry.get(field), str) or not entry[field].strip()]
        if missing:
            raise ValueError(f"{path.name}: {table} does not state {missing}")
        columns = entry.get("extra_columns")
        if not isinstance(columns, list) or not columns:
            raise ValueError(f"{path.name}: {table} must name exact extra_columns; table-wide exceptions are forbidden")
        seen = set()
        for column in columns:
            if not isinstance(column, dict) or set(column) != {"name", "logical_type", "nullable"}:
                raise ValueError(f"{path.name}: {table} requires a name, logical_type and nullable for each extra column")
            name = column["name"]
            if not isinstance(name, str) or not IDENTIFIER_PATTERN.fullmatch(name) or name in seen:
                raise ValueError(f"{path.name}: {table} has an invalid or duplicate extra column")
            if column["nullable"] is not True:
                raise ValueError(f"{path.name}: {table}.{name} must be nullable; an exception cannot require a backfill")
            if not isinstance(column["logical_type"], str):
                raise ValueError(f"{path.name}: {table}.{name} has an invalid logical_type")
            spark_sql_type(column["logical_type"])
            seen.add(name)
    return entries


def known_drift_for_contract(contract: dict, table_name: str) -> dict:
    entry = load_known_drift().get(table_name, {})
    if set(column_names(contract)).intersection(column["name"] for column in entry.get("extra_columns", ())):
        raise MigrationBlocked(f"{table_name}: remove the temporary extra-column exception with the parent-link contract")
    return entry


def migration_contract(contract: dict, actual: tuple[str, ...], table_name: str) -> dict:
    """Keep an explicit temporary extension local to the migrator; ordinary writers stay strict."""
    expected = column_names(contract)
    entry = known_drift_for_contract(contract, table_name)
    allowed = {column["name"]: column for column in entry.get("extra_columns", ())}
    unknown = [name for name in actual if name not in expected and name not in allowed]
    if unknown:
        raise MigrationBlocked(f"{table_name} carries columns the contract does not declare: {unknown}")
    retained = [name for name in allowed if name in actual]
    if entry and not retained:
        raise MigrationBlocked(f"{table_name}: remove the temporary exception; no declared extra columns remain")
    return {**contract, "columns": [*contract["columns"], *(
        {"name": name, "logical_type": allowed[name]["logical_type"], "required": False,
         "nullable": allowed[name]["nullable"]} for name in retained
    )]}


def plan_table(
    contract: dict, actual: tuple[str, ...], has_rows: bool, table_name: str, *,
    actual_types: dict[str, str] | None = None, needs_backfill: tuple[str, ...] = (),
    actual_nullability: dict[str, bool] | None = None,
    actual_properties: dict[str, str] | None = None,
) -> dict:
    """What bringing one table to its contract means, or why it cannot be done automatically."""
    effective = migration_contract(contract, actual, table_name)
    expected = column_names(effective)
    if actual_types is not None:
        for column in effective["columns"]:
            name = column["name"]
            if name in actual and actual_types.get(name) != spark_sql_type(column["logical_type"]).lower():
                raise MigrationBlocked(
                    f"{table_name}.{name} type {actual_types.get(name)!r} differs from {column['logical_type']}"
                )
    if actual_nullability is not None:
        for column in effective["columns"]:
            if "nullable" in column and column["name"] in actual:
                if actual_nullability.get(column["name"]) is not column["nullable"]:
                    raise MigrationBlocked(f"{table_name}.{column['name']} nullable differs from its known-drift declaration")
    present_in_contract_order = [name for name in expected if name in actual]
    reorder = list(actual) != present_in_contract_order
    required = {column["name"] for column in contract["columns"] if column["required"]}
    missing = [name for name in expected if name not in actual]
    backfills = [name for name in expected if name in required and has_rows
                 and (name in missing or name in needs_backfill)]
    unregistered = [name for name in backfills if (table_name, name) not in BACKFILLS]
    if unregistered:
        raise MigrationBlocked(
            f"{table_name} would gain required columns {unregistered} with no registered backfill; "
            "register the build's function in lakehouse_schema_migrate.BACKFILLS"
        )
    write_order = actual_properties is not None and write_order_drift(contract, actual_properties)
    plan = {"add": missing, "backfill": backfills, "reorder": reorder, "write_order": write_order}
    retained = [name for name in expected if name not in column_names(contract)]
    if retained:
        plan["temporary_extra_columns"] = retained
    return plan


def reorder_to_contract(spark: SparkSession, quoted: str, contract: dict, actual: tuple[str, ...]) -> None:
    """Put the present contract columns in contract order: a metadata change, no rows rewritten."""

    present = [name for name in column_names(contract) if name in actual]
    for index, name in enumerate(present):
        position = "FIRST" if index == 0 else f"AFTER {present[index - 1]}"
        spark.sql(f"ALTER TABLE {quoted} ALTER COLUMN {name} {position}")
    if [name for name in iceberg_table_columns(spark, quoted) if name in present] != present:
        raise MigrationBlocked(f"{quoted} did not take the contract order")


def backfill(spark: SparkSession, catalog: str, table_name: str, contract: dict, columns: list[str]) -> dict:
    from pyspark.sql import functions as F

    identifier = f"{catalog}.{table_name}"
    quoted = ".".join(f"`{part}`" for part in identifier.split("."))
    snapshot = spark.sql(
        f"SELECT snapshot_id FROM {quoted}.refs WHERE name = 'main'"
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
        empty = frame.where(empty_value(contract, name)).count()
        if empty:
            raise MigrationBlocked(f"{table_name}.{name} backfill left {empty} of {rows} rows empty")
    # Iceberg validates all concurrent appends/deletes since the pinned read before committing.
    # A plain INSERT OVERWRITE could silently discard a writer that committed during backfill.
    frame.writeTo(identifier).option("isolation-level", "serializable").option(
        "validate-from-snapshot-id", str(snapshot)
    ).overwrite(F.lit(True))
    persisted = spark.table(quoted)
    persisted_rows = persisted.count()
    still_empty = {name: persisted.where(empty_value(contract, name)).count() for name in columns}
    if persisted_rows != rows or any(still_empty.values()):
        raise MigrationBlocked(
            f"{table_name} backfill did not hold: rows {rows}->{persisted_rows}, empty {still_empty}"
        )
    return {"rows": rows, "from_snapshot": str(snapshot)}


def empty_value(contract: dict, name: str) -> Column:
    from pyspark.sql import functions as F

    invalid = F.col(name).isNull()
    column = next(column for column in contract["columns"] if column["name"] == name)
    if column["logical_type"] == "string":
        invalid = invalid | (F.length(F.trim(F.col(name))) == 0)
    return invalid


def inspect_table(spark: SparkSession, quoted: str, contract: dict, table_name: str) -> dict:
    frame = spark.table(quoted)
    actual = tuple(field.name for field in frame.schema.fields)
    types = {field.name: field.dataType.simpleString() for field in frame.schema.fields}
    nullable = {field.name: field.nullable for field in frame.schema.fields}
    properties = table_properties(spark, quoted)
    # Validate names/types before any data scan or ALTER, including retained temporary columns.
    plan_table(contract, actual, False, table_name, actual_types=types, actual_nullability=nullable)
    has_rows = frame.limit(1).count() > 0
    # ADD COLUMN and backfill are separate commits. A failed backfill must remain actionable on
    # the next deploy even though its required column now exists.
    incomplete = tuple(column["name"] for column in contract["columns"]
                       if column["required"] and column["name"] in actual
                       and (table_name, column["name"]) in BACKFILLS and has_rows
                       and frame.where(empty_value(contract, column["name"])).limit(1).count())
    return plan_table(contract, actual, has_rows, table_name, actual_types=types,
                      actual_nullability=nullable, needs_backfill=incomplete,
                      actual_properties=properties)


def apply_table(spark: SparkSession, catalog: str, table_name: str, contract: dict, plan: dict) -> dict:
    quoted = ".".join(f"`{part}`" for part in f"{catalog}.{table_name}".split("."))
    actual = iceberg_table_columns(spark, quoted)
    effective = migration_contract(contract, actual, table_name)
    if plan["reorder"]:
        reorder_to_contract(spark, quoted, effective, actual)
    added = evolve_iceberg_table_to_contract(spark, quoted, effective)
    result = {"added": list(added)}
    if plan["backfill"]:
        result["backfilled"] = backfill(spark, catalog, table_name, effective, plan["backfill"])
    if plan["write_order"]:
        result["write_ordered_by"] = list(contract["sort_order"]) if apply_write_order(
            spark, quoted, contract) else []
    remaining = inspect_table(spark, quoted, contract, table_name)
    if remaining["add"] or remaining["backfill"] or remaining["reorder"] or remaining["write_order"]:
        raise MigrationBlocked(f"{table_name} still needs migration: {remaining}")
    return result


def main() -> int:
    from pyspark.sql import SparkSession

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
        known_drift = load_known_drift()
        unknown_entries = sorted(set(known_drift) - set(contracts))
        if unknown_entries:
            raise MigrationBlocked(f"known drift names tables with no contract: {unknown_entries}")
        for table_name in known_drift:
            known_drift_for_contract(contracts[table_name], table_name)
        for table_name, contract in sorted(contracts.items()):
            quoted = ".".join(f"`{part}`" for part in f"{catalog}.{table_name}".split("."))
            if not spark.catalog.tableExists(f"{catalog}.{table_name}"):
                if table_name in known_drift:
                    raise MigrationBlocked(f"{table_name}: remove the temporary exception; the table is absent")
                report[table_name] = {"state": "absent"}
                print(f"lakehouse-migrate {table_name}: absent (its load creates it)")
                continue
            try:
                plan = inspect_table(spark, quoted, contract, table_name)
            except MigrationBlocked as error:
                blocked.append(str(error))
                report[table_name] = {"state": "blocked", "reason": str(error)}
                print(f"lakehouse-migrate {table_name}: BLOCKED {error}")
                continue
            if not plan["add"] and not plan["reorder"] and not plan["backfill"] and not plan["write_order"]:
                entry = ({"state": "known_drift", **plan} if plan.get("temporary_extra_columns")
                         else {"state": "matches"})
                if table_name in known_drift:
                    entry.update({field: known_drift[table_name][field] for field in ("since", "reason", "closes_when")})
                report[table_name] = entry
                print(f"lakehouse-migrate {table_name}: {json.dumps(entry)}")
                continue
            entry = {"state": "planned" if args.mode == "plan" else "applied", **plan}
            if table_name in known_drift:
                entry.update({field: known_drift[table_name][field] for field in ("since", "reason", "closes_when")})
            if args.mode == "apply":
                entry.update(apply_table(spark, catalog, table_name, contract, plan))
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
