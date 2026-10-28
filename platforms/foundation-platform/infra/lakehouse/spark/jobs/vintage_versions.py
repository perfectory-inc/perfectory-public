#!/usr/bin/env python3
"""Store the full copies of a vintage table as versions, and prove every vintage still reads back
exactly (root ADR-0317).

`silver.building_register_apartment_price` appends a full copy of the national price file every
vintage (about 202M rows, 7 GB), and `silver.unit_official_price` a full join of it every time the
join's inputs move (about 189M rows; six batches in 30 days held 1.207B rows, 2026-10-28). Between
vintages about 0.02% of the rows change (Seoul, 202609 -> 202610: 6,578 of 29.07M). This one-time
migration writes, for each table, its `_versions` table alongside it:

- for each vintage of the source, oldest first, that the target does not record yet
  (`read_editions`): the rows of the keys whose rows differ from the target's current rows, and a
  removal marker per vanished key (`silver_release_compare.release_changes`, root ADR-0275), in one
  commit that records the vintage and the source batch;
- then the parity check: for every vintage, `rows_as_of(target, vintage)` equals the source's copy
  of that vintage on the key and content columns, row for row with repeats counted (`exceptAll`
  both ways). Any difference fails the run.

It reads the source and writes only the target: the source tables are not touched, and readers
keep reading them until the switch (root ADR-0317 §5). A re-run continues from the first vintage the
target does not record, and repeats only the parity check. Run by an operator in a window: it reads
every vintage of the source.

The last stdout line, and `--summary-output`, is `vintage-versions {json}`.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from lakehouse_engine import apply_catalog_settings, assert_catalog_env, assert_iceberg_runtime_loaded, iceberg_packages
from lakehouse_ingest import (MAIN, edition_property_option, ingest_batch_token, read_editions, read_ingested_objects,
                              snapshot_property_options)
from platform_contracts import (column_names, current_row_latest, current_rows, ensure_contract_table,
                                load_lakehouse_contract, rows_as_of)
from silver_release_compare import commit_stamp, refuse_older_edition, release_changes

JOB_NAME = "vintage-versions"
# What a price line is, as the price lane compares it: its contract's compare.content_columns.
APARTMENT_PRICE_LANE = Path(__file__).resolve().parents[2] / "contracts" / "hub-building-register-apartment-price-source-objects.json"
APARTMENT_PRICE_CONTENT = tuple(
    json.loads(APARTMENT_PRICE_LANE.read_text(encoding="utf-8"))["silver_refresh"]["compare"]["content_columns"])
# What a joined unit price is, besides its key: where it is served and its value.
UNIT_PRICE_CONTENT = ("pnu", "dong_name", "ho_name", "price_won", "sido")


@dataclass(frozen=True)
class Migration:
    source: str
    content: tuple[str, ...]
    # (spark, the source table read whole, the source table's quoted name) -> {vintage: (rows, source batch id)}
    vintages: Callable[[Any, Any, str], dict[str, tuple[Any, str]]]
    # (target, vintage, the vintage's rows, source batch id) -> what the commit records as loaded
    records: Callable[[str, str, Any, str], list[str]]


def apartment_price_vintages(spark: Any, source: Any, qualified: str) -> dict[str, tuple[Any, str]]:
    """Each vintage's rows: the lane appends one full copy per vintage (root ADR-0169)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    batches: dict[str, list[str]] = {}
    for row in source.select("vintage", "source_snapshot_id").distinct().collect():
        batches.setdefault(row.vintage, []).append(row.source_snapshot_id)
    many = {vintage: ids for vintage, ids in batches.items() if len(ids) != 1}
    if many:
        # Two loads of one vintage are not told apart here; the operator decides, not this job.
        raise ValueError(f"vintages with more than one load: {many}")
    return {vintage: (source.where(F.col("vintage") == vintage), ids[0]) for vintage, ids in batches.items()}


def unit_price_vintages(spark: Any, source: Any, qualified: str) -> dict[str, tuple[Any, str]]:
    """Each vintage's rows: its last batch, the one the building panel serves (root ADR-0185)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    from unit_official_price import batch_commit_order, batch_vintage, newest_batch  # noqa: PLC0415

    by_vintage: dict[str, list[str]] = {}
    for row in source.select("source_snapshot_id").distinct().collect():
        vintage = batch_vintage(row.source_snapshot_id)
        if vintage is None:
            raise ValueError(f"{qualified} holds a batch the join did not write: {row.source_snapshot_id}")
        by_vintage.setdefault(vintage, []).append(row.source_snapshot_id)
    order = batch_commit_order(spark, qualified)
    chosen = {vintage: newest_batch(ids, order) for vintage, ids in by_vintage.items()}
    return {vintage: (source.where(F.col("source_snapshot_id") == batch).withColumn("vintage", F.lit(vintage)), batch)
            for vintage, batch in chosen.items()}


MIGRATIONS = {
    "silver.building_register_apartment_price_versions": Migration(
        "silver.building_register_apartment_price", APARTMENT_PRICE_CONTENT, apartment_price_vintages,
        # The vintage's handoff parts, as the price lane records them: once moved to the versions
        # table, the lane finds the migrated vintages loaded and does not export them again.
        lambda target, vintage, rows, batch: sorted(row[0] for row in rows.select("source_part_id").distinct().collect())),
    "silver.unit_official_price_versions": Migration(
        "silver.unit_official_price", UNIT_PRICE_CONTENT, unit_price_vintages,
        lambda target, vintage, rows, batch: [migration_record(target, vintage, batch)]),
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--target", required=True, choices=sorted(MIGRATIONS))
    parser.add_argument("--parity-only", action="store_true", help="Check every vintage; write nothing")
    parser.add_argument("--iceberg-catalog-name", default="r2")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    parser.add_argument("--summary-output")
    return parser.parse_args(argv)


def migration_record(target: str, vintage: str, batch: str) -> str:
    """The commit's load record: the vintage and the source batch it came from."""
    return f"{JOB_NAME}/{target}/{vintage}/{batch}".replace(",", "_")


def vintage_changes(staged: Any, versions: Any, contract: dict[str, Any], content: tuple[str, ...], vintage: str,
                    batch: str, instant: Any) -> Any:
    """The rows one vintage appends to `versions`: its changes against the current rows."""
    latest = current_row_latest(contract)
    stamp, _ = commit_stamp(latest, vintage, instant)
    columns = list(column_names(contract))
    return release_changes(staged, current_rows(versions, contract).select(*columns), key=latest.key,
                           content=list(content), removal=latest.removal_column, source_snapshot_id=batch,
                           ingested_at=instant, columns=columns, stamp=stamp)


def parity(versions: Any, full_copy: Any, contract: dict[str, Any], content: tuple[str, ...], vintage: str) -> dict[str, int]:
    """How many rows the reconstruction of `vintage` lacks and adds against its full copy."""
    compared = [*current_row_latest(contract).key, *content]
    rebuilt = rows_as_of(versions, contract, vintage).select(*compared)
    copy = full_copy.select(*compared)
    return {"missing": copy.exceptAll(rebuilt).count(), "extra": rebuilt.exceptAll(copy).count()}


def migrate(spark: Any, args: argparse.Namespace) -> dict[str, Any]:
    from pyspark.sql import functions as F  # noqa: PLC0415

    migration = MIGRATIONS[args.target]
    contract = load_lakehouse_contract(args.target)
    catalog = args.iceberg_catalog_name
    source_q = "`{}`.`{}`.`{}`".format(catalog, *migration.source.split(".", 1))
    target_u = f"{catalog}.{args.target}"
    target_q = "`{}`.`{}`.`{}`".format(catalog, *args.target.split(".", 1))
    columns = list(column_names(contract))
    vintages = migration.vintages(spark, spark.table(source_q), source_q)
    report: dict[str, Any] = {"target": args.target, "source": migration.source, "vintages": {}}
    if not args.parity_only:
        ensure_contract_table(spark, target_q, contract)
    for vintage in sorted(vintages):
        staged, batch = vintages[vintage]
        entry: dict[str, Any] = {"source_batch": batch}
        if not args.parity_only and vintage not in read_editions(spark, target_q, target_u, MAIN):
            refuse_older_edition(vintage, read_editions(spark, target_q, target_u, MAIN))
            records = migration.records(args.target, vintage, staged, batch)
            held = [record for record in records if record in read_ingested_objects(spark, target_q, target_u, MAIN)]
            if held:
                raise ValueError(f"{args.target} records {held[:3]} without their edition {vintage}")
            started = time.monotonic()
            instant = F.lit(datetime.now(timezone.utc)).cast("timestamp")
            # The full copy as versions: the commit's instant, and no removal mark on a source row.
            staged_rows = (staged.withColumn("ingested_at_utc", instant)
                           .withColumn("row_state", F.lit(None).cast("string")).select(*columns))
            changes = vintage_changes(staged_rows, spark.table(target_q), contract, migration.content, vintage,
                                      batch, instant)
            writer = changes.writeTo(target_u)
            options = {**snapshot_property_options(records, ingest_batch_token(records)),
                       **edition_property_option(vintage)}
            for option, value in options.items():
                writer = writer.option(option, value)
            writer.append()
            entry["appended_seconds"] = round(time.monotonic() - started, 1)
        appended = spark.table(target_q).where(F.col(current_row_latest(contract).newest_first[0]) == vintage)
        entry["version_rows"] = appended.count()
        entry["full_copy_rows"] = staged.count()
        entry.update(parity(spark.table(target_q), staged, contract, migration.content, vintage))
        print(f"{JOB_NAME} {args.target} {vintage}: {json.dumps(entry, sort_keys=True)}", flush=True)
        report["vintages"][vintage] = entry
    report["parity"] = all(entry["missing"] == 0 and entry["extra"] == 0 for entry in report["vintages"].values())
    return report


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    from pyspark.sql import SparkSession  # noqa: PLC0415

    assert_catalog_env(args.iceberg_catalog_name)
    builder = (SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "400"))
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        assert_iceberg_runtime_loaded(spark, args.iceberg_packages)
        summary = migrate(spark, args)
    finally:
        spark.stop()
    if args.summary_output:
        Path(args.summary_output).write_text(json.dumps(summary, sort_keys=True), encoding="utf-8")
    print(f"{JOB_NAME} " + json.dumps(summary, sort_keys=True), flush=True)
    return 0 if summary["parity"] else 1


if __name__ == "__main__":
    sys.exit(main())
