#!/usr/bin/env python3
"""Reindex the nation's apartment assessments by unit identity (root ADR-0095, ADR-0182).

One invocation joins one vintage, the newest both hub tables hold, for every province, and appends
it as one batch: the table never holds part of a vintage (root ADR-0182). A vintage the table
already holds is not derived again. Every reference date survives; only correction notices for that
same date resolve to their confirmed value. Source snapshots stay immutable and untouched.

The building panel reads the newest vintage's batch (`newest_batch`); each vintage carries the
whole history of reference dates, so the older batches stay as the record of what was served.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path

from lakehouse_engine import apply_catalog_settings, assert_catalog_env, assert_iceberg_runtime_loaded, iceberg_packages
from lakehouse_ingest import append_batch_once
from platform_contracts import (column_names, create_table_columns_sql, evolve_iceberg_table_to_contract,
                                load_lakehouse_contract, partition_clause_sql, rows_as_of)

CONTRACT = "silver.unit_official_price"
# Shared SQL is executed by both Spark and relational fixture tests.
DICTIONARY_SQL = """
SELECT DISTINCT mgmt_key, pnu, COALESCE(dong_name, '') AS dong_name,
       COALESCE(ho_name, '') AS ho_name
FROM exclusive_source
WHERE mgmt_key IS NOT NULL AND TRIM(mgmt_key) <> ''
"""
CONFLICTING_SQL = """
SELECT COUNT(*) AS conflicting FROM (
    SELECT mgmt_key FROM unit_dictionary GROUP BY mgmt_key HAVING COUNT(*) > 1
) conflicts
"""
PRICE_VALID = """mgmt_key IS NOT NULL AND TRIM(mgmt_key) <> ''
    AND TRIM(base_date) REGEXP '^[0-9]{8}$'
    AND CAST(SUBSTR(TRIM(base_date), 1, 4) AS INT) BETWEEN 1000 AND 9999
    AND TRIM(price_won) REGEXP '^[0-9]{1,18}$'"""
PRICES_SQL = f"""
SELECT mgmt_key, base_date, notice_date, price_won FROM (
    SELECT mgmt_key, base_date, notice_date, price_won,
           ROW_NUMBER() OVER (PARTITION BY mgmt_key, base_date
                              ORDER BY notice_date DESC, price_won DESC) AS notice_rank
    FROM (
        SELECT mgmt_key, TRIM(base_date) AS base_date, TRIM(notice_date) AS notice_date,
               CAST(TRIM(price_won) AS BIGINT) AS price_won
        FROM price_source WHERE {PRICE_VALID}
    ) normalized
) ranked WHERE notice_rank = 1
"""
JOIN_SQL = """
SELECT d.mgmt_key AS mgm_bldrgst_pk, d.pnu, d.dong_name, d.ho_name, p.base_date, p.price_won
FROM reference_prices p JOIN unit_dictionary d ON p.mgmt_key = d.mgmt_key
WHERE d.pnu REGEXP '^[0-9]{19}$'
"""


BATCH = re.compile(r"price:[0-9]+\|exclusive:[0-9]+\|vintage:([0-9]{6})")


def batch_source_snapshot_id(price_snapshot: int, exclusive_snapshot: int, vintage: str) -> str:
    """The `source_snapshot_id` of the batch joined from these two Silver snapshots."""
    return f"price:{price_snapshot}|exclusive:{exclusive_snapshot}|vintage:{vintage}"


def batch_vintage(source_snapshot_id: str | None) -> str | None:
    """The vintage a batch's `source_snapshot_id` names, or None for anything else."""
    match = BATCH.fullmatch(source_snapshot_id or "")
    return match.group(1) if match else None


def newest_batch(source_snapshot_ids, commit_order=()) -> str | None:
    """The batch of the newest vintage among `source_snapshot_ids` (root ADR-0182).

    None when any id is not one this job writes: the reader then cannot tell which is newest and
    keeps refusing the mix. A vintage joined again from newer hub snapshots (a release reloaded in
    full after a partial one, 2026-10-11) has a later batch; the last one appended wins, by
    `commit_order` (`batch_commit_order`). Without that order two batches of one vintage are
    refused by name rather than guessed between.
    """
    vintages = {source: batch_vintage(source) for source in set(source_snapshot_ids)}
    if not vintages or None in vintages.values():
        return None
    newest = max(vintages.values())
    batches = {source for source, vintage in vintages.items() if vintage == newest}
    if len(batches) == 1:
        return next(iter(batches))
    ordered = [batch for batch in commit_order if batch in batches]
    if set(ordered) != batches:
        raise ValueError(f"{CONTRACT} holds {len(batches)} batches of vintage {newest} "
                         f"and their order is unknown: {sorted(batches)}")
    return ordered[-1]


def batch_commit_order(spark, qualified_table: str) -> list[str]:
    """The batches main records, in the order they were appended (the ingest registry, ADR-0069).

    Each batch's load identities are `unit-official-price/base-date-v2/<sido>/<batch>`.
    """
    from lakehouse_ingest import read_ingested_objects, unquoted_table_name

    ingested = read_ingested_objects(spark, qualified_table, unquoted_table_name(qualified_table))
    # Position in main's lineage, oldest first: the parent chain orders commits exactly, where
    # commit times can tie.
    heads = spark.sql(f"SELECT snapshot_id FROM {qualified_table}.refs WHERE name = 'main'").collect()
    parents = {int(row.snapshot_id): (None if row.parent_id is None else int(row.parent_id))
               for row in spark.sql(f"SELECT snapshot_id, parent_id FROM {qualified_table}.snapshots").collect()}
    chain, current = [], int(heads[0].snapshot_id) if heads else None
    while current is not None and current in parents and current not in chain:
        chain.append(current)
        current = parents[current]
    position = {snapshot: index for index, snapshot in enumerate(reversed(chain))}
    last: dict[str, int] = {}
    for record, snapshot in ingested.items():
        parts = record.split("/", 3)
        if len(parts) == 4 and batch_vintage(parts[3]) is not None and snapshot in position:
            last[parts[3]] = max(last.get(parts[3], -1), position[snapshot])
    return sorted(last, key=last.__getitem__)


def refuse_conflicting(conflicting: int) -> None:
    if conflicting:
        raise ValueError(f"management keys name multiple units: conflicting={conflicting}")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vintage")
    parser.add_argument("--iceberg-catalog-name", default="r2")
    parser.add_argument("--price-snapshot-id", type=int)
    parser.add_argument("--exclusive-snapshot-id", type=int)
    parser.add_argument("--summary-output")
    args = parser.parse_args(argv)
    for value, pattern, name in [
        (args.vintage, r"[0-9]{6}", "vintage"),
        (args.iceberg_catalog_name, r"[A-Za-z_][A-Za-z0-9_]*", "catalog"),
    ]:
        # No --vintage: main() takes the newest one both tables hold (newest_shared_vintage).
        if value is not None and not re.fullmatch(pattern, value):
            parser.error(f"invalid {name}")
    for snapshot in (args.price_snapshot_id, args.exclusive_snapshot_id):
        if snapshot is not None and snapshot <= 0:
            parser.error("snapshot ids must be positive")
    return args


def newest_shared_vintage(exclusive_vintages, price_vintages):
    """The vintage to join when none is named: the newest month both Silver tables hold.

    The two hub lanes load their newest complete release from the Bronze ledger on their own
    (root ADR-0169), so for a while one table holds a month the other does not yet. A price month
    is never joined to another month's unit dictionary: the newest month both hold is joined, and
    once the slower lane lands the newer month, the next run joins that (root ADR-0182).
    """
    shared = set(exclusive_vintages) & set(price_vintages)
    if not shared:
        raise ValueError("the hub register tables share no vintage")
    return max(shared)


EXCLUSIVE_CONTRACT = "silver.building_register_exclusive_unit"


def exclusive_vintages(spark, qualified_table, frame, snapshot):
    """The vintages the unit table holds at `snapshot`: those its rows carry and those its sealed
    compares record, since a vintage that changed no unit adds no row (root ADR-0318)."""
    from changelog import released_versions, snapshot_lineage  # noqa: PLC0415

    snapshots = [{"snapshot_id": r.snapshot_id, "parent_id": r.parent_id, "summary": r.summary or {}}
                 for r in spark.sql(f"SELECT snapshot_id, parent_id, summary FROM {qualified_table}.snapshots").collect()]
    held = {row.vintage for row in frame.select("vintage").distinct().collect()}
    return sorted(held | released_versions(snapshots, snapshot_lineage(snapshots, snapshot)))


def read_snapshot(spark, qualified_table, requested):
    snapshot = requested
    if snapshot is None:
        row = spark.sql(f"SELECT snapshot_id FROM {qualified_table}.refs WHERE name = 'main'").first()
        if row is None:
            raise ValueError(f"no current snapshot for {qualified_table}")
        snapshot = int(row[0])
    return spark.read.format("iceberg").option("snapshot-id", str(snapshot)).load(qualified_table), snapshot


def main(argv=None):
    args = parse_args(argv)
    from pyspark.sql import SparkSession, functions as F

    assert_catalog_env()
    started = time.monotonic()
    builder = (SparkSession.builder.appName("foundation-platform-unit-official-price")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.autoBroadcastJoinThreshold", "-1")
               .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
               .config("spark.sql.shuffle.partitions", "400"))
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    try:
        assert_iceberg_runtime_loaded(spark, iceberg_packages())
        prefix = f"`{args.iceberg_catalog_name}`.`silver`"
        exclusive_table = f"{prefix}.`building_register_exclusive_unit`"
        exclusive, exclusive_snapshot = read_snapshot(spark, exclusive_table, args.exclusive_snapshot_id)
        prices, price_snapshot = read_snapshot(spark, f"{prefix}.`building_register_apartment_price`", args.price_snapshot_id)
        if args.vintage is None:
            args.vintage = newest_shared_vintage(
                exclusive_vintages(spark, exclusive_table, exclusive, exclusive_snapshot),
                [row.vintage for row in prices.select("vintage").distinct().collect()],
            )
            print(json.dumps({"vintage": args.vintage, "chosen_by": "newest_shared_vintage"}), flush=True)
        contract = load_lakehouse_contract(CONTRACT)
        target = f"{prefix}.`unit_official_price`"
        spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({create_table_columns_sql(contract)}) USING iceberg {partition_clause_sql(contract)}")
        # Refuse a year-only target; preserve it and replay the dated sources (ADR-0101).
        evolve_iceberg_table_to_contract(spark, target, contract)
        source_snapshot = batch_source_snapshot_id(price_snapshot, exclusive_snapshot, args.vintage)
        held = [row.source_snapshot_id for row in spark.table(target).select("source_snapshot_id").distinct().collect()
                if row.source_snapshot_id == source_snapshot]
        if held:
            # The batch of exactly these hub snapshots is already joined. A batch of the same vintage
            # from other snapshots is not: the price table's main moved since (a full reload after a
            # partial one on 2026-10-11), so the vintage is joined again and the panel reads the
            # later batch (newest_batch).
            summary = {"vintage": args.vintage, "appended": False, "held_by": held,
                       "elapsed_seconds": time.monotonic() - started}
            print(f"unit-official-price-unchanged {json.dumps(summary, sort_keys=True)}", flush=True)
            if args.summary_output:
                Path(args.summary_output).write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
            return
        # The unit table appends only each vintage's changed units (root ADR-0318): a vintage's units
        # are each unit's newest row up to it.
        rows_as_of(exclusive, load_lakehouse_contract(EXCLUSIVE_CONTRACT), args.vintage) \
            .createOrReplaceTempView("exclusive_source")
        # The whole dictionary: a key split across provinces is refused.
        dictionary = spark.sql(DICTIONARY_SQL)
        dictionary.createOrReplaceTempView("unit_dictionary")
        conflicting = int(spark.sql(CONFLICTING_SQL).first()[0])
        print(json.dumps({"conflicting": conflicting}), flush=True)
        refuse_conflicting(conflicting)
        price_source = prices.where(F.col("vintage") == args.vintage)
        price_source.createOrReplaceTempView("price_source")
        reference_prices = spark.sql(PRICES_SQL)
        reference_prices.createOrReplaceTempView("reference_prices")
        invalid = int(spark.sql(f"SELECT COUNT(*) FROM price_source WHERE COALESCE(({PRICE_VALID}), FALSE) = FALSE").first()[0])
        unmatched = int(spark.sql("""SELECT COUNT(*) FROM reference_prices p
            LEFT JOIN unit_dictionary d ON p.mgmt_key = d.mgmt_key
            WHERE d.mgmt_key IS NULL OR d.pnu IS NULL OR NOT (d.pnu REGEXP '^[0-9]{19}$')""").first()[0])
        sido = F.substring("pnu", 1, 2)
        # One load identity per province, as the per-province runs wrote them: the registry keeps
        # one record per province and vintage.
        frame = (spark.sql(JOIN_SQL)
                 .withColumn("sido", sido)
                 .withColumn("source_snapshot_id", F.lit(source_snapshot))
                 .withColumn("source_record_id",
                             F.concat(F.lit("unit-official-price/base-date-v2/"), sido, F.lit("/" + source_snapshot))))
        row_count = frame.count()
        if row_count == 0:
            raise ValueError("the vintage produced no price rows; refusing an empty append")
        appended = append_batch_once(spark, frame, column_names(contract), target, CONTRACT)
        summary = {
            "vintage": args.vintage, "source_snapshot_id": source_snapshot,
            "price_snapshot_id": price_snapshot, "exclusive_snapshot_id": exclusive_snapshot,
            "rows": row_count, "conflicting": conflicting, "invalid_prices": invalid,
            "unmatched_prices": unmatched, "elapsed_seconds": time.monotonic() - started,
            **appended,
        }
        text = json.dumps(summary, ensure_ascii=False, indent=2) + "\n"
        if args.summary_output:
            Path(args.summary_output).write_text(text, encoding="utf-8")
        print(text, flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
