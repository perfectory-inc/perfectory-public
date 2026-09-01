"""Rewrite a table's lineage values into the canonical form, and record them.

`silver.parcel_boundaries` holds 255 bare file names — `30563-23.zip` — where the canonical
identity is the full object key (root ADR-0068). Bare names are not merely shorter: sampling the
bucket found one file name under twelve datasets, so a bare name can mark eleven unloaded datasets
as loaded.

**Why this is not the backfill.** `lakehouse_registry_backfill` refuses a table that already
records something, and this one records 287 identities — 255 bare, plus 32 full keys left by the
appends rolled back on 2026-08-31. Those 32 are why the table is still exposed: a loader passing
full keys finds 32 of them recorded and 223 not, so a batch of sixteen that happens to contain
none of the 32 is entirely unrecognised and appends a second copy.

**Two operations, not one.** Probed on a scratch table: an `UPDATE` produces a snapshot carrying
no summary, and a zero-row `append` produces one that does. Rewriting the values without the
second step would leave the registry saying exactly what it says now.

**The mapping is checked, not assumed.** Every bare name must match exactly one object key in the
source contract. A name matching none, or more than one, stops the migration — inventing the
prefix is how the two spellings arose in the first place.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from lakehouse_ingest import (
    MAX_BATCH_SOURCE_RECORDS,
    ingest_batch_token,
    read_ingested_objects,
    snapshot_property_options,
    table_holds_rows,
    unquoted_table_name,
)
from platform_contracts import load_unit

SOURCE_CONTRACT_ENV = "VWORLD_PARCEL_SOURCE_CONTRACT"
DEFAULT_SOURCE_CONTRACT = (
    Path(__file__).resolve().parents[2] / "contracts" / "vworld-parcel-source-objects.json"
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", required=True, help="Logical table to migrate")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Rewrite and record. Without it the mapping is printed and nothing is committed.",
    )
    return parser.parse_args(argv)


def canonical_by_bare_name() -> dict[str, str]:
    """Map each bare file name to the one object key that ends with it.

    Built from the source contract rather than by gluing a prefix onto the name: the prefix is a
    property of the object, and a name that matches no object or two objects means the mapping is
    not a mapping. Verified 2026-09-01 — across all 272 objects, no two share a file name.
    """
    path = Path(os.environ.get(SOURCE_CONTRACT_ENV, str(DEFAULT_SOURCE_CONTRACT)))
    contract = json.loads(path.read_text(encoding="utf-8"))
    mapping: dict[str, list[str]] = {}
    for entry in contract["objects"]:
        key = entry["object_key"]
        mapping.setdefault(key.rsplit("/", 1)[-1], []).append(key)
    ambiguous = {name: keys for name, keys in mapping.items() if len(keys) > 1}
    if ambiguous:
        raise ValueError(
            f"{len(ambiguous)} file names match more than one object, so a bare name cannot be "
            f"resolved: {sorted(ambiguous)[:3]}"
        )
    return {name: keys[0] for name, keys in mapping.items()}


def plan_migration(values: list[str], mapping: dict[str, str]) -> dict[str, str]:
    """Return the rewrite for each value that is not already canonical.

    A value already carrying a path is left alone; a value that is neither canonical nor a known
    bare name stops the run rather than passing through, because a value nobody can explain is a
    value nobody should rewrite.
    """
    rewrite: dict[str, str] = {}
    unknown: list[str] = []
    for value in values:
        if "/" in value:
            continue
        canonical = mapping.get(value)
        if canonical is None:
            unknown.append(value)
            continue
        rewrite[value] = canonical
    if unknown:
        raise ValueError(
            f"{len(unknown)} lineage values are neither canonical nor a known object file name: "
            f"{sorted(unknown)[:3]}"
        )
    return rewrite


def chunked(items: list[str], size: int) -> list[list[str]]:
    """Split into records small enough for one commit to name."""
    return [items[start : start + size] for start in range(0, len(items), size)]


def refuse_values_sql_cannot_carry(rewrite: dict[str, str]) -> None:
    """Stop before writing a value into SQL text that would not survive being written there.

    The rewrite is issued as one `UPDATE ... CASE`, so a value holding a quote or a backslash would
    end the literal early. These are collected object names and none of them hold one — which is
    why refusing is the right answer rather than escaping: a name that needs escaping is a name
    this migration has not seen and should not guess at.
    """
    unsafe = sorted(
        value
        for pair in rewrite.items()
        for value in pair
        if "'" in value or "\\" in value
    )
    if unsafe:
        raise ValueError(
            f"{len(unsafe)} lineage values contain a quote or backslash and cannot be written into "
            f"the rewrite statement: {unsafe[:3]}"
        )


def migrate(spark: Any, args: argparse.Namespace) -> dict[str, Any]:
    declared = load_unit(args.table)
    if declared["unit"] != "object":
        raise ValueError(
            f"{args.table} identifies its loads by {declared['unit']}, and only an object identity "
            "has a canonical object key to migrate to"
        )
    column = declared["column"]
    qualified = f"`{args.iceberg_catalog_name}`.`{args.table.replace('.', '`.`')}`"
    unquoted = unquoted_table_name(qualified)

    if not table_holds_rows(spark, qualified, unquoted):
        raise ValueError(f"{args.table} holds no rows; there is nothing to migrate")

    frame = spark.table(unquoted)
    values = sorted({row[column] for row in frame.select(column).distinct().collect()})
    rewrite = plan_migration(values, canonical_by_bare_name())
    canonical = sorted({rewrite.get(value, value) for value in values})
    refuse_values_sql_cannot_carry(rewrite)

    print(f"MIGRATE {args.table} column={column} values={len(values)} rewrite={len(rewrite)}")
    for bare, full in sorted(rewrite.items())[:3]:
        print(f"MIGRATE     {bare} -> {full}")
    if len(rewrite) > 3:
        print(f"MIGRATE     ... {len(rewrite) - 3} more")

    if not args.apply:
        print("MIGRATE 계획만 보여줬다. 실제로 하려면 --apply")
        return {"migrated": False, "rewrite": rewrite, "canonical": canonical}

    if rewrite:
        # One statement, so a run that dies leaves the column entirely old or entirely new. Row by
        # row it could leave both, and both is the state this whole family of defects is.
        cases = " ".join(
            f"WHEN {column} = '{bare}' THEN '{full}'" for bare, full in sorted(rewrite.items())
        )
        spark.sql(
            f"UPDATE {qualified} SET {column} = CASE {cases} ELSE {column} END "
            f"WHERE {column} NOT LIKE '%/%'"
        )
        left = [
            row[column]
            for row in spark.table(unquoted).select(column).distinct().collect()
            if "/" not in row[column]
        ]
        if left:
            raise ValueError(f"{len(left)} values are still bare after the rewrite: {left[:3]}")
        print(f"MIGRATE {args.table} 값 {len(rewrite)}개를 정본 형태로 고쳤다")

    # The rewrite carries no summary of its own, so the identities are recorded separately, and in
    # chunks: one record may name at most `MAX_BATCH_SOURCE_RECORDS` objects and this table has 255.
    # The guard merges every snapshot's record, so four commits answer the same question one would.
    for chunk in chunked(canonical, MAX_BATCH_SOURCE_RECORDS):
        writer = spark.table(unquoted).limit(0).writeTo(qualified)
        for key, value in snapshot_property_options(chunk, ingest_batch_token(chunk)).items():
            writer = writer.option(key, value)
        writer.append()

    recorded = read_ingested_objects(spark, qualified, unquoted)
    missing = [name for name in canonical if name not in recorded]
    if missing:
        raise ValueError(
            f"the record landed but {args.table} still does not name {missing[:3]}; "
            "the summary did not survive the write"
        )
    print(f"MIGRATE {args.table} 기록함 {len(canonical)}개 (표 전체 기록 {len(recorded)}개)")
    return {"migrated": True, "rewrite": rewrite, "canonical": canonical}


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    from lakehouse_engine import apply_catalog_settings
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(
        "foundation-platform-lakehouse-lineage-migration"
    ).config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        migrate(spark, args)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
