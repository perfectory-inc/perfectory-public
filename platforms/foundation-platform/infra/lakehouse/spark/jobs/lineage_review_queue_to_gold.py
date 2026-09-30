#!/usr/bin/env python3
"""Write the parcels a person has to decide to `gold.lineage_review_queue` (root ADR-0113 §10).

Reads the whole `silver.parcel_lineage` (or one sido's successors), keeps each parcel whose best
row is `needs_review` or `pending` and that no steward row has decided, and rewrites the unit's
partition. The lineage rows needing review are few next to the country, so the rule runs on the
driver through `lineage_review_queue.review_queue`, the function the tests pin.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import lineage_review_queue as q
from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages
from parcel_lineage_to_silver import IDENTIFIER, PREFIX
from platform_contracts import (
    column_names,
    create_table_columns_sql,
    evolve_iceberg_table_to_contract,
    load_lakehouse_contract,
    partition_clause_sql,
    spark_sql_type,
)

JOB_NAME = "lineage_review_queue_to_gold"
CONTRACT = load_lakehouse_contract("gold.lineage_review_queue")
UNIT = "parcel"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sido", help="Only successors under these comma-separated sido prefixes (a smoke run)")
    parser.add_argument("--summary-output")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--gold-namespace", default="gold")
    parser.add_argument("--allow-non-smoke-write", action="store_true")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    for label in ("iceberg_catalog_name", "gold_namespace"):
        if not IDENTIFIER.fullmatch(getattr(args, label)):
            raise ValueError(f"{label} must be a plain SQL identifier")
    if args.sido is not None and not all(PREFIX.fullmatch(p) for p in args.sido.split(",")):
        raise ValueError("--sido must be comma-separated two-digit prefixes")
    if args.sido is not None and args.gold_namespace == "gold":
        raise ValueError("a --sido run would rewrite the national queue with one sido; write a smoke namespace")
    if args.gold_namespace == "gold" and not args.allow_non_smoke_write:
        raise ValueError("writing the gold namespace needs --allow-non-smoke-write")
    assert_catalog_env()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_args(args)
    from pyspark.sql import SparkSession  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415

    now = datetime.now(timezone.utc).replace(microsecond=0)
    builder = (
        SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).config("spark.jars.packages", args.iceberg_packages).getOrCreate()
    cat = f"`{args.iceberg_catalog_name}`"
    try:
        lineage = spark.table(f"{cat}.`silver`.`parcel_lineage`")
        if args.sido:
            lineage = lineage.where(F.substring("successor_pnu", 1, 2).isin(args.sido.split(",")))
        # Only successors that ever had a review-grade row can be on the queue; read all their rows.
        subjects = lineage.where(F.col("grade").isin(*sorted(q.REVIEW_GRADES))).select("successor_pnu").distinct()
        rows = [
            r.asDict()
            for r in lineage.join(subjects, on="successor_pnu", how="left_semi")
            .select("predecessor_pnu", "successor_pnu", "relation", "grade", "evidence_kind", "evidence_ref",
                    "from_snapshot_id", "to_snapshot_id")
            .collect()
        ]
        items, counts = q.review_queue(rows, UNIT)

        table = f"{cat}.`{args.gold_namespace}`.`lineage_review_queue`"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.`{args.gold_namespace}`")
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
{create_table_columns_sql(CONTRACT)}
            )
            USING iceberg
            {partition_clause_sql(CONTRACT)}
            TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
            """
        )
        evolve_iceberg_table_to_contract(spark, table, CONTRACT)
        columns = column_names(CONTRACT)
        schema = ", ".join(f"{c['name']} {spark_sql_type(c['logical_type'])}" for c in CONTRACT["columns"])
        out = [
            {"unit": UNIT, "subject_code": i.subject_code, "item_id": i.item_id, "status": i.status,
             "candidates_json": i.candidates_json, "evidence_etag": i.evidence_etag, "from_snapshot_id": i.from_snapshot_id or None,
             "to_snapshot_id": i.to_snapshot_id or None, "published_at_utc": now}
            for i in items
        ]
        spark.createDataFrame(out, schema=schema).select(*columns).createOrReplaceTempView("review_queue")
        if out:
            spark.sql(f"INSERT OVERWRITE {table} SELECT {', '.join(columns)} FROM review_queue")
        else:
            # Dynamic overwrite of an empty frame replaces no partition; an emptied queue must read empty.
            spark.sql(f"DELETE FROM {table} WHERE unit = '{UNIT}'")
        written = spark.table(table).where(F.col("unit") == UNIT).count()
        if written != len(out):
            raise ValueError(f"wrote {len(out)} review items but the table holds {written}")
        by_sido: dict[str, int] = {}
        for i in items:
            by_sido[i.subject_code[:2]] = by_sido.get(i.subject_code[:2], 0) + 1
        summary: dict[str, Any] = {"job": JOB_NAME, "unit": UNIT, "open": len(items), **dict(counts), "open_by_sido": by_sido}
    finally:
        spark.stop()
    if args.summary_output:
        Path(args.summary_output).write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("lineage-review-queue-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
