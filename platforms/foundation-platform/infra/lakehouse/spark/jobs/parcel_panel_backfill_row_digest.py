"""Bring the published Gold parcel panel up to its contract without rebuilding it.

The panel was last built before the contract gained `row_digest` (ADR-0099) and
`attached_via_json` (ADR-0113 §6). Rebuilding it would re-read and re-join every Silver source
for forty million parcels. Neither column needs that:

- `attached_via_json` says which sections came through the parcel lineage. No lineage exists yet,
  so a rebuild would write NULL for every parcel -- which is what an added column holds.
- `row_digest` is a function of the row's own content columns. It is computed here with the same
  function the build uses (`parcel_panel_silver_to_gold.row_digest_column`), and the NULL
  `attached_via_json` is one the digest skips, so every value equals what a rebuild would write.

The rows are read from the snapshot current at start, pinned by id, so the overwrite can never
read its own output. The table is evolved to the contract (columns added in contract order), every
row gets its digest, the result passes the build's own quality validation with the same row count,
and the overwrite is read back: same count, no NULL digest. Iceberg keeps the previous snapshot.
"""

from __future__ import annotations

import argparse
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse_engine import apply_catalog_settings, assert_iceberg_runtime_loaded, iceberg_packages
from parcel_panel_silver_to_gold import (
    GOLD_COLUMNS,
    GOLD_CONTRACT,
    row_digest_column,
    validate_gold_frame,
)
from platform_contracts import evolve_iceberg_table_to_contract


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--iceberg-catalog-name",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_CATALOG_NAME", "r2"),
    )
    parser.add_argument("--namespace", default="gold")
    parser.add_argument("--table", default="parcel_panel")
    parser.add_argument(
        "--iceberg-packages",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_PACKAGES", iceberg_packages()),
    )
    parser.add_argument("--summary-output")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    table = f"`{args.iceberg_catalog_name}`.`{args.namespace}`.`{args.table}`"
    builder = SparkSession.builder.appName("foundation-platform-parcel-panel-backfill-row-digest").config(
        "spark.sql.session.timeZone", "UTC"
    )
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    assert_iceberg_runtime_loaded(spark, args.iceberg_packages)
    try:
        before_snapshot = spark.sql(
            f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).first()[0]
        added = evolve_iceberg_table_to_contract(spark, table, GOLD_CONTRACT)

        identifier = f"{args.iceberg_catalog_name}.{args.namespace}.{args.table}"
        current = spark.read.format("iceberg").option("snapshot-id", before_snapshot).load(identifier)
        for column in ("attached_via_json", "row_digest"):
            if column not in current.columns:
                current = current.withColumn(column, F.lit(None).cast("string"))
        backfilled = current.withColumn("row_digest", row_digest_column()).select(*GOLD_COLUMNS)
        before_count = current.count()
        row_count, metrics = validate_gold_frame(backfilled, before_count)

        backfilled.createOrReplaceTempView("parcel_panel_backfilled")
        spark.sql(f"INSERT OVERWRITE {table} SELECT {', '.join(GOLD_COLUMNS)} FROM parcel_panel_backfilled")

        persisted = spark.table(table)
        persisted_count = persisted.count()
        missing_digest = persisted.where(F.col("row_digest").isNull()).count()
        after_snapshot = spark.sql(
            f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).first()[0]
        if persisted_count != row_count or missing_digest:
            raise ValueError(
                f"backfill did not hold: rows before={row_count} after={persisted_count}, "
                f"rows without a digest={missing_digest}"
            )

        summary = {
            "table": f"{args.namespace}.{args.table}",
            "added_columns": list(added),
            "rows": persisted_count,
            "rows_without_digest": missing_digest,
            "quality_metrics": metrics,
            "snapshot_before": str(before_snapshot),
            "snapshot_after": str(after_snapshot),
        }
        if args.summary_output:
            with open(args.summary_output, "w", encoding="utf-8") as handle:
                json.dump(summary, handle, ensure_ascii=False, indent=2)
        print(f"parcel-panel-backfill-ok {json.dumps(summary, ensure_ascii=False)}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
