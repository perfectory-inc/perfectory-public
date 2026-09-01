"""Record, for a table already loaded, the identities it holds — without adding a row.

The re-run guard reads a table's own commit log to answer "have I appended this before" (root
ADR-0062). Five of the six live tables were loaded before that guard existed, so their log says
nothing, and an empty log reads exactly like a table nothing has been loaded into. Measured
2026-09-01, those five hold 133,583,046 rows between them and re-running any of their loads would
have appended all of it a second time (root ADR-0069).

**It reads the answer out of the table.** The identities come from the rows already there, reduced
to the unit the contract declares, not from a list somebody types here. A typed list would be a
third place the fact lives, and this whole family of defects is that.

**It adds no rows.** A zero-row `append` produces a snapshot and carries its summary — probed on
a scratch table on 2026-08-31, because Iceberg may skip a commit that changes nothing and whether
this one is skipped decides whether the backfill works at all. An `UPDATE` produces a snapshot
with no summary, so a migration that also rewrites values needs both operations.

Refuses when the table already records something: a second backfill would append a second set of
identities for rows that have not moved, and the reader cannot tell which set is the real one.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from lakehouse_ingest import (
    INGEST_BATCH_OBJECTS_KEY,
    MAX_BATCH_SOURCE_RECORDS,
    ingest_batch_token,
    read_ingested_objects,
    snapshot_property_options,
    table_holds_rows,
    unquoted_table_name,
)
from platform_contracts import (
    column_names,
    load_identity_from_value,
    load_lakehouse_contract,
    load_unit,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", required=True, help="Logical table, e.g. silver.parcel_boundaries")
    parser.add_argument(
        "--iceberg-catalog-name",
        default="lakehouse",
        help="Spark catalog the table lives in",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the record. Without it the identities are printed and nothing is committed.",
    )
    return parser.parse_args(argv)


def identities_in_table(frame: Any, table_name: str, column: str) -> list[str]:
    """Reduce the table's own lineage values to the identities one load carries.

    Sorted and deduplicated so the same table always yields the same record, whatever order the
    rows came back in — the token is derived from this list.
    """
    values = [row[column] for row in frame.select(column).distinct().collect()]
    if not values:
        raise ValueError(f"{table_name} has no {column} values to record")
    identities = sorted({load_identity_from_value(table_name, value) for value in values})
    if len(identities) > MAX_BATCH_SOURCE_RECORDS:
        raise ValueError(
            f"{table_name} reduces to {len(identities)} identities and one record may carry at "
            f"most {MAX_BATCH_SOURCE_RECORDS}; the load unit it declares is probably finer than "
            "what a load actually carries"
        )
    return identities


def backfill(spark: Any, args: argparse.Namespace) -> dict[str, Any]:
    """Write one zero-row commit recording what the table already holds."""
    declared = load_unit(args.table)
    if declared["unit"] == "derived":
        raise ValueError(
            f"{args.table} is derived and its producer replaces rather than appends, so there is "
            "nothing for the guard to compare and nothing to record"
        )
    column = declared["column"]
    qualified = f"`{args.iceberg_catalog_name}`.`{args.table.replace('.', '`.`')}`"
    unquoted = unquoted_table_name(qualified)

    if not table_holds_rows(spark, qualified, unquoted):
        raise ValueError(f"{args.table} holds no rows; there is nothing to record")
    already = read_ingested_objects(spark, qualified, unquoted)
    if already:
        raise ValueError(
            f"{args.table} already records {len(already)} identities. A second record would "
            "describe rows that have not moved, and nothing could say which record is the real one."
        )

    frame = spark.table(unquoted)
    identities = identities_in_table(frame, args.table, column)
    token = ingest_batch_token(identities)
    print(
        f"REGISTRY {args.table} unit={declared['unit']} column={column} "
        f"identities={len(identities)} token={token[:12]}"
    )
    for identity in identities:
        print(f"REGISTRY     {identity}")

    if not args.apply:
        print("REGISTRY 계획만 보여줬다. 실제로 쓰려면 --apply")
        return {"recorded": False, "identities": identities, "token": token}

    # An empty frame with the table's own columns, so the write is a commit and nothing else.
    empty = frame.limit(0).select(*column_names(load_lakehouse_contract(args.table)))
    writer = empty.writeTo(qualified)
    for key, value in snapshot_property_options(identities, token).items():
        writer = writer.option(key, value)
    writer.append()

    written = read_ingested_objects(spark, qualified, unquoted)
    missing = [name for name in identities if name not in written]
    if missing:
        raise ValueError(
            f"the commit landed but {args.table} still does not record {missing[:4]}; "
            "the summary did not survive the write"
        )
    print(f"REGISTRY {args.table} 기록함 {len(written)}개")
    return {"recorded": True, "identities": identities, "token": token}


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    # PySpark 는 여기서 부른다. 이 모듈의 시험 레인에는 PySpark 가 없고, 최상단에서 부르면
    # 그 레인의 검사가 전부 조용히 건너뛰어진다.
    from lakehouse_engine import apply_catalog_settings
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(
        "foundation-platform-lakehouse-registry-backfill"
    ).config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        backfill(spark, args)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
