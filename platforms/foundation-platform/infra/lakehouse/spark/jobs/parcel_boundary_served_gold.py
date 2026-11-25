#!/usr/bin/env python3
"""Build `gold.parcel_boundary_served`: the parcel boundaries exactly as they are tiled.

Root ADR-0133 §2. One `source_snapshot_id` of `silver.parcel_boundaries` (about forty million rows,
EPSG:4326) is read, the served table is rewritten the way the other served-Gold tables are, and the
tile bake gets the v2 handoff: create-only JSONL parts written by the executors plus a summary
(`served_gold_common`, "The v2 handoff"). Silver rows stay in Spark; nothing comes to the driver.

Parcel geometry takes no staff edits (root ADR-0621 §1): the cadastre is the legal record, and a
wrong parcel is reported to its source. So the served Gold is the edition as Silver holds it, every
row of origin `source`.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import served_gold_common as common
import vworld_parcel_editions as editions
from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages
from platform_contracts import admit_write, column_names, declared_geometry_srid, load_lakehouse_contract, pinned_edition

JOB_NAME = "parcel_boundary_served_gold"
UNIT = "parcels"
FEATURE_ID_PROPERTY = "pnu"
# The tile carries the PNU and nothing else; the bake takes it from `feature_id`.
TILE_PROPERTIES: tuple[str, ...] = ()

SILVER_CONTRACT = load_lakehouse_contract("silver.parcel_boundaries")
SERVED_CONTRACT = load_lakehouse_contract("gold.parcel_boundary_served")
SERVED_COLUMNS: tuple[str, ...] = column_names(SERVED_CONTRACT)
GEOMETRY_SRID: int = declared_geometry_srid(SERVED_CONTRACT)
if declared_geometry_srid(SILVER_CONTRACT) != GEOMETRY_SRID:
    raise ValueError("silver.parcel_boundaries and the served table must share one CRS")
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SNAPSHOT_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
DEFAULT_HANDOFF_PARTS = 64


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--source-snapshot-id",
        help="The one silver.parcel_boundaries source_snapshot_id to serve; the source contract's served edition "
        "when omitted, and refused when it differs unless --allow-non-served-edition (root ADR-0148 §1).",
    )
    editions.add_reader_flag(parser)
    parser.add_argument(
        "--output-dir", required=True, help="Directory for the handoff parts. Created here; never reused."
    )
    parser.add_argument("--summary-output", help="Path for the v2 run summary JSON.")
    parser.add_argument("--handoff-parts", type=int, default=DEFAULT_HANDOFF_PARTS)
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-namespace", default="silver")
    parser.add_argument("--iceberg-table", default="parcel_boundaries")
    parser.add_argument("--served-iceberg-namespace", default="gold")
    parser.add_argument("--served-iceberg-table", default="parcel_boundary_served")
    parser.add_argument(
        "--allow-non-smoke-write",
        action="store_true",
        help="Required to write a served table whose name does not end in _smoke.",
    )
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    args = parser.parse_args(argv)
    args.source_snapshot_id = editions.served_reader_id(
        args.source_snapshot_id, args.allow_non_served_edition, "--source-snapshot-id"
    )
    return args


def validate_args(args: argparse.Namespace) -> None:
    for label in (
        "iceberg_catalog_name",
        "iceberg_namespace",
        "iceberg_table",
        "served_iceberg_namespace",
        "served_iceberg_table",
    ):
        if not IDENTIFIER_PATTERN.match(getattr(args, label)):
            raise ValueError(f"{label.replace('_', ' ')} is not a plain identifier")
    if not SNAPSHOT_ID_PATTERN.match(args.source_snapshot_id):
        raise ValueError("--source-snapshot-id is not a plain snapshot id")
    if not args.served_iceberg_table.endswith("_smoke") and not args.allow_non_smoke_write:
        raise ValueError(
            f"writing {args.served_iceberg_table} needs --allow-non-smoke-write; "
            "only *_smoke tables are written by default"
        )
    if args.handoff_parts <= 0:
        raise ValueError("--handoff-parts must be positive")
    assert_catalog_env()


def qualified(catalog: str, namespace: str, table: str) -> str:
    return f"`{catalog}`.`{namespace}`.`{table}`"


def check_silver(base: Any, source_snapshot_id: str) -> int:
    """Refuses an empty snapshot, a row in another CRS, or a PNU served twice; returns the row count."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    rows = base.count()
    if rows == 0:
        raise ValueError(f"silver.parcel_boundaries holds no rows of snapshot {source_snapshot_id}")
    wrong = base.where(F.col("geometry_srid") != GEOMETRY_SRID).count()
    if wrong:
        raise ValueError(f"{wrong} Silver rows are not EPSG:{GEOMETRY_SRID}")
    repeated = base.groupBy("pnu").count().where(F.col("count") > 1).orderBy("pnu").limit(5).collect()
    if repeated:
        named = ", ".join(f"{row['pnu']} x{row['count']}" for row in repeated)
        raise ValueError(f"snapshot {source_snapshot_id} holds a PNU more than once: {named}")
    return rows


def build_served_frame(base: Any) -> Any:
    """The edition's rows as the served table holds them, every one from the source.

    `base` carries `pnu`, `geometry_wkb`, `geometry_srid`, `geometry_checksum_sha256` and
    `source_snapshot_id`; the snapshot never comes to the driver.
    """

    from pyspark.sql import functions as F  # noqa: PLC0415

    return base.select(
        "pnu",
        "geometry_wkb",
        F.lit(GEOMETRY_SRID).cast("int").alias("geometry_srid"),
        "geometry_checksum_sha256",
        F.lit("source").alias("origin"),
        F.col("source_snapshot_id").cast("string").alias("source_snapshot_id"),
    )


def load_pyspark() -> Any:
    from pyspark.sql import SparkSession  # noqa: PLC0415

    return SparkSession


def build_spark_session(args: argparse.Namespace, SparkSession: Any) -> Any:
    builder = (
        SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(args.handoff_parts))
        .config("spark.executorEnv.PYTHONPATH", common.JOBS_DIR)
    )
    builder = apply_catalog_settings(builder, args.iceberg_catalog_name)
    return builder.config("spark.jars.packages", args.iceberg_packages).getOrCreate()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_args(args)
    output_dir = Path(args.output_dir)
    if output_dir.exists():
        raise FileExistsError(f"served-boundary bake handoff already exists and is evidence: {output_dir}")
    now = datetime.now(timezone.utc).replace(microsecond=0)

    spark = build_spark_session(args, load_pyspark())
    try:
        catalog = args.iceberg_catalog_name
        silver_table = qualified(catalog, args.iceberg_namespace, args.iceberg_table)
        silver_snapshot = common.current_snapshot(spark, silver_table)
        # Pinned to one Iceberg snapshot, so the checks and the write read the same rows.
        # One named edition, by the contract (root ADR-0286 §3, ADR-0376 §2); its rows are all open
        # (2026-11-04: 0 closed rows in either edition), so the predicate keeps every one.
        base = pinned_edition(
            spark.read.option("snapshot-id", str(silver_snapshot)).table(silver_table), SILVER_CONTRACT,
            args.source_snapshot_id,
        ).select("pnu", "geometry_wkb", "geometry_srid", "geometry_checksum_sha256", "source_snapshot_id")
        served_table = common.ensure_table(
            spark,
            qualified(catalog, args.served_iceberg_namespace, args.served_iceberg_table),
            catalog, args.served_iceberg_namespace, SERVED_CONTRACT,
        )
        gold_snapshot, parts, persisted, base_rows = write_full(
            spark, args, base, served_table, silver_snapshot, now, output_dir
        )
    finally:
        spark.stop()

    common.verify_handoff_parts(output_dir, parts, persisted)
    summary = common.summary_v2(
        source_snapshot_id=args.source_snapshot_id, parts=parts,
        job=JOB_NAME, unit=UNIT, feature_id_property=FEATURE_ID_PROPERTY, srid=GEOMETRY_SRID,
        gold_snapshot=gold_snapshot, through=0, handoff=0, appended=0, ledger=0,
        served=persisted, base=base_rows, output=output_dir,
        generated_at=now.isoformat().replace("+00:00", "Z"), counts={},
        extra={"silver_iceberg_snapshot_id": silver_snapshot, "handoff_scope": "full",
               "full_reason": "every parcel of the edition"},
    )
    if args.summary_output:
        path = Path(args.summary_output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("parcel-boundary-served-gold-summary-json " + json.dumps(summary, sort_keys=True))
    return 0


def write_full(
    spark: Any, args: argparse.Namespace, base: Any, served_table: str, silver_snapshot: str, now: Any,
    output_dir: Path,
) -> tuple[str, list[dict[str, Any]], int, int]:
    """The whole served table rewritten from the edition, and every row handed."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    base_rows = check_silver(base, args.source_snapshot_id)
    build_served_frame(base).select(
        *[F.col(name) for name in ("pnu", "geometry_wkb", "geometry_srid", "geometry_checksum_sha256",
                                   "origin", "source_snapshot_id")],
        F.lit(silver_snapshot).alias("silver_iceberg_snapshot_id"),
        F.lit(0).cast("long").alias("edits_through_change_seq"),
        F.lit(now).cast("timestamp").alias("published_at_utc"),
    ).select(*SERVED_COLUMNS).createOrReplaceTempView("served_candidate")
    admit_write(SERVED_CONTRACT["table_name"], "replace", target=served_table)
    spark.sql(f"INSERT OVERWRITE {served_table} SELECT {', '.join(SERVED_COLUMNS)} FROM served_candidate")
    gold_snapshot = common.current_snapshot(spark, served_table)
    written = spark.sql(f"SELECT pnu, geometry_wkb, origin FROM {served_table} VERSION AS OF {gold_snapshot}")
    persisted = written.count()
    if persisted != base_rows:
        raise ValueError(f"served table read back {persisted} rows; the edition holds {base_rows}")
    # The bake reads exactly the committed Gold snapshot, not a recomputation of it.
    parts = common.write_handoff_parts(
        written, output_dir, id_column=FEATURE_ID_PROPERTY, property_columns=TILE_PROPERTIES,
        srid=GEOMETRY_SRID, parts=args.handoff_parts,
    )
    return gold_snapshot, parts, persisted, base_rows


if __name__ == "__main__":
    raise SystemExit(main())
