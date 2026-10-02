#!/usr/bin/env python3
"""Build `gold.parcel_boundary_served`: the parcel boundaries exactly as they are tiled.

Root ADR-0133 §2. One `source_snapshot_id` of `silver.parcel_boundaries` (about forty million rows,
EPSG:4326) is read, every ledgered `parcels` edit in `silver.map_edit_ledger` is applied in
`change_seq` order, the served table is rewritten the way the other served-Gold tables are, and
the tile bake gets the v2 handoff: create-only JSONL parts written by the executors plus a summary
(`served_gold_common`, "The v2 handoff").

Unlike the complex and admin jobs, nothing is held on the driver but the edits. Silver rows stay in
Spark: an edited PNU is removed from the snapshot with a broadcast anti-join and its upsert, if
any, is unioned back. No parcel edits exist yet (ADR-0133 §6), so this job only reads the ledger;
the edit store's `parcels` unit, and with it an edit handoff, belongs to the change that builds
parcel editing.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import served_gold_common as common
from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages
from platform_contracts import column_names, declared_geometry_srid, load_lakehouse_contract

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
PNU_PATTERN = re.compile(r"^[0-9]{19}$")
WKB_HEX_PATTERN = re.compile(r"^(?:[0-9a-f]{2})+$")
CHECKSUM_PATTERN = re.compile(r"^[0-9a-f]{64}$")
DEFAULT_HANDOFF_PARTS = 64


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--source-snapshot-id", required=True, help="The one silver.parcel_boundaries source_snapshot_id to serve."
    )
    parser.add_argument(
        "--output-dir", required=True, help="Directory for the handoff parts. Created here; never reused."
    )
    parser.add_argument("--summary-output", help="Path for the v2 run summary JSON.")
    parser.add_argument("--handoff-parts", type=int, default=DEFAULT_HANDOFF_PARTS)
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-namespace", default="silver")
    parser.add_argument("--iceberg-table", default="parcel_boundaries")
    parser.add_argument("--ledger-iceberg-namespace", default="silver")
    parser.add_argument("--ledger-iceberg-table", default="map_edit_ledger")
    parser.add_argument("--served-iceberg-namespace", default="gold")
    parser.add_argument("--served-iceberg-table", default="parcel_boundary_served")
    parser.add_argument(
        "--allow-non-smoke-write",
        action="store_true",
        help="Required to write a served table whose name does not end in _smoke.",
    )
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    for label in (
        "iceberg_catalog_name",
        "iceberg_namespace",
        "iceberg_table",
        "ledger_iceberg_namespace",
        "ledger_iceberg_table",
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


def check_ledger(ledger: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The unit's ledgered edits in change order, refusing any the served table could not hold."""

    seen: set[int] = set()
    for edit in ledger:
        seq = int(edit["change_seq"])
        if seq <= 0 or seq in seen:
            raise ValueError(f"the parcels ledger repeats or lacks change_seq {seq}")
        seen.add(seq)
        if int(edit["geometry_srid"]) != GEOMETRY_SRID:
            raise ValueError(f"edit {seq} is EPSG:{edit['geometry_srid']}, the parcels are EPSG:{GEOMETRY_SRID}")
        if not PNU_PATTERN.match(str(edit["feature_id"])):
            raise ValueError(f"edit {seq} names {edit['feature_id']!r}, which is not a PNU")
        if edit["op"] == "upsert":
            if not WKB_HEX_PATTERN.match(edit.get("geometry_wkb_hex") or ""):
                raise ValueError(f"edit {seq} upserts a parcel without its geometry")
            if not CHECKSUM_PATTERN.match(edit.get("geometry_checksum_sha256") or ""):
                raise ValueError(f"edit {seq} upserts a parcel without its geometry checksum")
        elif edit["op"] != "delete":
            raise ValueError(f"edit {seq} has op {edit['op']}")
    return sorted(ledger, key=lambda item: int(item["change_seq"]))


def fold_edits(
    ledger: list[dict[str, Any]], present: set[str]
) -> tuple[dict[str, dict[str, Any] | None], dict[str, int]]:
    """Each edited PNU's final state (its last upsert, or None when deleted) and what the edits did.

    `present` is which edited PNUs the Silver snapshot holds. A delete of a parcel that is not
    there at that point in the order is counted, never passed over in silence.
    """

    final: dict[str, dict[str, Any] | None] = {}
    counts = {"upserts": 0, "deletes": 0, "deletes_of_absent_features": 0}
    for edit in check_ledger(ledger):
        feature = str(edit["feature_id"])
        exists = final[feature] is not None if feature in final else feature in present
        if edit["op"] == "delete":
            counts["deletes"] += 1
            if not exists:
                counts["deletes_of_absent_features"] += 1
            final[feature] = None
        else:
            counts["upserts"] += 1
            final[feature] = edit
    return final, counts


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


def build_served_frame(
    base: Any, base_rows: int, ledger: list[dict[str, Any]]
) -> tuple[Any, dict[str, int], int]:
    """Silver minus every edited PNU plus each edited PNU's final upsert, and the count it must have.

    `base` carries `pnu`, `geometry_wkb`, `geometry_srid`, `geometry_checksum_sha256` and
    `source_snapshot_id`. The edited PNUs travel to the executors as a broadcast; the snapshot never
    comes to the driver. The returned count is `base_rows - removed + upserted`, which the caller
    checks the written table against.
    """

    from pyspark.sql import functions as F  # noqa: PLC0415

    spark = base.sparkSession
    touched = sorted({str(edit["feature_id"]) for edit in ledger})
    present: set[str] = set()
    if touched:
        touched_frame = F.broadcast(spark.createDataFrame([(pnu,) for pnu in touched], "pnu string"))
        present = {row["pnu"] for row in base.join(touched_frame, "pnu", "left_semi").select("pnu").collect()}
        base = base.join(touched_frame, "pnu", "left_anti")
    final, counts = fold_edits(ledger, present)
    kept = base.select(
        "pnu",
        "geometry_wkb",
        F.lit(GEOMETRY_SRID).cast("int").alias("geometry_srid"),
        "geometry_checksum_sha256",
        F.lit("source").alias("origin"),
        F.col("source_snapshot_id").cast("string").alias("source_snapshot_id"),
    )
    upserts = [
        (
            pnu,
            bytes.fromhex(edit["geometry_wkb_hex"]),
            GEOMETRY_SRID,
            edit["geometry_checksum_sha256"],
            "edit",
            f"map-edit-{int(edit['change_seq'])}",
        )
        for pnu, edit in sorted(final.items())
        if edit is not None
    ]
    edited = spark.createDataFrame(
        upserts,
        "pnu string, geometry_wkb binary, geometry_srid int, geometry_checksum_sha256 string, "
        "origin string, source_snapshot_id string",
    )
    expected = base_rows - len(present) + len(upserts)
    return kept.unionByName(edited), counts, expected


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


def read_ledger(spark: Any, ledger_table: str) -> list[dict[str, Any]]:
    """Every ledgered `parcels` edit; the ledger is small, the snapshot is not."""

    return [
        row.asDict()
        for row in spark.sql(
            f"SELECT change_seq, feature_id, op, geometry_srid, lower(hex(geometry_wkb)) AS geometry_wkb_hex, "
            f"geometry_checksum_sha256 FROM {ledger_table} WHERE unit = '{UNIT}'"
        ).collect()
    ]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_args(args)
    output_dir = Path(args.output_dir)
    if output_dir.exists():
        raise FileExistsError(f"served-boundary bake handoff already exists and is evidence: {output_dir}")
    now = datetime.now(timezone.utc).replace(microsecond=0)

    spark = build_spark_session(args, load_pyspark())
    try:
        from pyspark.sql import functions as F  # noqa: PLC0415

        catalog = args.iceberg_catalog_name
        ledger_table = common.ensure_table(
            spark,
            qualified(catalog, args.ledger_iceberg_namespace, args.ledger_iceberg_table),
            catalog, args.ledger_iceberg_namespace, common.LEDGER_CONTRACT,
        )
        ledger = check_ledger(read_ledger(spark, ledger_table))
        through = max((int(row["change_seq"]) for row in ledger), default=0)

        silver_table = qualified(catalog, args.iceberg_namespace, args.iceberg_table)
        silver_snapshot = common.latest_snapshot(spark, silver_table)
        # Pinned to one Iceberg snapshot, so the checks and the write read the same rows.
        base = spark.sql(
            f"SELECT pnu, geometry_wkb, geometry_srid, geometry_checksum_sha256, source_snapshot_id "
            f"FROM {silver_table} VERSION AS OF {silver_snapshot} "
            f"WHERE source_snapshot_id = '{args.source_snapshot_id}'"
        )
        base_rows = check_silver(base, args.source_snapshot_id)
        served, counts, expected = build_served_frame(base, base_rows, ledger)

        served_table = common.ensure_table(
            spark,
            qualified(catalog, args.served_iceberg_namespace, args.served_iceberg_table),
            catalog, args.served_iceberg_namespace, SERVED_CONTRACT,
        )
        served.select(
            *[F.col(name) for name in ("pnu", "geometry_wkb", "geometry_srid", "geometry_checksum_sha256",
                                       "origin", "source_snapshot_id")],
            F.lit(silver_snapshot).alias("silver_iceberg_snapshot_id"),
            F.lit(through).cast("long").alias("edits_through_change_seq"),
            F.lit(now).cast("timestamp").alias("published_at_utc"),
        ).select(*SERVED_COLUMNS).createOrReplaceTempView("served_candidate")
        spark.sql(f"INSERT OVERWRITE {served_table} SELECT {', '.join(SERVED_COLUMNS)} FROM served_candidate")
        gold_snapshot = common.latest_snapshot(spark, served_table)
        written = spark.sql(f"SELECT pnu, geometry_wkb, origin FROM {served_table} VERSION AS OF {gold_snapshot}")
        persisted = written.count()
        if persisted != expected:
            raise ValueError(
                f"served table read back {persisted} rows; Silver {base_rows} minus the edited parcels "
                f"plus the upserts is {expected}"
            )
        # The bake reads exactly the committed Gold snapshot, not a recomputation of it.
        parts = common.write_handoff_parts(
            written, output_dir, id_column=FEATURE_ID_PROPERTY, property_columns=TILE_PROPERTIES,
            srid=GEOMETRY_SRID, parts=args.handoff_parts,
        )
    finally:
        spark.stop()

    common.verify_handoff_parts(output_dir, parts, persisted)
    summary = common.summary_v2(
        source_snapshot_id=args.source_snapshot_id, parts=parts,
        job=JOB_NAME, unit=UNIT, feature_id_property=FEATURE_ID_PROPERTY, srid=GEOMETRY_SRID,
        gold_snapshot=gold_snapshot, through=through, handoff=0, appended=0, ledger=len(ledger),
        served=persisted, base=base_rows, output=output_dir,
        generated_at=now.isoformat().replace("+00:00", "Z"), counts=counts,
        extra={"silver_iceberg_snapshot_id": silver_snapshot},
    )
    if args.summary_output:
        path = Path(args.summary_output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("parcel-boundary-served-gold-summary-json " + json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
