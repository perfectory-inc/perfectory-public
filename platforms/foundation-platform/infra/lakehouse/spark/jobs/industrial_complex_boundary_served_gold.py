#!/usr/bin/env python3
"""Build `gold.industrial_complex_boundary_served`: the complex boundaries exactly as they are tiled.

Root ADR-0112 §7·§9. Admin edits live in the small edit store until a bake folds them. Folding
starts here:

1. the edits the store still holds arrive as the handoff `export-map-edit-handoff` wrote (each
   polygon already reprojected into the Silver CRS, because this image has no projection library —
   root ADR-0042);
2. they are appended to `silver.map_edit_ledger`, once each: an edit already in the ledger with the
   same content is skipped, one with different content under the same `change_seq` is refused;
3. the served table is rewritten as the current official Silver boundaries with **every** ledgered
   edit of the unit applied in `change_seq` order. Every edit, not only the new ones: after a fold
   the store forgets an edit, so the ledger is the only place it still exists;
4. the committed Gold snapshot, the Silver snapshot it read and the last edit it includes are
   written to the summary, with a create-only JSONL of the served rows for the tile bake.

The Gold snapshot id is what the catalog records as the release's canonical snapshot, so a bake is
always traceable to one lakehouse state.
"""

from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import served_gold_common as common
from industrial_complex_boundaries_silver_to_postgis_handoff import (
    current_official_boundaries,
    project,
    qualified_table,
    read_sources,
    validate_identifier,
)
from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages
from platform_contracts import column_names, declared_geometry_srid, load_lakehouse_contract

JOB_NAME = "industrial_complex_boundary_served_gold"
UNIT = "complex"
FEATURE_ID_PROPERTY = "complex_id"
CODE_PROPERTY = "official_complex_code"
# The tile properties besides the id; the bake carries exactly these.
TILE_PROPERTIES: tuple[str, ...] = (CODE_PROPERTY,)

LEDGER_CONTRACT = common.LEDGER_CONTRACT
LEDGER_COLUMNS = common.LEDGER_COLUMNS
SERVED_CONTRACT = load_lakehouse_contract("gold.industrial_complex_boundary_served")
SERVED_COLUMNS: tuple[str, ...] = column_names(SERVED_CONTRACT)
GEOMETRY_SRID: int = declared_geometry_srid(SERVED_CONTRACT)
DEFAULT_MAX_ROWS = 100_000
contract_schema = common.contract_schema
edit_fingerprint = common.edit_fingerprint
new_ledger_rows = common.new_ledger_rows
write_create_only = common.write_create_only
latest_snapshot = common.latest_snapshot


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--edits-input", required=True, help="JSONL from export-map-edit-handoff.")
    parser.add_argument("--output", required=True, help="Served-row JSONL for the tile bake. Never overwritten.")
    parser.add_argument("--summary-output", help="Path for the run summary JSON.")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-namespace", default="silver")
    parser.add_argument("--iceberg-table", default="industrial_complex_boundaries")
    parser.add_argument("--complexes-iceberg-namespace", default="silver")
    parser.add_argument("--complexes-iceberg-table", default="industrial_complexes")
    parser.add_argument("--ledger-iceberg-namespace", default="silver")
    parser.add_argument("--ledger-iceberg-table", default="map_edit_ledger")
    parser.add_argument("--served-iceberg-namespace", default="gold")
    parser.add_argument("--served-iceberg-table", default="industrial_complex_boundary_served")
    parser.add_argument(
        "--allow-non-smoke-write",
        action="store_true",
        help="Required to write tables whose names do not end in _smoke.",
    )
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    parser.add_argument("--max-rows", type=int, default=DEFAULT_MAX_ROWS)
    args = parser.parse_args(argv)
    # `read_sources` is shared with the Silver-to-PostGIS handoff, which also reads JSONL.
    args.input_mode = "iceberg"
    return args


def validate_args(args: argparse.Namespace) -> None:
    for label in (
        "iceberg_catalog_name",
        "iceberg_namespace",
        "iceberg_table",
        "complexes_iceberg_namespace",
        "complexes_iceberg_table",
        "ledger_iceberg_namespace",
        "ledger_iceberg_table",
        "served_iceberg_namespace",
        "served_iceberg_table",
    ):
        validate_identifier(label.replace("_", " "), getattr(args, label))
    for table in (args.ledger_iceberg_table, args.served_iceberg_table):
        if not table.endswith("_smoke") and not args.allow_non_smoke_write:
            raise ValueError(
                f"writing {table} needs --allow-non-smoke-write; only *_smoke tables are written by default"
            )
    if args.max_rows <= 0:
        raise ValueError("--max-rows must be positive")
    assert_catalog_env()


def read_edit_handoff(lines: list[str]) -> list[dict[str, Any]]:
    """Parses the complex edit handoff; an upsert must name the complex's official code."""

    return common.read_edit_handoff(lines, UNIT, GEOMETRY_SRID, (CODE_PROPERTY,))


def apply_edits(
    base: list[dict[str, Any]], ledger: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Applies every ledgered edit, in change order, over the current Silver boundaries."""

    served = {row["complex_id"]: dict(row, origin="source") for row in base}
    counts = {"upserts": 0, "deletes": 0, "deletes_of_absent_features": 0}
    for edit in sorted(ledger, key=lambda item: int(item["change_seq"])):
        feature = edit["feature_id"]
        if edit["op"] == "delete":
            counts["deletes"] += 1
            if served.pop(feature, None) is None:
                counts["deletes_of_absent_features"] += 1
            continue
        counts["upserts"] += 1
        properties = json.loads(edit["properties_json"])
        served[feature] = {
            "complex_id": feature,
            "official_complex_code": str(properties[CODE_PROPERTY]),
            "geometry_wkb_hex": edit["geometry_wkb_hex"],
            "geometry_srid": GEOMETRY_SRID,
            "geometry_checksum_sha256": edit["geometry_checksum_sha256"],
            "source_snapshot_id": f"map-edit-{int(edit['change_seq'])}",
            "origin": "edit",
        }
    codes: dict[str, str] = {}
    for row in served.values():
        code = row["official_complex_code"]
        if code in codes:
            raise ValueError(
                f"official_complex_code {code} would be served by both {codes[code]} and "
                f"{row['complex_id']}; an edit gave two complexes one code"
            )
        codes[code] = row["complex_id"]
    return sorted(served.values(), key=lambda row: row["complex_id"]), counts


def bake_handoff_lines(rows: list[dict[str, Any]]) -> str:
    return common.bake_handoff_lines(rows, FEATURE_ID_PROPERTY, TILE_PROPERTIES, GEOMETRY_SRID)


def load_pyspark() -> tuple[Any, Any]:
    from pyspark.sql import SparkSession  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415

    return SparkSession, F


def build_spark_session(args: argparse.Namespace, SparkSession: Any) -> Any:
    builder = (
        SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
    )
    builder = apply_catalog_settings(builder, args.iceberg_catalog_name)
    return builder.getOrCreate()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_args(args)
    output = Path(args.output)
    if output.exists():
        raise FileExistsError(f"served-boundary bake handoff already exists and is evidence: {output}")
    handoff = read_edit_handoff(Path(args.edits_input).read_text(encoding="utf-8").splitlines())
    batch_id = f"map-edit-export-{uuid.uuid4()}"
    now = datetime.now(timezone.utc).replace(microsecond=0)

    SparkSession, F = load_pyspark()
    spark = build_spark_session(args, SparkSession)
    try:
        ledger_table = common.ensure_table(
            spark,
            qualified_table(args.iceberg_catalog_name, args.ledger_iceberg_namespace, args.ledger_iceberg_table),
            args.iceberg_catalog_name, args.ledger_iceberg_namespace, LEDGER_CONTRACT,
        )
        appended, ledger = common.append_to_ledger(spark, ledger_table, UNIT, handoff, batch_id, now)

        boundaries, complexes, silver_snapshot = read_sources(spark, args)
        selected = current_official_boundaries(boundaries, F)
        base = [row.asDict() for row in project(selected, complexes, F).collect()]
        if len(base) != selected.count():
            raise ValueError("a current official boundary names a complex the Silver complexes table lacks")
        served, counts = apply_edits(base, ledger)
        if len(served) > args.max_rows:
            raise ValueError(f"{len(served)} served rows exceed --max-rows {args.max_rows}")
        through = max((int(row["change_seq"]) for row in ledger), default=0)

        served_table = common.ensure_table(
            spark,
            qualified_table(args.iceberg_catalog_name, args.served_iceberg_namespace, args.served_iceberg_table),
            args.iceberg_catalog_name, args.served_iceberg_namespace, SERVED_CONTRACT,
        )
        spark.createDataFrame(
            [
                {
                    "complex_id": row["complex_id"],
                    "official_complex_code": row["official_complex_code"],
                    "geometry_wkb": bytes.fromhex(row["geometry_wkb_hex"]),
                    "geometry_srid": GEOMETRY_SRID,
                    "geometry_checksum_sha256": row["geometry_checksum_sha256"],
                    "origin": row["origin"],
                    "source_snapshot_id": str(row["source_snapshot_id"]),
                    "silver_iceberg_snapshot_id": silver_snapshot,
                    "edits_through_change_seq": through,
                    "published_at_utc": now,
                }
                for row in served
            ],
            schema=contract_schema(SERVED_CONTRACT),
        ).select(*SERVED_COLUMNS).createOrReplaceTempView("served_candidate")
        spark.sql(f"INSERT OVERWRITE {served_table} SELECT {', '.join(SERVED_COLUMNS)} FROM served_candidate")
        persisted = spark.sql(f"SELECT count(*) AS n FROM {served_table}").collect()[0]["n"]
        if persisted != len(served):
            raise ValueError(f"served table read back {persisted} rows, wrote {len(served)}")
        gold_snapshot = latest_snapshot(spark, served_table)
    finally:
        spark.stop()

    write_create_only(output, bake_handoff_lines(served))
    summary = common.summary(
        job=JOB_NAME, unit=UNIT, feature_id_property=FEATURE_ID_PROPERTY, srid=GEOMETRY_SRID,
        gold_snapshot=gold_snapshot, through=through, handoff=len(handoff), appended=appended,
        ledger=len(ledger), served=len(served), base=len(base), output=output,
        generated_at=now.isoformat().replace("+00:00", "Z"), counts=counts,
        extra={"silver_iceberg_snapshot_id": silver_snapshot},
    )
    if args.summary_output:
        path = Path(args.summary_output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("industrial-complex-boundary-served-gold-summary-json " + json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
