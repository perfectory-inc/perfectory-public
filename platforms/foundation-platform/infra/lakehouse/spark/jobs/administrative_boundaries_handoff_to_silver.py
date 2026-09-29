#!/usr/bin/env python3
"""Load the merged legal-dong boundary GeoJSON into `silver.administrative_boundaries`.

Root ADR-0112: the administrative boundary layer bakes from the lakehouse, so its geometry and its
identity live here first. The id belongs to the place, not to its code (root ADR-0113 §9, ADR-0103
§1): a legal dong keeps the id it had before, and a dong that was renumbered takes the id of the
dong it came from. Only a dong with no predecessor gets a new id, derived from its first code:

    administrative_unit_id = uuid5(NAMESPACE_URL, "scope:legal-dong:<first 10-digit code of the chain>")

Which new code came from which old code is the parcel lineage's dong pairing
(`parcel_lineage.pair_legal_dongs`), handed in as `--predecessor-map`.

The input is what `scripts/tiles/admin-boundary/convert.sh` + `merge.py` produce: EPSG:4326,
`-makevalid`, properties EMD_CD (8 digits), EMD_NM, SIGUNGU_CD (5 digits), SIGUNGU_NM. Each run
appends one whole source snapshot; a snapshot already present is refused, never rewritten.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import struct
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages
from platform_contracts import (
    column_names,
    create_table_columns_sql,
    declared_geometry_srid,
    evolve_iceberg_table_to_contract,
    load_lakehouse_contract,
    partition_clause_sql,
    spark_sql_type,
)

JOB_NAME = "administrative_boundaries_handoff_to_silver"
SUMMARY_SCHEMA_VERSION = "foundation-platform.administrative_boundaries_silver.v1"
CONTRACT = load_lakehouse_contract("silver.administrative_boundaries")
COLUMNS: tuple[str, ...] = column_names(CONTRACT)
GEOMETRY_SRID: int = declared_geometry_srid(CONTRACT)
SCOPE_KIND = "legal_dong"
SEED_PREFIX = "scope:legal-dong:"
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SNAPSHOT_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,200}$")
# The collected source is about 5,100 legal dongs; two orders of magnitude more is not a bigger
# source, it is a different one.
MAX_ROWS = 100_000


def administrative_unit_id(canonical_code: str) -> str:
    """The lakehouse id of a legal dong: RFC 4122 v5 of its stable key under NAMESPACE_URL."""

    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{SEED_PREFIX}{canonical_code}"))


def _ring(positions: Any) -> bytes:
    if not isinstance(positions, list) or len(positions) < 4:
        raise ValueError("a ring needs at least four positions")
    out = [struct.pack("<I", len(positions))]
    for position in positions:
        if not isinstance(position, list) or len(position) < 2:
            raise ValueError("a position needs two coordinates")
        x, y = float(position[0]), float(position[1])
        out.append(struct.pack("<dd", x, y))
    if positions[0][:2] != positions[-1][:2]:
        raise ValueError("a ring must be closed")
    return b"".join(out)


def _polygon(rings: Any) -> bytes:
    if not isinstance(rings, list) or not rings:
        raise ValueError("a polygon needs a ring")
    return b"".join([struct.pack("<BII", 1, 3, len(rings)), *(_ring(ring) for ring in rings)])


def multipolygon_wkb(geometry: dict[str, Any]) -> bytes:
    """Little-endian WKB MultiPolygon (2D) of a GeoJSON Polygon or MultiPolygon."""

    kind = geometry.get("type")
    coordinates = geometry.get("coordinates")
    if kind == "Polygon":
        polygons = [coordinates]
    elif kind == "MultiPolygon" and isinstance(coordinates, list) and coordinates:
        polygons = coordinates
    else:
        raise ValueError(f"geometry must be a Polygon or MultiPolygon, got {kind}")
    return b"".join([struct.pack("<BII", 1, 6, len(polygons)), *(_polygon(p) for p in polygons)])


def resolve_unit_ids(
    codes: list[str], predecessor_of: dict[str, str], previous_ids: dict[str, str]
) -> tuple[dict[str, str], dict[str, int]]:
    """The id each code carries in this snapshot (ADR-0113 §9).

    Kept when the code existed before; inherited from its predecessor when it was renumbered;
    otherwise the v5 of its first code (a first snapshot's predecessor code is that code). An id is
    never given to two codes: if two codes would inherit the same id, only the one that is the
    predecessor's own continuation — the same code, else the first in order — takes it and the other
    starts its own.
    """

    ids: dict[str, str] = {}
    taken: set[str] = set()
    counts = {"kept": 0, "inherited": 0, "new": 0, "collisions": 0}
    ordered = sorted(codes, key=lambda c: (c not in previous_ids, c))
    for code in ordered:
        if code in previous_ids:
            candidate, how = previous_ids[code], "kept"
        elif code in predecessor_of:
            old = predecessor_of[code]
            candidate, how = previous_ids.get(old, administrative_unit_id(old)), "inherited"
        else:
            candidate, how = administrative_unit_id(code), "new"
        if candidate in taken:
            candidate, how = administrative_unit_id(code), "new"
            counts["collisions"] += 1
            if candidate in taken:
                raise ValueError(f"no free id for legal dong {code}")
        ids[code] = candidate
        taken.add(candidate)
        counts[how] += 1
    return ids, counts


def silver_rows(
    features: list[dict[str, Any]],
    source_snapshot_id: str,
    source_record_id: str,
    ingested_at: datetime,
    unit_ids: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """One Silver row per legal dong, refusing anything the contract would refuse."""

    rows: dict[str, dict[str, Any]] = {}
    for number, feature in enumerate(features, start=1):
        properties = feature.get("properties") or {}
        emd_cd = str(properties.get("EMD_CD") or "").strip()
        emd_nm = str(properties.get("EMD_NM") or "").strip()
        sgg_cd = str(properties.get("SIGUNGU_CD") or "").strip()
        sgg_nm = str(properties.get("SIGUNGU_NM") or "").strip()
        if not (len(emd_cd) == 8 and emd_cd.isdigit() and len(sgg_cd) == 5 and sgg_cd.isdigit()):
            raise ValueError(f"feature {number} has no 8-digit EMD_CD and 5-digit SIGUNGU_CD")
        if not emd_nm or not sgg_nm:
            raise ValueError(f"feature {number} ({emd_cd}) has no name")
        # The parent is the self-governing sigungu (`COL_ADM_SE`). A legal dong in a city's
        # non-autonomous gu (e.g. `xxxx3` under city `xxxx0`) names the city, not its gu.
        own_sigungu = emd_cd[:5]
        city = f"{emd_cd[:4]}0"
        if sgg_cd not in (own_sigungu, city):
            raise ValueError(f"legal dong {emd_cd} does not belong to sigungu {sgg_cd}")
        canonical_code = f"{emd_cd}00"
        if canonical_code in rows:
            raise ValueError(f"legal dong {canonical_code} appears twice in one source snapshot")
        wkb = multipolygon_wkb(feature.get("geometry") or {})
        rows[canonical_code] = {
            "administrative_unit_id": (unit_ids or {}).get(canonical_code) or administrative_unit_id(canonical_code),
            "scope_kind": SCOPE_KIND,
            "canonical_code": canonical_code,
            "display_name": emd_nm,
            "parent_canonical_code": sgg_cd,
            "parent_display_name": sgg_nm,
            "geometry_wkb": wkb,
            "geometry_srid": GEOMETRY_SRID,
            "geometry_checksum_sha256": hashlib.sha256(wkb).hexdigest(),
            "source_record_id": source_record_id,
            "source_snapshot_id": source_snapshot_id,
            "ingested_at_utc": ingested_at,
        }
    if not rows:
        raise ValueError("the source holds no legal dong")
    if len(rows) > MAX_ROWS:
        raise ValueError(f"{len(rows)} legal dongs exceed the {MAX_ROWS} bound")
    return [rows[code] for code in sorted(rows)]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--input", required=True, help="Merged GeoJSON from merge.py (EPSG:4326).")
    parser.add_argument("--source-snapshot-id", required=True, help="e.g. vworldkr__boundary_emd-30603-202606")
    parser.add_argument("--source-record-id", required=True, help="The Bronze object key(s) this snapshot came from.")
    parser.add_argument("--predecessor-map", help="JSON {new 10-digit code: old 10-digit code} from the dong pairing")
    parser.add_argument("--summary-output")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-namespace", default="silver")
    parser.add_argument("--iceberg-table", default="administrative_boundaries")
    parser.add_argument("--allow-non-smoke-write", action="store_true")
    parser.add_argument("--validate-only", action="store_true", help="Check the input and stop before Spark.")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    for label in ("iceberg_catalog_name", "iceberg_namespace", "iceberg_table"):
        if not IDENTIFIER_PATTERN.fullmatch(getattr(args, label)):
            raise ValueError(f"{label} must be a plain SQL identifier")
    if not SNAPSHOT_PATTERN.fullmatch(args.source_snapshot_id):
        raise ValueError("--source-snapshot-id has characters a snapshot id does not use")
    if not args.source_record_id.strip():
        raise ValueError("--source-record-id is required")
    if args.validate_only:
        return
    if not args.iceberg_table.endswith("_smoke") and not args.allow_non_smoke_write:
        raise ValueError(f"writing {args.iceberg_table} needs --allow-non-smoke-write")
    assert_catalog_env()


def load_pyspark() -> Any:
    from pyspark.sql import SparkSession  # noqa: PLC0415

    return SparkSession


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_args(args)
    document = json.loads(Path(args.input).read_text(encoding="utf-8"))
    ingested_at = datetime.now(timezone.utc).replace(microsecond=0)
    predecessor_of = json.loads(Path(args.predecessor_map).read_text(encoding="utf-8")) if args.predecessor_map else {}
    features = document.get("features") or []
    codes = [f"{str((f.get('properties') or {}).get('EMD_CD') or '').strip()}00" for f in features]
    # Before Spark the previous snapshot is unknown; the ids computed here are what a first
    # snapshot would get, and `main` recomputes them against the table before writing.
    unit_ids, id_counts = resolve_unit_ids(codes, predecessor_of, {})
    rows = silver_rows(features, args.source_snapshot_id, args.source_record_id, ingested_at, unit_ids)
    summary: dict[str, Any] = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "job": JOB_NAME,
        "contract": CONTRACT["table_name"],
        "source_snapshot_id": args.source_snapshot_id,
        "row_count": len(rows),
        "geometry_srid": GEOMETRY_SRID,
        "status": "validated" if args.validate_only else "ready",
        "ids": id_counts,
    }
    if not args.validate_only:
        SparkSession = load_pyspark()
        builder = (
            SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
        )
        spark = apply_catalog_settings(builder, args.iceberg_catalog_name).config(
            "spark.jars.packages", args.iceberg_packages
        ).getOrCreate()
        try:
            table = f"`{args.iceberg_catalog_name}`.`{args.iceberg_namespace}`.`{args.iceberg_table}`"
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{args.iceberg_catalog_name}`.`{args.iceberg_namespace}`")
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
            present = spark.sql(
                f"SELECT count(*) AS n FROM {table} WHERE source_snapshot_id = '{args.source_snapshot_id}'"
            ).collect()[0]["n"]
            if present:
                raise ValueError(f"{args.source_snapshot_id} is already in {table}; the table is append-only")
            previous = spark.sql(
                f"SELECT source_snapshot_id FROM {table} GROUP BY source_snapshot_id "
                f"ORDER BY max(ingested_at_utc) DESC, source_snapshot_id DESC LIMIT 1"
            ).collect()
            previous_ids = {}
            if previous:
                previous_ids = {
                    r["canonical_code"]: r["administrative_unit_id"]
                    for r in spark.sql(
                        f"SELECT canonical_code, administrative_unit_id FROM {table} "
                        f"WHERE source_snapshot_id = '{previous[0]['source_snapshot_id']}'"
                    ).collect()
                }
                summary["previous_snapshot_id"] = previous[0]["source_snapshot_id"]
            unit_ids, id_counts = resolve_unit_ids(codes, predecessor_of, previous_ids)
            rows = silver_rows(features, args.source_snapshot_id, args.source_record_id, ingested_at, unit_ids)
            summary["row_count"] = len(rows)
            schema = ", ".join(f"{c['name']} {spark_sql_type(c['logical_type'])}" for c in CONTRACT["columns"])
            spark.createDataFrame(rows, schema=schema).select(*COLUMNS).createOrReplaceTempView("admin_candidate")
            spark.sql(f"INSERT INTO {table} SELECT {', '.join(COLUMNS)} FROM admin_candidate")
            written = spark.sql(
                f"SELECT count(*) AS n FROM {table} WHERE source_snapshot_id = '{args.source_snapshot_id}'"
            ).collect()[0]["n"]
            if written != len(rows):
                raise ValueError(f"{table} read back {written} rows for the snapshot, wrote {len(rows)}")
            summary["ids"] = id_counts
            summary["canonical_iceberg_snapshot_id"] = str(
                spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1")
                .collect()[0]["snapshot_id"]
            )
        finally:
            spark.stop()
    if args.summary_output:
        path = Path(args.summary_output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("administrative-boundaries-silver-summary-json " + json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
