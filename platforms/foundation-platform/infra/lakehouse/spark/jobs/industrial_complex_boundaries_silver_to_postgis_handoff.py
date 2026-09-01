#!/usr/bin/env python3
"""Export `silver.industrial_complex_boundaries` as the JSONL the PostGIS publisher reads.

`publish-industrial-complex-boundary-postgis` takes a file, like every other publisher in this
repository. This job is the only thing that can produce it:

* `complex_id` is filled by the Silver *writer* from its join against `silver.industrial_complexes`,
  so the Bronze-to-Silver boundary handoff carries a JSON null there and nothing downstream of the
  handoff can supply it without re-deriving an id whose only definition lives in
  `industrial_complex_bronze_to_silver.py`;
* the Rust Iceberg scan (`lakehouse_snapshot_scan`) decodes string, long, date, timestamp and
  decimal columns only — `geometry_wkb` is `binary` and the bbox and centroid columns are `double`,
  so the publisher cannot read this table itself.

The export carries the source CRS untouched. Reprojection to EPSG:4326 is PostGIS's job at the
serving edge (root ADR-0042), and `geometry_srid` travels so the publisher refuses a file that
claims a different one rather than reprojecting from the wrong origin.

`official_complex_code` comes from `silver.industrial_complexes`, joined on `complex_id`. The
boundary table does not carry the code — `boundary_id` embeds it in a urn, and parsing an id back
into a field is how the two spellings drift apart.

The canonical Iceberg snapshot the export was read from is recorded in the summary, because it is
what the operator passes to the publisher and what the promotion gate compares a release against.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages
from platform_contracts import (
    column_names,
    declared_geometry_srid,
    load_lakehouse_contract,
)


DEFAULT_ICEBERG_PACKAGES = iceberg_packages()
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
JOB_NAME = "industrial_complex_boundaries_silver_to_postgis_handoff"
SUMMARY_SCHEMA_VERSION = "foundation-platform.industrial_complex_boundary_postgis_handoff.v1"

BOUNDARIES_CONTRACT_NAME = "silver.industrial_complex_boundaries"
COMPLEXES_CONTRACT_NAME = "silver.industrial_complexes"
BOUNDARIES_CONTRACT = load_lakehouse_contract(BOUNDARIES_CONTRACT_NAME)
COMPLEXES_CONTRACT = load_lakehouse_contract(COMPLEXES_CONTRACT_NAME)
BOUNDARY_COLUMNS: tuple[str, ...] = column_names(BOUNDARIES_CONTRACT)
COMPLEX_COLUMNS: tuple[str, ...] = column_names(COMPLEXES_CONTRACT)
GEOMETRY_SRID: int = declared_geometry_srid(BOUNDARIES_CONTRACT)

# The authority's own published boundary. The other three `boundary_kind` values
# (`docs/catalog/industrial-complex-lakehouse-poc.md` §4.2) have no producer, and
# `serving_postgis.industrial_complex_boundary_publication` admits only this one.
OFFICIAL_BOUNDARY_KIND = "official"

# Exactly the fields `industrial_complex_boundary_postgis_publish.rs` reads, in a stable order so
# two runs over one snapshot produce byte-identical files.
EXPORT_COLUMNS: tuple[str, ...] = (
    "complex_id",
    "official_complex_code",
    "boundary_kind",
    "geometry_wkb_hex",
    "geometry_srid",
    "area_sqm_calculated",
    "geometry_checksum_sha256",
    "source_record_id",
    "source_snapshot_id",
)

# What this job reads from each Silver table, and nothing else. Checked against the contracts at
# startup so a column that is renamed there fails here by name instead of as a Spark analysis error
# in the middle of a run.
BOUNDARY_SELECT_COLUMNS: tuple[str, ...] = (
    "boundary_id",
    "complex_id",
    "boundary_kind",
    "geometry_srid",
    "geometry_wkb",
    "area_sqm_calculated",
    "geometry_checksum_sha256",
    "source_record_id",
    "source_snapshot_id",
    "valid_to_utc",
)
COMPLEX_SELECT_COLUMNS: tuple[str, ...] = (
    "complex_id",
    "official_complex_code",
    "valid_to_utc",
)

# A publish is one file the driver writes whole, so the export is collected rather than written as
# Spark part files. The bound is a refusal, not a truncation: the source is 1,442 complexes and a
# run that suddenly has two orders of magnitude more rows is not a bigger export, it is a different
# table.
DEFAULT_MAX_ROWS = 100_000


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export silver.industrial_complex_boundaries as the JSONL "
            "publish-industrial-complex-boundary-postgis reads."
        )
    )
    parser.add_argument("--output", required=True, help="JSONL path to write. Never overwritten.")
    parser.add_argument("--summary-output", help="Path for the run summary JSON.")
    parser.add_argument(
        "--input-mode",
        choices=("iceberg", "jsonl"),
        default="iceberg",
        help="Read Silver from the Iceberg REST catalog, or from local JSONL for a dry run.",
    )
    parser.add_argument("--boundaries-input", help="Boundary JSONL path for --input-mode=jsonl.")
    parser.add_argument("--complexes-input", help="Complex JSONL path for --input-mode=jsonl.")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-namespace", default="silver")
    parser.add_argument("--iceberg-table", default="industrial_complex_boundaries")
    parser.add_argument("--complexes-iceberg-namespace", default="silver")
    parser.add_argument("--complexes-iceberg-table", default="industrial_complexes")
    parser.add_argument(
        "--iceberg-packages",
        default=os.environ.get("FOUNDATION_PLATFORM_SPARK_ICEBERG_PACKAGES", DEFAULT_ICEBERG_PACKAGES),
    )
    parser.add_argument("--max-rows", type=int, default=DEFAULT_MAX_ROWS)
    return parser.parse_args(argv)








def validate_identifier(label: str, value: str) -> None:
    if not IDENTIFIER_PATTERN.fullmatch(value or ""):
        raise ValueError(f"{label} must be a plain SQL identifier, got {value!r}")


def needs_iceberg_catalog(args: argparse.Namespace) -> bool:
    return args.input_mode == "iceberg"


def validate_args(args: argparse.Namespace) -> None:
    if args.max_rows <= 0:
        raise ValueError("--max-rows must be positive")
    if args.input_mode == "jsonl":
        if not args.boundaries_input or not args.complexes_input:
            raise ValueError("--boundaries-input and --complexes-input are required for --input-mode=jsonl")
        return
    validate_identifier("iceberg catalog name", args.iceberg_catalog_name)
    validate_identifier("iceberg namespace", args.iceberg_namespace)
    validate_identifier("iceberg table", args.iceberg_table)
    validate_identifier("complexes iceberg namespace", args.complexes_iceberg_namespace)
    validate_identifier("complexes iceberg table", args.complexes_iceberg_table)
    assert_catalog_env()


def qualified_table(catalog: str, namespace: str, table: str) -> str:
    return f"`{catalog}`.`{namespace}`.`{table}`"


def assert_contract_columns() -> None:
    """Refuses to run when a column this job selects is not in the contract it selects it from."""

    for contract_name, declared, selected in (
        (BOUNDARIES_CONTRACT_NAME, BOUNDARY_COLUMNS, BOUNDARY_SELECT_COLUMNS),
        (COMPLEXES_CONTRACT_NAME, COMPLEX_COLUMNS, COMPLEX_SELECT_COLUMNS),
    ):
        missing = [name for name in selected if name not in declared]
        if missing:
            raise ValueError(
                f"{contract_name} no longer declares {', '.join(missing)}; "
                f"{JOB_NAME} reads columns the contract does not have"
            )


def export_row(row: dict[str, Any]) -> dict[str, Any]:
    """Project one joined Silver row into the publisher's line, checking what it must not guess.

    Every value is carried, never derived. `geometry_srid` is an int because the publisher compares
    it to a number; `area_sqm_calculated` stays exact decimal text because `decimal(18,2)` does not
    survive a float; everything else is the string Silver stores.
    """

    missing = [name for name in EXPORT_COLUMNS if row.get(name) in (None, "")]
    if missing:
        raise ValueError(
            f"boundary {row.get('boundary_id') or row.get('complex_id')} has no "
            f"{', '.join(missing)}; the projection cannot invent it"
        )
    if row["boundary_kind"] != OFFICIAL_BOUNDARY_KIND:
        raise ValueError(
            f"boundary {row['boundary_id']} is a {row['boundary_kind']} boundary; only "
            f"{OFFICIAL_BOUNDARY_KIND} boundaries have a serving projection"
        )
    if int(row["geometry_srid"]) != GEOMETRY_SRID:
        raise ValueError(
            f"boundary {row['boundary_id']} declares EPSG:{row['geometry_srid']} rather than the "
            f"EPSG:{GEOMETRY_SRID} the contract states"
        )
    return {
        "complex_id": str(row["complex_id"]),
        "official_complex_code": str(row["official_complex_code"]),
        "boundary_kind": str(row["boundary_kind"]),
        "geometry_wkb_hex": str(row["geometry_wkb_hex"]),
        "geometry_srid": int(row["geometry_srid"]),
        "area_sqm_calculated": str(row["area_sqm_calculated"]),
        "geometry_checksum_sha256": str(row["geometry_checksum_sha256"]),
        "source_record_id": str(row["source_record_id"]),
        "source_snapshot_id": str(row["source_snapshot_id"]),
    }


def export_lines(rows: list[dict[str, Any]]) -> str:
    """Renders the export, in `official_complex_code` order and refusing a repeated complex.

    The publisher refuses a repeat too, and the load's primary key refuses it a third time. This is
    the copy that can name the row, because it is the one holding `boundary_id`.
    """

    seen: dict[str, str] = {}
    projected = []
    for row in sorted(rows, key=lambda item: str(item.get("official_complex_code") or "")):
        exported = export_row(row)
        code = exported["official_complex_code"]
        if code in seen:
            raise ValueError(
                f"official_complex_code {code} has two active {OFFICIAL_BOUNDARY_KIND} boundaries "
                f"({seen[code]} and {row.get('boundary_id')}); the contract allows at most one"
            )
        seen[code] = str(row.get("boundary_id"))
        projected.append(exported)
    if not projected:
        raise ValueError("the Silver boundary snapshot produced no exportable rows")
    body = "\n".join(
        json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        for row in projected
    )
    return f"{body}\n"


def build_summary(
    args: argparse.Namespace,
    canonical_iceberg_snapshot_id: str | None,
    row_count: int,
    source_snapshot_ids: list[str],
    generated_at_utc: str,
) -> dict[str, Any]:
    """The summary is the publisher's input contract, written down.

    Every value the operator has to pass to `publish-industrial-complex-boundary-postgis` is here,
    read off the data rather than remembered: the canonical snapshot, the source snapshot, and the
    Bronze object key Silver cites.
    """

    limitations = ["does_not_write_postgis", "does_not_promote_a_runtime_manifest"]
    if canonical_iceberg_snapshot_id is None:
        limitations.append("canonical_iceberg_snapshot_id_is_unknown_outside_iceberg_input_mode")
    if len(source_snapshot_ids) > 1:
        limitations.append("the_snapshot_mixes_several_source_snapshots")
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "job": JOB_NAME,
        "generated_at_utc": generated_at_utc,
        "status": "ready",
        "completion_claim_allowed": False,
        "production_cutover_allowed": False,
        "contract": BOUNDARIES_CONTRACT_NAME,
        "input_mode": args.input_mode,
        "canonical_iceberg_snapshot_id": canonical_iceberg_snapshot_id,
        "source_snapshot_ids": source_snapshot_ids,
        "geometry_srid": GEOMETRY_SRID,
        "boundary_kind": OFFICIAL_BOUNDARY_KIND,
        "row_count": row_count,
        "output_path": str(args.output),
        "evidence_limitations": limitations,
    }


def write_create_only(path: Path, body: str) -> None:
    """Writes the export, refusing to replace one.

    An export is the evidence of what a projection load was built from; a rerun that truncates the
    previous file destroys that record. Same rule as the Bronze-to-Silver boundary handoff.
    """

    if path.exists():
        raise FileExistsError(
            f"industrial-complex boundary PostGIS handoff already exists and is append-only "
            f"evidence: {path}"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


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
    if needs_iceberg_catalog(args):
        builder = apply_catalog_settings(builder, args.iceberg_catalog_name)
        builder = builder.config("spark.jars.packages", args.iceberg_packages)
    return builder.getOrCreate()


def read_sources(spark: Any, args: argparse.Namespace) -> tuple[Any, Any, str | None]:
    """Returns the boundary frame, the complex frame, and the snapshot the boundaries came from."""

    if args.input_mode == "jsonl":
        return (
            spark.read.json(args.boundaries_input),
            spark.read.json(args.complexes_input),
            None,
        )
    boundaries_table = qualified_table(
        args.iceberg_catalog_name, args.iceberg_namespace, args.iceberg_table
    )
    complexes_table = qualified_table(
        args.iceberg_catalog_name, args.complexes_iceberg_namespace, args.complexes_iceberg_table
    )
    snapshot = spark.sql(
        f"SELECT snapshot_id FROM {boundaries_table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    if not snapshot:
        raise ValueError(f"{boundaries_table} has no Iceberg snapshot to project")
    return (
        spark.table(boundaries_table),
        spark.table(complexes_table),
        str(snapshot[0]["snapshot_id"]),
    )


def current_official_boundaries(boundaries: Any, F: Any) -> Any:
    """The rows this export is responsible for: current, and the authority's own boundary."""

    return boundaries.where(
        (F.col("valid_to_utc").isNull()) & (F.col("boundary_kind") == F.lit(OFFICIAL_BOUNDARY_KIND))
    ).select(
        "boundary_id",
        "complex_id",
        "boundary_kind",
        "geometry_srid",
        "geometry_wkb",
        "area_sqm_calculated",
        "geometry_checksum_sha256",
        "source_record_id",
        "source_snapshot_id",
    )
def project(current_boundaries: Any, complexes: Any, F: Any) -> Any:
    """Joins each boundary to its complex for the code, and shapes the export columns.

    The join is inner and `silver.industrial_complex_boundaries.complex_id` is a required column the
    Silver writer fills from this same join, so it should drop nothing. `main` compares the counts
    rather than trusting that: a boundary that quietly vanished here would be a complex missing from
    the map with nothing anywhere saying which one.
    """

    current_complexes = (
        complexes.where(F.col("valid_to_utc").isNull())
        .select("complex_id", "official_complex_code")
        .withColumnRenamed("complex_id", "complex_key")
    )
    return (
        current_boundaries.join(
            current_complexes,
            current_boundaries["complex_id"] == current_complexes["complex_key"],
            "inner",
        )
        .drop("complex_key")
        # `hex` returns upper case; the publisher and the Silver checksum both work in lower.
        .withColumn("geometry_wkb_hex", F.lower(F.hex(F.col("geometry_wkb"))))
        .withColumn("area_sqm_calculated", F.col("area_sqm_calculated").cast("string"))
        .drop("geometry_wkb")
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_args(args)
    assert_contract_columns()
    output = Path(args.output)
    if output.exists():
        raise FileExistsError(
            f"industrial-complex boundary PostGIS handoff already exists and is append-only "
            f"evidence: {output}"
        )

    SparkSession, F = load_pyspark()
    spark = build_spark_session(args, SparkSession)
    try:
        boundaries, complexes, canonical_iceberg_snapshot_id = read_sources(spark, args)
        selected = current_official_boundaries(boundaries, F)
        selected_count = selected.count()
        projected = project(selected, complexes, F)
        row_count = projected.count()
        if row_count != selected_count:
            raise ValueError(
                f"{selected_count - row_count} of {selected_count} current "
                f"{OFFICIAL_BOUNDARY_KIND} boundaries name a complex_id that "
                f"{COMPLEXES_CONTRACT_NAME} does not currently hold; the export would drop them "
                f"without saying so"
            )
        if row_count > args.max_rows:
            raise ValueError(
                f"the Silver boundary snapshot holds {row_count} exportable rows, above the "
                f"--max-rows bound of {args.max_rows}"
            )
        rows = [row.asDict() for row in projected.collect()]
    finally:
        spark.stop()

    body = export_lines(rows)
    write_create_only(output, body)
    source_snapshot_ids = sorted({str(row["source_snapshot_id"]) for row in rows})
    summary = build_summary(
        args,
        canonical_iceberg_snapshot_id,
        len(rows),
        source_snapshot_ids,
        datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    )
    if args.summary_output:
        summary_path = Path(args.summary_output)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    print(
        "industrial-complex-boundary-postgis-handoff-summary-json "
        + json.dumps(summary, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
