#!/usr/bin/env python3
"""Build `gold.administrative_boundary_served`: the administrative boundaries exactly as they are tiled.

Root ADR-0112 §7·§9, the administrative unit. The same fold as the industrial complex one
(`served_gold_common.py`): the store's unfolded edits are appended to `silver.map_edit_ledger`
once each, every ledgered `admin` edit is applied in `change_seq` order over the newest snapshot
of `silver.administrative_boundaries`, the served table is rewritten, and the tile bake gets a
create-only JSONL plus a summary naming the committed Gold snapshot.

Silver and the edits are both EPSG:4326, so nothing is reprojected on the way.
"""

from __future__ import annotations

import argparse
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import map_matching_gate
import served_gold_common as common
from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages
from platform_contracts import column_names, declared_geometry_srid, load_lakehouse_contract

JOB_NAME = "administrative_boundary_served_gold"
UNIT = "admin"
FEATURE_ID_PROPERTY = "administrative_unit_id"
# The tile properties besides the id; the bake carries exactly these.
TILE_PROPERTIES: tuple[str, ...] = ("scope_kind", "canonical_code", "display_name")

SILVER_CONTRACT = load_lakehouse_contract("silver.administrative_boundaries")
SERVED_CONTRACT = load_lakehouse_contract("gold.administrative_boundary_served")
SERVED_COLUMNS: tuple[str, ...] = column_names(SERVED_CONTRACT)
GEOMETRY_SRID: int = declared_geometry_srid(SERVED_CONTRACT)
if declared_geometry_srid(SILVER_CONTRACT) != GEOMETRY_SRID:
    raise ValueError("silver.administrative_boundaries and the served table must share one CRS")
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_MAX_ROWS = 100_000


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--edits-input", required=True, help="JSONL from export-map-edit-handoff.")
    parser.add_argument("--output", required=True, help="Served-row JSONL for the tile bake. Never overwritten.")
    parser.add_argument("--summary-output", help="Path for the run summary JSON.")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-namespace", default="silver")
    parser.add_argument("--iceberg-table", default="administrative_boundaries")
    parser.add_argument("--ledger-iceberg-namespace", default="silver")
    parser.add_argument("--ledger-iceberg-table", default="map_edit_ledger")
    parser.add_argument("--served-iceberg-namespace", default="gold")
    parser.add_argument("--served-iceberg-table", default="administrative_boundary_served")
    parser.add_argument(
        "--code-list-table",
        default="reference.legal_dong_code_snapshot",
        help="namespace.table of the official code list snapshots the matching gate reads (ADR-0113 §7)",
    )
    parser.add_argument(
        "--allow-non-smoke-write",
        action="store_true",
        help="Required to write tables whose names do not end in _smoke.",
    )
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    parser.add_argument("--max-rows", type=int, default=DEFAULT_MAX_ROWS)
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
    parts = args.code_list_table.split(".")
    if len(parts) != 2 or not all(IDENTIFIER_PATTERN.match(p) for p in parts):
        raise ValueError("--code-list-table must be namespace.table")
    for table in (args.ledger_iceberg_table, args.served_iceberg_table):
        if not table.endswith("_smoke") and not args.allow_non_smoke_write:
            raise ValueError(
                f"writing {table} needs --allow-non-smoke-write; only *_smoke tables are written by default"
            )
    if args.max_rows <= 0:
        raise ValueError("--max-rows must be positive")
    assert_catalog_env()


def qualified(catalog: str, namespace: str, table: str) -> str:
    return f"`{catalog}`.`{namespace}`.`{table}`"


def read_edit_handoff(lines: list[str]) -> list[dict[str, Any]]:
    """Parses the admin edit handoff; an upsert must carry every tile property."""

    return common.read_edit_handoff(lines, UNIT, GEOMETRY_SRID, TILE_PROPERTIES)


def apply_edits(
    base: list[dict[str, Any]], ledger: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Applies every ledgered edit, in change order, over the newest Silver snapshot."""

    served = {row[FEATURE_ID_PROPERTY]: dict(row, origin="source") for row in base}
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
            FEATURE_ID_PROPERTY: feature,
            **{key: str(properties[key]) for key in TILE_PROPERTIES},
            "geometry_wkb_hex": edit["geometry_wkb_hex"],
            "geometry_srid": GEOMETRY_SRID,
            "geometry_checksum_sha256": edit["geometry_checksum_sha256"],
            "source_snapshot_id": f"map-edit-{int(edit['change_seq'])}",
            "origin": "edit",
        }
    codes: dict[tuple[str, str], str] = {}
    for row in served.values():
        key = (row["scope_kind"], row["canonical_code"])
        if key in codes:
            raise ValueError(
                f"{key[0]} {key[1]} would be served by both {codes[key]} and "
                f"{row[FEATURE_ID_PROPERTY]}; an edit gave two boundaries one code"
            )
        codes[key] = row[FEATURE_ID_PROPERTY]
    return sorted(served.values(), key=lambda row: row[FEATURE_ID_PROPERTY]), counts


def bake_handoff_lines(rows: list[dict[str, Any]]) -> str:
    return common.bake_handoff_lines(rows, FEATURE_ID_PROPERTY, TILE_PROPERTIES, GEOMETRY_SRID)


def load_pyspark() -> Any:
    from pyspark.sql import SparkSession  # noqa: PLC0415

    return SparkSession


def build_spark_session(args: argparse.Namespace, SparkSession: Any) -> Any:
    builder = (
        SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
    )
    builder = apply_catalog_settings(builder, args.iceberg_catalog_name)
    return builder.getOrCreate()


def read_newest_silver(spark: Any, table: str) -> tuple[list[dict[str, Any]], str, str]:
    """The newest source snapshot's rows; Silver keeps every snapshot, the map shows the newest."""

    newest = spark.sql(
        f"SELECT source_snapshot_id FROM {table} GROUP BY source_snapshot_id "
        f"ORDER BY max(ingested_at_utc) DESC, source_snapshot_id DESC LIMIT 1"
    ).collect()
    if not newest:
        raise ValueError(f"{table} holds no administrative boundaries")
    source_snapshot = newest[0]["source_snapshot_id"]
    rows = [
        row.asDict()
        for row in spark.sql(
            f"SELECT administrative_unit_id, scope_kind, canonical_code, display_name, "
            f"lower(hex(geometry_wkb)) AS geometry_wkb_hex, geometry_srid, geometry_checksum_sha256, "
            f"source_snapshot_id FROM {table} WHERE source_snapshot_id = '{source_snapshot}'"
        ).collect()
    ]
    wrong = [row[FEATURE_ID_PROPERTY] for row in rows if int(row["geometry_srid"]) != GEOMETRY_SRID]
    if wrong:
        raise ValueError(f"{len(wrong)} Silver rows are not EPSG:{GEOMETRY_SRID}")
    return rows, source_snapshot, common.latest_snapshot(spark, table)


def matching_gate(
    spark: Any, catalog: str, code_list_table: str, served_table: str, served: list[dict[str, Any]], ledger: list[dict[str, Any]]
) -> map_matching_gate.GateReport:
    """ADR-0113 §7 for the administrative layer, before the served table is rewritten.

    The official code list is the newest `reference.legal_dong_code_snapshot`; "served before" is
    the served table as it stands, minus the places an admin edit deleted on purpose.
    """

    namespace, table = code_list_table.split(".")
    code_table = qualified(catalog, namespace, table)
    newest = spark.sql(f"SELECT CAST(max(snapshot_date) AS STRING) AS d FROM {code_table}").collect()[0]["d"]
    if not newest:
        report = map_matching_gate.GateReport()
        report.refuse("no official code list snapshot", code_list_table)
        return report
    official = {
        r["region_cd"]: (r["full_name"], r["status"])
        for r in spark.sql(f"SELECT region_cd, full_name, status FROM {code_table} WHERE snapshot_date = DATE '{newest}'").collect()
    }
    previous = {
        r[FEATURE_ID_PROPERTY]: r["canonical_code"]
        for r in spark.sql(f"SELECT {FEATURE_ID_PROPERTY}, canonical_code FROM {served_table}").collect()
    }
    deleted = {row["feature_id"] for row in ledger if row["op"] == "delete"}
    return map_matching_gate.check_admin_units(
        [(row[FEATURE_ID_PROPERTY], row["canonical_code"]) for row in served],
        official,
        {i: c for i, c in previous.items() if i not in deleted},
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_args(args)
    output = Path(args.output)
    if output.exists():
        raise FileExistsError(f"served-boundary bake handoff already exists and is evidence: {output}")
    handoff = read_edit_handoff(Path(args.edits_input).read_text(encoding="utf-8").splitlines())
    batch_id = f"map-edit-export-{uuid.uuid4()}"
    now = datetime.now(timezone.utc).replace(microsecond=0)

    spark = build_spark_session(args, load_pyspark())
    try:
        catalog = args.iceberg_catalog_name
        ledger_table = common.ensure_table(
            spark,
            qualified(catalog, args.ledger_iceberg_namespace, args.ledger_iceberg_table),
            catalog, args.ledger_iceberg_namespace, common.LEDGER_CONTRACT,
        )
        appended, ledger = common.append_to_ledger(spark, ledger_table, UNIT, handoff, batch_id, now)
        base, source_snapshot, silver_snapshot = read_newest_silver(
            spark, qualified(catalog, args.iceberg_namespace, args.iceberg_table)
        )
        served, counts = apply_edits(base, ledger)
        if len(served) > args.max_rows:
            raise ValueError(f"{len(served)} served rows exceed --max-rows {args.max_rows}")
        through = max((int(row["change_seq"]) for row in ledger), default=0)

        served_table = common.ensure_table(
            spark,
            qualified(catalog, args.served_iceberg_namespace, args.served_iceberg_table),
            catalog, args.served_iceberg_namespace, SERVED_CONTRACT,
        )
        gate_report = matching_gate(spark, catalog, args.code_list_table, served_table, served, ledger)
        if not gate_report.passed:
            raise ValueError(gate_report.message())
        spark.createDataFrame(
            [
                {
                    FEATURE_ID_PROPERTY: row[FEATURE_ID_PROPERTY],
                    **{key: row[key] for key in TILE_PROPERTIES},
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
            schema=common.contract_schema(SERVED_CONTRACT),
        ).select(*SERVED_COLUMNS).createOrReplaceTempView("served_candidate")
        spark.sql(f"INSERT OVERWRITE {served_table} SELECT {', '.join(SERVED_COLUMNS)} FROM served_candidate")
        persisted = spark.sql(f"SELECT count(*) AS n FROM {served_table}").collect()[0]["n"]
        if persisted != len(served):
            raise ValueError(f"served table read back {persisted} rows, wrote {len(served)}")
        gold_snapshot = common.latest_snapshot(spark, served_table)
    finally:
        spark.stop()

    common.write_create_only(output, bake_handoff_lines(served))
    summary = common.summary(
        job=JOB_NAME, unit=UNIT, feature_id_property=FEATURE_ID_PROPERTY, srid=GEOMETRY_SRID,
        gold_snapshot=gold_snapshot, through=through, handoff=len(handoff), appended=appended,
        ledger=len(ledger), served=len(served), base=len(base), output=output,
        generated_at=now.isoformat().replace("+00:00", "Z"), counts=counts,
        extra={"silver_iceberg_snapshot_id": silver_snapshot, "source_snapshot_id": source_snapshot,
               "matching_gate": gate_report.as_dict()},
    )
    if args.summary_output:
        path = Path(args.summary_output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("administrative-boundary-served-gold-summary-json " + json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
