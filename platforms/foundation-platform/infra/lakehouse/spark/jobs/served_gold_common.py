"""What every polygon unit's served-Gold job shares (root ADR-0112 §7·§9).

A served-Gold job appends the edit store's unfolded edits to `silver.map_edit_ledger`, applies every
ledgered edit of its unit over that unit's current Silver boundaries, rewrites the unit's served
Gold table, and hands the tile bake one create-only JSONL plus a summary. The unit-specific part is
only where the base rows come from and which properties a tile carries; everything below is the
same for every unit, so it lives once.

The bake handoff is unit-agnostic: one row per feature,

    {"feature_id": ..., "properties": {<tile property>: <text>}, "geometry_wkb_hex": ...,
     "geometry_srid": ..., "origin": "source" | "edit"}

and the summary names the unit, its feature id property and its CRS
(`SERVED_SUMMARY_SCHEMA_VERSION`, read by `bake-lakehouse-tiles`).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from platform_contracts import (
    column_names,
    create_table_columns_sql,
    evolve_iceberg_table_to_contract,
    load_lakehouse_contract,
    partition_clause_sql,
    spark_sql_type,
)

SERVED_SUMMARY_SCHEMA_VERSION = "foundation-platform.polygon_served_gold.v1"
LEDGER_CONTRACT = load_lakehouse_contract("silver.map_edit_ledger")
LEDGER_COLUMNS: tuple[str, ...] = column_names(LEDGER_CONTRACT)
# The edit handoff `export-map-edit-handoff` writes; checked by name so a rename fails here.
EDIT_HANDOFF_COLUMNS: tuple[str, ...] = (
    "unit",
    "change_seq",
    "feature_id",
    "op",
    "geometry_geojson",
    "geometry_wkb_hex",
    "geometry_srid",
    "geometry_checksum_sha256",
    "properties_json",
    "editor",
    "edited_at",
)


def read_edit_handoff(
    lines: list[str], unit: str, srid: int, required_properties: tuple[str, ...]
) -> list[dict[str, Any]]:
    """Parses the edit handoff for `unit`, refusing anything the ledger contract would refuse."""

    rows: list[dict[str, Any]] = []
    seen: set[int] = set()
    for number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        row = json.loads(line)
        missing = [name for name in EDIT_HANDOFF_COLUMNS if name not in row]
        if missing:
            raise ValueError(f"edit handoff line {number} has no {', '.join(missing)}")
        if row["unit"] != unit:
            raise ValueError(f"edit handoff line {number} is for unit {row['unit']}, not {unit}")
        if int(row["geometry_srid"]) != srid:
            raise ValueError(f"edit handoff line {number} is not in EPSG:{srid}")
        seq = int(row["change_seq"])
        if seq <= 0 or seq in seen:
            raise ValueError(f"edit handoff line {number} repeats or lacks a change_seq")
        seen.add(seq)
        if row["op"] not in ("upsert", "delete"):
            raise ValueError(f"edit {seq} has op {row['op']}")
        upsert = row["op"] == "upsert"
        carries = [row["geometry_wkb_hex"], row["geometry_geojson"], row["geometry_checksum_sha256"]]
        if upsert != all(value not in (None, "") for value in carries) or (
            not upsert and any(value not in (None, "") for value in carries)
        ):
            raise ValueError(f"edit {seq}: an upsert carries its geometry, a delete carries none")
        properties = json.loads(row["properties_json"])
        if not isinstance(properties, dict):
            raise ValueError(f"edit {seq} properties are not an object")
        absent = [name for name in required_properties if upsert and not properties.get(name)]
        if absent:
            raise ValueError(f"edit {seq} upserts a feature without {', '.join(absent)}")
        rows.append(row)
    return sorted(rows, key=lambda item: int(item["change_seq"]))


def edit_fingerprint(row: dict[str, Any]) -> tuple[Any, ...]:
    """What makes two ledger rows the same edit, independent of which export carried them."""

    return (
        int(row["change_seq"]),
        row["feature_id"],
        row["op"],
        row.get("geometry_checksum_sha256") or None,
        row["properties_json"],
        row["editor"],
        row["edited_at"],
    )


def new_ledger_rows(
    ledgered: dict[int, tuple[Any, ...]], handoff: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """The handoff rows the ledger does not hold yet; the same change_seq with other content is refused."""

    fresh = []
    for row in handoff:
        seq = int(row["change_seq"])
        known = ledgered.get(seq)
        if known is None:
            fresh.append(row)
        elif known != edit_fingerprint(row):
            raise ValueError(f"edit {seq} is already ledgered with different content; the ledger is append-only")
    return fresh


def bake_handoff_lines(
    rows: list[dict[str, Any]], id_key: str, property_keys: tuple[str, ...], srid: int
) -> str:
    """The unit-agnostic bake handoff: feature id, tile properties as text, WKB, CRS, origin."""

    if not rows:
        raise ValueError("the served set is empty; a bake of nothing would erase the layer")
    body = "\n".join(
        json.dumps(
            {
                "feature_id": str(row[id_key]),
                "properties": {key: str(row[key]) for key in property_keys},
                "geometry_wkb_hex": row["geometry_wkb_hex"],
                "geometry_srid": srid,
                "origin": row["origin"],
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        for row in rows
    )
    return f"{body}\n"


def write_create_only(path: Path, body: str) -> None:
    if path.exists():
        raise FileExistsError(f"served-boundary bake handoff already exists and is evidence: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def contract_schema(contract: dict[str, Any]) -> str:
    """The DataFrame schema a contract declares, so rows are typed by the contract, not by hand."""

    return ", ".join(
        f"{column['name']} {spark_sql_type(column['logical_type'])}" for column in contract["columns"]
    )


def ensure_table(spark: Any, qualified: str, catalog: str, namespace: str, contract: dict[str, Any]) -> str:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{catalog}`.`{namespace}`")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified} (
{create_table_columns_sql(contract)}
        )
        USING iceberg
        {partition_clause_sql(contract)}
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'zstd'
        )
        """
    )
    evolve_iceberg_table_to_contract(spark, qualified, contract)
    return qualified


def latest_snapshot(spark: Any, table: str) -> str:
    rows = spark.sql(
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    if not rows:
        raise ValueError(f"{table} has no committed snapshot")
    return str(rows[0]["snapshot_id"])


def append_to_ledger(
    spark: Any, ledger_table: str, unit: str, handoff: list[dict[str, Any]], batch_id: str, now: Any
) -> tuple[int, list[dict[str, Any]]]:
    """Appends the unit's unledgered edits once each, then returns every ledgered edit of the unit."""

    existing = spark.sql(
        f"SELECT change_seq, feature_id, op, geometry_checksum_sha256, properties_json, "
        f"editor, edited_at FROM {ledger_table} WHERE unit = '{unit}'"
    ).collect()
    ledgered = {int(row["change_seq"]): edit_fingerprint(row.asDict()) for row in existing}
    fresh = new_ledger_rows(ledgered, handoff)
    if fresh:
        spark.createDataFrame(
            [
                {
                    **{name: row[name] for name in EDIT_HANDOFF_COLUMNS if name != "geometry_wkb_hex"},
                    "change_seq": int(row["change_seq"]),
                    "geometry_srid": int(row["geometry_srid"]),
                    "geometry_wkb": bytes.fromhex(row["geometry_wkb_hex"]) if row["geometry_wkb_hex"] else None,
                    "export_batch_id": batch_id,
                    "ingested_at_utc": now,
                }
                for row in fresh
            ],
            schema=contract_schema(LEDGER_CONTRACT),
        ).select(*LEDGER_COLUMNS).createOrReplaceTempView("map_edit_ledger_append")
        spark.sql(f"INSERT INTO {ledger_table} SELECT {', '.join(LEDGER_COLUMNS)} FROM map_edit_ledger_append")
    ledger = [
        row.asDict()
        for row in spark.sql(
            f"SELECT change_seq, feature_id, op, lower(hex(geometry_wkb)) AS geometry_wkb_hex, "
            f"geometry_checksum_sha256, properties_json FROM {ledger_table} WHERE unit = '{unit}'"
        ).collect()
    ]
    return len(fresh), ledger


def summary(
    *, job: str, unit: str, feature_id_property: str, srid: int, gold_snapshot: str,
    through: int, handoff: int, appended: int, ledger: int, served: int, base: int,
    output: Path, generated_at: str, counts: dict[str, int], extra: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": SERVED_SUMMARY_SCHEMA_VERSION,
        "job": job,
        "generated_at_utc": generated_at,
        "status": "ready",
        "unit": unit,
        "feature_id_property": feature_id_property,
        "geometry_srid": srid,
        "canonical_iceberg_snapshot_id": gold_snapshot,
        "edits_through_change_seq": through,
        "edits_in_handoff": handoff,
        "edits_appended_to_ledger": appended,
        "edits_in_ledger": ledger,
        "served_row_count": served,
        "source_row_count": base,
        "output_path": str(output),
        **counts,
        **extra,
    }
