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

## The v2 handoff: many parts (root ADR-0133 §2)

A unit too large for one driver-written file (parcels: about forty million rows) hands the bake
`SERVED_SUMMARY_SCHEMA_VERSION_V2` instead. This section is the contract both the job and the oven
cite; nothing else restates it.

- Every part line is the v1 row above, byte for byte: `bake_handoff_line` writes both.
- For parcels `feature_id` is the PNU, `properties` is `{}`, and the summary's
  `feature_id_property` is `pnu`; the oven takes the id property from `feature_id` and refuses a
  row that repeats it as a property, so the tile carries exactly `pnu`.
- The v2 summary is every v1 field plus `source_snapshot_id` (the one Silver snapshot read) and
  `handoff_parts`: `[{"path", "rows", "sha256"}]`, where `path` is relative to `output_path` (the
  parts directory; never absolute, never `..`), `rows` counts the part's lines and `sha256` is the
  lowercase hex digest of the part file's bytes. The rows of all parts add up to
  `served_row_count`.
- Parts are written create-only by the Spark executors; the driver only sees one
  `(path, rows, sha256)` triple per part, never a row. A partition with no rows writes no part.

The v1 summary and single-file handoff of the complex and admin units are unchanged.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any

from lakehouse_engine import current_snapshot
from platform_contracts import (
    column_names,
    create_table_columns_sql,
    evolve_iceberg_table_to_contract,
    load_lakehouse_contract,
    partition_clause_sql,
    spark_sql_type,
)

SERVED_SUMMARY_SCHEMA_VERSION = "foundation-platform.polygon_served_gold.v1"
SERVED_SUMMARY_SCHEMA_VERSION_V2 = "foundation-platform.polygon_served_gold.v2"
PART_NAME_PATTERN = re.compile(r"^part-[0-9]{5}\.jsonl$")
# The executors run `write_part` and `bake_handoff_line` from this module, so they must be able to
# import it: a session that writes parts sets `spark.executorEnv.PYTHONPATH` to this directory.
JOBS_DIR = str(Path(__file__).resolve().parent)
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
        bake_handoff_line(
            str(row[id_key]),
            {key: str(row[key]) for key in property_keys},
            row["geometry_wkb_hex"],
            srid,
            row["origin"],
        )
        for row in rows
    )
    return f"{body}\n"


def bake_handoff_line(
    feature_id: str, properties: dict[str, str], geometry_wkb_hex: str, srid: int, origin: str
) -> str:
    """One bake handoff row without its newline; the v1 file and every v2 part are made of these."""

    return json.dumps(
        {
            "feature_id": feature_id,
            "properties": properties,
            "geometry_wkb_hex": geometry_wkb_hex,
            "geometry_srid": srid,
            "origin": origin,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def write_part(directory: str, index: int, lines: Any) -> list[tuple[str, int, str]]:
    """Writes one create-only part from `lines` and returns its (name, rows, sha256), or nothing.

    Runs on a Spark executor, once per partition. The file is opened when the first line arrives:
    an empty partition leaves no file, so no part promises rows it does not hold.
    """

    name = f"part-{index:05d}.jsonl"
    digest = hashlib.sha256()
    rows = 0
    handle = None
    try:
        for line in lines:
            if handle is None:
                handle = open(os.path.join(directory, name), "xb")  # noqa: SIM115
            data = f"{line}\n".encode("utf-8")
            handle.write(data)
            digest.update(data)
            rows += 1
    finally:
        if handle is not None:
            handle.close()
    return [(name, rows, digest.hexdigest())] if rows else []


def part_lines(rows: Any, property_columns: tuple[str, ...], srid: int) -> Any:
    """The handoff lines of one partition's staged rows, made on the executor."""

    for row in rows:
        yield bake_handoff_line(
            row["feature_id"],
            {name: row[name] for name in property_columns},
            row["geometry_wkb_hex"],
            srid,
            row["origin"],
        )


def write_handoff_parts(
    frame: Any, output_dir: Path, *, id_column: str, property_columns: tuple[str, ...], srid: int, parts: int
) -> list[dict[str, Any]]:
    """Writes `frame` as create-only JSONL parts from the executors and returns what each holds.

    `frame` carries `id_column`, `property_columns`, `geometry_wkb` (binary) and `origin`. The
    geometry becomes lowercase WKB hex, as the v1 handoff carries it. Rows are range partitioned
    and sorted by feature id, so one served set always yields the same parts. The executors write
    into `output_dir` directly, so it must be a path they share with the driver (`local[N]`).
    """

    from pyspark.sql import functions as F  # noqa: PLC0415

    if parts <= 0:
        raise ValueError("a handoff needs at least one part")
    executor_path = frame.sparkSession.sparkContext.environment.get("PYTHONPATH", "")
    if JOBS_DIR not in executor_path.split(os.pathsep):
        raise ValueError(
            f"the executors cannot import {JOBS_DIR}; set spark.executorEnv.PYTHONPATH to it"
        )
    if output_dir.exists():
        raise FileExistsError(f"served-boundary bake handoff already exists and is evidence: {output_dir}")
    output_dir.mkdir(parents=True)
    directory = str(output_dir)
    staged = (
        frame.select(
            F.col(id_column).cast("string").alias("feature_id"),
            *[F.col(name).cast("string").alias(name) for name in property_columns],
            F.lower(F.hex(F.col("geometry_wkb"))).alias("geometry_wkb_hex"),
            F.col("origin"),
        )
        .repartitionByRange(parts, "feature_id")
        .sortWithinPartitions("feature_id")
    )
    written = staged.rdd.mapPartitionsWithIndex(
        lambda index, rows: write_part(directory, index, part_lines(rows, property_columns, srid))
    ).collect()
    return [{"path": name, "rows": rows, "sha256": sha} for name, rows, sha in sorted(written)]


def verify_handoff_parts(output_dir: Path, parts: list[dict[str, Any]], served: int) -> None:
    """Refuses parts whose names, files, row counts or digests disagree with what was recorded."""

    if not parts:
        raise ValueError("the served set is empty; a bake of nothing would erase the layer")
    on_disk = sorted(path.name for path in output_dir.iterdir())
    named = sorted(part["path"] for part in parts)
    if on_disk != named:
        raise ValueError(f"the parts directory holds {on_disk}, the parts recorded {named}")
    total = 0
    for part in parts:
        if not PART_NAME_PATTERN.fullmatch(part["path"]):
            raise ValueError(f"part path {part['path']!r} is not a part name relative to the parts directory")
        digest = hashlib.sha256()
        lines = 0
        with (output_dir / part["path"]).open("rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
                lines += chunk.count(b"\n")
        if (lines, digest.hexdigest()) != (part["rows"], part["sha256"]):
            raise ValueError(
                f"part {part['path']} holds {lines} rows with sha256 {digest.hexdigest()}, "
                f"recorded {part['rows']} rows with sha256 {part['sha256']}"
            )
        total += part["rows"]
    if total != served:
        raise ValueError(f"the parts hold {total} rows, the served table {served}")


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


def summary_v2(*, source_snapshot_id: str, parts: list[dict[str, Any]], **v1: Any) -> dict[str, Any]:
    """The v1 summary plus the one Silver snapshot read and the parts that hold the served rows."""

    if sum(part["rows"] for part in parts) != v1["served"]:
        raise ValueError("the handoff parts do not add up to the served row count")
    for part in parts:
        if not PART_NAME_PATTERN.fullmatch(part["path"]):
            raise ValueError(f"part path {part['path']!r} is not relative to the parts directory")
    return {
        **summary(**v1),
        "schema_version": SERVED_SUMMARY_SCHEMA_VERSION_V2,
        "source_snapshot_id": source_snapshot_id,
        "handoff_parts": [{"path": p["path"], "rows": p["rows"], "sha256": p["sha256"]} for p in parts],
    }
