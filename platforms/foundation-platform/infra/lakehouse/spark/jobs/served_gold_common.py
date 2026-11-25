"""What every polygon unit's served-Gold job shares (root ADR-0112 §7·§9).

A served-Gold job appends the edit store's unfolded edits to `silver.map_edit_ledger`, applies every
ledgered edit of its unit over that unit's current Silver boundaries, rewrites the unit's served
Gold table, and hands the tile bake one create-only JSONL plus a summary. The unit-specific part is
only where the base rows come from and which properties a tile carries; everything below is the
same for every unit, so it lives once.

## Which edits a build applies (root ADR-0621)

An edit holds only while its target is as it was when the edit was saved. The first build that
ledgers an edit pins it once (`resolve_pins`): the release it was saved over names the Silver
snapshot that release was built from, and the edit keeps its target's geometry checksum there
(none when the source lacked the id). Every build then `judge`s each ledgered edit against the
current Silver: the same id with the same checksum, or still absent for an edit that added the
feature, applies; anything else is stale. A stale edit is not applied, so customers see the
official polygon; `record_outcomes` lists it in `gold.map_edit_stale`. No edit moves to another
id, and no overlap picks a target (root ADR-0113 §4).

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
from typing import Any, Callable

from lakehouse_engine import current_snapshot
from platform_contracts import (
    admit_write,
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
STALE_CONTRACT = load_lakehouse_contract("gold.map_edit_stale")
STALE_COLUMNS: tuple[str, ...] = column_names(STALE_CONTRACT)
# Why a standing edit is not applied (root ADR-0621 §4). An edit with no reason applies.
STALE_REASONS: tuple[str, ...] = (
    "pin_unreadable",  # the release it was saved over names no Silver snapshot that can be read
    "source_changed",  # the same id, another geometry
    "source_removed_id",  # the target is gone (a merge, a split, a deletion or a renumber)
    "source_gained_id",  # the edit added the feature, and the source now has that id
    "absent_at_base",  # a delete of an id the source did not have
)
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
    # Root ADR-0621 §3: the release the edit was saved over and that release's canonical Gold
    # snapshot, from the catalog. Either may be null; the edit's pin is then unreadable.
    "base_release_id",
    "base_served_snapshot_id",
)
# The pin a build writes on the edit the first time it ledgers it.
PIN_COLUMNS: tuple[str, ...] = ("base_release_id", "base_source_snapshot_id", "base_geometry_checksum_sha256")


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
    """The handoff lines of one partition's staged rows, made on the executor.

    A tile property is text, and a source that leaves a value blank gives an empty one: the bake
    reads `properties` as text only and refuses the row otherwise (2026-11-05, the first zoning
    bake: 126 zones without a zone name, one without a code). The bake's CSV writes an absent
    property as empty too, so a blank value is served the same way whichever path it took.
    """

    for row in rows:
        yield bake_handoff_line(
            row["feature_id"],
            {name: "" if row[name] is None else row[name] for name in property_columns},
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


def snapshot_exists(spark: Any, table: str, snapshot: Any) -> bool:
    """Whether `table` still holds the Iceberg snapshot `snapshot` (a metadata read, no data file)."""

    if not str(snapshot).isdigit():
        return False
    return bool(spark.sql(f"SELECT 1 FROM {table}.snapshots WHERE snapshot_id = {int(snapshot)}").take(1))


def read_at(spark: Any, table: str, snapshot: Any) -> Any:
    """`table` as it was at `snapshot`: how an edit's pin is resolved, once (root ADR-0621 §3)."""

    return spark.read.option("snapshot-id", str(int(snapshot))).table(table)


def resolve_pins(
    spark: Any, *, served_table: str, silver_table: str, fresh: list[dict[str, Any]],
    base_checksums: Callable[[Any], dict[str, str]],
) -> list[dict[str, Any]]:
    """Each fresh edit with its pin, to be written once with it to the ledger (root ADR-0621 §3).

    `base_served_snapshot_id` is the canonical Gold snapshot of the release the edit was saved over.
    Its rows name the Silver snapshot that Gold was built from (`silver_iceberg_snapshot_id`), and
    the edit's base checksum is its target's in `base_checksums(<Silver at that snapshot>)`, none when
    the source lacked the id. When either snapshot is gone the pin stays empty: the edit is stale.
    """

    pins: dict[str, tuple[str, dict[str, str]] | None] = {}
    pinned = []
    for row in fresh:
        gold = row.get("base_served_snapshot_id") or None
        if gold is not None and gold not in pins:
            pins[gold] = _pin_of(spark, served_table, silver_table, gold, base_checksums)
        pin = pins.get(gold) if gold is not None else None
        pinned.append({
            **row,
            "base_source_snapshot_id": None if pin is None else pin[0],
            "base_geometry_checksum_sha256": None if pin is None else pin[1].get(row["feature_id"]),
        })
    return pinned


def _pin_of(
    spark: Any, served_table: str, silver_table: str, gold: str, base_checksums: Callable[[Any], dict[str, str]]
) -> tuple[str, dict[str, str]] | None:
    if not snapshot_exists(spark, served_table, gold):
        return None
    silver = [row[0] for row in read_at(spark, served_table, gold).select("silver_iceberg_snapshot_id").distinct().collect()]
    if len(silver) != 1 or not snapshot_exists(spark, silver_table, silver[0]):
        return None
    return str(silver[0]), base_checksums(read_at(spark, silver_table, silver[0]))


def judge(ledger: list[dict[str, Any]], current: dict[str, str]) -> tuple[list[dict[str, Any]], dict[int, str]]:
    """The ledgered edits a build applies, and why each other one is stale (root ADR-0621 §4).

    `current` maps each feature id the current Silver serves to its geometry checksum. An edit applies
    only while its target is as pinned: the same checksum, or still absent when an upsert added the
    feature. Each edit is judged on its own, so a redraw saved over a newer release applies while the
    edit it replaces stays stale.
    """

    applied: list[dict[str, Any]] = []
    stale: dict[int, str] = {}
    for row in sorted(ledger, key=lambda item: int(item["change_seq"])):
        now = current.get(row["feature_id"])
        base = row.get("base_geometry_checksum_sha256") or None
        if not row.get("base_source_snapshot_id"):
            reason = "pin_unreadable"
        elif base is None:
            reason = ("source_gained_id" if now is not None
                      else None if row["op"] == "upsert" else "absent_at_base")
        elif now is None:
            reason = "source_removed_id"
        else:
            reason = None if now == base else "source_changed"
        if reason is None:
            applied.append(row)
        else:
            stale[int(row["change_seq"])] = reason
    return applied, stale


def outcome_changes(
    previous: dict[int, str], ledger: list[dict[str, Any]], stale: dict[int, str]
) -> list[dict[str, Any]]:
    """The edits whose outcome this build changed (root ADR-0621 §5).

    `previous` maps a change_seq to its newest recorded outcome; an edit with no row was applied, so a
    build that applies every edit writes nothing.
    """

    changes = []
    for row in sorted(ledger, key=lambda item: int(item["change_seq"])):
        seq = int(row["change_seq"])
        outcome = "stale" if seq in stale else "applied"
        if previous.get(seq, "applied") != outcome:
            changes.append({"change_seq": seq, "feature_id": row["feature_id"], "op": row["op"],
                            "outcome": outcome, "reason": stale.get(seq)})
    return changes


def record_outcomes(
    spark: Any, stale_table: str, unit: str, ledger: list[dict[str, Any]], stale: dict[int, str],
    silver_snapshot: str, batch_id: str, now: Any,
) -> list[dict[str, Any]]:
    """Appends one `gold.map_edit_stale` row for each edit whose outcome this build changed."""

    previous = {
        int(row["change_seq"]): row["outcome"]
        for row in spark.sql(
            f"SELECT change_seq, outcome FROM (SELECT change_seq, outcome, row_number() OVER (PARTITION BY "
            f"change_seq ORDER BY judged_at_utc DESC, export_batch_id DESC) AS newest FROM {stale_table} "
            f"WHERE unit = '{unit}') WHERE newest = 1"
        ).collect()
    }
    changes = outcome_changes(previous, ledger, stale)
    if changes:
        spark.createDataFrame(
            [{**change, "unit": unit, "silver_iceberg_snapshot_id": str(silver_snapshot),
              "export_batch_id": batch_id, "judged_at_utc": now} for change in changes],
            schema=contract_schema(STALE_CONTRACT),
        ).select(*STALE_COLUMNS).createOrReplaceTempView("map_edit_stale_append")
        admit_write(STALE_CONTRACT["table_name"], "append", target=stale_table)
        spark.sql(f"INSERT INTO {stale_table} SELECT {', '.join(STALE_COLUMNS)} FROM map_edit_stale_append")
    return changes


def stale_counts(stale: dict[int, str], changes: list[dict[str, Any]]) -> dict[str, int]:
    """The summary's stale counts: every edit this build left out, and the ones that went stale now."""

    return {"edits_stale": len(stale), "edits_newly_stale": sum(1 for c in changes if c["outcome"] == "stale")}


def append_to_ledger(
    spark: Any, ledger_table: str, unit: str, handoff: list[dict[str, Any]], batch_id: str, now: Any,
    pin: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
) -> tuple[int, list[dict[str, Any]]]:
    """Appends the unit's unledgered edits once each, then returns every ledgered edit of the unit.

    `pin` adds each fresh edit's pin before it is written (`resolve_pins`).
    """

    existing = spark.sql(
        f"SELECT change_seq, feature_id, op, geometry_checksum_sha256, properties_json, "
        f"editor, edited_at FROM {ledger_table} WHERE unit = '{unit}'"
    ).collect()
    ledgered = {int(row["change_seq"]): edit_fingerprint(row.asDict()) for row in existing}
    fresh = new_ledger_rows(ledgered, handoff)
    if fresh:
        fresh = pin(fresh)
    if fresh:
        spark.createDataFrame(
            [
                {
                    **{name: row[name] for name in EDIT_HANDOFF_COLUMNS
                       if name not in ("geometry_wkb_hex", *PIN_COLUMNS, "base_served_snapshot_id")},
                    **{name: row.get(name) for name in PIN_COLUMNS},
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
        admit_write(LEDGER_CONTRACT["table_name"], "append", target=ledger_table)
        spark.sql(f"INSERT INTO {ledger_table} SELECT {', '.join(LEDGER_COLUMNS)} FROM map_edit_ledger_append")
    ledger = [
        row.asDict()
        for row in spark.sql(
            f"SELECT change_seq, feature_id, op, lower(hex(geometry_wkb)) AS geometry_wkb_hex, "
            f"geometry_checksum_sha256, properties_json, base_source_snapshot_id, "
            f"base_geometry_checksum_sha256 FROM {ledger_table} WHERE unit = '{unit}'"
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
