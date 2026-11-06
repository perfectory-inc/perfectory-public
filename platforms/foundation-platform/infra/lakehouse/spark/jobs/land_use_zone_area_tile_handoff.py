#!/usr/bin/env python3
"""Hand the regulation-zone tile bake the zones a change touches, or every current zone.

Root ADR-0373. The `zoning` tile unit has no staff edits and no served Gold table: the Silver
current rows (newest row per `(layer_code, zone_feature_id, sigungu_code)`, removal markers dropped, read at one
pinned snapshot) are exactly what the tiles show, so this job writes them in the unit-agnostic v1
bake handoff (`served_gold_common.py`) and a summary whose `canonical_iceberg_snapshot_id` is that
Silver snapshot. Rewriting a Gold copy of 1.6 GB on every monthly change would be a full write the
tiles do not need (root ADR-0314).

- Feature id `<layer_code>:<zone_feature_id>:<sigungu_code>` (`D032:<A1>:<A4>`): unique, and its
  first four characters are the layer. The area number alone is not unique within a layer: the
  provider ships one area split at a 시군구 boundary as two polygons sharing `A1` (308 such pairs in
  D036 of 20261009, measured 2026-11-05), so the 시군구 is part of the key. The bake partitions by
  the layer which the bake partitions by (root ADR-0316) so an unchanged
  layer's archive is reused; the patch path (root ADR-0323) then re-tiles only the tiles a changed
  zone touches.
- Tile properties: the provider's own words, `layer_code`, `layer_name`, `zone_code`, `zone_name`.
- When `--served-snapshot` is the table's current snapshot nothing is read or written and the job
  exits 3: the tiles already show it.
- When the current rows are not one per key (two different rows of one key, a malformed key), the
  job writes nothing to the bake, writes every offending key to `<summary>.refused.json` and exits
  65 (`REFUSED_EXIT`): the run fails with a report, not a traceback (the first run of 2026-11-03
  raised on 308 keys and named five).

Root ADR-0375 §2: the handoff is partial. First `gold.land_use_zone_area_by_tile`
(`land_use_zone_area_by_tile.py`) is brought to the snapshot from the Silver rows appended since it
last was. Then, given the by-tile snapshot the served tiles were made from
(`--served-by-tile-snapshot`), the rows that table appended since name the changed zones and the
dirty z10 tiles; the handoff is the current zones of those tile partitions (the neighbourhood the
bake re-renders the dirty tiles from) plus a change list file: every changed zone id, `upsert` when
it is current and `delete` when not. The bake merges the list into the served snapshot's feature
index (root ADR-0323 §1) and patches. A partial handoff is never a base: when the bake declines the
patch it exits 65, and the job runs this again with `--full`, the declared exception that hands
every current zone, read from Silver as before. The summary says which (`handoff_scope`), why a full
one is full, the snapshot a partial one applies to, and every row it read (`rows_read`).

The executors write the rows as create-only parts; the driver then streams them into the one v1
file, so no row passes through the driver's memory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import served_gold_common as common
from lakehouse_engine import SHUFFLE_SETTINGS, apply_catalog_settings, assert_catalog_env, iceberg_packages
from platform_contracts import current_rows, load_lakehouse_contract

import land_use_zone_area_by_tile as by_tile

JOB_NAME = "land_use_zone_area_tile_handoff"
UNIT = "zoning"
FEATURE_ID_PROPERTY = "zone_id"
TILE_PROPERTIES: tuple[str, ...] = ("layer_code", "layer_name", "zone_code", "zone_name")
SILVER_TABLE = "silver.land_use_zone_area"
SILVER_CONTRACT = load_lakehouse_contract(SILVER_TABLE)
# The export reprojects every layer to EPSG:4326 (root ADR-0362 §4); each row is checked below.
GEOMETRY_SRID = 4326
# The Silver contract's key (`current_row_latest.key`), read so the two cannot disagree.
KEY: tuple[str, ...] = tuple(SILVER_CONTRACT["current_row_latest"]["key"])
# What makes two current rows of one key the same zone; keep_ties may return both (ADR-0328).
CONTENT: tuple[str, ...] = (*TILE_PROPERTIES, "geometry_checksum_sha256")
SNAPSHOT_PATTERN = re.compile(r"^[1-9][0-9]*$")
# `D032`: the bake partitions by these four characters (tile-bake-containers.contract.json).
LAYER_CODE_PATTERN = "^D[0-9]{3}$"
SIGUNGU_CODE_PATTERN = "^[0-9]{5}$"
UNCHANGED_EXIT = 3
# The shared refusal exit (foundation_publisher_core::refusal::Refused): the input is wrong, not the job.
REFUSED_EXIT = 65
REPORTED_KEYS = 1000


class Refused(Exception):
    """The current rows cannot be baked; `report` names why and which keys, for the operator."""

    def __init__(self, reason: str, report: dict[str, Any]) -> None:
        super().__init__(reason)
        self.reason = reason
        self.report = report
# The lane's layers not loaded yet (root ADR-0373 §4): the summary names them, so the bake log and
# the map say they are waiting instead of looking complete.
LANE_CONTRACT = Path(__file__).resolve().parents[2] / "contracts" / "vworld-land-use-zone-area-source-objects.json"


def pending_layers(contract: dict[str, Any]) -> list[dict[str, str]]:
    """`D<region>` of each pending region, refused if the lane also loads it."""

    loaded = set(contract["silver_refresh"]["completeness"]["regions"])
    pending = contract.get("pending_regions", [])
    both = sorted(item["region"] for item in pending if item["region"] in loaded)
    if both:
        raise ValueError(f"regions {both} are both loaded and pending")
    return [{"layer_code": f"D{item['region']}", "name": item["name"]} for item in pending]
DEFAULT_PARTS = 16
CHANGE_LIST_NAME = "changes.jsonl"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", required=True, help="The v1 bake handoff JSONL. Never overwritten.")
    parser.add_argument("--summary-output", required=True, help="The served summary the bake reads.")
    parser.add_argument("--served-snapshot", default="", help="The Silver snapshot the tiles show now, if any.")
    parser.add_argument("--served-by-tile-snapshot", default="",
                        help="The gold.land_use_zone_area_by_tile snapshot those tiles were made from, if any.")
    parser.add_argument("--full", action="store_true",
                        help="Hand every current zone: the bake declined a partial handoff (root ADR-0375 §2).")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    parser.add_argument("--parts", type=int, default=DEFAULT_PARTS)
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    for name in ("served_snapshot", "served_by_tile_snapshot"):
        value = getattr(args, name)
        if value and not SNAPSHOT_PATTERN.fullmatch(value):
            raise ValueError(f"--{name.replace('_', '-')} is an Iceberg snapshot id")
    if args.parts <= 0:
        raise ValueError("--parts must be positive")
    if Path(args.output).exists():
        raise FileExistsError(f"the bake handoff already exists and is evidence: {args.output}")
    assert_catalog_env()


def concatenate_parts(parts_dir: Path, parts: list[dict[str, Any]], output: Path) -> tuple[int, str]:
    """Streams the verified parts, in name order, into one create-only file; returns rows and sha256."""

    digest = hashlib.sha256()
    rows = 0
    with output.open("xb") as target:
        for part in sorted(parts, key=lambda item: item["path"]):
            with (parts_dir / part["path"]).open("rb") as source:
                for chunk in iter(lambda: source.read(1 << 20), b""):
                    target.write(chunk)
                    digest.update(chunk)
                    rows += chunk.count(b"\n")
    return rows, digest.hexdigest()


def build_spark_session(args: argparse.Namespace) -> Any:
    from pyspark.sql import SparkSession  # noqa: PLC0415

    builder = (
        SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.executorEnv.PYTHONPATH", common.JOBS_DIR)
        .config(map=SHUFFLE_SETTINGS)
    )
    builder = apply_catalog_settings(builder, args.iceberg_catalog_name)
    return builder.config("spark.jars.packages", args.iceberg_packages).getOrCreate()


def served_rows(spark: Any, table: str, snapshot: str) -> Any:
    """The current zones at `snapshot`, one row per key, with the tile id; divergent ties refused."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    frame = current_rows(spark.sql(f"SELECT * FROM {table} VERSION AS OF {snapshot}"), SILVER_CONTRACT)
    frame = frame.select(*KEY, *TILE_PROPERTIES, "geometry_checksum_sha256", "geometry_wkb", "geometry_srid")
    frame = frame.dropDuplicates([*KEY, *CONTENT])
    divergent = frame.groupBy(*KEY).count().where(F.col("count") > 1)
    count = int(divergent.count())
    if count:
        raise Refused("divergent_current_rows", refusal_report(
            snapshot, count, [row.asDict() for row in divergent.orderBy(*KEY).limit(REPORTED_KEYS).collect()]))
    wrong = int(frame.where(F.col("geometry_srid") != GEOMETRY_SRID).count())
    if wrong:
        raise ValueError(f"{wrong} zones are not EPSG:{GEOMETRY_SRID}")
    unnamed = frame.where(
        ~F.col("layer_code").rlike(LAYER_CODE_PATTERN)
        | F.col("zone_feature_id").isNull()
        | (F.trim(F.col("zone_feature_id")) == "")
        | F.col("zone_feature_id").contains(":")
        | F.col("sigungu_code").isNull()
        | ~F.col("sigungu_code").rlike(SIGUNGU_CODE_PATTERN)
    ).select(*KEY)
    count = int(unnamed.count())
    if count:
        raise Refused("malformed_key", refusal_report(
            snapshot, count, [row.asDict() for row in unnamed.limit(REPORTED_KEYS).collect()]))
    return frame.withColumn(FEATURE_ID_PROPERTY, F.concat_ws(":", *[F.col(name) for name in KEY])).withColumn(
        "origin", F.lit("source")
    )


def refusal_report(snapshot: str, count: int, keys: list[dict[str, Any]]) -> dict[str, Any]:
    """What an operator needs to act on a refusal: the snapshot, how many keys, and which."""

    return {"silver_iceberg_snapshot_id": snapshot, "key": list(KEY), "keys": count,
            "listed": len(keys), "sample": keys}


def metadata_object_key(spark: Any, table: str, snapshot: str) -> str:
    """The bucket-relative key of the metadata file that made `snapshot` current: the input the
    tiles are baked from, which a first release names as its source (root ADR-0374)."""

    rows = spark.sql(
        f"SELECT file FROM {table}.metadata_log_entries WHERE latest_snapshot_id = {snapshot} "
        "ORDER BY timestamp DESC LIMIT 1"
    ).collect()
    if not rows:
        raise ValueError(f"{table} has no metadata file for snapshot {snapshot}")
    return bucket_relative(str(rows[0]["file"]))


def bucket_relative(location: str) -> str:
    """`s3://bucket/a/b.json` -> `a/b.json`; anything else is refused."""

    match = re.fullmatch(r"[a-z][a-z0-9+.-]*://[^/]+/(.+)", location)
    if not match or ".." in match.group(1).split("/"):
        raise ValueError(f"{location!r} is not an object location")
    return match.group(1)


def partial_refusal(args: argparse.Namespace) -> str | None:
    """Why this run cannot hand a partial handoff, or None (root ADR-0375 §2)."""

    if args.full:
        return "the bake declined the partial handoff (--full)"
    if not args.served_snapshot:
        return "no tiles are served yet: the unit's first bake"
    if not args.served_by_tile_snapshot:
        return f"the served tiles name no {by_tile.TABLE} snapshot"
    return None


def change_list_file(directory: Path, name: str, lines: list[dict[str, str]]) -> dict[str, Any]:
    """Writes the change list create-only beside the handoff; returns what the summary records."""

    body = "".join(json.dumps(line, sort_keys=True, separators=(",", ":")) + "\n" for line in lines).encode("utf-8")
    with (directory / name).open("xb") as target:
        target.write(body)
    return {"path": name, "rows": len(lines), "sha256": hashlib.sha256(body).hexdigest()}


def partial_handoff(spark: Any, by_tile_table: str, appended: Any) -> tuple[Any, dict[str, Any], list[dict[str, str]]]:
    """The neighbourhood frame, the summary's partial block and the change list."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    memberships = appended.select("tile_key", "zone_id").distinct().collect()
    changed = {str(row["zone_id"]) for row in memberships}
    tiles = sorted({int(row["tile_key"]) for row in memberships})
    frame = by_tile.neighbourhood(spark.table(by_tile_table.replace("`", "")), tiles)
    wrong = int(frame.where(F.col("geometry_srid") != GEOMETRY_SRID).count())
    if wrong:
        raise ValueError(f"{wrong} filed zones are not EPSG:{GEOMETRY_SRID}")
    live: set[str] = set()
    if changed:
        ids = spark.createDataFrame([(zone,) for zone in sorted(changed)], "zone_id string")
        live = {str(row["zone_id"]) for row in frame.join(ids, "zone_id", "left_semi").select("zone_id").collect()}
    block = {
        "neighbourhood_zoom": by_tile.ZOOM,
        "neighbourhood_tiles": [list(by_tile.tile_xy(tile)) for tile in tiles],
        "by_tile_rows_appended": len(memberships),
        "by_tile_neighbourhood_rows": by_tile.partition_records(spark, by_tile_table, tiles),
    }
    return frame.withColumn("origin", F.lit("source")), block, by_tile.change_list(changed, live)


def write_frame(frame: Any, output: Path, parts_count: int, allow_empty: bool) -> tuple[int, str, dict[str, int]]:
    """Writes `frame` as the create-only v1 handoff; returns rows, sha256 and zones per layer."""

    parts_dir = output.with_name(output.name + ".parts")
    parts = common.write_handoff_parts(frame, parts_dir, id_column=FEATURE_ID_PROPERTY,
                                       property_columns=TILE_PROPERTIES, srid=GEOMETRY_SRID, parts=parts_count)
    per_layer = {row["layer_code"]: int(row["count"]) for row in frame.groupBy("layer_code").count().collect()}
    served = sum(part["rows"] for part in parts)
    if served == 0 and allow_empty:
        # A partial handoff whose changes left no zone in their tiles: the change list is the change.
        shutil.rmtree(parts_dir)
        output.open("xb").close()
        return 0, hashlib.sha256(b"").hexdigest(), per_layer
    common.verify_handoff_parts(parts_dir, parts, served)
    rows, sha256 = concatenate_parts(parts_dir, parts, output)
    if rows != served:
        raise ValueError(f"the handoff holds {rows} rows, the parts {served}")
    shutil.rmtree(parts_dir)
    return rows, sha256, per_layer


def report_refusal(summary_output: Path, refusal: Refused) -> int:
    """Writes the refusal report beside the summary the bake would have read, and says so."""

    path = summary_output.with_name(summary_output.name + ".refused.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"reason": refusal.reason, **refusal.report}, ensure_ascii=False, indent=2,
                               sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(f"land-use-zone-area-tile-handoff-refused reason={refusal.reason} "
          f"keys={refusal.report['keys']} report={path}")
    return REFUSED_EXIT


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_args(args)
    output = Path(args.output)
    catalog = args.iceberg_catalog_name
    table = f"`{catalog}`.`silver`.`land_use_zone_area`"
    by_tile_table = f"`{catalog}`.`gold`.`land_use_zone_area_by_tile`"
    now = datetime.now(timezone.utc).replace(microsecond=0)
    spark = build_spark_session(args)
    try:
        snapshot = common.current_snapshot(spark, table)
        if snapshot == args.served_snapshot:
            print(f"land-use-zone-area-tile-handoff-unchanged snapshot={snapshot}")
            return UNCHANGED_EXIT
        metadata_key = metadata_object_key(spark, table, snapshot)
        maintenance = by_tile.maintain(spark, by_tile_table, table, catalog, snapshot, now)
        print("land-use-zone-area-by-tile-maintained " + json.dumps(maintenance, sort_keys=True), flush=True)
        reason = partial_refusal(args)
        appended = None
        if reason is None:
            appended = by_tile.appended_between(spark, by_tile_table, args.served_by_tile_snapshot,
                                                maintenance["by_tile_snapshot"])
            if appended is None:
                reason = (f"{by_tile.TABLE} did not move from {args.served_by_tile_snapshot} to "
                          f"{maintenance['by_tile_snapshot']} by its own appends and compactions")
        rows_read = {"silver": maintenance["silver_rows_read"], "by_tile_key_scan": maintenance["key_rows_scanned"]}
        if reason is None:
            frame, block, changes = partial_handoff(spark, by_tile_table, appended)
            block.update(base_snapshot=args.served_snapshot, base_by_tile_snapshot=args.served_by_tile_snapshot,
                         change_list=change_list_file(output.parent, f"{output.name}.{CHANGE_LIST_NAME}", changes))
            rows_read.update(by_tile_appended=block["by_tile_rows_appended"],
                             by_tile_neighbourhood=block["by_tile_neighbourhood_rows"])
            scope = {"handoff_scope": "partial", "partial": block,
                     "changed_zones": {op: sum(line["op"] == op for line in changes) for op in ("upsert", "delete")}}
        else:
            try:
                frame = served_rows(spark, table, snapshot)
            except Refused as refusal:
                return report_refusal(Path(args.summary_output), refusal)
            rows_read["silver_full_read"] = by_tile.total_records(by_tile.snapshot_rows(spark, table), snapshot)
            scope = {"handoff_scope": "full", "full_reason": reason}
        served, sha256, per_layer = write_frame(frame, output, args.parts, allow_empty=reason is None)
    finally:
        spark.stop()
    print(f"land-use-zone-area-tile-handoff-read scope={scope['handoff_scope']} rows_handed={served} "
          f"rows_read={json.dumps(rows_read, sort_keys=True)}")
    summary = common.summary(
        job=JOB_NAME, unit=UNIT, feature_id_property=FEATURE_ID_PROPERTY, srid=GEOMETRY_SRID,
        gold_snapshot=snapshot, through=0, handoff=0, appended=0, ledger=0, served=served, base=served,
        output=output, generated_at=now.isoformat().replace("+00:00", "Z"), counts={},
        extra={"silver_table": SILVER_TABLE, "silver_iceberg_snapshot_id": snapshot,
               "silver_metadata_object_key": metadata_key,
               "by_tile_table": by_tile.TABLE, "by_tile_iceberg_snapshot_id": maintenance["by_tile_snapshot"],
               "by_tile_maintenance": maintenance, "rows_read": rows_read, **scope,
               "handoff_sha256": sha256, "zones_per_layer": dict(sorted(per_layer.items())),
               "pending_layers": pending_layers(json.loads(LANE_CONTRACT.read_text(encoding="utf-8")))},
    )
    path = Path(args.summary_output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("land-use-zone-area-tile-handoff-summary-json " + json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
