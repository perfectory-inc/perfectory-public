"""The regulation zones filed under each z10 tile they touch (root ADR-0375 §2).

`gold.land_use_zone_area_by_tile` holds one row per (z10 tile, zone): the zone's tile properties,
geometry and a content hash, under the tile its Silver box touches with the patch buffer. It is
partitioned by the tile, so the zoning tile handoff reads only the tiles a change touches instead
of every row of `silver.land_use_zone_area` (880,424 rows on 2026-11-04, history included).

- **Membership.** A zone is filed under every z10 tile its box touches, widened by the bake's patch
  buffer (`config/tile-bake-containers.contract.json` `patch_buffer_pixels`, the Rust
  `lakehouse_tile_patch.rs` `BUFFER_PIXELS`) plus one pixel, so a tile holds every zone the bake's
  neighbourhood of that tile can ask for (a superset is safe; the bake keeps only the rows it
  needs). `tile_range` is the Rust `tile_range` line for line; the golden fixture
  `tests/fixtures/tile_range_golden.json` holds what both must compute.
- **Append-only.** The current row of `(tile_key, zone_id)` is the newest; a zone that leaves a
  tile, or the source, gets a removal marker there. Every row of one maintenance carries the Silver
  snapshot it reflects, and the commit records that snapshot even when nothing changed
  (`append_source_object`), so the next run knows where to start.
- **Maintenance reads only what was appended.** The Silver lane only appends (changed zones and a
  removal marker per vanished zone, root ADR-0275, ADR-0311); the rows appended after the
  maintained snapshot (Iceberg's incremental append scan, `gold_incremental.read_appended`) name the
  changed zones. Their previous memberships are looked up by zone id over the narrow key columns,
  and only the differences are appended. When the table is absent, or the span is not appends only
  (an expired or rewritten history), every current zone is read once: the declared full exception.
- **The change set of a bake** is what this table appended after the snapshot the tiles were made
  from: the zones it names changed, the tiles it names are dirty, and the current rows of those
  tile partitions are the neighbourhood the bake re-renders them from.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from platform_contracts import column_names, current_rows, load_lakehouse_contract

TABLE = "gold.land_use_zone_area_by_tile"
CONTRACT = load_lakehouse_contract(TABLE)
COLUMNS: tuple[str, ...] = column_names(CONTRACT)
SILVER_TABLE = "silver.land_use_zone_area"
SILVER_CONTRACT = load_lakehouse_contract(SILVER_TABLE)
ZOOM = 10
TILE_PROPERTIES: tuple[str, ...] = ("layer_code", "layer_name", "zone_code", "zone_name")
# The Silver key (`current_row_latest.key`): the 시군구 is in it because an area split at a 시군구
# boundary ships as two polygons sharing its number (ADR-0362 revision 2026-11-05).
KEY: tuple[str, ...] = tuple(SILVER_CONTRACT["current_row_latest"]["key"])
BBOX: tuple[str, ...] = ("bbox_min_x", "bbox_min_y", "bbox_max_x", "bbox_max_y")
REMOVED = "removed"
TILE_CONTRACT = Path(__file__).resolve().parents[4] / "config" / "tile-bake-containers.contract.json"
# The deepest Web Mercator latitude, as the Rust tile math clamps it.
MAX_LATITUDE = 85.051_128_779_806_59
# The membership's float boxes must cover the bake's f32 boxes rounded outward: one more pixel.
MEMBERSHIP_MARGIN_PIXELS = 1.0


def patch_buffer_pixels(path: Path = TILE_CONTRACT) -> float:
    """The bake's patch buffer, from the tile contract."""

    value = json.loads(path.read_text(encoding="utf-8"))["patch_buffer_pixels"]
    if not isinstance(value, (int, float)) or value <= 0:
        raise ValueError("patch_buffer_pixels is a positive number of pixels")
    return float(value)


MEMBERSHIP_BUFFER_PIXELS = patch_buffer_pixels() + MEMBERSHIP_MARGIN_PIXELS


def pixel(lon: float, lat: float, zoom: int) -> tuple[float, float]:
    """Web Mercator pixel of a longitude/latitude at `zoom`, 256 pixels per tile."""

    scale = 256.0 * float(1 << zoom)
    phi = math.radians(min(max(lat, -MAX_LATITUDE), MAX_LATITUDE))
    return (
        (lon + 180.0) / 360.0 * scale,
        (1.0 - math.log(math.tan(phi) + 1.0 / math.cos(phi)) / math.pi) / 2.0 * scale,
    )


def tile_range(bbox: tuple[float, float, float, float], zoom: int,
               buffer_pixels: float) -> tuple[int, int, int, int] | None:
    """The inclusive tile range `(x0, y0, x1, y1)` a box touches with the buffer; None when empty."""

    if bbox[0] > bbox[2] or bbox[1] > bbox[3]:
        return None
    left, top = pixel(bbox[0], bbox[3], zoom)
    right, bottom = pixel(bbox[2], bbox[1], zoom)
    last = float((1 << zoom) - 1)

    def tile(value: float) -> int:
        return int(min(max(math.floor(value / 256.0), 0.0), last))

    return (tile(left - buffer_pixels), tile(top - buffer_pixels),
            tile(right + buffer_pixels), tile(bottom + buffer_pixels))


def tile_key(x: int, y: int) -> int:
    return x * (1 << ZOOM) + y


def tile_xy(key: int) -> tuple[int, int]:
    return divmod(int(key), 1 << ZOOM)


def membership_tiles(min_x: float | None, min_y: float | None, max_x: float | None,
                     max_y: float | None) -> list[int]:
    """The z10 tile keys a zone is filed under; none for a box with a missing or non-finite side."""

    box = (min_x, min_y, max_x, max_y)
    if any(value is None or not math.isfinite(value) for value in box):
        return []
    span = tile_range(box, ZOOM, MEMBERSHIP_BUFFER_PIXELS)  # type: ignore[arg-type]
    if span is None:
        return []
    x0, y0, x1, y1 = span
    return [tile_key(x, y) for x in range(x0, x1 + 1) for y in range(y0, y1 + 1)]


def maintained_through(recorded: set[str], silver_snapshots: list[dict[str, Any]], head: str) -> str | None:
    """The newest Silver snapshot on `head`'s lineage that a maintenance recorded, or None.

    Walked on Silver's lineage rather than ordered by this table's commit times: a record outlives
    its commit's snapshot (root ADR-0332), and a Silver snapshot a rollback left behind is not one.
    """

    parents = {str(row["snapshot_id"]): row["parent_id"] for row in silver_snapshots}
    current: str | None = str(head)
    seen: set[str] = set()
    while current is not None and current in parents and current not in seen:
        if current in recorded:
            return current
        seen.add(current)
        parent = parents[current]
        current = None if parent is None else str(parent)
    return None


def change_list(changed: set[str], live: set[str]) -> list[dict[str, str]]:
    """The bake's change list: every changed zone, an upsert if it is current and a delete if not."""

    return [{"feature_id": zone, "op": "upsert" if zone in live else "delete"} for zone in sorted(changed)]


# --- Spark -------------------------------------------------------------------------------------


def with_zone_id(frame: Any) -> Any:
    from pyspark.sql import functions as F  # noqa: PLC0415

    return frame.withColumn("zone_id", F.concat_ws(":", *[F.col(name) for name in KEY]))


def latest_zones(silver_rows: Any) -> Any:
    """Each zone of `silver_rows` in its newest state: its row when current, `live = false` when its
    newest row is a removal marker. Two different current rows of one zone are refused."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    content = F.sha2(F.to_json(F.struct(*TILE_PROPERTIES, "geometry_checksum_sha256")), 256)
    live = (
        with_zone_id(current_rows(silver_rows, SILVER_CONTRACT))
        .select("zone_id", *TILE_PROPERTIES, "geometry_wkb", "geometry_srid", *BBOX,
                "geometry_checksum_sha256", "source_snapshot_id")
        .withColumn("content_sha256", content)
        .dropDuplicates(["zone_id", "content_sha256"])
    )
    divergent = live.groupBy("zone_id").count().where(F.col("count") > 1)
    count = int(divergent.count())
    if count:
        samples = [row["zone_id"] for row in divergent.limit(5).collect()]
        raise ValueError(f"{count} zones have two different current rows: {samples}")
    gone = with_zone_id(silver_rows).select("zone_id", "layer_code").distinct().join(
        live.select("zone_id"), "zone_id", "left_anti")
    return live.withColumn("live", F.lit(True)).unionByName(gone.withColumn("live", F.lit(False)),
                                                            allowMissingColumns=True)


def current_memberships(by_tile_rows: Any) -> Any:
    """The current live rows of `(tile_key, zone_id)`: removal markers and older rows dropped."""

    return current_rows(by_tile_rows, CONTRACT)


def membership_changes(zones: Any, previous: Any, silver_snapshot: str, now: Any) -> Any:
    """The rows one maintenance appends: a live row under each tile a zone's box touches whose
    current row there differs, and a removal marker under each tile it held and no longer touches.

    `zones` is `latest_zones`; `previous` the current memberships of those zones (`tile_key`,
    `zone_id`, `content_sha256`, `source_snapshot_id`). A zone's vintage never goes back: an older
    Silver vintage than the one a zone is filed with is refused (the lane only appends newer ones).
    """

    from pyspark.sql import functions as F  # noqa: PLC0415
    from pyspark.sql.types import ArrayType, LongType  # noqa: PLC0415

    older = (zones.where("live").select("zone_id", F.col("source_snapshot_id").alias("vintage"))
             .join(previous.select("zone_id", "source_snapshot_id").distinct(), "zone_id")
             .where(F.col("vintage") < F.col("source_snapshot_id")))
    count = int(older.count())
    if count:
        raise ValueError(f"{count} zones arrive with an older Silver vintage than the one they are filed with")
    tiles = F.udf(membership_tiles, ArrayType(LongType()))
    filed = zones.where("live").withColumn("tile_key", F.explode(tiles(*[F.col(name) for name in BBOX])))
    fresh = filed.join(previous.select("tile_key", "zone_id", "content_sha256"),
                       ["tile_key", "zone_id", "content_sha256"], "left_anti")
    left = (previous.select("tile_key", "zone_id", "layer_code")
            .join(filed.select("tile_key", "zone_id"), ["tile_key", "zone_id"], "left_anti")
            .withColumn("row_state", F.lit(REMOVED)))
    stamped = fresh.withColumn("row_state", F.lit(None).cast("string")).unionByName(left, allowMissingColumns=True)
    stamped = stamped.withColumn("silver_iceberg_snapshot_id", F.lit(silver_snapshot)).withColumn(
        "ingested_at_utc", F.lit(now).cast("timestamp"))
    for column in CONTRACT["columns"]:
        if column["name"] not in stamped.columns:
            from platform_contracts import spark_sql_type  # noqa: PLC0415

            stamped = stamped.withColumn(column["name"], F.lit(None).cast(spark_sql_type(column["logical_type"])))
    return stamped.select(*COLUMNS)


def previous_memberships(by_tile_rows: Any, zone_ids: Any) -> Any:
    """The current memberships of `zone_ids`, read over the narrow key columns only."""

    narrow = by_tile_rows.select("tile_key", "zone_id", "layer_code", "content_sha256", "source_snapshot_id",
                                 "ingested_at_utc", "row_state")
    return current_memberships(narrow.join(zone_ids.select("zone_id").distinct(), "zone_id", "left_semi"))


def neighbourhood(by_tile_rows: Any, tiles: list[int]) -> Any:
    """The current zones of the tile partitions `tiles`, once each (a zone may touch several)."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    rows = current_memberships(by_tile_rows.where(F.col("tile_key").isin([int(tile) for tile in tiles])))
    zones = rows.drop("tile_key").dropDuplicates(["zone_id", "content_sha256"]).persist()
    split = zones.groupBy("zone_id").count().where(F.col("count") > 1)
    count = int(split.count())
    if count:
        raise ValueError(f"{count} zones are filed with two different contents")
    return zones


def snapshot_rows(spark: Any, table: str) -> list[dict[str, Any]]:
    rows = spark.sql(f"SELECT snapshot_id, parent_id, operation, committed_at, summary FROM {table}.snapshots").collect()
    return [{"snapshot_id": r.snapshot_id, "parent_id": r.parent_id, "operation": r.operation,
             "committed_ms": int(r.committed_at.timestamp() * 1000), "summary": r.summary or {}} for r in rows]


def own_appends(snapshots: list[dict[str, Any]]) -> list[tuple[Any, Any]]:
    """Every append of this table as a (parent, snapshot) step: all it ever commits besides compaction."""

    return [(s["parent_id"], s["snapshot_id"]) for s in snapshots
            if s["operation"] == "append" and s["parent_id"] is not None]


def appended_between(spark: Any, table: str, old: str, new: str) -> Any | None:
    """The rows this append-only table gained from snapshot `old` to `new` (Iceberg's incremental
    append scan per step), or None when the walk back from `new` passes anything but its own appends
    and compactions (`changelog.chain`): an expired or foreign history."""

    from changelog import chain  # noqa: PLC0415

    snapshots = snapshot_rows(spark, table)
    steps = chain(snapshots, own_appends(snapshots), old, new)
    if steps is None:
        return None
    unquoted = table.replace("`", "")
    frames = [spark.read.format("iceberg").option("start-snapshot-id", start).option("end-snapshot-id", end)
              .load(unquoted) for start, end in steps]
    if not frames:
        return spark.table(unquoted).limit(0)
    out = frames[0]
    for frame in frames[1:]:
        out = out.unionByName(frame)
    return out


def total_records(snapshots: list[dict[str, Any]], snapshot: str) -> int:
    """The rows a snapshot holds, from its summary: metadata, nothing read."""

    for row in snapshots:
        if str(row["snapshot_id"]) == str(snapshot):
            return int(row["summary"].get("total-records", 0))
    raise ValueError(f"no snapshot {snapshot}")


def partition_records(spark: Any, table: str, tiles: list[int]) -> int:
    """The rows the data files of these tile partitions hold, history included (metadata)."""

    if not tiles:
        return 0
    listed = ", ".join(str(int(tile)) for tile in tiles)
    row = spark.sql(f"SELECT coalesce(sum(record_count), 0) AS rows FROM {table}.files "
                    f"WHERE partition.tile_key IN ({listed})").collect()[0]
    return int(row["rows"])


def full_reason(existed: bool, through: str | None, snapshot: str) -> str:
    if not existed:
        return "the table did not exist: its first build reads every current zone once"
    if through is None:
        return "no maintenance of it is recorded"
    return f"Silver did not move from {through} to {snapshot} by recorded changes and compactions only"


def maintain(spark: Any, by_tile: str, silver: str, catalog: str, snapshot: str, now: Any) -> dict[str, Any]:
    """Brings `by_tile` to Silver `snapshot`, reading only the Silver rows appended since the snapshot
    it last maintained; returns what it read and appended, and the table's snapshot after it."""

    from changelog import changes_between  # noqa: PLC0415
    from lakehouse_ingest import append_source_object, read_ingested_objects, unquoted_table_name  # noqa: PLC0415
    from served_gold_common import ensure_table  # noqa: PLC0415

    unquoted = unquoted_table_name(by_tile)
    existed = spark.catalog.tableExists(unquoted)
    ensure_table(spark, by_tile, catalog, "gold", CONTRACT)
    snapshots = snapshot_rows(spark, by_tile) if existed else []
    silver_snapshots = snapshot_rows(spark, silver)
    through = maintained_through(set(read_ingested_objects(spark, by_tile, unquoted)), silver_snapshots, snapshot)
    stats: dict[str, Any] = {"maintained_from": through, "silver_rows_read": 0, "key_rows_scanned": 0,
                             "rows_appended": 0}
    if through == snapshot:
        return {**stats, "scope": "unchanged", "by_tile_snapshot": current_snapshot_id(spark, by_tile)}
    rows = changes_between(spark, silver, through, snapshot) if through else None
    if rows is not None:
        rows = rows.persist()
        stats.update(scope="changed-keys", silver_rows_read=int(rows.count()))
    else:
        rows = spark.sql(f"SELECT * FROM {silver} VERSION AS OF {snapshot}")
        stats.update(scope="full", full_reason=full_reason(existed, through, snapshot),
                     silver_rows_read=total_records(silver_snapshots, snapshot))
    zones = latest_zones(rows).persist()
    previous = previous_memberships(spark.table(unquoted), zones).persist()
    if existed and snapshots:
        # The key lookup reads the narrow key columns of every row (no geometry).
        stats["key_rows_scanned"] = total_records(snapshots, current_snapshot_id(spark, by_tile))
    changes = membership_changes(zones, previous, snapshot, now).persist()
    stats["rows_appended"] = int(changes.count())
    append_source_object(spark, changes, COLUMNS, by_tile, snapshot, contract_table=CONTRACT["table_name"])
    for frame in (changes, previous, zones, rows):
        frame.unpersist()
    return {**stats, "by_tile_snapshot": current_snapshot_id(spark, by_tile)}


def current_snapshot_id(spark: Any, table: str) -> str:
    from lakehouse_engine import current_snapshot  # noqa: PLC0415

    return current_snapshot(spark, table)
