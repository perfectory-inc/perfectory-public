#!/usr/bin/env python3
"""Build the regulation zones by-PNU kind's Gold from the zone polygons (root ADR-0362, ADR-0375).

A kind with its own producer (`by_pnu_kinds.Kind.producer`, root ADR-0287 §9): served on the parcel
lane like a panel section, but built from `silver.land_use_zone_area`, not from the parcel panel.
The parcel document's schema does not change, so adding this kind never re-bakes the parcel lane.

What a PNU's document holds (`regulation_zones`): every zone of the zone table's current rows (the
contract's `current_rows`, root ADR-0275) whose polygon covers part of the parcel's polygon, in the
provider's words: the layer's code and name, the zone's code and name as the provider wrote them,
`<layer_code>:<zone_feature_id>:<sigungu_code>` (the id the zoning map tiles give the same zone, root ADR-0362 §5),
and the date of the release (판) the zone's current row came from. At most ZONE_PAGE_BOUND zones are
listed; `zone_total` (the Gold column) counts all.

Which zones count (`counts`): the overlap is area, never a shared edge or corner, and at least
MIN_SHARE of the parcel or MIN_AREA_M2. Zone and parcel boundaries are drawn from different
surveys: on 2026-11-03 (세종, 562,424 bounding-box pairs of 4 layers, read-only), 17% of the pairs
with any overlap overlapped by less than 1 m² and 1% of the parcel, a sliver along a shared edge,
while the overlaps of 1% and more held 87,000 of 107,000 and only 3,351 lay between 1% and 5%.

The comparison is the standard-library kernel `zone_overlap` (no Sedona in the Spark image), run
on grid cells: a zone is cut into the cells of CELL_DEGREES it covers, and a parcel is compared
only with the pieces of the cells its bounding box touches, whose box meets its own.

Only PNUs the served parcel edition holds have a document (root ADR-0193 §9, ADR-0292), and the
parcel's PNU and polygon come from the same served row, so nothing is re-keyed across a code change.

Partial by design (root ADR-0312 §2): the PNUs a run recomputes are those of the parcels whose box
meets the box of a zone row the Silver release appended (a changed zone's new row, a removal
marker carrying the removed polygon) or of that zone's previous current row; of the parcels that
entered, left or changed in the served edition; and of the parcels under a code a code change row
names (`parcel_panel_silver_to_gold.code_change_pnus`, root ADR-0329 §3).
"""
from __future__ import annotations

import argparse
import json
import os
from typing import Any

JOB_NAME = "regulation_zones_silver_to_gold"
ZONE_SOURCE = "silver.land_use_zone_area"
PARCEL_SOURCE = "silver.parcel_boundaries"
CODE_CHANGE_SOURCE = "reference.legal_dong_code_change"
SECTION = "regulation_zones"
# The Silver key: an area split at a 시군구 boundary ships as two polygons sharing its number
# (ADR-0362 revision 2026-11-05), so the 시군구 is part of a zone's identity.
ZONE_KEY = ("layer_code", "zone_feature_id", "sigungu_code")
BOX = ("bbox_min_x", "bbox_min_y", "bbox_max_x", "bbox_max_y")
# The grid a zone is cut into (degrees, about 220 x 280 m). Measured 2026-11-03 (Trino, read-only)
# in the westernmost longitude band (a 2.5-degree-wide strip): 0.01° gave 6.15M parcel-piece pairs whose boxes meet, at 167 piece vertices a
# pair; 0.005° 5.60M at 75; 0.0025° 5.76M at 33, a fifth of the comparisons for as many pairs.
CELL_DEGREES = 0.0025
# Which overlaps count (see the module text for the measurement).
MIN_SHARE = 0.01
MIN_AREA_M2 = 1000.0
# A parcel's listed zones: past this the document keeps the first ones (zone_total says how many).
ZONE_PAGE_BOUND = 100
ZONE_FIELDS = ("zone_id", "layer_code", "layer_name", "zone_code", "zone_name", "release_date")
ZONE_STRUCT = ("array<struct<zone_id:string,layer_code:string,layer_name:string,zone_code:string,"
               "zone_name:string,release_date:string>>")
# The release a zone row came from: `vworldkr-land-use-zone:YYYYMMDD` (the 전체데이터 file's date).
RELEASE_DATE = r":(\d{4})(\d{2})(\d{2})$"
# The columns the build reads from each input, and the only ones it is handed.
BUILD_COLUMNS = {
    ZONE_SOURCE: (*ZONE_KEY, "layer_name", "zone_code", "zone_name", "geometry_wkb", *BOX, "source_snapshot_id"),
    PARCEL_SOURCE: ("pnu", "geometry_wkb", *BOX),
}
RINGS = "array<array<double>>"
PIECES = f"array<struct<i:bigint,j:bigint,rings:{RINGS}>>"
PAIRS = ("array<struct<pnu:string,layer_code:string,zone_feature_id:string,sigungu_code:string,overlap_m2:double,"
         "area_m2:double>>")
# How a document is written: nulls too, so its paths are always the registry's.
WRITTEN = {"ignoreNullFields": "false"}


def lane_inputs(lane: str) -> tuple[tuple[str, ...], dict[str, str], str, tuple[str, ...]]:
    """(required inputs, optional input -> flag without it, anchor input, whole-table inputs), the
    seam `gold_rebuild.producer_inputs` reads. No input rebuilds whole: a code change reaches the
    parcels under its codes."""
    del lane
    return (PARCEL_SOURCE, ZONE_SOURCE, CODE_CHANGE_SOURCE), {}, PARCEL_SOURCE, ()


def parse_args(argv=None) -> argparse.Namespace:
    """The arguments `scripts/ops/gold-panel-rebuild.sh` gives every producer (root ADR-0193 §7)."""
    import gold_incremental  # noqa: PLC0415
    import section_coverage  # noqa: PLC0415

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lane", required=True)
    parser.add_argument("--input-mode", choices=("iceberg",), default="iceberg")
    parser.add_argument("--write-mode", choices=("iceberg",), default="iceberg")
    parser.add_argument("--iceberg-catalog-name", default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_CATALOG_NAME", "r2"))
    parser.add_argument("--source-iceberg-namespace", default="silver")
    parser.add_argument("--target-iceberg-namespace", default="gold")
    parser.add_argument("--iceberg-snapshot-id", required=True, help="The silver.parcel_boundaries snapshot this build reads.")
    parser.add_argument("--source-snapshots-path", required=True, help="JSON pinning every input table.")
    parser.add_argument("--published-at-utc")
    parser.add_argument("--allow-non-smoke-overwrite", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--minimum-count", type=int, help="Refuse, before writing, a Gold with fewer rows (root ADR-0139).")
    parser.add_argument("--summary-output")
    parser.add_argument("--lineage-output")
    section_coverage.add_arguments(parser)
    gold_incremental.add_arguments(parser)
    args = parser.parse_args(argv)
    # The shared panel readers and build_or_merge read these; this producer writes the whole kind or merges.
    args.region_prefix, args.iceberg_write_mode, args.expected_count = None, "overwrite", None
    section_coverage.validate_arguments(args)
    if args.minimum_count is not None and args.minimum_count < 0:
        parser.error("--minimum-count must be non-negative")
    if not args.validate_only and not args.allow_non_smoke_overwrite:
        parser.error("overwriting the Gold needs --allow-non-smoke-overwrite")
    return args


def flat(rings: list) -> list[list[float]]:
    return [[c for point in ring for c in point] for ring in rings]


def unflat(rings: list) -> list:
    return [list(zip(ring[::2], ring[1::2])) for ring in rings]


def parcel_shape(wkb: bytes | None) -> tuple[list[list[float]], float]:
    """A parcel's rings and its area in m²; none for a geometry that is not a polygon (it covers no
    zone's area)."""
    import zone_overlap  # noqa: PLC0415 - shipped to the executors with addPyFile

    try:
        rings = zone_overlap.rings_of(wkb) if wkb is not None else []
    except ValueError:
        rings = []
    if not rings:
        return [], 0.0
    _, y0, _, y1 = zone_overlap.bbox(rings)
    return flat(rings), zone_overlap.square_metres(zone_overlap.region_area(rings), (y0 + y1) / 2)


def zone_pieces(wkb: bytes | None) -> list[tuple]:
    """A zone cut into its grid cells: (column, row, rings)."""
    import zone_overlap  # noqa: PLC0415

    try:
        rings = zone_overlap.rings_of(wkb) if wkb is not None else []
    except ValueError:
        rings = []
    return [(i, j, flat(piece)) for i, j, piece in zone_overlap.cell_pieces(rings, CELL_DEGREES)]


def cell_pairs(i: int, j: int, parcels: list, pieces: list) -> list[tuple]:
    """(pnu, layer code, zone id, overlap m², parcel m²) of every parcel and zone piece of one cell
    that overlap by some area."""
    import zone_overlap  # noqa: PLC0415

    found = zone_overlap.cell_overlaps((i * CELL_DEGREES, j * CELL_DEGREES), [unflat(p.rings) for p in parcels],
                                       [unflat(q.rings) for q in pieces])
    return [(parcels[a].pnu, pieces[b].layer_code, pieces[b].zone_feature_id, pieces[b].sigungu_code,
             zone_overlap.square_metres(area, parcels[a].latitude), parcels[a].area_m2) for a, b, area in found]


def with_cells(frame: Any) -> Any:
    """`frame` once per grid cell (`_i`, `_j`) its box touches."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    def span(low: str, high: str) -> Any:
        return F.explode(F.sequence(F.floor(F.col(low) / CELL_DEGREES).cast("bigint"),
                                    F.floor(F.col(high) / CELL_DEGREES).cast("bigint")))

    return frame.withColumn("_i", span("bbox_min_x", "bbox_max_x")).withColumn("_j", span("bbox_min_y", "bbox_max_y"))


def boxes_meet(left: Any, right: Any, keep: tuple[str, ...]) -> Any:
    """The distinct `keep` columns of the pairs of rows of `left` and `right` whose boxes meet (a
    grid-cell equi-join, then the box test). Each frame has the BOX columns."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    other = with_cells(right.select(*BOX)).select(*[F.col(c).alias(f"_r_{c}") for c in BOX], "_i", "_j")
    joined = with_cells(left).join(other, ["_i", "_j"])
    meet = ((F.col("bbox_min_x") <= F.col("_r_bbox_max_x")) & (F.col("_r_bbox_min_x") <= F.col("bbox_max_x"))
            & (F.col("bbox_min_y") <= F.col("_r_bbox_max_y")) & (F.col("_r_bbox_min_y") <= F.col("bbox_max_y")))
    return joined.where(meet).select(*keep).distinct()


def counts(overlap: Any, parcel_area: Any) -> Any:
    """Whether an overlap (m²) of a parcel of `parcel_area` m² puts the parcel in the zone."""
    return (overlap > 0) & ((overlap >= MIN_AREA_M2) | (overlap >= parcel_area * MIN_SHARE))


def gold_rows(frames: dict[str, Any], source_snapshot_id: str, published_at_utc: str,
              columns: tuple[str, ...]) -> Any:
    """One row per served PNU in at least one zone: `regulation_zones_json`, `zone_total`,
    `attached_via` (null)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    shape = F.udf(parcel_shape, f"struct<rings:{RINGS},area_m2:double>")
    cut = F.udf(zone_pieces, PIECES)
    pair = F.udf(cell_pairs, PAIRS)
    parcels = (with_cells(frames[PARCEL_SOURCE].select("pnu", *BOX, shape("geometry_wkb").alias("_shape")))
               .groupBy("_i", "_j").agg(F.collect_list(F.struct(
                   "pnu", F.col("_shape.rings").alias("rings"), F.col("_shape.area_m2").alias("area_m2"),
                   ((F.col("bbox_min_y") + F.col("bbox_max_y")) / 2).alias("latitude"))).alias("_parcels")))
    zones = frames[ZONE_SOURCE]
    pieces = (zones.select(*ZONE_KEY, F.explode(cut("geometry_wkb")).alias("_p"))
              .groupBy(F.col("_p.i").alias("_i"), F.col("_p.j").alias("_j"))
              .agg(F.collect_list(F.struct(*ZONE_KEY, F.col("_p.rings").alias("rings"))).alias("_pieces")))
    # One call per cell, so a piece's edges are taken once for all the cell's parcels.
    pairs = (parcels.join(pieces, ["_i", "_j"]).select(F.explode(pair("_i", "_j", "_parcels", "_pieces")).alias("_m"))
             .select(F.col("_m.pnu").alias("pnu"), F.col("_m.area_m2").alias("_area"),
                     *[F.col(f"_m.{c}").alias(c) for c in ZONE_KEY], F.col("_m.overlap_m2").alias("_overlap")))
    covered = (pairs.groupBy("pnu", "_area", *ZONE_KEY).agg(F.sum("_overlap").alias("_overlap"))
               .where(counts(F.col("_overlap"), F.col("_area"))).select("pnu", *ZONE_KEY))
    # A key may hold several rows of its newest release (keep_ties): one entry per zone, its words
    # the smallest of them so the same rows always give the same entry.
    words = zones.groupBy(*ZONE_KEY).agg(
        F.min("layer_name").alias("layer_name"), F.min("zone_code").alias("zone_code"),
        F.min("zone_name").alias("zone_name"), F.max("source_snapshot_id").alias("_release"))
    release = F.regexp_extract("_release", RELEASE_DATE, 1)
    entries = covered.join(words, list(ZONE_KEY)).select(
        "pnu", F.struct(
            "layer_code", "zone_code", "zone_feature_id",
            F.concat_ws(":", *ZONE_KEY).alias("zone_id"), "layer_name", "zone_name",
            F.when(release != "", F.concat_ws("-", release, F.regexp_extract("_release", RELEASE_DATE, 2),
                                              F.regexp_extract("_release", RELEASE_DATE, 3))).alias("release_date"),
        ).alias("_zone"))
    listed = entries.groupBy("pnu").agg(F.array_sort(F.collect_list("_zone")).alias("_zones"),
                                        F.count(F.lit(1)).alias("zone_total"))
    document = F.struct(F.coalesce(
        F.transform(F.slice("_zones", 1, ZONE_PAGE_BOUND), lambda z: F.struct(*[z[f].alias(f) for f in ZONE_FIELDS])),
        F.expr(f"CAST(array() AS {ZONE_STRUCT})")).alias("zones"))
    content = ("pnu", "regulation_zones_json", "zone_total", "attached_via")
    return (listed.select(
        "pnu", F.to_json(document, WRITTEN).alias("regulation_zones_json"),
        F.col("zone_total").cast("bigint").alias("zone_total"),
        F.lit(None).cast("string").alias("attached_via"),
        F.lit(source_snapshot_id).alias("source_snapshot_id"), F.lit(published_at_utc).alias("published_at_utc"))
        .withColumn("row_digest", F.sha2(F.to_json(F.struct(*content)), 256)).select(*columns))


def restrict_inputs(frames: dict[str, Any], keys: Any) -> dict[str, Any]:
    """The input rows the Gold rows of the PNUs `keys` are built from: their parcels, and the zones
    whose box meets one of theirs."""
    parcels = frames[PARCEL_SOURCE].join(keys, "pnu", "left_semi")
    near = boxes_meet(frames[ZONE_SOURCE], parcels, ZONE_KEY)
    return {PARCEL_SOURCE: parcels, ZONE_SOURCE: frames[ZONE_SOURCE].join(near, list(ZONE_KEY), "left_semi")}


def affected_pnus(changed: list[str], appended: list[str], *, appended_zones: Any, changed_zones: Any,
                  parcel_change: Any, code_change: Any) -> Any:
    """The PNUs whose document can differ from the current Gold's (root ADR-0312 §2): the parcels
    whose box meets a changed zone's old or new box (the zone rows appended since the pin, or after
    any other kind of commit the rows that differ), those that entered, left or changed in the served
    edition, and those under a code a changed code change row names. Each argument is a
    zero-argument callable returning a frame of `pnu`, called only when its input changed."""
    from functools import reduce  # noqa: PLC0415

    parts = []
    if ZONE_SOURCE in appended:
        parts.append(appended_zones())
    elif ZONE_SOURCE in changed:
        parts.append(changed_zones())
    if PARCEL_SOURCE in changed:
        parts.append(parcel_change())
    if CODE_CHANGE_SOURCE in changed:
        parts.append(code_change())
    if not parts:
        raise ValueError(f"affected_pnus was asked about {changed} and none of them maps to PNUs")
    return reduce(lambda a, b: a.unionByName(b), [p.select("pnu") for p in parts]).distinct()


def validate_gold(gold: Any, expected: int | None = None) -> tuple[int, dict[str, int]]:
    """One row per valid PNU, each with its section (the quality gates the contract names)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415

    metrics = {k: int(v or 0) for k, v in gold.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.sum(F.when(~F.col("pnu").rlike(panel.PNU_PATTERN), 1).otherwise(0)).alias("invalid_pnu_count"),
        F.sum(F.when(F.col("regulation_zones_json").isNull(), 1).otherwise(0)).alias("empty_section_count"),
    ).first().asDict().items()}
    duplicates = gold.groupBy("pnu").count().where(F.col("count") > 1).limit(3).count()
    if duplicates or metrics["invalid_pnu_count"] or metrics["empty_section_count"]:
        raise ValueError(f"Gold quality gates failed: {metrics}, {duplicates} duplicated PNUs")
    if expected is not None and metrics["row_count"] != expected:
        raise ValueError(f"Expected {expected} Gold rows, found {metrics['row_count']}")
    return metrics["row_count"], {k: v for k, v in metrics.items() if k != "row_count"}


def read_inputs(spark: Any, args: argparse.Namespace, at: dict[str, str]) -> tuple[dict[str, Any], Any]:
    """The build inputs at `at` (the zones' current rows, the served parcels) as the build columns,
    and the code change table."""
    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415

    zones = panel.read_source(spark, args, ZONE_SOURCE, at)
    parcels = panel.read_served_parcels(spark, args, at, region=False)
    changes = panel.read_source(spark, args, CODE_CHANGE_SOURCE, at)
    return ({ZONE_SOURCE: zones.select(*BUILD_COLUMNS[ZONE_SOURCE]),
             PARCEL_SOURCE: parcels.select(*BUILD_COLUMNS[PARCEL_SOURCE])}, changes)


def main(argv=None) -> int:
    from pyspark.sql import SparkSession  # noqa: PLC0415

    import by_pnu_kinds  # noqa: PLC0415
    import gold_incremental as incremental  # noqa: PLC0415
    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415
    import section_coverage  # noqa: PLC0415
    import zone_overlap  # noqa: PLC0415
    from gold_rebuild import assert_minimum_row_count  # noqa: PLC0415
    from lakehouse_engine import apply_catalog_settings, assert_catalog_env  # noqa: PLC0415
    from lakehouse_snapshot_pins import load_source_snapshot_pins  # noqa: PLC0415
    from platform_contracts import column_names, ensure_contract_table, load_lakehouse_contract  # noqa: PLC0415

    args = parse_args(argv)
    kind = by_pnu_kinds.kind(args.lane)
    if kind.producer != JOB_NAME:
        raise ValueError(f"{args.lane} names producer {kind.producer}, not {JOB_NAME}")
    args.published_at_utc = panel.normalize_utc_timestamp(args.published_at_utc)
    sources, _optional, anchor, whole = lane_inputs(args.lane)
    pins = load_source_snapshot_pins("iceberg", args.source_snapshots_path, sources, anchor, args.iceberg_snapshot_id)
    previous = incremental.validate_arguments(args, sources)
    contract = load_lakehouse_contract(kind.gold_table)
    columns = tuple(column_names(contract))
    if columns != ("pnu", *kind.columns, "attached_via", "row_digest", "source_snapshot_id", "published_at_utc"):
        raise ValueError(f"{kind.gold_table} columns {columns} are not the kind line's {kind.columns}")
    table = f"`{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`.`{kind.gold_table.split('.', 1)[1]}`"
    zone_table = f"{args.iceberg_catalog_name}.{args.source_iceberg_namespace}.{ZONE_SOURCE.split('.', 1)[1]}"
    assert_catalog_env(args.iceberg_catalog_name)
    spark = apply_catalog_settings(SparkSession.builder.appName(f"{JOB_NAME}:{args.lane}")
                                   .config("spark.sql.session.timeZone", "UTC"), args.iceberg_catalog_name).getOrCreate()
    # The executors' Python workers do not have this job's directory on their path (parcel_land, as
    # legal_dong_code_change_pairs ships it).
    spark.sparkContext.addPyFile(zone_overlap.__file__)
    spark.sparkContext.addPyFile(__file__)
    try:
        frames, changes = read_inputs(spark, args, pins)
        frames = {name: frame.cache() for name, frame in frames.items()}
        older: list[Any] = []

        def previous_inputs() -> tuple[dict[str, Any], Any]:
            if not older:
                older.append(read_inputs(spark, args, previous))
            return older[0]

        def zones_reach(rows: Any) -> Any:
            # The served parcels whose box meets the box of `rows` (zone rows, old or new).
            return boxes_meet(frames[PARCEL_SOURCE], rows.select(*BOX), ("pnu",))

        def appended_zones() -> Any:
            # A changed zone's new row and a removal marker (which carries the removed polygon), and
            # the previous current row of each key: the parcels it no longer covers lose it.
            appended = incremental.read_appended(spark, zone_table, previous[ZONE_SOURCE], pins[ZONE_SOURCE])
            keys = appended.select(*ZONE_KEY).distinct()
            before = previous_inputs()[0][ZONE_SOURCE].join(keys, list(ZONE_KEY), "left_semi")
            return zones_reach(appended.select(*BOX).unionByName(before.select(*BOX)))

        def changed_zones() -> Any:
            keys = incremental.changed_rows(previous_inputs()[0][ZONE_SOURCE], frames[ZONE_SOURCE], ZONE_KEY).distinct()
            rows = (previous_inputs()[0][ZONE_SOURCE].select(*ZONE_KEY, *BOX)
                    .unionByName(frames[ZONE_SOURCE].select(*ZONE_KEY, *BOX)))
            return zones_reach(rows.join(keys, list(ZONE_KEY), "left_semi"))

        def code_change() -> Any:
            old_frames, old_changes = previous_inputs()
            parts = panel.code_change_pnus({CODE_CHANGE_SOURCE: old_changes, PARCEL_SOURCE: old_frames[PARCEL_SOURCE]},
                                           {CODE_CHANGE_SOURCE: changes, PARCEL_SOURCE: frames[PARCEL_SOURCE]})
            return incremental.union_keys(parts) if parts else frames[PARCEL_SOURCE].select("pnu").limit(0)

        lane = incremental.Lane(
            build=lambda inputs: gold_rows(inputs, pins[PARCEL_SOURCE], args.published_at_utc, columns),
            restrict=restrict_inputs,
            affected=lambda changed, appended: affected_pnus(
                changed, appended, appended_zones=appended_zones, changed_zones=changed_zones,
                parcel_change=lambda: incremental.changed_rows(previous_inputs()[0][PARCEL_SOURCE],
                                                               frames[PARCEL_SOURCE], ("pnu",)),
                code_change=code_change),
            validate=validate_gold, content_columns=("pnu", *kind.columns, "attached_via"), gold_columns=columns,
            whole_table_inputs=whole, append_inputs={ZONE_SOURCE: zone_table})
        # Root ADR-0272: a 시도 that loses its documents refuses the build.
        coverage = section_coverage.guard_from_arguments(spark, args, table, {SECTION: kind.columns[0]}, changes,
                                                         shares=False)
        outcome = incremental.build_or_merge(spark, args, lane, frames, pins, previous, table, validate_gold,
                                             assert_minimum_row_count, coverage)
        try:
            if not args.validate_only:
                spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`")
                ensure_contract_table(spark, table, contract)
                outcome.commit(spark, table, pins, args.iceberg_write_mode)
                incremental.retain_input_snapshots(spark, args.iceberg_catalog_name, args.source_iceberg_namespace,
                                                   kind.gold_table, pins)
            summary = {
                "schema_version": panel.RUN_SUMMARY_SCHEMA_VERSION, "job_name": JOB_NAME, "contract": kind.gold_table,
                "lane": args.lane, "created_at_utc": panel.normalize_utc_timestamp(None), "row_count": outcome.row_count,
                "write_disposition": "validate_only" if args.validate_only else outcome.disposition(args),
                "quality_metrics": dict(outcome.metrics), "columns": list(columns),
                "source_iceberg_snapshots_by_dataset": dict(sorted(pins.items())), "rebuild": outcome.report(),
            }
        finally:
            outcome.release()
    finally:
        spark.stop()
    panel.emit_run_summary(summary, args.summary_output)
    if args.lineage_output and not args.validate_only:
        with open(args.lineage_output, "w", encoding="utf-8") as lineage:
            json.dump({"job_name": JOB_NAME, "output_dataset": kind.gold_table, "inputs": dict(sorted(pins.items()))},
                      lineage, sort_keys=True)
    print(f"gold-kind-{'validate' if args.validate_only else 'write'}-ok lane={args.lane} rows={outcome.row_count} "
          f"mode={outcome.mode}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
