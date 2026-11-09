#!/usr/bin/env python3
"""Build the parcel nearby by-PNU kind's Gold: what is near each served parcel (root ADR-0465).

A kind with its own producer (`by_pnu_kinds.Kind.producer`, root ADR-0287 §9), served on the parcel
lane. A PNU's document (`nearby`), from the parcel's centroid (Sedona `ST_Centroid` of its boundary):

- the nearest 공항, 철도역, 도시철도역, 초중등학교 and 대학 (`POINTS`, `UNIVERSITY`): the provider's name
  words and the distance in metres on the WGS84 spheroid, found by Apache Sedona's exact
  nearest-neighbour join (`ST_KNN`, k = 1, spheroid distance). A 대학 stands at the centroid of the
  parcel its address resolved to (the file gives no coordinates);
- `commercial_store_count`: the 상가(상권) stores whose coordinates fall in the H3 cells (resolution
  STORE_H3_RESOLUTION) within STORE_H3_RING rings of the parcel's cell (Sedona `ST_H3CellIDs`,
  `ST_H3KRing`), about STORE_RADIUS_M around it;
- `highway_access_points`: the 고속도로 출입시설 of the parcel's 시군구, by the 법정동 code the file's
  names resolved to (the file has neither coordinates nor a lot; root ADR-0401 §4), at most
  RECORD_BOUND, and their total.

A coordinate outside KOREA_BOUNDS or not a number is the source's error, not a place: that row is not a
candidate. A code under a reorganization is read under the served code (`renumbered`, root ADR-0329 §3).

Partial by design (root ADR-0312 §2): a changed store recomputes only the parcels whose cell is within
the rings of the cells it left or entered (`store_pnus`); a parcel that entered, left or changed its
boundary recomputes itself. A change of a facility file, whose nearest point any parcel may be, rebuilds
whole and publishes only the documents whose digest changed (`WHOLE_TABLE_INPUTS`, root ADR-0321 §5):
those files hold 14 to 12,004 rows and change a few times a year.
"""
from __future__ import annotations

import argparse
import json
from typing import Any

JOB_NAME = "parcel_nearby_silver_to_gold"
PARCEL_SOURCE = "silver.parcel_boundaries"
AIRPORT_SOURCE = "silver.airport"
RAILWAY_SOURCE = "silver.railway_station"
SUBWAY_SOURCE = "silver.subway_station"
SCHOOL_SOURCE = "silver.school_location"
UNIVERSITY_SOURCE = "silver.university"
STORE_SOURCE = "silver.commercial_district"
HIGHWAY_SOURCE = "silver.highway_access_point"
CODE_CHANGE_SOURCE = "reference.legal_dong_code_change"
SECTION = "nearby"
# Point files: source -> (document key, latitude header, longitude header, {document field: header}),
# headers as data-go-kr-file-data.contract.json names them.
POINTS = {
    AIRPORT_SOURCE: ("airport", "위도(WGS84좌표)", "경도(WGS84좌표)", {"name": "공항명"}),
    RAILWAY_SOURCE: ("railway_station", "위도좌표", "경도좌표", {"name": "역이름"}),
    SUBWAY_SOURCE: ("subway_station", "LATITUDE", "LONGITUDE", {"name": "STATN_NM", "route_name": "ROUTE_NM"}),
    SCHOOL_SOURCE: ("school", "LATITUDE", "LONGITUDE", {"name": "SCHOOL_NM", "school_level": "SCHOOL_SE"}),
}
UNIVERSITY = ("university", {"name": "SCHL_NM", "school_kind": "SCHL_SE_NM"})
HIGHWAY_FIELDS = {"route_name": "노선명", "facility_name": "출입시설"}
STORE_LATITUDE, STORE_LONGITUDE = "위도", "경도"
WHOLE_TABLE_INPUTS = (*POINTS, UNIVERSITY_SOURCE, HIGHWAY_SOURCE, CODE_CHANGE_SOURCE)
# Resolution 9 cells average 0.105 km² (edge about 174 m, h3geo.org); two rings span about 5 cell edges.
STORE_H3_RESOLUTION = 9
STORE_H3_RING = 2
STORE_RADIUS_M = 500
RECORD_BOUND = 20
# The mainland, Jeju, Ulleung and Dokdo, with margin (degrees).
KOREA_BOUNDS = {"lat": (32.5, 39.0), "lon": (124.0, 132.0)}  # public-repository-safety: reviewed-runtime-coordinate
# Every nearest-point file: source -> (document key, document fields).
NEAREST = {**{source: (key, tuple(fields)) for source, (key, _, _, fields) in POINTS.items()},
           UNIVERSITY_SOURCE: (UNIVERSITY[0], tuple(UNIVERSITY[1]))}
WRITTEN = {"ignoreNullFields": "false"}


def lane_inputs(lane: str) -> tuple[tuple[str, ...], dict[str, str], str, tuple[str, ...]]:
    """(required inputs, optional input -> flag without it, anchor input, whole-table inputs)."""
    del lane
    return ((PARCEL_SOURCE, *POINTS, UNIVERSITY_SOURCE, STORE_SOURCE, HIGHWAY_SOURCE, CODE_CHANGE_SOURCE), {},
            PARCEL_SOURCE, WHOLE_TABLE_INPUTS)


def parse_args(argv=None) -> argparse.Namespace:
    """The arguments `scripts/ops/gold-panel-rebuild.sh` gives every producer (root ADR-0193 §7)."""
    import land_ownership_silver_to_gold as ownership  # noqa: PLC0415

    return ownership.parse_args(argv)


def located(frame: Any, latitude: str, longitude: str) -> Any:
    """`frame` (with `latitude`, `longitude` strings) as Sedona points, the rows whose coordinates are numbers
    inside KOREA_BOUNDS only."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    lat, lon = F.col(latitude).cast("double"), F.col(longitude).cast("double")
    inside = lat.between(*KOREA_BOUNDS["lat"]) & lon.between(*KOREA_BOUNDS["lon"])
    return frame.where(inside).withColumn("geom", F.expr(f"ST_Point(CAST({longitude} AS DOUBLE), "
                                                         f"CAST({latitude} AS DOUBLE))"))


def parcel_points(parcels: Any) -> Any:
    """(pnu, geom, cell): each parcel's boundary centroid and its H3 cell at STORE_H3_RESOLUTION."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    return (parcels.where(F.col("geometry_wkb").isNotNull())
            .select("pnu", F.expr("ST_Centroid(ST_GeomFromWKB(geometry_wkb))").alias("geom"))
            .withColumn("cell", F.expr(f"ST_H3CellIDs(geom, {STORE_H3_RESOLUTION}, false)[0]")))


def store_cells(stores: Any) -> Any:
    """(row_key, cell): each located store's H3 cell."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    return (located(stores, "lat", "lon")
            .select("row_key", F.expr(f"ST_H3CellIDs(geom, {STORE_H3_RESOLUTION}, false)[0]").alias("cell")))


def nearest(points: Any, facilities: Any, key: str, fields: tuple[str, ...]) -> Any:
    """(pnu, `key`): each parcel's nearest facility (Sedona ST_KNN, spheroid), its `fields` and
    `distance_m`; a tie is broken by the fields and the key, so the answer is one and stable."""
    from pyspark.sql import Window  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415

    spark = points.sparkSession
    points.select("pnu", "geom").createOrReplaceTempView("_nearby_parcels")
    facilities.select("row_key", "geom", *fields).createOrReplaceTempView("_nearby_facilities")
    pairs = spark.sql(
        "SELECT p.pnu, f.row_key, " + "".join(f"f.{f}, " for f in fields)
        + "CAST(ROUND(ST_DistanceSpheroid(p.geom, f.geom)) AS BIGINT) AS distance_m "
        "FROM _nearby_parcels p JOIN _nearby_facilities f ON ST_KNN(p.geom, f.geom, 1, true)")
    order = Window.partitionBy("pnu").orderBy("distance_m", *fields, "row_key")
    one = pairs.withColumn("_n", F.row_number().over(order)).where("_n = 1")
    return one.select("pnu", F.struct(*fields, "distance_m").alias(key))


def gold_rows(frames: dict[str, Any], source_snapshot_id: str, published_at_utc: str,
              columns: tuple[str, ...]) -> Any:
    """One row per served parcel with a centroid: `nearby_json`, `attached_via` (null)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    points = parcel_points(frames[PARCEL_SOURCE]).cache()
    document = points.select("pnu", "cell")
    for source, (key, fields) in NEAREST.items():
        document = document.join(nearest(points, located(frames[source], "lat", "lon"), key, fields), "pnu", "left")

    stores = store_cells(frames[STORE_SOURCE]).groupBy("cell").agg(F.count(F.lit(1)).alias("_stores"))
    ring = (points.select("pnu", F.explode(F.expr(f"ST_H3KRing(cell, {STORE_H3_RING}, false)")).alias("cell"))
            .join(F.broadcast(stores), "cell").groupBy("pnu").agg(F.sum("_stores").alias("_store_count")))
    document = document.join(ring, "pnu", "left")

    highway = (frames[HIGHWAY_SOURCE].where(F.col("sigungu").isNotNull())
               .select("sigungu", *HIGHWAY_FIELDS).distinct())
    listed = highway.groupBy("sigungu").agg(
        F.count(F.lit(1)).cast("bigint").alias("total"),
        F.slice(F.array_sort(F.collect_list(F.struct(*HIGHWAY_FIELDS))), 1, RECORD_BOUND).alias("records"))
    listed = listed.select("sigungu", F.struct("total", "records").alias("highway_access_points"))
    document = (document.withColumn("sigungu", F.substring("pnu", 1, 5))
                .join(F.broadcast(listed), "sigungu", "left"))

    section = F.struct(*[F.col(key) for key, _ in NEAREST.values()],
                       F.coalesce(F.col("_store_count"), F.lit(0)).cast("bigint").alias("commercial_store_count"),
                       F.col("highway_access_points"))
    content = ("pnu", "nearby_json", "attached_via")
    return (document.select(
        "pnu", F.to_json(section, WRITTEN).alias("nearby_json"), F.lit(None).cast("string").alias("attached_via"),
        F.lit(source_snapshot_id).alias("source_snapshot_id"), F.lit(published_at_utc).alias("published_at_utc"))
        .withColumn("row_digest", F.sha2(F.to_json(F.struct(*content)), 256)).select(*columns))


def restrict_inputs(frames: dict[str, Any], keys: Any) -> dict[str, Any]:
    """The inputs the Gold rows of the PNUs `keys` are built from: their own parcels, and every
    facility and store (any of them may be nearest, or in a parcel's rings)."""
    return {name: frame.join(keys, "pnu", "left_semi") if name == PARCEL_SOURCE else frame
            for name, frame in frames.items()}


def store_pnus(changed_cells: Any, points: Any) -> Any:
    """The parcels whose rings hold a cell a changed store left or entered."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    touched = (changed_cells.where(F.col("cell").isNotNull())
               .select(F.explode(F.expr(f"ST_H3KRing(cell, {STORE_H3_RING}, false)")).alias("cell")).distinct())
    return points.join(touched, "cell", "left_semi").select("pnu").distinct()


def affected_pnus(changed: list[str], *, parcel_change: Any, store_change: Any) -> Any:
    """As `land_ownership_silver_to_gold.affected_pnus`: a changed parcel recomputes itself, a changed
    store the parcels within its rings. Every other input rebuilds whole (WHOLE_TABLE_INPUTS)."""
    from functools import reduce  # noqa: PLC0415

    parts = []
    if PARCEL_SOURCE in changed:
        parts.append(parcel_change())
    if STORE_SOURCE in changed:
        parts.append(store_change())
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
        F.sum(F.when(F.col("nearby_json").isNull(), 1).otherwise(0)).alias("empty_section_count"),
    ).first().asDict().items()}
    duplicates = gold.groupBy("pnu").count().where(F.col("count") > 1).limit(3).count()
    if duplicates or metrics["invalid_pnu_count"] or metrics["empty_section_count"]:
        raise ValueError(f"Gold quality gates failed: {metrics}, {duplicates} duplicated PNUs")
    if expected is not None and metrics["row_count"] != expected:
        raise ValueError(f"Expected {expected} Gold rows, found {metrics['row_count']}")
    return metrics["row_count"], {k: v for k, v in metrics.items() if k != "row_count"}


def read_inputs(spark: Any, args: argparse.Namespace, at: dict[str, str], counters: dict[str, int]
                ) -> tuple[dict[str, Any], Any]:
    """The build inputs at `at`, each reduced to what the build reads, and the code change table. The
    files' PNUs and 법정동 codes are re-keyed to the served codes as every parcel kind's are."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    import data_go_kr_file_data as files  # noqa: PLC0415
    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415

    changes, renumbering = panel.read_renumbering(spark, args, at, counters)
    frames = {PARCEL_SOURCE: panel.read_served_parcels(spark, args, at, region=False).select("pnu", "geometry_wkb")}
    for source, (_, latitude, longitude, fields) in POINTS.items():
        frames[source] = panel.read_source(spark, args, source, at).select(
            "row_key", files.field(latitude).alias("lat"), files.field(longitude).alias("lon"),
            *[files.field(header).alias(name) for name, header in fields.items()])
    # A 대학 stands at the centroid of its parcel, read here from every served parcel: a partial build's
    # parcels are only the affected ones. shortcut: a 대학 whose parcel changed shape moves only at the
    # next whole build (a facility file change), not with the parcel; a few metres on one of 1,995 rows.
    _, university_fields = UNIVERSITY
    universities = panel.renumbered(panel.read_source(spark, args, UNIVERSITY_SOURCE, at)
                                    .where(F.col("pnu").isNotNull()), renumbering)
    sites = parcel_points(frames[PARCEL_SOURCE].join(universities.select("pnu").distinct(), "pnu", "left_semi"))
    frames[UNIVERSITY_SOURCE] = universities.join(sites, "pnu").select(
        "row_key", F.expr("CAST(ST_Y(geom) AS STRING)").alias("lat"), F.expr("CAST(ST_X(geom) AS STRING)").alias("lon"),
        *[files.field(header).alias(name) for name, header in university_fields.items()])
    frames[STORE_SOURCE] = panel.read_source(spark, args, STORE_SOURCE, at).select(
        "row_key", files.field(STORE_LATITUDE).alias("lat"), files.field(STORE_LONGITUDE).alias("lon"))
    # A 법정동 code read as a PNU's first ten digits, so the one renumbering serves it.
    highway = (panel.read_source(spark, args, HIGHWAY_SOURCE, at).where(F.col("legal_dong_code").isNotNull())
               .withColumn("pnu", F.concat(F.col("legal_dong_code"), F.lit("0" * 9))))
    frames[HIGHWAY_SOURCE] = panel.renumbered(highway, renumbering).select(
        "row_key", F.substring("pnu", 1, 5).alias("sigungu"),
        *[files.field(header).alias(name) for name, header in HIGHWAY_FIELDS.items()])
    return frames, changes


def main(argv=None) -> int:
    from pyspark.sql import SparkSession  # noqa: PLC0415

    import by_pnu_kinds  # noqa: PLC0415
    import gold_incremental as incremental  # noqa: PLC0415
    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415
    import section_coverage  # noqa: PLC0415
    from gold_rebuild import assert_minimum_row_count  # noqa: PLC0415
    from lakehouse_engine import apply_catalog_settings, assert_catalog_env, with_sedona  # noqa: PLC0415
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
    assert_catalog_env(args.iceberg_catalog_name)
    builder = apply_catalog_settings(SparkSession.builder.appName(f"{JOB_NAME}:{args.lane}")
                                     .config("spark.sql.session.timeZone", "UTC"), args.iceberg_catalog_name)
    spark = with_sedona(builder).getOrCreate()
    counters: dict[str, int] = {}
    try:
        frames, changes = read_inputs(spark, args, pins, counters)
        frames = {name: frame.cache() for name, frame in frames.items()}
        older: dict[str, Any] = {}

        def previous_frames() -> dict[str, Any]:
            if not older:
                older.update(read_inputs(spark, args, previous, {})[0])
            return older

        lane = incremental.Lane(
            build=lambda inputs: gold_rows(inputs, pins[PARCEL_SOURCE], args.published_at_utc, columns),
            restrict=restrict_inputs,
            affected=lambda changed, _appended: affected_pnus(
                changed,
                parcel_change=lambda: incremental.changed_rows(previous_frames()[PARCEL_SOURCE],
                                                               frames[PARCEL_SOURCE], ("pnu",)),
                store_change=lambda: store_pnus(
                    incremental.changed_rows(store_cells(previous_frames()[STORE_SOURCE]),
                                             store_cells(frames[STORE_SOURCE]), ("cell",)),
                    parcel_points(frames[PARCEL_SOURCE]))),
            validate=validate_gold, content_columns=("pnu", *kind.columns, "attached_via"), gold_columns=columns,
            whole_table_inputs=whole)
        # Root ADR-0272: every served parcel has the section, so a 시도 that loses documents refuses the build.
        coverage = section_coverage.guard_from_arguments(spark, args, table, {SECTION: kind.columns[0]}, changes,
                                                         shares=False)
        outcome = incremental.build_or_merge(spark, args, lane, frames, pins, previous, table, validate_gold,
                                             assert_minimum_row_count, coverage)
        try:
            if not args.validate_only:
                spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`")
                ensure_contract_table(spark, table, contract)
                outcome.commit(spark, table, pins, args.iceberg_write_mode, contract_table=contract["table_name"])
                incremental.retain_input_snapshots(spark, args.iceberg_catalog_name, args.source_iceberg_namespace,
                                                   kind.gold_table, pins)
            summary = {
                "schema_version": panel.RUN_SUMMARY_SCHEMA_VERSION, "job_name": JOB_NAME, "contract": kind.gold_table,
                "lane": args.lane, "created_at_utc": panel.normalize_utc_timestamp(None), "row_count": outcome.row_count,
                "write_disposition": "validate_only" if args.validate_only else outcome.disposition(args),
                "quality_metrics": {**outcome.metrics, **counters}, "columns": list(columns),
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
