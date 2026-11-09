#!/usr/bin/env python3
"""Build the parcel building-permit by-PNU kind's Gold from its own Silver (root ADR-0356, ADR-0451).

A kind with its own producer (`by_pnu_kinds.Kind.producer`, root ADR-0287 §9), built from
`silver.building_permit_basis_outline` (건축HUB 건축인허가 기본개요), served on the parcel lane.

What a PNU's document holds (`building_permits`), from the parcel's current rows (one per
관리허가대장PK):

- `records`: the parcel's permits, newest first by the latest of 사용승인일, 실제착공일, 착공연기일,
  착공예정일 and 건축허가일, at most RECORD_BOUND; `building_permit_total` (the Gold column) counts
  them all. Each is the provider's words: 건축구분명, 주용도명, 대지·건축·연면적(㎡) and the five dates
  (hub-building-permit-basis-outline-source-objects.json), and the permit's 주차장 대수 from
  `silver.building_permit_parking` by the same 관리허가대장PK (옥내자주식·옥외자주식·옥내기계식대수,
  root ADR-0402, ADR-0452; null when the parking file has no row for it). 건물명 and 대지위치 are not
  carried (ADR-0356).
- one list per other file of the permit family (`DETAILS`, root ADR-0402, ADR-0463): 동별개요, 층별개요,
  대지위치, 대수선, 가설건축물, 부설주차장, 공작물관리대장, each row on the parcel its own PNU names, and
  the 건축물 정기점검이력, whose rows have no PNU and reach the parcel through the 표제부 at read (the
  ADR-0404 rule, `inspection_pnus`). Each is `{total, records}` in the provider's words, at most
  RECORD_BOUND records, null when the parcel has none. A parcel with any of them has a document.

A row keyed under a code a reorganization renumbered is read under the served code (`renumbered`,
root ADR-0329 §3). Only PNUs the served parcel edition holds have a document (ADR-0292).

Partial by design (root ADR-0312 §2): a run recomputes the PNUs whose current rows differ between
the pinned and the current Silver (`changed_rows`, both directions, so a permit whose PNU moved leaves
its old parcel; the Silver's key is 관리허가대장PK, not the PNU, so its appends alone would not name
the old PNU) and of parcels that entered or left the served edition; a change of the code change
table rebuilds whole (`WHOLE_TABLE_INPUTS`), as every own-producer parcel kind. A changed parking row
recomputes the PNUs its permit is on, before and after (`parking_pnus`). A changed row of a `DETAILS`
file recomputes its PNU before and after (`changed_rows` on that file); a changed 표제부 recomputes the
PNUs whose inspections it moved (the linked inspections compared before and after).
"""
from __future__ import annotations

import argparse
import json
from typing import Any

JOB_NAME = "building_permit_silver_to_gold"
PERMIT_SOURCE = "silver.building_permit_basis_outline"
PARKING_SOURCE = "silver.building_permit_parking"
PARCEL_SOURCE = "silver.parcel_boundaries"
CODE_CHANGE_SOURCE = "reference.legal_dong_code_change"
INSPECTION_SOURCE = "silver.building_maintenance_regular_inspection_history"
TITLE_SOURCE = "silver.building_register_titles"
WHOLE_TABLE_INPUTS = (CODE_CHANGE_SOURCE,)
SECTION = "building_permits"
# Measured 2026-11-09 (vintage 202610): 6,527,521 permits on 3,742,220 PNUs, at most 11,076 on one,
# 99% at most 8, 99.9% at most 22.
RECORD_BOUND = 50
RECORD_FIELDS = ("arch_kind_name", "main_purpose_name", "plot_area_m2", "building_area_m2", "total_floor_area_m2",
                 "permit_date", "start_scheduled_date", "start_delayed_date", "start_actual_date", "use_approval_date")
# Measured 2026-11-09: 2,835,072 parking rows, every 관리허가대장PK distinct (root ADR-0402).
PARKING_FIELDS = ("indoor_self_parking_count", "outdoor_self_parking_count", "indoor_mechanical_parking_count")
DOCUMENT_FIELDS = (*RECORD_FIELDS, *PARKING_FIELDS)
# The permit family's other files (root ADR-0402, ADR-0463): source -> (document key, record fields in the
# provider's words, the field the records are newest first by, or None for the fields' own order).
# Not carried: the keys, 건물명·대지위치·특수지명 (people's names, ADR-0402), 동명 of the inspection.
DETAILS = {
    "silver.building_permit_building_outline": ("building_outlines", (
        "main_annex_name", "main_purpose_name", "structure_name", "roof_name", "building_area_m2",
        "total_floor_area_m2", "far_floor_area_m2", "unit_count", "family_count", "household_count",
        "passenger_elevator_count", "emergency_elevator_count"), None),
    "silver.building_permit_floor_overview": ("floor_overviews", (
        "floor_kind_name", "floor_no", "main_purpose_name", "structure_name", "floor_area_m2", "arch_kind_name"), None),
    "silver.building_permit_site_location": ("site_locations", (
        "representative_flag", "main_building_name", "land_category_name"), None),
    "silver.building_permit_major_repair": ("major_repairs", ("major_repair_kind_name", "major_repair_change_name"),
                                            None),
    "silver.building_permit_temporary_building": ("temporary_buildings", (
        "main_purpose_name", "structure_name", "building_area_m2", "total_floor_area_m2", "plot_area_m2",
        "ground_floor_count", "existence_expiry_date"), "existence_expiry_date"),
    "silver.building_permit_annex_parking": ("annex_parkings", ("land_category_name", "related_jibun_name"), None),
    "silver.building_permit_structure_ledger": ("structures", (
        "facility_kind_name", "structure_name", "length", "height", "area_m2", "building_coverage_ratio",
        "zone_name", "district_name", "precinct_name"), None),
    INSPECTION_SOURCE: ("regular_inspections", ("inspection_agency_name", "inspection_start_date", "processed_date"),
                        "inspection_start_date"),
}
INSPECTION_COLUMNS = ("mgmt_key", "sigungu_cd", "bjdong_cd", "bonbeon", "bubeon", "building_register_key")
RECORD_STRUCT = "array<struct<" + ",".join(f"{f}:string" for f in DOCUMENT_FIELDS) + ">>"
BUILD_COLUMNS = {
    PERMIT_SOURCE: ("pnu", "mgmt_key", *RECORD_FIELDS),
    PARKING_SOURCE: ("mgmt_key", *PARKING_FIELDS),
    PARCEL_SOURCE: ("pnu",),
    **{source: ("pnu", "mgmt_key", *fields) for source, (_, fields, _) in DETAILS.items()},
}
WRITTEN = {"ignoreNullFields": "false"}


def lane_inputs(lane: str) -> tuple[tuple[str, ...], dict[str, str], str, tuple[str, ...]]:
    """(required inputs, optional input -> flag without it, anchor input, whole-table inputs)."""
    del lane
    return ((PARCEL_SOURCE, PERMIT_SOURCE, PARKING_SOURCE, *DETAILS, TITLE_SOURCE, CODE_CHANGE_SOURCE), {},
            PARCEL_SOURCE, WHOLE_TABLE_INPUTS)


def parse_args(argv=None) -> argparse.Namespace:
    """The arguments `scripts/ops/gold-panel-rebuild.sh` gives every producer (root ADR-0193 §7)."""
    import land_ownership_silver_to_gold as ownership  # noqa: PLC0415

    return ownership.parse_args(argv)


def gold_rows(frames: dict[str, Any], source_snapshot_id: str, published_at_utc: str,
              columns: tuple[str, ...]) -> Any:
    """One row per served PNU with a permit or a row of a `DETAILS` file: `building_permits_json`,
    `building_permit_total` (its permits, 0 without), `attached_via` (null)."""
    from pyspark.sql import Window  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415

    def text(name: str) -> Any:
        """The provider's value without padding; an empty one is null (the source's silence)."""
        trimmed = F.trim(F.col(name))
        return F.when(trimmed == "", F.lit(None).cast("string")).otherwise(trimmed)

    def blank(name: str) -> Any:
        return F.coalesce(F.col(name), F.lit(""))

    served = frames[PARCEL_SOURCE].select("pnu").distinct()
    # A permit is its 관리허가대장PK; a tied current row repeated (keep_ties) is one permit here.
    rows = (frames[PERMIT_SOURCE].where(F.col("pnu").isNotNull()).join(served, "pnu", "left_semi")
            .select("pnu", "mgmt_key", *[text(f).alias(f) for f in RECORD_FIELDS]).distinct())
    # One parking row per key; a tied repeat (keep_ties) is folded to its smallest value per field.
    parking = frames[PARKING_SOURCE].groupBy("mgmt_key").agg(*[F.min(text(f)).alias(f) for f in PARKING_FIELDS])
    rows = rows.join(parking, "mgmt_key", "left")
    # Newest first by the latest of its five dates (YYYYMMDD sorts as text); one without a date
    # sorts last; the remaining fields and the key make the order total.
    latest = F.greatest(*[blank(f) for f in ("use_approval_date", "start_actual_date", "start_delayed_date",
                                              "start_scheduled_date", "permit_date")])
    ordered = Window.partitionBy("pnu").orderBy(latest.desc(), *[F.col(f).asc_nulls_first() for f in RECORD_FIELDS],
                                                F.col("mgmt_key").asc_nulls_first())
    ranked = rows.withColumn("_n", F.row_number().over(ordered))
    per_pnu = ranked.groupBy("pnu").agg(
        F.array_sort(F.collect_list(F.when(F.col("_n") <= RECORD_BOUND, F.struct(
            F.col("_n").alias("_order"), *[F.col(f) for f in DOCUMENT_FIELDS]))))
        .alias("_records"),
        F.count(F.lit(1)).alias("building_permit_total"))

    def listed(source: str, key: str, fields: tuple[str, ...], newest: str | None) -> Any:
        """The parcel's rows of one `DETAILS` file as `{total, records}`: a row is its key (a tied
        repeat is one row), newest first by `newest` when the file has one, then by its fields."""
        detail = (frames[source].where(F.col("pnu").isNotNull()).join(served, "pnu", "left_semi")
                  .select("pnu", "mgmt_key", *[text(f).alias(f) for f in fields]).distinct())
        order = [*([blank(newest).desc()] if newest else []), *[F.col(f).asc_nulls_first() for f in fields],
                 F.col("mgmt_key").asc_nulls_first()]
        detail = detail.withColumn("_n", F.row_number().over(Window.partitionBy("pnu").orderBy(*order)))
        kept = F.array_sort(F.collect_list(F.when(F.col("_n") <= RECORD_BOUND, F.struct(
            F.col("_n").alias("_order"), *[F.col(f) for f in fields]))))
        return detail.groupBy("pnu").agg(F.struct(
            F.count(F.lit(1)).cast("bigint").alias("total"),
            F.transform(kept, lambda r: F.struct(*[r[f].alias(f) for f in fields])).alias("records")).alias(key))

    for source, (key, fields, newest) in DETAILS.items():
        per_pnu = per_pnu.join(listed(source, key, fields, newest), "pnu", "full_outer")
    document = F.struct(
        F.coalesce(F.transform("_records", lambda r: F.struct(*[r[f].alias(f) for f in DOCUMENT_FIELDS])),
                   F.expr(f"CAST(array() AS {RECORD_STRUCT})")).alias("records"),
        *[F.col(key) for key, _, _ in DETAILS.values()])
    content = ("pnu", "building_permits_json", "building_permit_total", "attached_via")
    return (per_pnu.select(
        "pnu", F.to_json(document, WRITTEN).alias("building_permits_json"),
        F.coalesce(F.col("building_permit_total"), F.lit(0)).cast("bigint").alias("building_permit_total"),
        F.lit(None).cast("string").alias("attached_via"),
        F.lit(source_snapshot_id).alias("source_snapshot_id"), F.lit(published_at_utc).alias("published_at_utc"))
        .withColumn("row_digest", F.sha2(F.to_json(F.struct(*content)), 256)).select(*columns))


def restrict_inputs(frames: dict[str, Any], keys: Any) -> dict[str, Any]:
    """The input rows the Gold rows of the PNUs `keys` are built from: their own rows, and the parking
    rows of their permits (joined by 관리허가대장PK, not by PNU)."""
    own = {name: frame.join(keys, "pnu", "left_semi") for name, frame in frames.items() if name != PARKING_SOURCE}
    permits = own[PERMIT_SOURCE].select("mgmt_key").distinct()
    return {**own, PARKING_SOURCE: frames[PARKING_SOURCE].join(permits, "mgmt_key", "left_semi")}


def parking_pnus(changed_keys: Any, *permit_frames: Any) -> Any:
    """The PNUs of the permits whose parking rows changed, under every permit frame given (the pinned
    and the current), so a permit that moved PNU recomputes both."""
    from functools import reduce  # noqa: PLC0415

    on = [frame.select("mgmt_key", "pnu").join(changed_keys.select("mgmt_key"), "mgmt_key", "left_semi")
          for frame in permit_frames]
    return reduce(lambda a, b: a.unionByName(b), on).select("pnu").where("pnu IS NOT NULL").distinct()


def affected_pnus(changed: list[str], *, changed_rows: Any, parcel_change: Any, parking_change: Any,
                  detail_change: Any) -> Any:
    """As `land_ownership_silver_to_gold.affected_pnus`, for the 건축인허가 sources. Compared, never read
    as an append: each Silver is keyed by its own 관리…PK, not the PNU, so a row that moved PNU must
    leave the old one. `detail_change(source)` is the changed PNUs of one `DETAILS` file; a changed
    표제부 is a change of the inspections it links."""
    from functools import reduce  # noqa: PLC0415

    parts = []
    if PERMIT_SOURCE in changed:
        parts.append(changed_rows())
    if PARKING_SOURCE in changed:
        parts.append(parking_change())
    if PARCEL_SOURCE in changed:
        parts.append(parcel_change())
    details = {INSPECTION_SOURCE if name == TITLE_SOURCE else name for name in changed}
    parts.extend(detail_change(name) for name in DETAILS if name in details)
    if not parts:
        raise ValueError(f"affected_pnus was asked about {changed} and none of them maps to PNUs")
    return reduce(lambda a, b: a.unionByName(b), [p.select("pnu") for p in parts]).distinct()


def inspection_pnus(inspections: Any, titles: Any) -> Any:
    """The 정기점검이력 rows on the parcel of the one 표제부 their 관리건축물대장PK names (root ADR-0404 §2):
    `S-N` names the 표제부 whose key is four characters, `1`, then N, and whose PNU starts with S or has
    this row's 법정동·번·지. A row that names no 표제부, or more than one, reaches no parcel. Read every
    run from the 표제부's current rows, never fixed at load: a 표제부 whose PNU changed moves its
    inspections with it."""
    from pyspark.sql import Window  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415

    key = F.trim(F.col("building_register_key"))
    rows = (inspections.withColumn("_s", F.substring_index(key, "-", 1))
            .withColumn("_n", F.expr("substring(trim(building_register_key), instr(trim(building_register_key), '-') + 1)"))
            .where(F.instr(key, "-") > 1))
    register = (titles.where(F.col("pnu").isNotNull() & (F.length("mgm_bldrgst_pk") > 5))
                .select(F.expr("substring(mgm_bldrgst_pk, 5)").alias("_suffix"), F.col("pnu").alias("_title_pnu")))
    joined = rows.join(register, F.concat(F.lit("1"), F.col("_n")) == F.col("_suffix"))
    title = F.col("_title_pnu")
    matched = joined.where((F.substring(title, 1, 5) == F.col("_s")) | (
        (F.substring(title, 6, 5) == F.trim("bjdong_cd")) & (F.substring(title, 12, 4) == F.trim("bonbeon"))
        & (F.substring(title, 16, 4) == F.trim("bubeon"))))
    one = matched.withColumn("_matches", F.count(F.lit(1)).over(Window.partitionBy("mgmt_key"))).where("_matches = 1")
    return one.select(title.alias("pnu"), *inspections.columns)


def validate_gold(gold: Any, expected: int | None = None) -> tuple[int, dict[str, int]]:
    """One row per valid PNU, each with its section (the quality gates the contract names)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415

    metrics = {k: int(v or 0) for k, v in gold.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.sum(F.when(~F.col("pnu").rlike(panel.PNU_PATTERN), 1).otherwise(0)).alias("invalid_pnu_count"),
        F.sum(F.when(F.col("building_permits_json").isNull(), 1).otherwise(0)).alias("empty_section_count"),
    ).first().asDict().items()}
    duplicates = gold.groupBy("pnu").count().where(F.col("count") > 1).limit(3).count()
    if duplicates or metrics["invalid_pnu_count"] or metrics["empty_section_count"]:
        raise ValueError(f"Gold quality gates failed: {metrics}, {duplicates} duplicated PNUs")
    if expected is not None and metrics["row_count"] != expected:
        raise ValueError(f"Expected {expected} Gold rows, found {metrics['row_count']}")
    return metrics["row_count"], {k: v for k, v in metrics.items() if k != "row_count"}


def read_inputs(spark: Any, args: argparse.Namespace, at: dict[str, str], counters: dict[str, int]
                ) -> tuple[dict[str, Any], Any]:
    """The build inputs at `at` (the current rows re-keyed to the served codes, the served parcels) and
    the code change table. Every file with a PNU is re-keyed as the permits are (root ADR-0329 §3); the
    inspections take the 표제부's PNU and are re-keyed after."""
    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415

    changes, renumbering = panel.read_renumbering(spark, args, at, counters)
    rows = panel.renumbered(panel.read_source(spark, args, PERMIT_SOURCE, at), renumbering)
    parcels = panel.read_served_parcels(spark, args, at, region=False)
    parking = panel.read_source(spark, args, PARKING_SOURCE, at)
    details = {source: panel.renumbered(panel.read_source(spark, args, source, at), renumbering)
               for source in DETAILS if source != INSPECTION_SOURCE}
    inspections = panel.read_source(spark, args, INSPECTION_SOURCE, at).select(
        *INSPECTION_COLUMNS, *DETAILS[INSPECTION_SOURCE][1])
    counters["regular_inspection_rows"] = inspections.select("mgmt_key").distinct().count()
    titles = panel.read_source(spark, args, TITLE_SOURCE, at).select("mgm_bldrgst_pk", "pnu")
    details[INSPECTION_SOURCE] = panel.renumbered(inspection_pnus(inspections, titles), renumbering)
    return ({PERMIT_SOURCE: rows.select(*BUILD_COLUMNS[PERMIT_SOURCE]),
             PARKING_SOURCE: parking.select(*BUILD_COLUMNS[PARKING_SOURCE]),
             **{source: frame.select(*BUILD_COLUMNS[source]) for source, frame in details.items()},
             PARCEL_SOURCE: parcels.select(*BUILD_COLUMNS[PARCEL_SOURCE])}, changes)


def main(argv=None) -> int:
    from pyspark.sql import SparkSession  # noqa: PLC0415

    import by_pnu_kinds  # noqa: PLC0415
    import gold_incremental as incremental  # noqa: PLC0415
    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415
    import section_coverage  # noqa: PLC0415
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
    assert_catalog_env(args.iceberg_catalog_name)
    spark = apply_catalog_settings(SparkSession.builder.appName(f"{JOB_NAME}:{args.lane}")
                                   .config("spark.sql.session.timeZone", "UTC"), args.iceberg_catalog_name).getOrCreate()
    counters: dict[str, int] = {}
    try:
        frames, changes = read_inputs(spark, args, pins, counters)
        frames = {name: frame.cache() for name, frame in frames.items()}
        # Root ADR-0404: the 표제부 link is an observed rule, not a provider table; its rate is counted every run.
        counters["regular_inspection_linked"] = frames[INSPECTION_SOURCE].select("mgmt_key").distinct().count()
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
                changed_rows=lambda: incremental.changed_rows(previous_frames()[PERMIT_SOURCE],
                                                              frames[PERMIT_SOURCE], ("pnu",)),
                parcel_change=lambda: incremental.changed_rows(previous_frames()[PARCEL_SOURCE],
                                                               frames[PARCEL_SOURCE], ("pnu",)),
                parking_change=lambda: parking_pnus(
                    incremental.changed_rows(previous_frames()[PARKING_SOURCE], frames[PARKING_SOURCE], ("mgmt_key",)),
                    previous_frames()[PERMIT_SOURCE], frames[PERMIT_SOURCE]),
                detail_change=lambda source: incremental.changed_rows(previous_frames()[source], frames[source],
                                                                      ("pnu",))),
            validate=validate_gold, content_columns=("pnu", *kind.columns, "attached_via"), gold_columns=columns,
            whole_table_inputs=whole)
        # Root ADR-0272: the 건축인허가 기본개요 is a register, so a 시도 that loses its documents refuses the build.
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
