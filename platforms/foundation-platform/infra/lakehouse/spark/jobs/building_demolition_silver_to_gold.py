#!/usr/bin/env python3
"""Build the parcel demolition-history by-PNU kind's Gold from its own Silver (root ADR-0397, ADR-0399).

A kind with its own producer (`by_pnu_kinds.Kind.producer`, root ADR-0287 §9), built from
`silver.building_permit_demolition_loss` (건축인허가 철거멸실관리대장), served on the parcel lane.

What a PNU's document holds (`demolitions`), from the parcel's current rows:

- `records`: the parcel's distinct 철거·멸실 reports, newest first by the latest of 철거멸실일,
  철거종료일 and 철거시작일, at most RECORD_BOUND; `demolition_total` (the Gold column) counts them all. Each is the
  provider's words: 철거멸실구분명, the three dates, 연면적(㎡), 건축물수, 주용도명, 구조명
  (data.go.kr 15044683). The provider repeats an identical row up to 47 times (2026-10); a repeat is
  one report here.

A row keyed under a code a reorganization renumbered is read under the served code (`renumbered`,
root ADR-0329 §3). Only PNUs the served parcel edition holds have a document (ADR-0292). The lineage
is not followed in this first version: a report on a parcel merged or split since stays under its old
PNU (measured: 78.3% of a 2,000-row sample is a served PNU, ADR-0397), see ADR-0399 §3.

Partial by design (root ADR-0312 §2): the PNUs a run recomputes are those of the rows appended since
the current Gold's pin and of parcels that entered or left the served edition; a change of the code
change table rebuilds whole (`WHOLE_TABLE_INPUTS`), as every own-producer parcel kind.
"""
from __future__ import annotations

import argparse
import json
from typing import Any

JOB_NAME = "building_demolition_silver_to_gold"
DEMOLITION_SOURCE = "silver.building_permit_demolition_loss"
PARCEL_SOURCE = "silver.parcel_boundaries"
CODE_CHANGE_SOURCE = "reference.legal_dong_code_change"
WHOLE_TABLE_INPUTS = (CODE_CHANGE_SOURCE,)
SECTION = "demolitions"
# Measured 2026-10: 425,040 parcels, at most 194 reports on one, 99% at most 3.
RECORD_BOUND = 50
RECORD_FIELDS = ("demolition_kind_name", "demolition_start_date", "demolition_end_date", "demolition_loss_date",
                 "total_floor_area_m2", "building_count", "main_purpose_name", "structure_name")
RECORD_STRUCT = "array<struct<" + ",".join(f"{f}:string" for f in RECORD_FIELDS) + ">>"
BUILD_COLUMNS = {
    DEMOLITION_SOURCE: ("pnu", *RECORD_FIELDS),
    PARCEL_SOURCE: ("pnu",),
}
WRITTEN = {"ignoreNullFields": "false"}


def lane_inputs(lane: str) -> tuple[tuple[str, ...], dict[str, str], str, tuple[str, ...]]:
    """(required inputs, optional input -> flag without it, anchor input, whole-table inputs)."""
    del lane
    return (PARCEL_SOURCE, DEMOLITION_SOURCE, CODE_CHANGE_SOURCE), {}, PARCEL_SOURCE, WHOLE_TABLE_INPUTS


def parse_args(argv=None) -> argparse.Namespace:
    """The arguments `scripts/ops/gold-panel-rebuild.sh` gives every producer (root ADR-0193 §7)."""
    import land_ownership_silver_to_gold as ownership  # noqa: PLC0415

    return ownership.parse_args(argv)


def gold_rows(frames: dict[str, Any], source_snapshot_id: str, published_at_utc: str,
              columns: tuple[str, ...]) -> Any:
    """One row per served PNU with a report: `demolitions_json`, `demolition_total`, `attached_via` (null)."""
    from pyspark.sql import Window  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415

    served = frames[PARCEL_SOURCE].select("pnu").distinct()
    rows = (frames[DEMOLITION_SOURCE].where(F.col("pnu").isNotNull()).join(served, "pnu", "left_semi")
            .select("pnu", *RECORD_FIELDS).distinct())

    def text(name: str) -> Any:
        """The provider's value without padding; an empty one is null (the source's silence)."""
        trimmed = F.trim(F.col(name))
        return F.when(trimmed == "", F.lit(None).cast("string")).otherwise(trimmed)

    def blank(name: str) -> Any:
        return F.coalesce(F.trim(F.col(name)), F.lit(""))

    # Newest first by the latest of its three dates (YYYYMMDD sorts as text); a report without a date
    # sorts last; the remaining fields make the order total.
    latest = F.greatest(blank("demolition_loss_date"), blank("demolition_end_date"), blank("demolition_start_date"))
    newest = Window.partitionBy("pnu").orderBy(latest.desc(), *[F.col(f).asc_nulls_first() for f in RECORD_FIELDS])
    ranked = rows.withColumn("_n", F.row_number().over(newest))
    per_pnu = ranked.groupBy("pnu").agg(
        F.array_sort(F.collect_list(F.when(F.col("_n") <= RECORD_BOUND, F.struct(
            F.col("_n").alias("_order"), *[text(f).alias(f) for f in RECORD_FIELDS]))))
        .alias("_records"),
        F.count(F.lit(1)).alias("demolition_total"))
    document = F.struct(
        F.coalesce(F.transform("_records", lambda r: F.struct(*[r[f].alias(f) for f in RECORD_FIELDS])),
                   F.expr(f"CAST(array() AS {RECORD_STRUCT})")).alias("records"))
    content = ("pnu", "demolitions_json", "demolition_total", "attached_via")
    return (per_pnu.select(
        "pnu", F.to_json(document, WRITTEN).alias("demolitions_json"),
        F.col("demolition_total").cast("bigint").alias("demolition_total"),
        F.lit(None).cast("string").alias("attached_via"),
        F.lit(source_snapshot_id).alias("source_snapshot_id"), F.lit(published_at_utc).alias("published_at_utc"))
        .withColumn("row_digest", F.sha2(F.to_json(F.struct(*content)), 256)).select(*columns))


def restrict_inputs(frames: dict[str, Any], keys: Any) -> dict[str, Any]:
    """The input rows the Gold rows of the PNUs `keys` are built from: their own rows only."""
    return {name: frame.join(keys, "pnu", "left_semi") for name, frame in frames.items()}


def affected_pnus(changed: list[str], appended: list[str], *, appended_rows: Any, changed_rows: Any,
                  parcel_change: Any) -> Any:
    """As `land_ownership_silver_to_gold.affected_pnus`, for the one demolition source."""
    from functools import reduce  # noqa: PLC0415

    parts = []
    if DEMOLITION_SOURCE in appended:
        parts.append(appended_rows())
    elif DEMOLITION_SOURCE in changed:
        parts.append(changed_rows())
    if PARCEL_SOURCE in changed:
        parts.append(parcel_change())
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
        F.sum(F.when(F.col("demolitions_json").isNull(), 1).otherwise(0)).alias("empty_section_count"),
    ).first().asDict().items()}
    duplicates = gold.groupBy("pnu").count().where(F.col("count") > 1).limit(3).count()
    if duplicates or metrics["invalid_pnu_count"] or metrics["empty_section_count"]:
        raise ValueError(f"Gold quality gates failed: {metrics}, {duplicates} duplicated PNUs")
    if expected is not None and metrics["row_count"] != expected:
        raise ValueError(f"Expected {expected} Gold rows, found {metrics['row_count']}")
    return metrics["row_count"], {k: v for k, v in metrics.items() if k != "row_count"}


def read_inputs(spark: Any, args: argparse.Namespace, at: dict[str, str], counters: dict[str, int]
                ) -> tuple[dict[str, Any], Any]:
    """The build inputs at `at` (rows re-keyed to the served codes, the served parcels) and the code
    change table."""
    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415

    changes, renumbering = panel.read_renumbering(spark, args, at, counters)
    rows = panel.renumbered(panel.read_source(spark, args, DEMOLITION_SOURCE, at), renumbering)
    parcels = panel.read_served_parcels(spark, args, at, region=False)
    return ({DEMOLITION_SOURCE: rows.select(*BUILD_COLUMNS[DEMOLITION_SOURCE]),
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
    source_table = f"{args.iceberg_catalog_name}.{args.source_iceberg_namespace}.{DEMOLITION_SOURCE.split('.', 1)[1]}"
    assert_catalog_env(args.iceberg_catalog_name)
    spark = apply_catalog_settings(SparkSession.builder.appName(f"{JOB_NAME}:{args.lane}")
                                   .config("spark.sql.session.timeZone", "UTC"), args.iceberg_catalog_name).getOrCreate()
    counters: dict[str, int] = {}
    try:
        frames, changes = read_inputs(spark, args, pins, counters)
        frames = {name: frame.cache() for name, frame in frames.items()}
        older: dict[str, Any] = {}

        def previous_frames() -> dict[str, Any]:
            if not older:
                older.update(read_inputs(spark, args, previous, {})[0])
            return older

        def appended_rows() -> Any:
            # Removal markers too: a parcel whose reports vanished must lose its document.
            _changes, renumbering = panel.read_renumbering(spark, args, pins, {})
            appended = incremental.read_appended(spark, source_table, previous[DEMOLITION_SOURCE], pins[DEMOLITION_SOURCE])
            return panel.renumbered(appended.select("pnu"), renumbering)

        lane = incremental.Lane(
            build=lambda inputs: gold_rows(inputs, pins[PARCEL_SOURCE], args.published_at_utc, columns),
            restrict=restrict_inputs,
            affected=lambda changed, appended: affected_pnus(
                changed, appended, appended_rows=appended_rows,
                changed_rows=lambda: incremental.changed_rows(previous_frames()[DEMOLITION_SOURCE],
                                                              frames[DEMOLITION_SOURCE], ("pnu",)),
                parcel_change=lambda: incremental.changed_rows(previous_frames()[PARCEL_SOURCE],
                                                               frames[PARCEL_SOURCE], ("pnu",))),
            validate=validate_gold, content_columns=("pnu", *kind.columns, "attached_via"), gold_columns=columns,
            whole_table_inputs=whole, append_inputs={DEMOLITION_SOURCE: source_table})
        # Root ADR-0272: the 철거멸실관리대장 is a register, so a 시도 that loses its documents refuses the build.
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
