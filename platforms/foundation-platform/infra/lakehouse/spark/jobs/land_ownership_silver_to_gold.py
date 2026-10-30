#!/usr/bin/env python3
"""Build the parcel ownership by-PNU kind's Gold from its own Silver (root ADR-0360, ADR-0361).

A kind with its own producer (`by_pnu_kinds.Kind.producer`, root ADR-0287 §9): served on the parcel
lane like a panel section, but built from `silver.land_ownership`, not from the parcel panel. The
parcel document's schema does not change, so adding this kind never re-bakes the parcel lane.

What a PNU's document holds (`ownership`), from the parcel's current owner rows (the contract's
`current_rows`: every row of the newest release that holds the parcel, root ADR-0275):

- `owners`: the parcel's own owners, one per co-owner (집합건물일련번호 all zeros), in the provider's
  order (공유인일련번호), at most OWNER_PAGE_BOUND of them; `owner_total` (the Gold column) counts all.
  Each is the provider's codes and words: 소유구분, 거주지구분, 국가기관구분, 소유권변동원인 and
  변동일자, 공유인수. The file names no person.
- `unit_owner_total` and `unit_owner_kinds`: for a collective building on the parcel (an apartment,
  a 지식산업센터), how many unit owner rows there are, counted by 소유구분 — the units themselves are
  not listed (a parcel holds up to thousands; measured 2026-10-30, 2,246 rows on one 세종 PNU).
- `latest_changed_on`, `base_year_month`, `data_reference_date`: the newest of each over the rows.

A row keyed under a code a reorganization renumbered is read under the served code
(`parcel_panel_silver_to_gold.renumbered`, as every parcel section). Only PNUs the served parcel
edition holds have a document (the kind's rows are a subset of its host's, root ADR-0193 §9,
ADR-0292); the lineage is not followed: owners are a fact of the parcel as registered now, never
inherited from a predecessor.

Partial by design (root ADR-0312 §2): a Silver release appends only the changed parcels' rows, so
the PNUs a run recomputes are the PNUs of the rows appended since the current Gold's pin; a served
parcel edition change recomputes the PNUs that entered or left it. A change of the code change table
rebuilds whole (`WHOLE_TABLE_INPUTS`): a reorganization renames whole 동s, rare enough that a full
build is the cheaper rule.
"""
from __future__ import annotations

import argparse
import json
import os
from typing import Any

JOB_NAME = "land_ownership_silver_to_gold"
OWNERSHIP_SOURCE = "silver.land_ownership"
PARCEL_SOURCE = "silver.parcel_boundaries"
CODE_CHANGE_SOURCE = "reference.legal_dong_code_change"
WHOLE_TABLE_INPUTS = (CODE_CHANGE_SOURCE,)
SECTION = "ownership"
# A parcel's listed owners: past this the document keeps the first ones (owner_total says how many).
# Measured 2026-10-30 on 세종 (AL_D161_36): at most 815 owner rows on one parcel's own land.
OWNER_PAGE_BOUND = 100
# The provider's land-level rows name no collective building: 집합건물일련번호 is all zeros.
LAND_ROW_SERIAL = "^0*$"
OWNER_FIELDS = ("co_owner_serial_no", "ownership_kind_code", "ownership_kind_name", "residence_kind_code",
                "residence_kind_name", "state_agency_kind_code", "state_agency_kind_name",
                "ownership_change_reason_code", "ownership_change_reason", "ownership_changed_on", "co_owner_count")
OWNER_STRUCT = ("array<struct<co_owner_serial_no:string,ownership_kind_code:string,ownership_kind_name:string,"
                "residence_kind_code:string,residence_kind_name:string,state_agency_kind_code:string,"
                "state_agency_kind_name:string,ownership_change_reason_code:string,ownership_change_reason:string,"
                "ownership_changed_on:string,co_owner_count:int>>")
KIND_STRUCT = "array<struct<ownership_kind_name:string,count:bigint>>"
# The columns the build reads from each input, and the only ones it is handed.
BUILD_COLUMNS = {
    OWNERSHIP_SOURCE: ("pnu", "collective_building_serial_no", "base_year_month", "data_reference_date", *OWNER_FIELDS),
    PARCEL_SOURCE: ("pnu",),
}
# How a document is written: nulls too, so its paths are always the registry's.
WRITTEN = {"ignoreNullFields": "false"}


def lane_inputs(lane: str) -> tuple[tuple[str, ...], dict[str, str], str, tuple[str, ...]]:
    """(required inputs, optional input -> flag without it, anchor input, whole-table inputs), the
    seam `gold_rebuild.producer_inputs` reads. No optional input: the lineage is not followed."""
    del lane
    return (PARCEL_SOURCE, OWNERSHIP_SOURCE, CODE_CHANGE_SOURCE), {}, PARCEL_SOURCE, WHOLE_TABLE_INPUTS


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


def land_rows(rows: Any) -> Any:
    from pyspark.sql import functions as F  # noqa: PLC0415

    serial = F.col("collective_building_serial_no")
    return rows.where(serial.isNull() | serial.rlike(LAND_ROW_SERIAL))


def unit_rows(rows: Any) -> Any:
    from pyspark.sql import functions as F  # noqa: PLC0415

    serial = F.col("collective_building_serial_no")
    return rows.where(serial.isNotNull() & ~serial.rlike(LAND_ROW_SERIAL))


def gold_rows(frames: dict[str, Any], source_snapshot_id: str, published_at_utc: str,
              columns: tuple[str, ...]) -> Any:
    """One row per served PNU with an owner row: `ownership_json`, `owner_total`, `attached_via` (null)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    rows = frames[OWNERSHIP_SOURCE].join(frames[PARCEL_SOURCE].select("pnu").distinct(), "pnu", "left_semi")
    owners = (land_rows(rows).groupBy("pnu").agg(
        F.array_sort(F.collect_list(F.struct(*[F.col(f) for f in OWNER_FIELDS]))).alias("_owners"),
        F.count(F.lit(1)).alias("owner_total")))
    kinds = (unit_rows(rows).groupBy("pnu", "ownership_kind_name").agg(F.count(F.lit(1)).alias("count"))
             .groupBy("pnu").agg(
                 # Most units first, then the provider's word: a stable order for the same rows.
                 F.array_sort(F.collect_list(F.struct((-F.col("count")).alias("_order"),"ownership_kind_name", "count")))
                 .alias("_kinds"),
                 F.sum("count").alias("unit_owner_total")))
    newest = rows.groupBy("pnu").agg(F.max("ownership_changed_on").alias("latest_changed_on"),
                                     F.max("base_year_month").alias("base_year_month"),
                                     F.max("data_reference_date").alias("data_reference_date"))
    joined = newest.join(owners, "pnu", "left").join(kinds, "pnu", "left")
    document = F.struct(
        F.coalesce(F.slice("_owners", 1, OWNER_PAGE_BOUND), F.expr(f"CAST(array() AS {OWNER_STRUCT})")).alias("owners"),
        F.coalesce("unit_owner_total", F.lit(0)).cast("bigint").alias("unit_owner_total"),
        F.coalesce(F.transform("_kinds", lambda k: F.struct(k["ownership_kind_name"].alias("ownership_kind_name"),
                                                            k["count"].alias("count"))),
                   F.expr(f"CAST(array() AS {KIND_STRUCT})")).alias("unit_owner_kinds"),
        "latest_changed_on", "base_year_month", "data_reference_date",
    )
    content = ("pnu", "ownership_json", "owner_total", "attached_via")
    return (joined.select(
        "pnu", F.to_json(document, WRITTEN).alias("ownership_json"),
        F.coalesce("owner_total", F.lit(0)).cast("bigint").alias("owner_total"),
        F.lit(None).cast("string").alias("attached_via"),
        F.lit(source_snapshot_id).alias("source_snapshot_id"), F.lit(published_at_utc).alias("published_at_utc"))
        .withColumn("row_digest", F.sha2(F.to_json(F.struct(*content)), 256)).select(*columns))


def restrict_inputs(frames: dict[str, Any], keys: Any) -> dict[str, Any]:
    """The input rows the Gold rows of the PNUs `keys` are built from: their own rows only."""
    return {name: frame.join(keys, "pnu", "left_semi") for name, frame in frames.items()}


def affected_pnus(changed: list[str], appended: list[str], *, appended_rows: Any, changed_rows: Any,
                  parcel_change: Any) -> Any:
    """The PNUs whose document can differ from the current Gold's (root ADR-0312 §2): those of the
    owner rows appended since the pin (or, after any other kind of commit, of the rows that differ),
    and those that entered or left the served parcel edition. Each argument is a zero-argument
    callable returning a frame of `pnu`, called only when its input changed."""
    from functools import reduce  # noqa: PLC0415

    parts = []
    if OWNERSHIP_SOURCE in appended:
        parts.append(appended_rows())
    elif OWNERSHIP_SOURCE in changed:
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
        F.sum(F.when(F.col("ownership_json").isNull(), 1).otherwise(0)).alias("empty_section_count"),
    ).first().asDict().items()}
    duplicates = gold.groupBy("pnu").count().where(F.col("count") > 1).limit(3).count()
    if duplicates or metrics["invalid_pnu_count"] or metrics["empty_section_count"]:
        raise ValueError(f"Gold quality gates failed: {metrics}, {duplicates} duplicated PNUs")
    if expected is not None and metrics["row_count"] != expected:
        raise ValueError(f"Expected {expected} Gold rows, found {metrics['row_count']}")
    return metrics["row_count"], {k: v for k, v in metrics.items() if k != "row_count"}


def read_inputs(spark: Any, args: argparse.Namespace, at: dict[str, str], counters: dict[str, int]
                ) -> tuple[dict[str, Any], Any]:
    """The build inputs at `at` (owner rows re-keyed to the served codes, the served parcels) and the
    code change table, as the build columns."""
    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415

    changes, renumbering = panel.read_renumbering(spark, args, at, counters)
    owners = panel.renumbered(panel.read_source(spark, args, OWNERSHIP_SOURCE, at), renumbering)
    parcels = panel.read_served_parcels(spark, args, at, region=False)
    return ({OWNERSHIP_SOURCE: owners.select(*BUILD_COLUMNS[OWNERSHIP_SOURCE]),
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
    owner_table = f"{args.iceberg_catalog_name}.{args.source_iceberg_namespace}.{OWNERSHIP_SOURCE.split('.', 1)[1]}"
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
            # Removal markers too: a parcel whose owners vanished must lose its document.
            _changes, renumbering = panel.read_renumbering(spark, args, pins, {})
            appended = incremental.read_appended(spark, owner_table, previous[OWNERSHIP_SOURCE], pins[OWNERSHIP_SOURCE])
            return panel.renumbered(appended.select("pnu"), renumbering)

        lane = incremental.Lane(
            build=lambda inputs: gold_rows(inputs, pins[PARCEL_SOURCE], args.published_at_utc, columns),
            restrict=restrict_inputs,
            affected=lambda changed, appended: affected_pnus(
                changed, appended, appended_rows=appended_rows,
                changed_rows=lambda: incremental.changed_rows(previous_frames()[OWNERSHIP_SOURCE],
                                                              frames[OWNERSHIP_SOURCE], ("pnu",)),
                parcel_change=lambda: incremental.changed_rows(previous_frames()[PARCEL_SOURCE],
                                                               frames[PARCEL_SOURCE], ("pnu",))),
            validate=validate_gold, content_columns=("pnu", *kind.columns, "attached_via"), gold_columns=columns,
            whole_table_inputs=whole, append_inputs={OWNERSHIP_SOURCE: owner_table})
        # Root ADR-0272: owners are a register, so a 시도 that loses its documents refuses the build.
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
