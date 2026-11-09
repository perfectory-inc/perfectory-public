#!/usr/bin/env python3
"""Build the parcel energy by-PNU kind's Gold from its own Silver (root ADR-0396, ADR-0398).

A kind with its own producer (`by_pnu_kinds.Kind.producer`, root ADR-0287 §9): served on the parcel
lane like a panel section, but built from the two 월별 건물 에너지 Silver tables, not from the parcel
panel. The parcel document's schema does not change, so adding this kind never re-bakes the parcel lane.

What a PNU's document holds (`energy`), from the 지번's current usage rows (the contracts'
`current_rows`: a usage month's rows of its newest publication, root ADR-0396 §4):

- `months`: the newest MONTH_BOUND usage months the parcel has either a 전기 or a 가스 row for, newest
  first. Each is the month (사용년월) and, per energy, the sum of the month's 사용량 rows and how many
  rows there were (a 지번 has a row per 수용가). A month with no row of an energy has that energy null:
  "no row" is not "0".
- `electricity_unit`: `kWh`, the provider's stated unit of 전기 사용량 (data.go.kr 15135963, 건축HUB
  건물에너지정보 서비스 제공단위). 가스 사용량 has no unit the provider states, so none is written: the
  panel shows it as the source's 사용량.
- `latest_use_ym`: the newest month of either energy.

A 사용량 that is not a number is left out of the sum and counted (`unparsed_usage_rows`); the rows still
count. The provider leaves out 산업·수송·발전·열병합·충전 uses and houses (data.go.kr 15054213/15054214),
which the panel says beside the numbers.

A row keyed under a code a reorganization renumbered is read under the served code
(`parcel_panel_silver_to_gold.renumbered`, as every parcel section and the ownership kind; root
ADR-0329 §3). Only PNUs the served parcel edition holds have a document (ADR-0292); the lineage is not
followed: usage is metered on the parcel as it is now.

Partial by design (root ADR-0312 §2): a Silver release appends only the changed 지번 of one usage month,
so the PNUs a run recomputes are the PNUs of the rows appended since the current Gold's pin, of either
energy; a served parcel edition change recomputes the PNUs that entered or left it. A change of the
code change table rebuilds whole (`WHOLE_TABLE_INPUTS`), as the ownership kind's.
"""
from __future__ import annotations

import argparse
import json
import os
from typing import Any

JOB_NAME = "building_energy_silver_to_gold"
ELECTRICITY_SOURCE = "silver.building_energy_monthly_electricity"
GAS_SOURCE = "silver.building_energy_monthly_gas"
ENERGY_SOURCES = {ELECTRICITY_SOURCE: "electricity", GAS_SOURCE: "gas"}
PARCEL_SOURCE = "silver.parcel_boundaries"
CODE_CHANGE_SOURCE = "reference.legal_dong_code_change"
WHOLE_TABLE_INPUTS = (CODE_CHANGE_SOURCE,)
SECTION = "energy"
# A year of months: the panel's chart. The Silver keeps every month (append-only).
MONTH_BOUND = 12
# data.go.kr 15135963 (건축HUB 건물에너지정보 서비스): 제공단위 KWh for 전기 사용량.
ELECTRICITY_UNIT = "kWh"
# A 사용량 the provider writes as digits with an optional fraction.
NUMBER = r"^\s*-?[0-9]+(\.[0-9]+)?\s*$"
MONTH_STRUCT = ("array<struct<use_ym:string,electricity_usage:decimal(38,3),electricity_rows:bigint,"
                "gas_usage:decimal(38,3),gas_rows:bigint>>")
BUILD_COLUMNS = {
    ELECTRICITY_SOURCE: ("pnu", "use_ym", "usage_amount"),
    GAS_SOURCE: ("pnu", "use_ym", "usage_amount"),
    PARCEL_SOURCE: ("pnu",),
}
# How a document is written: nulls too, so its paths are always the registry's.
WRITTEN = {"ignoreNullFields": "false"}


def lane_inputs(lane: str) -> tuple[tuple[str, ...], dict[str, str], str, tuple[str, ...]]:
    """(required inputs, optional input -> flag without it, anchor input, whole-table inputs), the
    seam `gold_rebuild.producer_inputs` reads. No optional input: the lineage is not followed."""
    del lane
    return ((PARCEL_SOURCE, ELECTRICITY_SOURCE, GAS_SOURCE, CODE_CHANGE_SOURCE), {}, PARCEL_SOURCE,
            WHOLE_TABLE_INPUTS)


def parse_args(argv=None) -> argparse.Namespace:
    """The arguments `scripts/ops/gold-panel-rebuild.sh` gives every producer (root ADR-0193 §7)."""
    import land_ownership_silver_to_gold as ownership  # noqa: PLC0415

    return ownership.parse_args(argv)


def monthly(rows: Any, energy: str) -> Any:
    """(pnu, use_ym, <energy>_usage, <energy>_rows, <energy>_unparsed) per parcel and month."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    number = F.col("usage_amount").rlike(NUMBER)
    return rows.groupBy("pnu", "use_ym").agg(
        F.sum(F.when(number, F.trim("usage_amount").cast("decimal(38,3)"))).alias(f"{energy}_usage"),
        F.count(F.lit(1)).alias(f"{energy}_rows"),
        F.sum(F.when(number, 0).otherwise(1)).alias(f"{energy}_unparsed"))


def gold_rows(frames: dict[str, Any], source_snapshot_id: str, published_at_utc: str,
              columns: tuple[str, ...]) -> Any:
    """One row per served PNU with a usage row: `energy_json`, `attached_via` (null)."""
    from pyspark.sql import Window  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415

    served = frames[PARCEL_SOURCE].select("pnu").distinct()
    electricity, gas = (monthly(frames[source].where(F.col("pnu").isNotNull()).join(served, "pnu", "left_semi"), energy)
                        for source, energy in ENERGY_SOURCES.items())
    months = electricity.join(gas, ["pnu", "use_ym"], "full")
    newest = Window.partitionBy("pnu").orderBy(F.col("use_ym").desc())
    kept = months.withColumn("_n", F.row_number().over(newest)).where(F.col("_n") <= MONTH_BOUND)
    per_pnu = kept.groupBy("pnu").agg(
        # Newest first: sorted by the negated position, a stable order for the same rows.
        F.array_sort(F.collect_list(F.struct(
            F.col("_n").alias("_order"), "use_ym", "electricity_usage",
            F.coalesce("electricity_rows", F.lit(0)).cast("bigint").alias("electricity_rows"),
            "gas_usage", F.coalesce("gas_rows", F.lit(0)).cast("bigint").alias("gas_rows")))).alias("_months"),
        F.max("use_ym").alias("latest_use_ym"),
        (F.sum(F.coalesce("electricity_unparsed", F.lit(0))) + F.sum(F.coalesce("gas_unparsed", F.lit(0))))
        .cast("bigint").alias("unparsed_usage_rows"))
    document = F.struct(
        F.coalesce(F.transform("_months", lambda m: F.struct(
            m["use_ym"].alias("use_ym"), m["electricity_usage"].alias("electricity_usage"),
            m["electricity_rows"].alias("electricity_rows"), m["gas_usage"].alias("gas_usage"),
            m["gas_rows"].alias("gas_rows"))), F.expr(f"CAST(array() AS {MONTH_STRUCT})")).alias("months"),
        F.lit(ELECTRICITY_UNIT).alias("electricity_unit"),
        "latest_use_ym", "unparsed_usage_rows",
    )
    content = ("pnu", "energy_json", "attached_via")
    return (per_pnu.select(
        "pnu", F.to_json(document, WRITTEN).alias("energy_json"),
        F.lit(None).cast("string").alias("attached_via"),
        F.lit(source_snapshot_id).alias("source_snapshot_id"), F.lit(published_at_utc).alias("published_at_utc"))
        .withColumn("row_digest", F.sha2(F.to_json(F.struct(*content)), 256)).select(*columns))


def restrict_inputs(frames: dict[str, Any], keys: Any) -> dict[str, Any]:
    """The input rows the Gold rows of the PNUs `keys` are built from: their own rows only."""
    return {name: frame.join(keys, "pnu", "left_semi") for name, frame in frames.items()}


def affected_pnus(changed: list[str], appended: list[str], *, appended_rows: Any, changed_rows: Any,
                  parcel_change: Any) -> Any:
    """The PNUs whose document can differ from the current Gold's (root ADR-0312 §2): per energy, those
    of the rows appended since the pin (or, after any other kind of commit, of the rows that differ),
    and those that entered or left the served parcel edition. `appended_rows` and `changed_rows` take
    the energy's source; every callable returns a frame of `pnu` and is called only when its input
    changed."""
    from functools import reduce  # noqa: PLC0415

    parts = []
    for source in ENERGY_SOURCES:
        if source in appended:
            parts.append(appended_rows(source))
        elif source in changed:
            parts.append(changed_rows(source))
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
        F.sum(F.when(F.col("energy_json").isNull(), 1).otherwise(0)).alias("empty_section_count"),
    ).first().asDict().items()}
    duplicates = gold.groupBy("pnu").count().where(F.col("count") > 1).limit(3).count()
    if duplicates or metrics["invalid_pnu_count"] or metrics["empty_section_count"]:
        raise ValueError(f"Gold quality gates failed: {metrics}, {duplicates} duplicated PNUs")
    if expected is not None and metrics["row_count"] != expected:
        raise ValueError(f"Expected {expected} Gold rows, found {metrics['row_count']}")
    return metrics["row_count"], {k: v for k, v in metrics.items() if k != "row_count"}


def read_inputs(spark: Any, args: argparse.Namespace, at: dict[str, str], counters: dict[str, int]
                ) -> tuple[dict[str, Any], Any]:
    """The build inputs at `at` (usage rows re-keyed to the served codes, the served parcels) and the
    code change table, as the build columns."""
    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415

    changes, renumbering = panel.read_renumbering(spark, args, at, counters)
    frames = {source: panel.renumbered(panel.read_source(spark, args, source, at), renumbering)
              .select(*BUILD_COLUMNS[source]) for source in ENERGY_SOURCES}
    parcels = panel.read_served_parcels(spark, args, at, region=False)
    return {**frames, PARCEL_SOURCE: parcels.select(*BUILD_COLUMNS[PARCEL_SOURCE])}, changes


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
    source_tables = {source: f"{args.iceberg_catalog_name}.{args.source_iceberg_namespace}.{source.split('.', 1)[1]}"
                     for source in ENERGY_SOURCES}
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

        def appended_rows(source: str) -> Any:
            # Removal markers too: a 지번 whose month vanished must lose that month.
            _changes, renumbering = panel.read_renumbering(spark, args, pins, {})
            appended = incremental.read_appended(spark, source_tables[source], previous[source], pins[source])
            return panel.renumbered(appended.select("pnu"), renumbering)

        lane = incremental.Lane(
            build=lambda inputs: gold_rows(inputs, pins[PARCEL_SOURCE], args.published_at_utc, columns),
            restrict=restrict_inputs,
            affected=lambda changed, appended: affected_pnus(
                changed, appended, appended_rows=appended_rows,
                changed_rows=lambda source: incremental.changed_rows(previous_frames()[source], frames[source],
                                                                     ("pnu",)),
                parcel_change=lambda: incremental.changed_rows(previous_frames()[PARCEL_SOURCE],
                                                               frames[PARCEL_SOURCE], ("pnu",))),
            validate=validate_gold, content_columns=("pnu", *kind.columns, "attached_via"), gold_columns=columns,
            whole_table_inputs=whole, append_inputs=source_tables)
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
