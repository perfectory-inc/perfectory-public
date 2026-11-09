#!/usr/bin/env python3
"""Build the parcel sub-parcel by-PNU kind's Gold from its own Silver (root ADR-0395, ADR-0399).

A kind with its own producer (`by_pnu_kinds.Kind.producer`, root ADR-0287 §9), built from
`silver.building_register_sub_parcel` (건축물대장 부속지번), served on the parcel lane.

A 부속지번 row ties two parcels: the register's own 대지 (`pnu`) and another parcel its building also
stands on (`sub_pnu`). A PNU's document (`sub_parcels`) reads the same rows from both ends:

- `on_other_parcels`: registers whose 대지 is this parcel, with the other parcel each also stands on;
- `from_other_parcels`: registers whose 대지 is another parcel and whose building also stands here.

Each entry is the register (관리건축물대장PK, 대장종류명) and the other parcel's PNU, so the panel links
to it, and its 지번 as the provider wrote it (시군구-법정동-대지구분-번-지; a 시군구 the crosswalk refused
leaves `sub_pnu` null, root ADR-0395 §3: the 지번 is still shown). At most ENTRY_BOUND of each, newest register key first; the
document counts all (`on_other_total`, `from_other_total`). A row naming the parcel itself as its other
parcel is not a link and is left out.

Both PNU columns of a row under a code a reorganization renumbered are read under the served code
(`renumbered`, root ADR-0329 §3). Only PNUs the served parcel edition holds have a document (ADR-0292).
Partial by design (root ADR-0312 §2): a run recomputes the PNUs at either end of the rows appended
since the current Gold's pin and the parcels that entered or left the served edition; a change of the
code change table rebuilds whole (`WHOLE_TABLE_INPUTS`), as every own-producer parcel kind.
"""
from __future__ import annotations

import argparse
import json
from typing import Any

JOB_NAME = "building_sub_parcel_silver_to_gold"
SUB_PARCEL_SOURCE = "silver.building_register_sub_parcel"
PARCEL_SOURCE = "silver.parcel_boundaries"
CODE_CHANGE_SOURCE = "reference.legal_dong_code_change"
WHOLE_TABLE_INPUTS = (CODE_CHANGE_SOURCE,)
SECTION = "sub_parcels"
# Measured 2026-10: one register carries up to 774 rows; the panel lists the first of each direction.
ENTRY_BOUND = 100
# The two 지번 of a row as the provider spells them: the other parcel and the register's own 대지.
JIBUN_PARTS = ("sub_sigungu_cd", "sub_bjdong_cd", "sub_san_gubun", "sub_bonbeon", "sub_bubeon")
SITE_PARTS = ("sigungu_cd", "bjdong_cd", "san_gubun", "bonbeon", "bubeon")
ENTRY_STRUCT = "array<struct<mgmt_key:string,register_kind_name:string,other_pnu:string,other_jibun:string>>"
BUILD_COLUMNS = {
    SUB_PARCEL_SOURCE: ("pnu", "sub_pnu", "mgmt_key", "register_kind_name", *JIBUN_PARTS, *SITE_PARTS),
    PARCEL_SOURCE: ("pnu",),
}
WRITTEN = {"ignoreNullFields": "false"}


def lane_inputs(lane: str) -> tuple[tuple[str, ...], dict[str, str], str, tuple[str, ...]]:
    """(required inputs, optional input -> flag without it, anchor input, whole-table inputs)."""
    del lane
    return (PARCEL_SOURCE, SUB_PARCEL_SOURCE, CODE_CHANGE_SOURCE), {}, PARCEL_SOURCE, WHOLE_TABLE_INPUTS


def parse_args(argv=None) -> argparse.Namespace:
    """The arguments `scripts/ops/gold-panel-rebuild.sh` gives every producer (root ADR-0193 §7)."""
    import land_ownership_silver_to_gold as ownership  # noqa: PLC0415

    return ownership.parse_args(argv)


def gold_rows(frames: dict[str, Any], source_snapshot_id: str, published_at_utc: str,
              columns: tuple[str, ...]) -> Any:
    """One row per served PNU at either end of a link: `sub_parcels_json`, `attached_via` (null)."""
    from pyspark.sql import Window  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415

    served = frames[PARCEL_SOURCE].select("pnu").distinct()
    rows = frames[SUB_PARCEL_SOURCE].where(F.col("pnu").isNull() | F.col("sub_pnu").isNull()
                                           | (F.col("pnu") != F.col("sub_pnu")))
    sub_jibun = F.concat_ws("-", *[F.trim(F.col(c)) for c in JIBUN_PARTS])
    site_jibun = F.concat_ws("-", *[F.trim(F.col(c)) for c in SITE_PARTS])

    def direction(here: str, other: Any, other_jibun: Any, name: str) -> Any:
        """(pnu, `<name>`, `<name>_total`): this end's entries, at most the bound, and their count."""
        entries = (rows.where(F.col(here).isNotNull())
                   .select(F.col(here).alias("pnu"), "mgmt_key", "register_kind_name",
                           other.alias("other_pnu"), other_jibun.alias("other_jibun"))
                   .distinct().join(served, "pnu", "left_semi"))
        order = Window.partitionBy("pnu").orderBy(F.col("mgmt_key").desc_nulls_last(), F.col("other_pnu").asc_nulls_last(),
                                                  F.col("other_jibun").asc_nulls_last())
        return (entries.withColumn("_n", F.row_number().over(order)).groupBy("pnu").agg(
            F.array_sort(F.collect_list(F.when(F.col("_n") <= ENTRY_BOUND, F.struct(
                F.col("_n").alias("_order"), "mgmt_key", "register_kind_name", "other_pnu", "other_jibun"))))
            .alias(f"_{name}"),
            F.count(F.lit(1)).alias(f"{name}_total")))

    # This parcel is the 대지: the other parcel is the row's 부속 지번, written as the provider spells it.
    on_other = direction("pnu", F.col("sub_pnu"), sub_jibun, "on_other")
    # This parcel is the 부속 지번: the other parcel is the register's 대지.
    from_other = direction("sub_pnu", F.col("pnu"), site_jibun, "from_other")
    joined = on_other.join(from_other, "pnu", "full")

    def entries(name: str) -> Any:
        return F.coalesce(F.transform(f"_{name}", lambda e: F.struct(
            e["mgmt_key"].alias("mgmt_key"), e["register_kind_name"].alias("register_kind_name"),
            e["other_pnu"].alias("other_pnu"), e["other_jibun"].alias("other_jibun"))),
            F.expr(f"CAST(array() AS {ENTRY_STRUCT})"))

    document = F.struct(
        entries("on_other").alias("on_other_parcels"),
        F.coalesce("on_other_total", F.lit(0)).cast("bigint").alias("on_other_total"),
        entries("from_other").alias("from_other_parcels"),
        F.coalesce("from_other_total", F.lit(0)).cast("bigint").alias("from_other_total"),
    )
    content = ("pnu", "sub_parcels_json", "attached_via")
    return (joined.select(
        "pnu", F.to_json(document, WRITTEN).alias("sub_parcels_json"),
        F.lit(None).cast("string").alias("attached_via"),
        F.lit(source_snapshot_id).alias("source_snapshot_id"), F.lit(published_at_utc).alias("published_at_utc"))
        .withColumn("row_digest", F.sha2(F.to_json(F.struct(*content)), 256)).select(*columns))


def restrict_inputs(frames: dict[str, Any], keys: Any) -> dict[str, Any]:
    """The input rows the Gold rows of the PNUs `keys` are built from: the rows with either end in them."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    sub_keys = keys.select(F.col("pnu").alias("_key"))
    rows = frames[SUB_PARCEL_SOURCE]
    linked = rows.join(sub_keys, (rows["pnu"] == sub_keys["_key"]) | (rows["sub_pnu"] == sub_keys["_key"]), "left_semi")
    return {SUB_PARCEL_SOURCE: linked, PARCEL_SOURCE: frames[PARCEL_SOURCE].join(keys, "pnu", "left_semi")}


def both_ends(frame: Any) -> Any:
    """The PNUs at either end of `frame`'s rows, as one `pnu` column."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    return (frame.select("pnu").unionByName(frame.select(F.col("sub_pnu").alias("pnu")))
            .where(F.col("pnu").isNotNull()))


def affected_pnus(changed: list[str], appended: list[str], *, appended_rows: Any, changed_rows: Any,
                  parcel_change: Any) -> Any:
    """As `land_ownership_silver_to_gold.affected_pnus`; the row callables return both ends' PNUs."""
    from functools import reduce  # noqa: PLC0415

    parts = []
    if SUB_PARCEL_SOURCE in appended:
        parts.append(appended_rows())
    elif SUB_PARCEL_SOURCE in changed:
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
        F.sum(F.when(F.col("sub_parcels_json").isNull(), 1).otherwise(0)).alias("empty_section_count"),
    ).first().asDict().items()}
    duplicates = gold.groupBy("pnu").count().where(F.col("count") > 1).limit(3).count()
    if duplicates or metrics["invalid_pnu_count"] or metrics["empty_section_count"]:
        raise ValueError(f"Gold quality gates failed: {metrics}, {duplicates} duplicated PNUs")
    if expected is not None and metrics["row_count"] != expected:
        raise ValueError(f"Expected {expected} Gold rows, found {metrics['row_count']}")
    return metrics["row_count"], {k: v for k, v in metrics.items() if k != "row_count"}


def renumbered_both(frame: Any, renumbering: Any) -> Any:
    """`frame` with both its `pnu` and its `sub_pnu` under the served code (root ADR-0329 §3)."""
    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415

    own = panel.renumbered(frame, renumbering)
    swapped = own.withColumnRenamed("pnu", "_own_pnu").withColumnRenamed("sub_pnu", "pnu")
    return (panel.renumbered(swapped, renumbering).withColumnRenamed("pnu", "sub_pnu")
            .withColumnRenamed("_own_pnu", "pnu"))


def read_inputs(spark: Any, args: argparse.Namespace, at: dict[str, str], counters: dict[str, int]
                ) -> tuple[dict[str, Any], Any]:
    """The build inputs at `at` (rows re-keyed to the served codes at both ends, the served parcels)
    and the code change table."""
    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415

    changes, renumbering = panel.read_renumbering(spark, args, at, counters)
    rows = renumbered_both(panel.read_source(spark, args, SUB_PARCEL_SOURCE, at)
                           .select(*BUILD_COLUMNS[SUB_PARCEL_SOURCE]), renumbering)
    parcels = panel.read_served_parcels(spark, args, at, region=False)
    return ({SUB_PARCEL_SOURCE: rows.select(*BUILD_COLUMNS[SUB_PARCEL_SOURCE]),
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
    source_table = f"{args.iceberg_catalog_name}.{args.source_iceberg_namespace}.{SUB_PARCEL_SOURCE.split('.', 1)[1]}"
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
            # Removal markers too, at both ends: a link that vanished must leave both documents.
            _changes, renumbering = panel.read_renumbering(spark, args, pins, {})
            appended = incremental.read_appended(spark, source_table, previous[SUB_PARCEL_SOURCE], pins[SUB_PARCEL_SOURCE])
            return both_ends(renumbered_both(appended.select("pnu", "sub_pnu"), renumbering))

        lane = incremental.Lane(
            build=lambda inputs: gold_rows(inputs, pins[PARCEL_SOURCE], args.published_at_utc, columns),
            restrict=restrict_inputs,
            affected=lambda changed, appended: affected_pnus(
                changed, appended, appended_rows=appended_rows,
                changed_rows=lambda: both_ends(incremental.changed_rows(previous_frames()[SUB_PARCEL_SOURCE],
                                                                        frames[SUB_PARCEL_SOURCE], ("pnu", "sub_pnu"))),
                parcel_change=lambda: incremental.changed_rows(previous_frames()[PARCEL_SOURCE],
                                                               frames[PARCEL_SOURCE], ("pnu",))),
            validate=validate_gold, content_columns=("pnu", *kind.columns, "attached_via"), gold_columns=columns,
            whole_table_inputs=whole, append_inputs={SUB_PARCEL_SOURCE: source_table})
        # Root ADR-0272: the 부속지번 is a register, so a 시도 that loses its documents refuses the build.
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
