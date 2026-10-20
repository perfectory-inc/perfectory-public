#!/usr/bin/env python3
"""Build one by-PNU data kind's Gold table (root ADR-0193): `--lane <a kind line>`.

A kind is one section of its host panel, named by its line in `config/r2-connections.contract.json`
`by_pnu_lanes` (`by_pnu_kinds.py`). This producer builds that section with the panel's own function
(`parcel_panel_silver_to_gold.SECTIONS`), not a copy, so until the panel retires both tables come
from the same code; `--parity-panel-snapshot-id` proves it on production pins. A new kind is a new
line: nothing here names one.

Only PNUs that have the section have a row. A parcel without its own takes its nearest lineage
predecessor's (root ADR-0113 §6) and names the path in `attached_via`. A change to an input the
panel reads whole rebuilds whole; any other change recomputes only the PNUs it reaches (root
ADR-0180).
"""
from __future__ import annotations

import argparse
import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_rebuild import assert_minimum_row_count
import by_pnu_kinds
import gold_incremental as incremental
import section_coverage
from lakehouse_engine import apply_catalog_settings, assert_catalog_env, assert_iceberg_runtime_loaded, iceberg_packages
from lakehouse_snapshot_pins import load_source_snapshot_pins
from platform_contracts import column_names, ensure_contract_table, load_lakehouse_contract, partition_column_names, required_column_names, sort_order
import parcel_panel_silver_to_gold as panel

JOB_NAME = "by_pnu_kind_silver_to_gold"
LINEAGE_COLUMNS = panel.LINEAGE_COLUMNS
ANCHOR_SOURCE = panel.PARCEL_SOURCE
LINEAGE_SOURCE = panel.LINEAGE_SOURCE


class KindSpec:
    """A kind line resolved against its host panel: its section, inputs and Gold contract."""

    def __init__(self, lane: str):
        self.kind = by_pnu_kinds.kind(lane)
        if self.kind.host != "parcel_by_pnu_gateway":
            raise ValueError(f"{lane}: this producer builds parcel kinds; {self.kind.host} has no section table yet")
        self.section = panel.SECTIONS_BY_NAME[self.kind.section]
        self.contract = load_lakehouse_contract(self.kind.gold_table)
        self.gold_columns = column_names(self.contract)
        self.required_columns = required_column_names(self.contract)
        self.content_columns = tuple(c for c in self.gold_columns if c not in LINEAGE_COLUMNS)
        self.sources = panel.section_sources(self.section)
        self.build_columns = {name: panel.BUILD_COLUMNS[name] for name in self.sources}
        self.whole_table_inputs = tuple(n for n in panel.WHOLE_TABLE_INPUTS if n in self.sources)
        if self.content_columns != ("pnu", *self.kind.columns, "attached_via"):
            raise ValueError(f"{self.kind.gold_table} columns {self.content_columns} are not the kind line's "
                             f"{self.kind.columns} (lakehouse contract artifact out of date?)")


def lane_inputs(lane: str) -> tuple[tuple[str, ...], dict[str, str], str, tuple[str, ...]]:
    """(required inputs, optional input -> flag without it, anchor input, whole-table inputs) of a
    kind, for the rebuild planner (gold_rebuild.producer_inputs)."""
    kind = by_pnu_kinds.kind(lane)
    section = panel.SECTIONS_BY_NAME[kind.section]
    sources = panel.section_sources(section)
    return (sources, {LINEAGE_SOURCE: "--no-carry-lineage"}, ANCHOR_SOURCE,
            tuple(n for n in panel.WHOLE_TABLE_INPUTS if n in sources))


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lane", required=True, help="A kind line of config/r2-connections.contract.json by_pnu_lanes.")
    parser.add_argument("--input-mode", choices=("parquet", "iceberg"), default="iceberg")
    parser.add_argument("--input-root", help="Parquet input root: one subdirectory per source table.")
    parser.add_argument("--output")
    parser.add_argument("--write-mode", choices=("parquet", "iceberg"), default="parquet")
    parser.add_argument("--iceberg-catalog-name", default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_CATALOG_NAME", "r2"))
    parser.add_argument("--source-iceberg-namespace", default="silver")
    parser.add_argument("--target-iceberg-namespace", default="gold")
    parser.add_argument("--iceberg-write-mode", choices=("append", "overwrite"), default="overwrite")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    parser.add_argument("--iceberg-snapshot-id", required=True,
                        help="Iceberg snapshot id of silver.parcel_boundaries represented by this projection.")
    parser.add_argument("--source-snapshots-path", help="JSON file pinning every input table.")
    parser.add_argument("--published-at-utc")
    parser.add_argument("--region-prefix", help="Optional PNU prefix every source is filtered by.")
    parser.add_argument("--allow-non-smoke-overwrite", action="store_true")
    parser.add_argument("--allow-intra-snapshot-duplicates", action="store_true",
                        help="As the parcel panel's: collapse duplicate PNU rows by a deterministic full-row order.")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--expected-count", type=int)
    parser.add_argument("--minimum-count", type=int, help="Refuse, before writing, a Gold with fewer rows (root ADR-0139).")
    parser.add_argument("--no-carry-lineage", dest="carry_lineage", action="store_false",
                        help="Do not attach a predecessor's section through silver.parcel_lineage (root ADR-0113 §6).")
    parser.add_argument("--parity-panel-snapshot-id",
                        help="Refuse, before writing, a Gold whose rows differ from this host panel snapshot's "
                             "section (root ADR-0193 §6-2); pass the panel's own pins.")
    parser.add_argument("--summary-output")
    parser.add_argument("--lineage-output")
    incremental.add_arguments(parser)
    section_coverage.add_arguments(parser)
    return parser.parse_args(argv)


def target_table(args, spec: KindSpec) -> str:
    return f"`{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`.`{spec.kind.gold_table.split('.', 1)[1]}`"


def input_sources(args, spec: KindSpec) -> tuple[str, ...]:
    return (*spec.sources, *((LINEAGE_SOURCE,) if args.carry_lineage else ()))


def validate_args(args, spec: KindSpec) -> dict[str, str]:
    if args.input_mode == "parquet" and not args.input_root:
        raise ValueError("--input-root is required when --input-mode=parquet")
    if args.write_mode == "parquet" and not args.output:
        raise ValueError("--output is required when --write-mode=parquet")
    if args.region_prefix is not None and not (args.region_prefix.isdigit() and len(args.region_prefix) <= 10):
        raise ValueError("--region-prefix must be 1 to 10 digits")
    for name in ("expected_count", "minimum_count"):
        if getattr(args, name) is not None and getattr(args, name) < 0:
            raise ValueError(f"--{name.replace('_', '-')} must be non-negative")
    if args.parity_panel_snapshot_id is not None and (
            args.input_mode != "iceberg" or not args.parity_panel_snapshot_id.isdigit()):
        raise ValueError(f"--parity-panel-snapshot-id is an Iceberg snapshot id of {spec.kind.host_panel} (iceberg input)")
    for name in ("iceberg_catalog_name", "source_iceberg_namespace", "target_iceberg_namespace"):
        panel.validate_identifier(name, getattr(args, name))
    if (args.write_mode == "iceberg" and args.iceberg_write_mode == "overwrite" and not args.validate_only
            and not args.allow_non_smoke_overwrite):
        raise ValueError("Refusing to overwrite a non-smoke Iceberg table without --allow-non-smoke-overwrite")
    if args.input_mode == "iceberg" or args.write_mode == "iceberg":
        assert_catalog_env(args.iceberg_catalog_name)
    incremental.validate_arguments(args, input_sources(args, spec))
    section_coverage.validate_arguments(args)
    return load_source_snapshot_pins(args.input_mode, args.source_snapshots_path, input_sources(args, spec),
                                     ANCHOR_SOURCE, args.iceberg_snapshot_id)


def read_inputs(spark, args, spec: KindSpec, pins, counters, snapshots=None):
    """The parcels and the section's inputs at `pins` reduced to their build columns, and the
    lineage's carry candidates: the panel's own reader, for this one section."""
    frames, candidates = panel.read_inputs(spark, args, pins, counters, snapshots, sections=(spec.section,))
    return {name: frames[name] for name in spec.sources}, candidates


def present(column: str) -> F.Column:
    """Whether a section value is there: the panel writes `[]` for a list section a PNU lacks."""
    return F.col(column).isNotNull() & (F.col(column) != F.lit("[]"))


def row_digest_column(spec: KindSpec) -> F.Column:
    """The row's content fingerprint (root ADR-0099); a null `attached_via` drops out of the JSON."""
    return F.sha2(F.to_json(F.struct(*[c for c in spec.content_columns])), 256)


def build_gold_frame(spark, args, spec: KindSpec, frames, candidates, counters, source_snapshot_id) -> DataFrame:
    built = spec.section.build(frames, spark, args, counters)
    carried = panel.carry_section(built, "attached_via", candidates)
    parcels = frames[ANCHOR_SOURCE].select("pnu").distinct()
    gold = parcels.join(carried, "pnu").where(present(spec.kind.columns[0])).select(
        *spec.content_columns,
        F.lit(source_snapshot_id).alias("source_snapshot_id"), F.lit(args.published_at_utc).alias("published_at_utc"))
    return gold.withColumn("row_digest", row_digest_column(spec)).select(*spec.gold_columns)


def validate_gold_frame(spec: KindSpec, gold, expected_count):
    if tuple(gold.columns) != spec.gold_columns:
        raise ValueError(f"Gold columns disagree with the lakehouse contract: {gold.columns}")
    checks = [F.count(F.lit(1)).alias("row_count")]
    checks += [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c + "__null_count") for c in spec.required_columns]
    checks += [F.sum(F.when(~F.col("pnu").rlike(panel.PNU_PATTERN), 1).otherwise(0)).alias("invalid_pnu_count"),
               F.sum(F.when(~present(spec.kind.columns[0]), 1).otherwise(0)).alias("empty_section_count")]
    metrics = {k: int(v or 0) for k, v in gold.agg(*checks).first().asDict().items()}
    if any(v for k, v in metrics.items() if k != "row_count"):
        raise ValueError(f"Gold quality gates failed: {metrics}")
    duplicates = gold.groupBy("pnu").count().where(F.col("count") > 1).limit(3).collect()
    if duplicates:
        raise ValueError(f"Gold must hold one row per pnu: {[r.pnu for r in duplicates]}")
    if expected_count is not None and metrics["row_count"] != expected_count:
        raise ValueError(f"Expected {expected_count} Gold rows, found {metrics['row_count']}")
    return metrics["row_count"], metrics


def parity_mismatches(spec: KindSpec, gold: DataFrame, panel_rows: DataFrame) -> DataFrame:
    """PNUs whose section here differs from the panel's (root ADR-0193 §6-2): the panel's rows that
    have the section, each content column and the section's member of `attached_via_json`."""
    section = spec.kind.columns
    expected = panel_rows.where(present(section[0])).select(
        "pnu", *[F.col(c).alias(f"expected_{c}") for c in section],
        F.get_json_object("attached_via_json", f"$.{spec.section.name}").alias("expected_via"))
    joined = gold.select("pnu", *section, "attached_via").join(expected, "pnu", "full_outer")
    same = F.col("attached_via").eqNullSafe(F.col("expected_via"))
    for c in section:
        same = same & F.col(c).eqNullSafe(F.col(f"expected_{c}"))
    return joined.where(~same).select("pnu")


def assert_parity(spark, args, spec: KindSpec, gold: DataFrame) -> dict[str, int]:
    if args.parity_panel_snapshot_id is None:
        return {}
    rows = spark.sql(f"SELECT pnu, {', '.join(spec.kind.columns)}, attached_via_json FROM "
                     f"`{args.iceberg_catalog_name}`.{spec.kind.host_panel} VERSION AS OF {int(args.parity_panel_snapshot_id)}")
    rows = panel.filtered_by_region(rows, args.region_prefix)
    mismatched = parity_mismatches(spec, gold, rows)
    count = mismatched.count()
    if count:
        examples = sorted(r.pnu for r in mismatched.limit(3).collect())
        raise ValueError(f"{count} PNUs' {spec.section.name} differ from {spec.kind.host_panel} snapshot "
                         f"{args.parity_panel_snapshot_id} (e.g. {examples}); built from different pins, "
                         "or the two producers drifted")
    return {"parity_mismatch_count": 0}


def restrict_inputs(spec: KindSpec, frames, keys, candidates):
    """The input rows the Gold rows of `keys` are built from: the panel's map, for this section."""
    restricted = panel.restrict_inputs(frames, keys, candidates)
    return {name: restricted[name] for name in spec.sources}


def build_session(args) -> SparkSession:
    builder = SparkSession.builder.appName(f"{JOB_NAME}:{args.lane}").config("spark.sql.session.timeZone", "UTC")
    if args.input_mode == "iceberg" or args.write_mode == "iceberg":
        builder = apply_catalog_settings(builder, args.iceberg_catalog_name)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if args.input_mode == "iceberg" or args.write_mode == "iceberg":
        assert_iceberg_runtime_loaded(spark, args.iceberg_packages)
    return spark


def main() -> int:
    args = parse_args()
    spec = KindSpec(args.lane)
    args.published_at_utc = panel.normalize_utc_timestamp(args.published_at_utc)
    pins = validate_args(args, spec)
    spark = build_session(args)
    try:
        return run(spark, args, spec, pins)
    finally:
        spark.stop()


def run(spark, args, spec: KindSpec, pins) -> int:
    """Build, check and write the Gold: whole, or only the changed PNUs merged (root ADR-0180)."""
    previous = incremental.validate_arguments(args, input_sources(args, spec))
    table = target_table(args, spec)
    counters: dict[str, int] = {}
    snapshots: dict[str, str] = {}
    frames, candidates = read_inputs(spark, args, spec, pins, counters, snapshots)
    source_id = snapshots[ANCHOR_SOURCE]
    lane = incremental.Lane(
        build=lambda inputs: build_gold_frame(spark, args, spec, inputs, candidates, counters, source_id),
        restrict=lambda inputs, keys: restrict_inputs(spec, inputs, keys, candidates),
        affected=lambda changed, _appended: panel.affected_pnus(
            *read_inputs(spark, args, spec, previous, {}), frames, candidates, changed),
        validate=lambda gold: validate_gold_frame(spec, gold, None),
        content_columns=spec.content_columns, gold_columns=spec.gold_columns,
        whole_table_inputs=spec.whole_table_inputs)
    # Root ADR-0272: a kind's rows are the PNUs that have its section, so their count is its coverage.
    coverage = section_coverage.guard_from_arguments(spark, args, table, {spec.kind.section: spec.kind.columns[0]},
                                                     frames.get(panel.CODE_CHANGE_SOURCE), shares=False)
    outcome = incremental.build_or_merge(spark, args, lane, frames, pins, previous, table,
                                         lambda gold, expected: validate_gold_frame(spec, gold, expected),
                                         assert_minimum_row_count, coverage)
    try:
        if outcome.mode == "full":
            counters.update(assert_parity(spark, args, spec, outcome.gold))
        persisted_count, metrics, added = None, outcome.metrics, ()
        if not args.validate_only:
            if args.write_mode == "parquet":
                (outcome.gold.repartition(*partition_column_names(spec.contract))
                 .sortWithinPartitions(*sort_order(spec.contract))
                 .write.mode("overwrite").partitionBy(*partition_column_names(spec.contract)).parquet(args.output))
                persisted = spark.read.parquet(args.output).select(*spec.gold_columns)
            else:
                spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`")
                # Range-ordered by pnu, so a by-PNU bake shard reads only the files of its prefix (ADR-0164).
                added = ensure_contract_table(spark, table, spec.contract)
                outcome.commit(spark, table, pins, args.iceberg_write_mode, contract_table=spec.contract["table_name"])
                incremental.retain_input_snapshots(spark, args.iceberg_catalog_name, args.source_iceberg_namespace,
                                                   spec.kind.gold_table, pins)
                persisted = spark.table(table).select(*spec.gold_columns)
            persisted_count, metrics = validate_gold_frame(spec, persisted, outcome.row_count)
        summary = build_run_summary(args, spec, outcome, persisted_count, {**metrics, **counters}, snapshots, pins, added)
    finally:
        outcome.release()
    panel.emit_run_summary(summary, args.summary_output)
    if args.lineage_output and not args.validate_only:
        panel.emit_lineage_event(build_lineage_event(args, spec, summary), args.lineage_output)
    print(f"gold-kind-{'validate' if args.validate_only else 'write'}-ok lane={args.lane} "
          f"rows={persisted_count or outcome.row_count} mode={outcome.mode}")
    return 0


def build_run_summary(args, spec: KindSpec, outcome, persisted_count, metrics, snapshots, pins, added) -> dict[str, Any]:
    return {
        "schema_version": panel.RUN_SUMMARY_SCHEMA_VERSION, "job_name": JOB_NAME, "contract": spec.kind.gold_table,
        "lane": args.lane, "created_at_utc": panel.normalize_utc_timestamp(None),
        "input": {"kind": args.input_mode, "sources": list(input_sources(args, spec)), "region_prefix": args.region_prefix},
        "target": {"kind": "parquet", "path": args.output} if args.write_mode == "parquet" else
                  {"kind": "iceberg", "catalog": args.iceberg_catalog_name,
                   "namespace": args.target_iceberg_namespace, "table": spec.kind.gold_table.split(".", 1)[1]},
        "write_mode": args.write_mode,
        "write_disposition": "validate_only" if args.validate_only else (
            "parquet_overwrite" if args.write_mode == "parquet" else outcome.disposition(args)),
        "row_count": outcome.row_count, "persisted_row_count": persisted_count, "quality_metrics": metrics,
        "columns": list(spec.gold_columns), "column_count": len(spec.gold_columns),
        "required_columns": list(spec.required_columns),
        "schema_evolution_added_columns": list(added), "source_snapshot_count": len(snapshots),
        "source_snapshot_ids": [snapshots[n] for n in sorted(snapshots)],
        "source_snapshots_by_dataset": dict(sorted(snapshots.items())),
        "source_iceberg_snapshots_by_dataset": dict(sorted(pins.items())),
        "source_snapshot_truncated": False,
        "rebuild": outcome.report(),
    }


def column_lineage(spec: KindSpec, carry_lineage: bool) -> list[dict[str, Any]]:
    def one(dataset: str, column: str, transform: str) -> list[dict[str, str]]:
        return [{"dataset": dataset, "column": column, "transform": transform}]

    section = [{"dataset": name, "column": ",".join(panel.BUILD_COLUMNS[name]), "transform": f"panel_section:{spec.section.name}"}
               for name in spec.section.inputs]
    lineage = {"pnu": one(ANCHOR_SOURCE, "pnu", "identity"),
               **{c: section for c in spec.kind.columns},
               "attached_via": one(LINEAGE_SOURCE, "predecessor_pnu,successor_pnu,relation,grade",
                                   "steward_resolved_nearest_holder_path") if carry_lineage
               else one("foundation-platform.job_arguments", "carry_lineage", "literal_null_when_disabled"),
               "source_snapshot_id": one(ANCHOR_SOURCE, "source_snapshot_id", "identity"),
               "published_at_utc": one("foundation-platform.job_arguments", "published_at_utc", "literal")}
    lineage["row_digest"] = [{**s, "transform": f"sha256_content:{s['transform']}"}
                             for c in spec.content_columns for s in lineage[c]]
    return [{"output_column": c, "inputs": lineage[c]} for c in spec.gold_columns]


def build_lineage_event(args, spec: KindSpec, summary: dict[str, Any]) -> dict[str, Any]:
    args.target_iceberg_table = spec.kind.gold_table.split(".", 1)[1]
    event = panel.build_lineage_event(args, summary)
    event.update({
        "job_name": JOB_NAME, "run_id": f"{JOB_NAME}:{args.lane}:{summary['created_at_utc']}",
        "output_dataset": {"qualified_name": spec.kind.gold_table, "namespace": "gold",
                           "table": spec.kind.gold_table.split(".", 1)[1], "storage_format": args.write_mode},
        "column_lineage": column_lineage(spec, args.carry_lineage),
    })
    event["openlineage_mapping"]["job_name"] = JOB_NAME
    return event


if __name__ == "__main__":
    raise SystemExit(main())
