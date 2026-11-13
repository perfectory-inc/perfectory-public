#!/usr/bin/env python3
"""Advance `silver.parcel_registry` to a cadastral snapshot (root ADR-0113 §2·§3·§7(b), ADR-0490 §4a).

The registry is read as of the snapshot's edition (`parcel_registry_frames.registry_through`): the
rows of that edition and the ones before it. Three modes, chosen from what that holds for the region:
  - bootstrap: the registry has nothing yet — every parcel of the snapshot gets its id;
  - advance:   no row registers this snapshot yet — ids are carried across strong identity links of
               `silver.parcel_lineage` (from `--from-snapshot-id`), other parcels get fresh ids,
               vanished ones close; the parcels both editions hold write nothing;
  - upgrade:   the snapshot is already registered — a lineage that got stronger since redirects the
               interim ids to the ids the land should have kept.
Until 2026-11-11 the mode was guessed from the share of unregistered parcels and the registry was
folded across every edition at once. The next edition's step is now appended before it is served
(the readiness candidate, ADR-0490 §4), and readers of the served edition fold as of it.

Everything runs as joins (`parcel_registry_frames`): a renumbered 시도 is millions of rows. Before
anything is appended the result is folded and checked: every parcel of the snapshot must carry exactly
one current id, otherwise nothing is written.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import parcel_identity as pi
import parcel_registry_frames as frames
import vworld_parcel_editions as editions
from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages
from lakehouse_ingest import append_batch_once
from parcel_lineage import DATE, IDENTIFIER, PREFIX, SNAPSHOT_ID, sql_prefixes
from platform_contracts import (
    column_names,
    create_table_columns_sql,
    evolve_iceberg_table_to_contract,
    load_lakehouse_contract,
    partition_clause_sql,
)

JOB_NAME = "parcel_registry_to_silver"
CONTRACT = load_lakehouse_contract("silver.parcel_registry")
COLUMNS: tuple[str, ...] = column_names(CONTRACT)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--to-snapshot-id",
                        help="silver.parcel_boundaries source_snapshot_id to register: the served edition when omitted (ADR-0148 §1)")
    editions.add_reader_flag(parser)
    parser.add_argument("--to-date", help="YYYY-MM-DD the snapshot reflects; required unless --probe-missing")
    parser.add_argument("--to-sido", help="Comma-separated sido prefixes of the snapshot's parcels; required unless --probe-missing")
    parser.add_argument("--probe-missing", action="store_true",
                        help="Only print the snapshot's sido prefixes the registry holds nothing for, with their parcel "
                             "counts, smallest first; writes nothing (root ADR-0243)")
    parser.add_argument("--probe-advance", action="store_true",
                        help="Only print the regions of the snapshot no registry row registers yet, as [to sido, from "
                             "sido, from snapshot, parcels] smallest first: the 시도 a lineage link joins read together, "
                             "each from the newest edition registered there; writes nothing (root ADR-0490 §4a)")
    parser.add_argument("--from-snapshot-id", help="The snapshot the lineage runs from (advance / upgrade)")
    parser.add_argument("--from-sido", help="Sido prefixes the registry held before, when they differ (29,46 -> 12)")
    parser.add_argument("--bootstrap", action="store_true", help="Allow the first registration of the region")
    parser.add_argument("--summary-output")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-namespace", default="silver")
    parser.add_argument("--iceberg-table", default="parcel_registry")
    parser.add_argument("--lineage-table", default="parcel_lineage")
    parser.add_argument("--allow-non-smoke-write", action="store_true")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    args = parser.parse_args(argv)
    allow = args.allow_non_served_edition
    args.to_snapshot_id = editions.served_reader_id(args.to_snapshot_id, allow, "--to-snapshot-id")
    args.from_snapshot_id = editions.edition_reader_id(args.from_snapshot_id, allow, "--from-snapshot-id")
    return args


def validate_args(args: argparse.Namespace) -> None:
    for label in ("iceberg_catalog_name", "iceberg_namespace", "iceberg_table", "lineage_table"):
        if not IDENTIFIER.fullmatch(getattr(args, label)):
            raise ValueError(f"{label} must be a plain SQL identifier")
    for label in ("to_snapshot_id", "from_snapshot_id"):
        value = getattr(args, label)
        if value is not None and not SNAPSHOT_ID.fullmatch(value):
            raise ValueError(f"--{label.replace('_', '-')} has characters a snapshot id does not use")
    if args.probe_missing and args.probe_advance:
        raise ValueError("--probe-missing and --probe-advance are two questions; ask one")
    if args.probe_missing:
        if args.to_date or args.to_sido or args.from_snapshot_id or args.from_sido or args.bootstrap:
            raise ValueError("--probe-missing takes only the snapshot and the tables")
        assert_catalog_env()
        return
    if args.probe_advance:
        if args.to_date or args.to_sido or args.from_sido or args.bootstrap or args.from_snapshot_id:
            raise ValueError("--probe-advance takes only the snapshot and the tables")
        assert_catalog_env()
        return
    if not args.to_date or not args.to_sido:
        raise ValueError("--to-date and --to-sido are required")
    if not DATE.fullmatch(args.to_date):
        raise ValueError("--to-date must be YYYY-MM-DD")
    for label in ("to_sido", "from_sido"):
        value = getattr(args, label)
        if value is not None and not all(PREFIX.fullmatch(p) for p in value.split(",")):
            raise ValueError(f"--{label.replace('_', '-')} must be comma-separated two-digit sido prefixes")
    if not args.bootstrap and not args.from_snapshot_id:
        raise ValueError("--from-snapshot-id is required unless --bootstrap")
    if not args.iceberg_table.endswith("_smoke") and not args.allow_non_smoke_write:
        raise ValueError(f"writing {args.iceberg_table} needs --allow-non-smoke-write")
    assert_catalog_env()


def missing_regions(snapshot_counts: dict[str, int], registered: set[str]) -> list[tuple[str, int]]:
    """The snapshot's sido prefixes the registry holds nothing for, smallest first.

    Smallest first so a run that stops at its time limit has registered as many regions as it could;
    the next run takes the rest (root ADR-0243).
    """

    return sorted(((sido, n) for sido, n in snapshot_counts.items() if sido not in registered), key=lambda p: (p[1], p[0]))


def mode(holds_region: bool, holds_snapshot: bool, bootstrap_allowed: bool, from_snapshot: str | None) -> str:
    """bootstrap, advance or upgrade, from what the registry as of the snapshot holds for the region:
    nothing; something, but no row of this snapshot; rows of this snapshot."""

    if not holds_region:
        if not bootstrap_allowed:
            raise ValueError("the registry holds nothing for this region; pass --bootstrap for the first registration")
        return "bootstrap"
    if holds_snapshot:
        return "upgrade"
    if from_snapshot is None:
        raise ValueError("advancing to a new snapshot reads its lineage: pass --from-snapshot-id")
    return "advance"


def probe_advance(spark, cat: str, table: str, args: argparse.Namespace) -> list[list]:
    """[to sido, from sido, from snapshot, parcels] of each region no row registers `--to-snapshot-id`
    for yet, smallest first. A region advances from the newest edition registered in it, over the
    lineage pair from that edition; the 시도 a link of it joins (29·46 → 12 on 2026-07-01) are one region.
    A region whose pair has no lineage yet is left out: advancing it would issue every renumbered parcel
    a fresh id (lineage_stewardship derives the pair, and the next run takes the region)."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    to_counts = {r["sido"]: int(r["n"]) for r in spark.sql(
        f"SELECT substr(pnu, 1, 2) AS sido, count(DISTINCT pnu) AS n FROM {cat}.`silver`.`parcel_boundaries` "
        f"WHERE source_snapshot_id = '{args.to_snapshot_id}' GROUP BY substr(pnu, 1, 2)").collect()}
    registry = frames.registry_through(spark.table(table), args.to_snapshot_id)
    newest = {r["sido"]: r["s"] for r in registry.groupBy(F.substring("pnu", 1, 2).alias("sido"))
              .agg(F.max("to_snapshot_id").alias("s")).collect()}
    held = {r["sido"] for r in registry.where(F.col("to_snapshot_id") == args.to_snapshot_id)
            .select(F.substring("pnu", 1, 2).alias("sido")).distinct().collect()}
    lineage = spark.table(f"{cat}.`{args.iceberg_namespace}`.`{args.lineage_table}`").where(
        F.col("to_snapshot_id") == args.to_snapshot_id)
    pairs = {r["from_snapshot_id"] for r in lineage.select("from_snapshot_id").distinct().collect()}
    sides = (lineage.where(F.coalesce(F.col("predecessor_pnu"), F.lit("")) != "")
             .select("from_snapshot_id", F.substring("predecessor_pnu", 1, 2).alias("a"),
                     F.substring("successor_pnu", 1, 2).alias("b"))
             .where(F.col("a") != F.col("b")).distinct().collect())
    waiting = []
    for source in sorted({s for s in newest.values() if s != args.to_snapshot_id}):
        from_sido = sorted(sido for sido, s in newest.items() if s == source)
        links = [(r["a"], r["b"]) for r in sides if r["from_snapshot_id"] == source]
        for to, read in frames.advance_groups(links, sorted(to_counts), from_sido):
            if set(to.split(",")) & held or source not in pairs:
                continue
            waiting.append([to, read, source, sum(to_counts[s] for s in to.split(","))])
    return sorted(waiting, key=lambda g: (g[3], g[0]))


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_args(args)
    from pyspark.sql import SparkSession, functions as F  # noqa: PLC0415

    now = datetime.now(timezone.utc).replace(microsecond=0)
    builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).config("spark.jars.packages", args.iceberg_packages).getOrCreate()
    cat = f"`{args.iceberg_catalog_name}`"
    table = f"{cat}.`{args.iceberg_namespace}`.`{args.iceberg_table}`"
    try:
        if args.probe_missing:
            counts = {
                r["sido"]: int(r["n"])
                for r in spark.sql(
                    f"SELECT substr(pnu, 1, 2) AS sido, count(DISTINCT pnu) AS n FROM {cat}.`silver`.`parcel_boundaries` "
                    f"WHERE source_snapshot_id = '{args.to_snapshot_id}' GROUP BY substr(pnu, 1, 2)"
                ).collect()
            }
            exists = spark.catalog.tableExists(f"{args.iceberg_catalog_name}.{args.iceberg_namespace}.{args.iceberg_table}")
            registered = (
                {r["sido"] for r in frames.registry_through(spark.table(table), args.to_snapshot_id)
                 .select(F.substring("pnu", 1, 2).alias("sido")).distinct().collect()}
                if exists else set()
            )
            missing = missing_regions(counts, registered)
            print("parcel-registry-probe " + json.dumps({"to_snapshot_id": args.to_snapshot_id, "missing": missing}))
            return 0
        if args.probe_advance:
            waiting = probe_advance(spark, cat, table, args) if spark.catalog.tableExists(
                f"{args.iceberg_catalog_name}.{args.iceberg_namespace}.{args.iceberg_table}") else []
            print("parcel-registry-advance-probe " + json.dumps({"to_snapshot_id": args.to_snapshot_id,
                                                                 "waiting": waiting}))
            return 0
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.`{args.iceberg_namespace}`")
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
{create_table_columns_sql(CONTRACT)}
            )
            USING iceberg
            {partition_clause_sql(CONTRACT)}
            TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
            """
        )
        evolve_iceberg_table_to_contract(spark, table, CONTRACT)
        region = ",".join(sorted(set(args.to_sido.split(",")) | set((args.from_sido or args.to_sido).split(","))))
        registry = frames.registry_through(
            spark.table(table).where(f"substr(pnu, 1, 2) IN ({sql_prefixes(region)})"), args.to_snapshot_id).persist()
        after = (spark.table(f"{cat}.`silver`.`parcel_boundaries`")
                 .where(f"source_snapshot_id = '{args.to_snapshot_id}' AND substr(pnu, 1, 2) IN ({sql_prefixes(args.to_sido)})")
                 .select("pnu").distinct().persist())
        snapshot_parcels = after.count()
        if not snapshot_parcels:
            raise ValueError(f"no parcels in {args.to_snapshot_id} for {args.to_sido}")
        holds_snapshot = not registry.where(F.col("to_snapshot_id") == args.to_snapshot_id).isEmpty()
        chosen = mode(not registry.isEmpty(), holds_snapshot, args.bootstrap, args.from_snapshot_id)
        lineage_runs: list[str] = []
        counts: dict[str, int] = {}
        if chosen == "bootstrap":
            rows = after.select(frames.parcel_id_expr(F.col("pnu"), args.to_date).alias("parcel_id"), "pnu", F.lit(pi.CURRENT).alias("status"),
                                F.lit(args.to_date).alias("valid_from"), F.lit(None).cast("string").alias("valid_to"),
                                F.lit(None).cast("string").alias("redirect_to"))
        else:
            # An upgrade without --from-snapshot-id has no lineage pair to read: nothing grew stronger.
            pair = (spark.table(f"{cat}.`{args.iceberg_namespace}`.`{args.lineage_table}`")
                    .where((F.col("from_snapshot_id") == (args.from_snapshot_id or "")) & (F.col("to_snapshot_id") == args.to_snapshot_id))
                    .select(*frames.LINEAGE_COLUMNS, "derivation_run_id").persist())
            lineage_runs = sorted(r["derivation_run_id"] for r in pair.select("derivation_run_id").distinct().collect())
            if chosen == "advance" and not lineage_runs:
                raise ValueError(f"no lineage from {args.from_snapshot_id} to {args.to_snapshot_id}: advancing would issue "
                                 "every renumbered parcel a fresh id")
            # A steward's standing decision replaces the derived rows of its parcel (root ADR-0115 §9).
            carry, conflicts, sample = frames.carrying(frames.resolved_lineage(pair.drop("derivation_run_id")))
            if conflicts:
                raise ValueError(f"{conflicts} identity conflicts in the lineage (e.g. {sample[:5]}); "
                                 "resolve them before registering ids")
            carry = carry.persist()
            current = frames.current_ids(registry).persist()
            if chosen == "advance":
                rows, counts = frames.advance_rows(current, after, carry, args.to_date)
            else:
                rows, counts = frames.upgrade_rows(current, frames.closed_ids(registry), carry, args.to_date)
        rows = rows.persist()
        found = frames.problems(registry, rows, after)
        if found:
            raise ValueError("refusing to register: " + "; ".join(found[:10]))
        run_id = "parcel-registry-" + hashlib.sha256(json.dumps({
            "to": args.to_snapshot_id, "from": args.from_snapshot_id, "sido": sorted(region.split(",")),
            "mode": chosen, "lineage": lineage_runs, "rules": pi.ID_RULES_VERSION,
            "registry_rows": registry.count(),
        }, sort_keys=True).encode()).hexdigest()[:24]
        written = rows.count()
        appended = False
        if written:
            frame = rows.select(
                *frames.REGISTRY_ROW,
                F.lit(args.to_snapshot_id).alias("to_snapshot_id"),
                F.lit(pi.ID_RULES_VERSION).alias("id_rules_version"),
                F.lit(run_id).alias("derivation_run_id"),
                F.lit(now).cast("timestamp").alias("recorded_at_utc"),
            ).select(*COLUMNS)
            appended = append_batch_once(spark, frame, COLUMNS, table, CONTRACT["table_name"])["appended"]
        summary = {"mode": chosen, "rows": written, **counts, "snapshot_parcels": snapshot_parcels}
        summary.update({"job": JOB_NAME, "derivation_run_id": run_id, "appended": appended, "to_snapshot_id": args.to_snapshot_id})
    finally:
        spark.stop()
    if args.summary_output:
        Path(args.summary_output).write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("parcel-registry-summary-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
