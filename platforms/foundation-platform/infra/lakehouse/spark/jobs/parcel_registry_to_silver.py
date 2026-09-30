#!/usr/bin/env python3
"""Advance `silver.parcel_registry` to a cadastral snapshot (root ADR-0113 §2·§3·§7(b)).

Three modes, chosen from what the registry already holds for the region:
  - bootstrap: the registry has nothing yet — every parcel of the snapshot gets its id;
  - advance:   the snapshot is new — ids are carried across strong identity links of
               `silver.parcel_lineage`, other parcels get fresh ids, vanished ones close;
  - upgrade:   the snapshot is already registered — a lineage that got stronger since redirects the
               interim ids to the ids the land should have kept.
Before anything is appended the result is folded and checked: every parcel of the snapshot must
carry exactly one current id, otherwise nothing is written.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import parcel_identity as pi
import parcel_lineage as pl
from lineage_review_queue import steward_resolved
from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages
from lakehouse_ingest import append_batch_once
from parcel_lineage_to_silver import DATE, IDENTIFIER, PREFIX, SNAPSHOT_ID, read_pnus, sql_prefixes
from platform_contracts import (
    column_names,
    create_table_columns_sql,
    evolve_iceberg_table_to_contract,
    load_lakehouse_contract,
    partition_clause_sql,
    spark_sql_type,
)

JOB_NAME = "parcel_registry_to_silver"
CONTRACT = load_lakehouse_contract("silver.parcel_registry")
COLUMNS: tuple[str, ...] = column_names(CONTRACT)
# More new PNUs than this share of the snapshot means the snapshot is new, not a re-run.
NEW_SNAPSHOT_SHARE = 0.001


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--to-snapshot-id", required=True, help="silver.parcel_boundaries source_snapshot_id to register")
    parser.add_argument("--to-date", required=True, help="YYYY-MM-DD the snapshot reflects")
    parser.add_argument("--to-sido", required=True, help="Comma-separated sido prefixes of the snapshot's parcels")
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
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    for label in ("iceberg_catalog_name", "iceberg_namespace", "iceberg_table", "lineage_table"):
        if not IDENTIFIER.fullmatch(getattr(args, label)):
            raise ValueError(f"{label} must be a plain SQL identifier")
    for label in ("to_snapshot_id", "from_snapshot_id"):
        value = getattr(args, label)
        if value is not None and not SNAPSHOT_ID.fullmatch(value):
            raise ValueError(f"--{label.replace('_', '-')} has characters a snapshot id does not use")
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


def plan(
    registry: list[pi.RegistryRow],
    after: set[str],
    lineage: list[pl.Link],
    to_date: str,
    bootstrap_allowed: bool,
) -> tuple[str, list[pi.RegistryRow], dict[str, Any]]:
    """Decide the mode, produce the rows, and refuse a result that leaves a parcel without one id."""

    state = pi.fold(registry)
    if not state.current and not state.last_id_of_closed:
        if not bootstrap_allowed:
            raise ValueError("the registry holds nothing for this region; pass --bootstrap for the first registration")
        mode, rows, counts = "bootstrap", pi.bootstrap(after, to_date), {}
    else:
        effective, conflicts = pl.effective_links(lineage)
        if conflicts:
            raise ValueError(f"{len(conflicts)} identity conflicts in the lineage; resolve them before registering ids")
        unregistered = after - set(state.current)
        if len(unregistered) > NEW_SNAPSHOT_SHARE * max(len(after), 1):
            t = pi.advance(state.current, after, effective, to_date)
            mode = "advance"
        else:
            t = pi.upgrade(state, effective, to_date)
            mode = "upgrade"
        rows = t.rows
        counts = {"kept": t.kept, "carried": t.carried, "issued": t.issued, "retired": t.retired, "redirected": t.redirected}
    final = pi.fold(registry + rows)
    problems = pi.check_every_parcel_has_one_current_id({p: i for p, i in final.current.items() if p in after}, sorted(after))
    if problems:
        raise ValueError("refusing to register: " + "; ".join(problems[:10]))
    return mode, rows, {"mode": mode, "rows": len(rows), **counts, "current_ids": len(final.current), "snapshot_parcels": len(after)}


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_args(args)
    from pyspark.sql import SparkSession  # noqa: PLC0415

    now = datetime.now(timezone.utc).replace(microsecond=0)
    builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).config("spark.jars.packages", args.iceberg_packages).getOrCreate()
    cat = f"`{args.iceberg_catalog_name}`"
    table = f"{cat}.`{args.iceberg_namespace}`.`{args.iceberg_table}`"
    try:
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
        registry = [
            pi.RegistryRow(r["parcel_id"], r["pnu"], r["status"], r["valid_from"] or "", r["valid_to"], r["redirect_to"])
            for r in spark.sql(
                f"SELECT parcel_id, pnu, status, valid_from, valid_to, redirect_to FROM {table} "
                f"WHERE substr(pnu, 1, 2) IN ({sql_prefixes(region)})"
            ).collect()
        ]
        after = read_pnus(spark, cat, args.to_snapshot_id, args.to_sido)
        if not after:
            raise ValueError(f"no parcels in {args.to_snapshot_id} for {args.to_sido}")
        lineage: list[pl.Link] = []
        lineage_runs: list[str] = []
        if args.from_snapshot_id:
            pair = [
                r.asDict()
                for r in spark.sql(
                    f"SELECT predecessor_pnu, successor_pnu, relation, grade, evidence_kind, evidence_ref, derivation_run_id "
                    f"FROM {cat}.`{args.iceberg_namespace}`.`{args.lineage_table}` "
                    f"WHERE from_snapshot_id = '{args.from_snapshot_id}' AND to_snapshot_id = '{args.to_snapshot_id}'"
                ).collect()
            ]
            lineage_runs = [r["derivation_run_id"] for r in pair]
            # A steward's standing decision replaces the derived rows of its parcel (root ADR-0115 §9).
            for r in steward_resolved(pair):
                lineage.append(pl.Link(r["predecessor_pnu"] or "", r["successor_pnu"], r["relation"], r["grade"], r["evidence_kind"]))
        mode, rows, summary = plan(registry, after, lineage, args.to_date, args.bootstrap)
        run_id = "parcel-registry-" + hashlib.sha256(json.dumps({
            "to": args.to_snapshot_id, "from": args.from_snapshot_id, "sido": sorted(region.split(",")),
            "mode": mode, "lineage": sorted(set(lineage_runs)), "rules": pi.ID_RULES_VERSION,
            "registry_rows": len(registry),
        }, sort_keys=True).encode()).hexdigest()[:24]
        out = [
            {
                "parcel_id": r.parcel_id,
                "pnu": r.pnu,
                "status": r.status,
                "valid_from": r.valid_from or None,
                "valid_to": r.valid_to,
                "redirect_to": r.redirect_to,
                "to_snapshot_id": args.to_snapshot_id,
                "id_rules_version": pi.ID_RULES_VERSION,
                "derivation_run_id": run_id,
                "recorded_at_utc": now,
            }
            for r in rows
        ]
        appended = False
        if out:
            schema = ", ".join(f"{c['name']} {spark_sql_type(c['logical_type'])}" for c in CONTRACT["columns"])
            frame = spark.createDataFrame(out, schema=schema).select(*COLUMNS)
            appended = append_batch_once(spark, frame, COLUMNS, table, CONTRACT["table_name"])["appended"]
        summary.update({"job": JOB_NAME, "derivation_run_id": run_id, "appended": appended, "to_snapshot_id": args.to_snapshot_id})
    finally:
        spark.stop()
    if args.summary_output:
        Path(args.summary_output).write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("parcel-registry-summary-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
