#!/usr/bin/env python3
"""Publish a release's id registry, bridge file and changelog to Gold (root ADR-0113 §8).

  --unit admin   reads the snapshots of `silver.administrative_boundaries` (scope `national`);
  --unit parcel  reads `silver.parcel_registry` for the named sido prefixes (scope = the prefixes).
The registry and the bridge are rewritten for (unit, scope); the changelog is appended once per
release id. Consumers holding an old reference resolve it through the bridge and follow redirects.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import parcel_identity as pi
import place_id_release as rel
from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages
from lakehouse_ingest import append_batch_once
from parcel_lineage_to_silver import DATE, IDENTIFIER, PREFIX, sql_prefixes
from platform_contracts import (
    column_names,
    create_table_columns_sql,
    evolve_iceberg_table_to_contract,
    load_lakehouse_contract,
    partition_clause_sql,
    spark_sql_type,
)

JOB_NAME = "place_id_release_to_gold"
CONTRACTS = {name: load_lakehouse_contract(f"gold.place_id_{name}") for name in ("registry", "bridge", "changelog")}
RELEASE_ID = re.compile(r"^[A-Za-z0-9._:-]{1,120}$")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--unit", required=True, choices=["admin", "parcel"])
    parser.add_argument("--release-id", required=True, help="The tile release or snapshot this publication belongs to")
    parser.add_argument("--sido", help="parcel: comma-separated sido prefixes (the scope)")
    parser.add_argument("--previous-as-of", help="parcel: YYYY-MM-DD of the previous release")
    parser.add_argument("--as-of", help="parcel: YYYY-MM-DD of this release")
    parser.add_argument("--previous-release-id", help="The release the changelog compares against")
    parser.add_argument("--summary-output")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--gold-namespace", default="gold")
    parser.add_argument("--allow-non-smoke-write", action="store_true")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    for label in ("iceberg_catalog_name", "gold_namespace"):
        if not IDENTIFIER.fullmatch(getattr(args, label)):
            raise ValueError(f"{label} must be a plain SQL identifier")
    for value in (args.release_id, args.previous_release_id):
        if value is not None and not RELEASE_ID.fullmatch(value):
            raise ValueError(f"{value!r} is not a release id")
    if args.unit == "parcel":
        if not args.sido or not all(PREFIX.fullmatch(p) for p in args.sido.split(",")):
            raise ValueError("--unit parcel needs --sido")
        if not (args.as_of and DATE.fullmatch(args.as_of) and args.previous_as_of and DATE.fullmatch(args.previous_as_of)):
            raise ValueError("--unit parcel needs --previous-as-of and --as-of as YYYY-MM-DD")
    if args.gold_namespace == "gold" and not args.allow_non_smoke_write:
        raise ValueError("writing the gold namespace needs --allow-non-smoke-write")
    assert_catalog_env()


def rows_for(
    unit: str, scope: str, release_id: str, previous_release_id: str | None, now: datetime,
    registry: list[rel.RegistryEntry], bridge: list[rel.BridgeEntry], changes: list[rel.Change],
) -> dict[str, list[dict[str, Any]]]:
    head = {"unit": unit, "scope": scope}
    tail = {"release_id": release_id, "published_at_utc": now}
    return {
        "registry": [{**head, "place_id": r.place_id, "status": r.status, "current_code": r.current_code, "redirect_to": r.redirect_to, **tail} for r in registry],
        "bridge": [{**head, "place_id": b.place_id, "code": b.code, "valid_from": b.valid_from, "valid_to": b.valid_to, **tail} for b in bridge],
        "changelog": [
            {**head, "place_id": c.place_id, "change": c.change, "from_code": c.from_code, "to_code": c.to_code,
             "redirect_to": c.redirect_to, "previous_release_id": previous_release_id, **tail}
            for c in changes
        ],
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_args(args)
    from pyspark.sql import SparkSession  # noqa: PLC0415

    now = datetime.now(timezone.utc).replace(microsecond=0)
    builder = (
        SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).config("spark.jars.packages", args.iceberg_packages).getOrCreate()
    cat = f"`{args.iceberg_catalog_name}`"
    summary: dict[str, Any] = {"job": JOB_NAME, "unit": args.unit, "release_id": args.release_id}
    try:
        if args.unit == "admin":
            scope = "national"
            order = spark.sql(
                f"SELECT source_snapshot_id FROM {cat}.`silver`.`administrative_boundaries` "
                f"GROUP BY source_snapshot_id ORDER BY max(ingested_at_utc), source_snapshot_id"
            ).collect()
            snapshots = []
            for row in order:
                label = row["source_snapshot_id"]
                units = {
                    r["administrative_unit_id"]: r["canonical_code"]
                    for r in spark.sql(
                        f"SELECT administrative_unit_id, canonical_code FROM {cat}.`silver`.`administrative_boundaries` "
                        f"WHERE source_snapshot_id = '{label}'"
                    ).collect()
                }
                snapshots.append((label, units))
            registry, bridge, changes = rel.admin_release(snapshots)
            summary["snapshots"] = [label for label, _ in snapshots]
        else:
            scope = args.sido
            rows = [
                pi.RegistryRow(r["parcel_id"], r["pnu"], r["status"], r["valid_from"] or "", r["valid_to"], r["redirect_to"])
                for r in spark.sql(
                    f"SELECT parcel_id, pnu, status, valid_from, valid_to, redirect_to FROM {cat}.`silver`.`parcel_registry` "
                    f"WHERE substr(pnu, 1, 2) IN ({sql_prefixes(args.sido)})"
                ).collect()
            ]
            registry, bridge = rel.parcel_registry(rows), rel.parcel_bridge(rows)
            changes = rel.parcel_changelog(rows, args.previous_as_of, args.as_of)
        out = rows_for(args.unit, scope, args.release_id, args.previous_release_id, now, registry, bridge, changes)
        for name, contract in CONTRACTS.items():
            table = f"{cat}.`{args.gold_namespace}`.`place_id_{name}`"
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.`{args.gold_namespace}`")
            spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
{create_table_columns_sql(contract)}
                )
                USING iceberg
                {partition_clause_sql(contract)}
                TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
                """
            )
            evolve_iceberg_table_to_contract(spark, table, contract)
            columns = column_names(contract)
            schema = ", ".join(f"{c['name']} {spark_sql_type(c['logical_type'])}" for c in contract["columns"])
            frame = spark.createDataFrame(out[name], schema=schema).select(*columns)
            if name == "changelog":
                if out[name]:
                    summary["changelog_appended"] = append_batch_once(spark, frame, columns, table, contract["table_name"])["appended"]
            else:
                frame.createOrReplaceTempView(f"release_{name}")
                spark.sql(f"INSERT OVERWRITE {table} SELECT {', '.join(columns)} FROM release_{name}")
        counts: dict[str, int] = {}
        for c in changes:
            counts[c.change] = counts.get(c.change, 0) + 1
        summary.update({
            "scope": scope,
            "registry": len(registry),
            "current": sum(1 for r in registry if r.status == pi.CURRENT),
            "bridge": len(bridge),
            "changes": counts,
        })
    finally:
        spark.stop()
    if args.summary_output:
        Path(args.summary_output).write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("place-id-release-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
