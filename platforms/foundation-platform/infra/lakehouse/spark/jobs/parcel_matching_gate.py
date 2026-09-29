#!/usr/bin/env python3
"""Run the matching gate (root ADR-0113 §7) over one cadastral snapshot before its parcels are baked.

Checks, from the lakehouse only:
  (a) every parcel's legal dong exists in the newest official code list and its 읍면동 has a
      served administrative boundary;
  (b) every parcel carries exactly one current parcel id (`silver.parcel_registry`), when the
      region has a registry;
  (c) every attribute named with --attribute (e.g. `silver.land_individual_price`) is attached to
      each parcel whose sources hold it, following `silver.parcel_lineage` for renumbered parcels.
Writes the verdict JSON and exits 1 when the gate refuses, so a bake behind it does not start.
"""

from __future__ import annotations

import argparse
import collections
import json
import re
from pathlib import Path
from typing import Any

import map_matching_gate as gate
import parcel_identity as pi
import parcel_lineage as pl
from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages
from parcel_lineage_to_silver import IDENTIFIER, PREFIX, SNAPSHOT_ID, read_pnus, sql_prefixes

JOB_NAME = "parcel_matching_gate"
QUALIFIED = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--snapshot-id", required=True, help="silver.parcel_boundaries source_snapshot_id to check")
    parser.add_argument("--sido", required=True, help="Comma-separated sido prefixes")
    parser.add_argument("--lineage-from-snapshot-id", help="Earlier snapshot of the lineage that reaches this one")
    parser.add_argument("--attribute", action="append", default=[], help="namespace.table:column_for_year=value, e.g. silver.land_individual_price:base_year=2026")
    parser.add_argument("--no-registry", action="store_true", help="The region has no parcel registry yet; skip check (b)")
    parser.add_argument("--summary-output")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    if not IDENTIFIER.fullmatch(args.iceberg_catalog_name):
        raise ValueError("--iceberg-catalog-name must be a plain SQL identifier")
    for value in (args.snapshot_id, args.lineage_from_snapshot_id):
        if value is not None and not SNAPSHOT_ID.fullmatch(value):
            raise ValueError(f"{value!r} has characters a snapshot id does not use")
    if not all(PREFIX.fullmatch(p) for p in args.sido.split(",")):
        raise ValueError("--sido must be comma-separated two-digit sido prefixes")
    for spec in args.attribute:
        table, _, where = spec.partition(":")
        if not QUALIFIED.fullmatch(table):
            raise ValueError(f"--attribute {spec!r}: expected namespace.table[:column=value]")
        if where and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*=[A-Za-z0-9_-]+", where):
            raise ValueError(f"--attribute {spec!r}: the filter must be column=value")
    assert_catalog_env()


def attribute_pnus(spark: Any, cat: str, spec: str, sido: str) -> tuple[str, set[str]]:
    table, _, where = spec.partition(":")
    namespace, name = table.split(".")
    condition = f"substr(pnu, 1, 2) IN ({sql_prefixes(sido)})"
    if where:
        column, value = where.split("=")
        condition += f" AND CAST({column} AS STRING) = '{value}'"
    rows = spark.sql(f"SELECT DISTINCT pnu FROM {cat}.`{namespace}`.`{name}` WHERE {condition}").collect()
    return table, {r["pnu"] for r in rows}


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_args(args)
    from pyspark.sql import SparkSession  # noqa: PLC0415

    builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).config("spark.jars.packages", args.iceberg_packages).getOrCreate()
    cat = f"`{args.iceberg_catalog_name}`"
    verdict: dict[str, Any] = {"job": JOB_NAME, "snapshot_id": args.snapshot_id, "sido": args.sido}
    try:
        pnus = read_pnus(spark, cat, args.snapshot_id, args.sido)
        newest = spark.sql(f"SELECT CAST(max(snapshot_date) AS STRING) AS d FROM {cat}.`reference`.`legal_dong_code_snapshot`").collect()[0]["d"]
        official = {
            r["region_cd"]: (r["full_name"], r["status"])
            for r in spark.sql(
                f"SELECT region_cd, full_name, status FROM {cat}.`reference`.`legal_dong_code_snapshot` WHERE snapshot_date = DATE '{newest}'"
            ).collect()
        } if newest else {}
        admin_codes = {r["canonical_code"] for r in spark.sql(f"SELECT canonical_code FROM {cat}.`gold`.`administrative_boundary_served`").collect()}
        current_ids = None
        if not args.no_registry:
            rows = spark.sql(
                f"SELECT parcel_id, pnu, status, valid_from, valid_to, redirect_to FROM {cat}.`silver`.`parcel_registry` "
                f"WHERE substr(pnu, 1, 2) IN ({sql_prefixes(args.sido)})"
            ).collect()
            current_ids = pi.fold(
                pi.RegistryRow(r["parcel_id"], r["pnu"], r["status"], r["valid_from"] or "", r["valid_to"], r["redirect_to"]) for r in rows
            ).current
        verdict["parcels"] = gate.check_parcels(pnus, official, admin_codes, current_ids).as_dict()
        verdict["official_code_snapshot"] = newest
        predecessors: dict[str, list[str]] = collections.defaultdict(list)
        if args.lineage_from_snapshot_id:
            for r in spark.sql(
                f"SELECT predecessor_pnu, successor_pnu, grade FROM {cat}.`silver`.`parcel_lineage` "
                f"WHERE from_snapshot_id = '{args.lineage_from_snapshot_id}' AND to_snapshot_id = '{args.snapshot_id}' "
                f"AND predecessor_pnu IS NOT NULL"
            ).collect():
                if pl.GRADE_RANK[r["grade"]] <= pl.GRADE_RANK["evidence_strong"]:
                    predecessors[r["successor_pnu"]].append(r["predecessor_pnu"])
        verdict["attributes"] = {}
        for spec in args.attribute:
            name, held = attribute_pnus(spark, cat, spec, args.sido)
            verdict["attributes"][spec] = gate.check_attribute(name, pnus, held, predecessors).as_dict()
    finally:
        spark.stop()
    verdict["passed"] = verdict["parcels"]["passed"] and all(a["passed"] for a in verdict["attributes"].values())
    if args.summary_output:
        Path(args.summary_output).write_text(json.dumps(verdict, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("parcel-matching-gate-json " + json.dumps(verdict, ensure_ascii=False, sort_keys=True))
    return 0 if verdict["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
