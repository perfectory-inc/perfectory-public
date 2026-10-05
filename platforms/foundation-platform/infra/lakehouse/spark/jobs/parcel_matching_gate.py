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

The checks are the ones `map_matching_gate` states over plain Python values, run here as counts and
anti-joins on the executors: a national snapshot is forty million parcels, and no PNU set, code
list or registry comes to the driver. What the driver receives is a count and at most `SAMPLE`
items per reason, plus the lineage rows of the parcels that lack an attribute, which the steward
fold (`steward_resolved`) must read whole per parcel.

The verdict is `VERDICT_SCHEMA_VERSION`. A lakehouse bake binds its revision to the snapshot only
with a verdict that passed and whose `parcels.checked` equals `snapshot_parcel_count`, the parcels of
the whole snapshot (root ADR-0133 §5); a verdict over some sido is evidence, not a licence to bake.
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
import vworld_parcel_editions as editions
from lineage_review_queue import steward_resolved
from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages
from parcel_lineage_to_silver import IDENTIFIER, PREFIX, SNAPSHOT_ID, sql_prefixes

JOB_NAME = "parcel_matching_gate"
VERDICT_SCHEMA_VERSION = "foundation-platform.parcel_matching_verdict.v1"
QUALIFIED = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")
PNU_SHAPE = "^[0-9]{19}$"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--snapshot-id",
                        help="silver.parcel_boundaries source_snapshot_id to check: the served edition when omitted (ADR-0148 §1)")
    editions.add_reader_flag(parser)
    parser.add_argument("--sido", required=True, help="Comma-separated sido prefixes")
    parser.add_argument("--lineage-from-snapshot-id", help="Earlier snapshot of the lineage that reaches this one")
    parser.add_argument("--attribute", action="append", default=[], help="namespace.table:column_for_year=value, e.g. silver.land_individual_price:base_year=2026")
    parser.add_argument("--no-registry", action="store_true", help="The region has no parcel registry yet; skip check (b)")
    parser.add_argument("--summary-output")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    args = parser.parse_args(argv)
    allow = args.allow_non_served_edition
    args.snapshot_id = editions.served_reader_id(args.snapshot_id, allow, "--snapshot-id")
    args.lineage_from_snapshot_id = editions.edition_reader_id(args.lineage_from_snapshot_id, allow, "--lineage-from-snapshot-id")
    return args


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


def violation(frame: Any, column: str) -> tuple[int, list[str]]:
    """How many items `frame` holds and the first `gate.SAMPLE` of them in order."""

    count = frame.count()
    if not count:
        return 0, []
    sample = [row[0] for row in frame.select(column).orderBy(column).limit(gate.SAMPLE).collect()]
    return count, sample


def report(checked: int, violations: dict[str, tuple[int, list[str]]], allowed: dict[str, int]) -> dict:
    """The shape `map_matching_gate.GateReport.as_dict` gives, from counts and samples."""

    found = {reason: {"count": count, "sample": sample} for reason, (count, sample) in sorted(violations.items()) if count}
    return {
        "passed": not found,
        "checked": checked,
        "violations": found,
        "allowed": {reason: n for reason, n in sorted(allowed.items()) if n},
    }


def current_ids(registry: Any) -> Any:
    """`parcel_identity.fold` over registry rows as a join: (pnu, parcel_id) of every current id.

    The fold replays rows by (valid_from or valid_to, current last) and keeps, per PNU, the id its
    last `current` row opened unless a later `historic` or `redirected` row closes that same id.
    Two `current` rows of one PNU on one date are ordered by parcel id here, where the fold keeps
    input order; the registry writes one current row per PNU and date.
    """

    from pyspark.sql import functions as F  # noqa: PLC0415

    rows = registry.select(
        "pnu",
        "parcel_id",
        "status",
        F.expr("coalesce(nullif(CAST(valid_from AS STRING), ''), CAST(valid_to AS STRING), '')").alias("k"),
    )
    last = (
        rows.where(F.col("status") == pi.CURRENT)
        .groupBy("pnu")
        .agg(F.max(F.struct("k", "parcel_id")).alias("last"))
        .select("pnu", F.col("last.k").alias("k"), F.col("last.parcel_id").alias("parcel_id"))
    )
    closing = rows.where(F.col("status").isin(pi.HISTORIC, pi.REDIRECTED)).select(
        F.col("pnu").alias("c_pnu"), F.col("parcel_id").alias("c_id"), F.col("k").alias("c_k")
    )
    closed = last.join(
        closing,
        (last.pnu == closing.c_pnu) & (last.parcel_id == closing.c_id) & (closing.c_k > last.k),
        "left_semi",
    )
    return last.join(closed.select("pnu"), "pnu", "left_anti").select("pnu", "parcel_id")


def check_parcels(parcels: Any, official: Any, admin_codes: Any, current: Any | None) -> dict:
    """`map_matching_gate.check_parcels` as joins: `parcels` (pnu), `official` (region_cd, status),
    `admin_codes` (canonical_code), `current` (pnu, parcel_id) or None without a registry."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    checked = parcels.count()
    shaped = F.col("pnu").rlike(PNU_SHAPE)
    well = parcels.where(shaped)
    existing = F.broadcast(official.where(F.col("status") == gate.EXISTS).select(F.col("region_cd").alias("dong")))
    boundaries = F.broadcast(admin_codes.select(F.col("canonical_code").alias("emd")))
    violations = {
        gate.MALFORMED_PNU: violation(parcels.where(~shaped), "pnu"),
        gate.NO_LEGAL_DONG: violation(
            well.join(existing, F.substring("pnu", 1, 10) == F.col("dong"), "left_anti"), "pnu"
        ),
        gate.NO_BOUNDARY: violation(
            well.join(boundaries, F.concat(F.substring("pnu", 1, 8), F.lit("00")) == F.col("emd"), "left_anti"),
            "pnu",
        ),
    }
    if current is not None:
        held = well.join(current, "pnu", "left")
        violations[gate.NO_CURRENT_ID] = violation(held.where(F.col("parcel_id").isNull()), "pnu")
        violations[gate.ID_ON_TWO_PARCELS] = violation(
            held.where(F.col("parcel_id").isNotNull()).groupBy("parcel_id").count().where(F.col("count") > 1),
            "parcel_id",
        )
    return report(checked, violations, {})


def check_attribute(name: str, parcels: Any, held: Any, lineage: Any | None) -> dict:
    """`map_matching_gate.check_attribute` with the PNU sets on the executors.

    `held` is (pnu) of parcels the attribute source holds; `lineage` the rows of
    `silver.parcel_lineage` between the two snapshots, or None. Only the lineage rows of parcels
    that lack the attribute come to the driver, each parcel's rows whole, because the steward fold
    decides per parcel which rows stand.
    """

    from pyspark.sql import functions as F  # noqa: PLC0415

    checked = parcels.count()
    missing = parcels.join(held, "pnu", "left_anti")
    lacking = missing.count()
    violators: list[str] = []
    if lineage is not None and lacking:
        rows = (
            lineage.join(missing.select(F.col("pnu").alias("successor_pnu")), "successor_pnu", "left_semi")
            .join(
                held.select(F.col("pnu").alias("predecessor_pnu"), F.lit(True).alias("predecessor_held")),
                "predecessor_pnu",
                "left",
            )
            .collect()
        )
        held_before = {r["predecessor_pnu"] for r in rows if r["predecessor_held"]}
        predecessors: dict[str, list[str]] = collections.defaultdict(list)
        for r in steward_resolved([r.asDict() for r in rows]):
            if r["predecessor_pnu"] and pl.GRADE_RANK[r["grade"]] <= pl.GRADE_RANK["evidence_strong"]:
                predecessors[r["successor_pnu"]].append(r["predecessor_pnu"])
        for successor, olds in sorted(predecessors.items()):
            found = [old for old in olds if old in held_before]
            if found:
                violators.append(f"{successor}<-{found[0]}")
    return report(
        checked,
        {gate.attribute_held_elsewhere(name): (len(violators), sorted(violators)[: gate.SAMPLE])},
        {gate.attribute_absent(name): lacking - len(violators)},
    )


def attribute_frame(spark: Any, cat: str, spec: str, sido: str) -> tuple[str, Any]:
    table, _, where = spec.partition(":")
    namespace, name = table.split(".")
    condition = f"substr(pnu, 1, 2) IN ({sql_prefixes(sido)})"
    if where:
        column, value = where.split("=")
        condition += f" AND CAST({column} AS STRING) = '{value}'"
    return table, spark.sql(f"SELECT DISTINCT pnu FROM {cat}.`{namespace}`.`{name}` WHERE {condition}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_args(args)
    from pyspark.sql import SparkSession  # noqa: PLC0415

    builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).config("spark.jars.packages", args.iceberg_packages).getOrCreate()
    cat = f"`{args.iceberg_catalog_name}`"
    verdict: dict[str, Any] = {
        "schema_version": VERDICT_SCHEMA_VERSION,
        "job": JOB_NAME,
        "snapshot_id": args.snapshot_id,
        "sido": args.sido,
    }
    try:
        snapshot = f"{cat}.`silver`.`parcel_boundaries` WHERE source_snapshot_id = '{args.snapshot_id}'"
        verdict["snapshot_parcel_count"] = spark.sql(f"SELECT count(DISTINCT pnu) AS n FROM {snapshot}").collect()[0]["n"]
        parcels = spark.sql(
            f"SELECT DISTINCT pnu FROM {snapshot} AND substr(pnu, 1, 2) IN ({sql_prefixes(args.sido)})"
        ).persist()
        newest = spark.sql(f"SELECT CAST(max(snapshot_date) AS STRING) AS d FROM {cat}.`reference`.`legal_dong_code_snapshot`").collect()[0]["d"]
        official = spark.sql(
            f"SELECT region_cd, status FROM {cat}.`reference`.`legal_dong_code_snapshot` "
            + (f"WHERE snapshot_date = DATE '{newest}'" if newest else "WHERE false")
        )
        admin_codes = spark.sql(f"SELECT canonical_code FROM {cat}.`gold`.`administrative_boundary_served`")
        current = None
        if not args.no_registry:
            current = current_ids(
                spark.sql(
                    f"SELECT parcel_id, pnu, status, valid_from, valid_to FROM {cat}.`silver`.`parcel_registry` "
                    f"WHERE substr(pnu, 1, 2) IN ({sql_prefixes(args.sido)})"
                )
            )
        verdict["parcels"] = check_parcels(parcels, official, admin_codes, current)
        verdict["official_code_snapshot"] = newest
        lineage = None
        if args.lineage_from_snapshot_id:
            lineage = spark.sql(
                f"SELECT predecessor_pnu, successor_pnu, relation, grade, evidence_kind, evidence_ref "
                f"FROM {cat}.`silver`.`parcel_lineage` "
                f"WHERE from_snapshot_id = '{args.lineage_from_snapshot_id}' AND to_snapshot_id = '{args.snapshot_id}'"
            )
        verdict["attributes"] = {}
        for spec in args.attribute:
            name, held = attribute_frame(spark, cat, spec, args.sido)
            verdict["attributes"][spec] = check_attribute(name, parcels, held, lineage)
    finally:
        spark.stop()
    verdict["passed"] = verdict["parcels"]["passed"] and all(a["passed"] for a in verdict["attributes"].values())
    if args.summary_output:
        Path(args.summary_output).write_text(json.dumps(verdict, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("parcel-matching-gate-json " + json.dumps(verdict, ensure_ascii=False, sort_keys=True))
    return 0 if verdict["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
