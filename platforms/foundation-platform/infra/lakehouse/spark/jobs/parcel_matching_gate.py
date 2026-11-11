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
Check (b) reads `silver.parcel_registry`, which the `parcel_registry` job creates (root ADR-0243). When
that table does not exist yet the gate does not run: it writes a verdict naming the absent input and
exits `INPUT_ABSENT` (3), so the job or rehearsal behind it waits (root ADR-0265); `--no-registry`
records `skipped`. A bake binds only to a verdict whose registry was `checked`.

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

import legal_dong_code_change_views as code_changes
import map_matching_gate as gate
import parcel_lineage as pl
import parcel_registry_frames as registry_frames
import vworld_parcel_editions as editions
from lineage_review_queue import steward_resolved
from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages
from parcel_lineage_to_silver import IDENTIFIER, PREFIX, SNAPSHOT_ID, sql_prefixes
from legal_dong_code_snapshot_to_reference import newest_snapshot
from platform_contracts import current_rows_of_versioned, load_lakehouse_contract, pinned_edition

JOB_NAME = "parcel_matching_gate"
VERDICT_SCHEMA_VERSION = "foundation-platform.parcel_matching_verdict.v1"
QUALIFIED = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")
PNU_SHAPE = "^[0-9]{19}$"
INPUT_ABSENT = 3
REGISTRY_TABLE = "silver.parcel_registry"
CODE_CHANGE_COLUMNS = ("kind", "old_code", "new_code", "level", "effective_date", "source")
# The national edition's bound on parcels whose legal dong nothing explains (map_matching_gate.UNEXPLAINED_LEGAL_DONG),
# like a dbt test's `error_if`: past it the gate refuses, within it the parcels are sampled in the verdict.
# 2026-11-11: 2 of 202609's 39,876,546 (a 리 code no list ever held, an abolished 동 with no pair). 100 is
# 0.00025% of an edition; a reorganization the change table misses is thousands and is still refused.
UNEXPLAINED_LEGAL_DONG_BOUND = 100
# (pnu, parcel_id) of every current id of registry rows (root ADR-0113 §7(b)); the gate reads the registry
# as of the snapshot it checks (root ADR-0490 §4a), so a candidate is checked against its own step.
current_ids = registry_frames.current_ids


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


def registry_state(no_registry: bool, exists: bool) -> str:
    """How check (b) runs (root ADR-0265): `checked`, `skipped` (--no-registry), or `wait` (the table
    does not exist yet: the gate must not run)."""

    if no_registry:
        return "skipped"
    return "checked" if exists else "wait"


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


def report(checked: int, violations: dict[str, tuple[int, list[str]]], allowed: dict[str, int],
           unexplained_sample: list[str] | None = None) -> dict:
    """The shape `map_matching_gate.GateReport.as_dict` gives, from counts and samples."""

    found = {reason: {"count": count, "sample": sample} for reason, (count, sample) in sorted(violations.items()) if count}
    made = {
        "passed": not found,
        "checked": checked,
        "violations": found,
        "allowed": {reason: n for reason, n in sorted(allowed.items()) if n},
    }
    if unexplained_sample:
        made["unexplained_sample"] = unexplained_sample
    return made


def check_parcels(parcels: Any, official: Any, admin_codes: Any, current: Any | None,
                  renumbered: Any | None = None, unexplained_bound: int = 0) -> dict:
    """`map_matching_gate.check_parcels` as joins: `parcels` (pnu), `official` (region_cd, status),
    `admin_codes` (canonical_code), `current` (pnu, parcel_id) or None without a registry, `renumbered`
    (renumbered_from, renumbered_to) or None: the small {legal dong: successor} map, broadcast."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    checked = parcels.count()
    shaped = F.col("pnu").rlike(PNU_SHAPE)
    well = parcels.where(shaped)
    existing = F.broadcast(official.where(F.col("status") == gate.EXISTS).select(F.col("region_cd").alias("dong")))
    boundaries = F.broadcast(admin_codes.select(F.col("canonical_code").alias("emd")))
    absent = well.join(existing, F.substring("pnu", 1, 10) == F.col("dong"), "left_anti")
    if renumbered is None:
        renumbered = parcels.sparkSession.createDataFrame([], "renumbered_from string, renumbered_to string")
    # A renumbered dong is placed under its successor only when the successor exists (the pure gate's rule).
    successors = F.broadcast(renumbered.join(existing, F.col("renumbered_to") == F.col("dong"), "left_semi"))
    moved = absent.join(successors, F.substring("pnu", 1, 10) == F.col("renumbered_from"), "left_semi")
    stray = absent.join(successors, F.substring("pnu", 1, 10) == F.col("renumbered_from"), "left_anti")
    # A parcel with no place has no boundary to check (the pure gate skips it too).
    placed = (well.join(stray, "pnu", "left_anti")
              .join(successors, F.substring("pnu", 1, 10) == F.col("renumbered_from"), "left")
              .select("pnu", F.coalesce(F.col("renumbered_to"), F.substring("pnu", 1, 10)).alias("placed_dong")))
    strays, stray_sample = violation(stray, "pnu")
    over = strays > unexplained_bound
    violations = {
        gate.MALFORMED_PNU: violation(parcels.where(~shaped), "pnu"),
        gate.NO_LEGAL_DONG: (strays, stray_sample) if over else (0, []),
        gate.NO_BOUNDARY: violation(
            placed.join(boundaries, F.concat(F.substring("placed_dong", 1, 8), F.lit("00")) == F.col("emd"),
                        "left_anti").select("pnu"),
            "pnu",
        ),
    }
    allowed = {gate.RENUMBERED_LEGAL_DONG: moved.count(), gate.UNEXPLAINED_LEGAL_DONG: 0 if over else strays}
    if current is not None:
        held = well.join(current, "pnu", "left")
        violations[gate.NO_CURRENT_ID] = violation(held.where(F.col("parcel_id").isNull()), "pnu")
        violations[gate.ID_ON_TWO_PARCELS] = violation(
            held.where(F.col("parcel_id").isNotNull()).groupBy("parcel_id").count().where(F.col("count") > 1),
            "parcel_id",
        )
    return report(checked, violations, allowed, [] if over else stray_sample)


def renumbered_into_existing(spark: Any, cat: str, official: Any) -> Any:
    """(renumbered_from, renumbered_to): each legal dong an official 1:1 pair in force renumbered into a code
    the newest list holds (legal_dong_code_change_views.renumbered_into, root ADR-0329), broadcast-sized.

    The change table and the changed codes are a few thousand rows; the code list stays on the executors
    except for the codes the pairs name."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    rows = [row.asDict() for row in spark.table(f"{cat}.`reference`.`legal_dong_code_change`")
            .select(*CODE_CHANGE_COLUMNS).collect()]
    named = sorted({code for old, news in code_changes.leaf_pairs(rows).items() for code in (old, *news)})
    existing = []
    if named:
        existing = [row.region_cd for row in official.where((F.col("status") == gate.EXISTS)
                                                            & F.col("region_cd").isin(named))
                    .select("region_cd").distinct().collect()]
    pairs = sorted(code_changes.renumbered_into(rows, existing).items())
    return spark.createDataFrame(pairs, "renumbered_from string, renumbered_to string")


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
        # Before keeping some successors' rows: a replaced code change link and the one in force reach
        # different successors (root ADR-0329 §3).
        from parcel_lineage_section import drop_outranked_code_changes_frame  # noqa: PLC0415

        rows = (
            drop_outranked_code_changes_frame(lineage).join(missing.select(F.col("pnu").alias("successor_pnu")), "successor_pnu", "left_semi")
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
    from pyspark.sql import functions as F  # noqa: PLC0415

    rows = current_rows_of_versioned(spark.table(f"{cat}.{namespace}.{name}"), table)
    return table, rows.where(F.expr(condition)).select("pnu").distinct()


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
        state = registry_state(args.no_registry,
                               spark.catalog.tableExists(f"{args.iceberg_catalog_name}.{REGISTRY_TABLE}"))
        verdict["registry"] = state
        if state == "wait":
            verdict.update(passed=False, inputs_absent=[REGISTRY_TABLE])
            if args.summary_output:
                Path(args.summary_output).write_text(json.dumps(verdict, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            print("parcel-matching-gate-json " + json.dumps(verdict, ensure_ascii=False, sort_keys=True))
            return INPUT_ABSENT
        # The one edition checked, by the contract (root ADR-0286 §3, ADR-0377); its rows are all open
        # (2026-11-04: 0 closed rows in either edition), so the predicate keeps every one.
        edition = pinned_edition(spark.table(f"{cat}.`silver`.`parcel_boundaries`"),
                                 load_lakehouse_contract("silver.parcel_boundaries"), args.snapshot_id)
        verdict["snapshot_parcel_count"] = edition.select("pnu").distinct().count()
        parcels = (edition.where(f"substr(pnu, 1, 2) IN ({sql_prefixes(args.sido)})")
                   .select("pnu").distinct().persist())
        # The newest snapshot of the code list, by its contract (root ADR-0286 §3).
        code_list = newest_snapshot(spark, f"{cat}.`reference`.`legal_dong_code_snapshot`")
        dated = code_list.selectExpr("CAST(snapshot_date AS STRING) AS d").limit(1).collect()
        newest = dated[0]["d"] if dated else None
        official = code_list.select("region_cd", "status")
        admin_codes = spark.sql(f"SELECT canonical_code FROM {cat}.`gold`.`administrative_boundary_served`")
        current = None
        if state == "checked":
            current = current_ids(registry_frames.registry_through(
                spark.sql(
                    f"SELECT parcel_id, pnu, status, valid_from, valid_to, to_snapshot_id FROM {cat}.`silver`.`parcel_registry` "
                    f"WHERE substr(pnu, 1, 2) IN ({sql_prefixes(args.sido)})"
                ),
                args.snapshot_id,
            ))
        renumbered = renumbered_into_existing(spark, cat, official)
        verdict["renumbered_legal_dongs"] = renumbered.count()
        verdict["parcels"] = check_parcels(parcels, official, admin_codes, current, renumbered,
                                           UNEXPLAINED_LEGAL_DONG_BOUND)
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
