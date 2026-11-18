#!/usr/bin/env python3
"""Publish a release's id registry, bridge file and changelog to Gold (root ADR-0113 §8).

  --unit admin   reads the snapshots of `silver.administrative_boundaries` (scope `national`);
  --unit parcel  reads `silver.parcel_registry` for one region (`--sido`, the scope), in Spark
                 (`parcel_place_id_release`, root ADR-0481); with `--probe-stale` it prints the regions
                 whose inputs moved since their last release and writes nothing.
The registry and the bridge are rewritten for (unit, scope) only when either differs from the release
already served (`place_id_release.release_changes`); the changelog is appended once per release id, and
only when the registry changed. Consumers holding an old reference resolve it through the bridge and
follow redirects.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import parcel_identity as pi
import parcel_place_id_release as parcel_release
import parcel_registry_frames as registry_frames
import place_id_release as rel
import place_identity_resolver as pir
import vworld_parcel_editions as editions
from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages
from lakehouse_ingest import append_batch_once
from parcel_lineage import IDENTIFIER, PREFIX
from platform_contracts import (
    admit_write,
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
    parser.add_argument("--release-id", help="The tile release or snapshot this publication belongs to")
    parser.add_argument("--sido", help="parcel: the region's two-digit sido prefix (the scope)")
    parser.add_argument("--probe-stale", action="store_true",
                        help="parcel: print the regions whose inputs moved since their last release; write nothing")
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
    if args.probe_stale:
        if args.unit != "parcel":
            raise ValueError("--probe-stale is the parcel unit's")
        assert_catalog_env()
        return
    if args.release_id is None:
        raise ValueError("--release-id is required")
    if args.unit == "parcel" and not (args.sido and PREFIX.fullmatch(args.sido)):
        raise ValueError("--unit parcel needs --sido, one two-digit sido prefix")
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


def published_release(spark: Any, catalog: str, namespace: str, unit: str, scope: str
                      ) -> tuple[list[rel.RegistryEntry], list[rel.BridgeEntry]] | None:
    """The registry and bridge (unit, scope) serves now, or None when nothing is published yet."""

    names = {name: f"{catalog}.{namespace}.place_id_{name}" for name in ("registry", "bridge")}
    if not all(spark.catalog.tableExists(name) for name in names.values()):
        return None
    where = f"WHERE unit = '{unit}' AND scope = '{scope}'"
    registry = [rel.RegistryEntry(r["place_id"], r["status"], r["current_code"], r["redirect_to"]) for r in spark.sql(
        f"SELECT place_id, status, current_code, redirect_to FROM `{catalog}`.`{namespace}`.`place_id_registry` {where}"
    ).collect()]
    bridge = [rel.BridgeEntry(r["place_id"], r["code"], r["valid_from"], r["valid_to"]) for r in spark.sql(
        f"SELECT place_id, code, valid_from, valid_to FROM `{catalog}`.`{namespace}`.`place_id_bridge` {where}"
    ).collect()]
    return (registry, bridge) if registry or bridge else None


def ensure_release_table(spark: Any, cat: str, namespace: str, name: str) -> str:
    """One release table, created and evolved to its contract; its qualified name."""

    contract = CONTRACTS[name]
    table = f"{cat}.`{namespace}`.`place_id_{name}`"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.`{namespace}`")
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
    return table


def registry_rows(spark: Any, cat: str, contract: Mapping[str, Any] | None = None) -> Any:
    """`silver.parcel_registry` as of the served edition (`parcel_registry_frames.registry_through`, root
    ADR-0490 §4a), with each id's region (`parcel_place_id_release.with_scope`) and `visible_at_utc`: when
    the row reached this read, its recording or the promotion that put its edition in the read
    (`vworld_parcel_editions.included_since`). A candidate's step is in the table before it is served;
    consumers see it on its promotion, and the regions it moved are released then."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    contract = contract or editions.load()
    served = editions.snapshot_id(contract, editions.served(contract))
    since = editions.included_since(contract)
    included = F.lit(None).cast("timestamp")
    for snapshot, instant in sorted(since.items()):
        included = F.when(F.col("to_snapshot_id") == snapshot, F.to_timestamp(F.lit(instant))).otherwise(included)
    rows = registry_frames.registry_through(spark.table(f"{cat}.`silver`.`parcel_registry`"), served, contract)
    return parcel_release.with_scope(rows.select(
        "parcel_id", "pnu", "status", "valid_from", "valid_to", "redirect_to", "recorded_at_utc",
        F.greatest(F.col("recorded_at_utc"), F.coalesce(included, F.col("recorded_at_utc"))).alias("visible_at_utc")))


def code_moved_scopes(spark: Any, cat: str, rows: Any, published: Any) -> set[str]:
    """The released regions a 법정동 code change reached since their release: the change table read
    as of each release time against now (`code_change_follow.prefixes_from_changes`, the views every
    reader uses, root ADR-0329 §3)."""

    import code_change_follow as follow  # noqa: PLC0415

    table = f"{cat}.`reference`.`legal_dong_code_change`"
    columns = ", ".join(follow.CHANGE_COLUMNS)
    try:
        after = [row.asDict() for row in spark.sql(f"SELECT {columns} FROM {table}").collect()]
    except Exception:  # noqa: BLE001 - no change table: no pair moves a region
        return set()
    moved: set[str] = set()
    times = published.groupBy("scope").agg({"published_at_utc": "max"}).withColumnRenamed("max(published_at_utc)", "at")
    by_time: dict[Any, list[str]] = {}
    for row in times.collect():
        by_time.setdefault(row["at"], []).append(row["scope"])
    for at, scopes in sorted(by_time.items()):
        try:
            before = [row.asDict() for row in spark.sql(
                f"SELECT {columns} FROM {table} TIMESTAMP AS OF '{at:%Y-%m-%d %H:%M:%S}'").collect()]
            prefixes: set[str] | None = follow.prefixes_from_changes(before, after)
        except Exception:  # noqa: BLE001 - an expired read is a region read again, never one skipped
            prefixes = None
        moved |= parcel_release.code_moved_scopes(rows.where(rows["scope"].isin(scopes)), prefixes)
    return moved


def published_parcel_rows(spark: Any, args: argparse.Namespace, name: str) -> Any | None:
    """The parcel unit's published rows of one release table, or None while it does not exist."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    if not spark.catalog.tableExists(f"{args.iceberg_catalog_name}.{args.gold_namespace}.place_id_{name}"):
        return None
    return (spark.table(f"`{args.iceberg_catalog_name}`.`{args.gold_namespace}`.`place_id_{name}`")
            .where(F.col("unit") == parcel_release.UNIT))


def probe_stale(spark: Any, args: argparse.Namespace) -> list[list[Any]]:
    """[[scope, ids]] of the regions to release, smallest first (`parcel_place_id_release.stale_scopes`)."""

    cat = f"`{args.iceberg_catalog_name}`"
    published = published_parcel_rows(spark, args, "registry")
    if published is None:
        published = spark.createDataFrame([], "scope string, published_at_utc timestamp")
    published = published.select("scope", "published_at_utc").cache()
    rows = registry_rows(spark, cat).cache()
    stale = parcel_release.stale_scopes(rows, published, code_moved_scopes(spark, cat, rows, published))
    return [[scope, ids] for scope, ids in stale]


def publish_parcel_region(spark: Any, args: argparse.Namespace, now: datetime) -> dict[str, Any]:
    """One region's release: its registry and bridge partitions replaced, and its changelog appended
    when its registry differs from what was published."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    cat = f"`{args.iceberg_catalog_name}`"
    scope = args.sido
    rows = registry_rows(spark, cat).where(F.col("scope") == scope).cache()
    remap, ambiguous = pir.pairs_from_lakehouse(spark, args.iceberg_catalog_name)
    registry = parcel_release.registry_of(rows).cache()
    bridge = parcel_release.bridge_of(rows, remap).cache()
    shapes = {"registry": ("scope", "place_id", "status", "current_code", "redirect_to"),
              "bridge": ("scope", "place_id", "code", "valid_from", "valid_to")}
    published = {name: published_parcel_rows(spark, args, name) for name in shapes}
    published = {name: (frame.where(F.col("scope") == scope).select(*shapes[name]) if frame is not None
                        else spark.createDataFrame([], registry.select(*shapes[name]).schema if name == "registry"
                                                   else bridge.select(*shapes[name]).schema))
                 for name, frame in published.items()}
    fresh = {"registry": registry, "bridge": bridge}
    differs = {name: not (fresh[name].select(*shapes[name]).exceptAll(published[name]).isEmpty()
                          and published[name].exceptAll(fresh[name].select(*shapes[name])).isEmpty())
               for name in shapes}
    changes = parcel_release.changelog_of(published["registry"], registry).cache()
    tail = {"unit": F.lit(parcel_release.UNIT), "release_id": F.lit(args.release_id),
            "published_at_utc": F.lit(now).cast("timestamp")}
    summary: dict[str, Any] = {"scope": scope, "code_pairs": len(remap), "code_pairs_ambiguous": len(ambiguous),
                               "registry_changed": differs["registry"], "bridge_changed": differs["bridge"],
                               # The probe asks only for a region whose inputs moved; writing it even when the
                               # result is equal moves its release time, so the same change is not read again.
                               "published": True}
    if summary["published"]:
        frames = {**fresh, "changelog": changes.withColumn("scope", F.lit(scope)).withColumn(
            "previous_release_id", F.lit(args.previous_release_id).cast("string"))}
        for name, frame in frames.items():
            contract = CONTRACTS[name]
            table = ensure_release_table(spark, cat, args.gold_namespace, name)
            columns = column_names(contract)
            for column, value in tail.items():
                frame = frame.withColumn(column, value)
            frame = frame.select(*columns)
            if name == "changelog":
                if differs["registry"] and not changes.isEmpty():
                    summary["changelog_appended"] = append_batch_once(spark, frame, columns, table,
                                                                      contract["table_name"])["appended"]
                continue
            frame.createOrReplaceTempView(f"parcel_release_{name}")
            admit_write(contract["table_name"], "replace", target=table)
            # Dynamic partition overwrite: only the (parcel, scope) partition is replaced.
            spark.sql(f"INSERT OVERWRITE {table} SELECT {', '.join(columns)} FROM parcel_release_{name}")
    summary.update({
        "registry": registry.count(),
        "current": registry.where(F.col("status") == pi.CURRENT).count(),
        "bridge": bridge.count(),
        "bridge_from_code_pairs": bridge.where(F.col("valid_from") == parcel_release.PAIR_SOURCE).count(),
        "changes": {row["change"]: row["count"] for row in changes.groupBy("change").count().collect()},
    })
    rows.unpersist()
    return summary


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
        if args.probe_stale:
            print("place-id-parcel-probe " + json.dumps({"stale": probe_stale(spark, args)}, sort_keys=True))
            return 0
        if args.unit == "parcel":
            summary.update(publish_parcel_region(spark, args, now))
            return emit(args, summary)
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
            # Root ADR-0288 §3: bridge the old codes the snapshots never carried (a 분구's abolished
            # dong codes) to their current place, from the code-change pairs the resolver reads.
            remap, ambiguous = pir.pairs_from_lakehouse(spark, args.iceberg_catalog_name)
            registry, bridge, changes = rel.admin_release(snapshots, remap, ambiguous)
            summary["snapshots"] = [label for label, _ in snapshots]
            summary["code_pairs_bridged"] = len(remap)
            summary["code_pairs_ambiguous"] = len(ambiguous)
        registry_changed, bridge_changed = rel.release_changes(
            published_release(spark, args.iceberg_catalog_name, args.gold_namespace, args.unit, scope), registry, bridge)
        summary.update(registry_changed=registry_changed, bridge_changed=bridge_changed,
                       published=registry_changed or bridge_changed)
        out = rows_for(args.unit, scope, args.release_id, args.previous_release_id, now, registry, bridge, changes)
        for name, contract in CONTRACTS.items() if summary["published"] else ():
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
                if out[name] and registry_changed:
                    summary["changelog_appended"] = append_batch_once(spark, frame, columns, table, contract["table_name"])["appended"]
            else:
                frame.createOrReplaceTempView(f"release_{name}")
                admit_write(contract["table_name"], "replace", target=table)
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
    return emit(args, summary)


def emit(args: argparse.Namespace, summary: dict[str, Any]) -> int:
    """The run's summary: the file the caller asked for, and the last stdout line."""

    if args.summary_output:
        Path(args.summary_output).write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("place-id-release-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
