#!/usr/bin/env python3
"""Derive `silver.parcel_lineage` for one pair of cadastral snapshots (root ADR-0113 §1·§4·§5).

Reads, all from the lakehouse:
  - the parcels of both snapshots (`silver.parcel_boundaries`, by `source_snapshot_id`),
  - the official code list at the later snapshot (`reference.legal_dong_code_snapshot`),
  - the land movement history (`silver.land_transfer_history`),
  - optionally the building register titles of both snapshots (`silver.building_register_titles`),
  - optionally ownership facts (JSONL, old and new side) that lift attribute evidence to strong.
Every rule lives in `parcel_lineage.py`; this job only feeds it and appends what it returns, once
per (snapshots, regions, code snapshot, rules version).

Scope is the regions named: the sido prefixes of the earlier snapshot and of the later one, which
differ when a sido was renumbered (29,46 -> 12). The summary carries the count reconciliation and
the grade counts the matching gate and the Slack report read.
"""

from __future__ import annotations

import argparse
import collections
import functools
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import parcel_lineage as pl
from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages
from lakehouse_ingest import append_batch_once
from platform_contracts import (
    column_names,
    create_table_columns_sql,
    evolve_iceberg_table_to_contract,
    load_lakehouse_contract,
    partition_clause_sql,
    spark_sql_type,
)

JOB_NAME = "parcel_lineage_to_silver"
CONTRACT = load_lakehouse_contract("silver.parcel_lineage")
COLUMNS: tuple[str, ...] = column_names(CONTRACT)
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SNAPSHOT_ID = re.compile(r"^[A-Za-z0-9._:-]{1,200}$")
PREFIX = re.compile(r"^[0-9]{2}$")
DATE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--from-snapshot-id", required=True, help="silver.parcel_boundaries source_snapshot_id, earlier")
    parser.add_argument("--to-snapshot-id", required=True, help="silver.parcel_boundaries source_snapshot_id, later")
    parser.add_argument("--from-date", required=True, help="YYYY-MM-DD the earlier snapshot reflects; history before it is already in it")
    parser.add_argument("--to-date", required=True, help="YYYY-MM-DD the later snapshot reflects")
    parser.add_argument("--from-sido", required=True, help="Comma-separated sido prefixes of the earlier snapshot, e.g. 29,46")
    parser.add_argument("--to-sido", required=True, help="Comma-separated sido prefixes of the later snapshot, e.g. 12")
    parser.add_argument("--code-snapshot-date", help="reference.legal_dong_code_snapshot date; default: latest on or before --to-date")
    parser.add_argument("--building-from-snapshot-id", help="silver.building_register_titles source_snapshot_id, earlier")
    parser.add_argument("--building-to-snapshot-id", help="silver.building_register_titles source_snapshot_id, later")
    parser.add_argument("--ownership-old-jsonl", help="{pnu, owner_kind, co_owner_count} for the earlier side")
    parser.add_argument("--ownership-new-jsonl", help="{pnu, owner_kind, co_owner_count, area_m2, land_category_code} for the later side")
    parser.add_argument("--summary-output")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-namespace", default="silver")
    parser.add_argument("--iceberg-table", default="parcel_lineage")
    parser.add_argument("--allow-non-smoke-write", action="store_true")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    for label in ("iceberg_catalog_name", "iceberg_namespace", "iceberg_table"):
        if not IDENTIFIER.fullmatch(getattr(args, label)):
            raise ValueError(f"{label} must be a plain SQL identifier")
    for label in ("from_snapshot_id", "to_snapshot_id", "building_from_snapshot_id", "building_to_snapshot_id"):
        value = getattr(args, label)
        if value is not None and not SNAPSHOT_ID.fullmatch(value):
            raise ValueError(f"--{label.replace('_', '-')} has characters a snapshot id does not use")
    for label in ("from_date", "to_date", "code_snapshot_date"):
        value = getattr(args, label)
        if value is not None and not DATE.fullmatch(value):
            raise ValueError(f"--{label.replace('_', '-')} must be YYYY-MM-DD")
    if args.from_date >= args.to_date:
        raise ValueError("--from-date must be before --to-date")
    for label in ("from_sido", "to_sido"):
        if not all(PREFIX.fullmatch(p) for p in getattr(args, label).split(",")):
            raise ValueError(f"--{label.replace('_', '-')} must be comma-separated two-digit sido prefixes")
    if bool(args.building_from_snapshot_id) != bool(args.building_to_snapshot_id):
        raise ValueError("--building-from-snapshot-id and --building-to-snapshot-id go together")
    if not args.iceberg_table.endswith("_smoke") and not args.allow_non_smoke_write:
        raise ValueError(f"writing {args.iceberg_table} needs --allow-non-smoke-write")
    assert_catalog_env()


def derivation_run_id(args: argparse.Namespace, code_snapshot_date: str) -> str:
    """The same inputs give the same id, so a re-run is recognised and not appended twice."""

    key = json.dumps(
        {
            "from": args.from_snapshot_id,
            "to": args.to_snapshot_id,
            "from_sido": sorted(args.from_sido.split(",")),
            "to_sido": sorted(args.to_sido.split(",")),
            "codes": code_snapshot_date,
            "building": [args.building_from_snapshot_id, args.building_to_snapshot_id],
            "ownership": [bool(args.ownership_old_jsonl), bool(args.ownership_new_jsonl)],
            "rules": pl.RULES_VERSION,
        },
        sort_keys=True,
    )
    return "parcel-lineage-" + hashlib.sha256(key.encode()).hexdigest()[:24]


def read_jsonl(path: str | None) -> dict[str, dict[str, Any]]:
    if not path:
        return {}
    out = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.strip():
            row = json.loads(line)
            out[row["pnu"]] = row
    return out


def derive(
    before_raw: set[str],
    after_raw: set[str],
    codes: dict[str, tuple[str, str]],
    events: set[pl.MovementEvent],
    from_date: str,
    to_date: str,
    facts_before: Any,
    buildings_before: dict[str, str],
    buildings_after: dict[str, str],
    ownership_old: dict[str, dict[str, Any]],
    facts_new: dict[str, dict[str, Any]],
) -> tuple[list[pl.Link], dict[str, Any]]:
    """Every rule of ADR-0113 §4·§5 in order; returns the links and the summary facts.

    `events` are the window's events the rules read (merge, split, conversion text and jurisdiction
    transfers); `facts_before(pnus)` returns the latest pre-window events of the parcels the attribute
    rule needs, so the whole history never has to be held at once.
    """

    before, bad_before = pl.partition_valid(before_raw)
    after, bad_after = pl.partition_valid(after_raw)
    lots_before, lots_after = pl.lots_by_dong(before), pl.lots_by_dong(after)
    pairing = pl.pair_legal_dongs(codes, lots_before, lots_before, lots_after)
    code_links, vanished, appeared = pl.carry_over(before, after, pairing)
    history = pl.history_links(events, from_date, to_date, pairing)
    moved_in = pl.transferred_in(events, from_date, to_date) & appeared
    building = pl.building_links(
        {k: pairing.to_old(p) for k, p in buildings_before.items()},
        {k: pairing.to_new(p) for k, p in buildings_after.items()},
        vanished,
        appeared,
    )
    pool = pl.unexplained(vanished, history + building)
    latest: dict[str, pl.MovementEvent] = {}
    for e in facts_before(pool):
        q = pairing.to_old(e.pnu)
        if q in pool and e.moved_at < from_date and (q not in latest or e.moved_at >= latest[q].moved_at):
            latest[q] = e
    old_facts = {
        p: pl.ParcelFacts(
            str(e.area_m2),
            e.land_category_code,
            (ownership_old.get(p) or {}).get("owner_kind"),
            (ownership_old.get(p) or {}).get("co_owner_count"),
        )
        for p, e in latest.items()
    }
    new_latest: dict[str, pl.MovementEvent] = {}
    for e in events:
        if e.pnu in moved_in and (e.pnu not in new_latest or e.moved_at >= new_latest[e.pnu].moved_at):
            new_latest[e.pnu] = e
    new_facts = {}
    for p in moved_in:
        f = facts_new.get(p) or {}
        e = new_latest.get(p)
        new_facts[p] = pl.ParcelFacts(
            str(f.get("area_m2", e.area_m2 if e else "")),
            str(f.get("land_category_code", e.land_category_code if e else "")),
            f.get("owner_kind"),
            f.get("co_owner_count"),
        )
    already = {link.successor_pnu for link in building}
    attribute = pl.attribute_links(old_facts, {p: v for p, v in new_facts.items() if p not in already}, "jurisdiction_transfer")
    links = code_links + history + building + attribute
    best, conflicts = pl.effective_links(links)
    rec = pl.reconcile(before, after, vanished, appeared, links)
    summary = {
        "reconciliation": rec.as_dict(),
        "malformed_source_pnus": {"before": sorted(bad_before)[:20], "after": sorted(bad_after)[:20], "count": len(bad_before) + len(bad_after)},
        "dongs": {"paired": len(pairing.pairs), "how": dict(collections.Counter(pairing.how.values())), "unpaired": pairing.unpaired},
        "split_signals": {c: round(pairing.lot_overlap[c], 4) for c in pairing.split_signals()},
        "links_by_relation": dict(collections.Counter(link.relation for link in links)),
        "links_by_grade": dict(collections.Counter(link.grade for link in links)),
        "transferred_in": len(moved_in),
        "effective_grade_of_transferred_in": dict(collections.Counter(best[p].grade if p in best else "none" for p in moved_in)),
        "identity_conflicts": [
            {"successor": a.successor_pnu, "kept": [a.predecessor_pnu, a.grade], "other": [b.predecessor_pnu, b.grade]} for a, b in conflicts[:50]
        ],
        "identity_conflict_count": len(conflicts),
    }
    return links, summary


# --- lakehouse readers ------------------------------------------------------------------------
# Plain functions of (spark, catalog, ...) so the deferred-PySpark check sees every name bound.

HISTORY_COLUMNS = "pnu, reason_code, reason, moved_at, erased_at, land_category_code, area_m2"


def sql_prefixes(csv: str) -> str:
    return ", ".join(f"'{p}'" for p in csv.split(","))


def as_event(row: Any) -> pl.MovementEvent:
    return pl.MovementEvent(
        row["pnu"], row["reason_code"] or "", row["reason"] or "", row["moved_at"] or "", row["erased_at"] or "",
        row["land_category_code"] or "", "" if row["area_m2"] is None else str(row["area_m2"]),
    )


def read_pnus(spark: Any, cat: str, snapshot: str, sidos: str) -> set[str]:
    return {
        row["pnu"]
        for row in spark.sql(
            f"SELECT DISTINCT pnu FROM {cat}.`silver`.`parcel_boundaries` "
            f"WHERE source_snapshot_id = '{snapshot}' AND substr(pnu, 1, 2) IN ({sql_prefixes(sidos)})"
        ).collect()
    }


def latest_code_snapshot(spark: Any, cat: str, to_date: str) -> str | None:
    return spark.sql(
        f"SELECT CAST(max(snapshot_date) AS STRING) AS d FROM {cat}.`reference`.`legal_dong_code_snapshot` "
        f"WHERE snapshot_date <= DATE '{to_date}'"
    ).collect()[0]["d"]


def read_codes(spark: Any, cat: str, snapshot_date: str) -> dict[str, tuple[str, str]]:
    return {
        row["region_cd"]: (row["full_name"], row["status"])
        for row in spark.sql(
            f"SELECT region_cd, full_name, status FROM {cat}.`reference`.`legal_dong_code_snapshot` "
            f"WHERE snapshot_date = DATE '{snapshot_date}'"
        ).collect()
    }


def read_window_events(spark: Any, cat: str, sidos: str, from_date: str, to_date: str) -> set[pl.MovementEvent]:
    """Only what a rule reads: merge/split/conversion texts and jurisdiction transfers. The renaming
    events (one per parcel of a renumbered sido) are read by no rule and would fill the driver."""

    return {
        as_event(row)
        for row in spark.sql(
            f"SELECT DISTINCT {HISTORY_COLUMNS} FROM {cat}.`silver`.`land_transfer_history` "
            f"WHERE substr(pnu, 1, 2) IN ({sql_prefixes(sidos)}) "
            f"AND moved_at >= '{from_date}' AND moved_at < '{to_date}' "
            f"AND (reason_code = '{pl.JURISDICTION_TRANSFER_CODE}' OR reason LIKE '%합병되어%말소%' "
            f"OR reason LIKE '%번에서%분할%' OR reason LIKE '%번에서%등록전환%')"
        ).collect()
    }


def read_facts_before(spark: Any, cat: str, from_date: str, pool: set[str]) -> list[pl.MovementEvent]:
    """The pre-window history of just the parcels the attribute rule needs."""

    if not pool:
        return []
    spark.createDataFrame([(p,) for p in sorted(pool)], "pnu STRING").createOrReplaceTempView("lineage_pool")
    return [
        as_event(row)
        for row in spark.sql(
            f"SELECT DISTINCT h.pnu, h.reason_code, h.reason, h.moved_at, h.erased_at, h.land_category_code, h.area_m2 "
            f"FROM {cat}.`silver`.`land_transfer_history` h JOIN lineage_pool p ON h.pnu = p.pnu "
            f"WHERE h.moved_at < '{from_date}'"
        ).collect()
    ]


def read_buildings(spark: Any, cat: str, snapshot: str, sidos: str) -> dict[str, str]:
    return {
        row["mgm_bldrgst_pk"]: row["pnu"]
        for row in spark.sql(
            f"SELECT mgm_bldrgst_pk, pnu FROM {cat}.`silver`.`building_register_titles` "
            f"WHERE source_snapshot_id = '{snapshot}' AND substr(pnu, 1, 2) IN ({sql_prefixes(sidos)})"
        ).collect()
        if row["pnu"]
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_args(args)
    from pyspark.sql import SparkSession  # noqa: PLC0415

    now = datetime.now(timezone.utc).replace(microsecond=0)
    builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).config("spark.jars.packages", args.iceberg_packages).getOrCreate()
    cat = f"`{args.iceberg_catalog_name}`"
    try:
        before = read_pnus(spark, cat, args.from_snapshot_id, args.from_sido)
        after = read_pnus(spark, cat, args.to_snapshot_id, args.to_sido)
        if not before or not after:
            raise ValueError(f"no parcels for one side: before={len(before)} after={len(after)}")
        code_date = args.code_snapshot_date or latest_code_snapshot(spark, cat, args.to_date)
        if not code_date:
            raise ValueError("no legal dong code snapshot on or before --to-date")
        codes = read_codes(spark, cat, code_date)
        all_sido = ",".join(sorted(set(args.from_sido.split(",")) | set(args.to_sido.split(","))))
        events = read_window_events(spark, cat, all_sido, args.from_date, args.to_date)
        buildings_before: dict[str, str] = {}
        buildings_after: dict[str, str] = {}
        if args.building_from_snapshot_id:
            buildings_before = read_buildings(spark, cat, args.building_from_snapshot_id, args.from_sido)
            buildings_after = read_buildings(spark, cat, args.building_to_snapshot_id, args.to_sido)
        links, summary = derive(
            before, after, codes, events, args.from_date, args.to_date,
            functools.partial(read_facts_before, spark, cat, args.from_date),
            buildings_before, buildings_after,
            read_jsonl(args.ownership_old_jsonl), read_jsonl(args.ownership_new_jsonl),
        )
        run_id = derivation_run_id(args, code_date)
        cards = pl.cardinality(links)
        rows = [
            {
                "predecessor_pnu": link.predecessor_pnu or None,
                "successor_pnu": link.successor_pnu,
                "relation": link.relation,
                "cardinality": cards.get((link.predecessor_pnu, link.successor_pnu)),
                "effective_date": link.effective_date or None,
                "grade": link.grade,
                "evidence_kind": link.evidence_kind,
                "evidence_ref": link.evidence_ref or None,
                "from_snapshot_id": args.from_snapshot_id,
                "to_snapshot_id": args.to_snapshot_id,
                "rules_version": pl.RULES_VERSION,
                "derivation_run_id": run_id,
                "derived_at_utc": now,
            }
            for link in links
        ]
        table = f"{cat}.`{args.iceberg_namespace}`.`{args.iceberg_table}`"
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
        schema = ", ".join(f"{c['name']} {spark_sql_type(c['logical_type'])}" for c in CONTRACT["columns"])
        frame = spark.createDataFrame(rows, schema=schema).select(*COLUMNS)
        outcome = append_batch_once(spark, frame, COLUMNS, table, CONTRACT["table_name"])
        summary.update({
            "job": JOB_NAME,
            "derivation_run_id": run_id,
            "code_snapshot_date": code_date,
            "rows": len(rows),
            "appended": outcome["appended"],
            "from_snapshot_id": args.from_snapshot_id,
            "to_snapshot_id": args.to_snapshot_id,
        })
    finally:
        spark.stop()
    if args.summary_output:
        Path(args.summary_output).write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("parcel-lineage-summary-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
