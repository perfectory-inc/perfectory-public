#!/usr/bin/env python3
"""The scheduled parcel lineage derivation (root ADR-0113 §1, ADR-0148): every consecutive pair of
the parcel editions the source contract holds, one derivation per sido group, each derived once.

`lineage-stewardship-cycle.sh` runs it in that unit's Spark slots. For each edition pair it pins the
current heads of the inputs once, groups the sido prefixes, and hands each group to
`parcel_lineage_to_silver.derive_and_append`. A prefix both editions hold is its own group; the
prefixes only one edition holds (a sido renumbered or merged, 29+46 -> 12) form one group, because
their parcels pair only with each other. The producer collects both sides of a group on the driver,
so a group is at most one or two sidos, never the country.

A group is derived once per (edition pair, group, rules version): it is skipped when
silver.parcel_lineage already holds a row of that pair and rules version whose successor lies in
the group, unless official evidence withdrew a 동 pair of the group after that row was derived
(`superseded_since`, root ADR-0329 §3): then that group alone is derived again. No group starts after the deadline; the next run continues where this one stopped.
A group the producer refuses (a vanished 동 code the code change table does not pair, say) is
recorded with its reason and the others go on: one region's missing record must not hold back
every region after it. The run says so (`refused`), and the stewardship unit then fails loudly.
"""

from __future__ import annotations

import argparse
import functools
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import parcel_lineage as pl
import parcel_lineage_to_silver as producer
import vworld_parcel_editions as editions
from lakehouse_engine import apply_catalog_settings, current_snapshot, iceberg_packages

JOB_NAME = "parcel_lineage_scheduled"
ROLE_TABLES = {
    "boundaries_from": "silver.parcel_boundaries",
    "boundaries_to": "silver.parcel_boundaries",
    "code_changes": "reference.legal_dong_code_change",
    "history": "silver.land_transfer_history",
}


def edition_pairs(contract: dict[str, Any]) -> list[tuple[str, str]]:
    """Each edition the contract holds with the one after it, oldest first."""
    names = editions.names(contract)
    return list(zip(names, names[1:]))


def awaits_load(before: set[str], after: set[str]) -> bool:
    """The later edition is in the source contract but not yet in silver.parcel_boundaries.

    An edition enters the contract first and is loaded into Silver after, from a release that holds
    it (runbook vworld-parcel-editions.md §4). Until then its pair waits: it is not a partial load,
    and failing the stewardship unit every day for it would page for a step that is simply next.
    """
    return bool(before) and not after


def sido_groups(before: set[str], after: set[str]) -> list[tuple[str, str]]:
    """(from prefixes, to prefixes) per derivation: the renumbered prefixes together first, then each
    shared prefix alone."""
    if not before or not after:
        raise ValueError(f"an edition holds no parcels: before={sorted(before)} after={sorted(after)}")
    groups = [(p, p) for p in sorted(before & after)]
    gone, new = sorted(before - after), sorted(after - before)
    if bool(gone) != bool(new):
        # A sido that vanished with nothing new (or appeared from nothing) is a partial load, not a
        # reorganization; deriving would read every parcel of it as vanished or born.
        raise ValueError(f"sido prefixes held by one edition only, with no counterpart: gone={gone} new={new}")
    if gone:
        # First: a reorganized region is what the lineage exists for, and its group is the largest,
        # so a run cut short by the deadline still derives it.
        groups.insert(0, (",".join(gone), ",".join(new)))
    return groups


def run_groups(
    groups: list[tuple[str, str]],
    is_held: Callable[[str, str], bool],
    derive_one: Callable[[str, str], dict[str, Any]],
    deadline_seconds: float,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, list[Any]]:
    """Derive each group not yet held, starting none after the deadline. A refusal (ValueError, the
    producer's) is recorded and the next group goes on; anything else stops the run."""
    started = clock()
    outcome: dict[str, list[Any]] = {"derived": [], "held": [], "deferred": [], "refused": []}
    for from_sido, to_sido in groups:
        group = {"from_sido": from_sido, "to_sido": to_sido}
        if is_held(from_sido, to_sido):
            outcome["held"].append(group)
        elif clock() - started >= deadline_seconds:
            outcome["deferred"].append(group)
        else:
            try:
                outcome["derived"].append({**group, **derive_one(from_sido, to_sido)})
            except ValueError as refusal:
                # The 동 codes a refusal names, when it names them, so the alert can say which.
                outcome["refused"].append({**group, "reason": str(refusal)[:2000],
                                           "unpaired_codes": list(getattr(refusal, "codes", []))})
    return outcome


def held_sql(table: str, from_snapshot_id: str, to_snapshot_id: str, to_sido: str, since: datetime | None = None) -> str:
    """A row of this pair, rules version and group; with `since`, only one derived at or after it."""

    after = f"AND derived_at_utc >= TIMESTAMP '{since:%Y-%m-%d %H:%M:%S}' " if since is not None else ""
    return (
        f"SELECT 1 FROM {table} WHERE from_snapshot_id = '{from_snapshot_id}' "
        f"AND to_snapshot_id = '{to_snapshot_id}' AND rules_version = '{pl.RULES_VERSION}' {after}"
        f"AND substr(successor_pnu, 1, 2) IN ({producer.sql_prefixes(to_sido)}) LIMIT 1"
    )


def superseded_since(supersessions: list[dict[str, Any]], from_sido: str) -> datetime | None:
    """When the last 동 pair of this group was withdrawn by official evidence (root ADR-0329 §3): the
    newest `recorded_at_utc` of a `supersession` row whose old code lies in `from_sido`, or None. A
    group derived before it carried the withdrawn pair's links and is derived again, and only that
    group: the lineage's own unit, since a group's parcels pair only with each other."""

    prefixes = set(from_sido.split(","))
    times = [row["recorded_at_utc"] for row in supersessions if str(row["old_code"])[:2] in prefixes]
    return max(times) if times else None


def read_supersessions(spark: Any, change_table: str) -> list[dict[str, Any]]:
    """The `supersession` rows of the pinned code change table (a few, next to its pairs)."""

    return [row.asDict() for row in spark.sql(
        f"SELECT old_code, recorded_at_utc FROM {change_table} WHERE kind = '{pl.views.SUPERSESSION}'").collect()]


def is_held(spark: Any, table: str, exists: bool, from_id: str, to_id: str, supersessions: list[dict[str, Any]],
            from_sido: str, to_sido: str) -> bool:
    since = superseded_since(supersessions, from_sido)
    return exists and bool(spark.sql(held_sql(table, from_id, to_id, to_sido, since)).collect())


def derive_one(spark: Any, args: argparse.Namespace, contract: dict[str, Any], pair: tuple[str, str],
               pins_path: Path, from_sido: str, to_sido: str) -> dict[str, Any]:
    one = producer.parse_args(producer_argv(args, contract, pair, (from_sido, to_sido), pins_path))
    result = producer.derive_and_append(spark, one, producer.validate_args(one),
                                        datetime.now(timezone.utc).replace(microsecond=0))
    return {k: result[k] for k in ("derivation_run_id", "rows", "appended", "links_by_relation",
                                   "links_by_grade", "reconciliation")}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--deadline-minutes", type=float, required=True,
                        help="Start no group after this many minutes; the next run continues")
    parser.add_argument("--work-dir", required=True, help="Where the pinned input documents are written")
    parser.add_argument("--summary-output")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-namespace", default="silver")
    parser.add_argument("--iceberg-table", default="parcel_lineage")
    parser.add_argument("--allow-non-smoke-write", action="store_true")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    args = parser.parse_args(argv)
    if args.deadline_minutes <= 0:
        raise ValueError("--deadline-minutes must be positive")
    return args


def producer_argv(args: argparse.Namespace, contract: dict[str, Any], pair: tuple[str, str],
                  group: tuple[str, str], pins_path: Path) -> list[str]:
    """The producer's own flags for one group; its parser validates them as a hand run's."""
    (from_name, to_name), (from_sido, to_sido) = pair, group
    argv = [
        "--from-snapshot-id", editions.snapshot_id(contract, from_name),
        "--to-snapshot-id", editions.snapshot_id(contract, to_name),
        # Both endpoints are editions the contract holds; the later one is usually not served yet.
        editions.ALLOW_OTHER_EDITION,
        "--from-date", editions.valid_from_utc(from_name)[:10],
        "--to-date", editions.valid_from_utc(to_name)[:10],
        "--from-sido", from_sido, "--to-sido", to_sido,
        "--source-snapshots-path", str(pins_path),
        "--iceberg-catalog-name", args.iceberg_catalog_name,
        "--iceberg-namespace", args.iceberg_namespace,
        "--iceberg-table", args.iceberg_table,
    ]
    return argv + (["--allow-non-smoke-write"] if args.allow_non_smoke_write else [])


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    contract = editions.load()
    from pyspark.sql import SparkSession  # noqa: PLC0415

    builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).config("spark.jars.packages", args.iceberg_packages).getOrCreate()
    cat = f"`{args.iceberg_catalog_name}`"
    table = f"{cat}.`{args.iceberg_namespace}`.`{args.iceberg_table}`"
    started = time.monotonic()
    summary: dict[str, Any] = {"job": JOB_NAME, "rules_version": pl.RULES_VERSION, "pairs": []}
    try:
        for pair in edition_pairs(contract):
            # One set of pins per pair: every group of it reads the same inputs.
            pins = {role: {"table": name, "snapshot_id": current_snapshot(spark, f"{cat}.{name}")}
                    for role, name in ROLE_TABLES.items()}
            pins_path = Path(args.work_dir) / f"lineage-pins-{pair[0]}-{pair[1]}.json"
            pins_path.write_text(json.dumps(pins, sort_keys=True) + "\n", encoding="utf-8")
            boundaries = f"{cat}.{ROLE_TABLES['boundaries_from']} VERSION AS OF {pins['boundaries_from']['snapshot_id']}"
            before, after = (
                {r["s"] for r in spark.sql(
                    f"SELECT DISTINCT substr(pnu, 1, 2) AS s FROM {boundaries} "
                    f"WHERE source_snapshot_id = '{editions.snapshot_id(contract, name)}'").collect()}
                for name in pair
            )
            if awaits_load(before, after):
                summary["pairs"].append({"from": pair[0], "to": pair[1], "waiting": "not loaded into silver.parcel_boundaries"})
                print(f"parcel-lineage-waiting {pair[0]}->{pair[1]}: {pair[1]} is not loaded into silver.parcel_boundaries yet")
                continue
            exists = spark.catalog.tableExists(f"{args.iceberg_catalog_name}.{args.iceberg_namespace}.{args.iceberg_table}")
            from_id, to_id = (editions.snapshot_id(contract, name) for name in pair)
            supersessions = read_supersessions(
                spark, f"{cat}.{ROLE_TABLES['code_changes']} VERSION AS OF {pins['code_changes']['snapshot_id']}")

            remaining = max(args.deadline_minutes * 60 - (time.monotonic() - started), 0)
            outcome = run_groups(sido_groups(before, after),
                                 functools.partial(is_held, spark, table, exists, from_id, to_id, supersessions),
                                 functools.partial(derive_one, spark, args, contract, pair, pins_path), remaining)
            summary["pairs"].append({"from": pair[0], "to": pair[1], "pins": pins, **outcome})
    finally:
        spark.stop()
    summary["derived"] = sum(len(p.get("derived", [])) for p in summary["pairs"])
    summary["deferred"] = sum(len(p.get("deferred", [])) for p in summary["pairs"])
    summary["refused"] = sum(len(p.get("refused", [])) for p in summary["pairs"])
    summary["waiting"] = [f"{p['from']}->{p['to']}" for p in summary["pairs"] if "waiting" in p]
    if args.summary_output:
        Path(args.summary_output).write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                                             encoding="utf-8")
    print("parcel-lineage-scheduled-json " + json.dumps(
        {k: summary[k] for k in ("derived", "deferred", "refused", "waiting", "rules_version")}, sort_keys=True))
    for pair in summary["pairs"]:
        for group in pair.get("refused", []):
            print(f"parcel-lineage-refused {pair['from']}->{pair['to']} {group['from_sido']}->{group['to_sido']}: "
                  f"{group['reason'][:300]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
