#!/usr/bin/env python3
"""Store the full copies of a vintage table as versions, and prove every vintage still reads back
exactly (root ADR-0317).

`silver.building_register_apartment_price` appends a full copy of the national price file every
vintage (about 202M rows, 7 GB), and `silver.unit_official_price` a full join of it every time the
join's inputs move (about 189M rows; six batches in 30 days held 1.207B rows, 2026-10-28). Between
vintages about 0.02% of the rows change (Seoul, 202609 -> 202610: 6,578 of 29.07M). This one-time
migration writes, for each table, its `_versions` table alongside it:

- for each vintage of the source, oldest first, that the target does not record yet
  (`read_editions`): the rows of the keys whose rows differ from the target's current rows, and a
  removal marker per vanished key (`silver_release_compare.release_changes`, root ADR-0275), in one
  commit that records the vintage and the source batch;
- then the parity check: for every vintage, `rows_as_of(target, vintage)` equals the source's copy
  of that vintage on the key and content columns, row for row with repeats counted (`exceptAll`
  both ways). Any difference fails the run.

It reads the source and writes only the target: the source tables are not touched, and readers
keep reading them until the switch (root ADR-0317 §5).

A run is bounded and resumable (2026-11-04: the first run did one vintage in 13,226 s and was killed
at its 8-hour limit with no record of how far it got). Its steps are each vintage's write and each
vintage's parity check, both targets in the order of `vintage-versions-migration.contract.json`; a
step starts only while the run's elapsed time plus the step's measured cost stays within the
contract's `run_time_budget_seconds`. A written vintage is recorded by its commit, a vintage whose
parity passed by the target's table property `foundation.parity.<vintage>`; the next run skips both
and starts at the first step not recorded. `--parity-only` checks every vintage again and records
nothing.

The last stdout line, and `--summary-output`, is `vintage-versions {json}`.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from lakehouse_engine import apply_catalog_settings, assert_catalog_env, assert_iceberg_runtime_loaded, iceberg_packages
from lakehouse_ingest import (MAIN, edition_property_option, ingest_batch_token, read_editions, read_ingested_objects,
                              snapshot_property_options)
from platform_contracts import (admit_write, column_names, current_row_latest, current_rows, ensure_contract_table,
                                load_lakehouse_contract, rows_as_of)
from silver_release_compare import commit_stamp, refuse_older_edition, release_changes

JOB_NAME = "vintage-versions"
CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contracts" / "vintage-versions-migration.contract.json"
PARITY_PROPERTY = "foundation.parity."
# What a price line is: everything the source file says about it. Not the vintage, the load's ids
# and instant, nor where the line sat in the file (its part and line number move between vintages).
APARTMENT_PRICE_CONTENT = ("sigungu_cd", "bjdong_cd", "san_gubun", "bonbeon", "bubeon", "price_won", "notice_date",
                           "pnu", "raw_columns")
# What a joined unit price is, besides its key: where it is served and its value.
UNIT_PRICE_CONTENT = ("pnu", "dong_name", "ho_name", "price_won", "sido")


@dataclass(frozen=True)
class Migration:
    source: str
    content: tuple[str, ...]
    # (spark, the source table read whole, the source table's quoted name) -> {vintage: (rows, source batch id)}
    vintages: Callable[[Any, Any, str], dict[str, tuple[Any, str]]]


def apartment_price_vintages(spark: Any, source: Any, qualified: str) -> dict[str, tuple[Any, str]]:
    """Each vintage's rows: the lane appends one full copy per vintage (root ADR-0169)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    batches: dict[str, list[str]] = {}
    for row in source.select("vintage", "source_snapshot_id").distinct().collect():
        batches.setdefault(row.vintage, []).append(row.source_snapshot_id)
    many = {vintage: ids for vintage, ids in batches.items() if len(ids) != 1}
    if many:
        # Two loads of one vintage are not told apart here; the operator decides, not this job.
        raise ValueError(f"vintages with more than one load: {many}")
    return {vintage: (source.where(F.col("vintage") == vintage), ids[0]) for vintage, ids in batches.items()}


def unit_price_vintages(spark: Any, source: Any, qualified: str) -> dict[str, tuple[Any, str]]:
    """Each vintage's rows: its last batch, the one the building panel serves (root ADR-0185)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    from unit_official_price import batch_commit_order, batch_vintage, newest_batch  # noqa: PLC0415

    by_vintage: dict[str, list[str]] = {}
    for row in source.select("source_snapshot_id").distinct().collect():
        vintage = batch_vintage(row.source_snapshot_id)
        if vintage is None:
            raise ValueError(f"{qualified} holds a batch the join did not write: {row.source_snapshot_id}")
        by_vintage.setdefault(vintage, []).append(row.source_snapshot_id)
    order = batch_commit_order(spark, qualified)
    chosen = {vintage: newest_batch(ids, order) for vintage, ids in by_vintage.items()}
    return {vintage: (source.where(F.col("source_snapshot_id") == batch).withColumn("vintage", F.lit(vintage)), batch)
            for vintage, batch in chosen.items()}


MIGRATIONS = {
    "silver.building_register_apartment_price_versions": Migration(
        "silver.building_register_apartment_price", APARTMENT_PRICE_CONTENT, apartment_price_vintages),
    "silver.unit_official_price_versions": Migration(
        "silver.unit_official_price", UNIT_PRICE_CONTENT, unit_price_vintages),
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--target", choices=sorted(MIGRATIONS),
                        help="One target only; by default every target of the migration contract, in its order")
    parser.add_argument("--parity-only", action="store_true", help="Check every vintage; write nothing")
    parser.add_argument("--iceberg-catalog-name", default="r2")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    parser.add_argument("--summary-output")
    return parser.parse_args(argv)


def migration_record(target: str, vintage: str, batch: str) -> str:
    """The commit's load record: the vintage and the source batch it came from."""
    return f"{JOB_NAME}/{target}/{vintage}/{batch}".replace(",", "_")


def shard_record(record: str, shard: int, shards: int) -> str:
    """One shard's load record of a vintage written in shards."""
    return f"{record}/shard-{shard + 1}-of-{shards}"


def vintage_changes(staged: Any, versions: Any, contract: dict[str, Any], content: tuple[str, ...], vintage: str,
                    batch: str, instant: Any, first: bool = False) -> Any:
    """The rows one vintage appends to `versions`: its changes against the current rows.

    The `first` vintage (`versions` holds no older one) has no current rows: every key is new and
    none vanished, so its changes are all its rows, as `release_changes` would find, without the
    comparison's shuffle of every row (2026-11-04: the first apartment vintage, 202M rows of 7 GB,
    took 13,226 s through it)."""
    latest = current_row_latest(contract)
    stamp, _ = commit_stamp(latest, vintage, instant)
    columns = list(column_names(contract))
    if first:
        for column, value in stamp.items():
            staged = staged.withColumn(column, value.cast(versions.schema[column].dataType))
        return staged.select(*columns)
    return release_changes(staged, current_rows(versions, contract).select(*columns), key=latest.key,
                           content=list(content), removal=latest.removal_column, source_snapshot_id=batch,
                           ingested_at=instant, columns=columns, stamp=stamp)


def row_hashes(frame: Any, columns: list[str]) -> list[Any]:
    """Two independent 64-bit hashes of each row's `columns`. A null and an empty string hash apart
    (root ADR-0317 §4): every column contributes its null flag and its text."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    parts = [part for name in columns
             for part in (F.col(name).isNull().cast("int"), F.coalesce(F.col(name).cast("string"), F.lit("")))]
    return [F.xxhash64(F.lit("h1"), *parts).alias("_h1"), F.xxhash64(F.lit("h2"), *parts).alias("_h2")]


def checksums(frame: Any, key: list[str], buckets: int) -> Any:
    """Per bucket of the key's hash: the rows and the two hash sums (data-diff's checksum segments)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    return (frame.withColumn("_bucket", F.pmod(F.xxhash64(*[F.col(name) for name in key]), F.lit(buckets)))
            .groupBy("_bucket")
            .agg(F.count(F.lit(1)).alias("_rows"), F.sum(F.col("_h1").cast("decimal(38,0)")).alias("_s1"),
                 F.sum(F.col("_h2").cast("decimal(38,0)")).alias("_s2")))


def parity(versions: Any, full_copy: Any, contract: dict[str, Any], content: tuple[str, ...], vintage: str,
           buckets: int = 4096) -> dict[str, int]:
    """How many rows the reconstruction of `vintage` lacks and adds against its full copy.

    Both sides are narrowed to the key, the version order and two row hashes before anything is
    ranked or shuffled: an equal vintage costs one read of each side and two small aggregations.
    Only the buckets whose checksums differ are compared row for row, so a difference is still
    counted exactly (repeats included, `exceptAll`)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    latest = current_row_latest(contract)
    key, compared = list(latest.key), [*latest.key, *content]
    order = [name for name in (*latest.newest_first, latest.removal_column) if name]
    narrow_versions = versions.select(*key, *order, *row_hashes(versions, compared))
    rebuilt = rows_as_of(narrow_versions, contract, vintage).select(*key, "_h1", "_h2")
    copy = full_copy.select(*key, *row_hashes(full_copy, compared))
    left, right = checksums(copy, key, buckets).alias("c"), checksums(rebuilt, key, buckets).alias("r")
    joined = left.join(right, "_bucket", "full_outer")
    differing = [row["_bucket"] for row in joined.where(
        ~(F.col("c._rows").eqNullSafe(F.col("r._rows")) & F.col("c._s1").eqNullSafe(F.col("r._s1"))
          & F.col("c._s2").eqNullSafe(F.col("r._s2")))).select("_bucket").collect()]
    totals = joined.agg(F.sum("c._rows").alias("copy"), F.sum("r._rows").alias("rebuilt")).first()
    result = {"full_copy_rows": int(totals["copy"] or 0), "rebuilt_rows": int(totals["rebuilt"] or 0),
              "buckets_differing": len(differing), "missing": 0, "extra": 0}
    if differing:
        def within(frame: Any, columns: list[str]) -> Any:
            bucket = F.pmod(F.xxhash64(*[F.col(name) for name in key]), F.lit(buckets))
            return frame.where(bucket.isin(differing)).select(*columns)

        exact_copy = within(full_copy, compared)
        exact_rebuilt = within(rows_as_of(versions, contract, vintage), compared)
        result["missing"] = exact_copy.exceptAll(exact_rebuilt).count()
        result["extra"] = exact_rebuilt.exceptAll(exact_copy).count()
    return result


def held_parity(spark: Any, target_q: str) -> set[str]:
    """The vintages whose parity a run has recorded on the target (`foundation.parity.<vintage>`)."""
    rows = spark.sql(f"SHOW TBLPROPERTIES {target_q}").collect()
    return {row[0][len(PARITY_PROPERTY):] for row in rows if str(row[0]).startswith(PARITY_PROPERTY)}


def record_parity(spark: Any, target: str, target_q: str, vintage: str, checked: dict[str, int]) -> None:
    """A metadata commit naming the vintage as checked: later vintages only append versions above
    it, so `rows_as_of(vintage)` cannot change and the check is not repeated."""
    admit_write(target, "record", target=target_q)
    value = json.dumps({k: checked[k] for k in ("full_copy_rows", "rebuilt_rows")}, sort_keys=True)
    spark.sql(f"ALTER TABLE {target_q} SET TBLPROPERTIES ('{PARITY_PROPERTY}{vintage}' = '{value}')")


@dataclass
class Budget:
    """The run's time budget: a step starts only if its cost still fits (root ADR-0300 H).

    A step's cost is the contract's measured one until a step of the same kind ran in this run; then
    it is the longest such step of the run, so a run measures what it plans with."""
    seconds: float
    started: float
    costs: dict[str, float]

    def allows(self, kind: str) -> bool:
        return time.monotonic() - self.started + self.costs[kind] <= self.seconds

    def took(self, kind: str, seconds: float, measured: set[str]) -> None:
        self.costs[kind] = max(self.costs[kind], seconds) if kind in measured else seconds
        measured.add(kind)


def load_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    contract = json.loads(path.read_text(encoding="utf-8"))
    budget = contract["run_time_budget_seconds"]
    for target in contract["targets"]:
        if target["table"] not in MIGRATIONS:
            raise ValueError(f"{path.name}: {target['table']} is not a migration of {JOB_NAME}")
        # A step that cannot fit an empty run would never start, and the migration never end.
        per_step = (target["write_seconds"] / target["write_shards"], target["parity_seconds"])
        if not (isinstance(target["write_shards"], int) and target["write_shards"] >= 1) or max(per_step) > budget:
            raise ValueError(f"{path.name}: a step of {target['table']} does not fit run_time_budget_seconds")
    return contract


def shard_of(key: list[str], shards: int) -> Any:
    from pyspark.sql import functions as F  # noqa: PLC0415

    return F.pmod(F.xxhash64(*[F.col(name) for name in key]), F.lit(shards))


def migrate(spark: Any, args: argparse.Namespace, budget: Budget | None = None, shards: int = 1,
            buckets: int = 4096) -> dict[str, Any]:
    """One target's steps, oldest vintage first, as many as `budget` allows (none given: all).

    A vintage is written in `shards` commits, each the changes of the keys whose hash falls in it:
    a key's versions are ranked within the key (`current_rows`), so the shards of one vintage are
    the same versions its one commit would be, and a run stopped between them continues with the
    next. The vintage 202608 written whole before shards (2026-11-04) is recorded by its one commit."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    budget = budget or Budget(float("inf"), time.monotonic(), {"write": 0, "parity": 0})
    measured: set[str] = set()
    migration = MIGRATIONS[args.target]
    contract = load_lakehouse_contract(args.target)
    key = list(current_row_latest(contract).key)
    catalog = args.iceberg_catalog_name
    source_q = "`{}`.`{}`.`{}`".format(catalog, *migration.source.split(".", 1))
    target_u = f"{catalog}.{args.target}"
    target_q = "`{}`.`{}`.`{}`".format(catalog, *args.target.split(".", 1))
    columns = list(column_names(contract))
    vintages = migration.vintages(spark, spark.table(source_q), source_q)
    report: dict[str, Any] = {"target": args.target, "source": migration.source, "vintages": {}}
    if not args.parity_only:
        ensure_contract_table(spark, target_q, contract)
    held = set() if args.parity_only else held_parity(spark, target_q)
    for vintage in sorted(vintages):
        staged, batch = vintages[vintage]
        entry: dict[str, Any] = {"source_batch": batch}
        record = migration_record(args.target, vintage, batch)
        recorded = read_ingested_objects(spark, target_q, target_u, MAIN)
        owed = [] if record in recorded else [
            shard for shard in range(shards) if shard_record(record, shard, shards) not in recorded]
        first = False
        if args.parity_only:
            owed = []
        elif owed:
            editions = read_editions(spark, target_q, target_u, MAIN)
            refuse_older_edition(vintage, editions)
            # Only this vintage's own shards before it: their keys are not this shard's.
            first = not any(edition < vintage for edition in editions)
        for shard in owed:
            if not budget.allows("write"):
                report["stopped"] = {"before": "write", "vintage": vintage, "shard": shard}
                break
            started = time.monotonic()
            instant = F.lit(datetime.now(timezone.utc)).cast("timestamp")
            mine = shard_of(key, shards) == shard
            # The full copy as versions: the commit's instant, and no removal mark on a source row.
            staged_rows = (staged.where(mine).withColumn("ingested_at_utc", instant)
                           .withColumn("row_state", F.lit(None).cast("string")).select(*columns))
            changes = vintage_changes(staged_rows, spark.table(target_q).where(mine), contract, migration.content,
                                      vintage, batch, instant, first)
            # The versions only grow (root ADR-0286): the shard's changes are appended.
            admit_write(args.target, "append", target=target_q)
            writer = changes.writeTo(target_u)
            loaded = shard_record(record, shard, shards)
            options = {**snapshot_property_options([loaded], ingest_batch_token([loaded])),
                       **edition_property_option(vintage)}
            for option, value in options.items():
                writer = writer.option(option, value)
            writer.append()
            seconds = round(time.monotonic() - started, 1)
            budget.took("write", seconds, measured)
            entry.setdefault("appended_seconds", {})[f"{shard + 1}/{shards}"] = seconds
            print(f"{JOB_NAME} {args.target} {vintage} shard {shard + 1}/{shards}: appended in {seconds} s", flush=True)
        if "stopped" in report:
            report["vintages"][vintage] = entry
            break
        written = not args.parity_only or vintage in read_editions(spark, target_q, target_u, MAIN)
        if not written:
            entry["written"] = False
        elif vintage in held:
            entry["parity"] = "held"
        else:
            if not budget.allows("parity"):
                report["stopped"] = {"before": "parity", "vintage": vintage}
                report["vintages"][vintage] = entry
                break
            started = time.monotonic()
            # The versions table is partitioned by vintage: this count reads one partition's metadata.
            entry["version_rows"] = spark.table(target_q).where(
                F.col(current_row_latest(contract).newest_first[0]) == vintage).count()
            entry.update(parity(spark.table(target_q), staged, contract, migration.content, vintage, buckets))
            entry["parity_seconds"] = round(time.monotonic() - started, 1)
            budget.took("parity", entry["parity_seconds"], measured)
            if not args.parity_only and entry["missing"] == 0 and entry["extra"] == 0:
                record_parity(spark, args.target, target_q, vintage, entry)
        print(f"{JOB_NAME} {args.target} {vintage}: {json.dumps(entry, sort_keys=True)}", flush=True)
        report["vintages"][vintage] = entry
        if "missing" in entry and (entry["missing"] or entry["extra"]):
            break  # a later vintage is appended against this one's state: never past a mismatch
    checked = [entry for entry in report["vintages"].values() if "missing" in entry]
    report["parity"] = all(entry["missing"] == 0 and entry["extra"] == 0 for entry in checked)
    report["complete"] = ("stopped" not in report and len(report["vintages"]) == len(vintages)
                          and all(entry.get("parity") == "held" or "missing" in entry and entry["missing"] == 0
                                  and entry["extra"] == 0 for entry in report["vintages"].values()))
    return report


def run(spark: Any, args: argparse.Namespace, contract: dict[str, Any]) -> dict[str, Any]:
    """Every target's steps in the contract's order, within one run's budget."""
    started = time.monotonic()
    summary: dict[str, Any] = {"targets": {}, "parity": True, "complete": True}
    for target in contract["targets"]:
        if args.target is not None and target["table"] != args.target:
            continue
        shards = target["write_shards"]
        costs = {"write": target["write_seconds"] / shards, "parity": target["parity_seconds"]}
        budget = Budget(float(contract["run_time_budget_seconds"]), started, costs)
        one = argparse.Namespace(**{**vars(args), "target": target["table"]})
        report = migrate(spark, one, budget, shards, int(contract["parity_buckets"]))
        summary["targets"][target["table"]] = report
        summary["parity"] = summary["parity"] and report["parity"]
        summary["complete"] = summary["complete"] and report["complete"]
        if not report["complete"]:
            break  # root ADR-0317 §5.2: the unit prices only after every apartment vintage matched
    return summary


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    from pyspark.sql import SparkSession  # noqa: PLC0415

    assert_catalog_env(args.iceberg_catalog_name)
    builder = (SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "400"))
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        assert_iceberg_runtime_loaded(spark, args.iceberg_packages)
        summary = run(spark, args, load_contract())
    finally:
        spark.stop()
    if args.summary_output:
        Path(args.summary_output).write_text(json.dumps(summary, sort_keys=True), encoding="utf-8")
    print(f"{JOB_NAME} " + json.dumps(summary, sort_keys=True), flush=True)
    return 0 if summary["parity"] else 1


if __name__ == "__main__":
    sys.exit(main())
