#!/usr/bin/env python3
"""Run Iceberg table maintenance: compact, expire snapshots, remove orphans.

None of these had ever run against this lakehouse. On 2026-08-30 the tables held 3,160 data
files averaging 5 MB, no snapshot had ever been expired, and 41 of 58 GB in R2 was serving
17 GB of live rows. Iceberg ships all three operations; we simply never called them.

The order is not interchangeable and the contract states why: compaction creates the
superseded files, expiry releases the snapshots pinning them, and orphan cleanup is the only
step that reaches files no snapshot ever referenced — the debris a job leaves when it dies
mid-write. Expiry cannot see those, so running cleanup first leaves them for the next pass
and running it never leaves them forever.

Thresholds live in `lakehouse-maintenance.contract.json`, not here. A number written in a job
is a number the next job disagrees with.

`--all-tables` is the scheduled job's run (`lakehouse_maintenance` in orchestration/jobs.v1.json):
every table of the lakehouse contract artifact that exists in the catalog, the least recently
maintained first, until the contract's deadline. A table it does not reach is taken first at the
next run.

Expiry never removes a snapshot this lakehouse's own readers read back out of the metadata
(`protected_snapshots`): the Silver loaders decide what is already loaded from every ingest record
in `main`'s ancestry, and the Gold rebuild plans from the newest source pins in it. Expiring those
would make a loader load a release twice and a rebuild refuse to plan. Before it expires anything
it also copies every ingest record past the window into a table property (root ADR-0332), so the
record no longer depends on that protection; what still does is named in the ADR.

No PySpark import at module scope: the lane that runs `infra/lakehouse/spark/tests` has none,
and a module-level import would make every check that touches this file skip itself.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from gold_rebuild import SOURCE_SNAPSHOTS_PROPERTY
from lakehouse_ingest import INGEST_BATCH_OBJECTS_KEY, ingest_record_properties

CONTRACT_PATH_ENV = "FOUNDATION_PLATFORM_LAKEHOUSE_MAINTENANCE_CONTRACT_PATH"
DEFAULT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[2] / "contracts" / "lakehouse-maintenance.contract.json"
)
CONTRACT_SCHEMA_VERSION = 1

# Iceberg refuses any orphan-cleanup interval under this, because deleting a live writer's
# output corrupts the commit it is about to make. Stated here so a contract that tried to go
# below it fails before the procedure does, with a reason instead of a stack trace.
MINIMUM_ORPHAN_SAFETY_HOURS = 24

STEPS = ("compaction", "snapshot_expiry", "orphan_cleanup")


def load_maintenance_contract() -> dict[str, Any]:
    path = Path(os.getenv(CONTRACT_PATH_ENV, str(DEFAULT_CONTRACT_PATH)))
    contract = json.loads(path.read_text(encoding="utf-8"))
    version = contract.get("schema_version")
    if version != CONTRACT_SCHEMA_VERSION:
        raise ValueError(
            f"unsupported maintenance contract schema_version {version!r}; "
            f"expected {CONTRACT_SCHEMA_VERSION!r}"
        )
    validate_maintenance_contract(contract)
    return contract


def validate_maintenance_contract(contract: dict[str, Any]) -> None:
    """Refuse a contract whose numbers would do harm, before any table is touched."""
    for name, entry in contract.get("tables", {}).items():
        unknown = set(entry["skip"]) - set(STEPS)
        if unknown or not entry.get("reason"):
            raise ValueError(f"tables.{name} must skip only {STEPS} and say why: got {entry!r}")
    scheduled = contract["scheduled"]
    if set(scheduled["skip"]) - set(STEPS) or scheduled["deadline_minutes"] < 1:
        raise ValueError(f"scheduled must skip only {STEPS} and give a positive deadline: got {scheduled!r}")
    retries = re.fullmatch(r"local\[\d+,\s*(\d+)\]", scheduled["spark"]["master"])
    if retries is None or int(retries.group(1)) < 2:
        raise ValueError(
            f"scheduled.spark.master={scheduled['spark']['master']!r} must be local[N,maxFailures] with "
            "maxFailures >= 2: plain local[N] ignores spark.task.maxFailures, so one R2 connection reset "
            "aborts the job (root ADR-0102)"
        )
    if contract["compaction"]["partial_progress_max_commits"] < 1:
        raise ValueError("compaction.partial_progress_max_commits must allow at least one commit")
    order = contract.get("order")
    if order != ["compaction", "snapshot_expiry", "orphan_cleanup"]:
        raise ValueError(
            "maintenance order must be compaction, snapshot_expiry, orphan_cleanup: "
            f"got {order!r}. Cleanup last is what reaches files no snapshot references."
        )

    safety_days = contract["orphan_cleanup"]["safety_days"]
    if safety_days * 24 < MINIMUM_ORPHAN_SAFETY_HOURS:
        raise ValueError(
            f"orphan_cleanup.safety_days={safety_days} is under Iceberg's {MINIMUM_ORPHAN_SAFETY_HOURS}h "
            "floor; deleting a running writer's output corrupts the commit it is about to make"
        )

    expiry = contract["snapshot_expiry"]
    if expiry["retain_last"] < 1:
        raise ValueError("snapshot_expiry.retain_last must keep at least the current snapshot")
    if expiry["retain_days"] < 1:
        raise ValueError("snapshot_expiry.retain_days must leave at least a day of rollback")

    monthly = expiry["month_end_tag"]
    if "%Y" not in monthly["name_format"] or "%m" not in monthly["name_format"]:
        raise ValueError(
            f"snapshot_expiry.month_end_tag.name_format={monthly['name_format']!r} must contain %Y "
            "and %m: a name two months share makes the second month's tag a no-op, and its "
            "snapshot expires"
        )
    if monthly["keep_months"] < 1:
        raise ValueError("snapshot_expiry.month_end_tag.keep_months must keep at least one month")

    yearly = expiry["year_end_tag"]["name_format"]
    if "%Y" not in yearly or "%m" in yearly:
        raise ValueError(
            f"snapshot_expiry.year_end_tag.name_format={yearly!r} must contain %Y and not %m: "
            "one name per year"
        )
    if month_of_tag(month_start(2026, 12).strftime(yearly), monthly["name_format"]) is not None:
        raise ValueError(
            "a year-end tag name must not read as a month-end tag name: the monthly window would "
            "drop the tag that is meant to be kept forever"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compact, expire and clean an Iceberg table.")
    which = parser.add_mutually_exclusive_group(required=True)
    which.add_argument("--table", help="Logical table, e.g. silver.parcel_boundaries")
    which.add_argument(
        "--all-tables",
        action="store_true",
        help="Every lakehouse contract table the catalog holds, under the contract's deadline.",
    )
    parser.add_argument(
        "--state-file",
        help="--all-tables: JSON recording when each table was last maintained (the rotation).",
    )
    parser.add_argument(
        "--catalog",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_CATALOG_NAME", "r2"),
        help="Spark catalog name.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what each step would do without deleting anything.",
    )
    parser.add_argument(
        "--skip",
        action="append",
        default=[],
        choices=["compaction", "snapshot_expiry", "orphan_cleanup"],
        help="Skip a step. Orphan cleanup lists the whole table location and is slow on a small host.",
    )
    return parser.parse_args()


def timestamp_before(days: int, now: datetime | None = None) -> str:
    """Iceberg's procedures take a literal timestamp, not an interval."""
    now = now or datetime.now(timezone.utc)
    return (now - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S.000")


def millis_text(millis: int) -> str:
    """A procedure's literal timestamp at millisecond precision, UTC."""
    moment = datetime.fromtimestamp(millis / 1000, tz=timezone.utc)
    return moment.strftime("%Y-%m-%d %H:%M:%S.") + f"{millis % 1000:03d}"


def ancestry(snapshots: dict[int, dict[str, Any]], head: int | None) -> list[int]:
    """`head` and each parent back as far as the metadata still holds them, newest first."""
    chain: list[int] = []
    while head is not None and head in snapshots and head not in chain:
        chain.append(head)
        head = snapshots[head]["parent_id"]
    return chain


def protected_snapshots(snapshots: dict[int, dict[str, Any]], head: int | None) -> set[int]:
    """The snapshots of `main` this lakehouse reads back out of the metadata.

    - Every snapshot recording an ingest (`lakehouse_ingest.INGEST_BATCH_OBJECTS_KEY`): the Silver
      refresh and the loaders take the union of them as "already loaded"; a part lane's release is
      loaded only while every part's record is still there (`decide_parts`). The record itself
      now outlives its snapshot (`ingest_record_properties`, root ADR-0332); the protection stays
      for what still reads these snapshots back (the Gold baselines and the FLOOR witness chain)
      until those have rules of their own.
    - The newest snapshot recording Gold source pins (`gold_rebuild.SOURCE_SNAPSHOTS_PROPERTY`):
      the rebuild plans from it and refuses without it.
    """
    chain = ancestry(snapshots, head)
    protected = {sid for sid in chain if INGEST_BATCH_OBJECTS_KEY in snapshots[sid]["summary"]}
    pinned = next((sid for sid in chain if SOURCE_SNAPSHOTS_PROPERTY in snapshots[sid]["summary"]), None)
    if pinned is not None:
        protected.add(pinned)
    return protected


def expiry_floor_millis(window_millis: int, snapshots: dict[int, dict[str, Any]], protected: set[int]) -> int:
    """`older_than` for expiry: the window, moved back to the oldest protected snapshot.

    Iceberg expires a snapshot committed strictly before `older_than`, so a protected snapshot
    committed exactly at the floor stays.
    """
    return min([window_millis, *(snapshots[sid]["committed_ms"] for sid in protected)])


def rotation_order(tables: list[str], last_maintained: dict[str, str]) -> list[str]:
    """Tables never maintained first, then the least recently maintained, then by name."""
    return sorted(tables, key=lambda table: (last_maintained.get(table, ""), table))


def month_start(year: int, month: int) -> datetime:
    return datetime(year, month, 1, tzinfo=timezone.utc)


def next_month(year: int, month: int) -> tuple[int, int]:
    return (year + 1, 1) if month == 12 else (year, month + 1)


def month_end_snapshots(
    history: list[tuple[datetime, int]], now: datetime
) -> list[tuple[int, int, int]]:
    """The snapshot `main` pointed at when each finished month ended, as (year, month, id).

    `history` is the table's snapshot log: when each snapshot became current. A month counts once
    it is over (UTC). The first month is the one the oldest surviving entry falls in; a month
    before that cannot be answered, because the snapshot that was current then is already gone.
    A month with no commit of its own ends on the snapshot the previous month ended on.
    """
    if not history:
        return []
    ordered = sorted(history)
    year, month = ordered[0][0].year, ordered[0][0].month
    result: list[tuple[int, int, int]] = []
    while month_start(*next_month(year, month)) <= now:
        boundary = month_start(*next_month(year, month))
        current = [snapshot_id for made_current_at, snapshot_id in ordered if made_current_at < boundary]
        result.append((year, month, current[-1]))
        year, month = next_month(year, month)
    return result


def months_ago(year: int, month: int, now: datetime) -> int:
    """How many month boundaries lie between that month and the month `now` falls in."""
    return (now.year * 12 + now.month) - (year * 12 + month)


def month_of_tag(name: str, name_format: str) -> tuple[int, int] | None:
    """The (year, month) a month-end tag names, or None for any other reference."""
    try:
        parsed = datetime.strptime(name, name_format)
    except ValueError:
        return None
    if parsed.strftime(name_format) != name:
        return None
    return parsed.year, parsed.month


def retention_tags(
    history: list[tuple[datetime, int]], existing_refs: set[str], expiry: dict[str, Any], now: datetime
) -> tuple[list[tuple[str, int]], list[str]]:
    """The tags to create and the month-end tags to drop: grandfather-father-son (root ADR-0237).

    Every finished month inside the monthly window gets a month-end tag, and every finished year a
    year-end tag on December's month-end snapshot. A month-end tag older than `keep_months` is
    dropped; a year-end tag never is. An existing tag is never moved or recreated.

    The window is counted by the month the tag names, not by the age of its snapshot. Iceberg's own
    `max-ref-age-ms` (`RETAIN`) counts from the snapshot's commit, so a table untouched since
    January would lose its September tag the next January, four months in.
    """
    monthly = expiry["month_end_tag"]
    yearly_format = expiry["year_end_tag"]["name_format"]
    create = []
    for year, month, snapshot_id in month_end_snapshots(history, now):
        names = []
        if months_ago(year, month, now) <= monthly["keep_months"]:
            names.append(month_start(year, month).strftime(monthly["name_format"]))
        if month == 12:
            names.append(month_start(year, month).strftime(yearly_format))
        create.extend((name, snapshot_id) for name in names if name not in existing_refs)
    drop = []
    for name in sorted(existing_refs):
        named = month_of_tag(name, monthly["name_format"])
        if named is not None and months_ago(*named, now) > monthly["keep_months"]:
            drop.append(name)
    return create, drop


def create_tag_sql(catalog: str, table: str, name: str, snapshot_id: int) -> str:
    """A tag with no `RETAIN` clause has no `max-ref-age-ms`: only this job's window removes it."""
    return (
        f"ALTER TABLE {catalog}.{table} CREATE TAG IF NOT EXISTS `{name}` "
        f"AS OF VERSION {snapshot_id}"
    )


def drop_tag_sql(catalog: str, table: str, name: str) -> str:
    return f"ALTER TABLE {catalog}.{table} DROP TAG IF EXISTS `{name}`"


def expire_snapshots_sql(catalog: str, table: str, cutoff: str, retain_last: int) -> str:
    """The one expiry call this lakehouse makes.

    Iceberg's `expire_snapshots` never removes a snapshot a live reference names: a branch, or a
    tag without `max-ref-age-ms`. The by-PNU publish tags the Gold snapshot each serving manifest
    reflects (`served-<lane>-...`, root ADR-0146 §2), so that snapshot outlives this call for as
    long as the manifest serves it; the month-end and year-end tags (root ADR-0237) are kept the
    same way, until `run_snapshot_expiry` drops a month-end tag past its window. Nothing here may
    pass options that drop references.
    """
    return (
        f"CALL {catalog}.system.expire_snapshots(table => '{table}', "
        f"older_than => TIMESTAMP '{cutoff}', retain_last => {retain_last})"
    )


def set_properties_sql(qualified: str, properties: dict[str, str]) -> str:
    """One `SET TBLPROPERTIES` commit; values are object names, quoted as Spark SQL literals."""
    def literal(text: str) -> str:
        return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"

    pairs = ", ".join(f"{literal(key)} = {literal(value)}" for key, value in sorted(properties.items()))
    return f"ALTER TABLE {qualified} SET TBLPROPERTIES ({pairs})"


def run_snapshot_expiry(
    spark: Any, catalog: str, table: str, expiry: dict[str, Any], now: datetime, dry_run: bool
) -> None:
    """Tag month and year ends, drop month-end tags past their window, then expire.

    The tags come first and in the same run: a month-end snapshot is pinned before the expiry
    that would otherwise remove it, so a month is lost only if this job does not run at all for
    `retain_days` after the month ends.
    """
    qualified = f"{catalog}.{table}"
    history = [
        (datetime.fromtimestamp(row.ms / 1000, tz=timezone.utc), int(row.snapshot_id))
        for row in spark.sql(
            f"SELECT unix_millis(made_current_at) AS ms, snapshot_id FROM {qualified}.history"
        ).collect()
    ]
    refs = {row.name: int(row.snapshot_id) for row in spark.sql(f"SELECT name, snapshot_id FROM {qualified}.refs").collect()}
    snapshots = {
        int(row.snapshot_id): {
            "parent_id": None if row.parent_id is None else int(row.parent_id),
            "committed_ms": int(row.ms),
            "summary": dict(row.summary or {}),
        }
        for row in spark.sql(
            f"SELECT snapshot_id, parent_id, unix_millis(committed_at) AS ms, summary FROM {qualified}.snapshots"
        ).collect()
    }
    alive = set(snapshots)
    create, drop = retention_tags(history, set(refs), expiry, now)
    for name, snapshot_id in create:
        if snapshot_id not in alive:
            # The month ended on a snapshot that is already gone; tagging another would record a
            # state the table was never in at that month's end.
            print(f"maintenance-skip step=retention_tag tag={name} reason=snapshot_expired", flush=True)
            continue
        if dry_run:
            print(f"maintenance-would step=retention_tag tag={name} snapshot={snapshot_id}", flush=True)
        else:
            spark.sql(create_tag_sql(catalog, table, name, snapshot_id)).collect()
            print(f"maintenance-done step=retention_tag tag={name} snapshot={snapshot_id}", flush=True)
    for name in drop:
        if dry_run:
            print(f"maintenance-would step=drop_month_end_tag tag={name}", flush=True)
        else:
            spark.sql(drop_tag_sql(catalog, table, name)).collect()
            print(f"maintenance-done step=drop_month_end_tag tag={name}", flush=True)

    window = int((now - timedelta(days=expiry["retain_days"])).timestamp() * 1000)
    # The ingest records past the window get their durable copy before anything expires (root
    # ADR-0332): the record is then in a summary, a property, or both at every commit.
    from platform_contracts import table_properties  # noqa: PLC0415

    carried = ingest_record_properties(
        snapshots, ancestry(snapshots, refs.get("main")), window, table_properties(spark, qualified))
    if carried and dry_run:
        print(f"maintenance-would step=ingest_record_properties count={len(carried)}", flush=True)
    elif carried:
        spark.sql(set_properties_sql(qualified, carried)).collect()
        print(f"maintenance-done step=ingest_record_properties count={len(carried)}", flush=True)

    protected = protected_snapshots(snapshots, refs.get("main"))
    cutoff = millis_text(expiry_floor_millis(window, snapshots, protected))
    if cutoff != millis_text(window):
        print(f"maintenance-floor step=snapshot_expiry protected={len(protected)} older_than={cutoff}", flush=True)
    if dry_run:
        print(f"maintenance-would step=snapshot_expiry older_than={cutoff}", flush=True)
        return
    result = spark.sql(expire_snapshots_sql(catalog, table, cutoff, expiry["retain_last"])).collect()[0]
    print(
        f"maintenance-done step=snapshot_expiry data_files={result[0]} "
        f"manifests={result[2]} older_than={cutoff}",
        flush=True,
    )


def rewrite_data_files_sql(catalog: str, table: str, compaction: dict[str, Any], ordered: bool = False) -> str:
    """The compaction call. Each file group commits on its own (`partial-progress.enabled`): a
    group that fails to read leaves the groups already rewritten in place instead of discarding them
    all, and the next run takes what is left (2026-10-15: one R2 connection reset discarded the
    whole of silver.parcel_boundaries's 152-file rewrite).

    `ordered`: the table has an Iceberg sort order (a range-distributed contract, root ADR-0164).
    It is then rewritten with the `sort` strategy, which sorts by that order; the default bin-pack
    concatenates files as they come and turned gold.parcel_panel's 34 PNU-range files into 9 files
    that each span the whole country (2026-10-26, root ADR-0307)."""
    options = {
        "min-input-files": compaction["min_input_files"],
        "target-file-size-bytes": compaction["target_file_bytes"],
        "partial-progress.enabled": "true",
        "partial-progress.max-commits": compaction["partial_progress_max_commits"],
    }
    pairs = ",".join(f"'{key}','{value}'" for key, value in options.items())
    strategy = "strategy => 'sort', " if ordered else ""
    return f"CALL {catalog}.system.rewrite_data_files(table => '{table}', {strategy}options => map({pairs}))"


def has_sort_order(spark: Any, qualified: str) -> bool:
    """Whether the live table carries an Iceberg sort order (Iceberg reports it as `sort-order`)."""
    from platform_contracts import SORT_ORDER_PROPERTY, table_properties  # noqa: PLC0415

    return bool(table_properties(spark, qualified).get(SORT_ORDER_PROPERTY, "").strip())


def file_shape(spark: Any, qualified: str) -> tuple[int, int]:
    row = spark.sql(f"SELECT count(*) c, coalesce(sum(file_size_in_bytes), 0) b FROM {qualified}.files").collect()[0]
    return int(row.c), int(row.b)


def small_file_count(spark: Any, qualified: str, target_bytes: int, fraction: float) -> int:
    """How many files sit under a fraction of the target size.

    Compaction rewrites every byte it reads, so it has to be worth doing. Counting the files
    that are actually small is the cheapest signal available — it comes from the manifest,
    without opening a single data file.
    """
    threshold = int(target_bytes * fraction)
    row = spark.sql(
        f"SELECT count(*) c FROM {qualified}.files WHERE file_size_in_bytes < {threshold}"
    ).collect()[0]
    return int(row.c)


def should_compact(small_files: int, total_files: int, trigger_count: int) -> bool:
    """Compact when enough files are small enough to be worth rewriting.

    A single small file is normal — the last file of any write is a remainder. The question is
    whether there are enough of them that reading the table pays the per-file cost repeatedly.
    """
    return small_files >= trigger_count and total_files > 1


def maintain_table(
    spark: Any, catalog: str, table: str, contract: dict[str, Any], skip: set[str], dry_run: bool
) -> None:
    """One table, every step the caller and the contract leave in, in the contract's order."""
    qualified = f"{catalog}.{table}"
    compaction = contract["compaction"]
    expiry = contract["snapshot_expiry"]
    cleanup = contract["orphan_cleanup"]
    skip = skip | set(contract.get("tables", {}).get(table, {}).get("skip", []))

    before_files, before_bytes = file_shape(spark, qualified)
    small = small_file_count(
        spark, qualified, compaction["target_file_bytes"], compaction["trigger_small_file_fraction"]
    )
    print(
        f"maintenance-before table={table} files={before_files} "
        f"bytes={before_bytes} small_files={small}",
        flush=True,
    )

    if "compaction" in skip:
        print("maintenance-skip step=compaction", flush=True)
    elif not should_compact(small, before_files, compaction["trigger_small_file_count"]):
        print(
            f"maintenance-skip step=compaction reason=below_trigger "
            f"small_files={small} trigger={compaction['trigger_small_file_count']}",
            flush=True,
        )
    elif dry_run:
        print(f"maintenance-would step=compaction small_files={small}", flush=True)
    else:
        ordered = has_sort_order(spark, qualified)
        result = spark.sql(rewrite_data_files_sql(catalog, table, compaction, ordered)).collect()[0]
        print(f"maintenance-done step=compaction read={result[0]} wrote={result[1]} sorted={ordered}", flush=True)

    if "snapshot_expiry" in skip:
        print("maintenance-skip step=snapshot_expiry", flush=True)
    else:
        run_snapshot_expiry(spark, catalog, table, expiry, datetime.now(timezone.utc), dry_run)

    if "orphan_cleanup" in skip:
        print("maintenance-skip step=orphan_cleanup", flush=True)
    else:
        cutoff = timestamp_before(cleanup["safety_days"])
        rows = spark.sql(
            f"CALL {catalog}.system.remove_orphan_files(table => '{table}', "
            f"older_than => TIMESTAMP '{cutoff}', dry_run => {str(dry_run).lower()})"
        ).collect()
        verb = "would" if dry_run else "done"
        print(f"maintenance-{verb} step=orphan_cleanup files={len(rows)} older_than={cutoff}", flush=True)

    after_files, after_bytes = file_shape(spark, qualified)
    print(
        f"maintenance-after table={table} files={before_files}->{after_files} "
        f"bytes={before_bytes}->{after_bytes}",
        flush=True,
    )


def catalog_tables(spark: Any, catalog: str, names: list[str]) -> list[str]:
    """The contract tables the catalog holds. A table no load has created yet is not an error."""
    held: set[str] = set()
    for namespace in sorted({name.split(".", 1)[0] for name in names}):
        try:
            rows = spark.sql(f"SHOW TABLES IN {catalog}.{namespace}").collect()
        except Exception:  # noqa: BLE001 - a namespace no load has created holds no table
            continue
        held.update(f"{namespace}.{row.tableName}" for row in rows)
    return [name for name in names if name in held]


def read_rotation(path: str | None) -> dict[str, str]:
    """When each table was last maintained. The file only orders the tables, so one that cannot be
    read starts the order over (every table first-come) instead of failing every run after it."""
    if path is None or not Path(path).exists():
        return {}
    try:
        rotation = json.loads(Path(path).read_text(encoding="utf-8"))
    except ValueError as error:
        print(f"maintenance-rotation-reset path={path} reason={type(error).__name__}: {error}", flush=True)
        return {}
    if not isinstance(rotation, dict):
        print(f"maintenance-rotation-reset path={path} reason=not a JSON object", flush=True)
        return {}
    return rotation


def write_rotation(path: str | None, rotation: dict[str, str]) -> None:
    """Replaces the file whole or not at all: a write that fails (2026-10-15: the disk was full)
    leaves the previous file, never an empty one."""
    if path is None:
        return
    temporary = Path(f"{path}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(rotation, indent=2, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def main() -> int:
    args = parse_args()
    contract = load_maintenance_contract()
    started = datetime.now(timezone.utc)

    from pyspark.sql import SparkSession  # noqa: PLC0415 - deferred on purpose, see module docstring
    from lakehouse_engine import apply_catalog_settings  # noqa: PLC0415

    builder = SparkSession.builder.appName("lakehouse-maintenance").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.catalog).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    if args.table is not None:
        maintain_table(spark, args.catalog, args.table, contract, set(args.skip), args.dry_run)
        spark.stop()
        return 0

    from platform_contracts import load_lakehouse_artifact  # noqa: PLC0415

    scheduled = contract["scheduled"]
    deadline = started + timedelta(minutes=scheduled["deadline_minutes"])
    skip = set(args.skip) | set(scheduled["skip"])
    rotation = read_rotation(args.state_file)
    tables = rotation_order(
        catalog_tables(spark, args.catalog, sorted(load_lakehouse_artifact()["contracts"])), rotation
    )
    done, failed, deferred = 0, [], 0
    for table in tables:
        if datetime.now(timezone.utc) >= deadline:
            deferred += 1
            print(f"maintenance-deferred table={table} reason=deadline", flush=True)
            continue
        try:
            maintain_table(spark, args.catalog, table, contract, skip, args.dry_run)
        except Exception as error:  # noqa: BLE001 - one table's failure must not stop the others
            failed.append(table)
            print(f"maintenance-failed table={table} error={type(error).__name__}: {str(error)[:300]}", flush=True)
            continue
        done += 1
        if not args.dry_run:
            rotation[table] = datetime.now(timezone.utc).isoformat()
            write_rotation(args.state_file, rotation)
    print(
        f"maintenance-summary tables={len(tables)} done={done} failed={len(failed)} deferred={deferred}",
        flush=True,
    )
    # Compaction rewrites files, never rows, and expiry removes history only: nothing downstream
    # has new data to read (root ADR-0171).
    print("foundation-job-outcome unchanged", flush=True)
    spark.stop()
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
