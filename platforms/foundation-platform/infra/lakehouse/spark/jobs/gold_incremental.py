"""Incremental panel Gold: recompute only the PNUs whose inputs changed and merge them (root ADR-0180).

A panel producer (`parcel_panel_silver_to_gold.py`, `building_panel_silver_to_gold.py`) builds
its Gold with one function from its input frames. The full rebuild hands it every input row. The
incremental rebuild hands it only the rows the changed PNUs read, then merges the result into the
current Gold. Both call the same function, so there is no second copy of the business logic; what
this module adds is the machinery around it:

1. **What changed.** Each input is read twice, at the Silver snapshot the current Gold was built
   from and at the new one, through the producer's own read path, and projected to the columns
   the build reads (`project_build_inputs`). The changed rows are the multiset difference of the
   two in both directions (`changed_rows`). A whole-table overwrite with a new `source_snapshot_id`
   changes no row here unless the columns the build reads changed: lineage columns are outside the
   projection, and a build that reads a column outside it fails on the missing column instead of
   silently seeing NULL — that is what makes the difference complete.
2. **Which PNUs.** The producer maps changed rows to the PNUs whose Gold row can depend on them
   (old and new side, so a PNU a row left is recomputed too), and restricts its inputs to the rows
   those PNUs read. Both are the producer's, next to the build they describe.
3. **Fallback.** More than `max_changed_key_fraction` of the Gold's PNUs changed, or the parity
   sample disagreed: the producer runs the full rebuild instead, and the run summary says why.
4. **Parity.** A deterministic sample of PNUs whose inputs did not change is rebuilt in the same
   pass; their rows must equal the current Gold's content. A disagreement means the current Gold is
   not what the build makes of its pinned inputs (the producer's code changed, or the difference
   missed a change), and the run falls back to the full rebuild.
5. **Merge.** Only PNUs whose content actually changed are merged, in one copy-on-write `MERGE
   INTO` whose snapshot records the new Silver pins (root ADR-0139). Rows of other PNUs keep
   their bytes, `source_snapshot_id` and `published_at_utc`; a data file that holds none of the
   merged PNUs is not rewritten. A table set to merge-on-read is refused: the by-PNU bake reads row
   data files only and refuses delete files.
6. **Keeping the comparison point.** After a commit the producer tags each pinned Silver snapshot
   (`input_tag`), so snapshot expiry cannot remove the state the next incremental run compares
   against. A pin that is gone anyway makes the planner choose the full rebuild.

PySpark is imported inside the functions: the CI runner has no PySpark and tests the pure parts.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Callable

from lakehouse_ingest import SNAPSHOT_PROPERTY_PREFIX

KEY = "pnu"
DELETE_FLAG = "_foundation_delete"
MERGE_SOURCE_VIEW = "foundation_gold_incremental_merge_source"
INPUT_TAG_PREFIX = "foundation-gold-input-"
# Iceberg's table properties for row-level commands. The bake refuses delete files.
MERGE_MODE_PROPERTY = "write.merge.mode"
COPY_ON_WRITE = "copy-on-write"
HASH_SPACE = 2**32


class IncrementalRefusal(ValueError):
    """The run must not merge; the reason goes to the summary and the full rebuild runs."""


def add_arguments(parser: Any) -> None:
    """The incremental rebuild's producer flags, the same in both producers."""
    parser.add_argument("--incremental-from-snapshots",
                        help="JSON file of the Silver pins the current Gold was built from: recompute only "
                             "the PNUs whose inputs changed since, and merge them (root ADR-0180).")
    parser.add_argument("--max-changed-key-fraction", type=float, default=None,
                        help="Above this fraction of the Gold's PNUs, run the full rebuild instead.")
    parser.add_argument("--parity-sample-keys", type=int, default=0,
                        help="PNUs whose inputs did not change, rebuilt and compared with the current Gold "
                             "before merging.")


def validate_arguments(args: Any, sources: tuple[str, ...]) -> dict[str, str] | None:
    """The previous pins an incremental run compares against, or None for a full rebuild."""
    from lakehouse_snapshot_pins import read_snapshot_document, snapshot_id

    if args.incremental_from_snapshots is None:
        return None
    if args.input_mode != "iceberg" or args.write_mode != "iceberg":
        raise ValueError("--incremental-from-snapshots reads Iceberg snapshots and merges into Iceberg")
    if args.iceberg_write_mode != "overwrite":
        raise ValueError("--incremental-from-snapshots replaces the full rebuild, whose write mode is overwrite")
    if getattr(args, "region_prefix", None) or getattr(args, "pnu_prefix", None):
        raise ValueError("--incremental-from-snapshots merges into the whole Gold; a region run cannot")
    if args.max_changed_key_fraction is None or not 0 < args.max_changed_key_fraction <= 1:
        raise ValueError("--incremental-from-snapshots needs --max-changed-key-fraction in (0, 1]")
    if args.parity_sample_keys < 0:
        raise ValueError("--parity-sample-keys must be non-negative")
    previous = read_snapshot_document(args.incremental_from_snapshots)
    if not isinstance(previous, dict):
        raise ValueError("--incremental-from-snapshots must map Silver tables to snapshot ids")
    unknown = sorted(set(previous) - set(sources))
    if unknown:
        raise ValueError(f"--incremental-from-snapshots names inputs this producer does not read: {unknown}")
    return {name: snapshot_id(value) for name, value in previous.items()}


def input_tag(gold_table: str) -> str:
    """The tag that keeps, on each Silver input, the snapshot `gold_table` was last built from."""
    return INPUT_TAG_PREFIX + gold_table.replace(".", "-")


def changed_inputs(previous: dict[str, str], current: dict[str, str]) -> list[str]:
    """Inputs whose pinned snapshot moved. Every current input must have a previous pin."""
    missing = sorted(set(current) - set(previous))
    if missing:
        raise IncrementalRefusal(f"the current Gold was not built from {missing}")
    return sorted(name for name in current if previous[name] != current[name])


def change_fraction_verdict(affected: int, gold_rows: int, max_fraction: float) -> str | None:
    """Why the change set is too large to merge, or None."""
    if not 0 < max_fraction <= 1:
        raise ValueError(f"max_changed_key_fraction must be in (0, 1], got {max_fraction}")
    if gold_rows <= 0:
        return "the current Gold is empty"
    if affected > gold_rows * max_fraction:
        return (f"{affected} of {gold_rows} PNUs have changed inputs, more than the "
                f"max_changed_key_fraction {max_fraction}")
    return None


def sample_threshold(sample_keys: int, gold_rows: int) -> int:
    """The 32-bit hash bound under which about `sample_keys` of `gold_rows` keys fall."""
    if sample_keys <= 0 or gold_rows <= 0:
        return 0
    return min(HASH_SPACE, math.ceil(HASH_SPACE * sample_keys / gold_rows))


def project_build_inputs(frames: dict[str, Any], build_columns: dict[str, tuple[str, ...]]) -> dict[str, Any]:
    """Each input reduced to the columns its build reads (root ADR-0180 §1).

    The build then cannot read anything the change detection does not compare: a column it needs
    and the declaration lacks fails as a missing column.
    """
    missing = sorted(set(frames) - set(build_columns))
    if missing:
        raise ValueError(f"no build columns are declared for {missing}")
    # A declared column the input lacks fails in the select, by name.
    return {name: frame.select(*build_columns[name]) for name, frame in frames.items()}


def changed_rows(old: Any, new: Any) -> Any:
    """The rows present in one side and not the other, counting duplicates (both directions)."""
    columns = list(new.columns)
    old = old.select(*columns)
    return old.exceptAll(new).unionByName(new.exceptAll(old))


def trimmed_keys(frame: Any, column: str = KEY, alias: str = KEY, trim: bool = True) -> Any:
    """The distinct non-blank (trimmed, unless `trim` is off) values of `column`, named `alias`."""
    from pyspark.sql import functions as F

    value = F.trim(F.col(column)) if trim else F.col(column)
    return frame.select(value.alias(alias)).where(F.col(alias).isNotNull() & (F.length(alias) > 0)).distinct()


def union_keys(frames: list[Any], alias: str = KEY) -> Any:
    from functools import reduce

    return reduce(lambda left, right: left.unionByName(right), [f.select(alias) for f in frames]).distinct()


def rows_matching_any(frame: Any, matches: list[tuple[str, Any, str, bool]]) -> Any:
    """The rows of `frame` whose `column` (trimmed when asked) is in any of the key frames.

    `matches`: (column of `frame`, key frame, its key column, trim). Each row is kept at most once
    and keeps exactly its own columns; the key frames are distinct, so the left joins add no rows.
    """
    from pyspark.sql import functions as F

    columns = list(frame.columns)
    flags = []
    for index, (column, keys, key_column, trim) in enumerate(matches):
        flag = f"_foundation_match_{index}"
        value = F.trim(F.col(column)) if trim else F.col(column)
        right = keys.select(F.col(key_column).alias(flag)).distinct()
        frame = frame.join(right, value == right[flag], "left")
        flags.append(F.col(flag).isNotNull())
    keep = flags[0]
    for flag in flags[1:]:
        keep = keep | flag
    return frame.where(keep).select(*columns)


@dataclass
class Lane:
    """What a producer hands the incremental rebuild: its build and its two dependency maps."""

    build: Callable[[dict[str, Any]], Any]
    restrict: Callable[[dict[str, Any], Any], dict[str, Any]]
    affected: Callable[[list[str]], Any]
    validate: Callable[[Any], tuple[int, dict[str, int]]]
    content_columns: tuple[str, ...]
    gold_columns: tuple[str, ...]
    whole_table_inputs: tuple[str, ...] = ()


@dataclass
class Outcome:
    """A built Gold (full) or a merge plan (incremental), and how to commit it."""

    mode: str
    row_count: int
    metrics: dict[str, int]
    gold: Any = None
    merge: dict[str, Any] | None = None
    full_reason: str | None = None
    changed_inputs: list[str] = field(default_factory=list)
    merged: bool | None = None

    def commit(self, spark: Any, table: str, pins: dict[str, str], write_mode: str) -> None:
        """Write the full Gold in `write_mode`, or merge the changed PNUs; one snapshot either way."""
        from pyspark.sql import functions as F

        from gold_rebuild import SOURCE_SNAPSHOTS_PROPERTY, pins_property_value, write_gold_snapshot

        if self.mode == "full":
            write_gold_snapshot(self.gold, table, write_mode, pins, F.lit(True))
            return
        self.merged = merge_into_gold(spark, table, self.merge["source"],
                                      {SOURCE_SNAPSHOTS_PROPERTY: pins_property_value(pins)})

    def disposition(self, args: Any) -> str:
        return "iceberg_merge" if self.mode == "incremental" else "iceberg_" + args.iceberg_write_mode

    def report(self) -> dict[str, Any]:
        report: dict[str, Any] = {"mode": self.mode, "changed_inputs": self.changed_inputs}
        if self.full_reason:
            report["full_reason"] = self.full_reason
        if self.merge is not None:
            report["counts"] = self.merge["counts"]
        if self.merged is not None:
            report["merged"] = self.merged
        return report

    def release(self) -> None:
        for frame in ([self.gold] if self.gold is not None else []) + (self.merge or {}).get("persisted", []):
            frame.unpersist()


def build_or_merge(spark: Any, args: Any, lane: Lane, frames: dict[str, Any], pins: dict[str, str],
                   previous: dict[str, str] | None, target_table: str, validate_full: Callable,
                   assert_minimum: Callable[[int, int | None], None]) -> Outcome:
    """The incremental plan when it applies, else the full build; checked, not yet written.

    The full build is the one the producer always ran: every input to `lane.build`, its quality
    gates, the expected count and the row floor. The incremental plan runs the same gates on the
    rows it recomputed and the floor on the row count the merge will leave.
    """
    from pyspark.storagelevel import StorageLevel

    reason, changed = None, []
    if previous is not None:
        try:
            changed = changed_inputs(previous, pins)
            whole = [name for name in changed if name in lane.whole_table_inputs]
            if whole:
                raise IncrementalRefusal(f"{whole} changed, and a change there is not mapped to PNUs")
            if not spark.catalog.tableExists(target_table.replace("`", "")):
                raise IncrementalRefusal(f"{target_table} does not exist")
            current = spark.table(target_table)
            if tuple(current.columns) != lane.gold_columns:
                raise IncrementalRefusal("the current Gold's columns differ from the contract")
            gold_rows = current.count()
            affected = lane.affected(changed) if changed else current.select(KEY).limit(0)
            plan = plan_merge(spark, build=lane.build, restrict=lane.restrict, new=frames, affected=affected,
                              current_gold=current, gold_rows=gold_rows, content_columns=lane.content_columns,
                              max_fraction=args.max_changed_key_fraction, sample_keys=args.parity_sample_keys,
                              seed=pins_seed(pins))
            _, metrics = lane.validate(plan["recomputed"])
            if args.expected_count is not None and plan["row_count"] != args.expected_count:
                raise ValueError(f"Expected {args.expected_count} Gold rows, the merge leaves {plan['row_count']}")
            assert_minimum(plan["row_count"], args.minimum_count)
            return Outcome("incremental", plan["row_count"], metrics, merge=plan, changed_inputs=changed)
        except IncrementalRefusal as refusal:
            reason = str(refusal)
            print(f"gold-incremental-fallback {reason}", flush=True)
    gold = lane.build(frames).persist(StorageLevel.MEMORY_AND_DISK)
    count, metrics = validate_full(gold, args.expected_count)
    assert_minimum(count, args.minimum_count)
    return Outcome("full", count, metrics, gold=gold, full_reason=reason, changed_inputs=changed)


def pins_seed(pins: dict[str, str]) -> str:
    """The parity sample's seed: the same pins pick the same sample, new pins a new one."""
    return ",".join(f"{name}={pins[name]}" for name in sorted(pins))


def plan_merge(spark: Any, *, build: Callable[[dict[str, Any]], Any],
               restrict: Callable[[dict[str, Any], Any], dict[str, Any]], new: dict[str, Any],
               affected: Any, current_gold: Any, gold_rows: int, content_columns: tuple[str, ...],
               max_fraction: float, sample_keys: int, seed: str) -> dict[str, Any]:
    """Recompute the affected PNUs and a parity sample; return the merge source and its counts.

    Raises IncrementalRefusal when the change set is too large or the parity sample disagrees.
    `current_gold` is the Gold table at its current snapshot; `affected` a frame of `pnu`.

    Each stage is materialized (`localCheckpoint`) before the next reads it: the build reads its
    inputs several times over, and an input restricted by key frames that are themselves joins of
    two snapshots would make one plan of every copy — on a 41-row fixture that plan alone took a
    4g driver heap. The checkpoints also fix the merge source before the MERGE reads the target.
    """
    from pyspark.sql import functions as F

    affected = affected.select(KEY).distinct().localCheckpoint()
    affected_count = affected.count()
    verdict = change_fraction_verdict(affected_count, gold_rows, max_fraction)
    if verdict:
        raise IncrementalRefusal(verdict)
    hashed = F.conv(F.substring(F.sha2(F.concat(F.col(KEY), F.lit("\x1f" + seed)), 256), 1, 8), 16, 10)
    sample = (current_gold.select(KEY).join(affected, KEY, "left_anti")
              .where(hashed.cast("long") < sample_threshold(sample_keys, gold_rows))
              .localCheckpoint())
    sample_count = sample.count()
    keys = affected.unionByName(sample).distinct().localCheckpoint()
    restricted = {name: frame.localCheckpoint() for name, frame in restrict(new, keys).items()}
    built = build(restricted).join(keys, KEY, "left_semi").localCheckpoint()

    compared = list(dict.fromkeys([KEY, *content_columns, "row_digest"]))
    rebuilt_sample = built.join(sample, KEY, "left_semi").select(*compared)
    gold_sample = current_gold.join(sample, KEY, "left_semi").select(*compared)
    disagreements = rebuilt_sample.exceptAll(gold_sample).unionByName(gold_sample.exceptAll(rebuilt_sample))
    disagreeing = disagreements.select(KEY).distinct()
    disagreeing_count = disagreeing.count()
    if disagreeing_count:
        examples = sorted(r[KEY] for r in disagreeing.limit(3).collect())
        raise IncrementalRefusal(
            f"parity sample: {disagreeing_count} of {sample_count} PNUs whose inputs did not change "
            f"rebuild differently from the current Gold (e.g. {examples}); the current Gold is not "
            "what the build makes of its pinned inputs")

    recomputed = built.join(affected, KEY, "left_semi")
    current = current_gold.select(KEY, F.col("row_digest").alias("_current_digest"))
    upserts = (recomputed.join(current, KEY, "left")
               .where(~F.col("row_digest").eqNullSafe(F.col("_current_digest"))))
    inserted = upserts.where(F.col("_current_digest").isNull()).count()
    updated = upserts.where(F.col("_current_digest").isNotNull()).count()
    deletes = (current_gold.select(KEY).join(affected, KEY, "left_semi")
               .join(recomputed.select(KEY), KEY, "left_anti"))
    deleted = deletes.count()
    gold_columns = list(current_gold.columns)
    source = upserts.select(*gold_columns).withColumn(DELETE_FLAG, F.lit(False)).unionByName(
        deletes.select(*[F.col(KEY) if c == KEY else F.lit(None).cast(current_gold.schema[c].dataType).alias(c)
                         for c in gold_columns]).withColumn(DELETE_FLAG, F.lit(True))).localCheckpoint()
    return {
        "source": source,
        "recomputed": recomputed,
        "counts": {
            "affected_pnus": affected_count,
            "parity_sample_pnus": sample_count,
            "inserted_pnus": inserted,
            "updated_pnus": updated,
            "deleted_pnus": deleted,
            "unchanged_recomputed_pnus": affected_count - inserted - updated - deleted,
        },
        "row_count": gold_rows + inserted - deleted,
        "persisted": [],
    }


def table_property(spark: Any, table: str, name: str) -> str | None:
    rows = spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
    return next((row["value"] for row in rows if row["key"] == name), None)


def refuse_merge_on_read(spark: Any, table: str) -> None:
    mode = table_property(spark, table, MERGE_MODE_PROPERTY) or COPY_ON_WRITE
    if mode != COPY_ON_WRITE:
        raise ValueError(
            f"{table} sets {MERGE_MODE_PROPERTY}={mode}; the by-PNU bake reads row data files only "
            "and refuses delete files, so the incremental merge needs copy-on-write")


def with_commit_properties(spark: Any, properties: dict[str, str], action: Callable[[], Any]) -> None:
    """Run `action` (SQL that commits) with `properties` in its Iceberg snapshot summary.

    SQL commands take snapshot properties only through Iceberg's thread-local CommitMetadata. The
    callable runs in this Python thread over PySpark's pinned py4j connection, so the SQL it issues
    runs in the Java thread that set the properties.
    """
    from pyspark.java_gateway import ensure_callback_server_started

    context = spark.sparkContext
    ensure_callback_server_started(context._gateway)
    jvm = context._jvm
    java_properties = jvm.java.util.HashMap()
    for key, value in properties.items():
        java_properties.put(key, value)
    failure = jvm.java.lang.Class.forName("java.lang.RuntimeException")
    jvm.org.apache.iceberg.spark.CommitMetadata.withCommitProperties(java_properties, JavaCallable(action), failure)


class JavaCallable:
    """A `java.util.concurrent.Callable` that runs a Python action (py4j callback)."""

    def __init__(self, action: Callable[[], Any]) -> None:
        self.action = action

    def call(self) -> int:
        self.action()
        return 0

    class Java:
        implements = ["java.util.concurrent.Callable"]


def merge_sql(table: str, columns: list[str]) -> str:
    update = ", ".join(f"t.`{c}` = s.`{c}`" for c in columns)
    names = ", ".join(f"`{c}`" for c in columns)
    values = ", ".join(f"s.`{c}`" for c in columns)
    return (
        f"MERGE INTO {table} t USING {MERGE_SOURCE_VIEW} s ON t.`{KEY}` = s.`{KEY}` "
        f"WHEN MATCHED AND s.`{DELETE_FLAG}` THEN DELETE "
        f"WHEN MATCHED AND NOT s.`{DELETE_FLAG}` AND NOT (t.`row_digest` <=> s.`row_digest`) "
        f"THEN UPDATE SET {update} "
        f"WHEN NOT MATCHED AND NOT s.`{DELETE_FLAG}` THEN INSERT ({names}) VALUES ({values})"
    )


def merge_into_gold(spark: Any, table: str, source: Any, properties: dict[str, str]) -> bool:
    """Merge `source` into `table` in one snapshot carrying `properties`. False when it was empty.

    An empty change set still commits, as an empty append with the same properties, so the Gold
    records that it reflects the new Silver pins and the next plan starts from them.
    """
    refuse_merge_on_read(spark, table)
    if source.limit(1).count() == 0:
        empty = spark.table(table).limit(0)
        writer = empty.writeTo(table)
        for key, value in properties.items():
            writer = writer.option(f"{SNAPSHOT_PROPERTY_PREFIX}{key}", value)
        writer.append()
        return False
    columns = [c for c in source.columns if c != DELETE_FLAG]
    source.createOrReplaceTempView(MERGE_SOURCE_VIEW)
    try:
        with_commit_properties(spark, properties, lambda: spark.sql(merge_sql(table, columns)))
    finally:
        spark.catalog.dropTempView(MERGE_SOURCE_VIEW)
    return True


def retain_input_snapshots(spark: Any, catalog: str, namespace: str, gold_table: str,
                           pins: dict[str, str]) -> list[str]:
    """Tag each pinned Silver snapshot so expiry keeps the next incremental run's comparison point.

    One tag per (input, Gold table), moved on every commit, so it holds at most one old snapshot.
    """
    tag = input_tag(gold_table)
    tagged = []
    for name, snapshot in sorted(pins.items()):
        table = f"`{catalog}`.`{namespace}`.`{name.split('.', 1)[1]}`"
        spark.sql(f"ALTER TABLE {table} CREATE OR REPLACE TAG `{tag}` AS OF VERSION {int(snapshot)}")
        tagged.append(name)
    return tagged
