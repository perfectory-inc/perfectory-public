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
   **An appended input skips the difference** (root ADR-0189). When every snapshot after the pin
   up to the new one is an Iceberg `append` (`appended_span`), the new snapshot holds the old one's
   rows unchanged plus the added ones, so the producer reads the added rows (`read_appended`, an
   incremental append scan) as the change set instead of reading both snapshots in full. A read
   that is not a row-local filter of the table (a producer that picks one batch of several) maps
   that itself; any other operation in between takes the difference as before.
2. **Which PNUs.** The producer maps changed rows to the PNUs whose Gold row can depend on them
   (old and new side, so a PNU a row left is recomputed too), and restricts its inputs to the rows
   those PNUs read. Both are the producer's, next to the build they describe.
3. **Fallback.** More than `max_changed_key_fraction` of the Gold's PNUs changed, or the parity
   sample disagreed: the producer runs the full rebuild instead, and the run summary says why.
4. **Parity.** A deterministic sample of PNUs whose inputs did not change is rebuilt in the same
   pass; their rows must equal the current Gold's content. A disagreement means the current Gold is
   not what the build makes of its pinned inputs (the producer's code changed, or the difference
   missed a change), and the run falls back to the full rebuild.
5. **Merge.** Only PNUs whose content actually changed are merged, in one `MERGE INTO` whose
   snapshot records the new Silver pins (root ADR-0139). Rows of other PNUs keep their bytes,
   `source_snapshot_id` and `published_at_utc`. The merge is merge-on-read, as the Gold rebuild
   contract's `merge.mode` says (root ADR-0344): it writes the merged rows as new data files and a
   position delete file per data file it touched, and rewrites no data file; compaction folds the
   deletes in on a cadence (lakehouse_maintenance).
6. **Recording the change.** Every commit that changes rows by a known set appends that set to
   `gold.<t>__changes` (root ADR-0191 §4, ADR-0312 §1): the merged PNUs under the commit's
   (parent, head) step, plus one NULL marker so a step that changed nothing is recorded too. The
   by-PNU bake's delta reads those PNUs instead of joining two whole snapshots; a step without a
   record (a backfill, a hand write) sends it back to that join. A full overwrite records its
   changed keys too, by the `row_digest` difference the publish in 8 takes (root ADR-0347).
7. **Keeping the comparison point.** After a commit the producer tags each pinned Silver snapshot
   (`input_tag`), so snapshot expiry cannot remove the state the next incremental run compares
   against. A pin that is gone anyway makes the planner choose the full rebuild.
8. **Publishing a code change by key** (root ADR-0321 §5). A producer code change recomputes every
   row, but `--publish-changed-keys` publishes the build Write-Audit-Publish style
   (`Outcome.publish_by_key`): written to a shadow branch, compared with main by `row_digest`, and
   only the differing keys merged and recorded as in 5-6; nothing differs, nothing is rewritten.

PySpark is imported inside the functions: the CI runner has no PySpark and tests the pure parts.
"""
from __future__ import annotations

import json
import math
import shutil
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from changelog import CHANGES_SUFFIX, FROM_SNAPSHOT, TO_SNAPSHOT, chain
from lakehouse_ingest import SNAPSHOT_PROPERTY_PREFIX

KEY = "pnu"
DELETE_FLAG = "_foundation_delete"
MERGE_SOURCE_VIEW = "foundation_gold_incremental_merge_source"
INPUT_TAG_PREFIX = "foundation-gold-input-"
STAGES_PREFIX = "gold-incremental-"
# Iceberg's table properties for row-level commands, all set to the Gold rebuild contract's
# `merge.mode` (root ADR-0344): a MERGE that updates or deletes writes by the first.
ROW_LEVEL_MODE_PROPERTIES = ("write.merge.mode", "write.update.mode", "write.delete.mode")
ROW_LEVEL_MODES = ("copy-on-write", "merge-on-read")
HASH_SPACE = 2**32
# The keys a commit changed, on the commit itself: the shape-ledger check weighs the bytes it
# rewrote against them (root ADR-0314 §4).
KEYS_CHANGED_PROPERTY = "foundation.keys-changed"
# The shadow branch a code change's full build is written to before it is published (root ADR-0321 §5).
AUDIT_BRANCH = "foundation_code_change_audit"


def keys_changed(counts: dict[str, int]) -> int:
    """The PNUs a merge inserts, updates or deletes; recomputed PNUs that came out equal are not."""
    return counts["inserted_pnus"] + counts["updated_pnus"] + counts["deleted_pnus"]


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
    parser.add_argument("--publish-changed-keys", type=float, default=None, metavar="FRACTION",
                        help="A code change (root ADR-0321 §5): write the full build to a shadow branch and "
                             "merge into main only the rows whose row_digest differs; above FRACTION of the "
                             "Gold's PNUs, fast-forward main to the branch instead.")


def validate_arguments(args: Any, sources: tuple[str, ...]) -> dict[str, str] | None:
    """The previous pins an incremental run compares against, or None for a full rebuild."""
    from lakehouse_snapshot_pins import read_snapshot_document, snapshot_id

    publish = getattr(args, "publish_changed_keys", None)
    if publish is not None:
        if not 0 < publish <= 1:
            raise ValueError("--publish-changed-keys must be in (0, 1]")
        if args.incremental_from_snapshots is not None:
            raise ValueError("--publish-changed-keys publishes a full build; an incremental run merges already")
        if args.input_mode != "iceberg" or args.write_mode != "iceberg" or args.iceberg_write_mode != "overwrite":
            raise ValueError("--publish-changed-keys replaces the full Iceberg overwrite")
        if getattr(args, "region_prefix", None) or getattr(args, "pnu_prefix", None):
            raise ValueError("--publish-changed-keys compares the whole Gold; a region run cannot")
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


def appended_span(snapshots: dict[int, tuple[int | None, str]], old: int, new: int) -> bool:
    """Whether `new` descends from `old` through `append` snapshots only (root ADR-0189).

    `snapshots` maps each snapshot id to (parent id, operation), as the table's `snapshots`
    metadata table holds them. False when the chain does not reach `old` (expired, or another
    branch's lineage) or passes any other operation (overwrite, delete, replace).
    """
    current, seen = new, set()
    while current != old:
        if current in seen or current not in snapshots:
            return False
        seen.add(current)
        parent, operation = snapshots[current]
        if operation != "append" or parent is None:
            return False
        current = parent
    return True


def appended_inputs(spark: Any, tables: dict[str, str], previous: dict[str, str], pins: dict[str, str],
                    changed: list[str]) -> list[str]:
    """The changed inputs among `tables` (input -> Iceberg table) whose pins moved by appends only."""
    appended = []
    for name in changed:
        if name not in tables:
            continue
        rows = spark.sql(f"SELECT snapshot_id, parent_id, operation FROM {tables[name]}.snapshots").collect()
        snapshots = {int(r.snapshot_id): (None if r.parent_id is None else int(r.parent_id), r.operation)
                     for r in rows}
        if appended_span(snapshots, int(previous[name]), int(pins[name])):
            appended.append(name)
    return appended


def read_appended(spark: Any, table: str, old: str, new: str) -> Any:
    """The rows the append snapshots after `old` up to `new` added (Iceberg's incremental append scan)."""
    return (spark.read.format("iceberg").option("start-snapshot-id", str(int(old)))
            .option("end-snapshot-id", str(int(new))).load(table))


def phase_done(name: str, started: float) -> float:
    """Print one phase's wall seconds to the run log; return the time it ended."""
    now = time.monotonic()
    print(f"gold-incremental-phase {name} seconds={now - started:.1f}", flush=True)
    return now


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


def changed_rows(old: Any, new: Any, keys: tuple[str, ...]) -> Any:
    """The `keys` of the rows present in one side and not the other, counting duplicates (both
    directions).

    Rows are compared by their keys and a 96-bit fingerprint (xxhash64 and murmur3) of every column instead of the
    columns themselves: the callers map changed rows to PNUs through `keys` alone, and the
    difference's shuffle then carries a few keys per row instead of the whole projection. Measured
    2026-10-10: a full-row difference of the 114M-row unit-area release took 20 of the incremental
    rebuild's 26 minutes. Each column's null flag is fingerprinted beside its value: Spark's hashes
    skip NULLs, which would make (NULL, x) and (x, NULL) one row.
    """
    columns = list(new.columns)
    missing = sorted(set(keys) - set(columns))
    if missing:
        raise ValueError(f"changed_rows keys {missing} are not columns of the compared rows")
    old, new = _fingerprinted(old.select(*columns), keys), _fingerprinted(new, keys)
    return old.exceptAll(new).unionByName(new.exceptAll(old)).select(*keys)


def _fingerprinted(frame: Any, keys: tuple[str, ...]) -> Any:
    """`keys` and two independent hashes of every column and its null flag."""
    from pyspark.sql import functions as F

    hashed = [*(F.col(c).isNull() for c in frame.columns), *(F.col(c) for c in frame.columns)]
    return frame.select(*keys, F.xxhash64(*hashed).alias("_fingerprint_a"), F.hash(*hashed).alias("_fingerprint_b"))


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
    """What a producer hands the incremental rebuild: its build and its two dependency maps.

    `affected(changed, appended)` maps the changed inputs to PNUs; `appended` are those of
    `append_inputs` (input -> Iceberg table: the inputs whose producer can read an append as their
    change set) whose pins moved by appends only (root ADR-0189).
    """

    build: Callable[[dict[str, Any]], Any]
    restrict: Callable[[dict[str, Any], Any], dict[str, Any]]
    affected: Callable[[list[str], list[str]], Any]
    validate: Callable[[Any], tuple[int, dict[str, int]]]
    content_columns: tuple[str, ...]
    gold_columns: tuple[str, ...]
    whole_table_inputs: tuple[str, ...] = ()
    append_inputs: dict[str, str] = field(default_factory=dict)
    # A Gold row's identity. A PNU stays the unit of change: every row of an affected PNU is
    # recomputed, and its rows the rebuild no longer makes are deleted (root ADR-0204).
    row_key: tuple[str, ...] = (KEY,)


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
    appended_inputs: list[str] = field(default_factory=list)
    merged: bool | None = None
    # True when the full build's content equals the current Gold's, so no row was rewritten.
    content_unchanged: bool | None = None
    # A full build of a code change (root ADR-0321 §5): above this fraction of the Gold's PNUs
    # changed, main is fast-forwarded to the shadow branch; at or below it, the changed keys merge.
    publish_changed_keys: float | None = None
    row_key: tuple[str, ...] = (KEY,)
    published: dict[str, Any] | None = None
    # A whole overwrite's changed keys, as it recorded them (root ADR-0347).
    overwrite_changes: dict[str, int] | None = None

    def commit(self, spark: Any, table: str, pins: dict[str, str], write_mode: str) -> None:
        """Write the full Gold in `write_mode`, or merge the changed PNUs; one snapshot either way.

        Root ADR-0274 §1: a full build whose content fingerprint equals the current Gold's rewrites
        no row; it commits an empty append carrying the new Silver pins and the same fingerprint, as
        an empty merge does, so the next plan starts from the new pins and the bake reflects the
        snapshot without comparing it. A full build of a code change is published by its changed
        keys (`publish_changed_keys`).
        """
        from pyspark.sql import functions as F

        import gold_fingerprint
        from gold_rebuild import pins_properties, write_gold_snapshot

        current = current_fingerprint(spark, table)
        if self.mode == "full":
            built = gold_fingerprint.of_frame(self.gold)
            properties = {} if built is None else {gold_fingerprint.PROPERTY: built}
            self.content_unchanged = built is not None and built == current
            if self.content_unchanged:
                write_gold_snapshot(spark.table(table).limit(0), table, "append", pins, F.lit(True), properties)
                record_changes(spark, table, None)
                if self.publish_changed_keys is not None:
                    self.merged, self.published = False, {"by": "metadata", "counts": {"affected_pnus": 0}}
                return
            if self.publish_changed_keys is not None and spark.catalog.tableExists(table.replace("`", "")):
                self.publish_by_key(spark, table, pins, properties)
                return
            self.overwrite_recording_changes(spark, table, write_mode, pins, properties)
            return
        started = time.monotonic()
        # An empty merge leaves the content as it is, so it keeps the current fingerprint.
        unchanged = {} if current is None else {gold_fingerprint.PROPERTY: current}
        self.merged = merge_into_gold(spark, table, self.merge["source"],
                                      {**pins_properties(table, pins),
                                       KEYS_CHANGED_PROPERTY: str(keys_changed(self.merge["counts"]))},
                                      self.merge["row_key"],
                                      unchanged)
        phase_done("merge", started)
        record_changes(spark, table, self.merge["source"] if self.merged else None)

    def overwrite_recording_changes(self, spark: Any, table: str, write_mode: str, pins: dict[str, str],
                                    properties: dict[str, str]) -> None:
        """Write the full Gold whole, and record the keys it changed like any other commit (root ADR-0347).

        The keys are the publish's own difference (`merge_source`: `row_key` and `row_digest` against
        the current Gold), fixed on disk before the overwrite replaces what it compares against. The
        overwrite carries their count (KEYS_CHANGED_PROPERTY) and appends them to `gold.<t>__changes`,
        so the bake's delta reads them instead of joining two whole snapshots, and a large change it
        finds is one a record explains (root ADR-0346). A first build (no table yet) has no previous
        rows to differ from; its commit has no parent step to record. A difference that cannot be
        taken (the current Gold lacks a column the build no longer makes) leaves the step unrecorded,
        as before: recording never fails the write (`record_changes`).
        """
        from pyspark.sql import functions as F

        from gold_rebuild import write_gold_snapshot

        if not spark.catalog.tableExists(table.replace("`", "")):
            write_gold_snapshot(self.gold, table, write_mode, pins, F.lit(True), properties)
            return
        started = time.monotonic()
        stages = Stages(spark)
        try:
            try:
                if write_mode == "append":
                    changed = self.gold.select(KEY)
                    counts = {"inserted_pnus": self.row_count, "updated_pnus": 0, "deleted_pnus": 0}
                else:
                    source, counts = merge_source(self.gold, spark.table(table), self.row_key)
                    changed = source.select(KEY)
                changed = stages.materialize("overwrite-changed-keys", changed.distinct())
                self.overwrite_changes = {"affected_pnus": changed.count(), **counts}
            except Exception as error:  # noqa: BLE001 - see the docstring
                print(f"gold-changes-unrecorded table={table} reason={error}", flush=True)
                changed = None
            started = phase_done("overwrite-diff", started)
            recorded = {} if changed is None else {KEYS_CHANGED_PROPERTY: str(keys_changed(counts))}
            write_gold_snapshot(self.gold, table, write_mode, pins, F.lit(True), {**properties, **recorded})
            phase_done("overwrite", started)
            if changed is not None:
                record_changes(spark, table, changed)
        finally:
            stages.remove()

    def publish_by_key(self, spark: Any, table: str, pins: dict[str, str], properties: dict[str, str]) -> None:
        """Write-Audit-Publish a full build (root ADR-0321 §5, Netflix's Iceberg WAP).

        Write: the build goes to the shadow branch AUDIT_BRANCH, made from main's head; main and its
        readers see nothing. Audit: the branch holds the rows the producer's gates passed. Publish:
        the branch is compared with main by `row_key` and `row_digest`; only the keys that differ are
        merged into main, in one commit that records them (`gold.<t>__changes`, KEYS_CHANGED_PROPERTY).
        No key differs: an empty commit adopts the new code version and pins. More than
        `publish_changed_keys` of the Gold's PNUs differ: main is fast-forwarded to the branch (its
        commit is the full write already made), and the changed keys are recorded all the same.
        The branch is dropped once main has the result.
        """
        from lakehouse_engine import current_snapshot
        from lakehouse_ingest import checked_ref
        from pyspark.sql import functions as F

        from gold_rebuild import pins_properties, write_gold_snapshot

        started = time.monotonic()
        branch = checked_ref(AUDIT_BRANCH)
        head = int(current_snapshot(spark, table))
        spark.sql(f"ALTER TABLE {table} CREATE OR REPLACE BRANCH `{branch}` AS OF VERSION {head}")
        write_gold_snapshot(self.gold, table, "overwrite", pins, F.lit(True), properties, branch=branch)
        shadow = spark.table(f"{table}.`branch_{branch}`")
        written = shadow.count()
        if written != self.row_count:
            raise ValueError(f"the shadow branch {branch} of {table} holds {written} rows, the build {self.row_count}; "
                             "main is untouched")
        started = phase_done("shadow-write", started)
        current = spark.table(table)
        stages = Stages(spark)
        try:
            source, counts = merge_source(shadow, current, self.row_key)
            source = stages.materialize("publish-source", source)
            changed = source.select(KEY).distinct().count()
            gold_pnus = self.row_count if self.row_key == (KEY,) else shadow.select(KEY).distinct().count()
            started = phase_done("diff", started)
            if changed > self.publish_changed_keys * max(gold_pnus, 1):
                catalog, namespace, name = table.replace("`", "").split(".")
                spark.sql(f"CALL `{catalog}`.system.fast_forward('{namespace}.{name}', 'main', '{branch}')")
                self.merged, by = True, "fast_forward"
            else:
                self.merged = merge_into_gold(spark, table, source, {**pins_properties(table, pins), **properties,
                                                                     KEYS_CHANGED_PROPERTY: str(keys_changed(counts))},
                                              self.row_key)
                by = "merge" if self.merged else "metadata"
            phase_done("publish", started)
            record_changes(spark, table, source if self.merged else None)
        finally:
            stages.remove()
        spark.sql(f"ALTER TABLE {table} DROP BRANCH IF EXISTS `{branch}`")
        self.published = {"by": by, "counts": {"affected_pnus": changed, **counts}}

    def disposition(self, args: Any) -> str:
        return "iceberg_merge" if self.mode == "incremental" else "iceberg_" + args.iceberg_write_mode

    def report(self) -> dict[str, Any]:
        report: dict[str, Any] = {"mode": self.mode, "changed_inputs": self.changed_inputs,
                                  "appended_inputs": self.appended_inputs}
        if self.full_reason:
            report["full_reason"] = self.full_reason
        if self.merge is not None:
            report["counts"] = self.merge["counts"]
        if self.published is not None:
            report["publish"] = {"to": "changed_keys", "by": self.published["by"]}
            report["counts"] = self.published["counts"]
        if self.overwrite_changes is not None:
            report["overwrite_changes"] = self.overwrite_changes
        if self.merged is not None:
            report["merged"] = self.merged
        if self.content_unchanged is not None:
            report["content_unchanged"] = self.content_unchanged
        return report

    def release(self) -> None:
        for frame in ([self.gold] if self.gold is not None else []) + (self.merge or {}).get("persisted", []):
            frame.unpersist()
        stages = (self.merge or {}).get("stages")
        if stages is not None:
            stages.remove()


def build_or_merge(spark: Any, args: Any, lane: Lane, frames: dict[str, Any], pins: dict[str, str],
                   previous: dict[str, str] | None, target_table: str, validate_full: Callable,
                   assert_minimum: Callable[[int, int | None], None], coverage: Any = None) -> Outcome:
    """The incremental plan when it applies, else the full build; checked, not yet written.

    The full build is the one the producer always ran: every input to `lane.build`, its quality
    gates, the expected count and the row floor. The incremental plan runs the same gates on the
    rows it recomputed and the floor on the row count the merge will leave. `coverage` (a
    section_coverage.Guard, root ADR-0272) checks the sections the Gold will hold against the served
    snapshot: the full build's, or the served counts with the affected PNUs' rows replaced.
    """
    from pyspark.storagelevel import StorageLevel

    reason, changed, appended = None, [], []
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
            started = time.monotonic()
            appended = appended_inputs(spark, lane.append_inputs, previous, pins, changed)
            affected = lane.affected(changed, appended) if changed else current.select(KEY).limit(0)
            plan = plan_merge(spark, build=lane.build, restrict=lane.restrict, new=frames, affected=affected,
                              current_gold=current, gold_rows=gold_rows, content_columns=lane.content_columns,
                              max_fraction=args.max_changed_key_fraction, sample_keys=args.parity_sample_keys,
                              seed=pins_seed(pins), started=started, row_key=lane.row_key)
            try:
                _, metrics = lane.validate(plan["recomputed"])
                if args.expected_count is not None and plan["row_count"] != args.expected_count:
                    raise ValueError(f"Expected {args.expected_count} Gold rows, the merge leaves {plan['row_count']}")
                assert_minimum(plan["row_count"], args.minimum_count)
                if coverage is not None:
                    from section_coverage import merged_counts
                    replaced = coverage.measure(current.join(plan["affected"], KEY, "left_semi"))
                    coverage.check(merged_counts(coverage.served, replaced, coverage.measure(plan["recomputed"])))
            except BaseException:
                plan["stages"].remove()
                raise
            return Outcome("incremental", plan["row_count"], metrics, merge=plan, changed_inputs=changed,
                           appended_inputs=appended)
        except IncrementalRefusal as refusal:
            reason = str(refusal)
            print(f"gold-incremental-fallback {reason}", flush=True)
    # On disk only: a full building panel does not fit the executor's memory store beside the
    # work that reads it (see Stages).
    gold = lane.build(frames).persist(StorageLevel.DISK_ONLY)
    count, metrics = validate_full(gold, args.expected_count)
    assert_minimum(count, args.minimum_count)
    if coverage is not None:
        coverage.check(coverage.measure(gold))
    return Outcome("full", count, metrics, gold=gold, full_reason=reason, changed_inputs=changed,
                   appended_inputs=appended, publish_changed_keys=getattr(args, "publish_changed_keys", None),
                   row_key=lane.row_key)


def pins_seed(pins: dict[str, str]) -> str:
    """The parity sample's seed: the same pins pick the same sample, new pins a new one."""
    return ",".join(f"{name}={pins[name]}" for name in sorted(pins))


class Stages:
    """Where `plan_merge` keeps each stage it materializes: Parquet on the run's local disk.

    `localCheckpoint` would cut the lineage too, but it keeps every block in the executor's memory
    store (MEMORY_AND_DISK) and caches a block read back from disk in memory again. On 2026-10-12
    the building panel's merge of a whole new unit price vintage held its keys, restricted inputs
    and rebuilt rows there; the store filled its 9.4 GiB, and a task reading a stage back died of
    heap after an hour. A stage written to Parquet and read back is a plan of one file scan that
    streams from disk and holds nothing in memory. The directory is under `spark.local.dir`, the
    disk Spark already spills to; `remove` deletes it once the merge committed or failed.

    A run killed outright (SIGKILL, the OOM killer, a timeout) never reaches `remove`. Gold runs
    are serialized by the rebuild lock (scripts/ops/gold-panel-rebuild.sh), so a sibling stage
    directory found here can only be a dead run's, and it is deleted before this run writes.
    """

    def __init__(self, spark: Any) -> None:
        local = Path((spark.conf.get("spark.local.dir", None) or "/tmp").split(",")[0])
        for dead in local.glob(STAGES_PREFIX + "*"):
            shutil.rmtree(dead, ignore_errors=True)
        self.spark = spark
        self.root = local / f"{STAGES_PREFIX}{uuid.uuid4().hex}"
        # Stages are read back row-wise: a vectorized batch of building panel rows asked for one
        # contiguous 568 MiB buffer and died of heap on 2026-10-12, the read ADR-0185 already makes
        # row-wise for the Gold table itself. The setting covers Spark's own Parquet reads only;
        # Iceberg tables keep their table property.
        spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

    def materialize(self, name: str, frame: Any) -> Any:
        path = (self.root / name).as_posix()
        frame.write.mode("errorifexists").parquet("file://" + path)
        return self.spark.read.parquet("file://" + path)

    def remove(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)


def plan_merge(spark: Any, *, build: Callable[[dict[str, Any]], Any],
               restrict: Callable[[dict[str, Any], Any], dict[str, Any]], new: dict[str, Any],
               affected: Any, current_gold: Any, gold_rows: int, content_columns: tuple[str, ...],
               max_fraction: float, sample_keys: int, seed: str,
               started: float | None = None, row_key: tuple[str, ...] = (KEY,)) -> dict[str, Any]:
    """Recompute the affected PNUs and a parity sample; return the merge source and its counts.

    Raises IncrementalRefusal when the change set is too large or the parity sample disagrees.
    `current_gold` is the Gold table at its current snapshot; `affected` a frame of `pnu`. Rows
    are merged by `row_key`; `gold_rows` counts rows, the change bound and the sample count PNUs.

    Each stage is materialized (`Stages`) before the next reads it: the build reads its inputs
    several times over, and an input restricted by key frames that are themselves joins of two
    snapshots would make one plan of every copy — on a 41-row fixture that plan alone took a 4g
    driver heap. The stages also fix the merge source before the MERGE reads the target.
    """
    from pyspark.sql import functions as F

    stages = Stages(spark)
    try:
        return plan_merge_in(stages, build=build, restrict=restrict, new=new, affected=affected,
                             current_gold=current_gold, gold_rows=gold_rows,
                             content_columns=content_columns, max_fraction=max_fraction,
                             sample_keys=sample_keys, seed=seed, F=F,
                             started=time.monotonic() if started is None else started, row_key=row_key)
    except BaseException:
        stages.remove()
        raise


def plan_merge_in(stages: Stages, *, build: Callable[[dict[str, Any]], Any],
                  restrict: Callable[[dict[str, Any], Any], dict[str, Any]], new: dict[str, Any],
                  affected: Any, current_gold: Any, gold_rows: int, content_columns: tuple[str, ...],
                  max_fraction: float, sample_keys: int, seed: str, F: Any, started: float,
                  row_key: tuple[str, ...] = (KEY,)) -> dict[str, Any]:
    if row_key[0] != KEY:
        raise ValueError(f"a Gold row key starts with {KEY}, got {row_key}")
    affected = stages.materialize("affected", affected.select(KEY).distinct())
    affected_count = affected.count()
    started = phase_done("affected", started)
    pnus = current_gold.select(KEY) if row_key == (KEY,) else current_gold.select(KEY).distinct()
    gold_pnus = gold_rows if row_key == (KEY,) else pnus.count()
    verdict = change_fraction_verdict(affected_count, gold_pnus, max_fraction)
    if verdict:
        raise IncrementalRefusal(verdict)
    hashed = F.conv(F.substring(F.sha2(F.concat(F.col(KEY), F.lit("\x1f" + seed)), 256), 1, 8), 16, 10)
    sample = stages.materialize("sample", pnus.join(affected, KEY, "left_anti")
                                .where(hashed.cast("long") < sample_threshold(sample_keys, gold_pnus)))
    sample_count = sample.count()
    keys = stages.materialize("keys", affected.unionByName(sample).distinct())
    restricted = {name: stages.materialize(f"input-{index}", frame)
                  for index, (name, frame) in enumerate(restrict(new, keys).items())}
    started = phase_done("restrict", started)
    built = stages.materialize("built", build(restricted).join(keys, KEY, "left_semi"))
    started = phase_done("build", started)

    compared = list(dict.fromkeys([KEY, *content_columns, "row_digest"]))
    rebuilt_sample = built.join(sample, KEY, "left_semi").select(*compared)
    gold_sample = current_gold.join(sample, KEY, "left_semi").select(*compared)
    disagreements = rebuilt_sample.exceptAll(gold_sample).unionByName(gold_sample.exceptAll(rebuilt_sample))
    disagreeing = disagreements.select(KEY).distinct()
    disagreeing_count = disagreeing.count()
    phase_done("parity", started)
    if disagreeing_count:
        examples = sorted(r[KEY] for r in disagreeing.limit(3).collect())
        raise IncrementalRefusal(
            f"parity sample: {disagreeing_count} of {sample_count} PNUs whose inputs did not change "
            f"rebuild differently from the current Gold (e.g. {examples}); the current Gold is not "
            "what the build makes of its pinned inputs")

    recomputed = built.join(affected, KEY, "left_semi")
    source, counts = merge_source(recomputed, current_gold, row_key, affected)
    source = stages.materialize("source", source)
    inserted, deleted = counts["inserted_pnus"], counts["deleted_pnus"]
    return {
        "source": source,
        "recomputed": recomputed,
        "affected": affected,
        "counts": {
            "affected_pnus": affected_count,
            "parity_sample_pnus": sample_count,
            **counts,
            "unchanged_recomputed_pnus": affected_count - keys_changed(counts),
        },
        "row_count": gold_rows + inserted - deleted,
        "row_key": row_key,
        "persisted": [],
        "stages": stages,
    }


def merge_source(rebuilt: Any, current_gold: Any, row_key: tuple[str, ...],
                 affected: Any | None = None) -> tuple[Any, dict[str, int]]:
    """The rows a MERGE must write to turn `current_gold` into `rebuilt`, and their counts.

    Upserts: rebuilt rows whose `row_digest` differs from the current row of their key, or that have
    none. Deletes (DELETE_FLAG): current rows of the `affected` PNUs (every PNU when None) that the
    rebuild no longer makes, e.g. a building whose units all moved away (root ADR-0204).
    """
    from pyspark.sql import functions as F

    key = list(row_key)
    current = current_gold.select(*key, F.col("row_digest").alias("_current_digest"))
    upserts = (rebuilt.join(current, key, "left")
               .where(~F.col("row_digest").eqNullSafe(F.col("_current_digest"))))
    inserted = upserts.where(F.col("_current_digest").isNull()).count()
    updated = upserts.where(F.col("_current_digest").isNotNull()).count()
    gone = current_gold.select(*key) if affected is None else current_gold.select(*key).join(affected, KEY, "left_semi")
    deletes = gone.join(rebuilt.select(*key), key, "left_anti")
    deleted = deletes.count()
    gold_columns = list(current_gold.columns)
    source = upserts.select(*gold_columns).withColumn(DELETE_FLAG, F.lit(False)).unionByName(
        deletes.select(*[F.col(c) if c in row_key else F.lit(None).cast(current_gold.schema[c].dataType).alias(c)
                         for c in gold_columns]).withColumn(DELETE_FLAG, F.lit(True)))
    return source, {"inserted_pnus": inserted, "updated_pnus": updated, "deleted_pnus": deleted}


def current_fingerprint(spark: Any, table: str) -> str | None:
    """The current Gold's content fingerprint (root ADR-0274 §1), only as the producer recorded it on
    the commit the current rows are (compactions may follow it); None otherwise.

    Never computed from the current rows: a row_digest only fingerprints the rows the producer wrote,
    and a Gold changed by anything else (a backfill, a hand write) may keep digests that no longer
    say what its rows hold. Such a Gold is rewritten, which records a fingerprint again.
    """
    import gold_fingerprint
    from lakehouse_engine import current_snapshot

    if not spark.catalog.tableExists(table.replace("`", "")):
        return None
    try:
        head = current_snapshot(spark, table)
    except ValueError:
        return None
    return gold_fingerprint.recorded_for_rows(spark, table, head)


def row_level_mode(contract: dict[str, Any]) -> str:
    """The Gold rebuild contract's `merge.mode`: how an incremental merge writes (root ADR-0344)."""
    mode = (contract.get("merge") or {}).get("mode")
    if mode not in ROW_LEVEL_MODES:
        raise ValueError(f"the gold rebuild contract's merge.mode is {mode!r}; expected one of {list(ROW_LEVEL_MODES)}")
    return mode


def apply_row_level_mode(spark: Any, table: str, mode: str) -> bool:
    """Set `table`'s row-level write mode to `mode` where it differs, in one metadata commit (no data
    file is written). Returns whether it changed the table."""
    from platform_contracts import table_properties

    live = table_properties(spark, table)
    drifted = [name for name in ROW_LEVEL_MODE_PROPERTIES if live.get(name) != mode]
    if drifted:
        pairs = ", ".join(f"'{name}' = '{mode}'" for name in drifted)
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ({pairs})")
    return bool(drifted)


def contract_row_level_mode() -> str:
    from gold_rebuild import CONTRACT_PATH

    return row_level_mode(json.loads(CONTRACT_PATH.read_text(encoding="utf-8")))


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


def merge_sql(table: str, columns: list[str], row_key: tuple[str, ...] = (KEY,)) -> str:
    update = ", ".join(f"t.`{c}` = s.`{c}`" for c in columns)
    names = ", ".join(f"`{c}`" for c in columns)
    values = ", ".join(f"s.`{c}`" for c in columns)
    match = " AND ".join(f"t.`{c}` = s.`{c}`" for c in row_key)
    return (
        f"MERGE INTO {table} t USING {MERGE_SOURCE_VIEW} s ON {match} "
        f"WHEN MATCHED AND s.`{DELETE_FLAG}` THEN DELETE "
        f"WHEN MATCHED AND NOT s.`{DELETE_FLAG}` AND NOT (t.`row_digest` <=> s.`row_digest`) "
        f"THEN UPDATE SET {update} "
        f"WHEN NOT MATCHED AND NOT s.`{DELETE_FLAG}` THEN INSERT ({names}) VALUES ({values})"
    )


def merge_into_gold(spark: Any, table: str, source: Any, properties: dict[str, str],
                    row_key: tuple[str, ...] = (KEY,), unchanged_properties: dict[str, str] | None = None) -> bool:
    """Merge `source` into `table` by `row_key` in one snapshot carrying `properties`. False when
    it was empty.

    An empty change set still commits, as an empty append with the same properties (and
    `unchanged_properties`, what stays true of unchanged content), so the Gold records that it
    reflects the new Silver pins and the next plan starts from them.
    """
    apply_row_level_mode(spark, table, contract_row_level_mode())
    if source.limit(1).count() == 0:
        empty = spark.table(table).limit(0)
        writer = empty.writeTo(table)
        for key, value in {**properties, **(unchanged_properties or {})}.items():
            writer = writer.option(f"{SNAPSHOT_PROPERTY_PREFIX}{key}", value)
        writer.append()
        return False
    columns = [c for c in source.columns if c != DELETE_FLAG]
    source.createOrReplaceTempView(MERGE_SOURCE_VIEW)
    try:
        with_commit_properties(spark, properties, lambda: spark.sql(merge_sql(table, columns, row_key)))
    finally:
        spark.catalog.dropTempView(MERGE_SOURCE_VIEW)
    return True


def retain_input_snapshots(spark: Any, catalog: str, namespace: str, gold_table: str,
                           pins: dict[str, str]) -> list[str]:
    """Tag each pinned Silver snapshot so expiry keeps the next incremental run's comparison point.

    One tag per (input, Gold table), moved on every commit, so it holds at most one old snapshot.
    `namespace` is where the Silver inputs live; an input of another namespace (a `reference.`
    table) is tagged in its own.
    """
    tag = input_tag(gold_table)
    tagged = []
    for name, snapshot in sorted(pins.items()):
        own, table_name = name.split(".", 1)
        table = f"`{catalog}`.`{namespace if own == 'silver' else own}`.`{table_name}`"
        spark.sql(f"ALTER TABLE {table} CREATE OR REPLACE TAG `{tag}` AS OF VERSION {int(snapshot)}")
        tagged.append(name)
    return tagged


def changes_table(table: str) -> str:
    """`table`'s change record (root ADR-0191 naming), quoted like `table` when it is."""
    return table[:-1] + CHANGES_SUFFIX + "`" if table.endswith("`") else table + CHANGES_SUFFIX


def record_changes(spark: Any, table: str, changed: Any | None) -> None:
    """Append the PNUs of `changed` (None: none) under the step `table`'s head commit just made.

    Written after the Gold commit: a record that fails leaves the step unrecorded, which only sends
    the next delta back to the two-snapshot join, so the failure is printed and not raised. Gold
    runs are serialized by the rebuild lock, so the head is this run's commit.
    """
    from pyspark.sql import functions as F
    from lakehouse_engine import current_snapshot

    try:
        head = int(current_snapshot(spark, table))
        parent = spark.sql(f"SELECT parent_id FROM {table}.snapshots WHERE snapshot_id = {head}").first()[0]
        target = changes_table(table)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({KEY} string, {FROM_SNAPSHOT} bigint, "
                  f"{TO_SNAPSHOT} bigint) USING iceberg PARTITIONED BY ({TO_SNAPSHOT})")
        marker = spark.createDataFrame([(None,)], f"{KEY} string")
        keys = marker if changed is None else changed.select(F.col(KEY).cast("string").alias(KEY)).distinct().unionByName(marker)
        (keys.withColumn(FROM_SNAPSHOT, F.lit(parent).cast("bigint")).withColumn(TO_SNAPSHOT, F.lit(head).cast("bigint"))
         .coalesce(1).writeTo(target).append())
        print(f"gold-changes-recorded table={table} step={parent}->{head}", flush=True)
    except Exception as error:  # noqa: BLE001 - see the docstring: an unrecorded step is a slower delta, not a loss
        print(f"gold-changes-unrecorded table={table} reason={error}", flush=True)


def recorded_steps(snapshots: list[dict[str, Any]], recorded_heads: set[str], old: str,
                   new: str) -> list[str] | None:
    """The recorded heads that take `main` from `old` to `new`, or None when a step has no record.

    Each record is the step from its head's parent (record_changes), so the walk is ADR-0191's
    `chain` over those steps; a compaction between them is content-preserving.
    """
    parents = {str(s["snapshot_id"]): s.get("parent_id") for s in snapshots}
    steps = [(parents[head], head) for head in recorded_heads if parents.get(head) is not None]
    walked = chain(snapshots, steps, old, new)
    return None if walked is None else [to for _, to in walked]


def recorded_changes(spark: Any, table: str, old: str, new: str) -> Any | None:
    """The PNUs whose rows `table` changed from snapshot `old` to `new`, or None when any step
    between them is unrecorded (then only comparing the two snapshots can say)."""
    from pyspark.sql import functions as F

    target = changes_table(table)
    if not spark.catalog.tableExists(target.replace("`", "")):
        return None
    snapshots = [{"snapshot_id": str(r.snapshot_id), "parent_id": None if r.parent_id is None else str(r.parent_id),
                  "operation": r.operation}
                 for r in spark.sql(f"SELECT snapshot_id, parent_id, operation FROM {table}.snapshots").collect()]
    heads = {str(r[0]) for r in spark.sql(f"SELECT partition.{TO_SNAPSHOT} FROM {target}.partitions").collect()}
    walked = recorded_steps(snapshots, heads, str(old), str(new))
    if walked is None:
        return None
    return (spark.table(target).where(F.col(TO_SNAPSHOT).isin([int(h) for h in walked]) & F.col(KEY).isNotNull())
            .select(KEY).distinct())
