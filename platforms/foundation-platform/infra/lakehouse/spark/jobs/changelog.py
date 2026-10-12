"""A Silver release as a changelog, computed once at load (root ADR-0191).

Sources arrive as whole releases. The load compares the staged release with the table's `main`
head once, by business key and a fingerprint of the content columns, and appends the difference to
`silver.<t>__changes`. Every consumer then reads the changes instead of diffing two snapshots.

- **Z-set.** A change row is one distinct (key, fingerprint) with an integer `_weight`: positive
  rows entered, negative rows left, an update is a −1 and a +1. Changes over several releases
  compose by summing weights per (key, fingerprint) and dropping zero (`net`), so a skipped run
  catches up with no logic of its own (Feldera/DBSP Z-sets, Iceberg `net_changes`).
- **Fingerprint.** `row_fingerprint` is xxhash64 over every content column and its null flag;
  content is the schema minus the contract's lineage columns, so a reload that only restamps
  lineage changes nothing. It is compared only within a key, so a changed row keeps its
  fingerprint with probability 2^-64.
- **Position.** A change row carries the `main` snapshots it moved between. A consumer walks
  `main` from the snapshot it last pinned to the new one (`chain`) and reads those steps; a step
  with no changelog sends it back to the snapshot diff (root ADR-0189, ADR-0180).
- **Closure.** Before publishing, the written changes must close the two releases' count and
  fingerprint sum (`closure_verdict`): a lost or doubled change row is refused.

PySpark is imported inside the functions: the CI runner has no PySpark and tests the pure parts.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

CHANGES_SUFFIX = "__changes"
FINGERPRINT = "row_fingerprint"
WEIGHT = "_weight"
FROM_SNAPSHOT = "_from_snapshot_id"
TO_SNAPSHOT = "_to_snapshot_id"
CHANGE_COLUMNS = (WEIGHT, FROM_SNAPSHOT, TO_SNAPSHOT)
# Compaction rewrites files without changing rows: a step with nothing to read.
CONTENT_PRESERVING_OPERATIONS = frozenset({"replace"})


def changes_table(table: str) -> str:
    return table + CHANGES_SUFFIX


def content_columns(columns: Iterable[str], lineage: Iterable[str]) -> list[str]:
    """The columns a fingerprint covers: the schema minus lineage and the changelog's own columns.

    Derived, not listed, so a column added to the table is content until the contract calls it lineage.
    """
    excluded = {*lineage, FINGERPRINT, *CHANGE_COLUMNS}
    content = [c for c in columns if c not in excluded]
    if not content:
        raise ValueError("a fingerprint needs at least one content column")
    return content


def with_fingerprint(frame: Any, lineage: Iterable[str]) -> Any:
    """`frame` with `row_fingerprint` over its content columns (replacing any present)."""
    from pyspark.sql import functions as F

    # Sorted: the same content in another column order is the same row.
    content = sorted(content_columns(frame.columns, lineage))
    # Spark's hashes skip NULLs; the null flags keep (NULL, x) and (x, NULL) apart.
    hashed = [*(F.col(c).isNull() for c in content), *(F.col(c) for c in content)]
    return frame.drop(FINGERPRINT).withColumn(FINGERPRINT, F.xxhash64(*hashed))


def change_set(old: Any, new: Any, keys: tuple[str, ...]) -> Any:
    """The Z-set taking `old` to `new`: one image per changed (key, fingerprint) and its `_weight`.

    Both sides carry `row_fingerprint`. Counting reads only the keys and the fingerprint; full
    images are fetched only for the changed pairs (new rows from `new`, rows that left from `old`).
    """
    from pyspark.sql import functions as F

    identity = [*keys, FINGERPRINT]
    for side, frame in (("old", old), ("new", new)):
        missing = sorted(set(identity) - set(frame.columns))
        if missing:
            raise ValueError(f"the {side} release lacks {missing}")
    counted_old = old.groupBy(*identity).agg(F.count(F.lit(1)).alias("_old")).alias("o")
    counted_new = new.groupBy(*identity).agg(F.count(F.lit(1)).alias("_new")).alias("n")
    weights = (
        counted_old.join(counted_new, _same("o", "n", identity), "full_outer")
        .select(*(F.coalesce(_col("o", c), _col("n", c)).alias(c) for c in identity),
                (F.coalesce(F.col("n._new"), F.lit(0)) - F.coalesce(F.col("o._old"), F.lit(0)))
                .alias(WEIGHT))
        .where(F.col(WEIGHT) != 0)
    )
    columns = list(new.columns)
    entered = _images(new, weights.where(F.col(WEIGHT) > 0), identity, columns)
    left = _images(old.select(*columns), weights.where(F.col(WEIGHT) < 0), identity, columns)
    return entered.unionByName(left)


def net(changes: Any, keys: tuple[str, ...], steps: list[tuple[Any, Any]]) -> Any:
    """The `steps` (from `chain`, oldest first) of `changes` as one: weights summed per
    (key, fingerprint), zeros dropped.

    Equal fingerprints are equal content but not equal lineage, so each pair carries the row of
    its latest step whole: columns picked one by one could mix lineage from different loads.
    Snapshot ids are random, so "latest" is the position in `steps`, not the larger id.
    """
    from itertools import chain as flatten

    from pyspark.sql import functions as F

    identity = [*keys, FINGERPRINT]
    images = [c for c in changes.columns if c not in identity and c not in CHANGE_COLUMNS]
    position = F.create_map(*flatten.from_iterable(
        (F.lit(int(to_id)), F.lit(index)) for index, (_, to_id) in enumerate(steps)))
    latest = F.max_by(F.struct(*images), position[F.col(TO_SNAPSHOT)]).alias("_latest")
    return (
        changes.groupBy(*identity)
        .agg(F.sum(WEIGHT).alias(WEIGHT), latest)
        .where(F.col(WEIGHT) != 0)
        .select(*identity, WEIGHT, *(F.col(f"_latest.`{c}`").alias(c) for c in images))
    )


def stamped(changes: Any, from_snapshot: int, to_snapshot: int) -> Any:
    """Changes labelled with the `main` step they record."""
    from pyspark.sql import functions as F

    return (changes.withColumn(FROM_SNAPSHOT, F.lit(from_snapshot).cast("long"))
            .withColumn(TO_SNAPSHOT, F.lit(to_snapshot).cast("long")))


@dataclass(frozen=True)
class Totals:
    """A release's (or a change set's weighted) row count and fingerprint sum, exact."""

    rows: int
    fingerprint_sum: int


def totals(frame: Any, weighted: bool = False) -> Totals:
    from pyspark.sql import functions as F

    weight = F.col(WEIGHT) if weighted else F.lit(1)
    # decimal(38,0) cannot overflow: 2^63 per row times 10^9 rows is under 10^38.
    row = frame.agg(
        F.coalesce(F.sum(weight.cast("decimal(38,0)")), F.lit(0)).alias("rows"),
        F.coalesce(F.sum(weight.cast("decimal(38,0)") * F.col(FINGERPRINT).cast("decimal(38,0)")),
                   F.lit(0)).alias("fingerprint_sum"),
    ).collect()[0]
    return Totals(int(row["rows"]), int(row["fingerprint_sum"]))


def closure_verdict(old: Totals, new: Totals, changes: Totals) -> str | None:
    """Why `changes` does not take `old` to `new`, or None."""
    if old.rows + changes.rows != new.rows:
        return (f"the changes do not close the row count: {old.rows} + {changes.rows} != {new.rows}")
    if old.fingerprint_sum + changes.fingerprint_sum != new.fingerprint_sum:
        return "the changes do not close the fingerprint sum"
    return None


def chain(snapshots: Iterable[dict[str, Any]], steps: Iterable[tuple[Any, Any]],
          old: Any, new: Any) -> list[tuple[str, str]] | None:
    """The recorded changelog steps that take `main` from `old` to `new`, oldest first, or None.

    `snapshots` are the table's snapshots (`snapshot_id`, `parent_id`, `operation`); `steps` the
    (from, to) pairs the changelog holds. Walking back from `new`: a recorded step jumps to its
    `from`, a content-preserving snapshot (compaction) steps to its parent. Anything else — a
    load with no changelog, an expired or foreign lineage — returns None and the consumer falls
    back to the snapshot diff. Steps whose `to` is not on the walk (a load whose publish failed)
    are never read.
    """
    old, new = str(old), str(new)
    parents = {str(s["snapshot_id"]): s for s in snapshots}
    recorded: dict[str, str] = {}
    for from_id, to_id in steps:
        from_id, to_id = str(from_id), str(to_id)
        if recorded.setdefault(to_id, from_id) != from_id:
            raise ValueError(f"snapshot {to_id} has two changelog steps")
    walked: list[tuple[str, str]] = []
    current, seen = new, set()
    while current != old:
        if current in seen:
            return None
        seen.add(current)
        if current in recorded:
            walked.append((recorded[current], current))
            current = recorded[current]
            continue
        snapshot = parents.get(current)
        if snapshot is None or snapshot.get("operation") not in CONTENT_PRESERVING_OPERATIONS \
                or snapshot.get("parent_id") is None:
            return None
        current = str(snapshot["parent_id"])
    walked.reverse()
    return walked


def _col(alias: str, column: str) -> Any:
    from pyspark.sql import functions as F

    return F.col(f"{alias}.`{column}`")


def _same(left: str, right: str, columns: list[str]) -> Any:
    """Null-safe equality of two aliased frames on `columns`: Silver holds NULL PNUs, and a NULL
    key is a key."""
    from functools import reduce

    return reduce(lambda acc, c: acc & _col(left, c).eqNullSafe(_col(right, c)),
                  columns[1:], _col(left, columns[0]).eqNullSafe(_col(right, columns[0])))


def _images(rows: Any, weights: Any, identity: list[str], columns: list[str]) -> Any:
    """One row of `rows` per pair in `weights`, carrying the pair's weight."""
    image = (rows.alias("r").join(weights.alias("w"), _same("r", "w", identity), "left_semi")
             .dropDuplicates(identity))
    return (image.alias("i").join(weights.alias("w"), _same("i", "w", identity), "inner")
            .select(*(_col("i", c) for c in columns), _col("w", WEIGHT)))
