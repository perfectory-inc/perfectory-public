"""What a release changes against a table's current rows: the compare the Spark jobs that still load
by release share (`rt_molit_real_transaction`, `vintage_versions`).

It was the Silver refresh lanes' compare job (root ADR-0275, ADR-0318). Those lanes now compare in
`DuckDB` inside the publisher (root ADR-0599: `release_compare_stage`), so the job, its buckets,
its branches and its geometry tolerance are gone; what is left is the library:

- `release_changes` / `compared_changes`: every staged row of a key that is new or whose rows differ
  from the key's current rows (a key's release is a set of rows: the source repeats a key, root
  ADR-0090), and for each key with current rows and no staged row a removal marker;
- `commit_stamp`, `edition_column`, `refuse_older_edition`: a key's versions are ordered by the
  release's edition, then the commit's instant (ADR-0328);
- `changed_only`: the staged rows whose key is new or whose content differs (root ADR-0311).
"""

from __future__ import annotations

from functools import reduce
from operator import and_
from typing import Any, Sequence

from platform_contracts import REMOVED, CurrentRowLatest


def edition_column(latest: CurrentRowLatest) -> str | None:
    """The column a release's edition is in, or None for a table ordered by its load instant alone.

    A compared table orders a key's versions by `[edition, ingested_at_utc]` or `[ingested_at_utc]`
    (ADR-0328), and the compare stamps both on every row it appends. Without the instant, a release
    loaded again ties with its earlier load: on 2026-10-21 `silver.land_transfer_history` appended
    vintage 20260930 a second time, its 1,585 removal markers tied with the rows they removed, and
    those keys stayed current (with 19 stale rows beside their new ones).
    """

    *editions, last = latest.newest_first
    if last != "ingested_at_utc" or len(editions) > 1 or "ingested_at_utc" in editions:
        raise ValueError(f"a compared table orders its versions by [edition, ingested_at_utc] or "
                         f"[ingested_at_utc], not {list(latest.newest_first)}")
    return editions[0] if editions else None


def release_changes(staged: Any, current: Any, *, key: tuple[str, ...], content: list[str], removal: str,
                    source_snapshot_id: str, ingested_at: Any, columns: list[str],
                    stamp: dict[str, Any] | None = None) -> Any:
    """The rows to append.

    The rows are every staged row of a key that is new or whose rows changed, and one removal
    marker per key with current rows and none staged.

    A key's release is a set of rows (the source repeats a key, root ADR-0090), so a key is compared
    by the multiset of its rows' content fingerprints, repeats counted. Keys match null-safe: a
    source row may lack a key column (a price line without a management key) and is still one key.
    A marker carries the release's `source_snapshot_id` and this run's `ingested_at_utc`. `stamp` then
    sets columns on every appended row, changed and marker alike, each cast to its column's type
    (`commit_stamp`): the release's edition and the commit's instant, so a marker outranks the row
    it removes (ADR-0328).
    """

    from pyspark.sql import Window  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415

    # Null-ness hashed apart from the value, so a null and an empty string differ.
    fingerprint = F.xxhash64(*(F.col(c).isNull() for c in content), *(F.col(c) for c in content))
    renamed = [f"_key{index}" for index in range(len(key))]

    def signatures(frame: Any, name: str) -> Any:
        return frame.groupBy(*key).agg(F.xxhash64(F.sort_array(F.collect_list(fingerprint))).alias(name))

    def keys_of(frame: Any) -> Any:
        # Renamed, so a join of two frames of one lineage never compares a column with itself.
        return frame.select(*(F.col(k).alias(r) for k, r in zip(key, renamed)))

    def matching(frame: Any, keys: Any) -> Any:
        return reduce(and_, (frame[k].eqNullSafe(keys[r]) for k, r in zip(key, renamed)))

    staged_signatures = signatures(staged, "_staged")
    old = signatures(current, "_current")
    old = old.select(*(F.col(k).alias(r) for k, r in zip(key, renamed)), *(c for c in old.columns if c not in key))
    differing = (staged_signatures.join(old, matching(staged_signatures, old), "left")
                 .where(F.col("_current").isNull() | (F.col("_current") != F.col("_staged"))))
    changed_keys = differing.select(*(staged_signatures[k].alias(r) for k, r in zip(key, renamed)))
    changed = staged.join(changed_keys, matching(staged, changed_keys), "left_semi")
    # One marker per vanished key, made from one of its current rows (the required columns need values).
    first = Window.partitionBy(*key).orderBy(fingerprint)
    staged_keys = keys_of(staged).distinct()
    removed = (current.join(staged_keys, matching(current, staged_keys), "left_anti")
               .withColumn("_n", F.row_number().over(first)).where(F.col("_n") == 1)
               .withColumn(removal, F.lit(REMOVED))
               .withColumn("source_snapshot_id", F.lit(source_snapshot_id))
               .withColumn("ingested_at_utc", ingested_at))
    for column, value in (stamp or {}).items():
        typed = value.cast(current.schema[column].dataType)
        changed, removed = changed.withColumn(column, typed), removed.withColumn(column, typed)
    return changed.select(*columns).unionByName(removed.select(*columns))


def commit_stamp(latest: CurrentRowLatest, edition: str | None, ingested_at: Any) -> tuple[dict[str, Any], str | None]:
    """The values that order this commit's rows after every version they replace, and the edition column.

    `current_rows` ranks a key's versions by `newest_first`: the edition (a vintage, a provider
    month, a release's source id), then the commit's instant (`edition_column`, ADR-0328). Every
    appended row gets the release's edition and one instant for the commit, so a key's staged rows
    that loaded in different batches stay one version, an edition loaded again (a full load after a
    partial one, root ADR-0185; a re-collected file) wins, and a removal marker outranks the row it
    removes.
    """

    from pyspark.sql import functions as F  # noqa: PLC0415

    column = edition_column(latest)
    stamp: dict[str, Any] = {"ingested_at_utc": ingested_at}
    if column is not None:
        if not edition:
            raise ValueError(f"a release of a table ordered by {column} names its edition")
        stamp[column] = F.lit(edition)
    return stamp, column


def refuse_older_edition(edition: str, held: set[str]) -> None:
    """An edition older than one already held would change the state the later editions were
    compared against, so their versions would no longer reconstruct them (root ADR-0317)."""

    newer = sorted(held_edition for held_edition in held if held_edition > edition)
    if newer:
        raise ValueError(f"edition {edition} is older than the held editions {newer}; versions are appended in edition order")


def changed_only(staged: Any, current: Any, key: Sequence[str], content: Sequence[str]) -> Any:
    """The staged rows (one per key) whose key is new or whose content differs from its current row
    (root ADR-0311: append only the changed keys). Null-ness is hashed apart from the value, so a
    null and an empty string differ, as `compared_changes` does. It was `data_go_kr_onbid.changed_only`;
    the 경매 Silver load reads it here since the 공매 load left Spark (root ADR-0559)."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    fingerprint = F.xxhash64(*(F.col(c).isNull() for c in content), *(F.col(c) for c in content))
    seen = current.select(*key, fingerprint.alias("_fingerprint"))
    return staged.withColumn("_fingerprint", fingerprint).join(seen, [*key, "_fingerprint"], "left_anti") \
        .drop("_fingerprint")
