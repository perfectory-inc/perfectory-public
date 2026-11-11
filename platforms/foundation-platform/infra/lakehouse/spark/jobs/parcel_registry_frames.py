"""`silver.parcel_registry` read as of an edition, and its next step, on the executors (root ADR-0490 §4a).

The registry is append-only and every row names the edition it registered (`to_snapshot_id`). A new
edition's step is appended as soon as its Silver is in, before it is served, so a reader folds the
registry **as of an edition**: the rows of that edition and the ones before it, in the source contract's
edition order (`registry_through`). The served edition's readers (the place-id release, ADR-0288) fold
as of the served edition and do not move until a promotion; the candidate's matching gate folds as of
the candidate. This is the canonical reader the table's contract names (`row_versions`, root ADR-0286):
a job that reads the registry without this module folds every edition at once.

`advance_rows` and `upgrade_rows` are `parcel_identity.advance` and `parcel_identity.upgrade` as joins:
the PNUs both editions hold write nothing and never reach the driver (2026-11-11: 6,517,338 of 202609's
39,876,546 parcels differ from 202606, 6,447,907 of them only by the 전남광주 code change), and a
national lineage pair is millions of rows, more than the driver holds (the Spark container's 20g). The
rules are the Python ones' constants (`parcel_identity.CARRY_MIN_GRADE`, `parcel_lineage.GRADE_RANK`
and `IDENTITY_RELATIONS`); `tests/test_parcel_registry_frames.py` checks the joins against
`parcel_identity` on the same fixtures.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

import parcel_identity as pi
import parcel_lineage as pl
import vworld_parcel_editions as editions
from lineage_review_queue import STEWARD, steward_resolved

REGISTRY_TABLE = "silver.parcel_registry"
REGISTRY_ROW = ("parcel_id", "pnu", "status", "valid_from", "valid_to", "redirect_to")
LINEAGE_COLUMNS = ("predecessor_pnu", "successor_pnu", "relation", "grade", "evidence_kind", "evidence_ref",
                   "derived_at_utc", "from_snapshot_id", "to_snapshot_id")


def through_predicate(snapshot: str, contract: Mapping[str, Any] | None = None) -> str:
    """SQL: the registry rows of `snapshot`'s edition and every edition before it."""

    ids = editions.snapshot_ids_through(contract or editions.load(), snapshot)
    return "to_snapshot_id IN (" + ", ".join(f"'{i}'" for i in ids) + ")"


def registry_through(frame: Any, snapshot: str, contract: Mapping[str, Any] | None = None) -> Any:
    """`frame` (the registry table) as of `snapshot`'s edition."""

    return frame.where(through_predicate(snapshot, contract))


def _key(F: Any) -> Any:
    # `parcel_identity.fold`'s order: the row's date, a current row after the others on one date.
    return F.expr("coalesce(nullif(CAST(valid_from AS STRING), ''), CAST(valid_to AS STRING), '')")


def current_ids(registry: Any) -> Any:
    """`parcel_identity.fold(...).current` as a join: (pnu, parcel_id) of every current id.

    The fold replays rows by (valid_from or valid_to, current last) and keeps, per PNU, the id its
    last `current` row opened unless a later `historic` or `redirected` row closes that same id.
    Two `current` rows of one PNU on one date are ordered by parcel id here, where the fold keeps
    input order; the registry writes one current row per PNU and date.
    """

    from pyspark.sql import functions as F  # noqa: PLC0415

    rows = registry.select("pnu", "parcel_id", "status", _key(F).alias("k"))
    last = (
        rows.where(F.col("status") == pi.CURRENT)
        .groupBy("pnu")
        .agg(F.max(F.struct("k", "parcel_id")).alias("last"))
        .select("pnu", F.col("last.k").alias("k"), F.col("last.parcel_id").alias("parcel_id"))
    )
    closing = rows.where(F.col("status").isin(pi.HISTORIC, pi.REDIRECTED)).select(
        F.col("pnu").alias("c_pnu"), F.col("parcel_id").alias("c_id"), F.col("k").alias("c_k")
    )
    closed = last.join(
        closing,
        (last.pnu == closing.c_pnu) & (last.parcel_id == closing.c_id) & (closing.c_k > last.k),
        "left_semi",
    )
    return last.join(closed.select("pnu"), "pnu", "left_anti").select("pnu", "parcel_id")


def closed_ids(registry: Any) -> Any:
    """`parcel_identity.fold(...).last_id_of_closed` as a join: (pnu, parcel_id) of each PNU whose last
    opening or closing row, in the fold's order, is `historic` (a `redirected` row leaves it as it is)."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    rows = registry.where(F.col("status").isin(pi.CURRENT, pi.HISTORIC)).select(
        "pnu", "parcel_id", "status", _key(F).alias("k"),
        F.when(F.col("status") == pi.CURRENT, 1).otherwise(0).alias("o"))
    last = rows.groupBy("pnu").agg(F.max(F.struct("k", "o", "parcel_id", "status")).alias("last"))
    return last.where(F.col("last.status") == pi.HISTORIC).select("pnu", F.col("last.parcel_id").alias("parcel_id"))


def resolved_lineage(lineage: Any) -> Any:
    """The lineage rows as every consumer reads them (`lineage_review_queue.steward_resolved`), without
    collecting the table: the newest derivation run per (pair, successor) and the outranked code
    changes are dropped on the executors; only the successors a steward row names are resolved on the
    driver, by the one function (as `parcel_lineage_section.build_lineage` does)."""

    from pyspark.sql import Window, functions as F  # noqa: PLC0415

    from parcel_lineage_section import drop_outranked_code_changes_frame  # noqa: PLC0415

    group = Window.partitionBy("from_snapshot_id", "to_snapshot_id", "successor_pnu")
    derived = F.coalesce(F.col("evidence_kind"), F.lit("")) != STEWARD
    newest = (lineage.withColumn("_newest", F.max(F.when(derived, F.col("derived_at_utc"))).over(group))
              .where(~derived | F.col("derived_at_utc").isNull() | (F.col("derived_at_utc") == F.col("_newest")))
              .drop("_newest"))
    rows = drop_outranked_code_changes_frame(newest)
    stewarded = rows.where(F.col("evidence_kind") == STEWARD).select("successor_pnu").distinct()
    decided = [row.asDict() for row in rows.join(stewarded, "successor_pnu", "left_semi").collect()]
    plain = rows.join(stewarded, "successor_pnu", "left_anti").select(*LINEAGE_COLUMNS)
    resolved = rows.sparkSession.createDataFrame(
        [tuple(row.get(c) for c in LINEAGE_COLUMNS) for row in steward_resolved(decided)], plain.schema)
    return plain.unionByName(resolved)


def carrying(resolved: Any) -> tuple[Any, int, list[str]]:
    """(successor_pnu, predecessor_pnu) of every link that carries an id (`parcel_identity.carrying_links`
    over `parcel_lineage.effective_links`), the number of successors with an identity conflict, and a
    sample of them. Per successor: its best-graded link (ties by relation, then predecessor, where the
    Python fold keeps input order) carries when it is an identity relation with a predecessor at
    `CARRY_MIN_GRADE` or stronger; a predecessor two successors claim carries neither."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    rank = F.lit(len(pl.GRADES))
    for grade, value in sorted(pl.GRADE_RANK.items(), key=lambda item: -item[1]):
        rank = F.when(F.col("grade") == grade, F.lit(value)).otherwise(rank)
    ranked = resolved.withColumn("_rank", rank)
    identity = (F.col("relation").isin(sorted(pl.IDENTITY_RELATIONS))
                & (F.coalesce(F.col("predecessor_pnu"), F.lit("")) != ""))
    conflicts = (ranked.where(identity).groupBy("successor_pnu")
                 .agg(F.countDistinct("predecessor_pnu").alias("_n")).where(F.col("_n") > 1).select("successor_pnu"))
    sample = [r["successor_pnu"] for r in conflicts.orderBy("successor_pnu").limit(20).collect()]
    best = (ranked.groupBy("successor_pnu")
            .agg(F.min(F.struct("_rank", "relation", F.coalesce("predecessor_pnu", F.lit("")).alias("p"))).alias("b"))
            .select("successor_pnu", F.col("b._rank").alias("_rank"), F.col("b.relation").alias("relation"),
                    F.col("b.p").alias("predecessor_pnu")))
    carry_rank = pl.GRADE_RANK[pi.CARRY_MIN_GRADE]
    candidates = best.where(F.col("relation").isin(sorted(pl.IDENTITY_RELATIONS)) & (F.col("predecessor_pnu") != "")
                            & (F.col("_rank") <= carry_rank)).select("successor_pnu", "predecessor_pnu")
    once = candidates.groupBy("predecessor_pnu").count().where(F.col("count") == 1).select("predecessor_pnu")
    return candidates.join(once, "predecessor_pnu", "left_semi"), conflicts.count() if sample else 0, sample


# uuid.NAMESPACE_URL's bytes: `parcel_identity.parcel_id_for` is uuid5 under it.
_NAMESPACE_URL_HEX = "6ba7b8119dad11d180b400c04fd430c8"


def parcel_id_expr(pnu: Any, first_seen: str) -> Any:
    """`parcel_identity.parcel_id_for(pnu, first_seen)` as a column expression: uuid5 (RFC 4122 §4.3) is the
    SHA-1 of the namespace and the name, with the version and variant bits set. On the executors without a
    Python UDF: the job's modules are not shipped to them (2026-11-11 CI: `No module named parcel_identity`)."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    name = F.concat(F.lit("parcel:"), pnu, F.lit(f":{first_seen}"))
    h = F.sha1(F.concat(F.unhex(F.lit(_NAMESPACE_URL_HEX)), F.encode(name, "UTF-8")))
    variant = F.lower(F.hex((F.conv(F.substring(h, 17, 1), 16, 10).cast("int").bitwiseAND(3)).bitwiseOR(8)))
    return F.concat(F.substring(h, 1, 8), F.lit("-"), F.substring(h, 9, 4), F.lit("-5"), F.substring(h, 14, 3),
                    F.lit("-"), variant, F.substring(h, 18, 3), F.lit("-"), F.substring(h, 21, 12))


def _rows(frame: Any, status: str, *, opened: Any = None, closed: Any = None, redirect: Any = None) -> Any:
    from pyspark.sql import functions as F  # noqa: PLC0415

    nothing = F.lit(None).cast("string")
    return frame.select("parcel_id", "pnu", F.lit(status).alias("status"),
                        (opened if opened is not None else nothing).alias("valid_from"),
                        (closed if closed is not None else nothing).alias("valid_to"),
                        (redirect if redirect is not None else nothing).alias("redirect_to"))


def advance_rows(current: Any, after: Any, carry: Any, to_date: str) -> tuple[Any, dict[str, int]]:
    """`parcel_identity.advance(current, after, effective, to_date)` (without earlier issues) as joins:
    the rows to append and the changelog counts. `current` (pnu, parcel_id) as of the earlier edition,
    `after` (pnu) the new edition's parcels, `carry` (successor_pnu, predecessor_pnu) from `carrying`."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    day = F.lit(to_date)
    new = after.join(current, "pnu", "left_anti").select("pnu").persist()
    gone = current.join(after, "pnu", "left_anti").persist()
    carried = (carry.join(new.withColumnRenamed("pnu", "successor_pnu"), "successor_pnu", "left_semi")
               .join(gone.withColumnRenamed("pnu", "predecessor_pnu"), "predecessor_pnu")
               .persist())
    issued = (new.join(carried.select(F.col("successor_pnu").alias("pnu")), "pnu", "left_anti")
              .withColumn("parcel_id", parcel_id_expr(F.col("pnu"), to_date)))
    retired = gone.join(carried.select(F.col("predecessor_pnu").alias("pnu")), "pnu", "left_anti")
    rows = (_rows(carried.withColumnRenamed("predecessor_pnu", "pnu"), pi.HISTORIC, closed=day)
            .unionByName(_rows(carried.withColumnRenamed("successor_pnu", "pnu"), pi.CURRENT, opened=day))
            .unionByName(_rows(issued, pi.CURRENT, opened=day))
            .unionByName(_rows(retired, pi.HISTORIC, closed=day)))
    counts = {"kept": after.join(current, "pnu", "left_semi").count(), "carried": carried.count(),
              "issued": issued.count(), "retired": retired.count(), "redirected": 0}
    return rows, counts


def upgrade_rows(current: Any, closed: Any, carry: Any, on_date: str) -> tuple[Any, dict[str, int]]:
    """`parcel_identity.upgrade(state, effective, on_date)` as joins: a current parcel whose carrying link
    now points at a closed PNU takes that PNU's id back, its interim id redirected (never deleted)."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    day = F.lit(on_date)
    pairs = (carry.join(current.select(F.col("pnu").alias("successor_pnu"), F.col("parcel_id").alias("interim")),
                        "successor_pnu")
             .join(closed.select(F.col("pnu").alias("predecessor_pnu"), F.col("parcel_id").alias("original")),
                   "predecessor_pnu")
             .where(F.col("interim") != F.col("original"))
             .select(F.col("successor_pnu").alias("pnu"), "interim", "original").persist())
    rows = (_rows(pairs.withColumnRenamed("interim", "parcel_id"), pi.REDIRECTED, opened=day,
                  redirect=F.col("original"))
            .unionByName(_rows(pairs.withColumnRenamed("original", "parcel_id"), pi.CURRENT, opened=day)))
    return rows, {"redirected": pairs.count()}


def problems(registry: Any, rows: Any, after: Any) -> list[str]:
    """`parcel_identity.check_every_parcel_has_one_current_id` over the registry with `rows` appended,
    as joins: at most twenty of each problem come to the driver."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    final = current_ids(registry.select(*REGISTRY_ROW).unionByName(rows.select(*REGISTRY_ROW)))
    held = final.join(after, "pnu", "left_semi")
    missing = [r["pnu"] for r in after.join(final, "pnu", "left_anti").orderBy("pnu").limit(20).collect()]
    twice = [r["parcel_id"] for r in held.groupBy("parcel_id").count().where(F.col("count") > 1)
             .orderBy("parcel_id").limit(20).collect()]
    return [f"no current id: {p}" for p in missing] + [f"id on two current PNUs: {i}" for i in twice]


def _root(parent: dict[str, str], s: str) -> str:
    while parent[s] != s:
        parent[s] = parent[parent[s]]
        s = parent[s]
    return s


def advance_groups(links: Sequence[tuple[str, str]], to_sido: Sequence[str], from_sido: Sequence[str]) -> list[list[str]]:
    """The regions one advance must read together: the 시도 a link joins (predecessor 시도 → successor
    시도, e.g. 29·46 → 12 on 2026-07-01) are one region, every other 시도 its own. Each group is
    [to 시도 (comma-joined), from 시도 (comma-joined)], smallest first by name."""

    # Union-find without a closure: the deferred-name check reads module functions only.
    parent = {s: s for s in set(to_sido) | set(from_sido) | {p for pair in links for p in pair}}
    for a, b in links:
        parent[_root(parent, a)] = _root(parent, b)
    groups: dict[str, set[str]] = {}
    for s in parent:
        groups.setdefault(_root(parent, s), set()).add(s)
    out = []
    for members in groups.values():
        to = sorted(members & set(to_sido))
        if to:
            out.append([",".join(to), ",".join(sorted(members & (set(from_sido) | set(to_sido))))])
    return sorted(out)
