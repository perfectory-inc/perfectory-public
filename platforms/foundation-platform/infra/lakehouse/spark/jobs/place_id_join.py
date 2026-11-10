"""Every parcel source's Gold row carries its parcel's stable place id (root ADR-0483, ADR-0288 §5).

A parcel source (a Silver the place-identity resolver placed, as the pipeline-graph guard reads it) is
joined by `place_id`, not by the PNU, which a 법정동 code change renumbers. Its by-PNU kind's Gold names
the join columns in its kind line (`place_identity`, `by_pnu_kinds.Kind.place_identity`):

- `place_id`: the parcel id `gold.place_id_bridge` holds for the row's PNU (unit `parcel`, the release
  of root ADR-0481): the open period of that code, else its latest closed one. A code-pair line
  (`legal-dong-code-change`) is a new code's form of an id, not a period the served PNU has, and is not
  read. A PNU whose region has not been released yet has none (null), and gets one when it is.
- `main_lot_group_place_ids`: for a row whose deals reached it by their 본번 group (root ADR-0405), the
  sorted place ids of the group's parcels as a JSON array: the group-level place id of a masked deal.

Partial (root ADR-0312): the build reads only the bridge rows of the PNUs it builds (`restrict` keeps
the `pnu` of this frame like any input's), and a run recomputes the PNUs whose place id moved:
the codes of the changelog rows the release appended since the Gold's pin (`gold.place_id_changelog`
is appended once per release, so an Iceberg append scan reads only those), or, when that span is not
appends only, the place ids that differ between the two bridge snapshots.
"""
from __future__ import annotations

from typing import Any, Callable

BRIDGE_SOURCE = "gold.place_id_bridge"
CHANGELOG_SOURCE = "gold.place_id_changelog"
INPUTS = (BRIDGE_SOURCE, CHANGELOG_SOURCE)
UNIT = "parcel"
# The bridge's word for a row a code pair carried, not a period (parcel_place_id_release.PAIR_SOURCE).
PAIR_SOURCE = "legal-dong-code-change"
PLACE_ID = "place_id"
GROUP_PLACE_IDS = "main_lot_group_place_ids"
# What the build is handed of the bridge: the PNU and its id (`place_ids`).
BUILD_COLUMNS = {BRIDGE_SOURCE: ("pnu", PLACE_ID)}
LINEAGE = ("row_digest", "source_snapshot_id", "published_at_utc")


def place_ids(bridge: Any) -> Any:
    """`pnu, place_id` of each parcel code: its open period's id, else its latest closed one's."""
    from pyspark.sql import Window, functions as F  # noqa: PLC0415

    periods = bridge.where((F.col("unit") == F.lit(UNIT)) & (F.col("valid_from") != F.lit(PAIR_SOURCE)))
    newest = Window.partitionBy("code").orderBy(F.col("valid_to").isNotNull().asc(), F.col("valid_to").desc(),
                                                F.col("place_id").asc())
    return (periods.withColumn("_n", F.row_number().over(newest)).where(F.col("_n") == 1)
            .select(F.col("code").alias("pnu"), F.col("place_id")))


def read(spark: Any, args: Any, at: dict[str, str]) -> Any:
    """The parcel place ids at the bridge's pin `at` (the parcel partitions only)."""
    import parcel_panel_silver_to_gold as panel  # noqa: PLC0415

    return place_ids(panel.read_source(spark, args, BRIDGE_SOURCE, at))


def with_place_ids(gold: Any, ids: Any, content: tuple[str, ...], columns: tuple[str, ...],
                   group_pnus: Any = None) -> Any:
    """`gold` with its join columns (`content` names which) and `row_digest` over `content` again.

    ``group_pnus``: a column of the PNUs of each row's 본번 group (array<string>, empty or null for
    none), when the kind carries `main_lot_group_place_ids`; their ids are read from ``ids``.
    """
    from pyspark.sql import functions as F  # noqa: PLC0415

    rows = gold.drop(*[c for c in (PLACE_ID, GROUP_PLACE_IDS) if c in gold.columns]).join(ids, "pnu", "left")
    if GROUP_PLACE_IDS in content:
        if group_pnus is None:
            raise ValueError(f"{GROUP_PLACE_IDS} needs the rows' 본번 group")
        members = (rows.select("pnu", F.explode(group_pnus).alias("_member"))
                   .join(ids.select(F.col("pnu").alias("_member"), F.col(PLACE_ID).alias("_member_id")), "_member")
                   .groupBy("pnu").agg(F.to_json(F.array_sort(F.collect_set("_member_id"))).alias(GROUP_PLACE_IDS)))
        rows = rows.join(members, "pnu", "left")
    return (rows.withColumn("row_digest", F.sha2(F.to_json(F.struct(*content)), 256)).select(*columns))


def changed_pnus(spark: Any, changed: list[str], appended: list[str], changelog_table: str,
                 previous: dict[str, str], pins: dict[str, str], old_ids: Callable[[], Any], new_ids: Any) -> Any | None:
    """The PNUs whose place id may have moved, or None when neither input moved.

    The changelog's appended rows name every code a release renumbered, added, retired or redirected
    (both sides); a span that is not appends only (a compaction, an expired pin) is compared instead.
    """
    import gold_incremental as incremental  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415

    if not set(INPUTS) & set(changed):
        return None
    # A bridge row outside the changelog is a code-pair line, which `place_ids` does not read.
    if CHANGELOG_SOURCE in appended:
        rows = incremental.read_appended(spark, changelog_table, previous[CHANGELOG_SOURCE], pins[CHANGELOG_SOURCE])
        rows = rows.where(F.col("unit") == F.lit(UNIT))
        return (rows.select(F.col("from_code").alias("pnu")).unionByName(rows.select(F.col("to_code").alias("pnu")))
                .where(F.col("pnu").isNotNull()).distinct())
    return incremental.changed_rows(old_ids(), new_ids, ("pnu",)).select("pnu").distinct()


def changelog_table(args: Any) -> str:
    """The changelog's Iceberg name, where its appends are scanned (in its own namespace, as read)."""
    return f"{args.iceberg_catalog_name}.{CHANGELOG_SOURCE}"


def lane_change(spark: Any, args: Any, previous: dict[str, str], pins: dict[str, str],
                old_frames: Callable[[], dict[str, Any]], new_frames: dict[str, Any]
                ) -> Callable[[list[str], list[str]], Any | None]:
    """`changed_pnus` for a producer's `affected(changed, appended)`: its frames hold the place ids
    under BRIDGE_SOURCE (`read`), the previous pins' through `old_frames`."""
    return lambda changed, appended: changed_pnus(
        spark, changed, appended, changelog_table(args), previous, pins,
        lambda: old_frames()[BRIDGE_SOURCE], new_frames[BRIDGE_SOURCE])


def built(columns: tuple[str, ...]) -> tuple[str, ...]:
    """The Gold columns a producer's own build writes: all but the join columns this module adds."""
    return tuple(c for c in columns if c not in (PLACE_ID, GROUP_PLACE_IDS))


def gold_content(kind: Any) -> tuple[str, ...]:
    """A kind's content columns: the PNU, its section, the lineage path and its join columns."""
    return ("pnu", *kind.columns, "attached_via", *kind.place_identity)


def gold_columns(kind: Any) -> tuple[str, ...]:
    return (*gold_content(kind), *LINEAGE)
