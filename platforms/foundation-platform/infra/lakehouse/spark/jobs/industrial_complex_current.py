#!/usr/bin/env python3
"""Which row of `silver.industrial_complexes` is a complex's current one: one rule for every reader.

The table is append-only: a reload of the same source snapshot (the 2026-10-01 source-words refill
appended all 1,442 complexes twice more) and a new provider version both add rows beside the old
ones. A complex's current row is the one not closed (`valid_to_utc` null) that came last: the latest
`valid_from_utc`, then the latest `ingested_at_utc`, then the smaller official code. The catalog Gold
chose that way already; the boundary loader refused instead ("more than one row"), which stopped
`complex_boundary_release` on its supervised run (2026-10-16). Both read this module now.

No PySpark import at module scope: the CI test lane has no PySpark, and `pick_current` lets the rule
be checked there on plain rows.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping

# (column, direction), most significant first.
CURRENT_ORDER: tuple[tuple[str, str], ...] = (
    ("valid_from_utc", "desc"),
    ("ingested_at_utc", "desc"),
    ("official_complex_code", "asc"),
)
PARTITION = "complex_id"
CLOSED = "valid_to_utc"


def current_complex_rows(frame: Any) -> Any:
    """The current row of each complex in a Spark frame of `silver.industrial_complexes` rows."""

    from pyspark.sql import Window  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415

    order = [
        F.col(column).desc_nulls_last() if direction == "desc" else F.col(column).asc_nulls_last()
        for column, direction in CURRENT_ORDER
    ]
    window = Window.partitionBy(PARTITION).orderBy(*order)
    return (
        frame.where(F.col(CLOSED).isNull())
        .withColumn("_current_rank", F.row_number().over(window))
        .where(F.col("_current_rank") == 1)
        .drop("_current_rank")
    )


def pick_current(rows: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """The same rule on plain rows (values compared as given; None sorts last either way)."""

    best: dict[Any, Mapping[str, Any]] = {}
    for row in rows:
        if row.get(CLOSED) is not None:
            continue
        key = row.get(PARTITION)
        if key not in best or _before(row, best[key]):
            best[key] = row
    return [best[key] for key in sorted(best, key=str)]


def _before(row: Mapping[str, Any], other: Mapping[str, Any]) -> bool:
    """Whether `row` comes before `other` in CURRENT_ORDER."""

    for column, direction in CURRENT_ORDER:
        a, b = row.get(column), other.get(column)
        if a == b:
            continue
        if a is None or b is None:
            return b is None
        return a > b if direction == "desc" else a < b
    return False
