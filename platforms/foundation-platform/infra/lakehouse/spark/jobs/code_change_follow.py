"""A Silver table's links follow the 법정동 code changes (root ADR-0329 §3, ADR-0416).

A table whose rows carry a PNU or 법정동 code found at load time records, in a commit, the snapshot of
`reference.legal_dong_code_change` those links were made against
(`follow:reference.legal_dong_code_change@<snapshot>@<UTC instant>`, an object key `append_source_object`
records). A later run that finds the change table moved re-links only the current rows under the codes
`views.changed_codes` names between the two snapshots, plus the rows nothing linked, and appends only
the rows whose link moved. A table that records no follow yet is followed once over all its current
rows (the first follow); an expired earlier snapshot is the same.

Plain Python for the records and the codes; `rows_under` and the snapshot readers take Spark.
"""

from __future__ import annotations

import re
import sys
from datetime import datetime
from typing import Any, Iterable, Sequence

CHANGE_TABLE = "reference.legal_dong_code_change"
FOLLOW_PREFIX = f"follow:{CHANGE_TABLE}@"
# The change table's columns a change is read by (the Gold build's, `views.changed_codes`).
CHANGE_COLUMNS = ("kind", "old_code", "new_code", "level")


def follow_record(snapshot: int, at: datetime) -> str:
    """The record of one linking against a change-table snapshot; the instant orders the records
    (snapshot ids are not ordered)."""

    return f"{FOLLOW_PREFIX}{snapshot}@{at:%Y%m%dT%H%M%SZ}"


def last_followed(recorded: Iterable[str]) -> int | None:
    """The change-table snapshot the table's links were last made against, from its commit records."""

    found = []
    for key in recorded:
        if not key.startswith(FOLLOW_PREFIX):
            continue
        snapshot, _, at = key[len(FOLLOW_PREFIX):].partition("@")
        if snapshot.isdigit() and re.fullmatch(r"\d{8}T\d{6}Z", at):
            found.append((at, int(snapshot)))
    return max(found)[1] if found else None


def prefixes_from_changes(before: Iterable[dict[str, Any]], after: Iterable[dict[str, Any]]) -> set[str]:
    """The PNU prefixes under every code that differs between two reads of the change table."""

    import legal_dong_code_change_views as views  # noqa: PLC0415

    return {views.code_prefix(code) for code in views.changed_codes(before, after, CHANGE_COLUMNS)}


def change_snapshot(spark: Any, catalog: str) -> int | None:
    """The change table's newest snapshot id, or None when the table does not exist."""

    schema, name = CHANGE_TABLE.split(".")
    if not spark.catalog.tableExists(f"{catalog}.{CHANGE_TABLE}"):
        return None
    row = spark.sql(f"SELECT snapshot_id FROM `{catalog}`.`{schema}`.`{name}`.snapshots "
                    "ORDER BY committed_at DESC LIMIT 1").first()
    return int(row["snapshot_id"]) if row else None


def changed_prefixes(spark: Any, catalog: str, before: int | None, after: int, job: str) -> set[str] | None:
    """The prefixes the change table moved between two snapshots, or None when every current row is
    linked again: the table records no follow yet (`before` is None) or the earlier snapshot expired."""

    if before is None:
        return None
    schema, name = CHANGE_TABLE.split(".")
    table = f"`{catalog}`.`{schema}`.`{name}`"
    try:
        reads = [[row.asDict() for row in spark.sql(
            f"SELECT {', '.join(CHANGE_COLUMNS)} FROM {table} VERSION AS OF {snapshot}").collect()]
            for snapshot in (before, after)]
    except Exception as error:  # noqa: BLE001 - an expired snapshot is a full re-link, never a skipped one
        print(f"{job}: change table snapshot {before} unreadable ({type(error).__name__}); linking every row again",
              file=sys.stderr)
        return None
    return prefixes_from_changes(*reads)


def rows_under(current: Any, prefixes: set[str] | None, code_columns: Sequence[str], unlinked_column: str) -> Any:
    """The current rows a code change can move: a code column under a changed prefix, or not linked
    at all (``unlinked_column`` null). Every current row when ``prefixes`` is None."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    if prefixes is None:
        return current
    under = F.col(unlinked_column).isNull()
    for column in code_columns:
        for digits in sorted({len(p) for p in prefixes}):
            under = under | F.substring(column, 1, digits).isin(sorted(p for p in prefixes if len(p) == digits))
    return current.where(under)
