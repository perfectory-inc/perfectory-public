"""The content fingerprint of a by-PNU Gold snapshot (root ADR-0274 §1).

`rows:<count>;digest_sum:<sum>`: the row count and the exact sum of every row's `row_digest` read as
its first 15 hex digits (60 bits; 40 million rows stay far inside decimal(38)). `row_digest` already
fingerprints exactly the columns the serving document is built from (root ADR-0099), so two
snapshots with the same fingerprint serve the same documents; the sum is order-free, so it does not
depend on how the files are laid out.

The producer records it in the snapshot summary under PROPERTY and does not rewrite a Gold whose
fingerprint equals the current one (gold_incremental.Outcome.commit); the bake's delta job reflects
two snapshots with equal fingerprints without scanning them (by_pnu_panel_delta.compare).
"""
from __future__ import annotations

from typing import Any

PROPERTY = "foundation.content-fingerprint"
DIGEST = "row_digest"
HEX_DIGITS = 15


def value(rows: int, digest_sum: int) -> str:
    return f"rows:{int(rows)};digest_sum:{int(digest_sum)}"


def of_frame(frame: Any) -> str | None:
    """The fingerprint of `frame`'s rows, or None for a table without `row_digest`."""
    from pyspark.sql import functions as F

    if DIGEST not in frame.columns:
        return None
    part = F.conv(F.substring(F.col(DIGEST), 1, HEX_DIGITS), 16, 10).cast("decimal(38,0)")
    row = frame.agg(F.count(F.lit(1)).alias("rows"), F.sum(part).alias("digest_sum")).first()
    return value(row["rows"], row["digest_sum"] or 0)


def recorded(spark: Any, table: str, snapshot_id: str | int) -> str | None:
    """The fingerprint the snapshot's summary records, or None (written before ADR-0274, or by hand)."""
    rows = spark.sql(
        f"SELECT summary['{PROPERTY}'] AS fingerprint FROM {table}.snapshots WHERE snapshot_id = {int(snapshot_id)}"
    ).collect()
    return rows[0]["fingerprint"] if rows and rows[0]["fingerprint"] else None
