"""What the 전유공용면적 Silver table's load identity is for a row loaded whole (root ADR-0391 addendum).

The table loads part by part since 2026-11: each row's `source_part_id` is the handoff part it came
from (the export writes it, as ADR-0357's part lanes do). A release loaded whole is one part named
by its run, `source_snapshot_id`, the identity the table recorded for that load. This is the
registered backfill of the rows loaded before the column (lakehouse_schema_migrate.BACKFILLS).
"""

from __future__ import annotations

from typing import Any

TABLE_CONTRACT = "silver.building_register_unit_areas"


def whole_run_part_column() -> Any:
    """`source_part_id` of a row loaded whole: its run."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    return F.col("source_snapshot_id")
