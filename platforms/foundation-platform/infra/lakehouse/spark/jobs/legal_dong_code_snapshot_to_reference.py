"""The newest snapshot of the official 법정동 code list (`reference.legal_dong_code_snapshot`), for
the jobs that read it.

The load itself left Spark (root ADR-0559): the publisher's `load-legal-dong-code-snapshot` reads the
code.go.kr 전체 표 the daily collector landed in Bronze, checks its shape and size, and appends it
(`foundation_publisher_silver::legal_dong_code_snapshot_stage`). What stays here is the one reading
of "the official code list" every Spark job takes: the contract's current rows.
"""

from __future__ import annotations

from typing import Any

from platform_contracts import current_rows, load_lakehouse_contract

JOB_NAME = "legal_dong_code_snapshot_to_reference"
CONTRACT = load_lakehouse_contract("reference.legal_dong_code_snapshot")


def newest_snapshot(spark, table: str) -> Any:
    """The rows of the newest snapshot of the code list `table` (qualified): its contract's current
    rows (`Editions` by `snapshot_date`, root ADR-0286 §3). Every reader of "the official code list"
    in the jobs takes it from here; a load of a second table on one day is the same edition."""

    return current_rows(spark.table(table), CONTRACT)


def newest_snapshot_codes(spark, table: str) -> tuple[str | None, dict[str, tuple[str, str]]]:
    """The newest snapshot of `table` (qualified) as (its date, {region_cd: (full_name, status)}).

    `(None, {})` when the table holds none. The administrative matching gate and the boundary round
    merge read it.
    """

    rows = newest_snapshot(spark, table).selectExpr(
        "region_cd", "full_name", "status", "CAST(snapshot_date AS STRING) AS d").collect()
    if not rows:
        return None, {}
    return rows[0]["d"], {row["region_cd"]: (row["full_name"], row["status"]) for row in rows}
