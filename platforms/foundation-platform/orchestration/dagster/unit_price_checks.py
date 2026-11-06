"""Asset checks of the unit-price slice (root ADR-0333 §8, stage 0): the check Airflow cannot hold.

ADR-0333 test (d) failed on Airflow 3.3.2 (#940): it has no check whose failure is recorded on the
asset and stops the asset's readers. Dagster's asset checks are that. In stage 0 their declarations
only report (`blocking: false`, severity WARN): Airflow still runs every job and nothing waits on
them. When a lane moves to Dagster (stage 1+), the Gold check's declaration turns `blocking: true`
and the asset gets its `sido` partitions, so a failing sido stops only that sido's bake.

Dagster runs both after airlift's sensor observes the asset materialized (the sensor asks for the
checks of what it saw, dagster_airlift sensor_builder), each in its own run. Which asset carries
which check, and whether it blocks, is its definition's `asset_checks` (asset_checks.py); this file
holds only the kinds.

- `rows_valid_per_sido`: gold.unit_price rows are valid, per sido. The contract's rules a query can decide
  (contracts/data/gold.unit_price.odcs.yaml `quality`): one row per (pnu, building_id), the PNU is 19
  digits, published_at_utc is present. Counted per sido (the PNU's first two digits, ADR-0333 §1's
  partition key) and recorded per sido, so the failing partitions are named. Three narrow columns of
  169,069 rows: measured 2026-10-29 on production Trino, a few seconds.
- `serves_the_newest_gold`: the unit-price lane serves the newest Gold (the node it `reads`). Its latest materialization is not older than Gold's,
  or Gold changed less than the lane freshness budget ago (config/r2-connections.contract.json
  `by_pnu_section_packs.freshness_budget_hours`, ADR-0306). This is ADR-0333 §7's asset lag,
  read from Dagster's own event log: no second store.
"""

import json
import os
import time
import urllib.parse

from dagster import AssetCheckResult, AssetCheckSeverity, MetadataValue

import observe_assets
from asset_checks import Kind

R2_CONNECTIONS = observe_assets.job_specs.PLATFORM_ROOT / "config" / "r2-connections.contract.json"
TRINO_URL = os.environ.get("FOUNDATION_DAGSTER_TRINO_URL", "http://127.0.0.1:18081")
TRINO_CATALOG = "r2"  # the lakehouse catalog every reader names (scripts/ops/data-quality-check.py)


def per_sido_sql(table):
    """The per-sido counts of the rules a query can decide; `table` is the graph node's table_name."""
    namespace, name = table.split(".", 1)
    return (
        "SELECT substr(pnu, 1, 2) AS sido, count(*) AS rows, "
        "count(*) - count(DISTINCT (pnu, building_id)) AS duplicate_keys, "
        "count_if(NOT regexp_like(pnu, '^[0-9]{19}$')) AS malformed_pnu, "
        "count_if(published_at_utc IS NULL OR published_at_utc = '') AS missing_published_at "
        f"FROM {TRINO_CATALOG}.{namespace}.{name} GROUP BY 1 ORDER BY 1"
    )


def rows_result(rows, severity=AssetCheckSeverity.WARN):
    """AssetCheckResult from per-sido rows (sido, rows, duplicate_keys, malformed_pnu, missing_published_at)."""
    per_sido = {sido: {"rows": int(count), "duplicate_keys": int(duplicates), "malformed_pnu": int(malformed),
                       "missing_published_at": int(missing)}
                for sido, count, duplicates, malformed, missing in rows}
    failing = sorted(sido for sido, counts in per_sido.items()
                     if counts["duplicate_keys"] or counts["malformed_pnu"] or counts["missing_published_at"])
    return AssetCheckResult(
        passed=bool(per_sido) and not failing,
        severity=severity,
        metadata={
            "rows": sum(counts["rows"] for counts in per_sido.values()),
            "sido_checked": len(per_sido),
            "failing_sido": MetadataValue.json(failing),
            "per_sido": MetadataValue.json(per_sido),
        },
    )


def trino_rows(sql):
    import trino  # the check's run, not the code location's load, needs the client

    url = urllib.parse.urlsplit(TRINO_URL)
    connection = trino.dbapi.connect(host=url.hostname, port=url.port or 8080, http_scheme=url.scheme,
                                     user="foundation-dagster-observe", catalog=TRINO_CATALOG)
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
    finally:
        connection.close()


def lag_result(gold_at, lane_at, now, budget_hours, severity=AssetCheckSeverity.WARN):
    """AssetCheckResult of the lane against Gold, from the two latest materialization times (epoch seconds)."""
    lag = 0.0 if gold_at is None or (lane_at is not None and lane_at >= gold_at) else now - gold_at
    return AssetCheckResult(
        passed=lag <= budget_hours * 3600,
        severity=severity,
        metadata={"asset_lag_seconds": round(lag), "freshness_budget_hours": budget_hours,
                  "gold_materialized_at": gold_at or 0, "lane_materialized_at": lane_at or 0},
    )


def _latest(instance, key):
    event = instance.get_latest_materialization_event(key)
    return event.timestamp if event is not None else None


def rows_valid_per_sido(context, check):
    return rows_result(trino_rows(per_sido_sql(check.node["table_name"])), check.severity)


def serves_the_newest_gold(context, check):
    (gold,) = check.reads
    budget = json.loads(R2_CONNECTIONS.read_text(encoding="utf-8"))["by_pnu_section_packs"]["freshness_budget_hours"]
    return lag_result(_latest(context.instance, check.key(gold["id"])), _latest(context.instance, check.key()),
                      time.time(), budget, check.severity)


KINDS = {
    "rows_valid_per_sido": Kind("One row per (pnu, building_id), a 19-digit PNU and published_at_utc, counted per sido.",
                                rows_valid_per_sido),
    "serves_the_newest_gold": Kind("The lane was baked after Gold last changed, or within the lane freshness budget of it.",
                                   serves_the_newest_gold),
}
