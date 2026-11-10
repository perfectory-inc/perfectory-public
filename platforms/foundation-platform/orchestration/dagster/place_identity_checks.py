"""Place-identity health: a parcel source never loses its parcels silently (root ADR-0288 §6, stage 5).

Every parcel source Silver reaches the current PNU through the one resolver (ADR-0288 §1). The defect
that ADR records, a 분구 (화성시 41590 → 동탄구 41597) that left 57 live 공매 objects on no parcel, was found by
a person whose join failed: nothing measured the resolution, so nothing said it fell. This check measures
it on every evaluation and compares it with the evaluation before, read from Dagster's own event log
(no second store, like `serves_the_newest_gold`):

- the share of current rows with a `pnu`, and the unresolved rows by `pnu_unresolved_reason` when the
  contract has that column;
- it fails when the share fell by `MAX_SHARE_DROP` or more, or when more rows became unresolved than the
  rows added since would explain at the previous rate (`newly_unresolved`, beyond `noise_bound`).

Its declarations do not block (`blocking: false`, WARN): showing a source's resolved rows always wins
over holding them back. A failure pages through Alertmanager (dual_run.post_alert, the dual run's own
route), once per table, held a day.

The current rows are the contract's (publish_checks.current_rows_sql); no table or column is written here.
"""

import datetime
import json
import math

from dagster import AssetCheckKey, AssetCheckResult, MetadataValue

import dual_run
import publish_checks
import unit_price_checks
from asset_checks import Kind

import platform_contracts  # noqa: E402  (publish_checks puts the Spark jobs directory on the path)

REASON = "pnu_unresolved_reason"
# A fall of one point of the whole table's share is a broken resolver or a lost code change, not a
# day's mix: measured 2026-11-11 (read-only Trino, current rows) court items 88.5%, registered factories
# 81.5%, 공매 items 53.3%, 실거래 4.2% (most lots masked), each moving well under a point a day.
MAX_SHARE_DROP = 0.01
# The fewest newly unresolved rows that fail, however few rows came: the 2026-10-20 화성 defect was 57 rows.
MIN_NEWLY_UNRESOLVED = 50
# Above the floor, rows added at the previous rate scatter binomially; four standard deviations of the
# added rows' unresolved count is noise, not a defect.
NOISE_SIGMAS = 4


def resolution_sql(table):
    """(current rows, rows with a pnu, unresolved rows by reason as JSON or NULL), in one scan of the
    current rows: three scans of silver.real_transaction's 13.4M rows exceeded Trino's 8GB a node
    (measured 2026-11-11)."""
    columns = platform_contracts.column_names(platform_contracts.load_lakehouse_contract(table))
    reason = REASON if REASON in columns else "CAST(NULL AS varchar)"
    current = publish_checks.current_rows_sql(table, ["pnu", *([REASON] if REASON in columns else [])])
    by_reason = (f"json_format(CAST(map_agg(coalesce(reason, 'none'), n) FILTER (WHERE NOT resolved) AS JSON))"
                 if REASON in columns else "NULL")
    return (f"SELECT sum(n), coalesce(sum(n) FILTER (WHERE resolved), 0), {by_reason} FROM "
            f"(SELECT pnu IS NOT NULL AS resolved, {reason} AS reason, count(*) AS n FROM ({current}) GROUP BY 1, 2)")


def noise_bound(added, previous_share):
    """Newly unresolved rows the added rows explain as chance, at the previous rate."""
    spread = math.sqrt(max(added, 0) * previous_share * (1 - previous_share))
    return max(MIN_NEWLY_UNRESOLVED, NOISE_SIGMAS * spread)


def judge(now, previous):
    """(passed, findings) of this evaluation (rows, resolved) against the previous one or None."""
    rows, resolved = now
    share = resolved / rows if rows else 0.0
    findings = {"rows": rows, "resolved": resolved, "resolved_share": round(share, 4)}
    if previous is None or not previous[0]:
        return True, findings
    before_rows, before_resolved = previous
    before_share = before_resolved / before_rows
    added = rows - before_rows
    # Rows unresolved now beyond those the previous unresolved and the added rows at its rate explain.
    newly = (rows - resolved) - (before_rows - before_resolved) - max(added, 0) * (1 - before_share)
    findings.update(previous_rows=before_rows, previous_resolved=before_resolved,
                    previous_share=round(before_share, 4), newly_unresolved=round(newly),
                    noise_bound=round(noise_bound(added, before_share)))
    dropped = before_share - share >= MAX_SHARE_DROP
    return not (dropped or newly > noise_bound(added, before_share)), findings


def previous_counts(instance, key):
    """(rows, resolved) the last evaluation of this check recorded, or None. The newest record can be this
    execution, planned and not yet evaluated: records without an evaluation are passed over."""
    for record in instance.event_log_storage.get_asset_check_execution_history(key, limit=5):
        evaluation = record.event.dagster_event.asset_check_evaluation_data if record.event else None
        metadata = evaluation.metadata if evaluation else {}
        if "rows" in metadata and "resolved" in metadata:
            return int(metadata["rows"].value), int(metadata["resolved"].value)
    return None


def page(table, findings):
    now = datetime.datetime.now(datetime.UTC)
    alert = {"labels": {"alertname": "FoundationPlaceIdentityResolutionFell", "service": "place-identity",
                        "environment": "prelaunch", "table": table},
             "annotations": {"summary": f"🟠 {table}: 필지 해석이 떨어졌어요 — 해석률 {findings.get('previous_share')} → "
                                        f"{findings['resolved_share']}, 새로 못 이은 행 {findings.get('newly_unresolved')} "
                                        f"(허용 {findings.get('noise_bound')}). 법정동 코드 변경·해석기를 보세요 (ADR-0288 §6)"},
             "startsAt": now.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
             "endsAt": (now + datetime.timedelta(hours=dual_run.PAGE_HOLD_HOURS)).strftime("%Y-%m-%dT%H:%M:%S.000Z")}
    return dual_run.post_alert(alert)


def result(table, row, previous, severity, pager=page):
    rows, resolved, by_reason = int(row[0] or 0), int(row[1] or 0), row[2]
    passed, findings = judge((rows, resolved), previous)
    metadata = {"table": table, **findings, "unresolved_by_reason": MetadataValue.json(json.loads(by_reason) if by_reason else {})}
    if not passed:
        metadata["paged"] = pager(table, findings)
    return AssetCheckResult(passed=passed, severity=severity, metadata=metadata)


def parcel_source_resolution(context, check):
    table = check.node["table_name"]
    (row,) = unit_price_checks.trino_rows(resolution_sql(table))
    previous = previous_counts(context.instance, AssetCheckKey(check.key(), "parcel_source_resolution"))
    return result(table, row, previous, check.severity)


KINDS = {
    "parcel_source_resolution": Kind("The share of current rows on a current parcel did not fall, and no more rows "
                                     "became unresolved than the added rows explain (ADR-0288 §6).",
                                     parcel_source_resolution),
}
