#!/usr/bin/env python3
"""Append standing steward decisions to `silver.parcel_lineage` (root ADR-0115 §9).

Step 2 of the fold. `foundation-outbox-publisher export-lineage-steward-fold` wrote the decisions
as lineage rows (`stewardship_domain::fold::lineage_row` decides their shape); this job checks the
file, appends the rows once, and writes a summary that `record-lineage-steward-folds` reads back.
The run id is derived from the decision ids, so rerunning the same file appends nothing twice.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages
from lakehouse_ingest import append_batch_once
from lineage_review_queue import STEWARD
from parcel_lineage_to_silver import IDENTIFIER
from platform_contracts import (
    column_names,
    load_lakehouse_contract,
    spark_sql_type,
)

JOB_NAME = "lineage_steward_fold_to_silver"
SCHEMA_VERSION = "foundation-platform.lineage_steward_fold_handoff.v1"
RULES_VERSION = "steward-v1"
CONTRACT = load_lakehouse_contract("silver.parcel_lineage")
PNU = re.compile(r"^[0-9]{19}$")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--input", required=True, help="The export-lineage-steward-fold file")
    parser.add_argument("--summary-output", required=True, help="Read by record-lineage-steward-folds")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--silver-namespace", default="silver")
    parser.add_argument("--allow-non-smoke-write", action="store_true")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    return parser.parse_args(argv)


def fold_rows(handoff: dict[str, Any], now: datetime) -> tuple[str, list[dict[str, Any]], list[str]]:
    """(derivation_run_id, lineage rows, decision ids) of a checked handoff.

    Refuses anything the exporter would not write: another schema, a count that disagrees, a row
    that is not a steward row, a link without a predecessor, a "not a link" with one.
    """

    if handoff.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"unknown fold handoff schema {handoff.get('schema_version')!r}")
    rows = handoff.get("rows") or []
    if handoff.get("row_count") != len(rows):
        raise ValueError(f"handoff says {handoff.get('row_count')} rows but holds {len(rows)}")
    decision_ids: list[str] = []
    out: list[dict[str, Any]] = []
    for row in rows:
        if row.get("evidence_kind") != STEWARD:
            raise ValueError(f"{row.get('successor_pnu')}: not a steward row")
        if not PNU.fullmatch(row.get("successor_pnu") or ""):
            raise ValueError(f"{row.get('successor_pnu')!r} is not a PNU")
        predecessor = row.get("predecessor_pnu")
        if row.get("grade") == "official":
            if not PNU.fullmatch(predecessor or ""):
                raise ValueError(f"{row['successor_pnu']}: a link names a predecessor PNU")
        elif row.get("grade") == "pending":
            if predecessor:
                raise ValueError(f"{row['successor_pnu']}: 'not a link' names no predecessor")
        else:
            raise ValueError(f"{row['successor_pnu']}: steward grade {row.get('grade')!r}")
        ref = json.loads(row["evidence_ref"])
        if ref.get("decision_id") != row.get("decision_id") or not ref.get("evidence_etag"):
            raise ValueError(f"{row['successor_pnu']}: evidence_ref does not describe its decision")
        decision_ids.append(row["decision_id"])
        out.append(
            {
                "predecessor_pnu": predecessor or None,
                "successor_pnu": row["successor_pnu"],
                "relation": row["relation"],
                "cardinality": None,
                "effective_date": row.get("effective_date"),
                "grade": row["grade"],
                "evidence_kind": STEWARD,
                "evidence_ref": row["evidence_ref"],
                "from_snapshot_id": row["from_snapshot_id"],
                "to_snapshot_id": row["to_snapshot_id"],
                "rules_version": RULES_VERSION,
                "derived_at_utc": now,
            }
        )
    if len(set(decision_ids)) != len(decision_ids):
        raise ValueError("a decision appears twice")
    run_id = "steward-fold-" + hashlib.sha256("\n".join(sorted(decision_ids)).encode()).hexdigest()[:24]
    for row in out:
        row["derivation_run_id"] = run_id
    return run_id, out, decision_ids


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    for label in ("iceberg_catalog_name", "silver_namespace"):
        if not IDENTIFIER.fullmatch(getattr(args, label)):
            raise ValueError(f"{label} must be a plain SQL identifier")
    if args.silver_namespace == "silver" and not args.allow_non_smoke_write:
        raise ValueError("writing the silver namespace needs --allow-non-smoke-write")
    now = datetime.now(timezone.utc).replace(microsecond=0)
    run_id, rows, decision_ids = fold_rows(json.loads(Path(args.input).read_text(encoding="utf-8")), now)
    summary: dict[str, Any] = {"job": JOB_NAME, "derivation_run_id": run_id, "decision_ids": decision_ids, "rows": len(rows)}
    if rows:
        assert_catalog_env()
        from pyspark.sql import SparkSession  # noqa: PLC0415

        builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
        spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
        try:
            columns = column_names(CONTRACT)
            schema = ", ".join(f"{c['name']} {spark_sql_type(c['logical_type'])}" for c in CONTRACT["columns"])
            frame = spark.createDataFrame(rows, schema=schema).select(*columns)
            table = f"`{args.iceberg_catalog_name}`.`{args.silver_namespace}`.`parcel_lineage`"
            summary["appended"] = append_batch_once(spark, frame, columns, table, CONTRACT["table_name"])["appended"]
        finally:
            spark.stop()
    Path(args.summary_output).write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("lineage-steward-fold-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
