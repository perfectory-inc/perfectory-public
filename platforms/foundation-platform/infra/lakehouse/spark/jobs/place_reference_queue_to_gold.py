#!/usr/bin/env python3
"""Write every parcel source's unresolved rows to the one steward queue (root ADR-0483 §5).

`gold.lineage_review_queue` unit `place_reference` (`place_reference_queue`). Partial: a source is read
only when its Silver snapshot moved since the queue last recorded it (each commit records the snapshot
it read of each source it wrote, `SNAPSHOT_PROPERTY_PREFIX + <table>`), and only that source's items
are rewritten, in one MERGE that inserts, updates and deletes the items under its table name and no
other row. A source never recorded is read once (its first items).
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import place_reference_queue as queue

JOB_NAME = "place_reference_queue_to_gold"
QUEUE = "gold.lineage_review_queue"
SNAPSHOT_PREFIX = "foundation.place-reference-snapshot."
VIEW = "foundation_place_reference_items"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--gold-namespace", default="gold")
    parser.add_argument("--allow-non-smoke-write", action="store_true")
    parser.add_argument("--summary-output")
    parser.add_argument("--handoff-output", help="the whole place_reference unit, as the steward database loads it")
    return parser.parse_args(argv)


def recorded_snapshots(summaries: list[dict[str, str]]) -> dict[str, str]:
    """{source table: the snapshot the queue last read of it}, newest commit first in `summaries`."""
    found: dict[str, str] = {}
    for summary in summaries:
        for key, value in (summary or {}).items():
            if key.startswith(SNAPSHOT_PREFIX):
                found.setdefault(key[len(SNAPSHOT_PREFIX):], value)
    return found


def merge_sql(table: str, columns: list[str], source: str) -> str:
    """Insert, update and delete the items of `source` only (subject codes start with its name)."""
    assignments = ", ".join(f"t.{c} = v.{c}" for c in columns)
    return (f"MERGE INTO {table} t USING {VIEW} v ON t.unit = v.unit AND t.subject_code = v.subject_code "
            f"WHEN MATCHED AND t.evidence_etag <> v.evidence_etag THEN UPDATE SET {assignments} "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) VALUES ({', '.join('v.' + c for c in columns)}) "
            f"WHEN NOT MATCHED BY SOURCE AND t.unit = '{queue.UNIT}' AND t.subject_code LIKE '{source}|%' THEN DELETE")


def unresolved(frame: Any, contract: dict[str, Any]) -> Any:
    """`reason, legal_dong_code, rows, sample_row_keys` of the current rows without a `pnu`."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    from platform_contracts import column_names, current_row_latest, current_rows  # noqa: PLC0415

    columns = set(column_names(contract))
    latest = current_row_latest(contract)
    key = list(latest.key) if latest is not None else (["row_key"] if "row_key" in columns else [])
    code = (F.substring("legal_dong_code", 1, 10) if "legal_dong_code" in columns else F.lit(None).cast("string"))
    sample = F.concat_ws("|", *[F.col(c).cast("string") for c in key]) if key else F.lit(None).cast("string")
    rows = current_rows(frame, contract).where(F.col("pnu").isNull())
    return (rows.select(F.coalesce(F.col(queue.REASON), F.lit("unknown")).alias("reason"),
                        code.alias("legal_dong_code"), sample.alias("_key"))
            .groupBy("reason", "legal_dong_code")
            .agg(F.count(F.lit(1)).alias("rows"),
                 F.slice(F.array_sort(F.collect_set("_key")), 1, queue.SAMPLE_KEYS).alias("sample_row_keys")))


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession  # noqa: PLC0415

    import gold_incremental as incremental  # noqa: PLC0415
    from lakehouse_engine import apply_catalog_settings, assert_catalog_env  # noqa: PLC0415
    from lakehouse_engine import current_snapshot  # noqa: PLC0415
    from platform_contracts import admit_write, column_names, load_lakehouse_artifact, load_lakehouse_contract  # noqa: PLC0415

    args = parse_args(argv)
    if args.gold_namespace == "gold" and not args.allow_non_smoke_write:
        raise ValueError("writing the gold namespace needs --allow-non-smoke-write")
    assert_catalog_env(args.iceberg_catalog_name)
    contract = load_lakehouse_contract(QUEUE)
    columns = list(column_names(contract))
    table = f"`{args.iceberg_catalog_name}`.`{args.gold_namespace}`.`lineage_review_queue`"
    now = datetime.now(timezone.utc).replace(microsecond=0)
    spark = apply_catalog_settings(SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}")
                                   .config("spark.sql.session.timeZone", "UTC"), args.iceberg_catalog_name).getOrCreate()
    summary: dict[str, Any] = {"job": JOB_NAME, "unit": queue.UNIT, "read": {}, "unchanged": []}
    try:
        history = [dict(r.summary or {}) for r in spark.sql(
            f"SELECT summary FROM {table}.snapshots ORDER BY committed_at DESC").collect()]
        recorded = recorded_snapshots(history)
        written: dict[str, list[queue.Item]] = {}
        for source in queue.sources(load_lakehouse_artifact()["contracts"]):
            namespace, name = source.split(".", 1)
            qualified = f"`{args.iceberg_catalog_name}`.`{namespace}`.`{name}`"
            if not spark.catalog.tableExists(f"{args.iceberg_catalog_name}.{namespace}.{name}"):
                continue
            snapshot = str(current_snapshot(spark, qualified))
            if recorded.get(source) == snapshot:
                summary["unchanged"].append(source)
                continue
            source_contract = load_lakehouse_contract(source)
            frame = spark.read.option("snapshot-id", snapshot).table(f"{args.iceberg_catalog_name}.{source}")
            found = queue.items(source, (r.asDict() for r in unresolved(frame, source_contract).collect()))
            out = [{"unit": queue.UNIT, "subject_code": i.subject_code, "item_id": i.item_id, "status": i.status,
                    "candidates_json": i.candidates_json, "evidence_etag": i.evidence_etag,
                    "from_snapshot_id": recorded.get(source), "to_snapshot_id": snapshot, "published_at_utc": now}
                   for i in found]
            schema = ", ".join(f"{c['name']} {_sql_type(c['logical_type'])}" for c in contract["columns"])
            spark.createDataFrame(out, schema=schema).select(*columns).createOrReplaceTempView(VIEW)
            admit_write(QUEUE, "replace", target=table)
            incremental.with_commit_properties(spark, {SNAPSHOT_PREFIX + source: snapshot},
                                               lambda: spark.sql(merge_sql(table, columns, source)))
            written[source] = found
            summary["read"][source] = snapshot
        summary["open"] = queue.summary(written)
        if args.handoff_output:
            rows = spark.sql(f"SELECT item_id, subject_code, status, candidates_json, evidence_etag, from_snapshot_id, "
                             f"to_snapshot_id FROM {table} WHERE unit = '{queue.UNIT}' ORDER BY subject_code").collect()
            document = handoff(rows, now.strftime("%Y-%m-%dT%H:%M:%SZ"))
            Path(args.handoff_output).write_text(json.dumps(document, ensure_ascii=False) + "\n", encoding="utf-8")
            summary["handoff_items"] = document["item_count"]
    finally:
        spark.stop()
    summary["outcome"] = "written" if summary["read"] else "unchanged"
    if args.summary_output:
        Path(args.summary_output).write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                                             encoding="utf-8")
    print("place-reference-queue-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


def handoff(rows: list[Any], published_at_utc: str) -> dict[str, Any]:
    """The unit's whole queue, as `load-lineage-review-items` replaces it in the steward database: every
    source's items, not only the ones this run rewrote (the load swaps the unit in one transaction)."""
    import lineage_review_queue as q  # noqa: PLC0415

    items = [q.ReviewItem(item_id=str(r["item_id"]), subject_code=r["subject_code"], status=r["status"],
                          candidates_json=r["candidates_json"], evidence_etag=r["evidence_etag"],
                          from_snapshot_id=r["from_snapshot_id"] or "", to_snapshot_id=r["to_snapshot_id"] or "")
             for r in rows]
    return q.handoff_document(items, queue.UNIT, published_at_utc)


def _sql_type(logical: str) -> str:
    from platform_contracts import spark_sql_type  # noqa: PLC0415

    return spark_sql_type(logical)


if __name__ == "__main__":
    raise SystemExit(main())
