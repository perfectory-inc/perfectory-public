#!/usr/bin/env python3
"""Authenticate retained FLOOR history before exporting recovered Bronze inputs.

This gate only reads Iceberg. The fixed witness binds source content to an old name;
the table's own snapshots remain the authority for whether its rows were committed.
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import os
import stat
import uuid
import json
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from lakehouse_ingest import (
    INGEST_BATCH_OBJECTS_KEY, INGEST_BATCH_TOKEN_KEY, OBJECT_NAME_SEPARATOR,
    identities_under_derivation, ingest_batch_token,
    read_recorded_append,
)

HISTORY_PATH_ENV = "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH"
HISTORY_SHA256_ENV = "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_SHA256"
OUTCOME_SCHEMA = "foundation-platform.floor-history-check.v1"
MAX_RECEIPT_BYTES = 65536
EXPORT_REQUIRED = 10
LINEAGE_COLUMNS = ("source_snapshot_id", "bronze_object_key", "valid_from_utc", "ingested_at_utc")


def read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ValueError("source evidence must be a regular file")
    with path.open("rb") as stream:
        data = stream.read(MAX_RECEIPT_BYTES + 1)
    if len(data) > MAX_RECEIPT_BYTES:
        raise ValueError("source evidence exceeds its byte limit")
    value = json.loads(data)
    if not isinstance(value, dict):
        raise ValueError("source evidence must be an object")
    return value


def _unique_json_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate history witness field")
        value[key] = item
    return value


def _canonical_witness_timestamp(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("history witness timestamp must be RFC3339 text")
    match = re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.([0-9]{1,9}))?(?:Z|[+-][0-9]{2}:[0-9]{2})", value)
    if match is None:
        raise ValueError("history witness timestamp must be RFC3339 text")
    fraction = match.group(1) or ""
    if any(digit != "0" for digit in fraction[6:]):
        raise ValueError("history witness timestamp exceeds Iceberg microsecond precision")
    parsed = utc(value)
    precision = "seconds" if parsed.microsecond == 0 else "milliseconds" if parsed.microsecond % 1000 == 0 else "microseconds"
    return parsed.isoformat(timespec=precision).replace("+00:00", "Z")


def validate_history_witness(value: Any) -> dict[str, Any]:
    fields = {"schema_version", "table_name", "table_uuid", "snapshot_id", "operation",
              "source_snapshot_id", "bronze_object_key", "valid_from_utc", "ingested_at_utc",
              "row_count", "inputs"}
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError("invalid history witness fields")
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ValueError("unsupported history witness schema")
    if value["table_name"] != "silver.building_register_floors" or value["operation"] != "overwrite":
        raise ValueError("invalid history witness table or operation")
    if not isinstance(value["table_uuid"], str) or uuid.UUID(value["table_uuid"]).int == 0:
        raise ValueError("invalid history witness table UUID")
    snapshot = value["snapshot_id"]
    if not isinstance(snapshot, str) or not re.fullmatch(r"[1-9][0-9]{0,18}", snapshot) or int(snapshot) >= 2**63:
        raise ValueError("invalid history witness snapshot")
    if type(value["row_count"]) is not int or not 0 < value["row_count"] < 2**64:
        raise ValueError("invalid history witness row count")
    source = value["source_snapshot_id"]
    if not isinstance(source, str) or not re.fullmatch(r"building-register-floor-selection-v2-[0-9a-f]{64}", source):
        raise ValueError("invalid historical source identity")
    value = dict(value)
    value["table_uuid"] = str(uuid.UUID(value["table_uuid"]))
    for field in ("valid_from_utc", "ingested_at_utc"):
        value[field] = _canonical_witness_timestamp(value[field])
    inputs = value["inputs"]
    if not isinstance(inputs, dict) or set(inputs) != {"floor", "title"}:
        raise ValueError("history witness requires both source roles")
    roles = {"floor": "hubgokr__building_register_floor_overview", "title": "hubgokr__building_register_main"}
    for role, slug in roles.items():
        item = inputs[role]
        if not isinstance(item, dict) or set(item) != {"role_slug", "provider_file_id", "provider_month", "size_bytes", "checksum_sha256"}:
            raise ValueError("invalid historical source fields")
        if item["role_slug"] != slug:
            raise ValueError("historical source role mismatch")
        if not isinstance(item["provider_file_id"], str) or not re.fullmatch(r"OPN[0-9A-Za-z_-]+", item["provider_file_id"]):
            raise ValueError("invalid historical provider file ID")
        if not isinstance(item["provider_month"], str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-01", item["provider_month"]) or date.fromisoformat(item["provider_month"]).day != 1:
            raise ValueError("invalid historical provider month")
        if type(item["size_bytes"]) is not int or not 0 < item["size_bytes"] < 2**64:
            raise ValueError("invalid historical source size")
        if not isinstance(item["checksum_sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", item["checksum_sha256"]):
            raise ValueError("invalid historical source checksum")
    if inputs["floor"]["provider_month"] != inputs["title"]["provider_month"]:
        raise ValueError("historical source months differ")
    floor = inputs["floor"]
    expected_key = f"bronze/source={floor['role_slug']}/{floor['provider_file_id']}--sha256-{floor['checksum_sha256']}.zip"
    if value["bronze_object_key"] != expected_key:
        raise ValueError("historical object key does not match its source")
    return value


def load_history_witness(path: Path, expected_sha256: str | None) -> dict[str, Any]:
    """Read the invocation's private witness once; a receipt cannot appoint its own authority."""
    if not isinstance(expected_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        raise ValueError("history witness requires its parent-provided SHA-256 pin")
    if not path.is_absolute() or path.resolve(strict=True) != path or not stat.S_ISREG(path.lstat().st_mode):
        raise ValueError("history witness must be an absolute regular file without symlinks")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_BINARY", 0)
    descriptor = os.open(path, flags)
    with os.fdopen(descriptor, "rb") as stream:
        metadata = os.fstat(stream.fileno())
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > MAX_RECEIPT_BYTES:
            raise ValueError("history witness must be a bounded regular file")
        data = stream.read(MAX_RECEIPT_BYTES + 1)
    if len(data) > MAX_RECEIPT_BYTES:
        raise ValueError("history witness exceeds its byte limit")
    if not hmac.compare_digest(hashlib.sha256(data).hexdigest(), expected_sha256):
        raise ValueError("history witness content hash changed")
    return validate_history_witness(json.loads(data, object_pairs_hook=_unique_json_fields))


def utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        # Spark timestamps are returned naive in the session's explicitly configured UTC zone.
        parsed = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("source timestamp must carry a timezone")
    else:
        raise ValueError("missing source timestamp")
    return parsed.astimezone(timezone.utc)


def validate_receipt(receipt: dict, witness: dict) -> dict | None:
    fields = {"schema_version", "source_snapshot_id", "bronze_object_key", "valid_from_utc", "ingested_at_utc",
              "inputs", "historical_binding"}
    if set(receipt) != fields or type(receipt["schema_version"]) is not int or receipt["schema_version"] != 2:
        raise ValueError("unsupported FLOOR source inspection receipt")
    if not isinstance(receipt["bronze_object_key"], str) or not receipt["bronze_object_key"].startswith("bronze/"):
        raise ValueError("missing committed Bronze object key")
    utc(receipt["ingested_at_utc"])
    utc(receipt["valid_from_utc"])
    inputs = receipt["inputs"]
    if not isinstance(inputs, dict) or set(inputs) != {"floor", "title"}:
        raise ValueError("FLOOR source inspection requires both source roles")
    binding = receipt["historical_binding"]
    if binding is None:
        if any(date.fromisoformat(inputs[role]["provider_month"]) <= date.fromisoformat(witness["inputs"][role]["provider_month"])
               for role in ["floor", "title"]):
            raise ValueError("historical provider month cannot fall through to a new load")
        if not re.fullmatch(r"building-register-floor-content-v1-[0-9a-f]{64}", receipt["source_snapshot_id"]):
            raise ValueError("unexpected new FLOOR source identity")
        return None
    binding = validate_history_witness(binding)
    if (binding != witness or inputs != witness["inputs"]
            or receipt["source_snapshot_id"] != witness["source_snapshot_id"]
            or utc(receipt["valid_from_utc"]) != utc(witness["valid_from_utc"])):
        raise ValueError("source inspection does not match the fixed historical witness")
    return binding


def verify_metadata(witness: dict, table_uuid: str, head: int, snapshots: list[dict]) -> None:
    pin = int(witness["snapshot_id"])
    by_id = {int(row["snapshot_id"]): row for row in snapshots}
    if table_uuid != witness["table_uuid"] or pin not in by_id or len(by_id) != len(snapshots):
        raise ValueError("historical Iceberg table or snapshot does not match")
    retained = by_id[pin]
    summary = retained["summary"]
    source = witness["source_snapshot_id"]
    if (retained["operation"] != witness["operation"]
            or summary.get(INGEST_BATCH_OBJECTS_KEY) != source
            or summary.get(INGEST_BATCH_TOKEN_KEY) != ingest_batch_token([source])
            or summary.get("total-records") != str(witness["row_count"])):
        raise ValueError("historical snapshot commit evidence differs")
    occurrences = [row["snapshot_id"] for row in snapshots
                   if source in row["summary"].get(INGEST_BATCH_OBJECTS_KEY, "").split(OBJECT_NAME_SEPARATOR)]
    if occurrences != [pin]:
        raise ValueError("historical source has ambiguous snapshot registry entries")
    verify_append_descendant(head, pin, by_id)


def verify_append_descendant(head: int, pin: int, by_id: dict) -> None:
    visited = set()
    while head != pin:
        if head in visited or head not in by_id or by_id[head]["operation"] != "append":
            raise ValueError("current table is not an append-only descendant of retained history")
        visited.add(head)
        head = by_id[head]["parent_id"]


def registered_snapshot(receipt: dict, derivation: str | None, head: int, snapshots: list[dict]) -> dict | None:
    identity = identities_under_derivation([receipt["source_snapshot_id"]], derivation)[0]
    matches = [row for row in snapshots
               if identity in row["summary"].get(INGEST_BATCH_OBJECTS_KEY, "").split(OBJECT_NAME_SEPARATOR)]
    if not matches:
        return None
    if len(matches) != 1:
        raise ValueError("FLOOR source has ambiguous append registry entries")
    record = matches[0]
    summary = record["summary"]
    if (record["operation"] != "append" or summary.get(INGEST_BATCH_OBJECTS_KEY) != identity
            or summary.get(INGEST_BATCH_TOKEN_KEY) != ingest_batch_token([identity])
            or not re.fullmatch(r"[1-9][0-9]*", summary.get("added-records", ""))):
        raise ValueError("FLOOR append commit evidence differs")
    by_id = {int(row["snapshot_id"]): row for row in snapshots}
    if len(by_id) != len(snapshots):
        raise ValueError("duplicate Iceberg snapshot IDs")
    verify_append_descendant(head, int(record["snapshot_id"]), by_id)
    return record


def verify_registered_lineage(receipt: dict, record: dict, rows: list[Any]) -> datetime:
    if len(rows) != 1:
        raise ValueError("recorded FLOOR append has missing or mixed row lineage")
    row = rows[0]
    if (row["source_snapshot_id"] != receipt["source_snapshot_id"]
            or row["bronze_object_key"] != receipt["bronze_object_key"]
            or utc(row["valid_from_utc"]) != utc(receipt["valid_from_utc"])
            or row["count"] != int(record["summary"]["added-records"])):
        raise ValueError("recorded FLOOR rows differ from authenticated source and count")
    return utc(row["ingested_at_utc"])


def authenticate_registered_source(spark: Any, receipt: dict, witness: dict,
                                   catalog: str, derivation: str | None) -> dict | None:
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", catalog):
        raise ValueError("invalid Iceberg catalog name")
    name = f"{catalog}.{witness['table_name']}"
    table, table_uuid, head, snapshots = inspect_table(spark, name)
    # A missing/replaced table or broken historical chain is not a new empty dataset.
    verify_metadata(witness, table_uuid, head, snapshots)
    record = registered_snapshot(receipt, derivation, head, snapshots)
    result = None
    if record is not None:
        frame = read_recorded_append(spark, name, int(record["snapshot_id"]))
        rows = frame.groupBy(*LINEAGE_COLUMNS).count().limit(2).collect()
        retained_time = verify_registered_lineage(receipt, record, rows)
        result = {"table_uuid": table_uuid, "snapshot_id": str(record["snapshot_id"]),
                  "observed_head_snapshot_id": str(head), "retained_row_count": rows[0]["count"],
                  "retained_ingested_at_utc": retained_time.isoformat(),
                  "retention_kind": "registered_append", "derivation": derivation}
    else:
        from pyspark.sql import functions as F
        current = (spark.read.format("iceberg").option("snapshot-id", str(head)).load(name)
                   .where(F.col("source_snapshot_id") == receipt["source_snapshot_id"]))
        # Snapshot expiry may remove the registry without removing the rows. Never reappend
        # such a source under the assumption that a missing registry means unprocessed.
        if derivation is None and current.limit(1).count():
            raise ValueError("FLOOR rows exist without their recorded append; recovery required")
        if derivation is not None and current.where(
                F.col("ingested_at_utc") >= utc(receipt["ingested_at_utc"])).limit(1).count():
            raise ValueError("changed derivation must be later than retained rows")
    table.refresh()
    if str(table.uuid()) != table_uuid or table.currentSnapshot().snapshotId() != head:
        raise ValueError("Iceberg head changed during append authentication; retry")
    return result


def verify_lineage(witness: dict, rows: list[Any]) -> None:
    if len(rows) != 1:
        raise ValueError("historical snapshot has missing or mixed row lineage")
    row = rows[0]
    if (row["source_snapshot_id"] != witness["source_snapshot_id"]
            or row["bronze_object_key"] != witness["bronze_object_key"]
            or utc(row["valid_from_utc"]) != utc(witness["valid_from_utc"])
            or utc(row["ingested_at_utc"]) != utc(witness["ingested_at_utc"])
            or row["count"] != witness["row_count"]):
        raise ValueError("historical rows do not match retained source, times and count")


def inspect_table(spark: Any, name: str) -> tuple[Any, str, int, list[dict]]:
    table = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, name)
    table.refresh()
    current = table.currentSnapshot()
    if current is None:
        raise ValueError("historical table has no current snapshot")
    snapshots = [{"snapshot_id": item.snapshotId(), "parent_id": item.parentId(),
                  "operation": item.operation(), "summary": dict(item.summary())}
                 for item in table.snapshots()]
    return table, str(table.uuid()), current.snapshotId(), snapshots


def authenticate_history(spark: Any, witness: dict, catalog: str) -> int:
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", catalog):
        raise ValueError("invalid Iceberg catalog name")
    name = f"{catalog}.{witness['table_name']}"
    table, table_uuid, head, snapshots = inspect_table(spark, name)
    verify_metadata(witness, table_uuid, head, snapshots)
    rows = (spark.read.format("iceberg").option("snapshot-id", witness["snapshot_id"])
            .load(name).groupBy(*LINEAGE_COLUMNS).count().limit(2).collect())
    verify_lineage(witness, rows)
    # The proof concerns one observed head, not an unpinned sequence of reads.
    table.refresh()
    if str(table.uuid()) != table_uuid or table.currentSnapshot().snapshotId() != head:
        raise ValueError("Iceberg head changed during historical authentication; retry")
    return head


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-receipt", required=True, type=Path)
    parser.add_argument("--summary-output", required=True, type=Path)
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--derivation")
    args = parser.parse_args(argv)
    witness_path = os.environ.get(HISTORY_PATH_ENV)
    if not witness_path:
        raise ValueError("missing explicit private FLOOR history witness path")
    witness = load_history_witness(Path(witness_path), os.environ.get(HISTORY_SHA256_ENV))
    receipt = read_json(args.source_receipt)
    binding = validate_receipt(receipt, witness)
    if args.derivation is not None:
        identities_under_derivation([receipt["source_snapshot_id"]], args.derivation)
    action = "export_required"
    observed_head = None
    retained = None
    from lakehouse_engine import apply_catalog_settings
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName("foundation-floor-history-check").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        if binding is not None:
            observed_head = authenticate_history(spark, binding, args.iceberg_catalog_name)
        if binding is None or args.derivation is not None:
            retained = authenticate_registered_source(
                spark, receipt, witness, args.iceberg_catalog_name, args.derivation)
    finally:
        spark.stop()
    if retained is not None:
        action = "already_retained"
    elif binding is not None and args.derivation is None:
        action = "already_retained"
    elif binding is not None and utc(receipt["ingested_at_utc"]) <= utc(binding["ingested_at_utc"]):
        raise ValueError("changed derivation must be later than retained historical rows")
    outcome = {"schema_version": OUTCOME_SCHEMA, "action": action,
               "source_snapshot_id": receipt["source_snapshot_id"],
               "inputs": receipt["inputs"], "derivation": args.derivation,
               "table_uuid": binding["table_uuid"] if binding else None,
               "snapshot_id": binding["snapshot_id"] if binding else None,
               "observed_head_snapshot_id": str(observed_head) if observed_head else None,
               "retained_row_count": binding["row_count"] if binding else 0,
               "persisted_row_count": 0}
    if retained is not None:
        outcome.update(retained)
    with args.summary_output.open("x", encoding="utf-8") as stream:
        json.dump(outcome, stream, sort_keys=True)
        stream.write("\n")
    # 10 is the explicit read-only preflight protocol: continue to the normal producer.
    return 0 if action == "already_retained" else EXPORT_REQUIRED


if __name__ == "__main__":
    raise SystemExit(main())
