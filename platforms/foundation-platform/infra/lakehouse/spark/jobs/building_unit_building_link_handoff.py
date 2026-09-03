#!/usr/bin/env python3
"""Export one (unit, building) register-key pair per linkable unit (ADR-0074 step 4).

Reads `silver.building_register_units` and writes gzip JSONL objects — one per five-digit PNU
prefix — plus a manifest naming every object with its row count. The third manifest handoff,
one join over: same manifest-last rule, same writer credentials, same left-behind counting as
`building_register_units_parcel_handoff.py` and `building_titles_catalog_handoff.py`.

Rows this job leaves behind and counts rather than drops silently:

- `unlinked_row_count` — units whose register states no building key (Silver's `unresolved`
  band; measured 140,105 of 19,765,555). They have no pair to carry.
- `null_pnu_row_count` — units whose register states no parcel. They never entered
  `catalog.building_unit`, so a pair for them would update nothing.
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import os
from datetime import datetime, timezone
from typing import Any

from lakehouse_engine import (
    apply_catalog_settings,
    assert_catalog_env,
    assert_iceberg_runtime_loaded,
    iceberg_packages,
)

JOB_NAME = "building_unit_building_link_handoff"
CONTRACT_PATH_ENV = "FOUNDATION_PLATFORM_BUILDING_UNIT_LINK_HANDOFF_CONTRACT_PATH"
DEFAULT_CONTRACT_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "contracts", "building-unit-building-link-handoff.json"
)
MANIFEST_SCHEMA_VERSION = "foundation-platform.building_unit_building_link_handoff_manifest.v1"


def load_handoff_contract() -> dict[str, Any]:
    path = os.getenv(CONTRACT_PATH_ENV, DEFAULT_CONTRACT_PATH)
    with open(path, encoding="utf-8") as handle:
        contract = json.load(handle)
    if contract.get("schema_version") != 1:
        raise ValueError(f"unsupported handoff contract schema_version in {path}")
    return contract


def load_pyspark() -> None:
    """Deferred so `infra/lakehouse/spark/tests` can import this file without PySpark."""

    global SparkSession, F

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export unit-to-building link pairs from silver.building_register_units."
    )
    parser.add_argument(
        "--iceberg-catalog-name",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_CATALOG_NAME", "r2"),
        help="Spark catalog name for Iceberg REST catalog reads.",
    )
    parser.add_argument(
        "--iceberg-packages",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_PACKAGES", iceberg_packages()),
        help="Comma-separated Iceberg Spark packages.",
    )
    parser.add_argument(
        "--summary-output",
        help="Optional path for a machine-readable run summary JSON file.",
    )
    return parser.parse_args(argv)


def build_spark_session(args: argparse.Namespace) -> Any:
    assert_catalog_env()
    builder = (
        SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "16")
    )
    builder = apply_catalog_settings(builder, args.iceberg_catalog_name)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    assert_iceberg_runtime_loaded(spark, args.iceberg_packages)
    return spark


def read_units(spark: Any, catalog: str) -> Any:
    """One candidate pair per unit row, in the loader's column names, plus the partition key."""

    return spark.table(f"`{catalog}`.`silver`.`building_register_units`").select(
        F.col("mgm_bldrgst_pk").alias("register_pk"),
        F.col("building_mgm_bldrgst_pk").alias("building_register_pk"),
        F.col("pnu"),
    )


def write_objects(
    frame: Any,
    storage_options: dict[str, str],
    bucket: str,
    prefix: str,
    suffix: str,
    columns: list[str],
) -> list[dict[str, Any]]:
    """Writes one gzip JSONL object per sigungu and returns the manifest entries."""

    import boto3

    client = boto3.client("s3", **storage_options)
    entries: list[dict[str, Any]] = []
    sigungus = [
        row.sigungu
        for row in frame.select(F.substring("pnu", 1, 5).alias("sigungu")).distinct().collect()
    ]
    for sigungu in sorted(sigungus):
        rows = frame.where(F.substring("pnu", 1, 5) == sigungu).select(*columns)
        payload = io.BytesIO()
        count = 0
        with gzip.GzipFile(fileobj=payload, mode="wb") as gz:
            for row in rows.toLocalIterator():
                gz.write(json.dumps(row.asDict(), ensure_ascii=False).encode("utf-8"))
                gz.write(b"\n")
                count += 1
        key = f"{prefix}/{sigungu}{suffix}"
        client.put_object(Bucket=bucket, Key=key, Body=payload.getvalue())
        entries.append({"key": key, "rows": count})
        print(f"building-unit-link-handoff-object key={key} rows={count}")
    return entries


def storage_client_options() -> tuple[dict[str, str], str]:
    """R2 writer credentials, refused by name when absent — this job is the writer."""

    values = {}
    for name in (
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT",
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID",
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY",
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET",
    ):
        value = os.getenv(name, "").strip()
        if not value:
            raise ValueError(f"{name} is required to write handoff objects")
        values[name] = value
    return (
        {
            "endpoint_url": values["FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT"],
            "aws_access_key_id": values["FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID"],
            "aws_secret_access_key": values[
                "FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY"
            ],
            "region_name": "auto",
        },
        values["FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET"],
    )


def main(argv: list[str] | None = None) -> int:
    contract = load_handoff_contract()
    load_pyspark()
    args = parse_args(argv)
    spark = build_spark_session(args)
    storage_options, bucket = storage_client_options()

    try:
        frame = read_units(spark, args.iceberg_catalog_name)
        has_pnu = F.col("pnu").isNotNull() & (F.length(F.col("pnu")) > 0)
        has_link = F.col("building_register_pk").isNotNull() & (
            F.length(F.trim(F.col("building_register_pk"))) > 0
        )
        exportable = frame.where(has_pnu & has_link).persist()
        null_pnu_count = int(frame.where(~has_pnu).count())
        unlinked_count = int(frame.where(has_pnu & ~has_link).count())

        entries = write_objects(
            exportable,
            storage_options,
            bucket,
            contract["handoff_prefix"].rstrip("/"),
            contract["handoff_suffix"],
            contract["columns"],
        )
        exported = sum(entry["rows"] for entry in entries)

        manifest = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "job_name": JOB_NAME,
            "created_at_utc": datetime.now(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z"),
            "columns": contract["columns"],
            "objects": entries,
            "exported_row_count": exported,
            "null_pnu_row_count": null_pnu_count,
            "unlinked_row_count": unlinked_count,
        }
        import boto3

        boto3.client("s3", **storage_options).put_object(
            Bucket=bucket,
            Key=contract["manifest_object"],
            Body=json.dumps(manifest, ensure_ascii=False, sort_keys=True).encode("utf-8"),
        )

        payload = json.dumps(manifest, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        if args.summary_output:
            os.makedirs(os.path.dirname(args.summary_output) or ".", exist_ok=True)
            with open(args.summary_output, "w", encoding="utf-8") as handle:
                handle.write(payload + "\n")
        print(f"building-unit-link-handoff-summary-json {payload}")
        print(
            f"building-unit-link-handoff-export-ok rows={exported} objects={len(entries)} "
            f"null_pnu={null_pnu_count} unlinked={unlinked_count}"
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
