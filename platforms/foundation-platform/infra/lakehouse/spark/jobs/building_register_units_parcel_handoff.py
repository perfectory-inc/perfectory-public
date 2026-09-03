#!/usr/bin/env python3
"""Export one row per exclusive-part unit, shaped for `catalog.building_unit` (ADR-0072).

Reads `silver.building_register_units` and `silver.building_register_unit_areas`, aggregates the
areas onto their unit, and writes gzip JSONL objects — one per five-digit PNU prefix — plus a
manifest that names every object with its row count.

**The manifest is the contract between this job and the loader.** The source is an Iceberg table,
so there is no pre-measured object list the way the parcel pipeline has one: this export decides
how many objects exist. A loader that listed the prefix instead would read an empty listing as
"no work to do" — the manifest is written last, so a loader that cannot find it refuses instead
of succeeding at nothing.

Rows this job deliberately leaves behind, and counts rather than drops silently (ADR-0072 §3):

- `null_pnu_row_count` — units whose register states no parcel number. Measured 2026-09-03:
  150,639 of 19,765,555.
- `invalid_pnu_row_count` — a pnu that is not 19 digits cannot name a parcel. Measured: rows on
  exactly one malformed pnu.

What it does not do: it does not check the pnu against `catalog.parcel`. That answer lives in
Postgres, and this job runs where Postgres is not; the loader owns the orphan count.
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

JOB_NAME = "building_register_units_parcel_handoff"
CONTRACT_PATH_ENV = "FOUNDATION_PLATFORM_BUILDING_UNIT_HANDOFF_CONTRACT_PATH"
DEFAULT_CONTRACT_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "contracts", "building-unit-handoff.json"
)
MANIFEST_SCHEMA_VERSION = "foundation-platform.building_unit_handoff_manifest.v1"
PNU_PATTERN = r"^[0-9]{19}$"


def load_handoff_contract() -> dict[str, Any]:
    path = os.getenv(CONTRACT_PATH_ENV, DEFAULT_CONTRACT_PATH)
    with open(path, encoding="utf-8") as handle:
        contract = json.load(handle)
    if contract.get("schema_version") != 1:
        raise ValueError(f"unsupported handoff contract schema_version in {path}")
    return contract


def load_pyspark() -> None:
    """Deferred so `infra/lakehouse/spark/tests` can import this file without PySpark."""

    global DataFrame, SparkSession, F, T, Window

    from pyspark.sql import DataFrame, SparkSession, Window
    from pyspark.sql import functions as F
    from pyspark.sql import types as T


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export catalog.building_unit handoff objects from the Silver unit tables."
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
    return spark.table(f"`{catalog}`.`silver`.`building_register_units`").select(
        F.col("mgm_bldrgst_pk").alias("register_pk"),
        F.trim(F.col("pnu")).alias("pnu"),
        F.col("building_mgm_bldrgst_pk"),
        F.col("dong_join_name"),
        F.col("dong_name_raw"),
        F.col("unit_label_ko"),
        F.col("unit_name_raw"),
        F.col("floor_kind"),
        F.col("floor_number"),
    )


def read_exclusive_areas(spark: Any, catalog: str) -> Any:
    """One row per register_pk: the exclusive-area sum and the largest row's usage/structure.

    `area_kind` is the producer's typed 전유/공용 split (`exclusive` / `common` / `unknown`);
    only `exclusive` describes the unit itself. A unit can hold several exclusive rows (one per
    usage), so the area is their sum and the usage/structure come from the largest — the fact a
    person means when they ask what a unit is (ADR-0072 §5).
    """

    areas = (
        spark.table(f"`{catalog}`.`silver`.`building_register_unit_areas`")
        .where(F.col("area_kind") == F.lit("exclusive"))
        .select("mgm_bldrgst_pk", "area_m2", "usage_name_raw", "structure_name_raw")
    )
    largest_first = Window.partitionBy("mgm_bldrgst_pk").orderBy(
        F.col("area_m2").desc_nulls_last()
    )
    ranked = areas.withColumn("_rank", F.row_number().over(largest_first))
    sums = areas.groupBy("mgm_bldrgst_pk").agg(
        F.sum("area_m2").alias("exclusive_area_m2")
    )
    tops = ranked.where(F.col("_rank") == 1).select(
        "mgm_bldrgst_pk",
        F.col("usage_name_raw").alias("usage_name"),
        F.col("structure_name_raw").alias("structure_name"),
    )
    return sums.join(tops, "mgm_bldrgst_pk", "left").withColumnRenamed(
        "mgm_bldrgst_pk", "register_pk"
    )


def floor_label_column() -> Any:
    """`floor_kind` + `floor_number` as a display label; empty when the register says nothing."""

    number = F.col("floor_number")
    kind = F.col("floor_kind")
    return (
        F.when(number.isNull(), F.lit(""))
        .when(kind == F.lit("basement"), F.concat(F.lit("지하 "), number.cast("string"), F.lit("층")))
        .when(kind == F.lit("rooftop"), F.lit("옥탑"))
        .otherwise(F.concat(number.cast("string"), F.lit("층")))
    )


def build_handoff_frame(units: Any, areas: Any, columns: list[str]) -> Any:
    joined = units.join(areas, "register_pk", "left")
    trimmed_link = F.trim(F.col("building_mgm_bldrgst_pk"))
    frame = joined.select(
        F.col("register_pk"),
        F.col("pnu"),
        # "The register wrote nothing" and "an empty key" are the same claim (ADR-0075 §2).
        F.when(F.length(trimmed_link) == 0, F.lit(None))
        .otherwise(trimmed_link)
        .alias("building_register_pk"),
        F.coalesce(F.col("dong_join_name"), F.lit("")).alias("building_name"),
        F.coalesce(F.col("dong_name_raw"), F.lit("")).alias("dong_name"),
        F.coalesce(F.col("unit_label_ko"), F.col("unit_name_raw"), F.lit("")).alias("ho_name"),
        floor_label_column().alias("floor_label"),
        F.col("exclusive_area_m2"),
        F.coalesce(F.col("usage_name"), F.lit("")).alias("usage_name"),
        F.coalesce(F.col("structure_name"), F.lit("")).alias("structure_name"),
    )
    return frame.select(*columns)


def split_by_pnu_validity(frame: Any) -> tuple[Any, int, int]:
    """Exportable rows, plus the two left-behind counts the manifest must carry."""

    null_pnu = frame.where(F.col("pnu").isNull() | (F.length(F.col("pnu")) == 0))
    non_null = frame.where(F.col("pnu").isNotNull() & (F.length(F.col("pnu")) > 0))
    invalid = non_null.where(~F.col("pnu").rlike(PNU_PATTERN))
    valid = non_null.where(F.col("pnu").rlike(PNU_PATTERN))
    return valid, int(null_pnu.count()), int(invalid.count())


def write_objects(
    frame: Any,
    storage_options: dict[str, str],
    bucket: str,
    prefix: str,
    suffix: str,
) -> list[dict[str, Any]]:
    """Writes one gzip JSONL object per sigungu and returns the manifest entries.

    Spark's own writers name files after task ids, and the loader must know each object's exact
    key and row count — so the rows come back to the driver one sigungu at a time and are written
    with a deterministic name. ~250 objects of tens of megabytes; the driver never holds more
    than one sigungu.
    """

    import boto3

    client = boto3.client("s3", **storage_options)
    entries: list[dict[str, Any]] = []
    sigungus = [
        row.sigungu
        for row in frame.select(F.substring("pnu", 1, 5).alias("sigungu")).distinct().collect()
    ]
    for sigungu in sorted(sigungus):
        rows = frame.where(F.substring("pnu", 1, 5) == sigungu)
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
        print(f"building-unit-handoff-object key={key} rows={count}")
    return entries


def storage_client_options() -> tuple[dict[str, str], str]:
    """R2 client settings for the handoff writes, from the writer credentials.

    Not `lakehouse_object_store`: that module is deliberately reader-only — its comment says a
    writer key there would grant the read path a power it never uses — and this job is the one
    place the power is used. The first draft borrowed the reader keys anyway, and the export
    would have died at its first `put_object` with an access error naming neither the variable
    nor the reason. Missing variables are refused by name here, before Spark starts.
    """

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
        units = read_units(spark, args.iceberg_catalog_name)
        areas = read_exclusive_areas(spark, args.iceberg_catalog_name)
        frame = build_handoff_frame(units, areas, contract["columns"])
        valid, null_pnu_count, invalid_pnu_count = split_by_pnu_validity(frame)
        valid = valid.persist()

        entries = write_objects(
            valid,
            storage_options,
            bucket,
            contract["handoff_prefix"].rstrip("/"),
            contract["handoff_suffix"],
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
            "invalid_pnu_row_count": invalid_pnu_count,
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
        print(f"building-unit-handoff-summary-json {payload}")
        print(
            f"building-unit-handoff-export-ok rows={exported} objects={len(entries)} "
            f"null_pnu={null_pnu_count} invalid_pnu={invalid_pnu_count}"
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
