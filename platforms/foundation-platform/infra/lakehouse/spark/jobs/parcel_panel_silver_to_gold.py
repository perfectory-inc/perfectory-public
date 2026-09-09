#!/usr/bin/env python3
"""Build the Gold parcel-panel projection from seven Silver sources (root ADR-0096).

One row per PNU, with each panel section pre-aggregated into a JSON string column shaped
exactly like the `foundation-contracts` response DTO the Catalog API serves from Postgres.
The by-PNU serving export re-parses those columns through the same DTOs, so a section this
job shapes wrongly fails the bake, not the browser.

Section semantics reproduce the Postgres projection loaders, not the repo SQL — the repo
reads are bare `WHERE pnu = $1` against single-row-per-PNU tables and every selection rule
lives in the loader:

  zonings      keep inclusion_code 1|2 only (3 접함 dropped); resolve each zone_code to its
               anchor by walking silver.land_use_zone_code parent edges (max 16 hops,
               anchors UQA100/UQA200/UQA300/UQA400/UQB001/UQC001/UQD001, UQA001 stands for
               itself); rows whose walk dies are dropped and counted; collapse per
               (pnu, zone_code) with min(inclusion_code) and max(trimmed zone_name);
               array ordered inclusion_code then zone_code, the API's ORDER BY.
  price        parse the string year/month/price as integers, refuse month outside 1..=12,
               non-positive year, negative price (skipped and counted); newest per pnu by
               (base_year, base_month, price_per_m2) descending — the loader's DISTINCT ON.
  characteristic / forest_ledger
               area_m2 must be finite and positive (skipped and counted); one row per pnu.
  transfer     parcel_history_seq must be present (the loader aborts on absence; this job
               refuses); dedup on (pnu, transfer_history_seq, parcel_history_seq) by the
               loader's NULLS FIRST total order; array ordered moved_at DESC NULLS LAST,
               then both sequences DESC — moved_at and parcel_history_seq compare as text,
               exactly like the Postgres columns.
  land_rights  dong/floor/ho/room NULL becomes '' before anything else (ADR-0093 key
               normalization); dedup on the six-part key by the loader's order; array is
               the first 200 by (right_serial_no, dong_name, floor_name, ho_name,
               room_name) as text ASC, land_right_total counts the full set — the API's
               page bound.

What this job deliberately does NOT reproduce, stated as refusals instead of drift:

  * The loaders' cross-vintage rules (contract-JSON `vintage` joins, `md5(row(...))`
    tiebreaks, per-object ON CONFLICT ordering) are Postgres-and-object specific. This job
    requires every attribute source to carry exactly ONE distinct source_snapshot_id after
    filtering and refuses otherwise — which is production reality today. Multi-vintage
    Silver needs the streaming/vintage follow-up, not a silent approximation.
  * Intra-snapshot duplicate PNUs in characteristic/forest are refused by default; pass
    --allow-intra-snapshot-duplicates to collapse them by a deterministic full-row order
    and count the drops in the run summary.
  * `kind` and parcel-level `area_m2` are Postgres-curated or permanently absent
    (ADR-0020/0070); both bake as null by declaration.
  * Postgres text ORDER BY collation: this job orders by codepoint. Verify `SHOW lc_collate`
    is C-compatible on the serving database before reading an ordering diff as a bug.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.storagelevel import StorageLevel

from lakehouse_engine import (
    apply_catalog_settings,
    assert_catalog_env,
    assert_iceberg_runtime_loaded,
    iceberg_packages,
)
from platform_contracts import (
    column_names,
    create_table_columns_sql,
    current_row_predicate,
    evolve_iceberg_table_to_contract,
    load_lakehouse_contract,
    partition_clause_sql,
    partition_column_names,
    required_column_names,
    sort_order,
)


DEFAULT_ICEBERG_PACKAGES = iceberg_packages()
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
JOB_NAME = "parcel_panel_silver_to_gold"
RUN_SUMMARY_SCHEMA_VERSION = "foundation-platform.spark_run_summary.v1"
LINEAGE_EVENT_SCHEMA_VERSION = "foundation-platform.lakehouse_lineage_event.v1"
LINEAGE_EVENT_TYPE = "lakehouse.lineage.dataset_materialized.v1"
GOLD_CONTRACT_NAME = "gold.parcel_panel"
GOLD_CONTRACT = load_lakehouse_contract(GOLD_CONTRACT_NAME)
GOLD_COLUMNS: tuple[str, ...] = column_names(GOLD_CONTRACT)
REQUIRED_GOLD_COLUMNS: tuple[str, ...] = required_column_names(GOLD_CONTRACT)
GOLD_PARTITION_COLUMNS: tuple[str, ...] = partition_column_names(GOLD_CONTRACT)
GOLD_SORT_COLUMNS: tuple[str, ...] = sort_order(GOLD_CONTRACT)

# The API path constraint (foundation-api get_parcel_by_pnu) and the serving key grammar
# (r2-connections.contract.json parcel_by_pnu_gateway.pnu_pattern) agree on this shape.
PNU_PATTERN = r"^[0-9]{10}[1289][0-9]{8}$"

PARCEL_SOURCE = "silver.parcel_boundaries"
ZONING_SOURCE = "silver.land_use_plan"
ZONE_CODE_SOURCE = "silver.land_use_zone_code"
PRICE_SOURCE = "silver.land_individual_price"
CHARACTERISTIC_SOURCE = "silver.land_characteristic"
FOREST_SOURCE = "silver.land_forest_ledger"
TRANSFER_SOURCE = "silver.land_transfer_history"
LAND_RIGHT_SOURCE = "silver.land_right_registration"
ATTRIBUTE_SOURCES = (
    ZONING_SOURCE,
    PRICE_SOURCE,
    CHARACTERISTIC_SOURCE,
    FOREST_SOURCE,
    TRANSFER_SOURCE,
    LAND_RIGHT_SOURCE,
)
ALL_SOURCES = (PARCEL_SOURCE, *ATTRIBUTE_SOURCES, ZONE_CODE_SOURCE)

# parcel_zoning_catalog_projection_load.rs anchor_for: the resolved endpoints of the
# parent_ucode walk, plus the urban root that stands for itself.
ZONING_ANCHORS = frozenset(
    ("UQA100", "UQA200", "UQA300", "UQA400", "UQB001", "UQC001", "UQD001")
)
ZONING_URBAN_ROOT = "UQA001"
ZONING_ANCHOR_MAX_DEPTH = 16
LAND_RIGHT_PAGE_BOUND = 200


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build gold.parcel_panel from the seven Silver parcel sources."
    )
    parser.add_argument(
        "--input-mode",
        choices=("parquet", "iceberg"),
        default="iceberg",
        help="Read Silver rows from an Iceberg REST catalog or per-table Parquet dirs.",
    )
    parser.add_argument(
        "--input-root",
        help=(
            "Parquet input root for --input-mode=parquet: one subdirectory per source, "
            "named by the table part of the contract name (e.g. land_individual_price)."
        ),
    )
    parser.add_argument(
        "--output",
        help="Gold Parquet output path when --write-mode=parquet.",
    )
    parser.add_argument(
        "--write-mode",
        choices=("parquet", "iceberg"),
        default="parquet",
        help="Write local Parquet or an Iceberg REST catalog table.",
    )
    parser.add_argument(
        "--iceberg-catalog-name",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_CATALOG_NAME", "r2"),
        help="Spark catalog name for Iceberg REST catalog reads and writes.",
    )
    parser.add_argument(
        "--source-iceberg-namespace",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_GOLD_SOURCE_NAMESPACE", "silver"),
        help="Iceberg namespace holding the seven Silver sources.",
    )
    parser.add_argument(
        "--target-iceberg-namespace",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_GOLD_TARGET_NAMESPACE", "gold"),
        help="Iceberg namespace for the target Gold table.",
    )
    parser.add_argument(
        "--target-iceberg-table",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_GOLD_TARGET_TABLE", "parcel_panel"),
        help="Iceberg table name for the target Gold table.",
    )
    parser.add_argument(
        "--iceberg-write-mode",
        choices=("append", "overwrite"),
        default="overwrite",
        help="How Gold rows are written to the Iceberg table.",
    )
    parser.add_argument(
        "--iceberg-packages",
        default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_PACKAGES", DEFAULT_ICEBERG_PACKAGES),
        help="Comma-separated Iceberg Spark packages used for REST catalog writes.",
    )
    parser.add_argument(
        "--iceberg-snapshot-id",
        required=True,
        help="Iceberg snapshot id of silver.parcel_boundaries represented by this projection.",
    )
    parser.add_argument(
        "--published-at-utc",
        default=None,
        help="Publication timestamp in UTC. Defaults to current UTC time.",
    )
    parser.add_argument(
        "--region-prefix",
        default=None,
        help=(
            "Optional PNU prefix filter applied to every source before joining "
            "(e.g. 11 for Seoul). The first lane proof runs Seoul only."
        ),
    )
    parser.add_argument(
        "--allow-intra-snapshot-duplicates",
        action="store_true",
        help=(
            "Collapse duplicate PNUs inside one characteristic/forest snapshot by a "
            "deterministic full-row order instead of refusing. Drops are counted."
        ),
    )
    parser.add_argument(
        "--allow-non-smoke-overwrite",
        action="store_true",
        help="Allow overwrite mode for target tables whose names do not end with _smoke.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate inputs, target config, and Gold quality gates without writing.",
    )
    parser.add_argument(
        "--expected-count",
        type=int,
        default=None,
        help="Optional row-count assertion for smoke and proof runs.",
    )
    parser.add_argument(
        "--summary-output",
        help="Optional path for a machine-readable Spark run summary JSON file.",
    )
    parser.add_argument(
        "--lineage-output",
        help="Optional path for a machine-readable lakehouse lineage event JSON file.",
    )
    return parser.parse_args()


def validate_identifier(label: str, value: str) -> None:
    if IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{label} must be a simple identifier: {value}")


def normalize_utc_timestamp(value: str | None) -> str:
    if value is None or value.strip() == "":
        return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("--published-at-utc must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def validate_args(args: argparse.Namespace) -> None:
    if args.input_mode == "parquet" and not args.input_root:
        raise ValueError("--input-root is required when --input-mode=parquet")
    if args.write_mode == "parquet" and not args.output:
        raise ValueError("--output is required when --write-mode=parquet")
    if args.region_prefix is not None and re.fullmatch(r"[0-9]{1,10}", args.region_prefix) is None:
        raise ValueError("--region-prefix must be 1 to 10 digits")

    validate_identifier("iceberg catalog name", args.iceberg_catalog_name)
    validate_identifier("source iceberg namespace", args.source_iceberg_namespace)
    validate_identifier("target iceberg namespace", args.target_iceberg_namespace)
    validate_identifier("target iceberg table", args.target_iceberg_table)

    if (
        args.write_mode == "iceberg"
        and args.iceberg_write_mode == "overwrite"
        and not args.validate_only
    ):
        is_smoke_table = args.target_iceberg_table.endswith("_smoke")
        if not is_smoke_table and not args.allow_non_smoke_overwrite:
            raise ValueError(
                "Refusing to overwrite a non-smoke Iceberg table without "
                "--allow-non-smoke-overwrite"
            )

    if args.input_mode == "iceberg" or args.write_mode == "iceberg":
        assert_catalog_env()


def source_table_name(contract_name: str) -> str:
    return contract_name.split(".", maxsplit=1)[1]


def read_source(spark: SparkSession, args: argparse.Namespace, contract_name: str) -> DataFrame:
    contract = load_lakehouse_contract(contract_name)
    expected_columns = column_names(contract)
    table = source_table_name(contract_name)
    if args.input_mode == "parquet":
        frame = spark.read.parquet(str(Path(args.input_root) / table))
    else:
        frame = spark.table(
            f"`{args.iceberg_catalog_name}`.`{args.source_iceberg_namespace}`.`{table}`"
        )
    missing = sorted(set(expected_columns) - set(frame.columns))
    if missing:
        raise ValueError(f"{contract_name} input is missing columns: {', '.join(missing)}")
    return frame.select(*expected_columns)


def filtered_by_region(frame: DataFrame, region_prefix: str | None) -> DataFrame:
    if region_prefix is None:
        return frame
    return frame.where(F.col("pnu").startswith(region_prefix))


def assert_single_snapshot(frame: DataFrame, contract_name: str) -> str:
    """Refuse multi-vintage input rather than approximating the loaders' vintage rules."""

    snapshot_ids = [
        row.source_snapshot_id
        for row in frame.select("source_snapshot_id").distinct().limit(3).collect()
    ]
    if len(snapshot_ids) != 1:
        raise ValueError(
            f"{contract_name} holds {len(snapshot_ids)} distinct source_snapshot_id values "
            f"({snapshot_ids}); this projection reproduces the loaders' newest-vintage rules "
            "only for a single-snapshot source — the multi-vintage follow-up is a separate "
            "change, not a bigger assumption"
        )
    return snapshot_ids[0]


def resolve_zoning_anchors(zone_codes: DataFrame) -> dict[str, str]:
    """The loader's anchor_for walk, computed once on the driver.

    silver.land_use_zone_code is ~1,270 rows; collecting it is cheaper and more legible than
    an iterative self-join, and the bounded depth is the loader's own cycle guard.
    """

    parents: dict[str, str | None] = {}
    for row in zone_codes.select("ucode", "parent_ucode").collect():
        raw_parent = (row.parent_ucode or "").strip()
        parents[row.ucode] = raw_parent if raw_parent not in ("", "000000") else None

    anchors: dict[str, str] = {}
    for ucode in parents:
        current = ucode
        for _ in range(ZONING_ANCHOR_MAX_DEPTH):
            if current in ZONING_ANCHORS:
                anchors[ucode] = current
                break
            if current == ZONING_URBAN_ROOT:
                anchors[ucode] = ZONING_URBAN_ROOT
                break
            next_parent = parents.get(current)
            if next_parent is None:
                break
            current = next_parent
    return anchors


def trimmed_or_null(column: str) -> F.Column:
    trimmed = F.trim(F.col(column))
    return F.when(F.length(trimmed) > 0, trimmed)


def build_zonings(
    plan: DataFrame,
    anchors: dict[str, str],
    spark: SparkSession,
    counters: dict[str, int],
) -> DataFrame:
    total = plan.count()
    verdicts = plan.where(F.col("inclusion_code").isin("1", "2"))
    adjacency_skipped = total - verdicts.count()

    anchor_rows = [(ucode, anchor) for ucode, anchor in sorted(anchors.items())]
    anchor_frame = spark.createDataFrame(
        anchor_rows, schema=T.StructType([
            T.StructField("zone_code", T.StringType(), False),
            T.StructField("anchor_code", T.StringType(), False),
        ])
    )
    anchored = verdicts.join(F.broadcast(anchor_frame), on="zone_code", how="left")
    unresolved_skipped = anchored.where(F.col("anchor_code").isNull()).count()
    resolved = anchored.where(F.col("anchor_code").isNotNull())

    counters["zoning_adjacency_skipped"] = int(adjacency_skipped)
    counters["zoning_unresolved_skipped"] = int(unresolved_skipped)

    collapsed = resolved.groupBy("pnu", "zone_code").agg(
        F.max(trimmed_or_null("zone_name")).alias("zone_name"),
        F.max("anchor_code").alias("anchor_code"),
        F.min("inclusion_code").alias("inclusion_code"),
    )
    entries = collapsed.select(
        "pnu",
        F.struct(
            F.col("zone_code"),
            F.col("zone_name"),
            F.col("anchor_code"),
            F.col("inclusion_code"),
        ).alias("entry"),
    )
    return entries.groupBy("pnu").agg(
        F.to_json(
            F.expr(
                """
                array_sort(collect_list(entry), (l, r) -> CASE
                    WHEN l.inclusion_code < r.inclusion_code THEN -1
                    WHEN l.inclusion_code > r.inclusion_code THEN 1
                    WHEN l.zone_code < r.zone_code THEN -1
                    WHEN l.zone_code > r.zone_code THEN 1
                    ELSE 0 END)
                """
            )
        ).alias("zonings_json")
    )


def build_price(price: DataFrame, counters: dict[str, int]) -> DataFrame:
    parsed = price.select(
        "pnu",
        F.trim(F.col("base_year")).cast(T.IntegerType()).alias("base_year"),
        F.trim(F.col("base_month")).cast(T.IntegerType()).alias("base_month"),
        F.trim(F.col("price_per_m2")).cast(T.LongType()).alias("price_per_m2"),
        trimmed_or_null("announced_date").alias("announced_date"),
    )
    valid = parsed.where(
        F.col("base_year").isNotNull()
        & (F.col("base_year") > 0)
        & F.col("base_month").isNotNull()
        & F.col("base_month").between(1, 12)
        & F.col("price_per_m2").isNotNull()
        & (F.col("price_per_m2") >= 0)
    )
    counters["price_rows_skipped"] = int(price.count() - valid.count())

    newest = Window.partitionBy("pnu").orderBy(
        F.col("base_year").desc(),
        F.col("base_month").desc(),
        F.col("price_per_m2").desc(),
    )
    return (
        valid.withColumn("_row_number", F.row_number().over(newest))
        .where(F.col("_row_number") == 1)
        .select(
            "pnu",
            F.to_json(
                F.struct("price_per_m2", "base_year", "base_month", "announced_date")
            ).alias("price_json"),
        )
    )


def one_row_per_pnu(
    frame: DataFrame,
    contract_name: str,
    payload_columns: tuple[str, ...],
    allow_duplicates: bool,
    counters: dict[str, int],
    counter_key: str,
) -> DataFrame:
    deterministic = Window.partitionBy("pnu").orderBy(
        *[F.col(column).asc_nulls_first() for column in payload_columns]
    )
    numbered = frame.withColumn("_row_number", F.row_number().over(deterministic))
    duplicate_count = numbered.where(F.col("_row_number") > 1).count()
    counters[counter_key] = int(duplicate_count)
    if duplicate_count > 0 and not allow_duplicates:
        samples = [
            row.pnu
            for row in numbered.where(F.col("_row_number") > 1).select("pnu").limit(5).collect()
        ]
        raise ValueError(
            f"{contract_name} carries {duplicate_count} duplicate PNU rows inside one snapshot "
            f"(samples: {samples}). The Postgres loader breaks these ties with "
            "md5(row(...)), which is not portable; pass --allow-intra-snapshot-duplicates "
            "to collapse them by a deterministic full-row order instead"
        )
    return numbered.where(F.col("_row_number") == 1).drop("_row_number")


def build_characteristics(
    characteristic: DataFrame,
    allow_duplicates: bool,
    counters: dict[str, int],
) -> DataFrame:
    valid = characteristic.where(~F.isnan("area_m2") & (F.col("area_m2") > 0.0))
    counters["characteristic_rows_skipped"] = int(characteristic.count() - valid.count())
    payload = valid.select(
        "pnu",
        "land_category",
        "area_m2",
        "land_use_situation",
        "terrain_height",
        "terrain_shape",
        "road_contact",
    )
    single = one_row_per_pnu(
        payload,
        CHARACTERISTIC_SOURCE,
        ("land_category", "area_m2", "land_use_situation", "terrain_height",
         "terrain_shape", "road_contact"),
        allow_duplicates,
        counters,
        "characteristic_duplicate_rows_dropped",
    )
    return single.select(
        "pnu",
        F.to_json(
            F.struct(
                "land_category",
                "area_m2",
                "land_use_situation",
                "terrain_height",
                "terrain_shape",
                "road_contact",
            )
        ).alias("characteristics_json"),
    )


def build_forest_ledger(
    forest: DataFrame,
    allow_duplicates: bool,
    counters: dict[str, int],
) -> DataFrame:
    valid = forest.where(
        ~F.isnan("area_m2")
        & (F.col("area_m2") > 0.0)
        & (F.col("co_owner_count").isNull() | (F.col("co_owner_count") >= 0))
    )
    counters["forest_rows_skipped"] = int(forest.count() - valid.count())
    payload = valid.select(
        "pnu",
        "land_category",
        "area_m2",
        F.col("ownership_kind_code").alias("ownership_kind"),
        "co_owner_count",
    )
    single = one_row_per_pnu(
        payload,
        FOREST_SOURCE,
        ("land_category", "area_m2", "ownership_kind", "co_owner_count"),
        allow_duplicates,
        counters,
        "forest_duplicate_rows_dropped",
    )
    return single.select(
        "pnu",
        F.to_json(
            F.struct("land_category", "area_m2", "ownership_kind", "co_owner_count")
        ).alias("forest_ledger_json"),
    )


def build_transfer_history(transfer: DataFrame) -> DataFrame:
    missing_parcel_seq = transfer.where(
        F.col("parcel_history_seq").isNull() | (F.length(F.trim(F.col("parcel_history_seq"))) == 0)
    ).count()
    if missing_parcel_seq > 0:
        raise ValueError(
            f"{TRANSFER_SOURCE} carries {missing_parcel_seq} rows without parcel_history_seq; "
            "the Postgres loader refuses these (ADR-0090 three-part identity) and so does "
            "this projection"
        )

    normalized = transfer.select(
        "pnu",
        F.col("transfer_history_seq"),
        F.trim(F.col("parcel_history_seq")).alias("parcel_history_seq"),
        "reason_code",
        "reason",
        "moved_at",
        "erased_at",
        "land_category",
        "area_m2",
        "closure_seq",
    )
    # The loader's DISTINCT ON identity with its NULLS FIRST total order over the payload.
    dedup = Window.partitionBy("pnu", "transfer_history_seq", "parcel_history_seq").orderBy(
        F.col("reason_code").asc_nulls_first(),
        F.col("reason").asc_nulls_first(),
        F.col("moved_at").asc_nulls_first(),
        F.col("erased_at").asc_nulls_first(),
        F.col("land_category").asc_nulls_first(),
        F.col("area_m2").asc_nulls_first(),
        F.col("closure_seq").asc_nulls_first(),
    )
    unique = (
        normalized.withColumn("_row_number", F.row_number().over(dedup))
        .where(F.col("_row_number") == 1)
        .drop("_row_number")
    )
    entries = unique.select(
        "pnu",
        F.struct(
            F.col("reason"),
            F.col("reason_code"),
            F.col("moved_at"),
            F.col("erased_at"),
            F.col("land_category"),
            F.col("area_m2"),
            F.col("transfer_history_seq").alias("history_seq"),
            F.col("parcel_history_seq"),
            F.col("closure_seq"),
        ).alias("entry"),
    )
    # The API's ORDER BY: moved_at DESC NULLS LAST, history_seq DESC, parcel_history_seq
    # DESC — moved_at and parcel_history_seq compare as text, history_seq as a number.
    return entries.groupBy("pnu").agg(
        F.to_json(
            F.expr(
                """
                array_sort(collect_list(entry), (l, r) -> CASE
                    WHEN l.moved_at IS NULL AND r.moved_at IS NOT NULL THEN 1
                    WHEN l.moved_at IS NOT NULL AND r.moved_at IS NULL THEN -1
                    WHEN l.moved_at > r.moved_at THEN -1
                    WHEN l.moved_at < r.moved_at THEN 1
                    WHEN l.history_seq > r.history_seq THEN -1
                    WHEN l.history_seq < r.history_seq THEN 1
                    WHEN l.parcel_history_seq > r.parcel_history_seq THEN -1
                    WHEN l.parcel_history_seq < r.parcel_history_seq THEN 1
                    ELSE 0 END)
                """
            )
        ).alias("transfer_history_json")
    )


def build_land_rights(land_right: DataFrame) -> DataFrame:
    # ADR-0093 key normalization first: the four unit designations are '' when blank, and
    # that empty string participates in both the dedup key and the API's sort.
    normalized = land_right.select(
        "pnu",
        "right_serial_no",
        "building_name",
        F.coalesce(F.col("dong_name"), F.lit("")).alias("dong_name"),
        F.coalesce(F.col("floor_name"), F.lit("")).alias("floor_name"),
        F.coalesce(F.col("ho_name"), F.lit("")).alias("ho_name"),
        F.coalesce(F.col("room_name"), F.lit("")).alias("room_name"),
        "right_ratio",
        F.col("closure_kind_name").alias("closure_kind"),
        "closure_kind_code",
        "source_snapshot_id",
    )
    dedup = Window.partitionBy(
        "pnu", "right_serial_no", "dong_name", "floor_name", "ho_name", "room_name"
    ).orderBy(
        F.col("building_name").asc_nulls_first(),
        F.col("right_ratio").asc_nulls_first(),
        F.col("closure_kind_code").asc_nulls_first(),
        F.col("closure_kind").asc_nulls_first(),
        F.col("source_snapshot_id").asc(),
    )
    unique = (
        normalized.withColumn("_row_number", F.row_number().over(dedup))
        .where(F.col("_row_number") == 1)
        .drop("_row_number", "source_snapshot_id")
    )
    entries = unique.select(
        "pnu",
        F.struct(
            F.col("right_serial_no"),
            F.col("building_name"),
            F.col("dong_name"),
            F.col("floor_name"),
            F.col("ho_name"),
            F.col("room_name"),
            F.col("right_ratio"),
            F.col("closure_kind"),
            F.col("closure_kind_code"),
        ).alias("entry"),
    )
    aggregated = entries.groupBy("pnu").agg(
        F.expr(
            """
            array_sort(collect_list(entry), (l, r) -> CASE
                WHEN l.right_serial_no < r.right_serial_no THEN -1
                WHEN l.right_serial_no > r.right_serial_no THEN 1
                WHEN l.dong_name < r.dong_name THEN -1
                WHEN l.dong_name > r.dong_name THEN 1
                WHEN l.floor_name < r.floor_name THEN -1
                WHEN l.floor_name > r.floor_name THEN 1
                WHEN l.ho_name < r.ho_name THEN -1
                WHEN l.ho_name > r.ho_name THEN 1
                WHEN l.room_name < r.room_name THEN -1
                WHEN l.room_name > r.room_name THEN 1
                ELSE 0 END)
            """
        ).alias("sorted_entries")
    )
    return aggregated.select(
        "pnu",
        F.to_json(F.slice(F.col("sorted_entries"), 1, LAND_RIGHT_PAGE_BOUND)).alias(
            "land_rights_json"
        ),
        F.size(F.col("sorted_entries")).cast(T.LongType()).alias("land_right_total"),
    )


def build_gold_panel_frame(
    parcels: DataFrame,
    sections: dict[str, DataFrame],
    source_snapshot_id: str,
    published_at_utc: str,
) -> DataFrame:
    base = parcels.select("pnu").distinct()
    joined = (
        base.join(sections["zonings"], on="pnu", how="left")
        .join(sections["price"], on="pnu", how="left")
        .join(sections["characteristics"], on="pnu", how="left")
        .join(sections["forest_ledger"], on="pnu", how="left")
        .join(sections["transfer_history"], on="pnu", how="left")
        .join(sections["land_rights"], on="pnu", how="left")
    )
    return joined.select(
        F.col("pnu"),
        # Postgres-curated (ADR-0070) and permanently absent (ADR-0020) respectively; the
        # Gold contract carries both so a future merge path changes data, not schema.
        F.lit(None).cast(T.StringType()).alias("kind"),
        F.lit(None).cast(T.LongType()).alias("area_m2"),
        F.coalesce(F.col("zonings_json"), F.lit("[]")).alias("zonings_json"),
        F.col("price_json"),
        F.col("characteristics_json"),
        F.col("forest_ledger_json"),
        F.coalesce(F.col("transfer_history_json"), F.lit("[]")).alias("transfer_history_json"),
        F.coalesce(F.col("land_rights_json"), F.lit("[]")).alias("land_rights_json"),
        F.coalesce(F.col("land_right_total"), F.lit(0)).cast(T.LongType()).alias(
            "land_right_total"
        ),
        F.lit(source_snapshot_id).alias("source_snapshot_id"),
        F.lit(published_at_utc).alias("published_at_utc"),
    ).select(*GOLD_COLUMNS)


def assert_columns(frame: DataFrame, expected_columns: tuple[str, ...]) -> None:
    actual_columns = tuple(frame.columns)
    if actual_columns != expected_columns:
        raise ValueError(
            "Unexpected Gold columns. "
            f"expected={list(expected_columns)} actual={list(actual_columns)}"
        )


def invalid_count(predicate: F.Column, alias: str) -> F.Column:
    return F.sum(F.when(predicate, F.lit(1)).otherwise(F.lit(0))).cast("long").alias(alias)


def collect_quality_metrics(gold: DataFrame) -> dict[str, int]:
    expressions: list[F.Column] = [F.count(F.lit(1)).cast("long").alias("row_count")]
    for column in REQUIRED_GOLD_COLUMNS:
        expressions.append(invalid_count(F.col(column).isNull(), f"{column}__null_count"))
    expressions.extend(
        (
            invalid_count(~F.col("pnu").rlike(PNU_PATTERN), "invalid_pnu_count"),
            invalid_count(F.col("land_right_total") < 0, "invalid_land_right_total_count"),
            invalid_count(
                F.length(F.col("published_at_utc")) == 0, "invalid_published_at_count"
            ),
        )
    )
    row = gold.agg(*expressions).first()
    if row is None:
        raise ValueError("Gold quality metric aggregation returned no row")
    return {key: int(value or 0) for key, value in row.asDict().items()}


def assert_quality_metrics(gold: DataFrame, metrics: dict[str, int]) -> None:
    failures = []
    for column in REQUIRED_GOLD_COLUMNS:
        if metrics[f"{column}__null_count"] > 0:
            failures.append(f"{column} must not be null ({metrics[f'{column}__null_count']})")
    if metrics["invalid_pnu_count"] > 0:
        failures.append(f"pnu must match the cadastral grammar ({metrics['invalid_pnu_count']})")
    if metrics["invalid_land_right_total_count"] > 0:
        failures.append("land_right_total must be non-negative")
    if metrics["invalid_published_at_count"] > 0:
        failures.append("published_at_utc must be present")
    if failures:
        samples = [
            str(sample)
            for sample in gold.where(~F.col("pnu").rlike(PNU_PATTERN)).limit(3).toJSON().collect()
        ]
        raise ValueError(f"Gold quality gates failed: {failures}; pnu samples={samples}")

    duplicate_keys = (
        gold.groupBy("pnu").count().where(F.col("count") > 1).limit(5).toJSON().collect()
    )
    if duplicate_keys:
        raise ValueError(f"Gold projection must contain one row per pnu: {duplicate_keys}")


def validate_gold_frame(gold: DataFrame, expected_count: int | None) -> tuple[int, dict[str, int]]:
    assert_columns(gold, GOLD_COLUMNS)
    metrics = collect_quality_metrics(gold)
    assert_quality_metrics(gold, metrics)
    actual_count = metrics["row_count"]
    if expected_count is not None and actual_count != expected_count:
        raise ValueError(f"Expected {expected_count} Gold rows, found {actual_count}")
    return actual_count, metrics


def json_payload(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def emit_run_summary(summary: dict[str, Any], output_path: str | None) -> None:
    payload = json_payload(summary)
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{payload}\n", encoding="utf-8")
    print(f"gold-parcel-panel-summary-json {payload}")


def build_run_summary(
    args: argparse.Namespace,
    row_count: int,
    persisted_row_count: int | None,
    quality_metrics: dict[str, int],
    section_counters: dict[str, int],
    source_snapshots: dict[str, str],
    schema_evolution_added_columns: tuple[str, ...] = (),
) -> dict[str, Any]:
    metrics = dict(quality_metrics)
    metrics.update(section_counters)
    if persisted_row_count is not None:
        metrics["persisted_row_count"] = int(persisted_row_count)
    return {
        "schema_version": RUN_SUMMARY_SCHEMA_VERSION,
        "job_name": JOB_NAME,
        "contract": GOLD_CONTRACT_NAME,
        "created_at_utc": datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "input": {
            "kind": args.input_mode,
            "sources": list(ALL_SOURCES),
            "region_prefix": args.region_prefix,
        },
        "target": (
            {"kind": "parquet", "path": args.output}
            if args.write_mode == "parquet"
            else {
                "kind": "iceberg",
                "catalog": args.iceberg_catalog_name,
                "namespace": args.target_iceberg_namespace,
                "table": args.target_iceberg_table,
            }
        ),
        "write_mode": args.write_mode,
        "write_disposition": (
            "validate_only"
            if args.validate_only
            else (
                "parquet_overwrite"
                if args.write_mode == "parquet"
                else f"iceberg_{args.iceberg_write_mode}"
            )
        ),
        "row_count": row_count,
        "persisted_row_count": persisted_row_count,
        "quality_metrics": metrics,
        "column_count": len(GOLD_COLUMNS),
        "columns": list(GOLD_COLUMNS),
        "required_columns": list(REQUIRED_GOLD_COLUMNS),
        "schema_evolution_added_columns": list(schema_evolution_added_columns),
        "source_snapshot_count": len(source_snapshots),
        "source_snapshot_ids": [source_snapshots[name] for name in sorted(source_snapshots)],
        "source_snapshots_by_dataset": dict(sorted(source_snapshots.items())),
        "source_snapshot_truncated": False,
    }


def build_lineage_event(args: argparse.Namespace, summary: dict[str, Any]) -> dict[str, Any]:
    payload = json_payload(summary)
    return {
        "schema_version": LINEAGE_EVENT_SCHEMA_VERSION,
        "event_type": LINEAGE_EVENT_TYPE,
        "occurred_at": summary["created_at_utc"],
        "producer": "foundation-platform.lakehouse",
        "job_name": JOB_NAME,
        "run_id": f"{JOB_NAME}:{summary['created_at_utc']}",
        "run_summary_schema_version": RUN_SUMMARY_SCHEMA_VERSION,
        "run_summary_ref": {
            "path": args.summary_output or "stdout",
            "checksum_sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        },
        "input_dataset": {
            "qualified_name": PARCEL_SOURCE,
            "namespace": "silver",
            "table": source_table_name(PARCEL_SOURCE),
            "storage_format": args.input_mode,
        },
        "additional_input_datasets": [
            {
                "qualified_name": name,
                "namespace": "silver",
                "table": source_table_name(name),
                "storage_format": args.input_mode,
            }
            for name in (*ATTRIBUTE_SOURCES, ZONE_CODE_SOURCE)
        ],
        "output_dataset": {
            "qualified_name": GOLD_CONTRACT_NAME,
            "namespace": "gold",
            "table": "parcel_panel",
            "storage_format": args.write_mode,
        },
        "source_snapshot_ids": summary["source_snapshot_ids"],
        "source_snapshot_truncated": False,
        "iceberg_snapshot": {
            "catalog": args.iceberg_catalog_name,
            "namespace": args.target_iceberg_namespace,
            "table": args.target_iceberg_table,
            "snapshot_id": args.iceberg_snapshot_id,
        },
        "quality_metrics": {
            **summary["quality_metrics"],
            "row_count": int(summary["row_count"]),
        },
        "column_lineage": column_lineage(),
        "openlineage_mapping": {
            "event_type": "COMPLETE",
            "job_namespace": "foundation-platform.lakehouse",
            "job_name": JOB_NAME,
            "input_namespace": f"{args.iceberg_catalog_name}.silver",
            "output_namespace": f"{args.iceberg_catalog_name}.gold",
        },
    }


def column_lineage() -> list[dict[str, Any]]:
    def one(dataset: str, column: str, transform: str) -> list[dict[str, str]]:
        return [{"dataset": dataset, "column": column, "transform": transform}]

    lineage_map: dict[str, list[dict[str, str]]] = {
        "pnu": one(PARCEL_SOURCE, "pnu", "identity"),
        "kind": one("foundation-platform.job_arguments", "null", "literal_null"),
        "area_m2": one("foundation-platform.job_arguments", "null", "literal_null"),
        "zonings_json": one(ZONING_SOURCE, "zone_code,zone_name,inclusion_code", "loader_semantics_json")
        + one(ZONE_CODE_SOURCE, "ucode,parent_ucode", "anchor_walk"),
        "price_json": one(
            PRICE_SOURCE, "base_year,base_month,price_per_m2,announced_date", "newest_assessment_json"
        ),
        "characteristics_json": one(
            CHARACTERISTIC_SOURCE,
            "land_category,area_m2,land_use_situation,terrain_height,terrain_shape,road_contact",
            "single_row_json",
        ),
        "forest_ledger_json": one(
            FOREST_SOURCE, "land_category,area_m2,ownership_kind_code,co_owner_count", "single_row_json"
        ),
        "transfer_history_json": one(
            TRANSFER_SOURCE,
            "transfer_history_seq,parcel_history_seq,reason,reason_code,moved_at,erased_at,land_category,area_m2,closure_seq",
            "timeline_json",
        ),
        "land_rights_json": one(
            LAND_RIGHT_SOURCE,
            "right_serial_no,building_name,dong_name,floor_name,ho_name,room_name,right_ratio,closure_kind_name,closure_kind_code",
            "page_bounded_json",
        ),
        "land_right_total": one(LAND_RIGHT_SOURCE, "right_serial_no", "count_full_set"),
        "source_snapshot_id": one(PARCEL_SOURCE, "source_snapshot_id", "identity"),
        "published_at_utc": one(
            "foundation-platform.job_arguments", "published_at_utc", "literal"
        ),
    }
    return [
        {"output_column": column, "inputs": lineage_map[column]} for column in GOLD_COLUMNS
    ]


def emit_lineage_event(event: dict[str, Any], output_path: str | None) -> None:
    if not output_path:
        return
    payload = json_payload(event)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{payload}\n", encoding="utf-8")
    print(f"gold-parcel-panel-lineage-json {payload}")


def write_gold_parquet(gold: DataFrame, output_path: str) -> None:
    frame = gold.repartition(*[F.col(column) for column in GOLD_PARTITION_COLUMNS])
    frame = frame.sortWithinPartitions(*GOLD_SORT_COLUMNS)
    frame.write.mode("overwrite").partitionBy(*GOLD_PARTITION_COLUMNS).parquet(output_path)


def qualified_target_table(args: argparse.Namespace) -> str:
    return (
        f"`{args.iceberg_catalog_name}`."
        f"`{args.target_iceberg_namespace}`.`{args.target_iceberg_table}`"
    )


def write_gold_iceberg(
    spark: SparkSession, gold: DataFrame, args: argparse.Namespace
) -> tuple[str, ...]:
    table = qualified_target_table(args)
    namespace = f"`{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`"
    temp_view = "gold_parcel_panel_candidate"

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
{create_table_columns_sql(GOLD_CONTRACT)}
        )
        USING iceberg
        {partition_clause_sql(GOLD_CONTRACT)}
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'zstd',
            'write.distribution-mode' = 'hash'
        )
        """
    )
    added_columns = evolve_iceberg_table_to_contract(spark, table, GOLD_CONTRACT)
    gold.select(*GOLD_COLUMNS).createOrReplaceTempView(temp_view)
    statement = "INSERT OVERWRITE" if args.iceberg_write_mode == "overwrite" else "INSERT INTO"
    spark.sql(f"{statement} {table} SELECT {', '.join(GOLD_COLUMNS)} FROM {temp_view}")
    return added_columns


def build_spark_session(args: argparse.Namespace) -> SparkSession:
    builder = (
        SparkSession.builder.appName("foundation-platform-parcel-panel-silver-to-gold")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if args.input_mode == "iceberg" or args.write_mode == "iceberg":
        builder = apply_catalog_settings(builder, args.iceberg_catalog_name)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if args.input_mode == "iceberg" or args.write_mode == "iceberg":
        assert_iceberg_runtime_loaded(spark, args.iceberg_packages)
    return spark


def main() -> int:
    args = parse_args()
    args.published_at_utc = normalize_utc_timestamp(args.published_at_utc)
    validate_args(args)
    spark = build_spark_session(args)

    try:
        counters: dict[str, int] = {}
        source_snapshots: dict[str, str] = {}

        parcel_predicate = current_row_predicate(load_lakehouse_contract(PARCEL_SOURCE))
        if parcel_predicate is None:
            raise ValueError(
                f"{PARCEL_SOURCE} declares no current_row_predicate; this projection cannot "
                "say which boundary row is the parcel's current one"
            )
        parcels = filtered_by_region(
            read_source(spark, args, PARCEL_SOURCE).where(F.expr(parcel_predicate)),
            args.region_prefix,
        )
        source_snapshots[PARCEL_SOURCE] = assert_single_snapshot(parcels, PARCEL_SOURCE)

        frames: dict[str, DataFrame] = {}
        for name in ATTRIBUTE_SOURCES:
            frame = filtered_by_region(read_source(spark, args, name), args.region_prefix)
            source_snapshots[name] = assert_single_snapshot(frame, name)
            frames[name] = frame

        zone_codes = read_source(spark, args, ZONE_CODE_SOURCE)
        source_snapshots[ZONE_CODE_SOURCE] = assert_single_snapshot(
            zone_codes, ZONE_CODE_SOURCE
        )
        anchors = resolve_zoning_anchors(zone_codes)

        sections = {
            "zonings": build_zonings(frames[ZONING_SOURCE], anchors, spark, counters),
            "price": build_price(frames[PRICE_SOURCE], counters),
            "characteristics": build_characteristics(
                frames[CHARACTERISTIC_SOURCE], args.allow_intra_snapshot_duplicates, counters
            ),
            "forest_ledger": build_forest_ledger(
                frames[FOREST_SOURCE], args.allow_intra_snapshot_duplicates, counters
            ),
            "transfer_history": build_transfer_history(frames[TRANSFER_SOURCE]),
            "land_rights": build_land_rights(frames[LAND_RIGHT_SOURCE]),
        }

        gold = build_gold_panel_frame(
            parcels,
            sections,
            source_snapshot_id=source_snapshots[PARCEL_SOURCE],
            published_at_utc=args.published_at_utc,
        ).persist(StorageLevel.MEMORY_AND_DISK)
        row_count, quality_metrics = validate_gold_frame(gold, args.expected_count)

        if args.validate_only:
            emit_run_summary(
                build_run_summary(
                    args,
                    row_count=row_count,
                    persisted_row_count=None,
                    quality_metrics=quality_metrics,
                    section_counters=counters,
                    source_snapshots=source_snapshots,
                ),
                args.summary_output,
            )
            print(f"gold-parcel-panel-validate-ok rows={row_count}")
            return 0

        added_columns: tuple[str, ...] = ()
        if args.write_mode == "parquet":
            write_gold_parquet(gold, args.output)
            persisted = spark.read.parquet(args.output).select(*GOLD_COLUMNS)
            success_target = f"output={args.output}"
        else:
            added_columns = write_gold_iceberg(spark, gold, args)
            persisted = (
                spark.table(qualified_target_table(args))
                .where(F.col("source_snapshot_id") == source_snapshots[PARCEL_SOURCE])
                .select(*GOLD_COLUMNS)
            )
            success_target = f"table={args.target_iceberg_namespace}.{args.target_iceberg_table}"

        persisted_count, persisted_quality_metrics = validate_gold_frame(
            persisted, args.expected_count
        )
        if persisted_count != row_count:
            raise ValueError(
                f"Persisted row count changed. before={row_count} after={persisted_count}"
            )

        summary = build_run_summary(
            args,
            row_count=row_count,
            persisted_row_count=persisted_count,
            quality_metrics=persisted_quality_metrics,
            section_counters=counters,
            source_snapshots=source_snapshots,
            schema_evolution_added_columns=added_columns,
        )
        emit_run_summary(summary, args.summary_output)
        emit_lineage_event(build_lineage_event(args, summary), args.lineage_output)
        print(f"gold-parcel-panel-write-ok rows={persisted_count} {success_target}")
        return 0
    finally:
        if "gold" in locals():
            gold.unpersist()
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
