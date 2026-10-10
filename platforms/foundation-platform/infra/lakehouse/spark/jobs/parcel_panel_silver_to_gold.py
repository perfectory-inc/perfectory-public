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

from lineage_review_queue import steward_resolved
from lakehouse_snapshot_pins import load_source_snapshot_pins, read_pinned_iceberg
from gold_rebuild import assert_minimum_row_count
import gold_incremental as incremental
from parcel_attribute_carry import carry_candidates
from parcel_lineage import Link
import vworld_parcel_editions as parcel_editions
from lakehouse_engine import (
    apply_catalog_settings,
    assert_catalog_env,
    assert_iceberg_runtime_loaded,
    iceberg_packages,
)
from platform_contracts import (
    column_names,
    current_row_predicate,
    ensure_contract_table,
    load_lakehouse_contract,
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

# The columns `row_digest` fingerprints (root ADR-0099): everything the serving document is
# built from, and nothing about when or from which snapshot it was built. Derived by
# subtraction from the contract so a content column added later cannot silently stay out of
# the fingerprint — the delta pipeline would then miss its changes.
LINEAGE_COLUMNS = ("row_digest", "source_snapshot_id", "published_at_utc")
CONTENT_DIGEST_COLUMNS: tuple[str, ...] = tuple(
    column for column in GOLD_COLUMNS if column not in LINEAGE_COLUMNS
)
# Content columns the fingerprint takes only when they hold a value. `concat_ws` skips a NULL,
# so a row whose every section is its own keeps the digest it had before this column existed —
# widening the contract must not mark forty million rows as changed (root ADR-0113 §6).
NULL_SKIPPED_DIGEST_COLUMNS: tuple[str, ...] = ("attached_via_json",)

PARCEL_SOURCE = "silver.parcel_boundaries"
ZONING_SOURCE = "silver.land_use_plan"
ZONE_CODE_SOURCE = "silver.land_use_zone_code"
PRICE_SOURCE = "silver.land_individual_price"
CHARACTERISTIC_SOURCE = "silver.land_characteristic"
FOREST_SOURCE = "silver.land_forest_ledger"
TRANSFER_SOURCE = "silver.land_transfer_history"
LAND_RIGHT_SOURCE = "silver.land_right_registration"
LINEAGE_SOURCE = "silver.parcel_lineage"
ATTRIBUTE_SOURCES = (
    ZONING_SOURCE,
    PRICE_SOURCE,
    CHARACTERISTIC_SOURCE,
    FOREST_SOURCE,
    TRANSFER_SOURCE,
    LAND_RIGHT_SOURCE,
)
ALL_SOURCES = (PARCEL_SOURCE, *ATTRIBUTE_SOURCES, ZONE_CODE_SOURCE)
# The columns build_panel reads from each input, and the only ones it is handed: the incremental
# rebuild compares exactly these between two Silver snapshots (root ADR-0180 §1), so a column the
# build reads must be here or the build fails on it.
BUILD_COLUMNS: dict[str, tuple[str, ...]] = {
    PARCEL_SOURCE: ("pnu",),
    ZONING_SOURCE: ("pnu", "inclusion_code", "zone_code", "zone_name"),
    PRICE_SOURCE: ("pnu", "base_year", "base_month", "price_per_m2", "announced_date"),
    CHARACTERISTIC_SOURCE: ("pnu", "area_m2", "land_category", "land_use_situation",
                            "terrain_height", "terrain_shape", "road_contact"),
    FOREST_SOURCE: ("pnu", "area_m2", "co_owner_count", "land_category", "ownership_kind_code"),
    TRANSFER_SOURCE: ("pnu", "transfer_history_seq", "parcel_history_seq", "reason_code", "reason",
                      "moved_at", "erased_at", "land_category", "area_m2", "closure_seq"),
    LAND_RIGHT_SOURCE: ("pnu", "right_serial_no", "building_name", "dong_name", "floor_name",
                        "ho_name", "room_name", "right_ratio", "closure_kind_name", "closure_kind_code"),
    ZONE_CODE_SOURCE: ("ucode", "parent_ucode"),
}
# Inputs whose change the incremental rebuild cannot map to PNUs: a zone code's anchor reaches
# every parcel zoned under it. A change to one rebuilds whole (root ADR-0180).
WHOLE_TABLE_INPUTS: tuple[str, ...] = (ZONE_CODE_SOURCE,)

# parcel_zoning_catalog_projection_load.rs anchor_for: the resolved endpoints of the
# parent_ucode walk, plus the urban root that stands for itself.
ZONING_ANCHORS = frozenset(
    ("UQA100", "UQA200", "UQA300", "UQA400", "UQB001", "UQC001", "UQD001")
)
ZONING_URBAN_ROOT = "UQA001"
ZONING_ANCHOR_MAX_DEPTH = 16
LAND_RIGHT_PAGE_BOUND = 200


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
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
        "--source-snapshots-path",
        help="JSON file pinning every input table, including parcel_lineage when carry is enabled.",
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
        "--minimum-count",
        type=int,
        default=None,
        help="Refuse, before writing, a Gold with fewer rows (root ADR-0139).",
    )
    parser.add_argument(
        "--summary-output",
        help="Optional path for a machine-readable Spark run summary JSON file.",
    )
    parser.add_argument(
        "--no-carry-lineage",
        dest="carry_lineage",
        action="store_false",
        help=(
            "Do not attach attributes held under an older PNU through silver.parcel_lineage "
            "(root ADR-0113 §6). Only for inputs that carry no lineage table."
        ),
    )
    parser.add_argument(
        "--lineage-output",
        help="Optional path for a machine-readable lakehouse lineage event JSON file.",
    )
    incremental.add_arguments(parser)
    return parser.parse_args(argv)


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


def validate_args(args: argparse.Namespace) -> dict[str, str]:
    if args.input_mode == "parquet" and not args.input_root:
        raise ValueError("--input-root is required when --input-mode=parquet")
    if args.write_mode == "parquet" and not args.output:
        raise ValueError("--output is required when --write-mode=parquet")
    if args.region_prefix is not None and re.fullmatch(r"[0-9]{1,10}", args.region_prefix) is None:
        raise ValueError("--region-prefix must be 1 to 10 digits")
    if args.minimum_count is not None and args.minimum_count < 0:
        raise ValueError("--minimum-count must be non-negative")

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
    incremental.validate_arguments(args, input_sources(args))
    return load_source_snapshot_pins(args.input_mode, args.source_snapshots_path,
        input_sources(args), PARCEL_SOURCE, args.iceberg_snapshot_id)


def input_sources(args: argparse.Namespace) -> tuple[str, ...]:
    return (*ALL_SOURCES, *((LINEAGE_SOURCE,) if args.carry_lineage else ()))


def source_table_name(contract_name: str) -> str:
    return contract_name.split(".", maxsplit=1)[1]


def read_source(
    spark: SparkSession, args: argparse.Namespace, contract_name: str, pins: dict[str, str]
) -> DataFrame:
    contract = load_lakehouse_contract(contract_name)
    expected_columns = column_names(contract)
    table = source_table_name(contract_name)
    if args.input_mode == "parquet":
        frame = spark.read.parquet(str(Path(args.input_root) / table))
    else:
        frame = read_pinned_iceberg(spark,
            f"{args.iceberg_catalog_name}.{args.source_iceberg_namespace}.{table}", contract_name, pins)
    missing = sorted(set(expected_columns) - set(frame.columns))
    if missing:
        raise ValueError(f"{contract_name} input is missing columns: {', '.join(missing)}")
    return frame.select(*expected_columns)


def filtered_by_region(frame: DataFrame, region_prefix: str | None) -> DataFrame:
    if region_prefix is None:
        return frame
    return frame.where(F.col("pnu").startswith(region_prefix))


def read_served_parcels(
    spark: SparkSession, args: argparse.Namespace, pins: dict[str, str], source: dict[str, Any] | None = None
) -> DataFrame:
    """The parcels the panel is built from: the current rows of the served edition only.

    Root ADR-0148: `silver.parcel_boundaries` holds several editions side by side, every one of
    them current by the predicate, and the panel is built from the one the map is (`served_edition`
    of the source contract, `source` here or the file). Without the edition filter the
    single-snapshot check refuses the table once a second edition is loaded, and every scheduled
    rebuild fails.
    """

    predicate = current_row_predicate(load_lakehouse_contract(PARCEL_SOURCE))
    if predicate is None:
        raise ValueError(
            f"{PARCEL_SOURCE} declares no current_row_predicate; this projection cannot "
            "say which boundary row is the parcel's current one"
        )
    source = source or parcel_editions.load()
    served_id = parcel_editions.snapshot_id(source, parcel_editions.served(source))
    return filtered_by_region(
        read_source(spark, args, PARCEL_SOURCE, pins)
        .where(F.expr(predicate))
        .where(F.expr(f"source_snapshot_id = '{served_id}'")),
        args.region_prefix,
    )


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
    )
    dedup = Window.partitionBy(
        "pnu", "right_serial_no", "dong_name", "floor_name", "ho_name", "room_name"
    ).orderBy(
        F.col("building_name").asc_nulls_first(),
        F.col("right_ratio").asc_nulls_first(),
        F.col("closure_kind_code").asc_nulls_first(),
        F.col("closure_kind").asc_nulls_first(),
    )
    unique = (
        normalized.withColumn("_row_number", F.row_number().over(dedup))
        .where(F.col("_row_number") == 1)
        .drop("_row_number")
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


SECTION_VIA_COLUMNS: dict[str, str] = {
    "zonings": "zonings_via",
    "price": "price_via",
    "characteristics": "characteristics_via",
    "forest_ledger": "forest_ledger_via",
    "transfer_history": "transfer_history_via",
    "land_rights": "land_rights_via",
}


def read_carry_candidates(
    spark: SparkSession, args: argparse.Namespace, counters: dict[str, int], pins: dict[str, str]
) -> DataFrame | None:
    """The lineage's candidate sources per successor PNU, as a small frame (root ADR-0113 §6).

    Lineage rows are few next to the panel (re-lotted parcels, not all parcels), so the chain walk
    runs on the driver through the same function the tests pin, and the result is broadcast.
    """

    if not args.carry_lineage:
        return None
    lineage = read_source(spark, args, LINEAGE_SOURCE, pins)
    if args.region_prefix is not None:
        lineage = lineage.where(F.col("successor_pnu").startswith(args.region_prefix))
    # A steward's standing decision replaces the derived rows of its parcel (root ADR-0115 §9).
    rows = [
        row.asDict()
        for row in lineage.select(
            "predecessor_pnu", "successor_pnu", "relation", "grade", "evidence_kind", "evidence_ref"
        )
        .distinct()
        .collect()
    ]
    links = [
        Link(row["predecessor_pnu"] or "", row["successor_pnu"], row["relation"], row["grade"], "")
        for row in steward_resolved(rows)
    ]
    candidates, stopped = carry_candidates(links)
    counters["lineage_links"] = len(links)
    counters["lineage_candidates"] = len(candidates)
    for reason, count in sorted(stopped.items()):
        counters[f"lineage_stopped_{reason}"] = count
    schema = "successor_pnu string, source_pnu string, hop int, path string"
    rows = [(c.successor_pnu, c.source_pnu, c.hop, c.path) for c in candidates]
    return F.broadcast(spark.createDataFrame(rows, schema=schema))


def with_lineage_sources(
    frame: DataFrame, region_prefix: str | None, candidates: DataFrame | None
) -> DataFrame:
    """The region's rows plus every row a candidate points at — a predecessor may sit under
    another region's prefix (a sido merger moves every PNU out of its old sido prefix)."""

    in_region = filtered_by_region(frame, region_prefix)
    if region_prefix is None or candidates is None:
        return in_region
    sources = candidates.select(F.col("source_pnu").alias("pnu")).distinct()
    elsewhere = frame.where(~F.col("pnu").startswith(region_prefix)).join(
        F.broadcast(sources), on="pnu", how="left_semi"
    )
    return in_region.unionByName(elsewhere)


def carry_section(section: DataFrame, via_column: str, candidates: DataFrame | None) -> DataFrame:
    """The section under its own PNU, plus — for a PNU without one — the nearest lineage source's.

    Same semantics as `parcel_attribute_carry.attach`: the own value wins, then the lowest hop.
    """

    own = section.withColumn(via_column, F.lit(None).cast(T.StringType()))
    if candidates is None:
        return own
    value_columns = [column for column in section.columns if column != "pnu"]
    nearest = Window.partitionBy("successor_pnu").orderBy(F.col("hop").asc())
    carried = (
        candidates.join(section.withColumnRenamed("pnu", "source_pnu"), on="source_pnu")
        .withColumn("_carry_rank", F.row_number().over(nearest))
        .where(F.col("_carry_rank") == 1)
        .select(
            F.col("successor_pnu").alias("pnu"),
            *value_columns,
            F.col("path").alias(via_column),
        )
        .join(section.select("pnu"), on="pnu", how="left_anti")
    )
    return own.unionByName(carried)


def build_gold_panel_frame(
    parcels: DataFrame,
    sections: dict[str, DataFrame],
    source_snapshot_id: str,
    published_at_utc: str,
) -> DataFrame:
    base = parcels.select("pnu").distinct()
    sections = {
        name: frame
        if SECTION_VIA_COLUMNS[name] in frame.columns
        else frame.withColumn(SECTION_VIA_COLUMNS[name], F.lit(None).cast(T.StringType()))
        for name, frame in sections.items()
    }
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
        attached_via_json(),
        F.lit(source_snapshot_id).alias("source_snapshot_id"),
        F.lit(published_at_utc).alias("published_at_utc"),
    ).withColumn("row_digest", row_digest_column()).select(*GOLD_COLUMNS)


def row_digest_column() -> F.Column:
    """내용 칼럼만의 결정적 지문 (root ADR-0099).

    계보(source_snapshot_id·published_at_utc)는 넣지 않는다 — 계보만 바뀐 행을 다시 굽지 않는
    것이 델타 파이프라인의 요점이다. NULL 은 어떤 실제 값과도 겹치지 않는 문지기 문자열로 고정해
    해시를 결정적으로 만든다. lakehouse_schema_migrate.py 의 등록 백필도 이 함수를 쓰므로, 백필한
    지문과 다시 만든 지문은 같은 칼럼의 같은 함수다.
    """

    return F.sha2(
        F.concat_ws(
            "\x1f",
            *(
                F.col(column).cast(T.StringType())
                if column in NULL_SKIPPED_DIGEST_COLUMNS
                else F.coalesce(F.col(column).cast(T.StringType()), F.lit("\x00null"))
                for column in CONTENT_DIGEST_COLUMNS
            ),
        ),
        256,
    )


def attached_via_json() -> F.Column:
    """{section: path} for the sections that came through the lineage; NULL when none did."""

    paths = F.to_json(
        F.struct(*(F.col(via).alias(name) for name, via in SECTION_VIA_COLUMNS.items()))
    )
    return F.when(paths == F.lit("{}"), F.lit(None).cast(T.StringType())).otherwise(paths).alias(
        "attached_via_json"
    )


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
    source_iceberg_snapshots: dict[str, str] | None = None,
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
            "sources": list(input_sources(args)),
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
        "source_iceberg_snapshots_by_dataset": dict(sorted((source_iceberg_snapshots or {}).items())),
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
            for name in summary["input"]["sources"] if name != PARCEL_SOURCE
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
        "column_lineage": column_lineage(args.carry_lineage),
        "openlineage_mapping": {
            "event_type": "COMPLETE",
            "job_namespace": "foundation-platform.lakehouse",
            "job_name": JOB_NAME,
            "input_namespace": f"{args.iceberg_catalog_name}.silver",
            "output_namespace": f"{args.iceberg_catalog_name}.gold",
        },
    }


def column_lineage(carry_lineage: bool = True) -> list[dict[str, Any]]:
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
    # A path exists only when a section has no own value and an eligible predecessor holds it.
    # Reuse section dependencies: filtering a source row can change that presence decision.
    lineage_map["attached_via_json"] = (
        one(
            LINEAGE_SOURCE,
            "predecessor_pnu,successor_pnu,relation,grade,evidence_kind,evidence_ref",
            "steward_resolved_nearest_holder_path",
        )
        + one(PARCEL_SOURCE, "pnu", "panel_membership")
        + [source for dataset in ATTRIBUTE_SOURCES
           for source in one(dataset, "pnu", "own_or_predecessor_section_presence")]
        + [{**source, "transform": f"section_presence:{source['transform']}"}
           for section in SECTION_VIA_COLUMNS for source in lineage_map[f"{section}_json"]]
        if carry_lineage
        else one("foundation-platform.job_arguments", "carry_lineage", "literal_null_when_disabled")
    )
    # Check the contract before deriving the digest, so new content columns fail with their
    # names here instead of a KeyError when a completion event is built after the Gold write.
    mapped = set(lineage_map) | {"row_digest"}
    missing, extra = sorted(set(GOLD_COLUMNS) - mapped), sorted(mapped - set(GOLD_COLUMNS))
    if missing or extra:
        raise ValueError(f"Gold column lineage does not match the contract: missing={missing} extra={extra}")
    # Trace the hash through the exact content inputs used by build_gold_panel_frame. Never
    # point it back at its own Gold row or maintain another list of fingerprint columns.
    lineage_map["row_digest"] = [
        {**source, "transform": f"sha256_content:{source['transform']}"}
        for column in CONTENT_DIGEST_COLUMNS for source in lineage_map[column]
    ]
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


def read_inputs(
    spark: SparkSession, args: argparse.Namespace, pins: dict[str, str], counters: dict[str, int],
    source_snapshots: dict[str, str] | None = None,
) -> tuple[dict[str, DataFrame], DataFrame | None]:
    """Every input at `pins` reduced to BUILD_COLUMNS, and the lineage's carry candidates.

    `source_snapshots`, when given, collects each input's single batch id (the build's refusal of
    multi-vintage input). The incremental rebuild reads its comparison snapshot without it.
    """

    frames = {PARCEL_SOURCE: read_served_parcels(spark, args, pins)}
    candidates = read_carry_candidates(spark, args, counters, pins)
    for name in ATTRIBUTE_SOURCES:
        frames[name] = with_lineage_sources(
            read_source(spark, args, name, pins), args.region_prefix, candidates
        )
    frames[ZONE_CODE_SOURCE] = read_source(spark, args, ZONE_CODE_SOURCE, pins)
    if source_snapshots is not None:
        for name, frame in frames.items():
            source_snapshots[name] = assert_single_snapshot(frame, name)
    return incremental.project_build_inputs(frames, BUILD_COLUMNS), candidates


def build_panel(
    spark: SparkSession, args: argparse.Namespace, frames: dict[str, DataFrame],
    candidates: DataFrame | None, counters: dict[str, int], source_snapshot_id: str,
) -> DataFrame:
    """The Gold rows of `frames` — every input row (full) or the changed PNUs' rows (ADR-0180)."""

    anchors = resolve_zoning_anchors(frames[ZONE_CODE_SOURCE])
    duplicates = args.allow_intra_snapshot_duplicates
    built = {
        "zonings": build_zonings(frames[ZONING_SOURCE], anchors, spark, counters),
        "price": build_price(frames[PRICE_SOURCE], counters),
        "characteristics": build_characteristics(frames[CHARACTERISTIC_SOURCE], duplicates, counters),
        "forest_ledger": build_forest_ledger(frames[FOREST_SOURCE], duplicates, counters),
        "transfer_history": build_transfer_history(frames[TRANSFER_SOURCE]),
        "land_rights": build_land_rights(frames[LAND_RIGHT_SOURCE]),
    }
    sections = {
        name: carry_section(frame, SECTION_VIA_COLUMNS[name], candidates)
        for name, frame in built.items()
    }
    return build_gold_panel_frame(
        frames[PARCEL_SOURCE], sections, source_snapshot_id, args.published_at_utc
    )


def affected_pnus(
    old: dict[str, DataFrame], old_candidates: DataFrame | None,
    new: dict[str, DataFrame], new_candidates: DataFrame | None, changed: list[str],
) -> DataFrame:
    """Every PNU whose Gold row can read a row that differs between `old` and `new` (ADR-0180 §2).

    Every section is keyed by its own PNU. Through the lineage, a successor also reads its
    predecessors' sections, so a changed predecessor reaches it; a changed lineage reaches the
    successors whose candidates differ.
    """

    keyed = [
        incremental.trimmed_keys(incremental.changed_rows(old[name], new[name]), trim=False)
        for name in changed if name in new
    ]
    candidates = [c for c in (old_candidates, new_candidates) if c is not None]
    if keyed and candidates:
        direct = incremental.union_keys(keyed)
        every = candidates[0] if len(candidates) == 1 else candidates[0].unionByName(candidates[1])
        sources = direct.select(F.col("pnu").alias("source_pnu"))
        keyed.append(every.join(sources, "source_pnu", "left_semi").select(F.col("successor_pnu").alias("pnu")))
    if LINEAGE_SOURCE in changed and len(candidates) == 2:
        moved = incremental.changed_rows(*candidates)
        keyed.append(moved.select(F.col("successor_pnu").alias("pnu")))
    if not keyed:
        return new[PARCEL_SOURCE].select("pnu").limit(0)
    return incremental.union_keys(keyed)


def restrict_inputs(
    frames: dict[str, DataFrame], keys: DataFrame, candidates: DataFrame | None
) -> dict[str, DataFrame]:
    """The input rows the Gold rows of `keys` are built from: their own and their lineage sources'."""

    sources = keys
    if candidates is not None:
        successors = keys.select(F.col("pnu").alias("successor_pnu"))
        sources = incremental.union_keys([keys, candidates.join(successors, "successor_pnu", "left_semi")
                                          .select(F.col("source_pnu").alias("pnu"))])
    restricted = {
        PARCEL_SOURCE: incremental.rows_matching_any(frames[PARCEL_SOURCE], [("pnu", keys, "pnu", False)]),
        ZONE_CODE_SOURCE: frames[ZONE_CODE_SOURCE],
    }
    for name in ATTRIBUTE_SOURCES:
        restricted[name] = incremental.rows_matching_any(frames[name], [("pnu", sources, "pnu", False)])
    return restricted


def main() -> int:
    args = parse_args()
    args.published_at_utc = normalize_utc_timestamp(args.published_at_utc)
    pins = validate_args(args)
    column_lineage(args.carry_lineage)
    spark = build_spark_session(args)
    try:
        return run(spark, args, pins)
    finally:
        spark.stop()


def run(spark: SparkSession, args: argparse.Namespace, pins: dict[str, str]) -> int:
    """Build, check and write the Gold: whole, or only the changed PNUs merged (root ADR-0180)."""

    previous = incremental.validate_arguments(args, input_sources(args))
    counters: dict[str, int] = {}
    source_snapshots: dict[str, str] = {}
    frames, candidates = read_inputs(spark, args, pins, counters, source_snapshots)
    parcel_snapshot = source_snapshots[PARCEL_SOURCE]
    lane = incremental.Lane(
        build=lambda inputs: build_panel(spark, args, inputs, candidates, counters, parcel_snapshot),
        restrict=lambda inputs, keys: restrict_inputs(inputs, keys, candidates),
        affected=lambda changed: affected_pnus(
            *read_inputs(spark, args, previous, {}), frames, candidates, changed),
        validate=lambda gold: validate_gold_frame(gold, None),
        content_columns=CONTENT_DIGEST_COLUMNS, gold_columns=GOLD_COLUMNS,
        whole_table_inputs=WHOLE_TABLE_INPUTS,
    )
    outcome = incremental.build_or_merge(spark, args, lane, frames, pins, previous,
                                         qualified_target_table(args), validate_gold_frame,
                                         assert_minimum_row_count)
    try:
        persisted_count, metrics, added_columns = None, outcome.metrics, ()
        success = "validate-ok"
        if not args.validate_only:
            if args.write_mode == "parquet":
                write_gold_parquet(outcome.gold, args.output)
                persisted = spark.read.parquet(args.output).select(*GOLD_COLUMNS)
            else:
                added_columns = write_gold_iceberg(spark, outcome, args, pins)
                persisted = spark.table(qualified_target_table(args)).select(*GOLD_COLUMNS)
            persisted_count, metrics = validate_gold_frame(persisted, outcome.row_count)
            success = "write-ok"
        summary = build_run_summary(
            args,
            row_count=outcome.row_count,
            persisted_row_count=persisted_count,
            quality_metrics=metrics,
            section_counters=counters,
            source_snapshots=source_snapshots,
            schema_evolution_added_columns=added_columns,
            source_iceberg_snapshots=pins,
        )
        summary["rebuild"] = outcome.report()
        if not args.validate_only and args.write_mode == "iceberg":
            summary["write_disposition"] = outcome.disposition(args)
        emit_run_summary(summary, args.summary_output)
        if not args.validate_only:
            emit_lineage_event(build_lineage_event(args, summary), args.lineage_output)
        print(f"gold-parcel-panel-{success} rows={persisted_count or outcome.row_count} mode={outcome.mode}")
        return 0
    finally:
        outcome.release()


def write_gold_iceberg(
    spark: SparkSession, outcome: incremental.Outcome, args: argparse.Namespace, pins: dict[str, str]
) -> tuple[str, ...]:
    table = qualified_target_table(args)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{args.iceberg_catalog_name}`.`{args.target_iceberg_namespace}`")
    # Range-ordered by pnu, so a by-PNU bake shard reads only the files of its prefix (ADR-0164).
    added_columns = ensure_contract_table(spark, table, GOLD_CONTRACT)
    # The snapshot records the Silver pins it was built from (root ADR-0139), and the pinned
    # Silver snapshots stay readable for the next incremental comparison (ADR-0180).
    outcome.commit(spark, table, pins, args.iceberg_write_mode)
    incremental.retain_input_snapshots(spark, args.iceberg_catalog_name, args.source_iceberg_namespace,
                                       GOLD_CONTRACT_NAME, pins)
    return added_columns


if __name__ == "__main__":
    raise SystemExit(main())
