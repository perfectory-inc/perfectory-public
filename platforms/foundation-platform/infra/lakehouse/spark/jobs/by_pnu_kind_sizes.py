#!/usr/bin/env python3
"""Measure what each data kind would weigh as its own by-PNU lane (root ADR-0193 §6-1).

Reads one snapshot of `gold.parcel_panel` or `gold.building_panel` and splits every row into the
kinds ADR-0193 names, the same way the per-kind lanes will carry them. For each kind it reports how
many PNUs have content, the per-PNU body size (raw and gzip, the pack member compression of
`by_pnu_pack.rs`) at p50/p95/max, the size of each 법정동 (sum of its members' gzip, `dong_bytes`),
and the packs the bake would write after ADR-0163 splits a large 법정동 into parts (`pack_bytes`,
the size ADR-0193 §7 bounds), with how many packs and documents pass the whole-read bound.
`bundle` is today's whole panel row, the baseline the kinds are compared with; `first-paint` is
the sum of the three kinds the parcel summary reads on open.

Sizes are of the document body only; the per-kind envelope (schema, pnu, source) adds about a
hundred bytes before gzip. Building kinds are re-serialised from the panel JSON, so number
formatting may differ from Spark's by a few bytes.

Writes nothing to the lakehouse: one local summary file.

    spark-submit by_pnu_kind_sizes.py --unit parcel --summary-output /work/parcel-kind-sizes.json
"""

from __future__ import annotations

import argparse
import gzip
import json
import time
from pathlib import Path
from typing import Any, Iterator

JOB_NAME = "by_pnu_kind_sizes"
SUMMARY_SCHEMA_VERSION = "foundation-platform.by_pnu_kind_sizes.v1"
GOLD_TABLES = {"parcel": "gold.parcel_panel", "building": "gold.building_panel"}
# shortcut: the kind names below and FIRST_PAINT_KINDS copy ADR-0193 §2/§5 by hand; replace them
# with the contract's by_pnu_lanes once the kinds land there.
# The lanes the parcel summary reads on open (ADR-0193 §5).
FIRST_PAINT_KINDS = ("parcel-zoning", "parcel-price", "parcel-characteristics")
LINEAGE_COLUMNS = ("row_digest", "source_snapshot_id", "published_at_utc")
JSON_SEPARATORS = (",", ":")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--unit", required=True, choices=sorted(GOLD_TABLES))
    parser.add_argument("--snapshot-id", default="", help="Gold snapshot to read; empty reads the current one.")
    parser.add_argument("--iceberg-catalog-name", default="r2")
    parser.add_argument("--summary-output", required=True)
    return parser.parse_args(argv)


def r2_contract_path() -> Path:
    # On the driver only: the Python workers import this module from Spark's file cache, outside the repo.
    return Path(__file__).resolve().parents[4] / "config" / "r2-connections.contract.json"


def pack_rules() -> tuple[int, int]:
    """(whole-read bound, part target) of the contract's section packs (ADR-0151, ADR-0163)."""
    packs = json.loads(r2_contract_path().read_text(encoding="utf-8"))["by_pnu_section_packs"]
    return packs["read_path"]["whole_pack_max_bytes"], packs["parts"]["part_target_bytes"]


def fnv1a32(text: str) -> int:
    """The part hash of ADR-0163 (contract by_pnu_section_packs.parts.hash_definition)."""
    value = 0x811C9DC5
    for byte in text.encode("ascii"):
        value = ((value ^ byte) * 0x01000193) & 0xFFFFFFFF
    return value


def dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=JSON_SEPARATORS)


def is_empty(text: str | None) -> bool:
    return text is None or text in ("", "null", "[]", "{}")


def parcel_kinds(row: dict[str, Any]) -> dict[str, str]:
    """A parcel panel row's bodies by ADR-0193 lane; kinds with no content are left out."""
    bodies = {
        "parcel-zoning": row.get("zonings_json"),
        "parcel-price": row.get("price_json"),
        "parcel-characteristics": row.get("characteristics_json"),
        "parcel-forest-ledger": row.get("forest_ledger_json"),
        "parcel-transfer-history": row.get("transfer_history_json"),
    }
    if not is_empty(row.get("land_rights_json")):
        bodies["parcel-land-rights"] = (
            f'{{"land_rights":{row["land_rights_json"]},"land_right_total":{dumps(row.get("land_right_total"))}}}'
        )
    return {kind: body for kind, body in bodies.items() if not is_empty(body)}


def building_kinds(row: dict[str, Any]) -> dict[str, str]:
    """A building panel row's bodies by ADR-0193 lane (and ADR-0190's unit-price)."""
    buildings = json.loads(row.get("buildings_json") or "[]")
    unlinked = json.loads(row.get("unlinked_units_json") or "[]")
    titles = [{k: v for k, v in b.items() if k not in ("floors", "units")} for b in buildings]
    floors = [{"id": b.get("id"), "floors": b["floors"]} for b in buildings if b.get("floors")]
    units = [u for b in buildings for u in b.get("units") or []]
    prices = [
        {"id": u.get("id"), "official_price_history": u["official_price_history"]}
        for u in units + unlinked
        if u.get("official_price_history")
    ]
    no_price = lambda us: [{k: v for k, v in u.items() if k != "official_price_history"} for u in us]  # noqa: E731
    bodies = {
        "building-titles": dumps(titles) if titles else None,
        "building-floors": dumps(floors) if floors else None,
        "building-units": dumps({"units": no_price(units), "unlinked_units": no_price(unlinked)})
        if units or unlinked
        else None,
        "unit-price": dumps(prices) if prices else None,
    }
    return {kind: body for kind, body in bodies.items() if body is not None}


KIND_SPLITTERS = {"parcel": parcel_kinds, "building": building_kinds}


def bundle_body(row: dict[str, Any]) -> str:
    """Today's panel row as one document: every content column (lineage excluded).

    A `*_json` column is embedded as the JSON it holds, as the served document carries it, not
    as a string of escaped JSON.
    """
    body = {}
    for key, value in row.items():
        if key == "pnu" or key in LINEAGE_COLUMNS:
            continue
        if key.endswith("_json"):
            body[key[: -len("_json")]] = None if value is None else json.loads(value)
        else:
            body[key] = value
    return dumps(body)


def gzip_size(text: str) -> int:
    # Same member as by_pnu_pack.rs: best compression, mtime 0.
    return len(gzip.compress(text.encode("utf-8"), compresslevel=9, mtime=0))


def measure_row(unit: str, row: dict[str, Any]) -> Iterator[tuple[str, str, int, int, int]]:
    """(pnu, kind, raw bytes, gzip bytes, part hash) for each kind the row carries, plus the bundle."""
    pnu = row["pnu"]
    part_hash = fnv1a32(pnu)
    for kind, body in KIND_SPLITTERS[unit](row).items():
        yield pnu, kind, len(body.encode("utf-8")), gzip_size(body), part_hash
    bundle = bundle_body(row)
    yield pnu, "bundle", len(bundle.encode("utf-8")), gzip_size(bundle), part_hash


def measure_frame(spark: Any, unit: str, frame: Any, rules: tuple[int, int] | None = None) -> dict[str, Any]:
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    bound, part_target = rules or pack_rules()

    # Row by row through the RDD: the Spark image has no pandas, and a building row can be 12.7M
    # characters (2026-10-11), so only one row's split is held at a time.
    # The row function runs in the Python workers, which import this module by name.
    spark.sparkContext.addPyFile(__file__)
    measured = spark.createDataFrame(
        frame.rdd.flatMap(lambda row: measure_row(unit, row.asDict())),
        "pnu string, kind string, raw_bytes long, gzip_bytes long, part_hash long",
    )
    if unit == "parcel":
        first_paint = (
            measured.where(F.col("kind").isin(*FIRST_PAINT_KINDS))
            .groupBy("pnu")
            .agg(F.sum("raw_bytes").alias("raw_bytes"), F.sum("gzip_bytes").alias("gzip_bytes"),
                 F.first("part_hash").alias("part_hash"))
            .withColumn("kind", F.lit("first-paint"))
            .select("pnu", "kind", "raw_bytes", "gzip_bytes", "part_hash")
        )
        measured = measured.unionByName(first_paint)
    measured = measured.withColumn("dong", F.substring("pnu", 1, 10)).persist(StorageLevel.DISK_ONLY)

    def pct(column: str) -> list[Any]:
        return [
            F.percentile_approx(column, 0.5, 10000).alias(f"{column}_p50"),
            F.percentile_approx(column, 0.95, 10000).alias(f"{column}_p95"),
            F.max(column).alias(f"{column}_max"),
        ]

    documents = measured.groupBy("kind").agg(
        F.count("*").alias("pnus"),
        F.sum(F.when(F.col("gzip_bytes") > bound, 1).otherwise(0)).alias("documents_over_bound"),
        *pct("raw_bytes"),
        *pct("gzip_bytes"),
    )
    packed = measured.where(F.col("kind") != "first-paint")
    # ADR-0163: K = max(1, ceil(dong bytes / part target)); a PNU goes to part fnv1a32(pnu) mod K.
    dongs = (
        packed.groupBy("kind", "dong")
        .agg(F.sum("gzip_bytes").alias("dong_bytes"))
        .withColumn("parts", F.greatest(F.lit(1), F.ceil(F.col("dong_bytes") / F.lit(part_target))))
    )
    packs = (
        packed.join(dongs.select("kind", "dong", "parts"), ["kind", "dong"])
        .groupBy("kind", "dong", F.pmod(F.col("part_hash"), F.col("parts")).alias("part"))
        .agg(F.sum("gzip_bytes").alias("pack_bytes"))
        .groupBy("kind")
        .agg(
            F.count("*").alias("packs"),
            F.sum(F.when(F.col("pack_bytes") > bound, 1).otherwise(0)).alias("packs_over_bound"),
            *pct("pack_bytes"),
        )
    )
    dong_summary = dongs.groupBy("kind").agg(
        F.count("*").alias("dongs"),
        F.sum(F.when(F.col("parts") > 1, 1).otherwise(0)).alias("parted_dongs"),
        *pct("dong_bytes"),
    )
    kinds = {row["kind"]: row.asDict() for row in documents.collect()}
    for summary in (dong_summary, packs):
        for row in summary.collect():
            kinds[row["kind"]].update(row.asDict())
    measured.unpersist()
    return {kind: {k: v for k, v in values.items() if k != "kind"} for kind, values in sorted(kinds.items())}


def write_summary(path: str, summary: dict[str, Any]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.snapshot_id and not args.snapshot_id.isdigit():
        raise SystemExit(f"--snapshot-id {args.snapshot_id!r} is not an Iceberg snapshot id")

    from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages
    from pyspark.sql import SparkSession

    assert_catalog_env(args.iceberg_catalog_name)
    started = time.monotonic()
    builder = (
        SparkSession.builder.appName(JOB_NAME)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", iceberg_packages())
        .config("spark.ui.enabled", "false")
    )
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    table = GOLD_TABLES[args.unit]
    qualified = f"`{args.iceberg_catalog_name}`.{table}"
    # Resolve "current" to its id first, so the summary names the snapshot it measured.
    snapshot_id = args.snapshot_id or str(
        spark.sql(f"SELECT snapshot_id FROM {qualified}.refs WHERE name = 'main'").collect()[0][0])
    frame = spark.sql(f"SELECT * FROM {qualified} VERSION AS OF {int(snapshot_id)}")
    bound, part_target = pack_rules()
    kinds = measure_frame(spark, args.unit, frame, (bound, part_target))
    write_summary(args.summary_output, {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "job_name": JOB_NAME,
        "table": table,
        "snapshot_id": snapshot_id,
        "pack_bound_bytes": bound,
        "part_target_bytes": part_target,
        "kinds": kinds,
        "wall_seconds": round(time.monotonic() - started, 1),
    })
    print(f"{JOB_NAME}-ok unit={args.unit} snapshot_id={snapshot_id} kinds={','.join(kinds)}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
