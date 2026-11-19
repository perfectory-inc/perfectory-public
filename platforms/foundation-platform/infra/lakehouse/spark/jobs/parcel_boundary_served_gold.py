#!/usr/bin/env python3
"""Build `gold.parcel_boundary_served`: the parcel boundaries exactly as they are tiled.

Root ADR-0133 §2. One `source_snapshot_id` of `silver.parcel_boundaries` (about forty million rows,
EPSG:4326) is read, every ledgered `parcels` edit in `silver.map_edit_ledger` is applied in
`change_seq` order, the served table is rewritten the way the other served-Gold tables are, and
the tile bake gets the v2 handoff: create-only JSONL parts written by the executors plus a summary
(`served_gold_common`, "The v2 handoff").

Unlike the complex and admin jobs, nothing is held on the driver but the edits. Silver rows stay in
Spark: an edited PNU is removed from the snapshot with a broadcast anti-join and its upsert, if
any, is unioned back.

`--edits-input` (the `export-map-edit-handoff` JSONL of the edit store's `parcels` unit, root
ADR-0112 §7) is appended to the ledger first, once per edit. `--fold` then folds only the edits after
the served Gold's sequence (`parcel_boundary_fold`): a keyed write of the changed PNUs and a partial
handoff of their neighbourhood (root ADR-0375 §2). When a fold cannot be partial the job says why
and exits 3 (`NOT_PARTIAL_EXIT`) having written nothing; the caller runs it again without `--fold`,
the declared full exception that rewrites the table and hands every row.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import parcel_boundary_fold as fold
import served_gold_common as common
import vworld_parcel_editions as editions
from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages
from platform_contracts import admit_write, column_names, declared_geometry_srid, load_lakehouse_contract, pinned_edition

JOB_NAME = "parcel_boundary_served_gold"
UNIT = "parcels"
FEATURE_ID_PROPERTY = "pnu"
# The tile carries the PNU and nothing else; the bake takes it from `feature_id`.
TILE_PROPERTIES: tuple[str, ...] = ()

SILVER_CONTRACT = load_lakehouse_contract("silver.parcel_boundaries")
SERVED_CONTRACT = load_lakehouse_contract("gold.parcel_boundary_served")
SERVED_COLUMNS: tuple[str, ...] = column_names(SERVED_CONTRACT)
GEOMETRY_SRID: int = declared_geometry_srid(SERVED_CONTRACT)
if declared_geometry_srid(SILVER_CONTRACT) != GEOMETRY_SRID:
    raise ValueError("silver.parcel_boundaries and the served table must share one CRS")
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SNAPSHOT_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
PNU_PATTERN = re.compile(r"^[0-9]{19}$")
WKB_HEX_PATTERN = re.compile(r"^(?:[0-9a-f]{2})+$")
CHECKSUM_PATTERN = re.compile(r"^[0-9a-f]{64}$")
DEFAULT_HANDOFF_PARTS = 64
# A fold that cannot be partial: nothing is written and the caller hands every row (root ADR-0375 §2).
NOT_PARTIAL_EXIT = 3
CHANGE_LIST_NAME = "change-list.jsonl"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--source-snapshot-id",
        help="The one silver.parcel_boundaries source_snapshot_id to serve; the source contract's served edition "
        "when omitted, and refused when it differs unless --allow-non-served-edition (root ADR-0148 §1).",
    )
    editions.add_reader_flag(parser)
    parser.add_argument(
        "--output-dir", required=True, help="Directory for the handoff parts. Created here; never reused."
    )
    parser.add_argument("--summary-output", help="Path for the v2 run summary JSON.")
    parser.add_argument("--handoff-parts", type=int, default=DEFAULT_HANDOFF_PARTS)
    parser.add_argument("--edits-input", help="JSONL from export-map-edit-handoff for unit parcels; ledgered first.")
    parser.add_argument(
        "--fold", action="store_true",
        help="Write only the PNUs edited after the served Gold and hand their neighbourhood (root ADR-0375 §2).",
    )
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-namespace", default="silver")
    parser.add_argument("--iceberg-table", default="parcel_boundaries")
    parser.add_argument("--ledger-iceberg-namespace", default="silver")
    parser.add_argument("--ledger-iceberg-table", default="map_edit_ledger")
    parser.add_argument("--served-iceberg-namespace", default="gold")
    parser.add_argument("--served-iceberg-table", default="parcel_boundary_served")
    parser.add_argument(
        "--allow-non-smoke-write",
        action="store_true",
        help="Required to write a served table whose name does not end in _smoke.",
    )
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    args = parser.parse_args(argv)
    args.source_snapshot_id = editions.served_reader_id(
        args.source_snapshot_id, args.allow_non_served_edition, "--source-snapshot-id"
    )
    return args


def validate_args(args: argparse.Namespace) -> None:
    for label in (
        "iceberg_catalog_name",
        "iceberg_namespace",
        "iceberg_table",
        "ledger_iceberg_namespace",
        "ledger_iceberg_table",
        "served_iceberg_namespace",
        "served_iceberg_table",
    ):
        if not IDENTIFIER_PATTERN.match(getattr(args, label)):
            raise ValueError(f"{label.replace('_', ' ')} is not a plain identifier")
    if not SNAPSHOT_ID_PATTERN.match(args.source_snapshot_id):
        raise ValueError("--source-snapshot-id is not a plain snapshot id")
    if not args.served_iceberg_table.endswith("_smoke") and not args.allow_non_smoke_write:
        raise ValueError(
            f"writing {args.served_iceberg_table} needs --allow-non-smoke-write; "
            "only *_smoke tables are written by default"
        )
    if args.handoff_parts <= 0:
        raise ValueError("--handoff-parts must be positive")
    assert_catalog_env()


def qualified(catalog: str, namespace: str, table: str) -> str:
    return f"`{catalog}`.`{namespace}`.`{table}`"


def check_ledger(ledger: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The unit's ledgered edits in change order, refusing any the served table could not hold."""

    seen: set[int] = set()
    for edit in ledger:
        seq = int(edit["change_seq"])
        if seq <= 0 or seq in seen:
            raise ValueError(f"the parcels ledger repeats or lacks change_seq {seq}")
        seen.add(seq)
        if int(edit["geometry_srid"]) != GEOMETRY_SRID:
            raise ValueError(f"edit {seq} is EPSG:{edit['geometry_srid']}, the parcels are EPSG:{GEOMETRY_SRID}")
        if not PNU_PATTERN.match(str(edit["feature_id"])):
            raise ValueError(f"edit {seq} names {edit['feature_id']!r}, which is not a PNU")
        if edit["op"] == "upsert":
            if not WKB_HEX_PATTERN.match(edit.get("geometry_wkb_hex") or ""):
                raise ValueError(f"edit {seq} upserts a parcel without its geometry")
            if not CHECKSUM_PATTERN.match(edit.get("geometry_checksum_sha256") or ""):
                raise ValueError(f"edit {seq} upserts a parcel without its geometry checksum")
        elif edit["op"] != "delete":
            raise ValueError(f"edit {seq} has op {edit['op']}")
    return sorted(ledger, key=lambda item: int(item["change_seq"]))


def fold_edits(
    ledger: list[dict[str, Any]], present: set[str]
) -> tuple[dict[str, dict[str, Any] | None], dict[str, int]]:
    """Each edited PNU's final state (its last upsert, or None when deleted) and what the edits did.

    `present` is which edited PNUs the Silver snapshot holds. A delete of a parcel that is not
    there at that point in the order is counted, never passed over in silence.
    """

    final: dict[str, dict[str, Any] | None] = {}
    counts = {"upserts": 0, "deletes": 0, "deletes_of_absent_features": 0}
    for edit in check_ledger(ledger):
        feature = str(edit["feature_id"])
        exists = final[feature] is not None if feature in final else feature in present
        if edit["op"] == "delete":
            counts["deletes"] += 1
            if not exists:
                counts["deletes_of_absent_features"] += 1
            final[feature] = None
        else:
            counts["upserts"] += 1
            final[feature] = edit
    return final, counts


def check_silver(base: Any, source_snapshot_id: str) -> int:
    """Refuses an empty snapshot, a row in another CRS, or a PNU served twice; returns the row count."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    rows = base.count()
    if rows == 0:
        raise ValueError(f"silver.parcel_boundaries holds no rows of snapshot {source_snapshot_id}")
    wrong = base.where(F.col("geometry_srid") != GEOMETRY_SRID).count()
    if wrong:
        raise ValueError(f"{wrong} Silver rows are not EPSG:{GEOMETRY_SRID}")
    repeated = base.groupBy("pnu").count().where(F.col("count") > 1).orderBy("pnu").limit(5).collect()
    if repeated:
        named = ", ".join(f"{row['pnu']} x{row['count']}" for row in repeated)
        raise ValueError(f"snapshot {source_snapshot_id} holds a PNU more than once: {named}")
    return rows


def build_served_frame(
    base: Any, base_rows: int, ledger: list[dict[str, Any]]
) -> tuple[Any, dict[str, int], int]:
    """Silver minus every edited PNU plus each edited PNU's final upsert, and the count it must have.

    `base` carries `pnu`, `geometry_wkb`, `geometry_srid`, `geometry_checksum_sha256` and
    `source_snapshot_id`. The edited PNUs travel to the executors as a broadcast; the snapshot never
    comes to the driver. The returned count is `base_rows - removed + upserted`, which the caller
    checks the written table against.
    """

    from pyspark.sql import functions as F  # noqa: PLC0415

    spark = base.sparkSession
    touched = sorted({str(edit["feature_id"]) for edit in ledger})
    present: set[str] = set()
    if touched:
        touched_frame = F.broadcast(spark.createDataFrame([(pnu,) for pnu in touched], "pnu string"))
        present = {row["pnu"] for row in base.join(touched_frame, "pnu", "left_semi").select("pnu").collect()}
        base = base.join(touched_frame, "pnu", "left_anti")
    final, counts = fold_edits(ledger, present)
    kept = base.select(
        "pnu",
        "geometry_wkb",
        F.lit(GEOMETRY_SRID).cast("int").alias("geometry_srid"),
        "geometry_checksum_sha256",
        F.lit("source").alias("origin"),
        F.col("source_snapshot_id").cast("string").alias("source_snapshot_id"),
    )
    upserts = [
        (
            pnu,
            bytes.fromhex(edit["geometry_wkb_hex"]),
            GEOMETRY_SRID,
            edit["geometry_checksum_sha256"],
            "edit",
            f"map-edit-{int(edit['change_seq'])}",
        )
        for pnu, edit in sorted(final.items())
        if edit is not None
    ]
    edited = spark.createDataFrame(
        upserts,
        "pnu string, geometry_wkb binary, geometry_srid int, geometry_checksum_sha256 string, "
        "origin string, source_snapshot_id string",
    )
    expected = base_rows - len(present) + len(upserts)
    return kept.unionByName(edited), counts, expected


def load_pyspark() -> Any:
    from pyspark.sql import SparkSession  # noqa: PLC0415

    return SparkSession


def build_spark_session(args: argparse.Namespace, SparkSession: Any) -> Any:
    builder = (
        SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(args.handoff_parts))
        .config("spark.executorEnv.PYTHONPATH", common.JOBS_DIR)
    )
    builder = apply_catalog_settings(builder, args.iceberg_catalog_name)
    return builder.config("spark.jars.packages", args.iceberg_packages).getOrCreate()


def read_ledger(spark: Any, ledger_table: str) -> list[dict[str, Any]]:
    """Every ledgered `parcels` edit; the ledger is small, the snapshot is not."""

    return [
        row.asDict()
        for row in spark.sql(
            f"SELECT change_seq, feature_id, op, geometry_srid, lower(hex(geometry_wkb)) AS geometry_wkb_hex, "
            f"geometry_checksum_sha256 FROM {ledger_table} WHERE unit = '{UNIT}'"
        ).collect()
    ]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_args(args)
    output_dir = Path(args.output_dir)
    if output_dir.exists():
        raise FileExistsError(f"served-boundary bake handoff already exists and is evidence: {output_dir}")
    now = datetime.now(timezone.utc).replace(microsecond=0)

    spark = build_spark_session(args, load_pyspark())
    try:
        from pyspark.sql import functions as F  # noqa: PLC0415

        catalog = args.iceberg_catalog_name
        ledger_table = common.ensure_table(
            spark,
            qualified(catalog, args.ledger_iceberg_namespace, args.ledger_iceberg_table),
            catalog, args.ledger_iceberg_namespace, common.LEDGER_CONTRACT,
        )
        appended = 0
        if args.edits_input:
            handed_edits = common.read_edit_handoff(
                Path(args.edits_input).read_text(encoding="utf-8").splitlines(), UNIT, GEOMETRY_SRID, TILE_PROPERTIES
            )
            appended, _ = common.append_to_ledger(
                spark, ledger_table, UNIT, handed_edits, f"map-edit-export-{uuid.uuid4()}", now
            )
        ledger = check_ledger(read_ledger(spark, ledger_table))
        through = max((int(row["change_seq"]) for row in ledger), default=0)

        silver_table = qualified(catalog, args.iceberg_namespace, args.iceberg_table)
        silver_snapshot = common.current_snapshot(spark, silver_table)
        # Pinned to one Iceberg snapshot, so the checks and the write read the same rows.
        # One named edition, by the contract (root ADR-0286 §3, ADR-0376 §2); its rows are all open
        # (2026-11-04: 0 closed rows in either edition), so the predicate keeps every one.
        base = pinned_edition(
            spark.read.option("snapshot-id", str(silver_snapshot)).table(silver_table), SILVER_CONTRACT,
            args.source_snapshot_id,
        ).select("pnu", "geometry_wkb", "geometry_srid", "geometry_checksum_sha256", "source_snapshot_id",
                 *fold.SILVER_BOX)
        served_table = common.ensure_table(
            spark,
            qualified(catalog, args.served_iceberg_namespace, args.served_iceberg_table),
            catalog, args.served_iceberg_namespace, SERVED_CONTRACT,
        )
        if args.fold:
            folded = fold_into_served(spark, args, base, ledger, served_table, silver_snapshot, now, output_dir)
            if "full_reason" in folded:
                print("parcel-boundary-fold-not-partial " + json.dumps(folded["full_reason"], ensure_ascii=False))
                return NOT_PARTIAL_EXIT
            gold_snapshot, parts, persisted = folded["gold_snapshot"], folded["parts"], folded["handed"]
            base_rows, counts, scope = folded["served_rows"], folded["counts"], folded["scope"]
        else:
            gold_snapshot, parts, persisted, base_rows, counts = write_full(
                spark, args, base, ledger, served_table, silver_snapshot, through, now, output_dir
            )
            scope = {"handoff_scope": "full", "full_reason": "a new edition, or a fold that could not be partial"}
    finally:
        spark.stop()

    common.verify_handoff_parts(output_dir, parts, persisted)
    summary = common.summary_v2(
        source_snapshot_id=args.source_snapshot_id, parts=parts,
        job=JOB_NAME, unit=UNIT, feature_id_property=FEATURE_ID_PROPERTY, srid=GEOMETRY_SRID,
        gold_snapshot=gold_snapshot, through=through, handoff=appended, appended=appended, ledger=len(ledger),
        served=persisted, base=base_rows, output=output_dir,
        generated_at=now.isoformat().replace("+00:00", "Z"), counts=counts,
        extra={"silver_iceberg_snapshot_id": silver_snapshot, **scope},
    )
    if args.summary_output:
        path = Path(args.summary_output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("parcel-boundary-served-gold-summary-json " + json.dumps(summary, sort_keys=True))
    return 0


def write_full(
    spark: Any, args: argparse.Namespace, base: Any, ledger: list[dict[str, Any]], served_table: str,
    silver_snapshot: str, through: int, now: Any, output_dir: Path,
) -> tuple[str, list[dict[str, Any]], int, int, dict[str, int]]:
    """The whole served table rewritten from the edition and every edit, and every row handed."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    base_rows = check_silver(base, args.source_snapshot_id)
    served, counts, expected = build_served_frame(base, base_rows, ledger)
    served.select(
        *[F.col(name) for name in ("pnu", "geometry_wkb", "geometry_srid", "geometry_checksum_sha256",
                                   "origin", "source_snapshot_id")],
        F.lit(silver_snapshot).alias("silver_iceberg_snapshot_id"),
        F.lit(through).cast("long").alias("edits_through_change_seq"),
        F.lit(now).cast("timestamp").alias("published_at_utc"),
    ).select(*SERVED_COLUMNS).createOrReplaceTempView("served_candidate")
    admit_write(SERVED_CONTRACT["table_name"], "replace", target=served_table)
    spark.sql(f"INSERT OVERWRITE {served_table} SELECT {', '.join(SERVED_COLUMNS)} FROM served_candidate")
    gold_snapshot = common.current_snapshot(spark, served_table)
    written = spark.sql(f"SELECT pnu, geometry_wkb, origin FROM {served_table} VERSION AS OF {gold_snapshot}")
    persisted = written.count()
    if persisted != expected:
        raise ValueError(
            f"served table read back {persisted} rows; Silver {base_rows} minus the edited parcels "
            f"plus the upserts is {expected}"
        )
    # The bake reads exactly the committed Gold snapshot, not a recomputation of it.
    parts = common.write_handoff_parts(
        written, output_dir, id_column=FEATURE_ID_PROPERTY, property_columns=TILE_PROPERTIES,
        srid=GEOMETRY_SRID, parts=args.handoff_parts,
    )
    return gold_snapshot, parts, persisted, base_rows, counts


def fold_into_served(
    spark: Any, args: argparse.Namespace, base: Any, ledger: list[dict[str, Any]], served_table: str,
    silver_snapshot: str, now: Any, output_dir: Path,
) -> dict[str, Any]:
    """Root ADR-0375 §2 for parcels: the keyed write of the PNUs edited after the served Gold and the
    partial handoff of their neighbourhood, or `{"full_reason": ...}` (nothing written) when the fold
    cannot be partial."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    served_snapshot = common.current_snapshot(spark, served_table)
    if served_snapshot is None:
        return {"full_reason": "the served table has no snapshot yet"}
    served = spark.sql(f"SELECT * FROM {served_table} VERSION AS OF {served_snapshot}")
    editions_served = sorted(
        str(row["source_snapshot_id"])
        for row in served.where(F.col("origin") == "source").select("source_snapshot_id").distinct().limit(2).collect()
    )
    if editions_served != [args.source_snapshot_id]:
        return {"full_reason": f"the served Gold holds editions {editions_served}, not {args.source_snapshot_id}"}
    served_through = int(served.agg(F.max("edits_through_change_seq")).first()[0] or 0)
    changed = sorted({str(edit["feature_id"]) for edit in ledger if int(edit["change_seq"]) > served_through})
    if not changed:
        return {"full_reason": f"no ledgered edit after the served Gold's change_seq {served_through}"}
    names = F.broadcast(spark.createDataFrame([(pnu,) for pnu in changed], "pnu string"))
    silver_boxes = {
        str(row["pnu"]): tuple(float(row[column]) for column in fold.SILVER_BOX)
        for row in base.join(names, "pnu", "left_semi").select("pnu", *fold.SILVER_BOX).collect()
        if all(row[column] is not None for column in fold.SILVER_BOX)
    }
    planned = fold.plan(ledger, served_through, silver_boxes)
    if "full_reason" in planned:
        return planned

    through = max(int(edit["change_seq"]) for edit in ledger)
    changes = spark.createDataFrame(
        [
            (
                pnu,
                bytes.fromhex(state["geometry_wkb_hex"]) if state else None,
                GEOMETRY_SRID,
                state["geometry_checksum_sha256"] if state else None,
                "edit",
                f"map-edit-{int(state['change_seq'])}" if state else None,
                "upsert" if state else "delete",
            )
            for pnu, state in sorted(planned["after"].items())
        ],
        "pnu string, geometry_wkb binary, geometry_srid int, geometry_checksum_sha256 string, origin string, "
        "source_snapshot_id string, op string",
    ).select(
        "*",
        F.lit(silver_snapshot).alias("silver_iceberg_snapshot_id"),
        F.lit(through).cast("long").alias("edits_through_change_seq"),
        F.lit(now).cast("timestamp").alias("published_at_utc"),
    )
    changes.createOrReplaceTempView("parcel_fold_changes")
    columns = ", ".join(SERVED_COLUMNS)
    values = ", ".join(f"s.{name}" for name in SERVED_COLUMNS)
    assignments = ", ".join(f"t.{name} = s.{name}" for name in SERVED_COLUMNS)
    # The table is replaced by `pnu` (its row_versions): the fold replaces exactly the changed keys.
    admit_write(SERVED_CONTRACT["table_name"], "replace", target=served_table)
    spark.sql(
        f"MERGE INTO {served_table} t USING parcel_fold_changes s ON t.pnu = s.pnu "
        f"WHEN MATCHED AND s.op = 'delete' THEN DELETE "
        f"WHEN MATCHED THEN UPDATE SET {assignments} "
        f"WHEN NOT MATCHED AND s.op = 'upsert' THEN INSERT ({columns}) VALUES ({values})"
    )
    gold_snapshot = common.current_snapshot(spark, served_table)

    ledger_pnus = F.broadcast(spark.createDataFrame([(pnu,) for pnu in planned["ledger_pnus"]], "pnu string"))
    neighbours = (
        base.where(F.expr(fold.silver_predicate(planned["tiles"])))
        .join(ledger_pnus, "pnu", "left_anti")
        .select("pnu")
        .unionByName(spark.createDataFrame([(pnu,) for pnu in planned["edited_in_neighbourhood"]], "pnu string"))
        .distinct()
    )
    written = spark.sql(f"SELECT pnu, geometry_wkb, origin FROM {served_table} VERSION AS OF {gold_snapshot}")
    parts = common.write_handoff_parts(
        written.join(F.broadcast(neighbours), "pnu", "left_semi"), output_dir, id_column=FEATURE_ID_PROPERTY,
        property_columns=TILE_PROPERTIES, srid=GEOMETRY_SRID, parts=args.handoff_parts,
    )
    change_list = change_list_file(output_dir.parent, f"{output_dir.name}.{CHANGE_LIST_NAME}", planned["changes"])
    _, counts = fold_edits([edit for edit in ledger if int(edit["change_seq"]) > served_through], set(silver_boxes))
    return {
        "gold_snapshot": gold_snapshot,
        "parts": parts,
        "handed": sum(part["rows"] for part in parts),
        "served_rows": int(written.count()),
        "counts": counts,
        "scope": {
            "handoff_scope": "partial",
            "partial": {
                "base_snapshot": str(served_snapshot),
                "neighbourhood_zoom": fold.NEIGHBOURHOOD_ZOOM,
                "neighbourhood_tiles": [list(tile) for tile in planned["tiles"]],
                "change_list": change_list,
            },
            "changed_parcels": {op: sum(line["op"] == op for line in planned["changes"]) for op in ("upsert", "delete")},
        },
    }


def change_list_file(directory: Path, name: str, lines: list[dict[str, str]]) -> dict[str, Any]:
    """Writes the change list beside the handoff, create-only, and returns the summary's record of it."""

    text = "".join(json.dumps(line, sort_keys=True) + "\n" for line in lines).encode("utf-8")
    with (directory / name).open("xb") as handle:
        handle.write(text)
    return {"path": name, "rows": len(lines), "sha256": hashlib.sha256(text).hexdigest()}


if __name__ == "__main__":
    raise SystemExit(main())
