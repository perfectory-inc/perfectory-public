"""주소DB → silver.juso_road_address (root ADR-0495).

One row per 도로명주소 관리번호: its road (도로명코드, 도로명, 시도·시군구 이름), its building number and the PNU of
its representative 지번. The base is the monthly full, loaded once; each day's change file after it appends only
the keys it changes (변경사유 31 신규, 34 변경) and a removal marker for an address it abolishes (63), compared
with the table's current rows (root ADR-0318). A row's `source_snapshot_id` is the day it holds (the full's
기준일, its month's last day; a change file's day), so the newest day per key is the address as it is now and a
change file of the base's month replayed onto it ends in the same row.

`load` takes the directories `plan-silver-load-objects` wrote, loads the full first and the change files by day.
The members are CP949 `|` text without a header (the provider's guide in the full zip); `stage` copies the
columns the table reads into UTF-8 tab files Spark reads.
"""

from __future__ import annotations

import argparse
import calendar
import json
import re
import shutil
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

JOB_NAME = "juso_address_db"
TABLE = "silver.juso_road_address"
CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contracts" / "juso-address-db.contract.json"
FULL = "address_db_monthly_full"
DAILY = "address_db_daily_change"
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
REMOVED_REASON = "63"
# The staged columns of each member, as their positions in the provider's lines.
ADDRESS = {"road_address_management_no": 0, "road_code": 1, "emd_seq": 2, "underground_code": 3,
           "building_main_no": 4, "building_sub_no": 5, "reason": 7}
# The full's 지번 line has 11 fields; a change file's adds its 변경사유 as the 12th.
JIBUN = {"road_address_management_no": 0, "legal_dong_code": 2, "mountain_yn": 7, "lot_main_no": 8,
         "lot_sub_no": 9, "representative": 10, "reason": 11}
ROAD = {"road_code": 0, "road_name": 1, "emd_seq": 3, "sido_name": 4, "sigungu_name": 6}
CONTENT = ["road_code", "sigungu_code", "emd_seq", "road_name", "sido_name", "sigungu_name", "underground_code",
           "building_main_no", "building_sub_no", "legal_dong_code", "mountain_yn", "lot_main_no", "lot_sub_no", "pnu"]


def load_source_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def source_slug(slug: str) -> str:
    return f"jusogokr__{slug}"


def snapshot_of(slug: str, object_key: str) -> str:
    """The day an object holds, `YYYYMMDD`: a full's 기준일 (its month's last day), a change file's day."""

    leaf = object_key.rsplit("/", 1)[-1]
    if slug == FULL:
        month = re.match(r"(\d{4})(\d{2})", leaf)
        if not month:
            raise ValueError(f"{object_key}: a monthly full's leaf does not start with its YYYYMM")
        year, number = int(month.group(1)), int(month.group(2))
        return f"{year:04d}{number:02d}{calendar.monthrange(year, number)[1]:02d}"
    day = re.match(r"(\d{8})", leaf)
    if not day:
        raise ValueError(f"{object_key}: a change file's leaf does not start with its YYYYMMDD")
    return day.group(1)


def member_kind(name: str, slug: str, members: Mapping[str, Any]) -> str | None:
    """Which staged member a zip member is (`address`, `jibun`, `road_code`), or None for one not read."""

    names = members["full" if slug == FULL else "daily"]
    base = name.rsplit("/", 1)[-1]
    for kind in ("address", "jibun", "road_code"):
        pattern = names[kind]
        if (slug == FULL and (base == pattern or (pattern.endswith("_") and base.startswith(pattern))
                              and base.endswith(".txt"))) or (slug == DAILY and base.endswith(pattern)):
            return kind
    return None


def _decoded(info: zipfile.ZipInfo) -> str:
    """A member name as the provider wrote it: CP949 without the UTF-8 flag."""

    if info.flag_bits & 0x800:
        return info.filename
    try:
        return info.filename.encode("cp437").decode("cp949")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return info.filename


def lines(raw: Iterator[bytes], encoding: str, separator: str) -> Iterator[list[str]]:
    for line in raw:
        text = line.decode(encoding).rstrip("\r\n")
        if text:
            yield text.split(separator)


def stage(path: Path, slug: str, contract: Mapping[str, Any], out_dir: Path) -> dict[str, Path]:
    """The zip's address, jibun and road members as UTF-8 tab files of the columns the table reads.

    A 지번 line that is not the representative one (대표여부 1) is left out: the table carries the
    representative 지번 only."""

    members = contract["members"]
    encoding, separator = members["encoding"], members["separator"]
    out_dir.mkdir(parents=True, exist_ok=True)
    layouts = {"address": ADDRESS, "jibun": JIBUN, "road_code": ROAD}
    outs = {kind: (out_dir / f"{kind}.tsv").open("w", encoding="utf-8", newline="\n") for kind in layouts}
    try:
        with zipfile.ZipFile(path) as archive:
            for info in archive.infolist():
                kind = member_kind(_decoded(info), slug, members)
                if kind is None:
                    continue
                layout = layouts[kind]
                with archive.open(info) as raw:
                    for fields in lines(raw, encoding, separator):
                        if kind == "jibun" and fields[JIBUN["representative"]] != "1":
                            continue
                        cells = [fields[index].strip() if index < len(fields) else "" for index in layout.values()]
                        outs[kind].write("\t".join(cells) + "\n")
    finally:
        for out in outs.values():
            out.close()
    return {kind: out_dir / f"{kind}.tsv" for kind in layouts}


def planned_objects(plan_dirs: Sequence[Path]) -> list[dict[str, Any]]:
    """The plans' objects, the full first, then the change files by day."""

    from data_go_kr_file_data import planned_objects as from_plans  # noqa: PLC0415 - the one plan reader

    objects = from_plans(plan_dirs, {source_slug(FULL): FULL, source_slug(DAILY): DAILY})
    for item in objects:
        item["snapshot"] = snapshot_of(item["dataset_slug"], item["object_key"])
    return sorted(objects, key=lambda item: (item["dataset_slug"] != FULL, item["snapshot"]))


# --- Spark ----------------------------------------------------------------------------------------


def read_staged(spark: Any, path: Path, layout: Mapping[str, int]) -> Any:
    names = ", ".join(f"{name} string" for name in layout)
    return spark.read.option("sep", "\t").option("header", False).option("quote", "\u0000").schema(names).csv(str(path))


def rows_of(addresses: Any, jibuns: Any, roads: Any) -> Any:
    """The table's business columns of each address: its road's names and its representative 지번."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    road_names = (roads.groupBy("road_code")
                  .agg(F.min(F.struct(F.col("emd_seq"), "road_name", "sido_name", "sigungu_name")).alias("_r"))
                  .select("road_code", "_r.road_name", "_r.sido_name", "_r.sigungu_name"))
    lots = (jibuns.groupBy("road_address_management_no")
            .agg(F.min(F.struct("legal_dong_code", "mountain_yn", "lot_main_no", "lot_sub_no")).alias("_j"))
            .select("road_address_management_no", "_j.legal_dong_code", "_j.mountain_yn",
                    F.col("_j.lot_main_no").cast("int").alias("lot_main_no"),
                    F.col("_j.lot_sub_no").cast("int").alias("lot_sub_no")))
    pnu = F.when(F.col("legal_dong_code").rlike(r"^\d{10}$") & (F.col("lot_main_no") <= 9999)
                 & (F.col("lot_sub_no") <= 9999),
                 F.concat(F.col("legal_dong_code"), F.when(F.col("mountain_yn") == "1", F.lit("2")).otherwise(F.lit("1")),
                          F.lpad(F.col("lot_main_no").cast("string"), 4, "0"),
                          F.lpad(F.col("lot_sub_no").cast("string"), 4, "0")))
    return (addresses
            .select("road_address_management_no", "road_code", F.substring("road_code", 1, 5).alias("sigungu_code"),
                    "emd_seq", "underground_code", F.col("building_main_no").cast("int").alias("building_main_no"),
                    F.col("building_sub_no").cast("int").alias("building_sub_no"))
            .join(road_names, "road_code", "left").join(lots, "road_address_management_no", "left")
            .withColumn("pnu", pnu))


def full_rows(spark: Any, staged: Mapping[str, Path]) -> Any:
    return rows_of(read_staged(spark, staged["address"], ADDRESS), read_staged(spark, staged["jibun"], JIBUN),
                   read_staged(spark, staged["road_code"], ROAD))


def daily_rows(spark: Any, staged: Mapping[str, Path], current: Any) -> tuple[Any, Any]:
    """(the changed keys' new rows, the keys the file abolishes) of one change file over ``current``.

    An address line replaces the address; a representative 지번 line its 지번; a road line the names of
    every address on that road. What the file does not say is the current row's."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    # An empty field reads as null: a reason is compared as text.
    reason = F.coalesce(F.col("reason"), F.lit(""))
    addresses = read_staged(spark, staged["address"], ADDRESS)
    jibuns = read_staged(spark, staged["jibun"], JIBUN)
    roads = read_staged(spark, staged["road_code"], ROAD)
    removed = addresses.where(reason == REMOVED_REASON).select("road_address_management_no").distinct()
    kept_jibuns = jibuns.where(reason != REMOVED_REASON)
    renamed = roads.select("road_code").distinct()
    keys = (addresses.where(reason != REMOVED_REASON).select("road_address_management_no")
            .union(kept_jibuns.select("road_address_management_no"))
            .union(current.join(renamed, "road_code", "left_semi").select("road_address_management_no"))
            .distinct().join(removed, "road_address_management_no", "left_anti"))
    now_rows = current.join(keys, "road_address_management_no", "left_semi")
    address_part = (addresses.where(reason != REMOVED_REASON).drop("reason")
                    .unionByName(now_rows.join(addresses.select("road_address_management_no"),
                                               "road_address_management_no", "left_anti")
                                 .select(*(F.col(c).cast("string") for c in ADDRESS if c != "reason"))))
    jibun_part = (kept_jibuns.drop("reason", "representative")
                  .unionByName(now_rows.join(kept_jibuns.select("road_address_management_no"),
                                             "road_address_management_no", "left_anti")
                               .select("road_address_management_no", "legal_dong_code", "mountain_yn",
                                       F.col("lot_main_no").cast("string"), F.col("lot_sub_no").cast("string"))))
    address_part = address_part.join(keys, "road_address_management_no", "left_semi")
    # The current names of only the roads these addresses are on, but a road the file renames.
    current_roads = (current.select("road_code", "emd_seq", "road_name", "sido_name", "sigungu_name")
                     .join(address_part.select("road_code").distinct(), "road_code", "left_semi")
                     .join(renamed, "road_code", "left_anti"))
    return rows_of(address_part, jibun_part, roads.unionByName(current_roads)), removed


def _summary_line(summary: Mapping[str, Any]) -> None:
    print("juso-address-db-summary-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True, default=str))


def _load(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="load")
    parser.add_argument("--plan-dir", action="append", default=[], required=True,
                        help="A directory `plan-silver-load-objects` wrote (plan.json + objects/); repeatable")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--table-suffix", default="")
    parser.add_argument("--allow-non-smoke-write", action="store_true")
    args = parser.parse_args(argv)
    if not IDENTIFIER.fullmatch(args.iceberg_catalog_name):
        parser.error("--iceberg-catalog-name must be a plain SQL identifier")
    if args.table_suffix not in ("", "_smoke"):
        parser.error("--table-suffix is empty or _smoke")
    if not args.table_suffix and not args.allow_non_smoke_write:
        parser.error("writing the Silver table needs --allow-non-smoke-write")

    from lakehouse_engine import apply_catalog_settings, assert_catalog_env  # noqa: PLC0415
    from lakehouse_ingest import append_source_object  # noqa: PLC0415
    from platform_contracts import (  # noqa: PLC0415
        column_names, create_table_columns_sql, current_rows, evolve_iceberg_table_to_contract,
        load_lakehouse_contract, partition_clause_sql,
    )
    from pyspark.sql import SparkSession, functions as F  # noqa: PLC0415
    from silver_release_compare import release_changes  # noqa: PLC0415

    assert_catalog_env()
    source = load_source_contract()
    objects = planned_objects([Path(d) for d in args.plan_dir])
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    catalog = args.iceberg_catalog_name
    summary: dict[str, Any] = {"job": JOB_NAME, "status": "loaded", "objects": []}
    builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, catalog).getOrCreate()
    try:
        contract = load_lakehouse_contract(TABLE)
        columns = list(column_names(contract))
        qualified = f"`{catalog}`.`silver`.`juso_road_address{args.table_suffix}`"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{catalog}`.`silver`")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {qualified} ({create_table_columns_sql(contract)}) USING iceberg "
                  f"{partition_clause_sql(contract)} TBLPROPERTIES ('format-version' = '2', "
                  "'write.parquet.compression-codec' = 'zstd')")
        evolve_iceberg_table_to_contract(spark, qualified, contract)
        for item in objects:
            slug, snapshot = item["dataset_slug"], item["snapshot"]
            work = item["path"].parent / f"{item['path'].name}.staged"
            staged_files = stage(item["path"], slug, source, work)
            current = current_rows(spark.table(qualified), contract).cache()
            stamp = {"source_record_id": F.lit(item["object_key"]), "source_snapshot_id": F.lit(snapshot),
                     "ingested_at_utc": F.lit(now).cast("timestamp"), "row_state": F.lit(None).cast("string")}
            if slug == FULL:
                rows, compared = full_rows(spark, staged_files), current
            else:
                rows, removed = daily_rows(spark, staged_files, current)
                # Only the keys the file names: an address it does not name is not removed by it.
                named = rows.select("road_address_management_no").union(removed)
                compared = current.join(named, "road_address_management_no", "left_semi")
            staged = rows
            for name, value in stamp.items():
                staged = staged.withColumn(name, value)
            staged = staged.select(*columns).cache()
            changes = release_changes(staged, compared, key=("road_address_management_no",), content=CONTENT,
                                      removal="row_state", source_snapshot_id=snapshot,
                                      ingested_at=F.lit(now).cast("timestamp"), columns=columns)
            changes = changes.withColumn("source_record_id", F.lit(item["object_key"])).select(*columns).cache()
            counts = {"object": item["object_key"], "kind": slug, "snapshot": snapshot, "rows": staged.count(),
                      "appended": changes.count(), "removed": changes.where(F.col("row_state") == "removed").count(),
                      "with_pnu": staged.where(F.col("pnu").isNotNull()).count()}
            append_source_object(spark, changes, columns, qualified, item["object_key"], contract_table=TABLE)
            for frame in (current, staged, changes):
                frame.unpersist()
            shutil.rmtree(work, ignore_errors=True)
            summary["objects"].append(counts)
    finally:
        spark.stop()
    _summary_line(summary)
    return 0


COMMANDS = {"load": _load}


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] not in COMMANDS:
        print(f"usage: {Path(__file__).name} {{{','.join(COMMANDS)}}} ...", file=sys.stderr)
        return 2
    return COMMANDS[argv[0]](argv[1:])


if __name__ == "__main__":
    sys.exit(main())
