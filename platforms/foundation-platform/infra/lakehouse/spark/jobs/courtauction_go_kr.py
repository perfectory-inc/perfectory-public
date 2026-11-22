"""Load 법원경매정보 list rows from Bronze into Silver (root ADR-0280, ADR-0281).

The collector (`collect-courtauction-go-kr`) lands, per night, one gzip JSON Lines object per search;
each line is one page request with the response body as received. What a load owes is decided from
R2 alone (`plan-silver-load-objects`): the objects of each search that no commit of its table records
yet, fetched into a plan directory of the load's own run. For each object, oldest first, this job:

- reads the rows (`data.dlt_srchResult`) of every 200 answer, one row per 물건 (court office, case,
  property and item sequence), the later answer winning when the list shifted between pages;
- gives each row its PNU through the one place-identity resolver (root ADR-0288, ADR-0382): the
  representative parcel's legal-dong code with its lot (`source_code_pnu_raw`) carried to the current
  code (`place_identity_resolver.resolve`); a row that path leaves unresolved, by its address
  (`address_pnu`): the 행정구역 names with the representative lot as a jibun address, else the road
  name with the building number;
- appends to `silver.court_auction_item` the rows whose content changed and, only when the night read
  the item search completely, a removal marker for each 물건 that left it (root ADR-0275); to
  `silver.court_auction_sale_result` the rows whose content changed, never a marker; whether the
  night read the item search completely is read from the lines themselves (every condition read to
  its last page);
- commits each object on its own, empty if need be, so the table records it;
- then follows the 법정동 code changes since each table's last follow (root ADR-0329 §3, ADR-0416).

The other documents are not this job's: the publisher's `load-courtauction-go-kr-silver` loads the
공통코드, 감정평가서, 매각물건명세서, 배당요구종기공고, 매각공고, 사건 문서 and 물건상세 objects in DuckDB
(root ADR-0559).

The last stdout line is `court-auction-summary-json {json}`.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import address_pnu
import code_change_follow as follow
import place_identity_resolver

JOB_NAME = "courtauction_go_kr"
ITEM_TABLE = "silver.court_auction_item"
RESULT_TABLE = "silver.court_auction_sale_result"
CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"
CONTRACT_PATH = CONTRACTS / "courtauction-go-kr.contract.json"
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
KEY = ("court_office_code", "case_no", "property_seq", "item_seq")
LOAD_COLUMNS = ("source_record_id", "source_line_number", "source_snapshot_id", "ingested_at_utc", "row_state")
ROLE_TABLE = {"item_search": ITEM_TABLE, "sale_result_search": RESULT_TABLE}
def load_source_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def offer(row: Mapping[str, Any], prefixes: Mapping[str, str], dongs: Mapping[tuple[str, str], str]) -> dict[str, Any]:
    """What a row's address offers the joins: the names with the lot as a jibun address, else the
    road name with the building number."""

    names = [row[k] for k in ("address_sido_name", "address_sigungu_name", "address_dong_name", "address_ri_name") if row[k]]
    jibun = address_pnu.candidates(" ".join([*names, row["representative_lot_raw"] or ""]), prefixes, dongs)
    if jibun["jibun_pnu"] or jibun["reason"] == "no_legal_dong" or not row["road_name"]:
        return jibun
    road = " ".join(n for n in (*names[:2], row["road_name"], row["building_no_raw"] or "") if n)
    return address_pnu.candidates(road, prefixes, dongs)


def content_columns(contract: Mapping[str, Any]) -> list[str]:
    return [c["name"] for c in contract["columns"] if c["name"] not in (*KEY, *LOAD_COLUMNS)]


def _summary_line(summary: Mapping[str, Any]) -> None:
    print("court-auction-summary-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True, default=str))


RESOLVED = ("pnu", "pnu_basis", "pnu_unresolved_reason", "pnu_candidate_count")
# What the joins read beside the table's own columns (road_name is the table's raw road name, so the
# road key travels as road_name_key and takes the joins' name only inside resolve).
OFFER_COLUMNS = (("jibun_pnu", "string"), ("reason", "string"), ("road_sido", "string"), ("road_sigungu", "string"),
                 ("road_name_key", "string"), ("road_main_no", "int"), ("road_sub_no", "int"))


def offered_schema(contract: Mapping[str, Any]) -> tuple[list[str], str]:
    """The names and DDL of the frame handed to the joins: the table's columns it does not derive,
    the offer's columns, and a row id."""

    from platform_contracts import spark_sql_type  # noqa: PLC0415

    own = [(c["name"], spark_sql_type(c["logical_type"])) for c in contract["columns"]
           if c["name"] not in (*RESOLVED, "row_state")]
    fields = [*own, *OFFER_COLUMNS, ("row_id", "int")]
    return [name for name, _ in fields], ", ".join(f"{name} {kind}" for name, kind in fields)


def link(offered: Any, names: Sequence[str], parcels: Any, titles: Any,
         crosswalk: tuple[Mapping[str, str], Any]) -> Any:
    """The offered rows with their PNU: the code path (``place_identity_resolver.resolve`` with
    ``crosswalk``) first, the address joins (``address_pnu.resolve``) for what it leaves unresolved.
    A night's load and a code-change follow both link through here (root ADR-0382, ADR-0416)."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    remap, ambiguous = crosswalk
    resolved = address_pnu.resolve(
        offered.withColumnRenamed("road_name", "road_name_raw").withColumnRenamed("road_name_key", "road_name"),
        parcels, titles, remap)
    if "source_code_pnu_raw" in names:
        by_code = place_identity_resolver.resolve(
            offered.select("row_id", F.col("source_code_pnu_raw").alias("pnu")), remap, ambiguous, parcels
        ).select("row_id", F.col("pnu").alias("_code_pnu"), F.col("place_identity_grade").alias("_grade"))
        resolved = resolved.join(by_code, "row_id", "left")
        by_code_ok = F.col("_code_pnu").isNotNull()
        resolved = (resolved
                    .withColumn("pnu", F.coalesce(F.col("_code_pnu"), F.col("pnu")))
                    .withColumn("pnu_basis", F.when(by_code_ok & (F.col("_grade") == F.lit("code_superseded")),
                                                    F.lit("legal_dong_code_superseded"))
                                .when(by_code_ok & (F.col("_grade") == F.lit(place_identity_resolver.SERVED_EDITION_GRADE)),
                                      F.lit("legal_dong_code_served_edition"))
                                .when(by_code_ok, F.lit("legal_dong_code")).otherwise(F.col("pnu_basis")))
                    .withColumn("pnu_candidate_count", F.when(by_code_ok, F.lit(1)).otherwise(F.col("pnu_candidate_count")))
                    .withColumn("pnu_unresolved_reason",
                                F.when(by_code_ok, F.lit(None).cast("string")).otherwise(F.col("pnu_unresolved_reason")))
                    .withColumn("legal_dong_code",
                                F.when(by_code_ok, F.substring(F.col("pnu"), 1, 10)).otherwise(F.col("legal_dong_code")))
                    .drop("_code_pnu", "_grade"))
    return resolved


def relinked_rows(rows: Sequence[Mapping[str, Any]], record: str, snapshot: str, ingested_at: datetime,
                  prefixes: Mapping[str, str], dongs: Mapping[tuple[str, str], str]) -> list[dict[str, Any]]:
    """The current rows again, each with what its kept address offers the joins now (root ADR-0416).

    The row keeps every value it was observed with; only the link inputs are made again. It carries
    the follow's record and instant, so the compare stamps it newer than the row it re-places."""

    out = []
    for current in rows:
        row = dict(current)
        given = offer(row, prefixes, dongs)
        road = given["road"]
        row.update(
            legal_dong_code=given["legal_dong_code"], jibun_pnu=given["jibun_pnu"], reason=given["reason"],
            road_sido=road.sido_code if road else None, road_sigungu=road.sigungu if road else None,
            road_name_key=road.road if road else None, road_main_no=road.main_no if road else None,
            road_sub_no=road.sub_no if road else None,
            source_record_id=record, source_snapshot_id=snapshot, ingested_at_utc=ingested_at,
        )
        out.append(row)
    return out


def follow_code_changes(spark: Any, catalog: str, table: str, contract: Mapping[str, Any], prefixes: Mapping[str, str],
                        dongs: Mapping[tuple[str, str], str], parcels: Any, titles: Any, now: datetime,
                        crosswalk: tuple[Mapping[str, str], Any] = ({}, ())) -> dict[str, Any] | None:
    """Re-link the current rows the 법정동 code changes since the table's last follow can move, and
    append only the ones whose link moved (root ADR-0329 §3, ADR-0416). None when there is nothing
    to follow (no change table, or the links are as of its newest snapshot)."""

    from lakehouse_ingest import append_source_object, read_ingested_objects, unquoted_table_name  # noqa: PLC0415
    from platform_contracts import column_names, current_rows  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415
    from silver_release_compare import release_changes  # noqa: PLC0415

    after = follow.change_snapshot(spark, catalog)
    if after is None:
        return None
    before = follow.last_followed(read_ingested_objects(spark, table, unquoted_table_name(table)))
    if before == after:
        return None
    columns = list(column_names(contract))
    moved = follow.changed_prefixes(spark, catalog, before, after, JOB_NAME)
    current = current_rows(spark.table(table), contract)
    if "row_state" in columns:
        current = current.where(F.col("row_state").isNull())
    affected = follow.rows_under(current, moved, ("legal_dong_code", "pnu"), "pnu").cache()
    record = follow.follow_record(after, now)
    rows = relinked_rows([r.asDict() for r in affected.collect()], record, f"{now:%Y%m%dT%H%M%SZ}", now,
                         prefixes, dongs)
    appended = 0
    if rows:
        names, ddl = offered_schema(contract)
        offered = spark.createDataFrame(
            [tuple(n if name == "row_id" else row.get(name) for name in names) for n, row in enumerate(rows)], schema=ddl)
        resolved = link(offered, names, parcels, titles, crosswalk)
        staged = (resolved.drop("road_name").withColumnRenamed("road_name_raw", "road_name")
                  .withColumn("row_state", F.lit(None).cast("string")).select(*columns))
        # Same keys on both sides: only the rows whose link moved are appended, and no removal marker.
        changes = release_changes(staged, affected, key=KEY, content=content_columns(contract), removal="row_state",
                                  source_snapshot_id=f"{now:%Y%m%dT%H%M%SZ}", ingested_at=F.lit(now).cast("timestamp"),
                                  columns=columns)
        changes = changes.join(staged.select(*KEY).distinct(), list(KEY), "left_semi").cache()
        appended = changes.count()
    else:
        changes = spark.createDataFrame([], schema=spark.table(table).schema).select(*columns)
    # The record is written even with no moved row: the next follow starts from this snapshot.
    append_source_object(spark, changes, columns, table, record, contract_table=contract["table_name"])
    checked = affected.count()
    affected.unpersist()
    return {"table": unquoted_table_name(table), "from_snapshot": before, "to_snapshot": after,
            "prefixes": None if moved is None else len(moved), "checked": checked, "appended": appended}


def _load(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="load")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--table-suffix", default="")
    parser.add_argument("--allow-non-smoke-write", action="store_true")
    args = parser.parse_args(argv)
    if not IDENTIFIER.fullmatch(args.iceberg_catalog_name):
        parser.error("--iceberg-catalog-name must be a plain SQL identifier")
    if args.table_suffix not in ("", "_smoke"):
        parser.error("--table-suffix is empty or _smoke")
    if not args.table_suffix and not args.allow_non_smoke_write:
        parser.error("writing the Silver tables needs --allow-non-smoke-write")

    from lakehouse_engine import apply_catalog_settings, assert_catalog_env  # noqa: PLC0415
    from platform_contracts import load_lakehouse_contract  # noqa: PLC0415
    from pyspark.sql import SparkSession  # noqa: PLC0415

    assert_catalog_env()
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    summary: dict[str, Any] = {"job": JOB_NAME, "status": "followed", "follows": []}
    builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    try:
        dongs = address_pnu.lakehouse_legal_dong_index(spark, args.iceberg_catalog_name)
        parcels, titles = address_pnu.lakehouse_inputs(spark, args.iceberg_catalog_name)
        prefixes = address_pnu.lakehouse_sido_prefixes(spark, args.iceberg_catalog_name)
        # ADR-0288 §1: the code pairs are read once for the run, as every parcel source does.
        crosswalk = place_identity_resolver.pairs_from_lakehouse(spark, args.iceberg_catalog_name)
        for contract_table in ROLE_TABLE.values():
            contract = load_lakehouse_contract(contract_table)
            table = f"`{args.iceberg_catalog_name}`.`silver`.`{contract_table.split('.', 1)[1]}{args.table_suffix}`"
            if spark.catalog.tableExists(table.replace("`", "")):
                followed = follow_code_changes(spark, args.iceberg_catalog_name, table, contract, prefixes, dongs,
                                               parcels, titles, now, crosswalk)
                if followed:
                    summary["follows"].append(followed)
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
