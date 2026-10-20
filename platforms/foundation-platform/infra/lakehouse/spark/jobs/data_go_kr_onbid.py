"""Load 차세대 온비드 public sale from Bronze into Silver (root ADR-0280).

The collector (`collect-data-go-kr-onbid`) lands, per run, one gzip JSON Lines object per API; each
line is one request: its parameters, its HTTP status and the response body as received. What a load
owes is decided from R2 alone (`plan-silver-load-objects`): the Bronze objects of the source that no
commit of the table records yet, fetched into a plan directory of the load's own run. This job reads
the plan directories and loads each object, oldest first, in one commit of its own that records it:

- an item list object into `silver.onbid_real_estate_item`: every row of an item condition that is
  new or whose content changed, and, only when the object holds the whole list (every
  (prptDivCd, pvctTrgtYn) combination read to its end, `item_pass_complete`), a removal marker for
  each condition with a current row that the pass no longer holds (root ADR-0275,
  `silver_release_compare.release_changes`);
- an item bid result object into `silver.onbid_item_bid_result`: every result answer, as given.

An object with nothing to write still commits, empty, so the table records that it was loaded.

The announcement objects stay in Bronze only. The provider's own PNU (`ltnoPnu`) is format-checked
(`Pnu::parse`'s copy in the source contract, held equal by a Rust test) and then resolved to the
CURRENT pnu by the one place-identity resolver (`place_identity_resolver`, root ADR-0288 §1): a
superseded legal-dong code is carried old → current and kept only when it is a current parcel. The
raw text stays in `ltno_pnu_raw`; a code that does not resolve leaves `pnu` null for the steward.

`parse` reads the plan directories and prints what it would load without Spark; `load` writes. The
last stdout line is `data-go-kr-onbid-summary-json {json}`.
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

JOB_NAME = "data_go_kr_onbid"
ITEM_TABLE = "silver.onbid_real_estate_item"
RESULT_TABLE = "silver.onbid_item_bid_result"
PHOTO_TABLE = "silver.onbid_real_estate_item_photo"
CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contracts" / "data-go-kr-onbid.contract.json"
PLAN_SCHEMA = "foundation-platform.silver_load_objects.v1"
SNAPSHOT_ID = re.compile(r"-(\d{8}t\d{6}z)\.gz$")
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
KST = timezone(timedelta(hours=9))
# Written by the load, never by the source: comparing them would make every row look changed.
LOAD_COLUMNS = ("source_record_id", "source_line_number", "source_snapshot_id", "ingested_at_utc", "row_state")
KEY = ("cltr_mng_no", "pbct_cdtn_no")


def load_source_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def valid_pnu(value: Any, contract: Mapping[str, Any]) -> str | None:
    """The standard PNU of the provider's ltnoPnu, or None.

    The provider writes the 11th digit in the hub dialect (0 ordinary, 1 mountain); it is converted
    with the contract's map, then the result must pass what Pnu::parse checks: the declared digit
    count, ASCII digits and a standard 대장구분 digit.
    """

    rule = contract["pnu"]
    text = str(value or "").strip()
    if len(text) != int(rule["digits"]) or not text.isascii() or not text.isdigit():
        return None
    standard = rule["provider_ledger_kind_digits"].get(text[10])
    if standard is None:
        return None
    pnu = text[:10] + standard + text[11:]
    return pnu if pnu[10] in rule["ledger_kind_digits"] else None


def kst_minute(value: Any) -> datetime | None:
    """`yyyyMMddHHmm` (or with seconds) in Korean time, as naive UTC; None when it is not one."""

    text = str(value or "").strip()
    for fmt in ("%Y%m%d%H%M%S", "%Y%m%d%H%M"):
        try:
            local = datetime.strptime(text, fmt).replace(tzinfo=KST)
        except ValueError:
            continue
        return local.astimezone(timezone.utc).replace(tzinfo=None)
    return None


def whole(value: Any) -> int | None:
    """A whole number from a number or a string of digits; None for anything else."""

    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    text = str(value).strip().replace(",", "")
    return int(text) if re.fullmatch(r"-?\d+", text) else None


def decimal(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def text_or_none(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    return text or None


def answer_header(body: Mapping[str, Any]) -> Mapping[str, Any]:
    # An error body carries its code under `result`, as the collector reads it.
    header = body.get("header")
    return header if isinstance(header, dict) else (body.get("result") or {})


def answer_items(line: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The items of one request's 00 answer; [] for an empty (03) or quota answer."""

    if line.get("http_status") != 200:
        return []
    body = json.loads(line["body"])
    if answer_header(body).get("resultCode") != "00":
        return []
    found = ((body.get("body") or {}).get("items") or {})
    found = found.get("item") if isinstance(found, dict) else None
    if isinstance(found, dict):
        return [found]
    return list(found) if isinstance(found, list) else []


def item_pass_complete(lines: Sequence[Mapping[str, Any]], contract: Mapping[str, Any]) -> bool:
    """Whether an item list object read every (prptDivCd, pvctTrgtYn) combination to its end.

    The same test the collector makes while paging: a combination ends at a 03 answer, at an empty
    page, or once its pages hold totalCount items. A combination the run never reached (budget or
    quota stopped it) has no such line, so the pass is not complete.
    """

    codes = contract["result_codes"]
    combos = {(code, flag) for code in contract["property_division_codes"] if not code.startswith("$")
              for flag in contract["item_list"]["private_contract_flags"]}
    counted: dict[tuple[str, str], int] = {}
    ended = set()
    for line in lines:
        request = line.get("request") or {}
        combo = (str(request.get("prptDivCd")), str(request.get("pvctTrgtYn")))
        if line.get("http_status") != 200:
            continue
        body = json.loads(line["body"])
        code = answer_header(body).get("resultCode")
        if code == codes["no_data"]:
            ended.add(combo)
        elif code == codes["ok"]:
            items = answer_items(line)
            counted[combo] = counted.get(combo, 0) + len(items)
            total = whole((body.get("body") or {}).get("totalCount"))
            if not items or (total is not None and counted[combo] >= total):
                ended.add(combo)
    return combos <= ended


def read_lines(path: Path) -> list[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def item_row(item: Mapping[str, Any], contract: Mapping[str, Any]) -> dict[str, Any]:
    raw_pnu = text_or_none(item.get("ltnoPnu"))
    lowest = text_or_none(item.get("lowstBidPrcIndctCont"))
    return {
        "cltr_mng_no": str(item["cltrMngNo"]).strip(),
        "pbct_cdtn_no": whole(item["pbctCdtnNo"]),
        "pnu": valid_pnu(raw_pnu, contract),
        "ltno_pnu_raw": raw_pnu,
        "rdnm_pnu_raw": text_or_none(item.get("rdnmPnu")),
        "onbid_cltr_no": whole(item.get("onbidCltrno")),
        "onbid_pbanc_no": whole(item.get("onbidPbancNo")),
        "pbct_no": whole(item.get("pbctNo")),
        "pbct_nsq": text_or_none(item.get("pbctNsq")),
        "prpt_div_cd": str(item["prptDivCd"]).strip(),
        "pvct_trgt_yn": str(item["pvctTrgtYn"]).strip(),
        "dsps_mthod_cd": text_or_none(item.get("dspsMthodCd")),
        "bid_div_cd": text_or_none(item.get("bidDivCd")),
        "bid_mthod_cd": text_or_none(item.get("bidMthodCd")),
        "cptn_mthod_cd": text_or_none(item.get("cptnMthodCd")),
        "totalamt_unpc_div_cd": text_or_none(item.get("totalamtUnpcDivCd")),
        "pbct_stat_cd": text_or_none(item.get("pbctStatCd")),
        "usg_lcls_ctgr_id": text_or_none(item.get("cltrUsgLclsCtgrId")),
        "usg_mcls_ctgr_id": text_or_none(item.get("cltrUsgMclsCtgrId")),
        "usg_scls_ctgr_id": text_or_none(item.get("cltrUsgSclsCtgrId")),
        "item_name": text_or_none(item.get("onbidCltrNm")),
        "usbd_nft": whole(item.get("usbdNft")),
        "bid_prgn_nft": whole(item.get("bidPrgnNft")),
        "bid_begins_at": kst_minute(item.get("cltrBidBgngDt")),
        "bid_ends_at": kst_minute(item.get("cltrBidEndDt")),
        "appraisal_amount_krw": whole(item.get("apslEvlAmt")),
        "lowest_bid_price_raw": lowest,
        "lowest_bid_price_krw": whole(lowest),
        "first_bid_price_krw": whole(item.get("frstBidPrc")),
        "land_area_m2": decimal(item.get("landSqms")),
        "building_area_m2": decimal(item.get("bldSqms")),
        "share_item_yn": text_or_none(item.get("alcYn")),
        "location_sido_name": text_or_none(item.get("lctnSdnm")),
        "location_sigungu_name": text_or_none(item.get("lctnSggnm")),
        "location_emd_name": text_or_none(item.get("lctnEmdNm")),
        "org_name": text_or_none(item.get("orgNm")),
        "provider_modified_at": kst_minute(item.get("mdfcnDt")),
    }


def result_row(item: Mapping[str, Any]) -> dict[str, Any]:
    lowest = text_or_none(item.get("lowstBidPrcIndctCont"))
    return {
        "cltr_mng_no": str(item["cltrMngNo"]).strip(),
        "pbct_cdtn_no": whole(item["pbctCdtnNo"]),
        "onbid_cltr_no": whole(item.get("onbidCltrno")),
        "onbid_pbanc_no": whole(item.get("onbidPbancNo")),
        "pbanc_mng_no": text_or_none(item.get("pbancMngNo")),
        "pbct_no": whole(item.get("pbctNo")),
        "pbct_nsq": text_or_none(item.get("pbctNsq")),
        "prpt_div_cd": text_or_none(item.get("prptDivCd")),
        "pbct_stat_cd": text_or_none(item.get("pbctStatCd")),
        "usbd_nft": whole(item.get("usbdNft")),
        "appraisal_amount_krw": whole(item.get("apslEvlAmt")),
        "lowest_bid_price_raw": lowest,
        "lowest_bid_price_krw": whole(lowest),
        "winning_bid_amount_krw": whole(item.get("scfbAmt")),
        "valid_bidder_count": whole(item.get("vldBddrNope")),
        "invalid_bidder_count": whole(item.get("nfctBddrNope")),
        "bid_begins_at": kst_minute(item.get("cltrBidBgngDt")),
        "bid_ends_at": kst_minute(item.get("cltrBidEndDt")),
        "opened_at": kst_minute(item.get("cltrOpbdDt")),
        "bid_amounts_raw": text_or_none(item.get("bidAmtClgCont")),
    }


def rows_of(lines: Sequence[Mapping[str, Any]], build: Any, object_key: str, snapshot_id: str,
            ingested_at: datetime) -> list[dict[str, Any]]:
    """One row per item condition of the object, from its last line that holds it.

    The list can shift while a pass pages through it, so a condition may appear on two pages; the
    later answer is the newer one.
    """

    rows: dict[tuple[str, int | None], dict[str, Any]] = {}
    for number, line in enumerate(lines, start=1):
        for item in answer_items(line):
            row = build(item)
            if not row["cltr_mng_no"] or row["pbct_cdtn_no"] is None:
                raise ValueError(f"{object_key} line {number}: an item without cltrMngNo and pbctCdtnNo")
            row.update(source_record_id=object_key, source_line_number=number, source_snapshot_id=snapshot_id,
                       ingested_at_utc=ingested_at)
            rows[(row["cltr_mng_no"], row["pbct_cdtn_no"])] = row
    return list(rows.values())


def photo_rows_of(lines: Sequence[Mapping[str, Any]], object_key: str, snapshot_id: str,
                  ingested_at: datetime) -> list[dict[str, Any]]:
    """One row per stored media file of each item: its content hash, kind and order in the list.

    The detail Bronze line carries each photo/appraisal entry with a `sha256` ref (root ADR-0290 §7);
    an entry the collector could not store has no `sha256`, so it is skipped. The last line that holds
    an (item, hash) wins, so a re-paged item keeps one row.
    """

    rows: dict[tuple[str, int | None, str], dict[str, Any]] = {}
    for number, line in enumerate(lines, start=1):
        for item in answer_items(line):
            cltr = str(item["cltrMngNo"]).strip()
            pbct = whole(item["pbctCdtnNo"])
            if not cltr or pbct is None:
                raise ValueError(f"{object_key} line {number}: an item without cltrMngNo and pbctCdtnNo")
            for media_kind, field in (("photo", "potoUrlList"), ("appraisal", "apslEvlClgList")):
                entries = item.get(field)
                if not isinstance(entries, list):
                    continue
                for sequence, entry in enumerate(entries):
                    if not isinstance(entry, dict):
                        continue
                    sha = text_or_none(entry.get("sha256"))
                    if sha is None:
                        continue
                    rows[(cltr, pbct, sha)] = {
                        "cltr_mng_no": cltr,
                        "pbct_cdtn_no": pbct,
                        "sha256": sha,
                        "media_kind": media_kind,
                        "sequence": sequence,
                        # The appraisal attachment names its organisation; a photo has no caption field.
                        "title": text_or_none(entry.get("apslEvlOrgNm")),
                        "media_format": text_or_none(entry.get("media_format")),
                        "size_bytes": whole(entry.get("size_bytes")),
                        "source_record_id": object_key,
                        "source_line_number": number,
                        "source_snapshot_id": snapshot_id,
                        "ingested_at_utc": ingested_at,
                    }
    return list(rows.values())


def source_slugs(contract: Mapping[str, Any]) -> dict[str, str]:
    """Source slug → the role its objects load as (root ADR-0014 `{provider}__{dataset}`)."""

    return {f"datagokr__{contract['item_list']['dataset_slug']}": "item_list",
            f"datagokr__{contract['item_bid_result']['dataset_slug']}": "item_bid_result",
            f"datagokr__{contract['item_detail']['dataset_slug']}": "item_photo"}


def snapshot_id(object_key: str) -> str:
    """The run id the collector put in the key (`...-20990102t030405z.gz` → `20990102T030405Z`)."""

    found = SNAPSHOT_ID.search(object_key)
    if not found:
        raise ValueError(f"{object_key} carries no run id")
    return found.group(1).upper()


def planned_objects(plan_dirs: Sequence[Path], contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Every object the plans fetched, with its role and local file, oldest run first."""

    roles = source_slugs(contract)
    objects = []
    for plan_dir in plan_dirs:
        plan = json.loads((plan_dir / "plan.json").read_text(encoding="utf-8"))
        if plan.get("schema_version") != PLAN_SCHEMA:
            raise ValueError(f"{plan_dir}: not a {PLAN_SCHEMA} plan")
        for entry in plan["objects"]:
            if entry["source_slug"] not in roles:
                raise ValueError(f"{entry['object_key']}: {entry['source_slug']} is not an Onbid source this job loads")
            objects.append({"object_key": entry["object_key"], "role": roles[entry["source_slug"]],
                            "source_snapshot_id": snapshot_id(entry["object_key"]),
                            "path": plan_dir / entry["local_path"]})
    return sorted(objects, key=lambda o: (o["source_snapshot_id"], o["object_key"]))


def read_object(entry: Mapping[str, Any], contract: Mapping[str, Any], ingested_at: datetime) -> dict[str, Any]:
    """What one object loads: its rows and, for an item list, whether the pass was complete."""

    lines = read_lines(entry["path"])
    if entry["role"] == "item_photo":
        return {"rows": photo_rows_of(lines, entry["object_key"], entry["source_snapshot_id"], ingested_at),
                "complete": False}
    build = (lambda item: item_row(item, contract)) if entry["role"] == "item_list" else result_row
    return {"rows": rows_of(lines, build, entry["object_key"], entry["source_snapshot_id"], ingested_at),
            "complete": entry["role"] == "item_list" and item_pass_complete(lines, contract)}


def content_columns(contract: Mapping[str, Any]) -> list[str]:
    return [c["name"] for c in contract["columns"] if c["name"] not in (*KEY, *LOAD_COLUMNS)]


def _summary_line(summary: Mapping[str, Any]) -> None:
    print("data-go-kr-onbid-summary-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True, default=str))


def plan_dir_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--plan-dir", action="append", default=[], required=True,
                        help="A directory `plan-silver-load-objects` wrote (plan.json + objects/); repeatable")


def _parse(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="parse")
    plan_dir_arguments(parser)
    args = parser.parse_args(argv)
    contract = load_source_contract()
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    report = []
    for entry in planned_objects([Path(d) for d in args.plan_dir], contract):
        loads = read_object(entry, contract, now)
        line = {"object": entry["object_key"], "role": entry["role"], "rows": len(loads["rows"])}
        if entry["role"] == "item_list":
            line.update(complete=loads["complete"], rows_with_pnu=sum(1 for row in loads["rows"] if row["pnu"]))
        report.append(line)
    _summary_line({"job": JOB_NAME, "status": "parsed", "objects": report})
    return 0


def ensure_table(spark: Any, args: argparse.Namespace, contract_table: str) -> tuple[str, dict[str, Any], list[str], str]:
    """Creates the table when it is new and evolves it to its contract; its name, contract, columns, schema."""

    from platform_contracts import (  # noqa: PLC0415
        column_names, create_table_columns_sql, evolve_iceberg_table_to_contract, load_lakehouse_contract,
        partition_clause_sql, spark_sql_type,
    )

    contract = load_lakehouse_contract(contract_table)
    name = contract_table.split(".", 1)[1] + args.table_suffix
    table = f"`{args.iceberg_catalog_name}`.`{args.iceberg_namespace}`.`{name}`"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{args.iceberg_catalog_name}`.`{args.iceberg_namespace}`")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({create_table_columns_sql(contract)}) USING iceberg "
              f"{partition_clause_sql(contract)} TBLPROPERTIES ('format-version' = '2', "
              "'write.parquet.compression-codec' = 'zstd')")
    evolve_iceberg_table_to_contract(spark, table, contract)
    schema = ", ".join(f"{c['name']} {spark_sql_type(c['logical_type'])}" for c in contract["columns"])
    return table, contract, list(column_names(contract)), schema


def _load(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="load")
    plan_dir_arguments(parser)
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-namespace", default="silver")
    parser.add_argument("--table-suffix", default="", help="`_smoke` for a smoke run")
    parser.add_argument("--allow-non-smoke-write", action="store_true")
    args = parser.parse_args(argv)
    for label in ("iceberg_catalog_name", "iceberg_namespace"):
        if not IDENTIFIER.fullmatch(getattr(args, label)):
            parser.error(f"{label} must be a plain SQL identifier")
    if args.table_suffix not in ("", "_smoke"):
        parser.error("--table-suffix is empty or _smoke")
    if not args.table_suffix and not args.allow_non_smoke_write:
        parser.error("writing the Silver tables needs --allow-non-smoke-write")

    import address_pnu  # noqa: PLC0415
    import place_identity_resolver  # noqa: PLC0415
    from lakehouse_engine import apply_catalog_settings, assert_catalog_env  # noqa: PLC0415
    from lakehouse_ingest import append_source_object  # noqa: PLC0415
    from platform_contracts import REMOVED, current_rows  # noqa: PLC0415
    from pyspark.sql import SparkSession, functions as F  # noqa: PLC0415
    from silver_release_compare import release_changes  # noqa: PLC0415

    assert_catalog_env()
    source = load_source_contract()
    objects = planned_objects([Path(d) for d in args.plan_dir], source)
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    summary: dict[str, Any] = {"job": JOB_NAME, "objects": [], "status": "loaded"}
    builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()

    try:
        item_table, item_contract, item_columns, item_schema = ensure_table(spark, args, ITEM_TABLE)
        result_table, _, result_columns, result_schema = ensure_table(spark, args, RESULT_TABLE)
        photo_table, _, photo_columns, photo_schema = ensure_table(spark, args, PHOTO_TABLE)
        if any(entry["role"] == "item_list" for entry in objects):
            # ADR-0288 §1: every parcel source resolves its code to the current pnu here, not by
            # copying the source code. The pairs and the current parcels are read once for the run.
            remap, ambiguous = place_identity_resolver.pairs_from_lakehouse(spark, args.iceberg_catalog_name)
            parcels, _titles = address_pnu.lakehouse_inputs(spark, args.iceberg_catalog_name)
        for entry in objects:
            loads = read_object(entry, source, now)
            rows = loads["rows"]
            line: dict[str, Any] = {"object": entry["object_key"], "role": entry["role"], "rows": len(rows)}
            if entry["role"] == "item_list":
                built = spark.createDataFrame([tuple(r.get(c) for c in item_columns) for r in rows], schema=item_schema)
                # The source code (ltnoPnu, kept in ltno_pnu_raw) becomes the CURRENT pnu, or null.
                resolved = place_identity_resolver.resolve(built, remap, ambiguous, parcels).cache()
                graded = {row["place_identity_grade"]: row["count"]
                          for row in resolved.groupBy("place_identity_grade").count().collect()}
                staged = resolved.select(*item_columns)
                current = current_rows(spark.table(item_table), item_contract)
                changes = release_changes(
                    staged, current, key=KEY, content=content_columns(item_contract), removal="row_state",
                    source_snapshot_id=entry["source_snapshot_id"], ingested_at=F.lit(now).cast("timestamp"),
                    columns=item_columns,
                )
                if not loads["complete"]:
                    # A pass that stopped early says nothing about what it did not read.
                    changes = changes.where(F.col("row_state").isNull() | (F.col("row_state") != REMOVED))
                # A marker is made from the condition's last row; it belongs to this object's load.
                changes = changes.withColumn("source_record_id", F.lit(entry["object_key"])).cache()
                line.update(complete=loads["complete"], source_coded=sum(1 for row in rows if row["pnu"]),
                            resolved_by_grade=graded,
                            changes=changes.count(), removed=changes.where(F.col("row_state") == REMOVED).count())
                line["appended"] = append_source_object(spark, changes, item_columns, item_table, entry["object_key"],
                                                         contract_table=ITEM_TABLE)
                changes.unpersist()
                resolved.unpersist()
            elif entry["role"] == "item_photo":
                frame = spark.createDataFrame([tuple(r.get(c) for c in photo_columns) for r in rows], schema=photo_schema)
                line["appended"] = append_source_object(spark, frame, photo_columns, photo_table, entry["object_key"])
            else:
                frame = spark.createDataFrame([tuple(r.get(c) for c in result_columns) for r in rows], schema=result_schema)
                line["appended"] = append_source_object(spark, frame, result_columns, result_table, entry["object_key"],
                                                         contract_table=RESULT_TABLE)
            summary["objects"].append(line)
    finally:
        spark.stop()
    _summary_line(summary)
    return 0


COMMANDS = {"parse": _parse, "load": _load}


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] not in COMMANDS:
        print(f"usage: {Path(__file__).name} {{{','.join(COMMANDS)}}} ...", file=sys.stderr)
        return 2
    return COMMANDS[argv[0]](argv[1:])


if __name__ == "__main__":
    sys.exit(main())
