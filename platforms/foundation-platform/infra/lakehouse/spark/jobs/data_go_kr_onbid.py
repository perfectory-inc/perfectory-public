"""Load 차세대 온비드 public sale from Bronze into Silver (root ADR-0280).

The collector (`collect-data-go-kr-onbid`) lands, per run, one gzip JSON Lines object per API; each
line is one request: its parameters, its HTTP status and the response body as received. The collect
script stages a run as a handoff (`pending/<run>/manifest.json` + the objects). This job loads each
handoff, oldest first:

- the item list object into `silver.onbid_real_estate_item`: every staged row of an item condition
  that is new or whose content changed, and, only when the run read the whole list (the manifest's
  `report.item_pass_complete`), a removal marker for each condition with a current row that the
  pass no longer holds (root ADR-0275, `silver_release_compare.release_changes`);
- the item bid result object into `silver.onbid_item_bid_result`: every result answer, as given.

The job only reads the handoffs; the host script that owns them moves each one to `loaded/` after
the job succeeded (the Spark container runs as another user and cannot remove them).

The announcement objects stay in Bronze only. A row keeps the provider's own PNU (`ltnoPnu`) when
`Pnu::parse` would accept it (the rule's copy in the source contract, held equal by a Rust test),
else `pnu` is null and the raw text stays in `ltno_pnu_raw`. Codes stay codes.

`parse` reads a handoff and prints what it would load without Spark; `load` writes. The last stdout
line is `data-go-kr-onbid-summary-json {json}`.
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
CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contracts" / "data-go-kr-onbid.contract.json"
MANIFEST_SCHEMA = "foundation-platform.data_go_kr_onbid_collect.v1"
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


def answer_items(line: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The items of one request's 00 answer; [] for an empty (03) or quota answer."""

    if line.get("http_status") != 200:
        return []
    body = json.loads(line["body"])
    if (body.get("header") or {}).get("resultCode") != "00":
        return []
    found = ((body.get("body") or {}).get("items") or {})
    found = found.get("item") if isinstance(found, dict) else None
    if isinstance(found, dict):
        return [found]
    return list(found) if isinstance(found, list) else []


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


def read_handoff(handoff: Path, contract: Mapping[str, Any], ingested_at: datetime) -> dict[str, Any]:
    """What one staged run loads: the item rows, whether the pass was complete, and the result rows."""

    manifest = json.loads((handoff / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("schema_version") != MANIFEST_SCHEMA:
        raise ValueError(f"{handoff}: not a {MANIFEST_SCHEMA} manifest")
    snapshot_id = manifest["source_snapshot_id"]
    loads: dict[str, Any] = {"handoff": handoff.name, "source_snapshot_id": snapshot_id,
                             "item_pass_complete": bool(manifest["report"]["item_pass_complete"]),
                             "items": None, "results": None}
    for entry in manifest["objects"]:
        lines = read_lines(handoff / entry["local_path"])
        if entry["role"] == "item_list":
            loads["items"] = (entry["object_key"], rows_of(lines, lambda item: item_row(item, contract),
                                                           entry["object_key"], snapshot_id, ingested_at))
        elif entry["role"] == "item_bid_result":
            loads["results"] = (entry["object_key"], rows_of(lines, result_row, entry["object_key"], snapshot_id,
                                                             ingested_at))
    return loads


def pending_handoffs(state_dir: Path) -> list[Path]:
    pending = state_dir / "pending"
    return sorted(path for path in pending.iterdir() if path.is_dir()) if pending.is_dir() else []


def content_columns(contract: Mapping[str, Any]) -> list[str]:
    return [c["name"] for c in contract["columns"] if c["name"] not in (*KEY, *LOAD_COLUMNS)]


def _summary_line(summary: Mapping[str, Any]) -> None:
    print("data-go-kr-onbid-summary-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True, default=str))


def _parse(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="parse")
    parser.add_argument("--state-dir", required=True)
    args = parser.parse_args(argv)
    contract = load_source_contract()
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    report = []
    for handoff in pending_handoffs(Path(args.state_dir)):
        loads = read_handoff(handoff, contract, now)
        items = loads["items"][1] if loads["items"] else []
        report.append({"handoff": handoff.name, "complete": loads["item_pass_complete"], "items": len(items),
                       "items_with_pnu": sum(1 for row in items if row["pnu"]),
                       "results": len(loads["results"][1]) if loads["results"] else 0})
    _summary_line({"job": JOB_NAME, "status": "parsed", "handoffs": report})
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
    parser.add_argument("--state-dir", required=True, help="The collect script's state root, holding pending/ and loaded/")
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

    from lakehouse_engine import apply_catalog_settings, assert_catalog_env  # noqa: PLC0415
    from lakehouse_ingest import append_batch_once  # noqa: PLC0415
    from platform_contracts import REMOVED, current_rows  # noqa: PLC0415
    from pyspark.sql import SparkSession, functions as F  # noqa: PLC0415
    from silver_release_compare import release_changes  # noqa: PLC0415

    assert_catalog_env()
    source = load_source_contract()
    state_dir = Path(args.state_dir)
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    summary: dict[str, Any] = {"job": JOB_NAME, "handoffs": [], "status": "loaded"}
    builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()

    try:
        item_table, item_contract, item_columns, item_schema = ensure_table(spark, args, ITEM_TABLE)
        result_table, _, result_columns, result_schema = ensure_table(spark, args, RESULT_TABLE)
        for handoff in pending_handoffs(state_dir):
            loads = read_handoff(handoff, source, now)
            entry: dict[str, Any] = {"handoff": handoff.name, "complete": loads["item_pass_complete"]}
            if loads["items"]:
                object_key, rows = loads["items"]
                staged = spark.createDataFrame([tuple(r.get(c) for c in item_columns) for r in rows], schema=item_schema)
                current = current_rows(spark.table(item_table), item_contract)
                changes = release_changes(
                    staged, current, key=KEY, content=content_columns(item_contract), removal="row_state",
                    source_snapshot_id=loads["source_snapshot_id"], ingested_at=F.lit(now).cast("timestamp"),
                    columns=item_columns,
                )
                if not loads["item_pass_complete"]:
                    # A pass that stopped early says nothing about what it did not read.
                    changes = changes.where(F.col("row_state").isNull() | (F.col("row_state") != REMOVED))
                # A marker is made from the condition's last row; it belongs to this object's load.
                changes = changes.withColumn("source_record_id", F.lit(object_key)).cache()
                entry["item_rows"] = len(rows)
                entry["items_with_pnu"] = sum(1 for row in rows if row["pnu"])
                entry["item_changes"] = changes.count()
                entry["item_removed"] = changes.where(F.col("row_state") == REMOVED).count()
                if entry["item_changes"]:
                    entry["item_appended"] = append_batch_once(spark, changes, item_columns, item_table, ITEM_TABLE)["appended"]
                changes.unpersist()
            if loads["results"] and loads["results"][1]:
                _, rows = loads["results"]
                frame = spark.createDataFrame([tuple(r.get(c) for c in result_columns) for r in rows], schema=result_schema)
                entry["results"] = len(rows)
                entry["results_appended"] = append_batch_once(spark, frame, result_columns, result_table, RESULT_TABLE)["appended"]
            summary["handoffs"].append(entry)
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
