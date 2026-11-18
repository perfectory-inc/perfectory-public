"""Load 산업입지정보시스템(ILIS) 산업단지 고시 reads and attachments from Bronze into Silver (root ADR-0484).

The collector (`collect-industrial-complex-notice-attachments`) lands, per run, gzip JSON Lines
objects of at most `notice_detail.commit_every` lines; each line is one notice detail read with the
response as received, each attachment of it annotated with what the collector did (its hash,
format, size, source URL and store outcome). What a load owes is decided from R2 alone
(`plan-silver-load-objects` against the read table). For each object, oldest first, this job appends:

- to `silver.industrial_complex_notice_attachment` the attachments stored in the shared media store
  (store outcome `stored` or `deduplicated`) whose (notice, hash) row is new or changed — the index
  the media reconciler and, later, the intelligence platform read the store through;
- then to `silver.industrial_complex_notice_detail` one row per read, failed reads included: the
  record the collector's next frontier is computed from. The attachment table commits first, so an
  object the read table records has its attachments.

It reads nothing inside the files (root ADR-0484 §1). `parse` reads the plan directories without
Spark. The last stdout line is `industrial-complex-notice-summary-json {json}`.
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

JOB_NAME = "industryland_or_kr"
CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"
CONTRACT_PATH = CONTRACTS / "industryland-or-kr.contract.json"
PLAN_SCHEMA = "foundation-platform.silver_load_objects.v1"
SNAPSHOT_ID = re.compile(r"-(\d{8}t\d{6}z)-\d{3}\.gz$")
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DETAIL_TABLE = "silver.industrial_complex_notice_detail"
ATTACHMENT_TABLE = "silver.industrial_complex_notice_attachment"
ATTACHMENT_KEY = ("notice_sequence", "sha256")
LOAD_COLUMNS = ("source_record_id", "source_line_number", "source_snapshot_id", "ingested_at_utc")
IN_STORE = ("stored", "deduplicated")


def load_source_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def text_or_none(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    return text or None


def whole(value: Any) -> int | None:
    text = str(value if value is not None else "").strip().replace(",", "")
    return int(text) if re.fullmatch(r"-?\d+", text) else None


def ymd(value: Any) -> date | None:
    try:
        return datetime.strptime(str(value or "").strip(), "%Y-%m-%d").date()
    except ValueError:
        return None


def utc(value: Any) -> datetime | None:
    """An RFC 3339 time as a naive UTC datetime, the tables' timestamp form."""

    from platform_contracts import rfc3339_utc  # noqa: PLC0415

    return rfc3339_utc(value)


def snapshot_id(object_key: str) -> str:
    """The run id the collector put in the key (`...-20990102t030405z-000.gz` → `20990102T030405Z`)."""

    found = SNAPSHOT_ID.search(object_key)
    if not found:
        raise ValueError(f"{object_key} carries no run id")
    return found.group(1).upper()


def source_slug(contract: Mapping[str, Any]) -> str:
    return "industrylandorkr__" + contract["notice_detail"]["dataset_slug"]


def planned_objects(plan_dirs: Sequence[Path], contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Every object the plans fetched, with its local file, oldest run first."""

    slug = source_slug(contract)
    objects = []
    for plan_dir in plan_dirs:
        plan = json.loads((plan_dir / "plan.json").read_text(encoding="utf-8"))
        if plan.get("schema_version") != PLAN_SCHEMA:
            raise ValueError(f"{plan_dir}: not a {PLAN_SCHEMA} plan")
        for entry in plan["objects"]:
            if entry["source_slug"] != slug:
                raise ValueError(f"{entry['object_key']}: {entry['source_slug']} is not a source this job loads")
            objects.append({"object_key": entry["object_key"], "source_snapshot_id": snapshot_id(entry["object_key"]),
                            "path": plan_dir / entry["local_path"]})
    return sorted(objects, key=lambda o: (o["source_snapshot_id"], o["object_key"]))


def rows_of(lines: Sequence[Mapping[str, Any]], contract: Mapping[str, Any], object_key: str, snapshot: str,
            ingested_at: datetime) -> dict[str, list[dict[str, Any]]]:
    """One object's rows: a read row per line, and an attachment row per file in the store."""

    detail = contract["notice_detail"]
    reads, attachments = [], {}
    for number, line in enumerate(lines, start=1):
        notice = whole(line.get("notice_sequence"))
        requested = utc(line.get("requested_at_utc"))
        if notice is None or requested is None:
            continue
        load = {"source_record_id": object_key, "source_line_number": number, "source_snapshot_id": snapshot,
                "ingested_at_utc": ingested_at}
        result = (line.get("body") or {}).get(detail["result_pointer"].strip("/")) or {}
        if not isinstance(result, dict):
            result = {}
        files = result.get(detail["files_field"]) or []
        files = files if isinstance(files, list) else []
        stored = 0
        code = text_or_none(result.get("danji_cd"))
        for file in files:
            sha256 = text_or_none(file.get("sha256"))
            media_format = text_or_none(file.get("media_format"))
            if file.get("store_outcome") not in IN_STORE or not sha256 or not media_format:
                continue
            stored += 1
            attachments[(notice, sha256)] = {
                "notice_sequence": notice, "sha256": sha256, "file_sequence": whole(file.get("file_sequence")) or 0,
                "official_complex_code": code, "file_title": text_or_none(file.get("file_name")),
                "file_extension_raw": text_or_none(file.get("file_extension")),
                "provider_file_size": whole(file.get("file_size")),
                "provider_file_path": text_or_none(file.get(contract["attachments"]["path_field"])),
                "provider_file_id": text_or_none(file.get(contract["attachments"]["name_field"])),
                "source_url": text_or_none(file.get("source_url")) or "", "media_format": media_format,
                "size_bytes": whole(file.get("size_bytes")) or 0,
                "fetched_at_utc": utc(file.get("fetched_at_utc")) or requested, **load}
        reads.append({
            "notice_sequence": notice, "requested_at_utc": requested, "http_status": whole(line.get("http_status")),
            "read_outcome": str(line.get("read_outcome") or "failed"), "official_complex_code": code,
            "notice_title": text_or_none(result.get("noti_title")), "notice_number": text_or_none(result.get("noti_num")),
            "notice_date": ymd(result.get("noti_dtm")), "notice_type_code": text_or_none(result.get("noti_type")),
            "notice_type_raw": text_or_none(result.get("noti_type_nm")),
            "complex_kind_code": text_or_none(result.get("noti_dantype")),
            "complex_kind_raw": text_or_none(result.get("noti_dantype_nm")),
            "file_count": len(files) if result else None, "files_stored": stored if result else None, **load})
    return {"rows": reads, "attachment_rows": list(attachments.values())}


def read_object(entry: Mapping[str, Any], contract: Mapping[str, Any], ingested_at: datetime) -> dict[str, Any]:
    with gzip.open(entry["path"], "rt", encoding="utf-8") as handle:
        lines = [json.loads(line) for line in handle if line.strip()]
    return {"object_key": entry["object_key"],
            **rows_of(lines, contract, entry["object_key"], entry["source_snapshot_id"], ingested_at)}


def _summary_line(summary: Mapping[str, Any]) -> None:
    print("industrial-complex-notice-summary-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True, default=str))


def load_object(spark: Any, args: argparse.Namespace, entry: Mapping[str, Any], read: Mapping[str, Any]) -> dict[str, Any]:
    """Commits one object: its new attachments, then its reads."""

    from data_go_kr_onbid import changed_only  # noqa: PLC0415 - the same changed-key compare
    from lakehouse_ingest import append_source_object  # noqa: PLC0415
    from platform_contracts import (  # noqa: PLC0415
        column_names, current_rows, ensure_contract_table, load_lakehouse_contract, spark_sql_type,
    )

    counts: dict[str, Any] = {"object": entry["object_key"], "reads": len(read["rows"]),
                              "attachments": len(read["attachment_rows"])}
    for name, rows in ((ATTACHMENT_TABLE, read["attachment_rows"]), (DETAIL_TABLE, read["rows"])):
        contract = load_lakehouse_contract(name)
        table = f"`{args.iceberg_catalog_name}`.`silver`.`{name.split('.', 1)[1]}{args.table_suffix}`"
        ensure_contract_table(spark, table, contract)
        columns = list(column_names(contract))
        ddl = ", ".join(f"{c['name']} {spark_sql_type(c['logical_type'])}" for c in contract["columns"])
        staged = spark.createDataFrame([tuple(row.get(c) for c in columns) for row in rows], ddl)
        if name == ATTACHMENT_TABLE:
            content = [c for c in columns if c not in (*ATTACHMENT_KEY, *LOAD_COLUMNS)]
            staged = changed_only(staged, current_rows(spark.table(table), contract), ATTACHMENT_KEY, content)
        staged = staged.select(*columns).cache()
        short = name.split(".", 1)[1]
        counts[f"{short}_appended_rows"] = staged.count()
        counts[f"{short}_appended"] = append_source_object(spark, staged, columns, table, entry["object_key"],
                                                           contract_table=name)
        staged.unpersist()
    return counts


def plan_dir_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--plan-dir", action="append", default=[], required=True,
                        help="A directory `plan-silver-load-objects` wrote (plan.json + objects/); repeatable")


def _load(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="load")
    plan_dir_arguments(parser)
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
    from pyspark.sql import SparkSession  # noqa: PLC0415

    assert_catalog_env()
    source = load_source_contract()
    objects = planned_objects([Path(d) for d in args.plan_dir], source)
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    summary: dict[str, Any] = {"job": JOB_NAME, "status": "loaded", "objects": []}
    builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{args.iceberg_catalog_name}`.`silver`")
        for entry in objects:
            summary["objects"].append(load_object(spark, args, entry, read_object(entry, source, now)))
    finally:
        spark.stop()
    _summary_line(summary)
    return 0


def _parse(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="parse")
    plan_dir_arguments(parser)
    args = parser.parse_args(argv)
    source = load_source_contract()
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    report = []
    for entry in planned_objects([Path(d) for d in args.plan_dir], source):
        read = read_object(entry, source, now)
        report.append({"object": entry["object_key"], "reads": len(read["rows"]),
                       "attachments": len(read["attachment_rows"])})
    _summary_line({"job": JOB_NAME, "status": "parsed", "objects": report})
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
