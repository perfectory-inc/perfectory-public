#!/usr/bin/env python3
"""Load VWorld 필지고유번호변동연혁 (MK/30527) into `silver.parcel_number_change_history`
(root ADR-0144 §4, ADR-0145).

The provider's official old PNU -> new PNU record: one zip per 시도, each holding one `|`-delimited
UTF-8 text file. Its shape is read from `vworld-parcel-number-change-history.contract.json`; a file
whose header, member name or row width differs is refused whole (`SourceFormatError`), nothing of it
is loaded and the job exits non-zero.

A row whose 대장구분 is 0 and 본번·부번 0000 moves a whole 법정동 (`is_dong_level`): the provider
writes a renumbered 동 as one such row. A row whose 토지이동일자 is not a date is kept with
`changed_on` null and `quarantine_reason` set, and so is a row whose PNU parts are not the contract's
digits; neither is ever evidence. More than the contract's share of such rows refuses the file.

Readers: the 법정동 code pairing reads (old_pnu, new_pnu) of the rows dated in its window
(legal_dong_code_change_pairs.py, `--official-history-table`). This table is that evidence's one
home (ADR-0145 §1); the pairing never reads the parcel lineage (ADR-0145 §2).

    vworld_parcel_number_change_history.py endpoint-catalog | changed-files | landed-objects   (collection, no Spark)
    vworld_parcel_number_change_history.py stage-handoff --inventory I --evidence E --download-dir D --state-dir S
    vworld_parcel_number_change_history.py load --handoff-dir H [--validate-only]

One file is one load: loading the same Bronze object twice is refused by the ingest registry.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import sys
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

JOB_NAME = "vworld_parcel_number_change_history"
TABLE_CONTRACT = "silver.parcel_number_change_history"
CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contracts" / "vworld-parcel-number-change-history.contract.json"
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SourceFormatError(ValueError):
    """The file no longer has the shape the contract names: nothing of it is loaded."""


def load_source_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    contract = json.loads(path.read_text(encoding="utf-8"))
    if contract.get("version") != 1:
        raise ValueError(f"{path.name}: version must be 1")
    widths = contract["pnu_parts"]["widths"]
    if sum(widths) != 19 or len(widths) != len(contract["pnu_parts"]["old"]) or len(widths) != len(contract["pnu_parts"]["new"]):
        raise ValueError(f"{path.name}: pnu_parts must name five fields adding up to 19 digits")
    return contract


def is_dong_level(pnu: str) -> bool:
    """대장구분 0 and 본번·부번 0000: the row moves a whole 법정동, not one parcel."""

    return len(pnu) == 19 and pnu[10:] == "000000000"


def read_member(raw: bytes, contract: Mapping[str, Any]) -> tuple[str, str]:
    """(member name, text) of the one text file the zip must hold."""

    files = contract["files"]
    try:
        archive = zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile as error:
        raise SourceFormatError("the file is not a zip") from error
    with archive:
        names = [name for name in archive.namelist() if not name.endswith("/")]
        if len(names) != 1 or not re.fullmatch(files["member_name_pattern"], names[0]):
            raise SourceFormatError(f"expected one member matching {files['member_name_pattern']}, found {names[:3]}")
        data = archive.read(names[0])
    try:
        return names[0], data.decode(files["encoding"])
    except UnicodeDecodeError as error:
        raise SourceFormatError(f"{names[0]} is not {files['encoding']}") from error


def _date_or_none(value: str) -> date | None:
    if not re.fullmatch(r"[0-9]{8}", value):
        return None
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError:
        return None


def parse_rows(text: str, contract: Mapping[str, Any], collected_on: date | None = None) -> list[dict[str, Any]]:
    """Every distinct data row as a record, in file order. Raises `SourceFormatError` on any changed
    shape. A row repeated exactly within the file is kept once, at its first line, and
    `duplicate_count` says how many times the file held it; rows that differ only in date or reason
    are different rows (a chain of changes keeps every step). With `collected_on`, a date later
    than the day after it is not a change the provider can have recorded: quarantined
    (`implausible_land_mov_ymd`)."""

    delimiter = contract["files"]["delimiter"]
    header = contract["header"]
    lines = text.splitlines()
    if not lines or lines[0].lstrip("\ufeff").split(delimiter) != header:
        raise SourceFormatError(f"the header is not {delimiter.join(header)}")
    index = {name: position for position, name in enumerate(header)}
    parts = contract["pnu_parts"]
    rows: list[dict[str, Any]] = []
    seen: dict[tuple[str, ...], dict[str, Any]] = {}
    for line_no, line in enumerate(lines[1:], start=2):
        if not line.strip():
            continue
        cells = line.split(delimiter)
        if len(cells) != len(header):
            raise SourceFormatError(f"line {line_no} has {len(cells)} fields, the header {len(header)}")
        key = tuple(cell.strip() for cell in cells)
        if key in seen:
            seen[key]["duplicate_count"] += 1
            continue
        pnus = []
        malformed = False
        for side in ("old", "new"):
            values = [cells[index[name]].strip() for name in parts[side]]
            malformed |= any(len(value) != width or not value.isdigit() for value, width in zip(values, parts["widths"]))
            pnus.append("".join(values))
        raw_date = cells[index["LAND_MOV_YMD"]].strip()
        changed_on = _date_or_none(raw_date)
        if changed_on and collected_on and changed_on > collected_on + timedelta(days=1):
            quarantine = "implausible_land_mov_ymd"
        else:
            quarantine = "malformed_pnu" if malformed else None if changed_on else "unparseable_land_mov_ymd"
        rows.append({
            "old_pnu": pnus[0],
            "new_pnu": pnus[1],
            "is_dong_level": not malformed and is_dong_level(pnus[0]) and is_dong_level(pnus[1]),
            "reason_code": cells[index["LAND_MOV_RSN_CD"]].strip() or None,
            "changed_on": changed_on,
            "changed_on_raw": raw_date or None,
            "quarantine_reason": quarantine,
            "source_sigungu_code": cells[index["COL_ADM_SECT_CD"]].strip() or None,
            "source_line_number": line_no,
            "duplicate_count": 1,
        })
        seen[key] = rows[-1]
    return rows


def check_rows(rows: Sequence[Mapping[str, Any]], member: str, contract: Mapping[str, Any]) -> None:
    """Refuse a file below the floor or with more quarantined rows (no date, or PNU parts that are not
    the contract's digits) than the contract allows: then the layout moved, not a few rows."""

    if len(rows) < contract["bounds"]["min_rows_per_file"]:
        raise SourceFormatError(f"{member}: {len(rows)} rows, below the contract's floor")
    quarantined = sum(1 for row in rows if row["quarantine_reason"])
    bound = contract["quarantine"]
    if quarantined > bound["max_rows_any_file"] and quarantined / len(rows) > bound["max_share"]:
        raise SourceFormatError(
            f"{member}: {quarantined} of {len(rows)} rows are quarantined (no date or a malformed PNU), above the "
            "contract's share; the layout moved"
        )


def check_shrink(rows: int, previous: int | None, member: str, contract: Mapping[str, Any]) -> None:
    """The history only grows: refuse a file smaller than the one it replaces beyond the contract's share."""

    if previous and rows < previous * (1 - contract["bounds"]["max_shrink_share"]):
        raise SourceFormatError(f"{member}: {rows} rows, shrunk from {previous}; refused")


def table_rows(
    rows: Iterable[Mapping[str, Any]], *, member: str, source_record_id: str, source_snapshot_id: str,
    provider_updated_on: date, now: datetime,
) -> list[dict[str, Any]]:
    """The parsed rows with their lineage columns, as `silver.parcel_number_change_history` holds them."""

    return [
        {**row, "source_file_name": member, "source_record_id": source_record_id,
         "source_snapshot_id": source_snapshot_id, "provider_updated_on": provider_updated_on, "ingested_at_utc": now}
        for row in rows
    ]


def official_links(rows: Iterable[Mapping[str, Any]], floor: date) -> list[tuple[str, str, str]]:
    """(old PNU, new PNU, 토지이동일자 YYYYMMDD) of the rows the pairing may read: dated on or after
    `floor`, not quarantined, one link per distinct dated pair. The same link arrives from an old-name
    file and its renamed 시도's. The date says which change a link belongs to: the pairing holds a
    derived pair only against official rows of the same change (root ADR-0156)."""

    return sorted({
        (row["old_pnu"], row["new_pnu"], row["changed_on"].strftime("%Y%m%d"))
        for row in rows
        if row.get("changed_on") is not None and row["changed_on"] >= floor and not row.get("quarantine_reason")
    })


# --- the handoff (no Spark) ---------------------------------------------------------------------


def _accepted_path(state_dir: Path) -> Path:
    return state_dir / "accepted.json"


def is_reference_file(item: Mapping[str, Any], contract: Mapping[str, Any]) -> bool:
    """The provider's table definition: collected into Bronze as evidence of the layout, never loaded."""

    return bool(re.fullmatch(contract["files"]["reference_file_name_pattern"], item.get("provider_file_name", "")))


def content_key_checksum(object_key: str, file_key: str, contract: Mapping[str, Any]) -> str:
    """The SHA-256 a Bronze key of file `file_key` (`<download_ds_id>-<file_no>`) names. A key that
    names no checksum, or another file, is refused: the provider reuses file numbers, so only the
    checksum tells which upload a key holds (root ADR-0152). The plain keys of 2026-10-05
    (`30527-<n>.zip`) are such keys and are never handed off."""

    match = re.fullmatch(contract["bronze_objects"]["key_pattern"], object_key or "")
    if not match or match.group("file_key") != file_key:
        raise SourceFormatError(f"{object_key!r} is not a content-addressed key of {file_key}")
    return match.group("sha256")


def changed_files(inventory: Mapping[str, Any], accepted: Mapping[str, Any], contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Every file of 30527 (the 시도 zips and the table definition) whose 갱신일 or size differs from
    the one last accepted, or that is new. A file the listing names that is neither is refused: the
    provider added something this contract has not read."""

    data = re.compile(contract["files"]["provider_file_name_pattern"])
    selector = contract["provider_dataset_selector"]
    held = accepted.get("files", {})
    out = []
    for job in inventory.get("jobs", []):
        for item in job.get("files", []):
            if item.get("svc_cde") != selector["svc_cde"] or item.get("ds_id") != selector["ds_id"]:
                continue
            name = item.get("provider_file_name", "")
            if not data.fullmatch(name) and not is_reference_file(item, contract):
                raise SourceFormatError(f"the listing names {name!r}, neither a 시도 file nor the table definition")
            key = f"{item['download_ds_id']}-{item['file_no']}"
            last = held.get(key, {})
            if (last.get("updated_at"), last.get("size_kib")) != (item.get("updated_at"), str(item.get("size_kib"))):
                out.append(item)
    return sorted(out, key=lambda item: (item["download_ds_id"], int(item["file_no"]) if item["file_no"].isdigit() else item["file_no"]))


def select_inventory(inventory: Mapping[str, Any], files: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """The inventory narrowed to `files`, for the ingestor's FILE_INVENTORY_PATH."""

    keep = {(item["download_ds_id"], item["file_no"]) for item in files}
    jobs = []
    for job in inventory.get("jobs", []):
        selected = [item for item in job.get("files", []) if (item["download_ds_id"], item["file_no"]) in keep]
        if selected:
            jobs.append({**job, "files": selected, "file_count": len(selected)})
    return {**inventory, "jobs": jobs}


def stage_handoff(
    files: Sequence[Mapping[str, Any]], evidence: Mapping[str, Any], download_dir: Path, state_dir: Path,
    contract: Mapping[str, Any], now: datetime,
) -> dict[str, Any]:
    """Check every downloaded file and write one pending-load handoff naming them.

    `files` are the changed inventory items; `evidence` is the ingestor's evidence (it names each
    file's Bronze key); `download_dir` holds the same bytes as `<download_ds_id>-<file_no>.zip`
    (read back from Bronze). A file that fails any check refuses the whole handoff: the accepted
    state is not moved, so the next run tries again.
    """

    by_id = {f"{item['download_ds_id']}-{item['file_no']}": item for item in evidence.get("files", [])}
    accepted_path = _accepted_path(state_dir)
    accepted = json.loads(accepted_path.read_text(encoding="utf-8")) if accepted_path.exists() else {"files": {}}
    objects = []
    references = []
    for item in files:
        key = f"{item['download_ds_id']}-{item['file_no']}"
        report = by_id.get(key)
        if not report or report.get("status") not in ("succeeded", "skipped_existing") or not report.get("object_key"):
            raise SourceFormatError(f"{key} ({item['provider_file_name']}) did not land in Bronze")
        named = content_key_checksum(report["object_key"], key, contract)
        if is_reference_file(item, contract):
            references.append({"file_key": key, "object_key": report["object_key"], "updated_at": item["updated_at"],
                               "size_kib": str(item.get("size_kib"))})
            continue
        raw = (download_dir / f"{key}.zip").read_bytes()
        if hashlib.sha256(raw).hexdigest() != named:
            raise SourceFormatError(f"the bytes read back from {report['object_key']} are not the ones its key names")
        member, text = read_member(raw, contract)
        rows = parse_rows(text, contract, now.date())
        check_rows(rows, member, contract)
        check_shrink(len(rows), accepted["files"].get(key, {}).get("rows"), member, contract)
        objects.append({
            "file_key": key, "object_key": report["object_key"], "provider_file_name": item["provider_file_name"],
            "member": member, "updated_at": item["updated_at"], "base_ym": item.get("base_ym", ""),
            "rows": len(rows), "quarantined": sum(1 for row in rows if row["quarantine_reason"]),
            "duplicates": sum(row["duplicate_count"] - 1 for row in rows), "size_kib": str(item.get("size_kib")),
            "checksum_sha256": hashlib.sha256(raw).hexdigest(), "local_path": f"objects/{key}.zip",
        })
    for ref in references:
        accepted["files"][ref["file_key"]] = {"updated_at": ref["updated_at"], "size_kib": ref["size_kib"],
                                              "object_key": ref["object_key"], "reference": True}
    if not objects:
        if references:
            accepted["checked_at_utc"] = now.isoformat()
            _write_accepted(accepted_path, accepted)
        return {"status": "unchanged" if not references else "reference_only", "files": 0,
                "reference_files": len(references)}
    name = f"pnch-{now:%Y%m%dT%H%M%SZ}"
    pending = state_dir / "pending" / name
    (pending / "objects").mkdir(parents=True)
    for obj in objects:
        (pending / obj["local_path"]).write_bytes((download_dir / f"{obj['file_key']}.zip").read_bytes())
    (pending / "handoff.json").write_text(
        json.dumps({"staged_at_utc": now.isoformat(), "objects": objects}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    for obj in objects:
        accepted["files"][obj["file_key"]] = {"updated_at": obj["updated_at"], "size_kib": obj["size_kib"],
                                              "rows": obj["rows"], "object_key": obj["object_key"]}
    accepted["checked_at_utc"] = now.isoformat()
    _write_accepted(accepted_path, accepted)
    return {"status": "staged", "handoff": name, "files": len(objects), "rows": sum(obj["rows"] for obj in objects),
            "reference_files": len(references)}


def _write_accepted(path: Path, accepted: Mapping[str, Any]) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(accepted, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def handoff_rows(handoff_dir: Path, contract: Mapping[str, Any], now: datetime) -> list[tuple[dict[str, Any], list[dict[str, Any]]]]:
    """(object, its table rows) for every object of one handoff, checked again against its checksum."""

    handoff = json.loads((handoff_dir / "handoff.json").read_text(encoding="utf-8"))
    collected_on = datetime.fromisoformat(handoff["staged_at_utc"]).date()
    out = []
    for obj in handoff["objects"]:
        raw = (handoff_dir / obj["local_path"]).read_bytes()
        if hashlib.sha256(raw).hexdigest() != obj["checksum_sha256"]:
            raise SourceFormatError(f"{obj['local_path']} is not the file the handoff checked")
        if content_key_checksum(obj["object_key"], obj["file_key"], contract) != obj["checksum_sha256"]:
            raise SourceFormatError(f"{obj['object_key']} does not name the bytes the handoff holds")
        member, text = read_member(raw, contract)
        rows = parse_rows(text, contract, collected_on)
        check_rows(rows, member, contract)
        updated = date.fromisoformat(obj["updated_at"])
        out.append((obj, table_rows(rows, member=member, source_record_id=obj["object_key"],
                                    source_snapshot_id=f"{obj['file_key']}@{obj['updated_at']}",
                                    provider_updated_on=updated, now=now)))
    return out


# --- commands -----------------------------------------------------------------------------------


def _stage(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="stage-handoff")
    parser.add_argument("--inventory", required=True, help="The ingestor's file inventory, narrowed to the changed files.")
    parser.add_argument("--evidence", required=True, help="The ingestor's evidence of the same run.")
    parser.add_argument("--download-dir", required=True, help="The same files read back from Bronze, <download_ds_id>-<file_no>.zip.")
    parser.add_argument("--state-dir", required=True)
    args = parser.parse_args(argv)
    contract = load_source_contract()
    inventory = json.loads(Path(args.inventory).read_text(encoding="utf-8"))
    files = [item for job in inventory.get("jobs", []) for item in job.get("files", [])]
    evidence = json.loads(Path(args.evidence).read_text(encoding="utf-8"))
    result = stage_handoff(files, evidence, Path(args.download_dir), Path(args.state_dir), contract,
                           datetime.now(timezone.utc).replace(microsecond=0))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


def _changed(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="changed-files")
    parser.add_argument("--inventory", required=True)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--output", required=True, help="The inventory narrowed to the changed files.")
    args = parser.parse_args(argv)
    contract = load_source_contract()
    inventory = json.loads(Path(args.inventory).read_text(encoding="utf-8"))
    accepted_path = _accepted_path(Path(args.state_dir))
    accepted = json.loads(accepted_path.read_text(encoding="utf-8")) if accepted_path.exists() else {}
    files = changed_files(inventory, accepted, contract)
    Path(args.output).write_text(json.dumps(select_inventory(inventory, files), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(len(files))
    return 0


def _endpoint_catalog(argv: Sequence[str]) -> int:
    """The endpoint catalog narrowed to 30527, so planning and inventory list this dataset only."""

    parser = argparse.ArgumentParser(prog="endpoint-catalog")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--summary-output", required=True, help="The inventory summary CSV the plan reads beside the catalog.")
    args = parser.parse_args(argv)
    contract = load_source_contract()
    catalog = json.loads(Path(args.catalog).read_text(encoding="utf-8"))
    entries = [entry for entry in catalog["endpoints"] if entry.get("endpoint_slug") == contract["endpoint_slug"]]
    if len(entries) != 1 or entries[0].get("provider_dataset_selector") != contract["provider_dataset_selector"]:
        raise SourceFormatError(f"the endpoint catalog must hold {contract['endpoint_slug']} once, with the contract's selector")
    Path(args.output).write_text(json.dumps({**catalog, "endpoints": entries}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    Path(args.summary_output).write_text(inventory_summary_csv(contract), encoding="utf-8")
    return 0


def inventory_summary_csv(contract: Mapping[str, Any]) -> str:
    """The one-dataset summary `plan-vworld-dataset-collection` reads: the contract's expected listing."""

    selector, listing = contract["provider_dataset_selector"], contract["expected_listing"]
    pages = -(-listing["file_count"] // 10)
    return ("module,svc_cde,ds_id,file_pages,file_count,large_file_count,listed_gib\n"
            f"vworld_dataset,{selector['svc_cde']},{selector['ds_id']},{pages},{listing['file_count']},0,{listing['listed_gib']:.2f}\n")


def landed_objects(evidence: Mapping[str, Any], contract: Mapping[str, Any]) -> list[tuple[str, str]]:
    """(file key, Bronze object key) of every 시도 file the ingestor landed or already held: the ones
    read back and checked. The table definition stays in Bronze unread."""

    landed = sorted(
        (f"{item['download_ds_id']}-{item['file_no']}", item["object_key"])
        for item in evidence.get("files", [])
        if item.get("status") in ("succeeded", "skipped_existing") and item.get("object_key")
        and not is_reference_file(item, contract)
    )
    for file_key, object_key in landed:
        content_key_checksum(object_key, file_key, contract)
    return landed


def _landed(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="landed-objects")
    parser.add_argument("--evidence", required=True)
    args = parser.parse_args(argv)
    evidence = json.loads(Path(args.evidence).read_text(encoding="utf-8"))
    for file_key, object_key in landed_objects(evidence, load_source_contract()):
        print(file_key, object_key)
    return 0


def _load(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="load")
    parser.add_argument("--handoff-dir", required=True)
    parser.add_argument("--summary-output")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-namespace", default="silver")
    parser.add_argument("--iceberg-table", default="parcel_number_change_history")
    parser.add_argument("--allow-non-smoke-write", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args(argv)
    for label in ("iceberg_catalog_name", "iceberg_namespace", "iceberg_table"):
        if not IDENTIFIER.fullmatch(getattr(args, label)):
            parser.error(f"{label} must be a plain SQL identifier")
    if not args.validate_only and not args.iceberg_table.endswith("_smoke") and not args.allow_non_smoke_write:
        parser.error(f"writing {args.iceberg_table} needs --allow-non-smoke-write")
    now = datetime.now(timezone.utc).replace(microsecond=0)
    loads = handoff_rows(Path(args.handoff_dir), load_source_contract(), now)
    summary: dict[str, Any] = {
        "job": JOB_NAME, "contract": TABLE_CONTRACT, "files": len(loads),
        "rows": sum(len(rows) for _, rows in loads),
        "dong_level_rows": sum(1 for _, rows in loads for row in rows if row["is_dong_level"]),
        "quarantined_rows": sum(1 for _, rows in loads for row in rows if row["quarantine_reason"]),
        "duplicate_rows_folded": sum(row["duplicate_count"] - 1 for _, rows in loads for row in rows),
        "status": "validated" if args.validate_only else "ready", "appended_files": 0,
    }
    if not args.validate_only:
        from lakehouse_engine import apply_catalog_settings, assert_catalog_env  # noqa: PLC0415
        from lakehouse_ingest import append_batch_once  # noqa: PLC0415
        from platform_contracts import (  # noqa: PLC0415
            column_names, create_table_columns_sql, evolve_iceberg_table_to_contract, load_lakehouse_contract,
            partition_clause_sql, spark_sql_type,
        )
        from pyspark.sql import SparkSession  # noqa: PLC0415

        assert_catalog_env()
        contract = load_lakehouse_contract(TABLE_CONTRACT)
        columns = column_names(contract)
        builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
        spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
        try:
            table = f"`{args.iceberg_catalog_name}`.`{args.iceberg_namespace}`.`{args.iceberg_table}`"
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{args.iceberg_catalog_name}`.`{args.iceberg_namespace}`")
            spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({create_table_columns_sql(contract)}) USING iceberg "
                      f"{partition_clause_sql(contract)} TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')")
            evolve_iceberg_table_to_contract(spark, table, contract)
            schema = ", ".join(f"{c['name']} {spark_sql_type(c['logical_type'])}" for c in contract["columns"])
            for _, rows in loads:  # one Bronze object is one load unit
                frame = spark.createDataFrame([tuple(row.get(name) for name in columns) for row in rows], schema=schema)
                if append_batch_once(spark, frame, columns, table, TABLE_CONTRACT)["appended"]:
                    summary["appended_files"] += 1
        finally:
            spark.stop()
    if args.summary_output:
        Path(args.summary_output).write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("parcel-number-change-history-summary-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


COMMANDS = {
    "endpoint-catalog": _endpoint_catalog, "changed-files": _changed, "landed-objects": _landed,
    "stage-handoff": _stage, "load": _load,
}


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] not in COMMANDS:
        print(f"usage: {Path(__file__).name} {{{','.join(COMMANDS)}}} ...", file=sys.stderr)
        return 2
    return COMMANDS[argv[0]](argv[1:])


if __name__ == "__main__":
    sys.exit(main())
