#!/usr/bin/env python3
"""Load the authority legal-dong code registry into the temporal dictionary (ADR-0103 ②).

`reference.legal_dong_code` is the versioned record of every 법정동코드 the authority
(행정표준코드, `getStanReginCdList`) has ever issued, with its 생성일 and 말소일. The
resolver reads it instead of any hardcoded code→region fact, so the next merger or split
is absorbed by data, not by an engineer.

The parse, resolve, and derive kernels are pure on purpose: the lane that runs
`infra/lakehouse/spark/tests` has no PySpark install, and a module-level import would make
every check that touches this file skip itself. Only `main` and the Spark helpers it calls
(`append_derived_crosswalk`, `read_prior_snapshot_rows`) touch Spark, and they import PySpark
inside their own bodies rather than at module load, so importing this module stays PySpark-free.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from collections import namedtuple
from pathlib import Path
from typing import Any, Sequence

from lakehouse_engine import apply_catalog_settings, assert_catalog_env, assert_iceberg_runtime_loaded, iceberg_packages
from lakehouse_ingest import append_batch_once
from platform_contracts import (column_names, create_table_columns_sql, evolve_iceberg_table_to_contract,
                                load_lakehouse_contract, partition_clause_sql)

CONTRACT = "reference.legal_dong_code"
CROSSWALK_CONTRACT = "reference.sigungu_canonical_crosswalk"

# One authority row, already split into fields, in this order. The collector extracts them
# from the `getStanReginCdList` payload; this order is the only wire contract between the
# extract and the parse, so a reordered extract fails loudly on the digit checks below
# rather than landing names in code columns.
REGISTRY_FIELDS = (
    "region_cd",
    "sido_cd",
    "sgg_cd",
    "umd_cd",
    "ri_cd",
    "locatadd_nm",
    "locathigh_cd",
    "created_date",
    "abolished_date",
)

REGION_CD_PATTERN = re.compile(r"[0-9]{10}")
DATE_PATTERN = re.compile(r"[0-9]{8}")

SigunguResolution = namedtuple("SigunguResolution", ("canonical", "unresolved"))


def parse_registry_row(fields: Sequence[str]) -> dict[str, Any]:
    """Turn one authority registry row into a contract row.

    `is_current` is derived, not carried: the authority states currency by leaving 말소일
    empty, and deriving it here keeps the rule in one place. A malformed code or date is
    refused rather than repaired — a fabricated registry entry would later resolve real
    parcels onto a place that does not exist.
    """
    if len(fields) != len(REGISTRY_FIELDS):
        raise ValueError(
            f"registry row carries {len(fields)} fields; expected "
            f"{len(REGISTRY_FIELDS)} ({', '.join(REGISTRY_FIELDS)})"
        )
    row: dict[str, Any] = {
        name: ("" if value is None else str(value).strip())
        for name, value in zip(REGISTRY_FIELDS, fields)
    }
    if not REGION_CD_PATTERN.fullmatch(row["region_cd"]):
        raise ValueError(f"region_cd must be a 10-digit 법정동코드: {row['region_cd']!r}")
    if not DATE_PATTERN.fullmatch(row["created_date"]):
        raise ValueError(f"created_date must be YYYYMMDD: {row['created_date']!r}")
    if row["abolished_date"] and not DATE_PATTERN.fullmatch(row["abolished_date"]):
        raise ValueError(f"abolished_date must be empty or YYYYMMDD: {row['abolished_date']!r}")
    row["is_current"] = row["abolished_date"] == ""
    return row


# getStanReginCdList names its code/name fields exactly like the registry's columns, so the map is
# nearly 1:1. Only 생성일 is renamed (adpt_de -> created_date); 말소일 has no API field at all — the
# current-only API never carries one, and abolition is inferred later by snapshot diff (ADR-0104).
STANREGIN_API_FIELD = {
    "region_cd": "region_cd",
    "sido_cd": "sido_cd",
    "sgg_cd": "sgg_cd",
    "umd_cd": "umd_cd",
    "ri_cd": "ri_cd",
    "locatadd_nm": "locatadd_nm",
    "locathigh_cd": "locathigh_cd",
    "created_date": "adpt_de",
}


def stanregin_api_row_to_registry_fields(row: dict[str, Any]) -> list[str]:
    """Map one `getStanReginCdList` JSON row to a REGISTRY_FIELDS-ordered list (ADR-0104).

    This is the bridge from the collected Bronze API payload to the registry job's input: each API
    row becomes one delimited registry row (`abolished_date` always empty). The output feeds
    `parse_registry_row`, which enforces the digit/date rules — a malformed API row is refused there
    rather than repaired.
    """
    fields: list[str] = []
    for name in REGISTRY_FIELDS:
        if name == "abolished_date":
            fields.append("")
            continue
        value = row.get(STANREGIN_API_FIELD[name])
        fields.append("" if value is None else str(value).strip())
    return fields


def resolve_sigungu(code: str, as_of: str, crosswalk: Sequence[dict[str, Any]]) -> SigunguResolution:
    """Resolve one source 시군구 code through the crosswalk at a point in time.

    ``crosswalk`` rows are shaped like ``reference.sigungu_canonical_crosswalk``:
    ``source_code``, ``canonical_code``, ``valid_from``, ``valid_to`` (empty = open).
    A code the crosswalk does not name at ``as_of`` comes back ``unresolved`` — never a
    fabricated canonical code — so the caller quarantines it instead of misjoining
    (ADR-0103 ④). Two live rows naming different canonicals is a broken dictionary and is
    refused outright rather than picked from.
    """
    code = ("" if code is None else str(code).strip())
    as_of = ("" if as_of is None else str(as_of).strip())
    if not DATE_PATTERN.fullmatch(as_of):
        raise ValueError(f"as_of must be YYYYMMDD: {as_of!r}")
    canonicals = {
        row["canonical_code"]
        for row in crosswalk
        if row["source_code"] == code
        and (row.get("valid_from") or "") <= as_of
        and (not row.get("valid_to") or as_of <= row["valid_to"])
    }
    if len(canonicals) > 1:
        raise ValueError(
            f"crosswalk names {code} to multiple canonical codes at {as_of}: {sorted(canonicals)}"
        )
    if not canonicals:
        return SigunguResolution(canonical=None, unresolved=True)
    return SigunguResolution(canonical=canonicals.pop(), unresolved=False)


def crosswalk_rows_from_seed(seed: dict[str, Any]) -> list[dict[str, str]]:
    """Flatten the checked-in seed json into crosswalk table rows.

    The seed links each authority-current 시군구 code (12xxx) to the superseded cadastral
    code (29xxx/46xxx) the parcel map still carries, so the two sources meet on one place.
    The link opens at the merger date the seed's sido entry states and stays open until the
    authority says otherwise; per-entry provenance survives so the steward can later confirm
    or replace each pair against the authority's 생성/말소 file.
    """
    opened = {
        entry["current_code"]: entry.get("effective_from", "")
        for entry in seed.get("sido", [])
    }
    return [
        {
            "source_code": entry["current_code"],
            "canonical_code": entry["superseded_code"],
            "valid_from": opened.get(entry["current_code"][:2], ""),
            "valid_to": "",
            "provenance": entry["provenance"],
        }
        for entry in seed["sigungu"]
    ]


def _is_sigungu_code(region_cd: str) -> bool:
    """True for a 시군구-level 법정동코드: 10 digits, 읍면동·리 zeroed, and a real 시군구 part.

    A 시도 code also ends in five zeros (e.g. ``2900000000``), so the 시군구 part must be non-zero
    to keep 시도 rows out of the 시군구 crosswalk.
    """
    return len(region_cd) == 10 and region_cd[5:] == "00000" and region_cd[2:5] != "000"


def _sigungu_name(locatadd_nm: str) -> str:
    """The last whitespace-separated token of a 법정동 address is its 시군구 name."""
    parts = (locatadd_nm or "").split()
    return parts[-1] if parts else ""


def _registry_sigungu_groups(
    rows: Sequence[dict[str, Any]],
) -> tuple[dict[tuple[str, str], list[str]], dict[tuple[str, str], list[str]]]:
    """Group a batch's 시군구 rows by (name, date) into (created, abolished).

    Shared by the derivation and the steward-review feed so the two read the batch the same way
    and cannot drift.
    """
    created: dict[tuple[str, str], list[str]] = {}
    abolished: dict[tuple[str, str], list[str]] = {}
    for row in rows:
        region_cd = str(row.get("region_cd", "") or "")
        if not _is_sigungu_code(region_cd):
            continue
        name = _sigungu_name(row.get("locatadd_nm", ""))
        if not name:
            continue
        abolished_date = (row.get("abolished_date") or "").strip()
        created_date = (row.get("created_date") or "").strip()
        if abolished_date:
            abolished.setdefault((name, abolished_date), []).append(region_cd)
        elif created_date:
            created.setdefault((name, created_date), []).append(region_cd)
    return created, abolished


def derive_crosswalk_from_registry(rows: Sequence[dict[str, Any]]) -> list[dict[str, str]]:
    """Derive the 시군구 crosswalk from one registry batch that carries both 생성 and 말소 rows.

    A merger abolishes each old 시군구 code and creates a new one that keeps the same
    시군구 name on the same date (a source that stamps both the 말소일 and the 생성일). So a
    superseded code and a freshly-created code that share a 시군구 name at one date are the
    same place, and we link current -> superseded (the direction the parcel map still carries).
    This reproduces the checked-in seed from such a source's 생성/말소 dates alone (ADR-0103 ③).
    Use it for the checked-in seed reload or a KIKcd 말소코드포함 import; the current-only REST API
    carries no 말소 row, so its snapshots feed `derive_crosswalk_across_snapshots` instead (ADR-0104).

    Only an unambiguous 1:1 name match at one date is emitted. A renamed 시군구 (no name match)
    or a many-to-one merger (several old codes onto one name) is left out on purpose so the
    steward decides it instead of the code guessing (ADR-0103 ④); `steward_review_from_registry`
    reports exactly those left-out cases.
    """
    created, abolished = _registry_sigungu_groups(rows)
    crosswalk: list[dict[str, str]] = []
    for (name, date), old_codes in sorted(abolished.items()):
        new_codes = created.get((name, date), [])
        if len(old_codes) != 1 or len(new_codes) != 1:
            continue  # ambiguous (rename / many-to-one) -> steward, not a guess
        crosswalk.append(
            {
                "source_code": new_codes[0][:5],
                "canonical_code": old_codes[0][:5],
                "valid_from": date,
                "valid_to": "",
                "provenance": f"derived:registry:sigungu-name+date:{name}:{date}",
            }
        )
    return crosswalk


def steward_review_from_registry(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """Report the 말소 rows a single batch could not auto-resolve, for the steward (ADR-0103 ④).

    Every abolished 시군구 whose (name, date) does not map to exactly one created code is withheld
    from the crosswalk; instead of dropping it silently, name it here with why — a many-to-one
    merger or a renamed/abolished-without-replacement 시군구 — so a human decides it.
    """
    created, abolished = _registry_sigungu_groups(rows)
    review: list[dict[str, Any]] = []
    for (name, date), old_codes in sorted(abolished.items()):
        new_codes = created.get((name, date), [])
        if len(old_codes) == 1 and len(new_codes) == 1:
            continue  # cleanly resolved, not for the steward
        review.append(
            {
                "sigungu_name": name,
                "reason": "ambiguous_name_match" if new_codes else "abolished_without_replacement",
                "old_codes": sorted(code[:5] for code in old_codes),
                "new_codes": sorted(code[:5] for code in new_codes),
                "as_of": date,
                "method": "registry",
            }
        )
    return review


def _snapshot_sigungu_diff(
    prev_rows: Sequence[dict[str, Any]],
    curr_rows: Sequence[dict[str, Any]],
) -> tuple[dict[str, list[str]], dict[str, list[tuple[str, str]]]]:
    """Return ``(vanished_by_name, appeared_by_name)`` between two current-only snapshots.

    ``vanished_by_name``: 시군구 name -> region_cds present in the earlier snapshot and gone from the
    later one. ``appeared_by_name``: name -> ``(region_cd, created_date)`` present in the later one
    and not the earlier one. Shared by the derivation and the steward-review feed so the two read the
    diff identically and cannot drift.
    """

    def sigungu_index(rows: Sequence[dict[str, Any]]) -> dict[str, tuple[str, str]]:
        index: dict[str, tuple[str, str]] = {}
        for row in rows:
            region_cd = str(row.get("region_cd", "") or "")
            if not _is_sigungu_code(region_cd):
                continue
            name = _sigungu_name(row.get("locatadd_nm", ""))
            if not name:
                continue
            index[region_cd] = (name, (row.get("created_date") or "").strip())
        return index

    prev = sigungu_index(prev_rows)
    curr = sigungu_index(curr_rows)

    vanished_by_name: dict[str, list[str]] = {}
    for code, (name, _date) in prev.items():
        if code not in curr:
            vanished_by_name.setdefault(name, []).append(code)
    appeared_by_name: dict[str, list[tuple[str, str]]] = {}
    for code, (name, date) in curr.items():
        if code not in prev:
            appeared_by_name.setdefault(name, []).append((code, date))
    return vanished_by_name, appeared_by_name


def derive_crosswalk_across_snapshots(
    prev_rows: Sequence[dict[str, Any]],
    curr_rows: Sequence[dict[str, Any]],
) -> list[dict[str, str]]:
    """Derive the 시군구 crosswalk from two consecutive current-only authority snapshots (ADR-0104).

    The authority REST API (`getStanReginCdList`) lists only current codes with a 생성일 and no
    말소일: a merger shows up as a 시군구 code that was in the earlier snapshot and is gone from the
    later one (abolished), while a freshly-created code carrying the merger date appears in the
    later one. A code that vanished and a code that appeared under the same 시군구 name are the same
    place, and we link current -> superseded (the direction the parcel map still carries), opening
    the link at the new code's 생성일.

    Only an unambiguous 1:1 name match is emitted: exactly one vanished and one appeared 시군구 for
    a name. A rename (no name match), a many-to-one merger, or a one-to-many split is left out for
    the steward (ADR-0103 ④) and reported by `steward_review_across_snapshots`. The provenance prefix
    (`derived:snapshot-diff:…`) differs from the single-batch derivation's (`derived:registry:…`) so
    the two never collide and stay auditable, and it is stable across snapshots so re-running a diff
    appends nothing twice.
    """
    vanished_by_name, appeared_by_name = _snapshot_sigungu_diff(prev_rows, curr_rows)
    crosswalk: list[dict[str, str]] = []
    for name in sorted(set(vanished_by_name) & set(appeared_by_name)):
        old_codes = vanished_by_name[name]
        new_codes = appeared_by_name[name]
        if len(old_codes) != 1 or len(new_codes) != 1:
            continue  # ambiguous (rename / many-to-one / split) -> steward, not a guess
        new_code, date = new_codes[0]
        crosswalk.append(
            {
                "source_code": new_code[:5],
                "canonical_code": old_codes[0][:5],
                "valid_from": date,
                "valid_to": "",
                "provenance": f"derived:snapshot-diff:sigungu-name+date:{name}:{date}",
            }
        )
    return crosswalk


def steward_review_across_snapshots(
    prev_rows: Sequence[dict[str, Any]],
    curr_rows: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Report the snapshot-diff 시군구 changes that did not auto-resolve, for the steward (ADR-0103 ④).

    A change the derivation cannot pin to a clean 1:1 name match — an ambiguous match (both sides but
    not one-to-one), a code that vanished with no same-name replacement, or a new code with no
    same-name predecessor (a rename) — is named here with why, so the steward decides it instead of
    the code guessing or dropping it silently.
    """
    vanished_by_name, appeared_by_name = _snapshot_sigungu_diff(prev_rows, curr_rows)
    review: list[dict[str, Any]] = []
    for name in sorted(set(vanished_by_name) | set(appeared_by_name)):
        old_codes = vanished_by_name.get(name, [])
        new_codes = appeared_by_name.get(name, [])
        if len(old_codes) == 1 and len(new_codes) == 1:
            continue  # cleanly resolved, not for the steward
        if old_codes and new_codes:
            reason = "ambiguous_name_match"
        elif old_codes:
            reason = "abolished_without_replacement"
        else:
            reason = "appeared_without_predecessor"
        dates = sorted({date for _code, date in new_codes if date})
        review.append(
            {
                "sigungu_name": name,
                "reason": reason,
                "old_codes": sorted(code[:5] for code in old_codes),
                "new_codes": sorted(code[:5] for code, _date in new_codes),
                "as_of": dates[0] if len(dates) == 1 else "",
                "method": "snapshot-diff",
            }
        )
    return review


def derive_unit_transitions(crosswalk: Sequence[dict[str, Any]]) -> list[dict[str, str]]:
    """Promote crosswalk pairs into canonical administrative-unit transition edges (ADR-0105).

    Each `reference.sigungu_canonical_crosswalk` row links a 현행 신코드 (`source_code`) to the
    폐지 구코드 (`canonical_code`) the parcel map still carries. A transition edge is that same fact
    read as identity over time, so its direction is inverted to predecessor -> successor
    (`from_code` = 폐지 구코드, `to_code` = 현행 신코드), matching ADR-0103's `superseded_by`.

    `transition_kind` is named by the cardinality of the change, so one kernel serves both the
    auto-derived crosswalk (only clean 1:1 pairs, so every edge is `replaced_by`) and a steward-
    approved merge or split:

    - one predecessor, one successor -> `replaced_by`
    - many predecessors, one successor -> `merged_into` (each old code merged into the one new code)
    - one predecessor, many successors -> `split_from` (each new code split from the one old code)

    A pair caught in a many-to-many reshuffle (a shared predecessor *and* a shared successor) cannot
    be classified without guessing, so it is left out for the steward (ADR-0103 ④) instead of being
    stamped with a made-up kind. The provenance `derived:transition:{kind}:{from}->{to}:{valid_from}`
    is deterministic, so re-running over the whole append-only crosswalk yields the same edges and the
    publisher lands none twice — the same discipline `append_derived_crosswalk` relies on.
    """

    def edge(row: dict[str, Any]) -> tuple[str, str, str] | None:
        from_code = (row.get("canonical_code") or "").strip()  # 폐지 구코드 = predecessor
        to_code = (row.get("source_code") or "").strip()  # 현행 신코드 = successor
        valid_from = (row.get("valid_from") or "").strip()
        # A self-pair is not a change (the table's from_unit <> to_unit), and a dateless pair cannot
        # open an effective period (its period_check) — neither is a transition. Both passes below
        # read a row through this one gate so the cardinality count and the emission cannot drift.
        if not from_code or not to_code or from_code == to_code or not valid_from:
            return None
        return from_code, to_code, valid_from

    successors_by_pred: dict[str, set[str]] = {}
    predecessors_by_succ: dict[str, set[str]] = {}
    for row in crosswalk:
        parsed = edge(row)
        if parsed is None:
            continue
        from_code, to_code, _valid_from = parsed
        successors_by_pred.setdefault(from_code, set()).add(to_code)
        predecessors_by_succ.setdefault(to_code, set()).add(from_code)

    transitions: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in crosswalk:
        parsed = edge(row)
        if parsed is None:
            continue
        from_code, to_code, valid_from = parsed
        if (from_code, to_code) in seen:
            continue
        seen.add((from_code, to_code))
        n_succ = len(successors_by_pred[from_code])
        n_pred = len(predecessors_by_succ[to_code])
        if n_succ > 1 and n_pred > 1:
            continue  # many-to-many reshuffle -> steward, not a guess (ADR-0103 ④)
        kind = "split_from" if n_succ > 1 else "merged_into" if n_pred > 1 else "replaced_by"
        transitions.append(
            {
                "from_code": from_code,
                "to_code": to_code,
                "transition_kind": kind,
                "valid_from": valid_from,
                "provenance": f"derived:transition:{kind}:{from_code}->{to_code}:{valid_from}",
            }
        )
    transitions.sort(key=lambda t: (t["valid_from"], t["from_code"], t["to_code"]))
    return transitions


def append_derived_crosswalk(spark, T, prefix, derived_crosswalk):
    """Append newly-derived crosswalk pairs to reference.sigungu_canonical_crosswalk.

    The table is append-only and keyed on ``provenance`` (its declared load unit): each pair's
    provenance is ``derived:registry:sigungu-name+date:{name}:{date}``, stable across snapshots,
    so re-deriving the same merger yields the same object and lands nothing twice. Only the pairs
    the table does not already record are handed to the batch guard: a full re-derivation legiti-
    mately repeats every earlier pair and adds the new one, and the guard refuses a batch that is
    part-in and part-out (it is built for file loaders that must not regroup mid-run). Narrowing to
    the unrecorded provenance turns that straddle into a clean all-new append. Returns the count
    actually appended (0 when the snapshot names no new merger).
    """
    if not derived_crosswalk:
        return 0
    contract = load_lakehouse_contract(CROSSWALK_CONTRACT)
    names = column_names(contract)
    target = f"{prefix}.`sigungu_canonical_crosswalk`"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({create_table_columns_sql(contract)}) USING iceberg {partition_clause_sql(contract)}")
    evolve_iceberg_table_to_contract(spark, target, contract)
    recorded = {row["provenance"] for row in spark.sql(f"SELECT DISTINCT provenance FROM {target}").collect()}
    fresh = [pair for pair in derived_crosswalk if pair["provenance"] not in recorded]
    if not fresh:
        return 0
    schema = T.StructType([
        T.StructField(column["name"], T.StringType(), not column["required"])
        for column in contract["columns"]
    ])
    frame = spark.createDataFrame([tuple(pair[name] for name in names) for pair in fresh], schema=schema)
    result = append_batch_once(spark, frame, names, target, CROSSWALK_CONTRACT)
    return len(fresh) if result["appended"] else 0


def read_prior_snapshot_rows(spark, target, current_snapshot_id):
    """Return the most recent earlier snapshot's rows from `reference.legal_dong_code`.

    "Earlier" is the largest `source_snapshot_id` strictly less than this batch's (ids are
    YYYYMMDD-shaped, so lexicographic order is chronological). Returns only the fields the
    snapshot-diff derivation reads, or an empty list when no earlier snapshot exists (the
    first-ever load). The snapshot id is compared as a bound column value, never interpolated
    into SQL text.
    """
    from pyspark.sql import functions as F

    frame = spark.sql(
        f"SELECT region_cd, locatadd_nm, created_date, source_snapshot_id FROM {target}"
    )
    earlier = frame.filter(F.col("source_snapshot_id") < current_snapshot_id)
    prior = earlier.agg(F.max("source_snapshot_id").alias("sid")).collect()
    prior_id = prior[0]["sid"] if prior else None
    if not prior_id:
        return []
    return [
        {
            "region_cd": row["region_cd"],
            "locatadd_nm": row["locatadd_nm"],
            "created_date": row["created_date"],
        }
        for row in earlier.filter(F.col("source_snapshot_id") == prior_id).collect()
    ]


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True,
                        help="Authority extract: one row per line. TSV in REGISTRY_FIELDS order, or "
                             "getStanReginCdList JSON objects with --input-format stanregin-json.")
    parser.add_argument("--input-format", choices=["tsv", "stanregin-json"], default="tsv",
                        help="tsv: delimited REGISTRY_FIELDS. stanregin-json: one getStanReginCdList row per line.")
    parser.add_argument("--delimiter", default="\t")
    parser.add_argument("--source-snapshot-id", required=True)
    parser.add_argument("--iceberg-catalog-name", default="r2")
    parser.add_argument("--summary-output")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.iceberg_catalog_name):
        parser.error("invalid catalog")
    # The snapshot id becomes the batch's source_record_id, and the ingest registry
    # separates object names with a comma inside one snapshot-summary value.
    if not args.source_snapshot_id.strip() or "," in args.source_snapshot_id:
        parser.error("invalid source snapshot id")
    return args


def main(argv=None):
    args = parse_args(argv)
    from pyspark.sql import SparkSession, types as T

    assert_catalog_env()
    started = time.monotonic()
    source_record = f"mois/legal-dong-code/{args.source_snapshot_id}"
    rows = []
    for line in Path(args.input).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        if args.input_format == "stanregin-json":
            fields = stanregin_api_row_to_registry_fields(json.loads(line))
        else:
            fields = line.split(args.delimiter)
        row = parse_registry_row(fields)
        row["source_snapshot_id"] = args.source_snapshot_id
        row["source_record_id"] = source_record
        rows.append(row)
    if not rows:
        raise ValueError("registry extract produced no rows; refusing an empty append")

    builder = (SparkSession.builder.appName("foundation-platform-legal-dong-code-registry")
               .config("spark.sql.session.timeZone", "UTC"))
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    try:
        assert_iceberg_runtime_loaded(spark, iceberg_packages())
        contract = load_lakehouse_contract(CONTRACT)
        names = column_names(contract)
        # An explicit schema rather than inference: a registry column that happens to be
        # empty on every row would otherwise fail type inference.
        field_types = {"string": T.StringType(), "boolean": T.BooleanType()}
        schema = T.StructType([
            T.StructField(column["name"], field_types[column["logical_type"]], not column["required"])
            for column in contract["columns"]
        ])
        frame = spark.createDataFrame([tuple(row[name] for name in names) for row in rows], schema=schema)
        prefix = f"`{args.iceberg_catalog_name}`.`reference`"
        target = f"{prefix}.`legal_dong_code`"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {prefix}")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({create_table_columns_sql(contract)}) USING iceberg {partition_clause_sql(contract)}")
        evolve_iceberg_table_to_contract(spark, target, contract)
        appended = append_batch_once(spark, frame, names, target, CONTRACT)
        current_rows = sum(1 for row in rows if row["is_current"])
        # Auto-derive the 시군구 crosswalk from this snapshot's 생성/말소 dates and persist it to
        # reference.sigungu_canonical_crosswalk, so the authority's own dates — not a hand-kept
        # seed — become the source→canonical link the resolver reads (ADR-0103 ③). An ambiguous
        # merger never reaches here: derive_crosswalk_from_registry withholds it for the steward
        # (ADR-0103 ④).
        derived_crosswalk = derive_crosswalk_from_registry(rows)
        # The current-only REST API carries no 말소 row, so a merger is only visible as the
        # difference between two snapshots: diff this batch against the most recent earlier one
        # already in the table (ADR-0104). A first-ever load has no prior snapshot and yields
        # nothing here; the checked-in seed bootstraps the pre-collection 광주 merger. Both
        # derivations feed one append — append_derived_crosswalk dedups by provenance.
        prior_rows = read_prior_snapshot_rows(spark, target, args.source_snapshot_id)
        snapshot_diff_crosswalk = derive_crosswalk_across_snapshots(prior_rows, rows)
        crosswalk_new = append_derived_crosswalk(
            spark, T, prefix, derived_crosswalk + snapshot_diff_crosswalk
        )
        # A 시군구 change the derivation could not pin to a clean 1:1 name match is not dropped
        # silently: it is named here with why, so a steward can decide it (ADR-0103 ④). The queue
        # itself (a table + an admin surface) is later work; reporting the items in the run summary
        # is the first place an operator sees "these changes need a human".
        steward_review = steward_review_from_registry(rows) + steward_review_across_snapshots(
            prior_rows, rows
        )
        summary = {
            "source_snapshot_id": args.source_snapshot_id,
            "rows": len(rows), "current_rows": current_rows,
            "abolished_rows": len(rows) - current_rows,
            "derived_crosswalk_pairs": len(derived_crosswalk),
            "snapshot_diff_crosswalk_pairs": len(snapshot_diff_crosswalk),
            "crosswalk_pairs_appended": crosswalk_new,
            "steward_review_items": len(steward_review),
            "steward_review": steward_review,
            "elapsed_seconds": time.monotonic() - started,
            **appended,
        }
        text = json.dumps(summary, ensure_ascii=False, indent=2) + "\n"
        if args.summary_output:
            Path(args.summary_output).write_text(text, encoding="utf-8")
        print(text, flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
