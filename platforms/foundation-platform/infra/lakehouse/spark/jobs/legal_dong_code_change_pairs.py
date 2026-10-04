#!/usr/bin/env python3
"""Pair 법정동 code changes and derive the 시군구 crosswalk the hub loaders read (root ADR-0143 §3–5,
ADR-0144).

One run reads one code.go.kr full-table snapshot (`reference.legal_dong_code_snapshot`, loaded by
`legal_dong_code_snapshot_to_reference.py`) and the changes already recorded
(`reference.legal_dong_code_change`), optionally two parcel snapshots and the parcel lineage, then:

1. pairs every change on or after the contract's floor from data only
   (`code_go_kr_legal_dong.pair_changes`): what the change table already records, the date + name
   rule, the 지번 sets of the two parcel snapshots (`--parcels-before-snapshot-id`,
   `--parcels-after-snapshot-id`; off without them), the official parcel-number history in the
   parcel lineage (`--parcel-lineage-table`; off without it), and a roll-up of 동 pairs to the
   시군구 and 시도 above them;
2. appends the changes not yet recorded to `reference.legal_dong_code_change`, and the 시군구
   crosswalk entries not yet recorded to `reference.sigungu_canonical_crosswalk`;
3. writes the crosswalk projection (`--projection-output`) the hub exports read, naming the snapshot
   it was built from and the crosswalk table snapshot that holds its entries, and the steward list
   (`--review-output`).

The 코드변경안내 notice board is not a source (ADR-0144).

`--validate-only` does all of it from a local full-table file (`--table-html`) and writes nothing to
the lakehouse: the real-data check runs this way.

A steward approval is staged as a file (`stage-steward-decision`, no Spark), checked against the
steward list the last run wrote; the next run (`--steward-decisions`) checks it again against the
table, appends it as `steward:<who>` rows and pairs with it. Only codes on the steward list are
accepted.
"""

from __future__ import annotations

import argparse
import functools
import json
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import code_go_kr_legal_dong as cg

JOB_NAME = "legal_dong_code_change_pairs"
CHANGE_CONTRACT = "reference.legal_dong_code_change"
CROSSWALK_CONTRACT = "reference.sigungu_canonical_crosswalk"
SNAPSHOT_CONTRACT = "reference.legal_dong_code_snapshot"
PROJECTION_SCHEMA = "foundation-platform.sigungu_crosswalk_projection.v1"
PARCEL_SOURCE_PATH = Path(__file__).resolve().parents[2] / "contracts" / "vworld-parcel-source-objects.json"
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SNAPSHOT_ID = re.compile(r"^[A-Za-z0-9._:-]{1,200}$")
STEWARD_ID = re.compile(r"^[A-Za-z0-9._@-]{1,64}$")


def cadastral_sido(parcel_source: Mapping[str, Any]) -> list[str]:
    """The 시도 the cadastral parcel set carries: its `granularity: sido` objects."""

    return sorted({obj["region_code"][:2] for obj in parcel_source["objects"] if obj["granularity"] == "sido"})


def change_key(kind: str, source: str, old_code: str, new_code: str) -> str:
    return f"{kind}|{source}|{old_code}|{new_code}"


def decided_pairs(recorded: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    """Every pair the change table records, with its source: they stand in every later run."""

    return [
        {"old_code": row["old_code"], "new_code": row["new_code"], "source": row["source"],
         "rule_verdict": row.get("rule_verdict") or "", "detail": row.get("detail") or ""}
        for row in recorded
        if row["kind"] == "pair"
    ]


def pairing_run_id(table_source_record_id: str, now: datetime) -> str:
    """This run's `derivation_run_id`: the table it paired and the second it ran.

    The column is the change table's load unit (the ingest registry skips a unit it already holds),
    so it must be new on every run. Keyed on the table alone, a steward-only rerun on the same
    snapshot named the unit the first run had loaded and its new rows were skipped without an error.
    A row is recorded once by its `change_key` (the pair and its evidence), not by its run.
    """

    return f"code-go-kr-pairing:{table_source_record_id}:{now:%Y%m%dT%H%M%SZ}"


def append_new_rows(append: Callable[[Sequence[Mapping[str, Any]]], bool], rows: Sequence[Mapping[str, Any]], what: str) -> bool:
    """Appends rows the table does not hold yet, and refuses when the registry says it does.

    The rows were already filtered against the table (by `change_key`, by crosswalk provenance),
    so a registry that answers "already loaded" means their load unit collided with an earlier
    run's: appending nothing would drop them silently.
    """

    if not rows:
        return False
    if not append(rows):
        units = sorted({str(row.get("derivation_run_id") or row.get("provenance")) for row in rows})
        raise ValueError(
            f"the ingest registry already holds the load unit {units[:3]} of {len(rows)} {what} rows the table "
            "does not hold; they would be dropped without a word. Each run must load under its own unit."
        )
    return True


def change_row(kind: str, source: str, old: str, new: str, run_id: str, now: datetime, **fields: Any) -> dict[str, Any]:
    """One `reference.legal_dong_code_change` row; blank codes and fields are stored as null."""

    return {
        "change_key": change_key(kind, source, old, new), "kind": kind, "old_code": old or None,
        "new_code": new or None, "level": fields.get("level"), "effective_date": fields.get("effective_date") or None,
        "source": source, "rule_verdict": fields.get("rule_verdict"), "detail": fields.get("detail") or None,
        "derivation_run_id": run_id, "recorded_at_utc": now,
    }


def plan_derivation(
    rows: Sequence[Mapping[str, str]],
    recorded: Sequence[Mapping[str, Any]],
    contract: Mapping[str, Any],
    cadastral: Sequence[str],
    derivation_run_id: str,
    now: datetime,
    jibun: cg.JibunEvidence | None = None,
    official_links: Iterable[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    """Everything one run decides, without touching the lakehouse.

    Returns the change rows not yet recorded, the crosswalk (projection body and table rows), the
    steward list and the counts. Pure, so the planted-failure tests drive it directly.
    """

    pairing = contract["pairing"]
    result = cg.pair_changes(rows, pairing["floor_date"], decided_pairs(recorded), jibun,
                             None if official_links is None else list(official_links),
                             float(pairing["jibun_overlap_min_share"]))
    known = {row["change_key"] for row in recorded}
    fresh: list[dict[str, Any]] = []
    for pair in result.pairs:
        row = change_row("pair", pair["source"], pair["old_code"], pair["new_code"], derivation_run_id, now,
                         level=pair["level"], effective_date=pair["effective_date"], rule_verdict=pair["rule_verdict"],
                         detail=pair["detail"])
        if row["change_key"] not in known:
            known.add(row["change_key"])
            fresh.append(row)

    crosswalk, crosswalk_review = cg.sigungu_crosswalk(result.pairs, cadastral)
    crosswalk_rows = [
        {"source_code": entry["current_code"], "canonical_code": entry["superseded_code"],
         "valid_from": entry["valid_from"], "valid_to": "",
         "provenance": f"{entry['provenance']}:{entry['current_code']}->{entry['superseded_code']}"}
        for entry in crosswalk["sigungu"]
    ]
    review = result.review + crosswalk_review
    by_source: dict[str, int] = {}
    for pair in result.pairs:
        by_source[pair["rule_verdict"]] = by_source.get(pair["rule_verdict"], 0) + 1
    return {
        "fresh_changes": fresh,
        "crosswalk": crosswalk,
        "crosswalk_rows": crosswalk_rows,
        "review": review,
        "counts": {
            "table_rows": len(rows),
            "pairs": len(result.pairs),
            "pairs_by_evidence": by_source,
            "jibun_evidence": jibun.label if jibun is not None else "off",
            "fresh_changes": len(fresh),
            "crosswalk_entries": len(crosswalk["sigungu"]),
            "governed_sido": [sido["current_code"] for sido in crosswalk["sido"]],
            "review_items": len(review),
            "review_by_status": {
                status: sum(1 for item in review if item.get("status", "steward") == status)
                for status in sorted({item.get("status", "steward") for item in review})
            },
        },
    }


def projection_document(
    plan: Mapping[str, Any], snapshot_date: str, snapshot_record: str, crosswalk_snapshot_id: str, now: datetime
) -> dict[str, Any]:
    """The file the hub exports read in place of the hand seed (root ADR-0143 §5)."""

    return {
        "schema_version": PROJECTION_SCHEMA,
        "built_at_utc": now.isoformat(),
        "legal_dong_snapshot_date": snapshot_date,
        "legal_dong_snapshot_record": snapshot_record,
        "crosswalk_table": CROSSWALK_CONTRACT,
        "crosswalk_table_snapshot_id": crosswalk_snapshot_id,
        "sido": plan["crosswalk"]["sido"],
        "sigungu": plan["crosswalk"]["sigungu"],
    }


def steward_rows(
    review: Sequence[Mapping[str, Any]],
    approvals: Sequence[str],
    steward: str,
    reason: str,
    table_codes: set[str] | None,
    derivation_run_id: str,
    now: datetime,
) -> list[dict[str, Any]]:
    """Steward rows for approvals of items on the current steward list; anything else is refused.

    `approvals` are `OLD=NEW`. OLD must be an open pair item whose status is `steward`: an item
    awaiting data, or a split, is the data's to decide (ADR-0144 §4). NEW must be one of its
    candidates when it has any, and a code the full table holds in every case (`table_codes`; None
    when staging a decision without the table, the pairing run checks it again).
    """

    if not STEWARD_ID.fullmatch(steward):
        raise ValueError(f"--steward must be a plain id: {steward!r}")
    if not reason.strip():
        raise ValueError("--reason is required: the decision must say why")
    open_pairs = {item["old_code"]: item for item in review if item["kind"] == "pair"}
    source = f"steward:{steward}"
    rows = []
    for approval in approvals:
        old, sep, new = approval.partition("=")
        if not sep or not cg.CODE_PATTERN.fullmatch(old) or not cg.CODE_PATTERN.fullmatch(new):
            raise ValueError(f"--approve takes OLD=NEW with two 10-digit codes: {approval!r}")
        item = open_pairs.get(old)
        if item is None:
            raise ValueError(f"{old} is not on the steward list; only listed changes can be approved")
        if item.get("status", "steward") != "steward":
            raise ValueError(f"{old} is {item['status']} ({item.get('jibun', '')}); the data decides it, not a steward (ADR-0144 §4)")
        if (table_codes is not None and new not in table_codes) or (item["candidates"] and new not in item["candidates"]):
            raise ValueError(f"{new} is not a candidate for {old} ({item['candidates'] or 'a code in the full table'})")
        rows.append(change_row("pair", source, old, new, derivation_run_id, now, level=cg.code_level(old),
                               rule_verdict="steward", detail=reason))
    if not rows:
        raise ValueError("a steward decision needs at least one --approve")
    return rows


# --- steward decisions ------------------------------------------------------------------------


def stage_steward_decision(argv: list[str]) -> int:
    """`stage-steward-decision`: write one steward decision file for the next pairing run (no Spark).

    The decision is checked against the steward list the last pairing run wrote, so an approval of a
    change that is not listed, or of a code that is not a candidate, is refused here, at the
    steward's terminal, not in the scheduled run. The file is immutable once written; the next run
    checks it again against the table and records it (`steward:<who>` rows) or sets it aside as
    rejected, and reports which.
    """

    parser = argparse.ArgumentParser(prog="stage-steward-decision")
    parser.add_argument("--review", required=True, help="steward-review.json the last pairing run wrote")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--steward", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--approve", action="append", default=[])
    args = parser.parse_args(argv)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    review = json.loads(Path(args.review).read_text(encoding="utf-8"))["review"]
    steward_rows(review, args.approve, args.steward, args.reason, None, "staging", now)
    decision = {"steward": args.steward, "reason": args.reason, "approve": args.approve, "staged_at_utc": now.isoformat()}
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{now:%Y%m%dT%H%M%SZ}-{args.steward}.json"
    with path.open("x", encoding="utf-8") as handle:  # never overwrite a decision
        handle.write(json.dumps(decision, ensure_ascii=False, indent=2) + "\n")
    print(path)
    return 0


def fold_steward_decisions(
    decisions: Sequence[tuple[str, Mapping[str, Any]]],
    review: Sequence[Mapping[str, Any]],
    table_codes: set[str],
    now: datetime,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """The steward rows of every decision that still applies, and each decision's verdict.

    A decision the steward list no longer supports (the change was paired since, or the code left
    the table) is not recorded; its verdict says why, so the operator sees it instead of losing it.
    """

    rows: list[dict[str, Any]] = []
    verdicts: dict[str, str] = {}
    for name, decision in decisions:
        try:
            rows.extend(
                steward_rows(review, decision.get("approve", []), decision.get("steward", ""), decision.get("reason", ""),
                             table_codes, f"steward:{decision.get('steward', '')}:{Path(name).stem}", now)
            )
            verdicts[name] = "recorded"
        except ValueError as error:
            verdicts[name] = f"rejected: {error}"
    return rows, verdicts


# --- inputs -----------------------------------------------------------------------------------


def evidence_codes(rows: Sequence[Mapping[str, str]], floor: str) -> tuple[set[str], set[str]]:
    """The 동·리 whose 지번 the pairing can use: (abolished on or after the floor, current ones
    created on or after it). Only these are read from the parcel snapshots."""

    before = {row["region_cd"] for row in rows if row["status"] == cg.ABOLISHED and row.get("abolished_date", "") >= floor
              and cg.code_level(row["region_cd"]) in cg.LEAF_LEVELS}
    after = {row["region_cd"] for row in rows if row["status"] == cg.EXISTS and row.get("created_date", "") >= floor
             and cg.code_level(row["region_cd"]) in cg.LEAF_LEVELS}
    return before, after


def _read_pnu_file(path: str) -> list[str]:
    return [line.strip() for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]


# --- Spark ------------------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--snapshot-date", required=True, help="YYYY-MM-DD of the full-table snapshot to pair.")
    parser.add_argument("--table-source-record-id", required=True, help="The Bronze key of that full table.")
    parser.add_argument("--table-html", help="With --validate-only: the local full table instead of the snapshot table.")
    parser.add_argument("--recorded-changes", help="With --validate-only: a JSON list of recorded change rows.")
    parser.add_argument("--parcels-before-snapshot-id", help="silver.parcel_boundaries source_snapshot_id taken before the change.")
    parser.add_argument("--parcels-after-snapshot-id", help="silver.parcel_boundaries source_snapshot_id taken after it.")
    parser.add_argument("--parcels-before-pnus", help="With --validate-only: one PNU per line, before the change.")
    parser.add_argument("--parcels-after-pnus", help="With --validate-only: one PNU per line, after it.")
    parser.add_argument("--parcel-lineage-table", help="The parcel lineage table whose official links settle renumbered 지번.")
    parser.add_argument("--steward-decisions", help="A directory of staged steward decision files to fold in.")
    parser.add_argument("--projection-output")
    parser.add_argument("--review-output")
    parser.add_argument("--summary-output")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-namespace", default="reference")
    parser.add_argument("--snapshot-table", default="legal_dong_code_snapshot")
    parser.add_argument("--change-table", default="legal_dong_code_change")
    parser.add_argument("--crosswalk-table", default="sigungu_canonical_crosswalk")
    parser.add_argument("--parcel-table", default="silver.parcel_boundaries")
    parser.add_argument("--allow-non-smoke-write", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args(argv)
    for label in ("iceberg_catalog_name", "iceberg_namespace", "snapshot_table", "change_table", "crosswalk_table"):
        if not IDENTIFIER.fullmatch(getattr(args, label)):
            parser.error(f"{label} must be a plain SQL identifier")
    for label in ("parcel_table", "parcel_lineage_table"):
        value = getattr(args, label)
        if value is not None and not all(IDENTIFIER.fullmatch(part) for part in value.split(".")):
            parser.error(f"{label} must be namespace.table")
    for label in ("parcels_before_snapshot_id", "parcels_after_snapshot_id"):
        value = getattr(args, label)
        if value is not None and not SNAPSHOT_ID.fullmatch(value):
            parser.error(f"{label} is not a snapshot id")
    if (args.parcels_before_snapshot_id is None) != (args.parcels_after_snapshot_id is None):
        parser.error("the 지번 evidence needs both --parcels-before-snapshot-id and --parcels-after-snapshot-id")
    if (args.parcels_before_pnus is None) != (args.parcels_after_pnus is None):
        parser.error("the 지번 evidence needs both --parcels-before-pnus and --parcels-after-pnus")
    date.fromisoformat(args.snapshot_date)
    if args.validate_only and not args.table_html:
        parser.error("--validate-only reads --table-html")
    if not args.validate_only:
        for label in ("change_table", "crosswalk_table"):
            if not getattr(args, label).endswith("_smoke") and not args.allow_non_smoke_write:
                parser.error(f"writing {getattr(args, label)} needs --allow-non-smoke-write")
    return args


def _write_json(path: str | None, value: Any) -> None:
    if path:
        Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _read_decisions(directory: str | None) -> list[tuple[str, dict[str, Any]]]:
    if not directory:
        return []
    return [(path.name, json.loads(path.read_text(encoding="utf-8"))) for path in sorted(Path(directory).glob("*.json"))]


def _schema(T, contract: Mapping[str, Any]):
    types = {"string": T.StringType(), "timestamp": T.TimestampType()}
    return T.StructType([T.StructField(c["name"], types[c["logical_type"]], not c["required"]) for c in contract["columns"]])


def _ensure_table(spark, table: str, contract: Mapping[str, Any]) -> None:
    from platform_contracts import create_table_columns_sql, evolve_iceberg_table_to_contract, partition_clause_sql  # noqa: PLC0415

    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({create_table_columns_sql(contract)}) USING iceberg {partition_clause_sql(contract)}")
    evolve_iceberg_table_to_contract(spark, table, contract)


def _append(spark, T, table: str, contract_name: str, rows: Sequence[Mapping[str, Any]]) -> bool:
    from lakehouse_ingest import append_batch_once  # noqa: PLC0415
    from platform_contracts import column_names, load_lakehouse_contract  # noqa: PLC0415

    if not rows:
        return False
    contract = load_lakehouse_contract(contract_name)
    names = column_names(contract)
    frame = spark.createDataFrame([tuple(row.get(name) for name in names) for row in rows], schema=_schema(T, contract))
    return bool(append_batch_once(spark, frame, names, table, contract_name)["appended"])


def _qualified(catalog: str, name: str) -> str:
    return ".".join(f"`{part}`" for part in [catalog, *name.split(".")])


def _snapshot_pnus(spark, F, table: str, snapshot_id: str, codes: set[str]) -> list[str]:
    """The PNUs of one parcel snapshot under `codes` only (a handful of 동, not the country)."""

    if not codes:
        return []
    frame = spark.table(table).filter(F.col("source_snapshot_id") == F.lit(snapshot_id))
    frame = frame.filter(F.substring(F.col("pnu"), 1, 10).isin(sorted(codes)))
    return [row["pnu"] for row in frame.select("pnu").collect()]


def _official_links(spark, F, table: str, codes: set[str]) -> list[tuple[str, str]]:
    """The parcel lineage's 필지고유번호변동연혁 links out of `codes` (ADR-0144 §3.3; graded
    `evidence_strong` there). Its history-text links are left out: the lineage writes them through its
    own dong pairing, so they would only echo it."""

    if not codes:
        return []
    frame = spark.table(table).filter(
        (F.col("evidence_kind") == F.lit(cg.pl.PARCEL_NUMBER_HISTORY)) & F.col("predecessor_pnu").isNotNull()
    )
    frame = frame.filter(F.substring(F.col("predecessor_pnu"), 1, 10).isin(sorted(codes)))
    return [(row["predecessor_pnu"], row["successor_pnu"]) for row in frame.select("predecessor_pnu", "successor_pnu").collect()]


def _emit(args: argparse.Namespace, plan: Mapping[str, Any], snapshot_id: str, now: datetime, summary: dict[str, Any]) -> None:
    _write_json(args.projection_output, projection_document(plan, args.snapshot_date, args.table_source_record_id, snapshot_id, now))
    _write_json(args.review_output, {"review": plan["review"]})
    _write_json(args.summary_output, summary)
    print("legal-dong-code-change-pairs-summary-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True, default=str))


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv[:1] == ["stage-steward-decision"]:
        return stage_steward_decision(argv[1:])
    args = parse_args(argv)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    contract = cg.load_source_contract()
    floor = contract["pairing"]["floor_date"]
    cadastral = cadastral_sido(json.loads(PARCEL_SOURCE_PATH.read_text(encoding="utf-8")))
    run_id = pairing_run_id(args.table_source_record_id, now)
    decisions = _read_decisions(args.steward_decisions)

    if args.validate_only:
        rows = cg.parse_full_table_html(Path(args.table_html).read_text(encoding="utf-8"), contract)
        recorded = json.loads(Path(args.recorded_changes).read_text(encoding="utf-8")) if args.recorded_changes else []
        jibun = None
        if args.parcels_before_pnus:
            before_codes, after_codes = evidence_codes(rows, floor)
            jibun = cg.JibunEvidence(cg.jibun_sets(_read_pnu_file(args.parcels_before_pnus), before_codes),
                                     cg.jibun_sets(_read_pnu_file(args.parcels_after_pnus), after_codes), "local-files")
        plan = plan_derivation(rows, recorded, contract, cadastral, run_id, now, jibun)
        steward, verdicts = fold_steward_decisions(decisions, plan["review"], {row["region_cd"] for row in rows}, now)
        if steward:
            plan = plan_derivation(rows, recorded + steward, contract, cadastral, run_id, now, jibun)
        _emit(args, plan, "validate-only", now, {"job": JOB_NAME, "status": "validated", "steward_decisions": verdicts, **plan["counts"]})
        return 0

    from lakehouse_engine import apply_catalog_settings, assert_catalog_env  # noqa: PLC0415
    from platform_contracts import load_lakehouse_contract  # noqa: PLC0415
    from pyspark.sql import SparkSession, functions as F, types as T  # noqa: PLC0415

    assert_catalog_env()
    builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    try:
        prefix = f"`{args.iceberg_catalog_name}`.`{args.iceberg_namespace}`"
        snapshot_table = f"{prefix}.`{args.snapshot_table}`"
        change_table = f"{prefix}.`{args.change_table}`"
        crosswalk_table = f"{prefix}.`{args.crosswalk_table}`"
        _ensure_table(spark, change_table, load_lakehouse_contract(CHANGE_CONTRACT))
        _ensure_table(spark, crosswalk_table, load_lakehouse_contract(CROSSWALK_CONTRACT))
        snapshot = spark.table(snapshot_table).filter(
            (F.col("snapshot_date") == F.lit(date.fromisoformat(args.snapshot_date)))
            & (F.col("source_record_id") == F.lit(args.table_source_record_id))
        )
        rows = [
            {name: (row[name] or "") for name in ("region_cd", "full_name", "status", "parent_cd", "created_date", "abolished_date")}
            for row in snapshot.collect()
        ]
        if not rows:
            raise ValueError(f"{snapshot_table} holds no rows of {args.table_source_record_id} on {args.snapshot_date}")
        cg.check_table_size(len(rows), None, contract)
        before_codes, after_codes = evidence_codes(rows, floor)
        jibun = None
        if args.parcels_before_snapshot_id:
            parcels = _qualified(args.iceberg_catalog_name, args.parcel_table)
            jibun = cg.JibunEvidence(
                cg.jibun_sets(_snapshot_pnus(spark, F, parcels, args.parcels_before_snapshot_id, before_codes)),
                cg.jibun_sets(_snapshot_pnus(spark, F, parcels, args.parcels_after_snapshot_id, after_codes)),
                f"{args.parcels_before_snapshot_id}->{args.parcels_after_snapshot_id}",
            )
        links: list[tuple[str, str]] | None = None
        if args.parcel_lineage_table:
            links = _official_links(spark, F, _qualified(args.iceberg_catalog_name, args.parcel_lineage_table), before_codes)
        recorded = [row.asDict() for row in spark.table(change_table).collect()]
        plan = plan_derivation(rows, recorded, contract, cadastral, run_id, now, jibun, links)
        # Steward decisions are judged against the list as it stands before them, then recorded,
        # then the pairing runs again so today's projection already carries them.
        steward, verdicts = fold_steward_decisions(decisions, plan["review"], {row["region_cd"] for row in rows}, now)
        known = {row["change_key"] for row in recorded}
        steward = [row for row in steward if row["change_key"] not in known]
        append_changes = functools.partial(_append, spark, T, change_table, CHANGE_CONTRACT)
        steward_appended = False
        for _, rows_of_one in _group_by_run(steward):
            steward_appended |= append_new_rows(append_changes, rows_of_one, "steward")
        if steward:
            plan = plan_derivation(rows, recorded + steward, contract, cadastral, run_id, now, jibun, links)

        changes_appended = append_new_rows(append_changes, plan["fresh_changes"], "change")
        recorded_provenance = {row["provenance"] for row in spark.table(crosswalk_table).select("provenance").collect()}
        fresh_crosswalk = [row for row in plan["crosswalk_rows"] if row["provenance"] not in recorded_provenance]
        crosswalk_appended = append_new_rows(
            functools.partial(_append, spark, T, crosswalk_table, CROSSWALK_CONTRACT), fresh_crosswalk, "crosswalk")
        # Every projected entry must now be a row of the crosswalk table at the snapshot it names.
        held = {row["provenance"] for row in spark.table(crosswalk_table).select("provenance").collect()}
        missing = [row["provenance"] for row in plan["crosswalk_rows"] if row["provenance"] not in held]
        if missing:
            raise ValueError(f"{crosswalk_table} does not hold the projected entries {missing}")
        # Always the table's current snapshot ("" while it has none): the hub exports compare it
        # with the catalog's, so a projection the table has moved past is refused there.
        crosswalk_snapshot = _main_snapshot(spark, crosswalk_table)
        _emit(args, plan, crosswalk_snapshot, now, {
            "job": JOB_NAME, "status": "ready", "changes_appended": changes_appended,
            "steward_decisions": verdicts, "steward_rows_appended": steward_appended,
            "crosswalk_entries_appended": len(fresh_crosswalk) if crosswalk_appended else 0,
            "crosswalk_table_snapshot_id": crosswalk_snapshot,
            "official_parcel_links": "off" if links is None else len(links), **plan["counts"],
        })
        return 0
    finally:
        spark.stop()


def _main_snapshot(spark, table: str) -> str:
    """The snapshot the table's `main` branch points at, or "" for a table nothing was written to."""

    rows = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").collect()
    return str(rows[0]["snapshot_id"]) if rows else ""


def _group_by_run(rows: Sequence[Mapping[str, Any]]) -> list[tuple[str, list[Mapping[str, Any]]]]:
    """Steward rows by decision file: each file is its own load (its own `derivation_run_id`)."""

    groups: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        groups.setdefault(row["derivation_run_id"], []).append(row)
    return sorted(groups.items())


if __name__ == "__main__":
    sys.exit(main())
