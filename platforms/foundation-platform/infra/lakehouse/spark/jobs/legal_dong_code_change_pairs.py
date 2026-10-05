#!/usr/bin/env python3
"""Pair 법정동 code changes into the one record of them, and project the 시군구 crosswalk the hub
loaders read (root ADR-0143, ADR-0144, ADR-0145).

One run reads one code.go.kr full-table snapshot (`reference.legal_dong_code_snapshot`, loaded by
`legal_dong_code_snapshot_to_reference.py`) and the changes already recorded
(`reference.legal_dong_code_change`), optionally two parcel snapshots, then:

1. pairs every change on or after the contract's floor from data only
   (`code_go_kr_legal_dong.pair_changes`): what the change table already records, the date + name
   rule, the 지번 sets of the parcel editions that bracket each change (`--jibun-evidence
   editions`: per abolished 동, the latest edition the source contract holds extracted before its
   abolition and the earliest after, read from `silver.parcel_boundaries`; a code whose editions
   the contract does not hold, or the table does not, waits for them by name), and a roll-up of
   동 pairs to the 시군구 and 시도 above them. The official parcel-number history (필지고유번호변동연혁) is not collected yet,
   so that step stays off and what only it could settle is reported `awaiting_data` (ADR-0144 §4).
   The parcel lineage is not evidence here: it reads its 동 pairs from this table (ADR-0145 §2);
2. appends the changes not yet recorded to `reference.legal_dong_code_change`, the only table it
   writes;
3. writes the crosswalk projection (`--projection-output`) the hub exports read: a view of the
   change table (`legal_dong_code_change_views.sigungu_crosswalk_view`) naming the change table
   snapshot it was read at, and the steward list (`--review-output`).

The 코드변경안내 notice board is not a source (ADR-0144).

`--validate-only` does all of it and writes nothing to the lakehouse, only the output files. With
`--table-html` it reads a local full-table file (and `--edition-pnus` files for the parcel table);
without it, it reads the lakehouse as a scheduled run does (the snapshot, the recorded changes and the
parcel editions) — the real-data check (`scripts/ops/legal-dong-code-validate.sh`).

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
from typing import Any, Callable, Mapping, Sequence

import code_go_kr_legal_dong as cg
import legal_dong_code_change_views as views
import vworld_parcel_editions as editions

JOB_NAME = "legal_dong_code_change_pairs"
CHANGE_CONTRACT = "reference.legal_dong_code_change"
SNAPSHOT_CONTRACT = "reference.legal_dong_code_snapshot"
PROJECTION_SCHEMA = "foundation-platform.sigungu_crosswalk_projection.v2"
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
STEWARD_ID = re.compile(r"^[A-Za-z0-9._@-]{1,64}$")


def cadastral_sido(parcel_source: Mapping[str, Any]) -> list[str]:
    """The 시도 the served cadastral parcel set carries (`vworld_parcel_editions.cadastral_sido`)."""

    return editions.cadastral_sido(parcel_source)


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
    jibun: cg.JibunEvidence | cg.EditionEvidence | None = None,
    official_links: Sequence[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    """Everything one run decides, without touching the lakehouse.

    Returns the change rows not yet recorded, the crosswalk the change table will hold once they
    are appended (its view over the recorded rows and these), the steward list and the counts.
    Pure, so the planted-failure tests drive it directly. The official parcel-number history is
    not collected (ADR-0144 §4), so its step is off: `pair_changes` gets no links.
    """

    pairing = contract["pairing"]
    # Raises cg.PairingConflict when the official history and the 지번 step disagree: the run stops
    # here, before anything is appended, and the message names both answers.
    result = cg.pair_changes(rows, pairing["floor_date"], decided_pairs(recorded), jibun, official_links,
                             float(pairing["jibun_overlap_min_share"]), cg.land_match_tolerance(contract))
    known = {row["change_key"] for row in recorded}
    fresh: list[dict[str, Any]] = []
    for pair in result.pairs:
        row = change_row("pair", pair["source"], pair["old_code"], pair["new_code"], derivation_run_id, now,
                         level=pair["level"], effective_date=pair["effective_date"], rule_verdict=pair["rule_verdict"],
                         detail=pair["detail"])
        if row["change_key"] not in known:
            known.add(row["change_key"])
            fresh.append(row)

    crosswalk, crosswalk_review = views.sigungu_crosswalk_view([*recorded, *fresh], cadastral)
    review = result.review + crosswalk_review
    by_source: dict[str, int] = {}
    for pair in result.pairs:
        by_source[pair["rule_verdict"]] = by_source.get(pair["rule_verdict"], 0) + 1
    return {
        "fresh_changes": fresh,
        "crosswalk": crosswalk,
        "review": review,
        "counts": {
            "table_rows": len(rows),
            "pairs": len(result.pairs),
            "pairs_by_evidence": by_source,
            "jibun_evidence": evidence_label(jibun),
            "fresh_changes": len(fresh),
            "crosswalk_entries": len(crosswalk["sigungu"]),
            "governed_sido": [sido["new_code"] for sido in crosswalk["sido"]],
            "review_items": len(review),
            "review_by_status": {
                status: sum(1 for item in review if item.get("status", "steward") == status)
                for status in sorted({item.get("status", "steward") for item in review})
            },
            **awaiting_counts(review),
        },
    }


def awaiting_counts(review: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, int]]:
    """The awaiting_data items by 시도 and by the evidence they wait for (which parcel edition, or
    the official record), for the daily summary and the steward list."""

    waiting = [item for item in review if item.get("status") == "awaiting_data"]
    by_sido: dict[str, int] = {}
    by_evidence: dict[str, int] = {}
    for item in waiting:
        by_sido[item["old_code"][:2]] = by_sido.get(item["old_code"][:2], 0) + 1
        what = item.get("waiting_for") or item.get("jibun") or "unknown"
        by_evidence[what] = by_evidence.get(what, 0) + 1
    return {"awaiting_by_sido": dict(sorted(by_sido.items())), "awaiting_by_evidence": dict(sorted(by_evidence.items()))}


def projection_document(
    plan: Mapping[str, Any], snapshot_date: str, snapshot_record: str, change_snapshot_id: str, now: datetime
) -> dict[str, Any]:
    """The file the hub exports read (root ADR-0143 §5): the crosswalk view of the change table at
    `change_snapshot_id`, old → new (ADR-0145)."""

    return {
        "schema_version": PROJECTION_SCHEMA,
        "built_at_utc": now.isoformat(),
        "legal_dong_snapshot_date": snapshot_date,
        "legal_dong_snapshot_record": snapshot_record,
        "change_table": CHANGE_CONTRACT,
        "change_table_snapshot_id": change_snapshot_id,
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


Parcel = tuple[str, str, float]
"""One parcel as the 지번 step reads it: (PNU, 지목, area m²)."""


def edition_evidence(
    rows: Sequence[Mapping[str, str]],
    floor: str,
    parcel_source: Mapping[str, Any],
    parcels_of: Callable[[str, set[str], set[str] | None], list[Parcel] | None],
) -> cg.EditionEvidence:
    """Each abolished 동·리's 지번 evidence from the two editions that bracket its own abolition.

    The editions are the source contract's (`vworld_parcel_editions.bracketing`): the latest one
    extracted wholly before the abolition and the earliest wholly after it, so a 2026-01 merger and
    a 2026-07 one are each read across their own change, not across one pair chosen for all.
    `parcels_of(edition, codes, lots)` returns that edition's parcels (PNU, 지목, area) under `codes`
    (and, when `lots` is given, only those whose 지번 is in it), or None when the edition is not in
    the parcel table. The 지목 and area let the step count a lot only where it is the same land in
    both (`code_go_kr_legal_dong.same_land`). The later edition is read for the 지번 the abolished
    동 held only: a 지번 none of them held cannot count toward any overlap, and the codes created
    since the floor hold whole provinces (2026-07's 전남광주 is 6.3 million parcels) where the
    abolished 동 hold thousands. Either way a missing edition is not read as "no parcels": the code
    waits, and its reason names the edition it needs (ADR-0144 §4).
    """

    before_codes, after_codes = evidence_codes(rows, floor)
    abolished_on = {row["region_cd"]: row["abolished_date"] for row in rows if row["region_cd"] in before_codes}
    by_pair: dict[tuple[str, str], set[str]] = {}
    awaiting: dict[str, str] = {}
    for code, day in sorted(abolished_on.items()):
        before, after = editions.bracketing(parcel_source, day)
        if before is None or after is None:
            side = "before" if before is None else "after"
            awaiting[code] = f"needs a parcel edition extracted {side} {day}; the source contract holds none"
            continue
        by_pair.setdefault((before, after), set()).add(code)
    by_code: dict[str, cg.JibunEvidence] = {}
    for (before, after), codes in sorted(by_pair.items()):
        before_parcels = parcels_of(before, codes, None)
        before_sets = cg.land_sets(before_parcels or [], codes)
        after_parcels = parcels_of(after, after_codes, {lot for lands in before_sets.values() for lot in lands})
        missing = [name for name, read in ((before, before_parcels), (after, after_parcels)) if read is None]
        if missing:
            named = ", ".join(f"{name} ({editions.snapshot_id(parcel_source, name)})" for name in missing)
            for code in codes:
                awaiting[code] = f"needs parcel edition {named}, which is not loaded into the parcel table"
            continue
        label = f"{editions.snapshot_id(parcel_source, before)}->{editions.snapshot_id(parcel_source, after)}"
        evidence = cg.JibunEvidence(before_sets, cg.land_sets(after_parcels, after_codes), label)
        for code in codes:
            by_code[code] = evidence
    return cg.EditionEvidence(by_code, awaiting)


def evidence_label(jibun: cg.JibunEvidence | cg.EditionEvidence | None) -> str:
    """What the 지번 step read, for the summary: off, one pair, or the edition pairs and how many wait."""

    if jibun is None:
        return "off"
    if isinstance(jibun, cg.JibunEvidence):
        return jibun.label
    pairs = sorted({evidence.label for evidence in jibun.by_code.values()})
    return f"editions[{', '.join(pairs) or 'none'}] awaiting={len(jibun.awaiting)}"


def _read_parcel_file(path: str) -> list[Parcel]:
    """`PNU<TAB>지목<TAB>area m²` per line, the columns `_snapshot_parcels` reads from the table."""

    parcels = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.strip():
            pnu, jimok, area = line.rstrip("\n").split("\t")
            parcels.append((pnu.strip(), jimok.strip(), float(area)))
    return parcels


def _local_parcels(files: Mapping[str, str], edition: str, codes: set[str], lots: set[str] | None) -> list[Parcel] | None:
    """`--edition-pnus` standing in for the parcel table, with the same filters the table read applies."""

    if edition not in files:
        return None
    return [p for p in _read_parcel_file(files[edition])
            if p[0][:10] in codes and (lots is None or (cg.pl.PNU_PATTERN.fullmatch(p[0]) and cg.pl.lot(p[0]) in lots))]


# --- Spark ------------------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--snapshot-date", required=True, help="YYYY-MM-DD of the full-table snapshot to pair.")
    parser.add_argument("--table-source-record-id", required=True, help="The Bronze key of that full table.")
    parser.add_argument("--table-html", help="With --validate-only: the local full table instead of the snapshot table.")
    parser.add_argument("--recorded-changes", help="With --validate-only: a JSON list of recorded change rows.")
    parser.add_argument("--jibun-evidence", choices=("off", "editions"), default="off",
                        help="editions: read each change across the parcel editions bracketing it (the source contract).")
    parser.add_argument("--edition-pnus", action="append", default=[], metavar="EDITION=FILE",
                        help="With --validate-only: PNU<TAB>지목<TAB>area m² per line of that edition, standing in for the parcel table.")
    parser.add_argument("--steward-decisions", help="A directory of staged steward decision files to fold in.")
    parser.add_argument("--projection-output")
    parser.add_argument("--review-output")
    parser.add_argument("--summary-output")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-namespace", default="reference")
    parser.add_argument("--snapshot-table", default="legal_dong_code_snapshot")
    parser.add_argument("--change-table", default="legal_dong_code_change")
    parser.add_argument("--parcel-table", default="silver.parcel_boundaries")
    parser.add_argument("--allow-non-smoke-write", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args(argv)
    for label in ("iceberg_catalog_name", "iceberg_namespace", "snapshot_table", "change_table"):
        if not IDENTIFIER.fullmatch(getattr(args, label)):
            parser.error(f"{label} must be a plain SQL identifier")
    if not all(IDENTIFIER.fullmatch(part) for part in args.parcel_table.split(".")):
        parser.error("parcel_table must be namespace.table")
    args.edition_pnu_files = {}
    for value in args.edition_pnus:
        name, sep, path = value.partition("=")
        if not sep or not editions.EDITION.fullmatch(name) or not path or name in args.edition_pnu_files:
            parser.error(f"--edition-pnus takes EDITION=FILE once per edition: {value!r}")
        args.edition_pnu_files[name] = path
    if args.edition_pnu_files and not (args.validate_only and args.table_html and args.jibun_evidence == "editions"):
        parser.error("--edition-pnus stands in for the parcel table: it needs --validate-only, --table-html and "
                     "--jibun-evidence editions")
    date.fromisoformat(args.snapshot_date)
    if (args.table_html or args.recorded_changes) and not args.validate_only:
        parser.error("--table-html and --recorded-changes stand in for the lakehouse: they need --validate-only")
    if not args.validate_only and not args.change_table.endswith("_smoke") and not args.allow_non_smoke_write:
        parser.error(f"writing {args.change_table} needs --allow-non-smoke-write")
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


def _snapshot_parcels(spark, F, table: str, snapshot_id: str, codes: set[str], lots: set[str] | None = None) -> list[Parcel] | None:
    """The parcels (PNU, 지목, area m²) of one parcel edition under `codes`, and with a 지번 in `lots`
    when it is given, or None when the table holds no row of that edition at all: an edition not
    loaded is not "no parcels". The 지목 is read from `jibun` and the area from the boundary on the
    executors (`parcel_land.jimok_of`, `wkb_area_m2`); only those three values come back."""

    from pyspark.sql import types as T  # noqa: PLC0415

    edition = spark.table(table).filter(F.col("source_snapshot_id") == F.lit(snapshot_id))
    if not edition.limit(1).collect():
        return None
    if not codes or (lots is not None and not lots):
        return []
    frame = edition.filter(F.substring(F.col("pnu"), 1, 10).isin(sorted(codes)))
    if lots is not None:
        wanted = spark.createDataFrame([(lot,) for lot in sorted(lots)], "lot string")
        frame = frame.filter(F.col("pnu").rlike(r"^[0-9]{19}$")).join(
            F.broadcast(wanted), F.substring(F.col("pnu"), 11, 9) == wanted["lot"], "left_semi")
    # The executors' Python workers do not have this job's directory on their path; parcel_land is
    # standard library only, so it can be shipped to them as it is.
    import parcel_land  # noqa: PLC0415

    spark.sparkContext.addPyFile(parcel_land.__file__)
    jimok = F.udf(parcel_land.jimok_of, T.StringType())
    area = F.udf(parcel_land.wkb_area_m2, T.DoubleType())
    rows = frame.where(F.col("geometry_wkb").isNotNull()).select(
        "pnu", jimok("jibun").alias("jimok"), area("geometry_wkb").alias("area")).collect()
    return [(row["pnu"], row["jimok"], row["area"]) for row in rows]


def _emit(args: argparse.Namespace, plan: Mapping[str, Any], snapshot_id: str, now: datetime, summary: dict[str, Any]) -> None:
    _write_json(args.projection_output, projection_document(plan, args.snapshot_date, args.table_source_record_id, snapshot_id, now))
    _write_json(args.review_output, {"review": plan["review"], **awaiting_counts(plan["review"])})
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
    parcel_source = editions.load()
    cadastral = cadastral_sido(parcel_source)
    run_id = pairing_run_id(args.table_source_record_id, now)
    decisions = _read_decisions(args.steward_decisions)

    if args.validate_only and args.table_html:
        rows = cg.parse_full_table_html(Path(args.table_html).read_text(encoding="utf-8"), contract)
        recorded = json.loads(Path(args.recorded_changes).read_text(encoding="utf-8")) if args.recorded_changes else []
        jibun = None
        if args.jibun_evidence == "editions":
            files = args.edition_pnu_files
            jibun = edition_evidence(rows, floor, parcel_source,
                                     lambda name, codes, lots: _local_parcels(files, name, codes, lots))
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
        if not args.validate_only:
            _ensure_table(spark, change_table, load_lakehouse_contract(CHANGE_CONTRACT))
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
        jibun = None
        if args.jibun_evidence == "editions":
            parcels = _qualified(args.iceberg_catalog_name, args.parcel_table)
            jibun = edition_evidence(
                rows, floor, parcel_source,
                lambda name, codes, lots: _snapshot_parcels(spark, F, parcels, editions.snapshot_id(parcel_source, name), codes, lots),
            )
        recorded = [row.asDict() for row in spark.table(change_table).collect()]
        plan = plan_derivation(rows, recorded, contract, cadastral, run_id, now, jibun)
        # Steward decisions are judged against the list as it stands before them, then recorded,
        # then the pairing runs again so today's projection already carries them.
        steward, verdicts = fold_steward_decisions(decisions, plan["review"], {row["region_cd"] for row in rows}, now)
        known = {row["change_key"] for row in recorded}
        steward = [row for row in steward if row["change_key"] not in known]
        if args.validate_only:  # the lakehouse as it stands, read and judged; nothing written to it
            if steward:
                plan = plan_derivation(rows, recorded + steward, contract, cadastral, run_id, now, jibun)
            _emit(args, plan, "validate-only", now, {"job": JOB_NAME, "status": "validated", "reads": "lakehouse",
                                                     "steward_decisions": verdicts, **plan["counts"],
                                                     "would_append": [{k: row[k] for k in ("old_code", "new_code", "level", "source", "detail")}
                                                                      for row in plan["fresh_changes"]]})
            return 0
        append_changes = functools.partial(_append, spark, T, change_table, CHANGE_CONTRACT)
        steward_appended = False
        for _, rows_of_one in _group_by_run(steward):
            steward_appended |= append_new_rows(append_changes, rows_of_one, "steward")
        if steward:
            plan = plan_derivation(rows, recorded + steward, contract, cadastral, run_id, now, jibun)

        changes_appended = append_new_rows(append_changes, plan["fresh_changes"], "change")
        # The projection is the crosswalk view of the table as it now stands, read back, not the
        # plan held in memory: the file can then only say what the table says.
        held = [row.asDict() for row in spark.table(change_table).collect()]
        crosswalk, crosswalk_review = views.sigungu_crosswalk_view(held, cadastral)
        if crosswalk != plan["crosswalk"]:
            raise ValueError(f"{change_table} read back a crosswalk other than the one this run planned; "
                             "another writer appended to it meanwhile. Run the pairing again.")
        # Always the table's current snapshot ("" while it has none): the hub exports compare it
        # with the catalog's, so a projection the table has moved past is refused there.
        change_snapshot = _main_snapshot(spark, change_table)
        _emit(args, plan, change_snapshot, now, {
            "job": JOB_NAME, "status": "ready", "changes_appended": changes_appended,
            "steward_decisions": verdicts, "steward_rows_appended": steward_appended,
            "change_table_snapshot_id": change_snapshot, "crosswalk_review_items": len(crosswalk_review),
            "official_parcel_links": "awaiting_data", **plan["counts"],
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
