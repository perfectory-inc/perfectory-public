#!/usr/bin/env python3
"""Whether each cadastral edition is all there, and the one gated way to serve it (root ADR-0291 §2, §4).

`readiness` reads the source contract and one evidence file the host measured
(`scripts/ops/parcel-edition-readiness.sh`) and reports, per edition the contract holds, three stages:

- `source`: the contract measured every object of the edition (`measured_at_utc`) and the provider
  re-uploaded none of its files after the extraction the contract describes;
- `silver`: `silver.parcel_boundaries` holds the edition with at least (1 − `max_row_loss_fraction`)
  of the served edition's rows (read from the table's per-edition partition counts, root ADR-0316);
- `rehearsal`: the rehearsal Gold and the rehearsal bake of that edition both passed
  (runbook parcel-tile-bake.md §2).

An edition is `ready` only when all three pass; otherwise the report names the first stage that did
not. A stage the evidence says nothing about fails: unmeasured is not ready.

`promote` is the only writer of `served_edition`. It refuses an edition whose readiness report is not
`ready`, is older than `max_report_age_hours`, or was made from another contract than the one it
changes, and otherwise moves `served_edition` and appends the promotion — who, why, when, and the
readiness it rested on — to the contract's `promotions` ledger, which is never rewritten
(`vworld_parcel_editions.validate` refuses a served edition the ledger's last entry does not name).
The contract change still lands through a reviewed pull request, so the gate holds from the moment
it merges, not one deploy later.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import vworld_parcel_editions as editions

STAGES = ("source", "silver", "rehearsal")
REPORT_SCHEMA_VERSION = 1


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _utc(value: str, what: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as error:
        raise editions.EditionError(f"{what} is not an ISO-8601 UTC instant: {value!r}") from error
    if parsed.tzinfo is None:
        raise editions.EditionError(f"{what} names no time zone: {value!r}")
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def contract_digest(contract: Mapping[str, Any]) -> str:
    """What a report was made from: the editions and the served one, not the prose around them."""

    held = {"served_edition": contract["served_edition"], "editions": contract["editions"]}
    return hashlib.sha256(json.dumps(held, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def _stage(passed: bool, detail: str) -> dict[str, Any]:
    return {"passed": passed, "detail": detail}


def source_stage(contract: Mapping[str, Any], name: str, evidence: Mapping[str, Any]) -> dict[str, Any]:
    entry = editions.edition(contract, name)
    if not entry.get("measured_at_utc"):
        return _stage(False, "the contract has not measured this edition's objects (measured_at_utc unset)")
    checked = evidence.get("provider_reuploads")
    if not isinstance(checked, Mapping) or name not in checked:
        return _stage(False, "the provider inventory was not checked for re-uploads of this edition")
    reuploads = list(checked[name])
    if reuploads:
        return _stage(False, f"the provider re-uploaded {len(reuploads)} file(s) after extraction: {', '.join(reuploads[:3])}")
    counts = entry["granularity_counts"]
    return _stage(True, f"{counts['sigungu']} 시군구 objects measured {entry['measured_at_utc']}, none re-uploaded")


def silver_stage(contract: Mapping[str, Any], name: str, evidence: Mapping[str, Any]) -> dict[str, Any]:
    rows = (evidence.get("silver") or {}).get("rows_by_edition")
    if not isinstance(rows, Mapping):
        return _stage(False, "silver.parcel_boundaries was not measured")
    held = int(rows.get(name) or 0)
    if held <= 0:
        return _stage(False, "silver.parcel_boundaries holds no row of this edition")
    served = editions.served(contract)
    baseline = int(rows.get(served) or 0)
    if name == served or baseline <= 0:
        return _stage(True, f"{held} rows")
    loss = float(contract["readiness"]["max_row_loss_fraction"])
    floor = int(baseline * (1 - loss))
    if held < floor:
        return _stage(False, f"{held} rows, under {floor} (served {served} holds {baseline}, at most {loss:.0%} fewer)")
    return _stage(True, f"{held} rows (served {served} holds {baseline})")


def rehearsal_stage(contract: Mapping[str, Any], name: str, evidence: Mapping[str, Any]) -> dict[str, Any]:
    if name == editions.served(contract):
        return _stage(True, "served: its live release is its bake")
    rehearsal = (evidence.get("rehearsals") or {}).get(name) or {}
    for half in ("gold", "bake"):
        result = rehearsal.get(half) or {}
        if result.get("passed") is not True:
            why = result.get("detail") or "no passing run recorded"
            return _stage(False, f"rehearsal {half}: {why}")
    return _stage(True, f"gold {rehearsal['gold'].get('detail', 'passed')}; bake {rehearsal['bake'].get('detail', 'passed')}")


def silver_rows(contract: Mapping[str, Any], partitions_csv: str) -> dict[str, int]:
    """Rows per edition from `SELECT partition.source_snapshot_id, record_count FROM
    silver."parcel_boundaries$partitions"` as headerless CSV: table metadata, no data file is read."""

    rows: dict[str, int] = {}
    for line in partitions_csv.splitlines():
        if not line.strip():
            continue
        snapshot, count = (part.strip().strip('"') for part in line.split(",")[:2])
        try:
            name = editions.edition_of_snapshot_id(contract, snapshot)
        except editions.EditionError:
            continue  # a snapshot of no held edition is not evidence for one
        rows[name] = rows.get(name, 0) + int(count)
    return rows


def _edition_named(contract: Mapping[str, Any], words: Sequence[str]) -> str | None:
    for word in words:
        if word.startswith(contract["snapshot_id_prefix"]):
            try:
                return editions.edition_of_snapshot_id(contract, word)
            except editions.EditionError:
                return None
    return None


def rehearsals(contract: Mapping[str, Any], journal: Sequence[str]) -> dict[str, dict[str, Any]]:
    """The newest rehearsal of each edition, from the tile bake's journal (scripts/ops/parcel-tile-bake.sh).

    A rehearsal Gold passes when it built the served Gold and its matching gate did not refuse (the
    job's own Gold half stops at a refusal); a bake passes when it baked that Gold. A newer Gold run of
    an edition makes the bake before it stale."""

    found: dict[str, dict[str, Any]] = {}
    refused: set[str] = set()
    for line in journal:
        at, _, text = line.strip().partition(" ")
        name = _edition_named(contract, text.split())
        if name is None:
            continue
        entry = found.setdefault(name, {})
        took = text.split(" in ", 1)[-1]
        if text.startswith("rehearsal: the matching gate refused"):
            refused.add(name)
        elif text.startswith("rehearsal-gold: served Gold of"):
            entry["gold"] = ({"passed": False, "detail": f"{at}: built, but its matching gate refused"} if name in refused
                             else {"passed": True, "detail": f"{at}: {took}"})
            entry.pop("bake", None)
            refused.discard(name)
        elif text.startswith("rehearsal-gold ") and " FAILED" in text:
            entry["gold"] = {"passed": False, "detail": f"{at}: failed"}
            entry.pop("bake", None)
            refused.discard(name)
        elif text.startswith("rehearsal baked"):
            entry["bake"] = {"passed": True, "detail": f"{at}: {took}"}
        elif text.startswith("rehearsal-bake ") and " FAILED" in text:
            entry["bake"] = {"passed": False, "detail": f"{at}: failed"}
    return found


def evidence(contract: Mapping[str, Any], partitions_csv: str | None, inventory: Mapping[str, Any] | None,
             journal: Sequence[str] | None, now: datetime | None = None) -> dict[str, Any]:
    """The evidence `readiness` reads, from what the host holds; what was not given is left out (not ready)."""

    found: dict[str, Any] = {"measured_at_utc": _iso(now or _now())}
    if partitions_csv is not None:
        found["silver"] = {"rows_by_edition": silver_rows(contract, partitions_csv)}
    if inventory is not None:
        try:
            found["provider_reuploads"] = {name: editions.held_reuploads(contract, inventory, name)
                                           for name in editions.names(contract)}
        except editions.EditionError as error:
            # An inventory the collection could not read is no evidence: the source stage stays not ready.
            found["provider_inventory_error"] = str(error)
    if journal is not None:
        found["rehearsals"] = rehearsals(contract, journal)
    return found


def readiness(contract: Mapping[str, Any], evidence: Mapping[str, Any], now: datetime | None = None) -> dict[str, Any]:
    """The report: per edition, its stages, whether it is ready and the first stage that is not."""

    served = editions.served(contract)
    rows = []
    for name in editions.names(contract):
        stages = {
            "source": source_stage(contract, name, evidence),
            "silver": silver_stage(contract, name, evidence),
            "rehearsal": rehearsal_stage(contract, name, evidence),
        }
        failing = next((stage for stage in STAGES if not stages[stage]["passed"]), None)
        rows.append({
            "edition": name,
            "provider_base_month": editions.edition(contract, name)["provider_base_month"],
            "served": name == served,
            "ready": failing is None,
            "first_failing_stage": failing,
            "stages": stages,
        })
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "checked_at_utc": _iso(now or _now()),
        "evidence_measured_at_utc": evidence.get("measured_at_utc"),
        "contract_sha256": contract_digest(contract),
        "served_edition": served,
        "editions": rows,
        "promotions": list(contract.get("promotions") or []),
    }


def promote(contract: Mapping[str, Any], report: Mapping[str, Any], name: str, by: str, reason: str,
            now: datetime | None = None) -> dict[str, Any]:
    """The contract with `name` served and the promotion appended, or the reason it is refused."""

    now = now or _now()
    editions.edition(contract, name)
    if name == editions.served(contract):
        raise editions.EditionError(f"{name} is already the served edition")
    if not by.strip() or not reason.strip() or any(ch in reason for ch in "\r\n"):
        raise editions.EditionError("a promotion names who asked (--by) and one line of why (--reason)")
    if report.get("schema_version") != REPORT_SCHEMA_VERSION:
        raise editions.EditionError(f"the readiness report is schema {report.get('schema_version')!r}, not {REPORT_SCHEMA_VERSION}")
    if report.get("contract_sha256") != contract_digest(contract):
        raise editions.EditionError("the readiness report was made from another contract; measure again")
    age = now - _utc(report.get("checked_at_utc"), "checked_at_utc")
    limit = timedelta(hours=float(contract["readiness"]["max_report_age_hours"]))
    if age > limit:
        raise editions.EditionError(f"the readiness report is {age} old, older than {limit}; measure again")
    row = next((row for row in report.get("editions") or [] if row.get("edition") == name), None)
    if row is None:
        raise editions.EditionError(f"the readiness report says nothing of {name}")
    # The report's verdict is recomputed from its stages, so a hand-edited `ready` cannot pass.
    failing = next((stage for stage in STAGES if (row.get("stages") or {}).get(stage, {}).get("passed") is not True), None)
    if failing is not None or row.get("ready") is not True:
        detail = (row.get("stages") or {}).get(failing or "", {}).get("detail", "")
        raise editions.EditionError(f"{name} is not ready: stage {failing or row.get('first_failing_stage')} failed ({detail})")
    promoted = json.loads(json.dumps(contract))
    promoted["served_edition"] = name
    promoted.setdefault("promotions", []).append({
        "edition": name,
        "from_edition": editions.served(contract),
        "promoted_at_utc": _iso(now),
        "by": by.strip(),
        "reason": reason.strip(),
        "readiness": {
            "checked_at_utc": report["checked_at_utc"],
            "contract_sha256": report["contract_sha256"],
            "stages": {stage: row["stages"][stage]["detail"] for stage in STAGES},
        },
    })
    editions.validate(promoted)
    return promoted


def _text(path: str | None) -> str | None:
    """A file the host may not hold yet: absent is no evidence, not an error."""

    return Path(path).read_text(encoding="utf-8") if path and Path(path).is_file() else None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("evidence", "readiness", "promote"))
    parser.add_argument("--contract", help=f"defaults to ${editions.CONTRACT_ENV}, then the contract beside this job")
    parser.add_argument("--evidence", help="readiness: the host's measured evidence (JSON)")
    parser.add_argument("--report", help="promote: the readiness report to rest on")
    parser.add_argument("--edition", help="promote: YYYYMM")
    parser.add_argument("--by", default="", help="promote: who asked")
    parser.add_argument("--reason", default="", help="promote: one line of why")
    parser.add_argument("--partitions", help='evidence: silver."parcel_boundaries$partitions" as headerless CSV')
    parser.add_argument("--inventory", help="evidence: the newest provider inventory of the edition collection")
    parser.add_argument("--journal", help="evidence: the parcel tile bake's journal")
    parser.add_argument("--output", help="evidence, readiness: where to write (stdout without)")
    args = parser.parse_args(argv)
    path = Path(args.contract) if args.contract else editions.contract_path()
    try:
        contract = editions.load(path)
        if args.command == "evidence":
            inventory, journal = _text(args.inventory), _text(args.journal)
            made = evidence(contract, _text(args.partitions), json.loads(inventory) if inventory else None,
                            journal.splitlines() if journal is not None else None)
        elif args.command == "readiness":
            made = readiness(contract, json.loads(_text(args.evidence) or "{}"))
        if args.command != "promote":
            body = json.dumps(made, ensure_ascii=False, indent=2) + "\n"
            if args.output:
                Path(args.output).write_text(body, encoding="utf-8")
            else:
                sys.stdout.write(body)
            return 0
        if not args.report:
            raise editions.EditionError("promote needs --report, the readiness report it rests on")
        report = json.loads(Path(args.report).read_text(encoding="utf-8"))
        promoted = promote(contract, report, args.edition or "", args.by, args.reason)
        path.write_text(json.dumps(promoted, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")
        print(f"served_edition {editions.served(contract)} -> {args.edition}; open the pull request with this change")
        return 0
    except (editions.EditionError, OSError, json.JSONDecodeError, KeyError) as error:
        print(f"parcel-edition-readiness: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
