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


def edition_inputs(contract: Mapping[str, Any], name: str, evidence: Mapping[str, Any]) -> str:
    """The digest of everything one edition's verdict reads: its contract entry, the served edition and
    its rows (the Silver bound), the gate's bounds, and the evidence about it. Equal digests, equal
    verdict: the scheduled check recomputes only the editions whose digest moved (root ADR-0380)."""

    served = editions.served(contract)
    rows = (evidence.get("silver") or {}).get("rows_by_edition") or {}
    held = {
        "entry": editions.edition(contract, name),
        "served": served,
        "readiness": contract.get("readiness"),
        "rows": [rows.get(name), rows.get(served)],
        "silver_measured": "silver" in evidence,
        "reuploads": (evidence.get("provider_reuploads") or {}).get(name),
        "rehearsal": (evidence.get("rehearsals") or {}).get(name),
    }
    return hashlib.sha256(json.dumps(held, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def readiness(contract: Mapping[str, Any], evidence: Mapping[str, Any], now: datetime | None = None,
              previous: Mapping[str, Any] | None = None, previous_inputs: Mapping[str, str] | None = None) -> dict[str, Any]:
    """The report: per edition, its stages, whether it is ready and the first stage that is not.

    With the previous report and its per-edition input digests, an edition whose digest did not move
    keeps its row as it was; only the others are judged again."""

    served = editions.served(contract)
    kept = {row["edition"]: row for row in (previous or {}).get("editions") or []}
    rows = []
    for name in editions.names(contract):
        if name in kept and (previous_inputs or {}).get(name) == edition_inputs(contract, name, evidence):
            rows.append(kept[name])
            continue
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
        "silver_measured": isinstance((evidence.get("silver") or {}).get("rows_by_edition"), Mapping),
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


def lag(contract: Mapping[str, Any], report: Mapping[str, Any], now: datetime | None = None) -> dict[str, Any]:
    """How far the served edition is behind the newest edition Silver holds (root ADR-0490 §7).

    `newest` is the newest edition the report found rows of in Silver; it has been available since its
    objects were measured (`measured_at_utc`, when the collection proposed it). `behind_seconds` is 0 when
    it is the served edition or older. `silver_measured` is False when the report could not read Silver
    (2026-11-04..11: every run, so 202609 stood unjudged and nothing said so): then no lag is claimed."""

    now = now or _now()
    served = editions.served(contract)
    held = [row["edition"] for row in report.get("editions") or []
            if (row.get("stages") or {}).get("silver", {}).get("passed") is True]
    measured = report.get("silver_measured") is True
    newest = max(held, default=served)
    behind = 0
    if newest > served:
        since = _utc(f"{editions.edition(contract, newest)['measured_at_utc']}T00:00:00Z", "measured_at_utc")
        behind = max(0, int((now - since).total_seconds()))
    return {"served": served, "newest": newest, "behind_seconds": behind, "silver_measured": measured,
            "budget_seconds": int(float(contract["readiness"]["max_served_lag_days"]) * 86400)}


def lag_series(found: Mapping[str, Any], now: datetime | None = None) -> list[str]:
    """`lag` as node-exporter textfile lines for FoundationParcelServedEditionBehind and its two companions
    (infra/observability/prometheus/foundation-api.rules.yml)."""

    at = (now or _now()).timestamp()
    labels = f'served="{found["served"]}",newest="{found["newest"]}"'
    return [
        "# HELP foundation_parcel_served_edition_behind_seconds How long the newest cadastral edition Silver holds has waited to be served (root ADR-0490).",
        "# TYPE foundation_parcel_served_edition_behind_seconds gauge",
        f"foundation_parcel_served_edition_behind_seconds{{{labels}}} {found['behind_seconds']}",
        "# HELP foundation_parcel_served_edition_behind_budget_seconds How long a new edition may wait before it is said (readiness.max_served_lag_days).",
        "# TYPE foundation_parcel_served_edition_behind_budget_seconds gauge",
        f"foundation_parcel_served_edition_behind_budget_seconds {found['budget_seconds']}",
        "# HELP foundation_parcel_edition_silver_measured Whether the readiness check could read Silver's rows per edition.",
        "# TYPE foundation_parcel_edition_silver_measured gauge",
        f"foundation_parcel_edition_silver_measured {int(bool(found['silver_measured']))}",
        "# HELP foundation_parcel_edition_readiness_timestamp_seconds When the readiness check last judged the editions.",
        "# TYPE foundation_parcel_edition_readiness_timestamp_seconds gauge",
        f"foundation_parcel_edition_readiness_timestamp_seconds {at:.0f}",
    ]


PROMOTION_JOB = "parcel_edition_readiness"


def parse_request(text: str) -> dict[str, str]:
    """A promotion request as foundation-api's switch board writes it: `key=value` lines (root ADR-0381)."""

    fields = dict(line.split("=", 1) for line in text.splitlines() if "=" in line)
    for key in ("request", "actor", "requested_at", "reason", "job", "expires_at", "edition"):
        if not fields.get(key):
            raise editions.EditionError(f"a promotion request names no {key}")
    return fields


def answer(contract: Mapping[str, Any], report: Mapping[str, Any], request: Mapping[str, str],
           now: datetime | None = None) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """The answer to one console request, and the promoted contract when it is granted.

    Granted means `promote` accepted it: the edition is ready by a fresh report of this contract. The
    promoted contract is then a reviewed change away from served (the GitOps step, like a proposed
    edition entry); the ledger entry it carries names the staff member who asked and why."""

    now = now or _now()
    record = {"request": request["request"], "edition": request["edition"], "by": request["actor"],
              "reason": request["reason"], "requested_at": request["requested_at"], "answered_at": _iso(now)}
    if request["job"] != PROMOTION_JOB:
        return {**record, "outcome": f"refused: asked of {request['job']}, not {PROMOTION_JOB}"}, None
    if _utc(request["expires_at"], "expires_at") < now:
        return {**record, "outcome": "expired"}, None
    try:
        promoted = promote(contract, report, request["edition"], request["actor"], request["reason"], now)
    except editions.EditionError as error:
        return {**record, "outcome": f"refused: {error}"}, None
    return {**record, "outcome": "granted"}, promoted


CONTROLLER = "edition-controller"


def controller_request(contract: Mapping[str, Any], report: Mapping[str, Any], now: datetime | None = None) -> dict[str, str] | None:
    """The request the judgement makes itself (root ADR-0485 §5): the newest edition newer than the served
    one that the report holds `ready`, once per edition and contract (its id names both), or None.

    Like Argo Rollouts promoting a canary its analysis passed: the verdict, not a person, asks. A console
    request (ADR-0381 §4) stays the way to ask for anything else."""

    now = now or _now()
    served = editions.served(contract)
    ready = [row["edition"] for row in report.get("editions") or [] if row.get("ready") is True and row["edition"] > served]
    if not ready:
        return None
    name = max(ready)
    return {"request": f"{CONTROLLER}-{name}-{str(report.get('contract_sha256'))[:12]}", "actor": CONTROLLER,
            "requested_at": _iso(now), "reason": f"{name} passed every readiness stage (root ADR-0485 §5)",
            "job": PROMOTION_JOB, "expires_at": _iso(now + timedelta(hours=1)), "edition": name}


def grant(contract: Mapping[str, Any], report_path: Path, requests: Path, answers: Path,
          controller: bool = False) -> list[str]:
    """Answers every request in `requests` that `answers` does not hold yet, each once, and lists every
    answer in the report under `promotion_requests`. With `controller`, the judgement's own request for
    the newest ready edition is answered the same way, once. Returns the ids answered now."""

    report = json.loads(report_path.read_text(encoding="utf-8"))
    answers.mkdir(parents=True, exist_ok=True)
    answered = []
    own = controller_request(contract, report) if controller else None
    if own is not None and not (answers / f"{own['request']}.json").exists():
        record, promoted = answer(contract, report, own)
        if promoted is not None:
            target = answers / f"{own['request']}.contract.json"
            target.write_text(json.dumps(promoted, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            record["contract"] = str(target)
        (answers / f"{own['request']}.json").write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")
        answered.append(own["request"])
    for path in sorted(requests.glob("*")) if requests.is_dir() else []:
        if not path.is_file() or path.name.endswith(".partial") or (answers / f"{path.name}.json").exists():
            continue
        try:
            request = parse_request(path.read_text(encoding="utf-8"))
        except editions.EditionError as error:
            record, promoted = {"request": path.name, "outcome": f"refused: {error}", "answered_at": _iso(_now())}, None
        else:
            record, promoted = answer(contract, report, request)
        if promoted is not None:
            target = answers / f"{path.name}.contract.json"
            target.write_text(json.dumps(promoted, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            record["contract"] = str(target)
        # The answer is written last and once: a run that dies before it answers again, never twice.
        (answers / f"{path.name}.json").write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")
        answered.append(path.name)
    report["promotion_requests"] = sorted(
        (json.loads(path.read_text(encoding="utf-8")) for path in answers.glob("*.json")
         if not path.name.endswith(".contract.json")),
        key=lambda record: str(record.get("requested_at") or ""))
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return answered


def _text(path: str | None) -> str | None:
    """A file the host may not hold yet: absent is no evidence, not an error."""

    return Path(path).read_text(encoding="utf-8") if path and Path(path).is_file() else None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("evidence", "readiness", "promote", "grant", "lag"))
    parser.add_argument("--contract", help=f"defaults to ${editions.CONTRACT_ENV}, then the contract beside this job")
    parser.add_argument("--evidence", help="readiness: the host's measured evidence (JSON)")
    parser.add_argument("--report", help="promote: the readiness report to rest on")
    parser.add_argument("--edition", help="promote: YYYYMM")
    parser.add_argument("--by", default="", help="promote: who asked")
    parser.add_argument("--reason", default="", help="promote: one line of why")
    parser.add_argument("--partitions", help='evidence: silver."parcel_boundaries$partitions" as headerless CSV')
    parser.add_argument("--inventory", help="evidence: the newest provider inventory of the edition collection")
    parser.add_argument("--journal", help="evidence: the parcel tile bake's journal")
    parser.add_argument("--requests", help="grant: the console's promotion requests (switches/promote.d)")
    parser.add_argument("--answers", help="grant: where each answer and granted contract is kept")
    parser.add_argument("--controller", action="store_true",
                        help="grant: also promote the newest ready edition on the judgement's own request (ADR-0485 §5)")
    parser.add_argument("--previous", help="readiness: the report the last run wrote, if any")
    parser.add_argument("--inputs", help="readiness: the per-edition input digests beside it; rewritten")
    parser.add_argument("--output", help="evidence, readiness: where to write (stdout without); lag: the .prom file")
    args = parser.parse_args(argv)
    path = Path(args.contract) if args.contract else editions.contract_path()
    try:
        contract = editions.load(path)
        if args.command == "evidence":
            inventory, journal = _text(args.inventory), _text(args.journal)
            made = evidence(contract, _text(args.partitions), json.loads(inventory) if inventory else None,
                            journal.splitlines() if journal is not None else None)
        elif args.command == "readiness":
            found = json.loads(_text(args.evidence) or "{}")
            previous_text, inputs_text = _text(args.previous), _text(args.inputs)
            previous = json.loads(previous_text) if previous_text else None
            inputs = json.loads(inputs_text) if inputs_text else {}
            made = readiness(contract, found, previous=previous, previous_inputs=inputs)
            digests = {name: edition_inputs(contract, name, found) for name in editions.names(contract)}
            moved = sorted(name for name in digests if inputs.get(name) != digests[name])
        if args.command in ("evidence", "readiness"):
            body = json.dumps(made, ensure_ascii=False, indent=2) + "\n"
            if args.output:
                Path(args.output).write_text(body, encoding="utf-8")
            else:
                sys.stdout.write(body)
            if args.command == "readiness":
                # The digests follow the report: a run that dies between the two judges again, never skips.
                if args.inputs:
                    Path(args.inputs).write_text(json.dumps(digests, indent=2) + "\n", encoding="utf-8")
                # The editions judged again, one line (empty when none): the job's outcome rests on it.
                print(",".join(moved), file=sys.stdout if args.output else sys.stderr)
            return 0
        if not args.report:
            raise editions.EditionError(f"{args.command} needs --report, the readiness report it rests on")
        if args.command == "lag":
            found = lag(contract, json.loads(Path(args.report).read_text(encoding="utf-8")))
            lines = "\n".join(lag_series(found)) + "\n"
            if args.output:
                target = Path(args.output)
                temporary = target.with_name(f".{target.name}.partial")
                temporary.write_text(lines, encoding="utf-8")
                temporary.replace(target)
            else:
                sys.stdout.write(lines)
            print(f"{found['served']} served, {found['newest']} newest in Silver, behind {found['behind_seconds'] // 86400} days",
                  file=sys.stderr)
            return 0
        if args.command == "grant":
            if not args.requests or not args.answers:
                raise editions.EditionError("grant needs --requests and --answers")
            answered = grant(contract, Path(args.report), Path(args.requests), Path(args.answers), args.controller)
            print(",".join(answered))
            return 0
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
