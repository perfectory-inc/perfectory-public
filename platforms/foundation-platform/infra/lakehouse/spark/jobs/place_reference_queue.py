"""The rows no parcel source could place, as items of the one steward queue (root ADR-0483 §5).

Every parcel source Silver the resolver places carries `pnu_unresolved_reason` (root ADR-0288 §4).
Its unresolved current rows stand in `gold.lineage_review_queue` under unit `place_reference`, the
queue the parcel lineage (unit `parcel`) and the unpaired 법정동 codes (unit `legal_dong`) already
use, so a steward reads one queue whatever the source.

An item is one (source table, reason, 법정동 code) with its row count and a few sample row keys, not
one row: what a steward decides is a code's pair or a lot's form, and 12.8M masked deals are not
12.8M decisions. A reason the source itself withheld (a masked lot, no address) is
`awaiting_data`: no steward can approve it, it leaves when the source gives the value.

Pure Python: the job (`place_reference_queue_to_gold`) aggregates in Spark and hands rows here.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

import lineage_review_queue as q

UNIT = "place_reference"
REASON = "pnu_unresolved_reason"
# The reasons the source withheld: the value is not in the row, so a steward cannot decide it.
AWAITING_DATA = frozenset({"jibun_masked", "no_address", "no_jibun", "no_source_code"})
STEWARD, AWAITING = "steward", "awaiting_data"
SAMPLE_KEYS = 5
NO_CODE = "-"


@dataclass(frozen=True)
class Item:
    subject_code: str
    item_id: str
    status: str
    candidates_json: str
    evidence_etag: str


def sources(contracts: Mapping[str, Mapping[str, Any]]) -> list[str]:
    """The Silver tables whose rows the resolver placed: those carrying a `pnu` and its reason."""
    found = []
    for name, contract in sorted(contracts.items()):
        columns = {column["name"] for column in contract.get("columns", [])}
        if name.startswith("silver.") and {"pnu", REASON} <= columns:
            found.append(name)
    return found


def subject(table: str, reason: str, code: str | None) -> str:
    return f"{table}|{reason}|{code or NO_CODE}"


def items(table: str, rows: Iterable[Mapping[str, Any]]) -> list[Item]:
    """One item per (reason, 법정동 code) row of `table`'s unresolved rows: `reason`, `legal_dong_code`
    (or None), `rows`, `sample_row_keys`."""
    out = []
    for row in sorted(rows, key=lambda r: (str(r["reason"]), str(r.get("legal_dong_code") or ""))):
        reason = str(row["reason"] or "unknown")
        code = row.get("legal_dong_code") or None
        body = {"source": table, "reason": reason, "legal_dong_code": code, "rows": int(row["rows"]),
                "sample_row_keys": sorted(row.get("sample_row_keys") or [])[:SAMPLE_KEYS]}
        text = json.dumps(body, ensure_ascii=False, sort_keys=True)
        key = subject(table, reason, code)
        status = AWAITING if reason in AWAITING_DATA else STEWARD
        # The steward database recomputes this fingerprint on load (stewardship_domain::evidence_etag).
        out.append(Item(key, q.item_id(UNIT, key), status, text, q.evidence_etag(status, text)))
    return out


def summary(by_source: Mapping[str, list[Item]]) -> dict[str, Any]:
    """Open items and rows per source and status, for the run summary and the owner's report."""
    out: dict[str, Any] = {}
    for table, found in sorted(by_source.items()):
        counts = {STEWARD: 0, AWAITING: 0}
        rows = {STEWARD: 0, AWAITING: 0}
        for item in found:
            counts[item.status] += 1
            rows[item.status] += json.loads(item.candidates_json)["rows"]
        out[table] = {"items": counts, "rows": rows}
    return out
