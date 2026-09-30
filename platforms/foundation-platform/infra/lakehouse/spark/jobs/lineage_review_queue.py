"""The parcels a person has to look at, and nothing else (root ADR-0113 §10, ADR-0103 §4).

A parcel is on the queue when the best lineage row it has is `needs_review` (ownership disagrees
with an otherwise unique match) or `pending` (nothing matched). Everything graded higher is in
effect and never shown. An item leaves the queue by data, not by code:

  * a later derivation finds stronger evidence (a higher-grade row for the same parcel), or
  * a steward decides it — the decision is itself a lineage row with `evidence_kind = "steward"`
    (decider, reason and time in `evidence_ref`), so the lineage stays the one record of why.

Each item carries its candidates — the non-pending rows for the parcel with their evidence — so
the reviewer sees what the derivation saw. Pure Python; `lineage_review_queue_to_gold.py` writes it.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from parcel_lineage import GRADE_RANK

REVIEW_GRADES = frozenset({"needs_review", "pending"})
STEWARD = "steward"


@dataclass(frozen=True)
class ReviewItem:
    item_id: str
    subject_code: str
    status: str
    candidates_json: str
    evidence_etag: str
    from_snapshot_id: str
    to_snapshot_id: str


def evidence_etag(status: str, candidates_json: str) -> str:
    """SHA-256 hex of `<status>\n<candidates_json>` — what a steward saw (root ADR-0115 §3).

    The API recomputes it from the same stored bytes (`stewardship_domain::evidence_etag`); a
    decision carrying another value is refused, and a decision whose value no longer matches the
    queue puts the parcel back in front of a person.
    """

    return hashlib.sha256(f"{status}\n{candidates_json}".encode("utf-8")).hexdigest()


def item_id(unit: str, subject_code: str) -> str:
    """Stable per parcel: the question is "where did this parcel come from", whichever run asked."""

    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"lineage-review:{unit}:{subject_code}"))


def review_queue(rows: Iterable[Mapping[str, Any]], unit: str = "parcel") -> tuple[list[ReviewItem], Counter[str]]:
    """Open items and the counts behind them (`open_<status>`, `closed_by_steward`,
    `closed_by_evidence`)."""

    by_successor: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_successor[row["successor_pnu"]].append(row)

    counts: Counter[str] = Counter()
    items: list[ReviewItem] = []
    for successor in sorted(by_successor):
        group = by_successor[successor]
        if all(r["grade"] not in REVIEW_GRADES for r in group):
            continue
        if any(r.get("evidence_kind") == STEWARD for r in group):
            counts["closed_by_steward"] += 1
            continue
        best = min(group, key=lambda r: GRADE_RANK[r["grade"]])
        if best["grade"] not in REVIEW_GRADES:
            counts["closed_by_evidence"] += 1
            continue
        candidates = sorted(
            (
                {
                    "predecessor_pnu": r["predecessor_pnu"],
                    "relation": r["relation"],
                    "grade": r["grade"],
                    "evidence_kind": r.get("evidence_kind") or "",
                    "evidence_ref": r.get("evidence_ref") or "",
                    # The link in effect, if any; a review item's best row is below that line, so
                    # only a sampled automatic link (ADR-0115 §10) will carry true here.
                    "in_effect": False,
                }
                for r in group
                if r["predecessor_pnu"]
            ),
            key=lambda c: (GRADE_RANK[c["grade"]], c["predecessor_pnu"]),
        )
        latest = max(group, key=lambda r: (r.get("to_snapshot_id") or "", r.get("from_snapshot_id") or ""))
        counts[f"open_{best['grade']}"] += 1
        candidates_json = json.dumps(candidates, ensure_ascii=False, sort_keys=True)
        items.append(
            ReviewItem(
                item_id=item_id(unit, successor),
                subject_code=successor,
                status=best["grade"],
                candidates_json=candidates_json,
                evidence_etag=evidence_etag(best["grade"], candidates_json),
                from_snapshot_id=latest.get("from_snapshot_id") or "",
                to_snapshot_id=latest.get("to_snapshot_id") or "",
            )
        )
    return items, counts


HANDOFF_SCHEMA_VERSION = "foundation-platform.lineage_review_handoff.v1"


def handoff_document(items: Iterable[ReviewItem], unit: str, published_at_utc: str) -> dict[str, Any]:
    """The queue as the steward API's database loads it (root ADR-0115 §9: the database is a
    projection the lakehouse can rebuild at any time).

    One JSON document, not JSON lines: a file cut short does not parse, so a partial queue can never
    be loaded as if it were the whole one. `item_count` is checked against `items` on load.
    """

    rows = [
        {
            "item_id": i.item_id,
            "subject_code": i.subject_code,
            "status": i.status,
            "candidates_json": i.candidates_json,
            "evidence_etag": i.evidence_etag,
            "from_snapshot_id": i.from_snapshot_id or None,
            "to_snapshot_id": i.to_snapshot_id or None,
        }
        for i in items
    ]
    return {
        "schema_version": HANDOFF_SCHEMA_VERSION,
        "unit": unit,
        "published_at_utc": published_at_utc,
        "item_count": len(rows),
        "items": rows,
    }
