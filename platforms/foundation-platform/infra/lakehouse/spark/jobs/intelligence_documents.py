"""The fold rule of the intelligence document jobs (root ADR-0477): what a complex's panel may state.

    intelligence_documents.py gate      --extraction <contract> --dir <run dir>
    intelligence_documents.py fold-rows --extraction <contract> --dir <run dir>

The lakehouse half of the jobs (what is owed, the rows a run wrote, the fold's reads and its append)
is the publisher's `intelligence-documents` command, computed by `DuckDB` (root ADR-0559); it was
this file on Spark. What stays here is foundation's own read of the facts (root ADR-0477 §1:
foundation decides what a screen shows), as plain Python between the publisher's two fold steps:

- `gate` writes `gate.json`, the (fact_kind, method, page_method) a screen may show, as the fold
  records it; the publisher's `fold-export` reads it to tell whether the table holds rows folded
  under another gate (the declared full refold);
- `fold-rows` reads `fold-rows.jsonl` (the facts joined to their attachment, notice and page) and
  `fold-codes.txt` (the complexes in scope), and writes `fold-append.jsonl`: one row per complex
  of `gold.complex_document_facts`, which the publisher's `fold-append` appends. Only the kinds that
  passed the hand-checked gate are shown (§5), each from the newest notice that states it, with the
  page it was read on.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

COMPLEX_FACTS_SCHEMA = "complex-document-facts.v1"
# The kinds a complex's panel states. A parcel_lot row is one 토지조서 lot: which parcels the
# complex holds is foundation's place_id resolver's to decide (ADR-0477 §2), not a panel line.
PANEL_KINDS = ("land_use_area", "invited_industry", "sale_price")
SUMMARY_PREFIX = "intelligence-documents-summary-json "
GATE_FILE = "gate.json"
FOLD_ROWS_FILE = "fold-rows.jsonl"
FOLD_CODES_FILE = "fold-codes.txt"
FOLD_APPEND_FILE = "fold-append.jsonl"
# gold.complex_document_facts, less the run the publisher stamps.
FOLD_COLUMNS = ("official_complex_code", "document_facts_json", "fact_count", "fact_extractor_version",
                "accuracy_gate_json", "folded_at_utc")


def load_extraction(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def summary_line(summary: Mapping[str, Any]) -> None:
    print(SUMMARY_PREFIX + json.dumps(summary, ensure_ascii=False, sort_keys=True), flush=True)


def admitted(extraction: Mapping[str, Any]) -> list[tuple[str, str, str]]:
    """The (fact_kind, method, page_method) a screen may show: checked by hand on at least the gate's
    sample with at least its precision (root ADR-0477 §5). What was never measured is not shown."""
    gate = extraction["outputs"]["document_fact"]["accuracy_gate"]
    return sorted({(m["fact_kind"], m["method"], m["page_method"]) for m in gate["measured"]
                   if m["checked"] >= gate["hand_checked_sample_min"]
                   and m["correct"] >= gate["min_precision"] * m["checked"]})


def newest_first(row: Mapping[str, Any]) -> tuple:
    """Newest notice first (one with no date last), then the attachment the notice lists first."""
    notice_date = row.get("notice_date")
    return (notice_date is None, -(notice_date.toordinal() if notice_date else 0), -row["notice_sequence"],
            row.get("file_sequence") or 0, row["sha256"])


def complex_facts(rows: Iterable[Mapping[str, Any]], gate: Iterable[tuple[str, str, str]]) -> dict[str, dict[str, Any]]:
    """{official_complex_code: document} from fact rows joined to their attachment, notice and page.

    Per kind one source: the newest notice's attachment that states the kind, with all its admitted
    facts in order. An older notice's table is the plan a later one changed; showing both would
    state two areas for one 용지."""
    allowed = set(gate)
    by_kind: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for row in rows:
        if row["fact_kind"] in PANEL_KINDS and (row["fact_kind"], row["method"], row["page_method"]) in allowed:
            by_kind.setdefault((row["official_complex_code"], row["fact_kind"]), []).append(row)
    documents: dict[str, dict[str, Any]] = {}
    for (code, kind), found in sorted(by_kind.items()):
        source = min(found, key=newest_first)
        facts = sorted((r for r in found if (r["sha256"], r["notice_sequence"]) == (source["sha256"], source["notice_sequence"])),
                       key=lambda r: r["fact_index"])
        notice_date = source.get("notice_date")
        documents.setdefault(code, {"schema": COMPLEX_FACTS_SCHEMA, "kinds": {}})["kinds"][kind] = {
            "source": {
                "notice_sequence": source["notice_sequence"],
                "notice_number": source.get("notice_number"),
                "notice_title": source.get("notice_title"),
                "notice_date": notice_date.isoformat() if notice_date else None,
                "sha256": source["sha256"],
                "media_format": source["media_format"],
                "file_title": source.get("file_title"),
            },
            "facts": [{"value": json.loads(r["value_json"]), "page_number": r["page_number"], "quote": r["quote"],
                       "method": r["method"], "page_method": r["page_method"], "check": r["check"]} for r in facts],
        }
    return documents


def complex_fact_rows(documents: Mapping[str, Mapping[str, Any]], codes: Iterable[str], extraction: Mapping[str, Any],
                      gate: Sequence[tuple[str, str, str]], run_id: str, now: datetime) -> list[tuple]:
    """gold.complex_document_facts rows for `codes`, in its column order. A complex in scope with no
    admitted fact gets a row with none, so a fact the gate no longer admits leaves its panel."""
    recorded_gate = gate_json(gate)
    version = extraction["document_fact"]["extractor_version"]
    rows = []
    for code in sorted(set(codes)):
        document = documents.get(code) or {"schema": COMPLEX_FACTS_SCHEMA, "kinds": {}}
        count = sum(len(kind["facts"]) for kind in document["kinds"].values())
        rows.append((code, json.dumps(document, ensure_ascii=False, sort_keys=True), count, version, recorded_gate, now, run_id))
    return rows


def gate_json(gate: Sequence[tuple[str, str, str]]) -> str:
    """The gate as a folded row records it (`accuracy_gate_json`)."""
    return json.dumps([list(g) for g in gate], ensure_ascii=False)


def fold_rows(directory: Path, extraction: Mapping[str, Any], now: datetime) -> dict[str, Any]:
    """Writes `fold-append.jsonl` from the publisher's fold files; returns the step's summary."""
    gate = admitted(extraction)
    rows = []
    for line in (directory / FOLD_ROWS_FILE).read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        row["notice_date"] = date.fromisoformat(row["notice_date"]) if row["notice_date"] else None
        rows.append(row)
    codes = [code for code in (directory / FOLD_CODES_FILE).read_text(encoding="utf-8").splitlines() if code]
    documents = complex_facts(rows, gate)
    folded = complex_fact_rows(documents, codes, extraction, gate, "", now)
    with (directory / FOLD_APPEND_FILE).open("w", encoding="utf-8") as handle:
        for row in folded:
            values = dict(zip(FOLD_COLUMNS, row[:len(FOLD_COLUMNS)], strict=True))
            values["folded_at_utc"] = values["folded_at_utc"].strftime("%Y-%m-%dT%H:%M:%SZ")
            handle.write(json.dumps(values, ensure_ascii=False) + "\n")
    return {"step": "fold-rows", "admitted": [list(g) for g in gate], "complexes": len(folded),
            "with_facts": len(documents), "facts": sum(row[2] for row in folded)}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("gate", "fold-rows"))
    parser.add_argument("--extraction", type=Path, required=True, help="the intelligence image's document-extraction.contract.json")
    parser.add_argument("--dir", type=Path, required=True, help="the run's directory")
    args = parser.parse_args(argv)
    extraction = load_extraction(args.extraction)
    if args.command == "gate":
        (args.dir / GATE_FILE).write_text(gate_json(admitted(extraction)), encoding="utf-8")
        summary_line({"step": "gate", "admitted": [list(g) for g in admitted(extraction)]})
        return 0
    summary_line(fold_rows(args.dir, extraction, datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
