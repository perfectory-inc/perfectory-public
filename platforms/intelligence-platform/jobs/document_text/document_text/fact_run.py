"""One fact run: the documents whose text this fact version has not read, rules first, the model for the rest.

    python -m document_text.fact_run --pages DIR --done done.txt --media DIR --out DIR

`--pages` holds the text run's `document_text_outcome.jsonl` and `document_text_page.jsonl`,
`--done` the hashes this fact version already has an outcome for, `--media` the bytes (only the
vision cross-check reads them). The run writes `document_fact.jsonl`,
`document_fact_outcome.jsonl` and `summary.json`.

Every written fact passed its check (facts.py); what failed is counted in the outcome, not
written. A document's facts and its outcome are written together, once: a document that needs
the model (MODEL_RUNTIME_*, model_port.py) when the model is not there, errs, or this run's
model budget is spent is not written at all, so the next run reads it again whole (its rules
take milliseconds) and no fact is appended twice.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from .facts import Fact, load_ksic, rule_facts
from .model_facts import land_use_by_model, page_png, vision_confirms
from .model_port import Port
from .pages import Page
from .run import CONTRACT, load_contract


def fact_rows(contract: dict, sha: str, facts: list[Fact], at: str) -> list[dict]:
    settings = contract["document_fact"]
    columns = contract["outputs"]["document_fact"]["columns"]
    rows = []
    for index, fact in enumerate(facts):
        row = {
            "sha256": sha,
            "extractor_version": settings["extractor_version"],
            "text_extractor_version": settings["text_extractor_version"],
            "fact_kind": fact.fact_kind,
            "fact_index": index,
            "value_json": json.dumps(fact.value, ensure_ascii=False, sort_keys=True),
            "page_number": fact.page_number,
            "char_start": fact.char_start,
            "char_end": fact.char_end,
            "quote": fact.quote,
            "method": fact.method,
            "model": fact.model,
            "check": fact.check,
            "extracted_at_utc": at,
        }
        assert list(row) == columns
        rows.append(row)
    return rows


def needs_vision(fact: Fact, methods: dict[int, str]) -> bool:
    """An OCR'd value with no table total behind it (ADR-0477 §5: OCR numbers were 85 % right)."""
    return methods[fact.page_number] == "ocr" and fact.check != "table_total"


def read_document(contract: dict, sha: str, media_format: str, pages: list[Page], ksic, port: Port | None, media: Path | None, budget: dict) -> tuple[list[Fact], dict]:
    settings = contract["document_fact"]
    methods = {p.page_number: p.method for p in pages}
    found = rule_facts(pages, ksic)
    stats: dict = {"unsolved_land_use_pages": found.land_use_unsolved_pages, "complete": True}
    facts = list(found.facts)
    if found.land_use_unsolved_pages:
        if port is None or budget["model_pages"] < len(found.land_use_unsolved_pages):
            stats["complete"] = False
            return [], stats
        budget["model_pages"] -= len(found.land_use_unsolved_pages)
        facts.extend(land_use_by_model(port, pages, found.land_use_unsolved_pages, stats))
        if stats.get("model_errors"):
            stats["complete"] = False
            return [], stats
    kept: list[Fact] = []
    dropped = Counter()
    pdf = media / f"{sha}.{media_format}" if media else None
    images: dict[int, bytes] = {}
    for fact in facts:
        if not needs_vision(fact, methods):
            kept.append(fact)
            continue
        if media_format not in settings["vision_formats"]:
            dropped[f"{fact.fact_kind}:ocr_unchecked"] += 1  # a printed HWP is not kept to draw
            continue
        if port is None or pdf is None or not pdf.is_file():
            stats["complete"] = False
            return [], stats
        try:
            if fact.page_number not in images:
                images[fact.page_number] = page_png(pdf, fact.page_number, settings["render_timeout_seconds"])
        except Exception:  # a page Ghostscript cannot draw keeps its value out, counted
            dropped[f"{fact.fact_kind}:page_not_drawn"] += 1
            continue
        try:
            confirmed = vision_confirms(port, settings["vision_model"], images[fact.page_number], fact)
        except Exception:  # the model being away: read the document again next run
            stats["complete"] = False
            return [], stats
        stats["vision_calls"] = stats.get("vision_calls", 0) + 1
        if confirmed:
            kept.append(Fact(fact.fact_kind, fact.value, fact.page_number, fact.char_start, fact.char_end, fact.quote, fact.check + "+vision", fact.method, fact.model))
        else:
            dropped[f"{fact.fact_kind}:vision_disagreed"] += 1
    stats["dropped"] = dict(dropped)
    return kept, stats


def outcome_row(contract: dict, sha: str, facts: list[Fact], stats: dict, model_calls: int, elapsed_ms: int, at: str) -> dict:
    settings = contract["document_fact"]
    row = {
        "sha256": sha,
        "extractor_version": settings["extractor_version"],
        "text_extractor_version": settings["text_extractor_version"],
        "fact_counts_json": json.dumps(dict(Counter(f.fact_kind for f in facts)), ensure_ascii=False, sort_keys=True),
        "unsolved_pages_json": json.dumps(stats["unsolved_land_use_pages"]),
        "dropped_json": json.dumps(stats["dropped"], ensure_ascii=False, sort_keys=True),
        "model_calls": model_calls,
        "elapsed_ms": elapsed_ms,
        "extracted_at_utc": at,
    }
    assert list(row) == contract["outputs"]["document_fact_outcome"]["columns"]
    return row


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pages", type=Path, required=True)
    parser.add_argument("--done", type=Path)
    parser.add_argument("--media", type=Path)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--contract", type=Path, default=CONTRACT)
    args = parser.parse_args(argv)

    contract = load_contract(args.contract)
    settings = contract["document_fact"]
    done = set(args.done.read_text(encoding="utf-8").split()) if args.done else set()
    formats = {}
    for line in (args.pages / "document_text_outcome.jsonl").read_text(encoding="utf-8").splitlines():
        if line.strip():
            row = json.loads(line)
            if row["extractor_version"] == settings["text_extractor_version"] and row["sha256"] not in done:
                formats[row["sha256"]] = row["media_format"]
    pages: dict[str, list[Page]] = defaultdict(list)
    for line in (args.pages / "document_text_page.jsonl").read_text(encoding="utf-8").splitlines():
        if line.strip():
            row = json.loads(line)
            if row["sha256"] in formats and row["extractor_version"] == settings["text_extractor_version"]:
                pages[row["sha256"]].append(Page(row["page_number"], row["char_start"], row["char_end"], row["method"], row["text"]))
    planned = sorted(formats)[: settings["per_run_limit"]]
    port = Port.from_env(settings["model_timeout_seconds"])
    ksic = load_ksic()
    budget = {"model_pages": settings["model_pages_per_run"]}

    args.out.mkdir(parents=True, exist_ok=True)
    totals: Counter = Counter()
    dropped: Counter = Counter()
    started = time.monotonic()
    with (args.out / "document_fact.jsonl").open("w", encoding="utf-8") as fact_file, (args.out / "document_fact_outcome.jsonl").open("w", encoding="utf-8") as outcome_file:
        for sha in planned:
            if time.monotonic() - started >= settings["run_time_budget_seconds"]:
                break  # unread documents have no outcome, so the next run plans them
            doc_pages = sorted(pages[sha], key=lambda p: p.page_number)
            t0 = time.monotonic()
            facts, stats = read_document(contract, sha, formats[sha], doc_pages, ksic, port, args.media, budget)
            if not stats["complete"]:
                totals["deferred"] += 1
                continue
            at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            for row in fact_rows(contract, sha, facts, at):
                fact_file.write(json.dumps(row, ensure_ascii=False) + "\n")
            outcome_file.write(json.dumps(outcome_row(contract, sha, facts, stats, stats.get("model_calls", 0) + stats.get("vision_calls", 0), int((time.monotonic() - t0) * 1000), at), ensure_ascii=False) + "\n")
            fact_file.flush()
            outcome_file.flush()
            totals.update(f"{f.fact_kind}:{f.method}" for f in facts)
            dropped.update(stats["dropped"])
            totals["documents"] += 1
            totals["unsolved_land_use_pages"] += len(stats["unsolved_land_use_pages"])
            totals["model_tables_closed"] += stats.get("model_tables_closed", 0)
            totals["model_tables_rejected"] += stats.get("model_tables_rejected", 0)
            totals["model_errors"] += stats.get("model_errors", 0)

    summary = {
        "extractor_version": settings["extractor_version"],
        "planned": len(planned),
        "model": port.model if port else None,
        "run_seconds": round(time.monotonic() - started, 1),
        "counts": dict(totals),
        "dropped": dict(dropped),
    }
    (args.out / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
