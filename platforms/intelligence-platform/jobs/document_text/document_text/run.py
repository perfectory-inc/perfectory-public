"""One run: plan the unread hashes, extract each, write the contract's output rows.

    python -m document_text.run --index index.jsonl --done done.txt --media DIR --out DIR

`--index` holds rows of the attachment index (the contract's `inputs.attachment_index.columns`),
`--done` the hashes this extractor version already has an outcome for, `--media` the bytes as
`<sha256>.<media_format>`. The run writes `document_text_outcome.jsonl`,
`document_text_page.jsonl` and `summary.json` (per format: documents, outcomes, pages, seconds;
the peak memory of the run and of its tool processes).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from .extract import extract
from .plan import plan

CONTRACT = Path(__file__).resolve().parents[3] / "config" / "document-extraction.contract.json"


def load_contract(path: Path = CONTRACT) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def outcome_row(contract: dict, doc, result, elapsed_ms: int, at: str) -> dict:
    row = {
        "sha256": doc.sha256,
        "extractor_version": contract["document_text"]["extractor_version"],
        "media_format": doc.media_format,
        "outcome": result.outcome,
        "page_count": len(result.pages),
        "ocr_page_count": result.ocr_page_count,
        "char_count": result.char_count,
        "error": result.error,
        "elapsed_ms": elapsed_ms,
        "extracted_at_utc": at,
    }
    assert list(row) == contract["outputs"]["document_text_outcome"]["columns"]
    return row


def page_rows(contract: dict, doc, result) -> list[dict]:
    version = contract["document_text"]["extractor_version"]
    columns = contract["outputs"]["document_text_page"]["columns"]
    rows = []
    for page in result.pages:
        row = {
            "sha256": doc.sha256,
            "extractor_version": version,
            "page_number": page.page_number,
            "char_start": page.char_start,
            "char_end": page.char_end,
            "method": page.method,
            "text": page.text,
        }
        assert list(row) == columns
        rows.append(row)
    return rows


def peak_memory_mib() -> dict:
    try:
        import resource
    except ImportError:  # not Linux: the measurement is only meaningful in the image
        return {}
    return {
        "self": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024,
        "largest_child": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss // 1024,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", type=Path, required=True)
    parser.add_argument("--done", type=Path)
    parser.add_argument("--media", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--contract", type=Path, default=CONTRACT)
    args = parser.parse_args(argv)

    contract = load_contract(args.contract)
    settings = contract["document_text"]
    rows = [json.loads(line) for line in args.index.read_text(encoding="utf-8").splitlines() if line.strip()]
    done = set(args.done.read_text(encoding="utf-8").split()) if args.done else set()
    docs = plan(rows, done, limit=settings["per_run_limit"])

    args.out.mkdir(parents=True, exist_ok=True)
    by_format: dict[str, dict] = defaultdict(lambda: {"documents": 0, "pages": 0, "seconds": 0.0, "outcomes": defaultdict(int)})
    started = time.monotonic()
    with (args.out / "document_text_outcome.jsonl").open("w", encoding="utf-8") as outcomes, (
        args.out / "document_text_page.jsonl"
    ).open("w", encoding="utf-8") as pages:
        for doc in docs:
            source = args.media / f"{doc.sha256}.{doc.media_format}"
            t0 = time.monotonic()
            result = extract(source, doc.media_format, settings)
            elapsed = time.monotonic() - t0
            at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            outcomes.write(json.dumps(outcome_row(contract, doc, result, int(elapsed * 1000), at), ensure_ascii=False) + "\n")
            for row in page_rows(contract, doc, result):
                pages.write(json.dumps(row, ensure_ascii=False) + "\n")
            stats = by_format[doc.media_format]
            stats["documents"] += 1
            stats["pages"] += len(result.pages)
            stats["seconds"] += elapsed
            stats["outcomes"][result.outcome] += 1
            print(f"{doc.media_format}\t{result.outcome}\t{len(result.pages)}p\t{elapsed:.1f}s\t{doc.sha256[:12]}\t{result.error or ''}", file=sys.stderr, flush=True)

    summary = {
        "extractor_version": settings["extractor_version"],
        "planned": len(docs),
        "run_seconds": round(time.monotonic() - started, 1),
        "peak_memory_mib": peak_memory_mib(),
        "by_format": {
            fmt: {**stats, "seconds": round(stats["seconds"], 1), "outcomes": dict(stats["outcomes"])}
            for fmt, stats in sorted(by_format.items())
        },
    }
    (args.out / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
