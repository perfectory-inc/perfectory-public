"""One document's extraction in a process of its own.

    python -m document_text.worker <source> <media_format> <settings.json> <result.json>

The run starts one per document, so what a document holds in memory is gone when it ends, and a
document that needs more than the image's cap is the one killed. The worker raises its own OOM
score first; the tools it starts (LibreOffice, ocrmypdf, Tesseract) inherit it, so under the
container's cap the kernel picks this document's processes and never the run. On 2026-11-12 the
run itself (610 MiB after a hundred documents) was killed three times in a row at the 2 GiB cap,
losing every document of each run.
"""

from __future__ import annotations

import json
import sys
from dataclasses import asdict
from pathlib import Path

from .extract import extract


def prefer_this_process_under_memory_pressure() -> None:
    """Raise this process's OOM score to the top. Any process may raise its own score; lowering
    it would need a capability the image does not have."""
    try:
        Path("/proc/self/oom_score_adj").write_text("1000", encoding="utf-8")
    except OSError:
        pass  # not Linux (a test): there is no kernel choice to steer


def main(argv: list[str] | None = None) -> int:
    source, media_format, settings_path, result_path = argv if argv is not None else sys.argv[1:]
    prefer_this_process_under_memory_pressure()
    settings = json.loads(Path(settings_path).read_text(encoding="utf-8"))
    result = extract(Path(source), media_format, settings)
    Path(result_path).write_text(json.dumps({
        "outcome": result.outcome,
        "pages": [asdict(page) for page in result.pages],
        "ocr_page_count": result.ocr_page_count,
        "error": result.error,
    }, ensure_ascii=False), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
