"""One document's bytes to positioned page text, with mature tools only (root ADR-0477 §3).

- PDF: the text layer through pdfplumber (MIT). A page with fewer than `ocr_below_chars`
  visible characters is a scan: those pages alone go through ocrmypdf (MPL-2.0) with
  Tesseract (Apache-2.0), and their text is read back from the OCR'd file.
- HWP 5 / HWPX: LibreOffice (MPL-2.0) with the H2Orestart import filter (GPL-3.0, run as a
  separate process, never linked) prints the document to PDF, which then takes the PDF path.
  Page numbers are those of the printed PDF. pyhwp is not used: it is AGPL and would be
  imported into this proprietary tree.

The tools run as child processes with their own timeouts so that one bad file ends as a
`failed` outcome, not as the run.
"""

from __future__ import annotations

import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from .pages import Page, assemble


@dataclass
class Result:
    outcome: str
    pages: list[Page] = field(default_factory=list)
    ocr_page_count: int = 0
    error: str | None = None

    @property
    def char_count(self) -> int:
        return sum(len(page.text) for page in self.pages)


def visible_chars(text: str) -> int:
    return sum(1 for ch in text if not ch.isspace())


def pages_needing_ocr(texts: list[str], below: int) -> list[int]:
    """1-based numbers of the pages whose text layer is too thin to be the page."""
    return [number for number, text in enumerate(texts, start=1) if visible_chars(text) < below]


def classify(page_count: int, ocr_pages: int, char_count: int) -> str:
    if page_count == 0 or char_count == 0:
        return "empty"
    if ocr_pages == 0:
        return "text"
    if ocr_pages == page_count:
        return "ocr"
    return "partial_ocr"


def pdf_page_texts(path: Path) -> list[str]:
    import pdfplumber  # the image's pinned dependency; unit tests never reach here

    with pdfplumber.open(path) as pdf:
        texts = []
        for page in pdf.pages:
            texts.append(page.extract_text() or "")
            page.flush_cache()  # keeps one page's objects in memory, not the document's
        return texts


def ocr_pages(src: Path, dst: Path, pages: list[int], languages: str, jobs: int, timeout: int) -> None:
    subprocess.run(
        [
            "ocrmypdf",
            "--force-ocr",
            "--pages",
            ",".join(str(p) for p in pages),
            "-l",
            languages,
            "--jobs",
            str(jobs),
            "--output-type",
            "pdf",
            "--quiet",
            str(src),
            str(dst),
        ],
        check=True,
        timeout=timeout,
        capture_output=True,
    )


def convert_to_pdf(src: Path, workdir: Path, timeout: int) -> Path:
    profile = workdir / "lo-profile"
    subprocess.run(
        [
            "soffice",
            f"-env:UserInstallation={profile.as_uri()}",
            "--headless",
            "--norestore",
            "--convert-to",
            "pdf",
            "--outdir",
            str(workdir),
            str(src),
        ],
        check=True,
        timeout=timeout,
        capture_output=True,
    )
    out = workdir / (src.stem + ".pdf")
    if not out.is_file():
        raise RuntimeError("LibreOffice printed no PDF (the import filter did not read the file)")
    return out


def extract(path: Path, media_format: str, settings: dict) -> Result:
    fmt = media_format.lower()
    if fmt not in settings["pdf_formats"] and fmt not in settings["converted_formats"]:
        return Result("unsupported_format")
    try:
        with tempfile.TemporaryDirectory(prefix="document-text-") as tmp:
            workdir = Path(tmp)
            pdf = path
            if fmt in settings["converted_formats"]:
                pdf = convert_to_pdf(path, workdir, settings["convert_timeout_seconds"])
            texts = pdf_page_texts(pdf)
            methods = ["text_layer"] * len(texts)
            thin = pages_needing_ocr(texts, settings["ocr_below_chars"])
            if thin:
                ocred = workdir / "ocr.pdf"
                ocr_pages(pdf, ocred, thin, settings["ocr_languages"], settings["ocr_jobs"], settings["ocr_timeout_seconds_per_page"] * len(thin))
                ocr_texts = pdf_page_texts(ocred)
                for number in thin:
                    texts[number - 1] = ocr_texts[number - 1]
                    methods[number - 1] = "ocr"
            pages = assemble(list(zip(methods, texts)))
            result = Result("", pages, len(thin))
            result.outcome = classify(len(pages), len(thin), result.char_count)
            return result
    except subprocess.TimeoutExpired as error:
        return Result("failed", error=f"timeout after {error.timeout}s: {Path(str(error.cmd[0])).name}")
    except subprocess.CalledProcessError as error:
        tail = (error.stderr or b"").decode("utf-8", "replace").strip().splitlines()[-1:]
        return Result("failed", error=f"{Path(str(error.cmd[0])).name} exit {error.returncode}: {' '.join(tail)}")
    except Exception as error:  # a parser's failure on one file is that file's outcome
        return Result("failed", error=f"{type(error).__name__}: {error}")
