"""The document text job's own logic: positions, citations, the partial plan, the contract rows.

The tools themselves (pdfplumber, ocrmypdf, LibreOffice) run in the image and are measured
there (root ADR-0477 §Consequences); nothing here imports them.
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from document_text import extract as extract_module
from document_text.extract import Result, classify, extract, pages_needing_ocr
from document_text.pages import assemble, citation_holds, cite, document_text
from document_text.plan import plan
from document_text.run import (
    CONTRACT,
    MEMORY_KILL_MARK,
    extract_isolated,
    load_contract,
    main,
    mark_memory_kill,
    oom_kills,
    outcome_row,
    page_rows,
)

SHA_A = "a" * 64
SHA_B = "b" * 64


def events(root: Path, kills: int = 0) -> list[str]:
    """A cgroup's memory.events as the run reads it, in `root`: the arguments that point the run at it."""
    path = root / "memory.events"
    path.write_text(f"low 0\nhigh 0\nmax 0\noom 0\noom_kill {kills}\n", encoding="ascii")
    return ["--memory-events", str(path)]


class PagesTest(unittest.TestCase):
    def test_offsets_index_the_joined_text(self) -> None:
        pages = assemble([("text_layer", "산업단지 지정"), ("ocr", ""), ("text_layer", "면적 1,234㎡")])
        whole = document_text(pages)
        for page in pages:
            self.assertEqual(whole[page.char_start : page.char_end], page.text)
        self.assertEqual([p.page_number for p in pages], [1, 2, 3])

    def test_a_citation_holds_only_where_the_page_says_it(self) -> None:
        pages = assemble([("text_layer", "고시 제9999-1호"), ("text_layer", "총 면적 1,234㎡ 이다")])
        page, start, end = cite(pages, "1,234㎡")
        self.assertEqual(page, 2)
        self.assertTrue(citation_holds(pages, page, start, end, "1,234㎡"))
        self.assertFalse(citation_holds(pages, page, start, end, "1,235㎡"))
        self.assertFalse(citation_holds(pages, 1, start, end, "1,234㎡"))

    def test_no_quote_spans_two_pages(self) -> None:
        pages = assemble([("text_layer", "앞쪽 끝"), ("text_layer", "뒤쪽")])
        self.assertIsNone(cite(pages, "끝\f뒤"))
        self.assertIsNone(cite(pages, ""))


class PlanTest(unittest.TestCase):
    def test_only_unread_hashes_newest_notice_first(self) -> None:
        rows = [
            {"sha256": SHA_A, "media_format": "PDF", "notice_sequence": 10},
            {"sha256": SHA_A, "media_format": "pdf", "notice_sequence": 30},
            {"sha256": SHA_B, "media_format": "hwp", "notice_sequence": 20},
            {"sha256": "c" * 64, "media_format": "pdf", "notice_sequence": 40},
        ]
        docs = plan(rows, {"c" * 64}, limit=10)
        self.assertEqual([(d.sha256, d.notice_sequence, d.media_format) for d in docs], [(SHA_A, 30, "pdf"), (SHA_B, 20, "hwp")])
        self.assertEqual(len(plan(rows, set(), limit=1)), 1)


class ExtractTest(unittest.TestCase):
    settings = load_contract()["document_text"]

    def test_thin_pages_are_the_ones_sent_to_ocr(self) -> None:
        self.assertEqual(pages_needing_ocr(["가" * 30, "  \n ", "나" * 5], 20), [2, 3])

    def test_outcome_classes(self) -> None:
        self.assertEqual(classify(3, 0, 10), "text")
        self.assertEqual(classify(3, 1, 10), "partial_ocr")
        self.assertEqual(classify(3, 3, 10), "ocr")
        self.assertEqual(classify(0, 0, 0), "empty")
        self.assertEqual(classify(2, 0, 0), "empty")

    def test_an_unknown_format_is_counted_not_read(self) -> None:
        self.assertEqual(extract(Path("x.zip"), "zip", self.settings).outcome, "unsupported_format")

    def test_only_thin_pages_take_the_ocr_text(self) -> None:
        # The raster holds the thin pages alone, so its OCR'd text has one page per thin page.
        layers = iter([["가" * 30, "", "나" * 30, " "], ["스캔한 쪽", "넷째 쪽"]])
        with mock.patch.object(extract_module, "pdf_page_texts", side_effect=lambda _p: next(layers)), mock.patch.object(
            extract_module, "rasterize_pages"
        ) as raster, mock.patch.object(extract_module, "ocr_pages"):
            result = extract(Path("x.pdf"), "pdf", self.settings)
        self.assertEqual(raster.call_args.args[2], [2, 4])
        self.assertEqual([(p.method, p.text) for p in result.pages],
                         [("text_layer", "가" * 30), ("ocr", "스캔한 쪽"), ("text_layer", "나" * 30), ("ocr", "넷째 쪽")])

    def test_an_ocr_that_lost_a_page_is_that_files_failure(self) -> None:
        layers = iter([["", ""], ["스캔한 쪽"]])
        with mock.patch.object(extract_module, "pdf_page_texts", side_effect=lambda _p: next(layers)), mock.patch.object(
            extract_module, "rasterize_pages"
        ), mock.patch.object(extract_module, "ocr_pages"):
            result = extract(Path("x.pdf"), "pdf", self.settings)
        self.assertEqual((result.outcome, result.pages), ("failed", []))
        self.assertIn("not the 2 sent", result.error)

    def test_one_thin_page_of_two_is_partial_ocr(self) -> None:
        layers = iter([["가" * 30, ""], ["스캔한 쪽"]])
        with mock.patch.object(extract_module, "pdf_page_texts", side_effect=lambda _p: next(layers)), mock.patch.object(
            extract_module, "rasterize_pages"
        ), mock.patch.object(extract_module, "ocr_pages"):
            result = extract(Path("x.pdf"), "pdf", self.settings)
        self.assertEqual(result.outcome, "partial_ocr")
        self.assertEqual([(p.method, p.text) for p in result.pages], [("text_layer", "가" * 30), ("ocr", "스캔한 쪽")])

    def test_a_tool_failure_is_that_files_outcome(self) -> None:
        with mock.patch.object(extract_module, "convert_to_pdf", side_effect=RuntimeError("no PDF")):
            result = extract(Path("x.hwp"), "hwp", self.settings)
        self.assertEqual(result.outcome, "failed")
        self.assertIn("no PDF", result.error)


class ContractTest(unittest.TestCase):
    def test_rows_are_the_contract_columns(self) -> None:
        contract = load_contract()
        docs = plan([{"sha256": SHA_A, "media_format": "pdf", "notice_sequence": 1}], set(), limit=1)
        result = Result("text", assemble([("text_layer", "본문")]))
        self.assertEqual(list(outcome_row(contract, docs[0], result, 5, "t")), contract["outputs"]["document_text_outcome"]["columns"])
        self.assertEqual(list(page_rows(contract, docs[0], result)[0]), contract["outputs"]["document_text_page"]["columns"])

    def test_every_outcome_and_method_is_declared(self) -> None:
        outputs = load_contract()["outputs"]
        for outcome in ["text", "partial_ocr", "ocr", "empty", "unsupported_format", "failed"]:
            self.assertIn(outcome, outputs["document_text_outcome"]["outcomes"])
        self.assertEqual(set(outputs["document_text_page"]["methods"]), {"text_layer", "ocr"})

    def test_inputs_and_outputs_share_no_table(self) -> None:
        contract = load_contract()
        inputs = contract["inputs"]
        read = {inputs["attachment_index"]["table"], *inputs["place_identity"]["tables"]}
        written = {v["table"] for k, v in contract["outputs"].items() if isinstance(v, dict)}
        self.assertFalse(read & written)
        self.assertTrue(all(t.startswith("intelligence.") for t in written))

    def test_a_run_skips_what_is_done_and_writes_its_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "index.jsonl").write_text(
                "\n".join(json.dumps({"sha256": s, "media_format": f, "notice_sequence": 1}) for s, f in [(SHA_A, "pdf"), (SHA_B, "zip")]),
                encoding="utf-8",
            )
            (root / "done.txt").write_text(SHA_A + "\n", encoding="utf-8")
            with mock.patch("sys.stdout"):
                self.assertEqual(main(["--index", str(root / "index.jsonl"), "--done", str(root / "done.txt"), "--media", tmp, "--out", str(root / "out"), *events(root)]), 0)
            outcomes = [json.loads(l) for l in (root / "out" / "document_text_outcome.jsonl").read_text(encoding="utf-8").splitlines()]
            self.assertEqual([(o["sha256"], o["outcome"]) for o in outcomes], [(SHA_B, "unsupported_format")])
            self.assertEqual(json.loads((root / "out" / "summary.json").read_text(encoding="utf-8"))["by_format"]["zip"]["documents"], 1)

    def test_a_run_past_its_budget_starts_no_new_document(self) -> None:
        contract = load_contract()
        contract["document_text"]["run_time_budget_seconds"] = 0
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "contract.json").write_text(json.dumps(contract), encoding="utf-8")
            (root / "index.jsonl").write_text(json.dumps({"sha256": SHA_A, "media_format": "zip", "notice_sequence": 1}), encoding="utf-8")
            with mock.patch("sys.stdout"):
                main(["--index", str(root / "index.jsonl"), "--media", tmp, "--out", str(root / "out"), "--contract", str(root / "contract.json")])
            self.assertEqual((root / "out" / "document_text_outcome.jsonl").read_text(encoding="utf-8"), "")
            summary = json.loads((root / "out" / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual((summary["planned"], summary["read"]), (1, 0))

    def test_a_worker_that_dies_is_that_documents_failure_and_the_run_goes_on(self) -> None:
        dies = (sys.executable, "-c", "import os; os._exit(137)")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "index.jsonl").write_text(
                "\n".join(json.dumps({"sha256": s, "media_format": "pdf", "notice_sequence": n}) for s, n in [(SHA_A, 2), (SHA_B, 1)]),
                encoding="utf-8",
            )
            with mock.patch("document_text.run.WORKER", dies), mock.patch("sys.stdout"), mock.patch("sys.stderr"):
                self.assertEqual(main(["--index", str(root / "index.jsonl"), "--media", tmp, "--out", str(root / "out"), *events(root)]), 0)
            outcomes = [json.loads(l) for l in (root / "out" / "document_text_outcome.jsonl").read_text(encoding="utf-8").splitlines()]
            self.assertEqual([(o["sha256"], o["outcome"]) for o in outcomes], [(SHA_A, "failed"), (SHA_B, "failed")])
            self.assertTrue(all(o["error"].startswith("worker exit 137") for o in outcomes))

    def test_a_run_that_cannot_read_the_kill_count_fails_after_writing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "index.jsonl").write_text(json.dumps({"sha256": SHA_A, "media_format": "zip", "notice_sequence": 1}), encoding="utf-8")
            with mock.patch("sys.stdout"):
                rc = main(["--index", str(root / "index.jsonl"), "--media", tmp, "--out", str(root / "out"),
                           "--memory-events", str(root / "absent")])
            summary = json.loads((root / "out" / "summary.json").read_text(encoding="utf-8"))
        self.assertEqual((rc, summary["memory_kill_counter"], summary["read"]), (1, False, 1))

    def test_a_worker_hands_back_what_extract_read(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            scratch = Path(tmp)
            result = extract_isolated(scratch / "x.zip", "zip", load_contract()["document_text"], scratch)
        self.assertEqual((result.outcome, result.pages, result.error), ("unsupported_format", [], None))

    def test_a_document_the_memory_cap_ended_says_so_and_the_run_counts_it(self) -> None:
        # The worker stands in for an OCR that the cgroup's cap killed: the counter goes up while it runs.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            events = root / "memory.events"
            events.write_text("low 0\nhigh 0\nmax 3\noom 1\noom_kill 1\n", encoding="ascii")
            killed = (sys.executable, "-c", f"import os, pathlib; p = pathlib.Path({str(events)!r}); "
                      "p.write_text(p.read_text().replace('oom_kill 1', 'oom_kill 2')); os._exit(137)")
            (root / "index.jsonl").write_text(
                "\n".join(json.dumps({"sha256": s, "media_format": "pdf", "notice_sequence": n}) for s, n in [(SHA_A, 2), (SHA_B, 1)]),
                encoding="utf-8",
            )
            with mock.patch("document_text.run.WORKER", killed), mock.patch("sys.stdout"), mock.patch("sys.stderr"):
                rc = main(["--index", str(root / "index.jsonl"), "--media", tmp, "--out", str(root / "out"), "--memory-events", str(events)])
            outcomes = [json.loads(l) for l in (root / "out" / "document_text_outcome.jsonl").read_text(encoding="utf-8").splitlines()]
            summary = json.loads((root / "out" / "summary.json").read_text(encoding="utf-8"))
        # Only the first document ran while the counter rose; the second's failure is its own.
        self.assertEqual([o["error"].startswith(MEMORY_KILL_MARK) for o in outcomes], [True, False])
        self.assertEqual((summary["memory_killed"], rc), (1, 1))

    def test_a_failure_with_no_kill_counted_is_the_documents_own(self) -> None:
        result = Result("failed", error="soffice exit 1: x")
        self.assertFalse(mark_memory_kill(result, 4, 4))
        self.assertFalse(mark_memory_kill(result, None, None))  # not the image: nothing to read
        self.assertFalse(mark_memory_kill(Result("text"), 4, 5))
        self.assertEqual(result.error, "soffice exit 1: x")
        self.assertIsNone(oom_kills(Path(tempfile.gettempdir()) / "absent-memory.events"))

    def test_the_planner_knows_every_memory_kill_error_the_run_writes(self) -> None:
        kill = load_contract()["document_text"]["memory_kill"]
        self.assertIn(MEMORY_KILL_MARK, kill["error_prefixes"])
        self.assertGreater(kill["attempts"], 1)
        # The two errors of the runs before the mark (2026-11-12..19), as the run wrote them.
        for error in ["ocrmypdf exit -9: ", "worker killed by signal 9 (the image's memory cap)"]:
            self.assertTrue(error.startswith(tuple(kill["error_prefixes"])), error)

    def test_the_pages_are_drawn_at_the_contracts_resolution_before_ocr(self) -> None:
        settings = load_contract()["document_text"]
        layers = iter([["가" * 30, ""], ["스캔한 쪽"]])
        with mock.patch.object(extract_module, "pdf_page_texts", side_effect=lambda _p: next(layers)), mock.patch.object(
            extract_module.subprocess, "run"
        ) as run:
            extract(Path("x.pdf"), "pdf", settings)
        gs, ocr = (call.args[0] for call in run.call_args_list)
        self.assertEqual(gs[0], "gs")
        self.assertIn(f"-r{settings['ocr_raster_dpi']}", gs)
        self.assertIn("-sPageList=2", gs)
        self.assertEqual(ocr[0], "ocrmypdf")
        self.assertNotIn("--pages", ocr)  # every page of the raster is a page to OCR
        self.assertEqual(ocr[-2], gs[gs.index("-o") + 1])
        argv = ocr
        self.assertIn("--tesseract-downsample-large-images", argv)
        self.assertEqual(argv[argv.index("--tesseract-downsample-above") + 1], str(settings["ocr_downsample_above_px"]))

    def test_the_contract_is_the_areas_file(self) -> None:
        self.assertEqual(CONTRACT.parent.name, "config")
        self.assertTrue(CONTRACT.is_file())


if __name__ == "__main__":
    unittest.main()
