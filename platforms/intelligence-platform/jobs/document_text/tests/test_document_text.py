"""The document text job's own logic: positions, citations, the partial plan, the contract rows.

The tools themselves (pdfplumber, ocrmypdf, LibreOffice) run in the image and are measured
there (root ADR-0477 §Consequences); nothing here imports them.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from document_text import extract as extract_module
from document_text.extract import Result, classify, extract, pages_needing_ocr
from document_text.pages import assemble, citation_holds, cite, document_text
from document_text.plan import plan
from document_text.run import CONTRACT, load_contract, main, outcome_row, page_rows

SHA_A = "a" * 64
SHA_B = "b" * 64


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
        layers = iter([["가" * 30, ""], ["가" * 30 + "(ocr)", "스캔한 쪽"]])
        with mock.patch.object(extract_module, "pdf_page_texts", side_effect=lambda _p: next(layers)), mock.patch.object(
            extract_module, "ocr_pages"
        ) as ocr:
            result = extract(Path("x.pdf"), "pdf", self.settings)
        self.assertEqual(ocr.call_args.args[2], [2])
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
                self.assertEqual(main(["--index", str(root / "index.jsonl"), "--done", str(root / "done.txt"), "--media", tmp, "--out", str(root / "out")]), 0)
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

    def test_the_contract_is_the_areas_file(self) -> None:
        self.assertEqual(CONTRACT.parent.name, "config")
        self.assertTrue(CONTRACT.is_file())


if __name__ == "__main__":
    unittest.main()
