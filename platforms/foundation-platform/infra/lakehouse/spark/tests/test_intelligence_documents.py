"""The fold rule of the intelligence document jobs (root ADR-0477): the gate and what a panel states.

The lakehouse half (the plans, the run rows and their parity with the intelligence contract) is the
publisher's and is tested there (`intelligence_documents_stage`).
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import intelligence_documents as d  # noqa: E402

PLATFORM = Path(__file__).resolve().parents[4]
EXTRACTION_PATH = PLATFORM.parent / "intelligence-platform" / "config" / "document-extraction.contract.json"
ARTIFACT = PLATFORM / "infra" / "lakehouse" / "contracts" / "industrial_complex_lakehouse_contracts.json"
SHA = "a" * 64


def fact(code="999990", notice=99001, kind="land_use_area", index=0, sha=SHA, page_method="text_layer", method="rule",
         notice_date=date(2099, 1, 1), file_sequence=1, value=None):
    return {"official_complex_code": code, "notice_sequence": notice, "notice_number": f"합성시 고시 제{notice}호",
            "notice_title": "합성 산업단지계획(변경) 승인 고시", "notice_date": notice_date, "sha256": sha,
            "file_sequence": file_sequence, "file_title": "고시문.pdf", "media_format": "pdf", "fact_kind": kind,
            "fact_index": index, "value_json": json.dumps(value or {"label": f"용지{index}", "area_m2": 100.0 + index}),
            "page_number": 3, "quote": f"용지{index} 100", "method": method, "page_method": page_method,
            "check": "table_total"}


GATE = [("invited_industry", "rule", "text_layer"), ("land_use_area", "rule", "text_layer")]


class ComplexFactsTest(unittest.TestCase):
    def test_the_gate_admits_only_what_was_checked_by_hand_enough_and_precisely_enough(self) -> None:
        extraction = {"outputs": {"document_fact": {"accuracy_gate": {"hand_checked_sample_min": 30, "min_precision": 0.98, "measured": [
            {"fact_kind": "land_use_area", "method": "rule", "page_method": "text_layer", "checked": 30, "correct": 30},
            {"fact_kind": "invited_industry", "method": "rule", "page_method": "ocr", "checked": 30, "correct": 29},
            {"fact_kind": "parcel_lot", "method": "rule", "page_method": "text_layer", "checked": 29, "correct": 29},
        ]}}}}
        self.assertEqual(d.admitted(extraction), [("land_use_area", "rule", "text_layer")])

    def test_the_contract_admits_only_the_kinds_whose_last_measurement_passes(self) -> None:
        # land_use_area was 30/30 on the 2026-11-11 sample and 21/30 on the production facts of
        # 2026-11-13 (a per-phase table and a park facility table both close on their own totals), so
        # the gate takes it off the screen until the rules are fixed and measured again.
        self.assertEqual(d.admitted(d.load_extraction(EXTRACTION_PATH)), [
            ("invited_industry", "rule", "text_layer"), ("parcel_lot", "rule", "text_layer")])

    def test_each_kind_comes_from_the_newest_notice_that_states_it_in_order(self) -> None:
        rows = [fact(notice=99001, index=0, sha="b" * 64), fact(notice=99002, index=1, notice_date=date(2099, 6, 1)),
                fact(notice=99002, index=0, notice_date=date(2099, 6, 1)),
                fact(notice=99001, kind="invited_industry", sha="b" * 64, value={"ksic_division": "24"})]
        document = d.complex_facts(rows, GATE)["999990"]
        land = document["kinds"]["land_use_area"]
        self.assertEqual((land["source"]["notice_sequence"], land["source"]["notice_date"]), (99002, "2099-06-01"))
        self.assertEqual([f["value"]["label"] for f in land["facts"]], ["용지0", "용지1"])
        self.assertEqual(land["facts"][0]["page_number"], 3)
        # An older notice still answers the kinds a newer one does not state.
        self.assertEqual(document["kinds"]["invited_industry"]["source"]["sha256"], "b" * 64)

    def test_what_the_gate_does_not_admit_and_the_lots_are_not_shown(self) -> None:
        rows = [fact(page_method="ocr"), fact(method="local_llm"), fact(kind="parcel_lot"), fact(page_method=None)]
        self.assertEqual(d.complex_facts(rows, [*GATE, ("parcel_lot", "rule", "text_layer")]), {})

    def test_a_complex_in_scope_without_admitted_facts_gets_an_empty_row(self) -> None:
        extraction = d.load_extraction(EXTRACTION_PATH)
        documents = d.complex_facts([fact()], GATE)
        rows = d.complex_fact_rows(documents, ["999991", "999990", "999990"], extraction, GATE, "20991231T000000Z",
                                   datetime(2099, 12, 31))
        self.assertEqual([(r[0], r[2]) for r in rows], [("999990", 1), ("999991", 0)])
        self.assertEqual(json.loads(rows[1][1]), {"schema": d.COMPLEX_FACTS_SCHEMA, "kinds": {}})
        self.assertEqual(json.loads(rows[0][4]), [list(g) for g in GATE])
        contract = json.loads(ARTIFACT.read_text(encoding="utf-8"))["contracts"]["gold.complex_document_facts"]
        # The rule writes the table's columns but the run, which the publisher stamps.
        self.assertEqual([*d.FOLD_COLUMNS, "run_id"], [c["name"] for c in contract["columns"]])
        self.assertEqual(contract["load"], {"unit": "run", "column": "run_id"})

    def test_the_fold_reads_the_publishers_files_and_writes_the_rows_to_append(self) -> None:
        extraction = d.load_extraction(EXTRACTION_PATH)
        with tempfile.TemporaryDirectory() as raw:
            work = Path(raw)
            row = dict(fact(), notice_date="2099-01-01")
            (work / d.FOLD_ROWS_FILE).write_text(json.dumps(row, ensure_ascii=False) + "\n", encoding="utf-8")
            (work / d.FOLD_CODES_FILE).write_text("999990\n999991\n", encoding="utf-8")
            summary = d.fold_rows(work, extraction, datetime(2099, 12, 31))
            written = [json.loads(line) for line in (work / d.FOLD_APPEND_FILE).read_text(encoding="utf-8").splitlines()]
        self.assertEqual([list(r) for r in written], [list(d.FOLD_COLUMNS)] * 2)
        self.assertEqual([r["official_complex_code"] for r in written], ["999990", "999991"])
        self.assertEqual(written[0]["folded_at_utc"], "2099-12-31T00:00:00Z")
        self.assertEqual(written[0]["accuracy_gate_json"], d.gate_json(d.admitted(extraction)))
        self.assertEqual(summary["complexes"], 2)


if __name__ == "__main__":
    unittest.main()
