"""The lakehouse half of the intelligence document jobs (root ADR-0477), without Spark.

The intelligence platform's contract names the columns its image writes; the lakehouse contract
artifact names the columns the tables hold. Two files, two writers: if they drift, a run of a
newer image is refused at append (run_rows) — this holds them equal before that.
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


class ContractParityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.extraction = d.load_extraction(EXTRACTION_PATH)
        self.contracts = json.loads(ARTIFACT.read_text(encoding="utf-8"))["contracts"]

    def test_every_output_table_is_the_intelligence_columns_then_the_run(self) -> None:
        for table in d.TABLES:
            with self.subTest(table=table):
                held = [c["name"] for c in self.contracts[table]["columns"]]
                self.assertEqual(held, d.table_columns(self.extraction, table))

    def test_every_output_of_the_intelligence_contract_has_a_table(self) -> None:
        outputs = {v["table"] for k, v in self.extraction["outputs"].items() if not k.startswith("$")}
        self.assertEqual(outputs, set(d.TABLES))

    def test_the_tables_key_is_the_intelligence_key_and_the_run_orders_a_repeat(self) -> None:
        for table in d.TABLES:
            output, _ = d.OUTPUTS[table]
            latest = self.contracts[table]["current_row_latest"]
            self.assertEqual(latest["key"], self.extraction["outputs"][output]["key"])
            self.assertEqual(latest["newest_first"], [d.RUN_COLUMN])
            self.assertEqual(self.contracts[table]["load"], {"unit": "run", "column": d.RUN_COLUMN})

    def test_the_media_prefix_is_read_through_the_pointer(self) -> None:
        self.assertEqual(d.media_prefix(PLATFORM, self.extraction), "onbid/media")


class PlanTest(unittest.TestCase):
    def test_a_media_key_is_the_content_address(self) -> None:
        self.assertEqual(d.media_key("onbid/media/", SHA, "pdf"), f"onbid/media/sha256/{SHA}.pdf")
        with self.assertRaises(ValueError):
            d.media_key("onbid/media", "../" + SHA[3:], "pdf")
        with self.assertRaises(ValueError):
            d.media_key("onbid/media", SHA, "p/df")

    def test_only_the_formats_the_text_job_reads_are_fetched(self) -> None:
        self.assertEqual(set(d.readable_formats(d.load_extraction(EXTRACTION_PATH))), {"pdf", "hwp", "hwpx"})


class RunRowsTest(unittest.TestCase):
    def test_rows_are_stamped_with_the_run_in_the_table_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "x.jsonl"
            path.write_text(json.dumps({"sha256": SHA, "n": 1, "at": "2026-11-11T01:02:03Z"}) + "\n\n", encoding="utf-8")
            rows = d.run_rows(path, ["sha256", "n", "at", "run_id"], "20261111T010000Z", ["at"])
        self.assertEqual(rows, [(SHA, 1, datetime(2026, 11, 11, 1, 2, 3), "20261111T010000Z")])

    def test_a_row_the_contract_does_not_name_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "x.jsonl"
            path.write_text(json.dumps({"sha256": SHA, "extra": 1}) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "x.jsonl:1"):
                d.run_rows(path, ["sha256", "run_id"], "r", [])

    def test_a_run_that_wrote_no_file_has_no_rows(self) -> None:
        self.assertEqual(d.run_rows(Path(tempfile.gettempdir()) / "absent-intelligence.jsonl", ["run_id"], "r", []), [])


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

    def test_the_contract_admits_the_three_kinds_measured_on_text_pages(self) -> None:
        self.assertEqual(d.admitted(d.load_extraction(EXTRACTION_PATH)), [
            ("invited_industry", "rule", "text_layer"), ("land_use_area", "rule", "text_layer"),
            ("parcel_lot", "rule", "text_layer")])

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
        contract = json.loads(ARTIFACT.read_text(encoding="utf-8"))["contracts"][d.COMPLEX_FACTS]
        self.assertEqual(len(rows[0]), len(contract["columns"]))
        self.assertEqual(contract["load"], {"unit": "run", "column": d.RUN_COLUMN})


if __name__ == "__main__":
    unittest.main()
