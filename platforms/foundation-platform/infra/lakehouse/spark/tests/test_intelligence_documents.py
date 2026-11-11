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
from datetime import datetime
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


if __name__ == "__main__":
    unittest.main()
