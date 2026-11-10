"""The fact rules, their checks, the model's checked fallback and the fact run (root ADR-0477 §5).

Every page here is synthetic text shaped like the notice tables the rules were measured on;
no real complex, place or person is named. The model is a fake port: the real one is measured
on the hand-checked sample, not here.
"""

from __future__ import annotations

import json
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from document_text import fact_run
from document_text.facts import land_use_facts, load_ksic, parcel_lot_facts, rule_facts, sale_price_facts
from document_text.model_facts import checked_land_use
from document_text.pages import assemble, citation_holds
from document_text.run import load_contract

KSIC = load_ksic()
SHA = "f" * 64


def pages_of(*texts: str, method: str = "text_layer"):
    return assemble([(method, text) for text in texts])


def land(pages):
    facts, unsolved = land_use_facts(pages)
    return {(f.value["category"], f.value["label"], f.value["parent"], f.value["area_m2"]) for f in facts}, unsolved, facts


class LandUseTest(unittest.TestCase):
    def test_a_flat_table_closes_on_its_total_and_ratios(self) -> None:
        pages = pages_of("가. 토지이용계획\n구 분 면 적 (㎡) 구성비(%) 비 고\n계 1,000 100.0\n산업시설용지 600 60.0\n지원시설용지 100 10.0\n공공시설용지 300 30.0\n나. 기반시설계획")
        got, unsolved, facts = land(pages)
        self.assertEqual(got, {("합계", "계", None, 1000.0), ("산업시설용지", "산업시설용지", None, 600.0), ("지원시설용지", "지원시설용지", None, 100.0), ("공공시설용지", "공공시설용지", None, 300.0)})
        self.assertEqual(unsolved, [])
        for fact in facts:
            self.assertEqual(fact.check, "table_total")
            self.assertTrue(citation_holds(pages, fact.page_number, fact.char_start, fact.char_end, fact.quote))

    def test_a_table_that_does_not_close_is_left_to_the_model(self) -> None:
        pages = pages_of("토지이용계획\n계 1,000 100.0\n산업시설용지 600 60.0\n공공시설용지 300 30.0")
        got, unsolved, _ = land(pages)
        self.assertEqual(got, set())
        self.assertEqual(unsolved, [1])

    def test_a_ratio_that_disagrees_with_the_area_rejects_the_column(self) -> None:
        pages = pages_of("토지이용계획\n계 1,000 100.0\n산업시설용지 600 50.0\n공공시설용지 400 50.0")
        self.assertEqual(land(pages)[0], set())

    def test_subtotal_groups_take_their_name_from_the_merged_cell_beside_them(self) -> None:
        pages = pages_of(
            "토지이용계획\n구 분 면 적(㎡) 비 고\n합 계 1,000.0\n소계 700.0\n산업\n시설 공장용지 650.0\n용지\n폐기물처리장 50.0\n"
            "소계 300.0\n도로 200.0\n공공 녹지 60.0\n시설\n용지 배수지 40.0"
        )
        got, _, _ = land(pages)
        self.assertIn(("산업시설용지", "소계", None, 700.0), got)
        self.assertIn(("공장용지", "공장용지", "산업시설용지", 650.0), got)
        self.assertIn(("공공시설용지", "소계", None, 300.0), got)
        self.assertIn((None, "녹지", "공공시설용지", 60.0), got)
        self.assertIn((None, "배수지", "공공시설용지", 40.0), got)

    def test_the_changed_column_wins_over_the_prior_one(self) -> None:
        pages = pages_of("토지이용계획(변경)\n구 분 기정 증감 변경 구성비\n합 계 1,000 감) 10.0 990.0 100.0\n산업시설용지 600 감 10.0 590.0 59.6\n공공시설용지 400 - 400.0 40.4")
        got, _, facts = land(pages)
        self.assertIn(("산업시설용지", "산업시설용지", None, 590.0), got)
        self.assertEqual({f.value["column"] for f in facts}, {2})

    def test_a_table_goes_on_over_the_page(self) -> None:
        pages = pages_of("토지이용계획\n계 1,000 100.0\n산업시설용지 600 60.0", "- 7 -\n지원시설용지 100 10.0\n공공시설용지 300 30.0")
        got, _, facts = land(pages)
        self.assertEqual(len(got), 4)
        self.assertEqual({f.page_number for f in facts}, {1, 2})

    def test_a_total_read_only_in_part_is_not_solved(self) -> None:
        pages = pages_of("토지이용계획\n계 1,000 감x 10.0 990.0 100.0\n산업시설용지 600 감x 10.0 590.0\n공공시설용지 400 - 400.0")
        got, unsolved, _ = land(pages)
        self.assertEqual(got, set())
        self.assertEqual(unsolved, [1])


class IndustryTest(unittest.TestCase):
    def facts(self, text: str):
        return [f.value["ksic_division"] for f in rule_facts(pages_of(text), KSIC).facts if f.fact_kind == "invited_industry"]

    def test_a_code_counts_only_with_its_own_name(self) -> None:
        text = "주요유치업종\n◦ 섬유제품 제조업(13), 전기장비 제조업(28), 가구 제조업(30)\nC29 기타 기계 및 장비 제조업\n24.1차금속 제조업"
        self.assertEqual(self.facts(text), ["13", "28", "29", "24"])

    def test_a_section_letter_must_be_the_divisions(self) -> None:
        self.assertEqual(self.facts("유치업종\n전기장비 제조업(H28)"), [])

    def test_a_list_that_ends_in_an_exclusion_is_not_invited(self) -> None:
        text = "유치업종\n○ 금속가공제품 제조업(C25)\n음료 제조업(11), 담배 제조업(12),\n가죽, 가방 및 신발 제조업(15)\n미분류\n폐기물 수집, 운반, 처리 및 원료 재생업(38)을 제외한 업종"
        self.assertEqual(self.facts(text), ["25"])

    def test_a_prior_list_replaced_by_a_changed_one_is_not_read(self) -> None:
        text = "5. 주요 유치업종(변경)\n○ 기정 : 가구 제조업(C32)\n○ 변경 : 전기장비 제조업(C28)"
        self.assertEqual(self.facts(text), ["28"])

    def test_a_page_without_an_industry_section_yields_none(self) -> None:
        self.assertEqual(self.facts("입주현황\n전기장비 제조업(28)"), [])


class LotAndPriceTest(unittest.TestCase):
    def test_lot_rows_in_sequence_quote_up_to_the_included_area(self) -> None:
        text = "토지조서\n편입면적\n1 가나동 16 공 2,000.0 70.0 시청\n2 가나동 산20-1 잡 1,700.0 1,700.0 홍** 다라시 마바로 1\n3 가나리 21 전 900 450"
        facts = parcel_lot_facts(pages_of(text)[0])
        self.assertEqual([f.value["lot"] for f in facts], ["16", "산20-1", "21"])
        self.assertEqual(facts[1].quote, "2 가나동 산20-1 잡 1,700.0 1,700.0")  # no owner, no address
        self.assertTrue(all(f.check == "row_sequence" for f in facts))

    def test_a_gap_in_the_row_numbers_drops_the_page(self) -> None:
        self.assertEqual(parcel_lot_facts(pages_of("토지조서\n1 가나동 16 공 2,000 70\n3 가나동 18 전 500 500")[0]), [])

    def test_an_included_area_above_the_parcel_area_is_not_a_row(self) -> None:
        self.assertEqual(parcel_lot_facts(pages_of("토지조서\n1 가나동 16 공 70 2,000\n2 가나동 17 전 500 500")[0]), [])

    def test_a_sale_price_needs_its_label_and_unit(self) -> None:
        facts = sale_price_facts(pages_of("○ 분양가격 : 산업시설용지 352,000원/㎡ (부가세 별도)\n○ 면적 352,000㎡")[0])
        self.assertEqual([(f.value["won_per_unit"], f.value["per"]) for f in facts], [(352000.0, "㎡")])


class ModelCheckTest(unittest.TestCase):
    def setUp(self) -> None:
        self.pages = pages_of("토지이용계획\n합계902,628.0 100.0\n산업시설용지470,009.0 52.1\n공공시설용지432,619.0 47.9\n도로400,000.0\n녹지32,619.0", method="ocr")
        self.by_number = {p.page_number: p for p in self.pages}

    def row(self, line, name, area, level, parent=None, ratio=None):
        return {"page": 1, "line": line, "name": name, "area": area, "ratio": ratio, "level": level, "parent": parent}

    def test_rows_that_cite_and_close_are_facts_even_on_an_ocr_page(self) -> None:
        rows = [
            self.row("합계902,628.0 100.0", "합계", "902,628.0", "total"),
            self.row("산업시설용지470,009.0 52.1", "산업시설용지", "470,009.0", "category", ratio="52.1"),
            self.row("공공시설용지432,619.0 47.9", "공공시설용지", "432,619.0", "category", ratio="47.9"),
            self.row("도로400,000.0", "도로", "400,000.0", "sub", "공공시설용지"),
            self.row("녹지32,619.0", "녹지", "32,619.0", "sub", "공공시설용지"),
        ]
        facts = checked_land_use(rows, self.by_number, "m")
        self.assertEqual(len(facts), 5)
        self.assertTrue(all(f.method == "local_llm" and f.model == "m" and f.check == "table_total" for f in facts))
        for fact in facts:
            self.assertTrue(citation_holds(self.pages, fact.page_number, fact.char_start, fact.char_end, fact.quote))

    def test_a_value_the_page_does_not_say_rejects_the_table(self) -> None:
        rows = [
            self.row("합계902,628.0 100.0", "합계", "902,628.0", "total"),
            self.row("산업시설용지470,009.0 52.1", "산업시설용지", "470,009.0", "category"),
            self.row("공공시설용지432,619.0 47.9", "공공시설용지", "432,619.0", "category"),
            self.row("주거시설용지 1,000.0", "주거시설용지", "1,000.0", "category"),  # not on the page: dropped, sum still closes
        ]
        self.assertEqual(len(checked_land_use(rows, self.by_number, "m")), 3)
        rows[2] = self.row("공공시설용지432,619.0 47.9", "공공시설용지", "432,610.0", "category")  # area not in its line
        self.assertEqual(checked_land_use(rows, self.by_number, "m"), [])


class FactRunTest(unittest.TestCase):
    def write(self, root: Path, pages: list[tuple[str, str]]) -> None:
        version = load_contract()["document_fact"]["text_extractor_version"]
        assembled = assemble(pages)
        (root / "document_text_outcome.jsonl").write_text(json.dumps({"sha256": SHA, "extractor_version": version, "media_format": "pdf"}) + "\n", encoding="utf-8")
        (root / "document_text_page.jsonl").write_text(
            "".join(json.dumps({"sha256": SHA, "extractor_version": version, "page_number": p.page_number, "char_start": p.char_start, "char_end": p.char_end, "method": p.method, "text": p.text}, ensure_ascii=False) + "\n" for p in assembled),
            encoding="utf-8",
        )

    def run_once(self, pages, env=None):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.write(root, pages)
            with unittest.mock.patch.dict("os.environ", env or {}, clear=True):
                fact_run.main(["--pages", str(root), "--out", str(root / "out")])
            facts = [json.loads(l) for l in (root / "out" / "document_fact.jsonl").read_text(encoding="utf-8").splitlines()]
            outcomes = [json.loads(l) for l in (root / "out" / "document_fact_outcome.jsonl").read_text(encoding="utf-8").splitlines()]
            return facts, outcomes, json.loads((root / "out" / "summary.json").read_text(encoding="utf-8"))

    def test_rule_facts_are_written_with_the_contract_columns(self) -> None:
        facts, outcomes, _ = self.run_once([("text_layer", "토지이용계획\n계 1,000 100.0\n산업시설용지 600 60.0\n공공시설용지 400 40.0")])
        self.assertEqual(len(facts), 3)
        self.assertEqual(list(facts[0]), load_contract()["outputs"]["document_fact"]["columns"])
        self.assertEqual(len(outcomes), 1)

    def test_a_document_that_needs_the_absent_model_is_not_written(self) -> None:
        facts, outcomes, summary = self.run_once([("text_layer", "토지이용계획\n계 1,000 100.0\n산업시설용지 600 60.0\n공공시설용지 300 30.0")])
        self.assertEqual((facts, outcomes), ([], []))
        self.assertEqual(summary["counts"]["deferred"], 1)

    def test_an_ocr_value_without_a_total_waits_for_the_vision_check(self) -> None:
        facts, outcomes, _ = self.run_once([("ocr", "주요유치업종\n전기장비 제조업(28)")])
        self.assertEqual((facts, outcomes), ([], []))


if __name__ == "__main__":
    unittest.main()
