"""실거래가 Silver 적재(root ADR-0348, ADR-0349)의 판독부를 합성 자료로 본다.

지역은 합성 99 시도(99999 시군구)를 쓰고 거래는 지어낸 것이다. 각 관문은 거부해야 할 것을 심어서 확인한다:
같은 두 거래 중 하나만 해제된 묶음, 가린 지번, 산 지번, 지번 없는 원천, 이름이 두 코드를 가리키는 동, 구가 있는 시,
200 이 아닌 줄, 묶음을 묻지 않는 줄, 이 적재가 맡지 않은 원천, 다른 형식의 계획.
"""

from __future__ import annotations

import gzip
import json
import sys
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path

SPARK_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK_DIR / "jobs"))

import data_go_kr_rtms as job  # noqa: E402
from platform_contracts import load_lakehouse_contract  # noqa: E402

NOW = datetime(2099, 1, 2, 3, 4, 5)
DATASET = "real_transaction_industrial_trade"


def line(items, lawd="99999", month="209812", page=1):
    body = {"response": {"header": {"resultCode": "000", "resultMsg": "OK"},
                         "body": {"items": {"item": items} if items else "", "totalCount": len(items)}}}
    return {"operation": "getRTMSDataSvcInduTrade",
            "request": {"LAWD_CD": lawd, "DEAL_YMD": month, "pageNo": str(page), "numOfRows": "1000"},
            "requested_at_utc": "2099-01-02T03:04:05+00:00", "http_status": 200,
            "body": json.dumps(body, ensure_ascii=False)}


def factory(jibun="12-3", amount="146,500", cancel=""):
    return {"buildYear": 2007, "buildingAr": 684, "buildingType": "집합", "buildingUse": "공장",
            "buyerGbn": "법인", "cdealDay": cancel, "cdealType": "O" if cancel else " ", "dealAmount": amount,
            "dealDay": 15, "dealMonth": 12, "dealYear": 2098, "dealingGbn": "중개거래", "estateAgentSggNm": "",
            "floor": "", "jibun": jibun, "landUse": "계획관리", "plottageAr": 2050, "sggCd": "99999",
            "sggNm": "합성시", "shareDealingType": "", "slerGbn": "개인", "umdNm": "합성면 가상리"}


class ReadsRows(unittest.TestCase):
    def test_columns_are_typed_and_the_raw_fields_kept(self):
        rows, buckets, unknown = job.rows_of([line([factory()])], DATASET, "k", "20990102T030405Z", NOW)
        self.assertEqual(buckets, [(DATASET, "99999", "209812")])
        self.assertEqual(unknown, [])
        row = rows[0]
        self.assertEqual(row["deal_amount_10k_krw"], 146500)
        self.assertEqual(row["deal_date"], date(2098, 12, 15))
        self.assertEqual((row["building_area_m2"], row["building_area_kind"]), (684.0, "building"))
        self.assertEqual((row["land_area_m2"], row["land_area_kind"]), (2050.0, "plottage"))
        self.assertFalse(row["cancelled"])
        self.assertEqual(json.loads(row["provider_fields_json"])["jibun"], "12-3")
        self.assertEqual(row["row_key"].rsplit("-", 1)[1], "1")

    def test_every_contract_column_is_known_to_the_row(self):
        contract = load_lakehouse_contract(job.TABLE)
        rows, _, _ = job.rows_of([line([factory()])], DATASET, "k", "20990102T030405Z", NOW)
        produced = (set(rows[0]) | {column for column, _ in job.FIELD_COLUMNS.values()}
                    | {name for column, kind, _ in job.AREA_FIELDS.values() for name in (column, kind)}
                    | {"legal_dong_code", "pnu", "place_identity_grade", "pnu_unresolved_reason", "row_state"})
        columns = {c["name"] for c in contract["columns"]}
        self.assertEqual(columns - produced, set())
        self.assertEqual(produced - columns, set())

    def test_two_equal_deals_keep_their_names_when_one_is_cancelled(self):
        """ADR-0349 §2: rank by the fields that change after the fact."""

        before, _, _ = job.rows_of([line([factory(), factory()])], DATASET, "a", "20990101T000000Z", NOW)
        after, _, _ = job.rows_of([line([factory(cancel="26.01.01"), factory()])], DATASET, "b",
                                  "20990102T000000Z", NOW)
        self.assertEqual(sorted(r["row_key"] for r in before), sorted(r["row_key"] for r in after))
        cancelled = [r for r in after if r["cancelled"]]
        self.assertEqual(len(cancelled), 1)
        self.assertEqual(cancelled[0]["cancel_date"], date(2026, 1, 1))
        # The cancelled one takes the second rank (`O` sorts after the blank), the other keeps the first.
        self.assertTrue(cancelled[0]["row_key"].endswith("-2"))

    def test_a_corrected_amount_is_a_new_name(self):
        a, _, _ = job.rows_of([line([factory()])], DATASET, "a", "s", NOW)
        b, _, _ = job.rows_of([line([factory(amount="150,000")])], DATASET, "b", "s", NOW)
        self.assertNotEqual(a[0]["row_key"], b[0]["row_key"])

    def test_an_empty_answer_still_names_its_bucket(self):
        rows, buckets, _ = job.rows_of([line([], lawd="99998")], DATASET, "k", "s", NOW)
        self.assertEqual((rows, buckets), ([], [(DATASET, "99998", "209812")]))

    def test_a_line_that_is_not_an_answer_or_asks_no_bucket_is_refused(self):
        bad = line([factory()])
        bad["http_status"] = 429
        with self.assertRaises(ValueError):
            job.rows_of([bad], DATASET, "k", "s", NOW)
        no_bucket = line([factory()])
        no_bucket["request"].pop("DEAL_YMD")
        with self.assertRaises(ValueError):
            job.rows_of([no_bucket], DATASET, "k", "s", NOW)

    def test_an_unknown_field_is_counted(self):
        item = factory()
        item["newField"] = "x"
        _, _, unknown = job.rows_of([line([item])], DATASET, "k", "s", NOW)
        self.assertEqual(unknown, ["newField"])


class LinksPlaces(unittest.TestCase):
    SNAPSHOT = [
        ("9999900000", "합성도 합성시", "존재"),
        ("9999933021", "합성도 합성시 합성면 가상리", "존재"),
        ("9999933022", "합성도 합성시 합성면 겹친리", "존재"),
        ("9999933023", "합성도 합성시 합성면 겹친리", "존재"),
        ("9999933024", "합성도 합성시 합성면 옛리", "폐지"),
        ("9999800000", "합성도 다른시", "폐지"),
    ]

    def test_names_resolve_to_one_code_or_none(self):
        dongs = job.dong_index(self.SNAPSHOT)
        self.assertEqual(job.legal_dong_code({"sgg_cd": "99999", "umd_nm": "합성면  가상리"}, dongs),
                         ("9999933021", None))
        self.assertEqual(job.legal_dong_code({"sgg_cd": "99999", "umd_nm": "합성면 옛리"}, dongs),
                         ("9999933024", None))
        # Two current codes share the name: no guess.
        self.assertEqual(job.legal_dong_code({"sgg_cd": "99999", "umd_nm": "합성면 겹친리"}, dongs),
                         (None, "legal_dong_not_found"))
        # 아파트 매매 carries the code itself.
        self.assertEqual(job.legal_dong_code({"sgg_cd": "99999", "umd_cd": "33021"}, dongs), ("9999933021", None))

    def test_lots_become_candidates_unless_masked_or_absent(self):
        self.assertEqual(job.jibun_parts({"jibun_raw": "12-3"}), ("ok", (False, 12, 3)))
        self.assertEqual(job.jibun_parts({"jibun_raw": "산 7"}), ("ok", (True, 7, 0)))
        self.assertEqual(job.jibun_parts({"jibun_raw": "3**"}), ("jibun_masked", None))
        self.assertEqual(job.jibun_parts({}), ("no_jibun", None))
        self.assertEqual(job.jibun_parts({"jibun_raw": "가-1"}), ("unparsed", None))
        self.assertEqual(job.jibun_parts({"bonbun": "0012", "bubun": "0000", "land_cd": "2"}), ("ok", (True, 12, 0)))
        self.assertEqual(job.candidate_pnu("9999933021", (False, 12, 3)), "9999933021100120003")
        self.assertEqual(job.candidate_pnu("9999933021", (True, 7, 0)), "9999933021200070000")
        self.assertIsNone(job.candidate_pnu("9999933021", (False, 0, 0)))
        self.assertIsNone(job.candidate_pnu(None, (False, 1, 0)))

    def test_the_link_report_and_the_bound_count_only_unmasked_lots(self):
        rows = [
            {"dataset_slug": DATASET, "pnu": "9999933021100120003", "legal_dong_code": "9999933021",
             "pnu_unresolved_reason": None},
            {"dataset_slug": DATASET, "pnu": None, "legal_dong_code": "9999933021",
             "pnu_unresolved_reason": "jibun_masked"},
            {"dataset_slug": DATASET, "pnu": None, "legal_dong_code": "9999933021",
             "pnu_unresolved_reason": "parcel_absent"},
        ]
        report = job.link_report(rows)[DATASET]
        self.assertEqual((report["rows"], report["pnu"], report["legal_dong_only"]), (3, 1, 2))
        self.assertEqual(report["unresolved"], {"jibun_masked": 1, "parcel_absent": 1})
        self.assertEqual(job.unmasked_unresolved_share(rows), 0.5)


class AsksTheRightPlaces(unittest.TestCase):
    def test_a_city_is_asked_through_its_districts(self):
        rows = [("9911000000", "합성도 합성시", "존재"), ("9911100000", "합성도 합성시 가구", "존재"),
                ("9911300000", "합성도 합성시 나구", "존재"), ("9913000000", "합성도 다른군", "존재"),
                ("9913500000", "합성도 이웃군", "존재"), ("9913033021", "합성도 다른군 가면", "존재"),
                ("9915000000", "합성도 없어진시", "폐지"), ("9900000000", "합성도", "존재")]
        # 99130 and 99135 share their first four digits but neither is the other's 시 (영동군·증평군).
        self.assertEqual(job.lawd_codes(rows), ["99111", "99113", "99130", "99135"])

    def test_an_empty_list_is_never_written(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                job.write_lawd_set(Path(tmp) / "lawd-set.json", [], 0, NOW)
            job.write_lawd_set(Path(tmp) / "lawd-set.json", ["99999"], 3, NOW)
            written = json.loads((Path(tmp) / "lawd-set.json").read_text(encoding="utf-8"))
            self.assertEqual((written["schema_version"], written["lawd_codes"]), (job.LAWD_SET_SCHEMA, ["99999"]))


class ReadsPlans(unittest.TestCase):
    def plan(self, root: Path, slug: str, table="silver.real_transaction", schema=job.PLAN_SCHEMA) -> Path:
        plan_dir = root / "plan"
        (plan_dir / "objects").mkdir(parents=True)
        key = f"bronze/source={slug}/x/real-transaction-industrial-trade-0001-20990102t030405z.gz"
        with gzip.open(plan_dir / "objects" / "a.gz", "wt", encoding="utf-8") as handle:
            handle.write(json.dumps(line([factory()])) + "\n")
        (plan_dir / "plan.json").write_text(json.dumps({
            "schema_version": schema, "table": table,
            "objects": [{"object_key": key, "source_slug": slug, "local_path": "objects/a.gz"}]}), encoding="utf-8")
        return plan_dir

    def test_a_plan_of_this_source_reads(self):
        with tempfile.TemporaryDirectory() as tmp:
            objects = job.planned_objects([self.plan(Path(tmp), f"datagokr__{DATASET}")], job.load_source_contract())
            self.assertEqual([(o["dataset"], o["source_snapshot_id"]) for o in objects],
                             [(DATASET, "20990102T030405Z")])

    def test_another_source_table_or_plan_format_is_refused(self):
        contract = job.load_source_contract()
        for kwargs in ({"slug": "datagokr__onbid_real_estate_item"},
                       {"slug": f"datagokr__{DATASET}", "table": "silver.onbid_real_estate_item"},
                       {"slug": f"datagokr__{DATASET}", "schema": "something.else.v1"}):
            with tempfile.TemporaryDirectory() as tmp, self.assertRaises(ValueError):
                job.planned_objects([self.plan(Path(tmp), **kwargs)], contract)

    def test_the_load_plans_every_dataset_of_the_contract(self):
        contract = job.load_source_contract()
        self.assertEqual(len(contract["datasets"]), 12)
        self.assertGreater(contract["silver_load"]["max_unmasked_unresolved_share"], 0)


if __name__ == "__main__":
    unittest.main()
