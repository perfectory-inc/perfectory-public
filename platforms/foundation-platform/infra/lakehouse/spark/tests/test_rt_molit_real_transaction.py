"""실거래가 Silver 적재(root ADR-0348, ADR-0349)의 판독부를 합성 자료로 본다.

지역은 합성 99 시도를 쓰고 거래는 지어낸 것이다. 각 관문은 거부해야 할 것을 심어서 확인한다:
같은 두 거래 중 하나만 해제된 달, 고쳐진 금액, 가린 지번, 산 지번, 이름이 두 코드를 가리키는 동, 머리줄이 없는 내보내기,
칸 수가 틀린 줄, 달 밖의 계약, 이 적재가 맡지 않은 원천, 다른 형식의 계획, 시각이 없는 계획.
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path

SPARK_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK_DIR / "jobs"))

import rt_molit_real_transaction as job  # noqa: E402
from platform_contracts import load_lakehouse_contract  # noqa: E402

NOW = datetime(2099, 1, 2, 3, 4, 5)
DATASET = "real_transaction_industrial_trade"
CSV = job.load_source_contract()["csv"]
HEADER = ["NO", "시군구", "유형", "지번", "도로명", "용도지역", "건축물주용도", "도로조건", "전용/연면적(㎡)", "대지면적(㎡)",
          "거래금액(만원)", "층", "매수", "매도", "계약년월", "계약일", "지분구분", "건축년도", "해제사유발생일", "거래유형",
          "중개사소재지"]


def factory(no=1, jibun="12-3", amount="146,500", cancel="-", month="209812", day_="15"):
    return [str(no), "합성도 합성시 합성면 가상리", "집합", jibun, "", "계획관리", "공장", "8m미만", "684.00", "2050.00",
            amount, "", "법인", "개인", month, day_, "", "2007", cancel, "중개거래", "-"]


def export(*rows, header=HEADER, month="209812") -> bytes:
    lines = ['"□ 본 서비스에서 제공하는 정보는 법적인 효력이 없으므로 참고용으로만 활용하시기 바랍니다."', '""',
             f'"계약일자 : {month[:4]}-{month[4:]}-01 ~ {month[:4]}-{month[4:]}-28"',
             ",".join(f'"{h}"' for h in header)]
    lines += [",".join(f'"{c}"' for c in row) for row in rows]
    return ("\r\n".join(lines) + "\r\n").encode("cp949")


def rows_of(data: bytes, month="209812"):
    rows, unknown, rejected = job.rows_of(data, DATASET, month, "k", "20990102T030405Z", NOW, CSV)
    assert not rejected, rejected
    return rows, unknown


def rows_and_rejected(data: bytes, month="209812"):
    return job.rows_of(data, DATASET, month, "k", "20990102T030405Z", NOW, CSV)


class ReadsExports(unittest.TestCase):
    def test_columns_are_typed_and_the_raw_fields_kept_without_the_row_number(self):
        rows, unknown = rows_of(export(factory()))
        self.assertEqual(unknown, [])
        row = rows[0]
        self.assertEqual(row["deal_amount_10k_krw"], 146500)
        self.assertEqual(row["deal_date"], date(2098, 12, 15))
        self.assertEqual((row["building_area_m2"], row["building_area_kind"]), (684.0, "exclusive_or_total_floor"))
        self.assertEqual((row["land_area_m2"], row["land_area_kind"]), (2050.0, "plottage"))
        self.assertEqual(row["build_year"], 2007)
        self.assertIsNone(row["estate_agent_sgg_nm"])  # the provider's `-` is no value
        self.assertFalse(row["cancelled"])
        self.assertNotIn("NO", json.loads(row["provider_fields_json"]))
        self.assertEqual(row["source_line_number"], 5)

    def test_every_contract_column_is_known_to_the_row(self):
        contract = load_lakehouse_contract(job.TABLE)
        rows, _ = rows_of(export(factory()))
        produced = (set(rows[0]) | {column for column, _ in job.COLUMNS.values()}
                    | {name for column, kind, _ in job.AREAS.values() for name in (column, kind)}
                    | {"legal_dong_code", "pnu", "place_identity_grade", "pnu_unresolved_reason", "row_state"})
        columns = {c["name"] for c in contract["columns"]}
        self.assertEqual(columns - produced, set())
        self.assertEqual(produced - columns, set())

    def test_the_row_number_and_the_cancellation_do_not_change_a_name(self):
        """ADR-0349 §2: NO is per export; 해제 is filled in later; two equal deals keep their names."""

        before, _ = rows_of(export(factory(no=1), factory(no=2)))
        after, _ = rows_of(export(factory(no=1), factory(no=2, cancel="20990101")))
        self.assertEqual(sorted(r["row_key"] for r in before), sorted(r["row_key"] for r in after))
        cancelled = [r for r in after if r["cancelled"]]
        self.assertEqual(len(cancelled), 1)
        self.assertEqual(cancelled[0]["cancel_date"], date(2099, 1, 1))
        self.assertTrue(cancelled[0]["row_key"].endswith("-2"))  # `-` sorts before a date

    def test_a_corrected_amount_is_a_new_name(self):
        a, _ = rows_of(export(factory()))
        b, _ = rows_of(export(factory(amount="150,000")))
        self.assertNotEqual(a[0]["row_key"], b[0]["row_key"])

    def test_land_contract_area_is_the_land(self):
        header = ["NO", "시군구", "번지", "지목", "용도지역", "도로조건", "계약년월", "계약일", "계약면적", "거래금액(만원)",
                  "지분구분", "해제사유발생일", "거래유형", "중개사소재지"]
        row = ["1", "합성도 합성시 합성면 가상리", "3**", "전", "계획관리", "-", "209812", "1", "330", "1,000", "", "-",
               "직거래", "-"]
        rows, unknown, _ = job.rows_of(export(row, header=header), "real_transaction_land_trade", "209812", "k",
                                       "s", NOW, CSV)
        self.assertEqual(unknown, [])
        self.assertEqual((rows[0]["land_area_m2"], rows[0]["land_area_kind"]), (330.0, "deal"))
        self.assertTrue(rows[0]["jibun_masked"])

    def test_an_export_that_is_not_the_months_csv_is_refused_whole(self):
        with self.assertRaises(ValueError):
            rows_and_rejected(b"<html><body>blocked</body></html>")
        with self.assertRaises(ValueError):
            rows_and_rejected(export(factory(), month="209811"))
        entry = {"path": None, "dataset": DATASET, "month": "209812", "object_key": "k", "source_snapshot_id": "s"}

        class Bytes:
            def read_bytes(self):
                return b"<html></html>"

        entry["path"] = Bytes()
        rows, _, rejected, refused = job.read_export(entry, CSV, NOW)
        self.assertEqual((rows, refused, rejected[0].line), ([], True, 0))

    def test_bad_lines_are_rejected_with_their_raw_text_and_the_rest_loads(self):
        """ADR-0364: the 2026-10-30 export carried another export's 14-column rows after its own; one
        line split by a form feed inside a field stays one record."""

        land = ["1", "합성도 합성시 가상리", "3**", "전", "계획관리", "-", "209812", "1", "330", "1,000", "", "-", "직거래", "-"]
        broken = factory(no=3)
        broken[1] = "합성도 합성시\x0c합성면 가상리"
        data = export(factory(no=1), factory(no=2), broken, factory(no=4, amount="99,000"), land,
                      factory(no=5, month="209811"))
        rows, _, rejected = rows_and_rejected(data)
        self.assertEqual(sorted(r["source_line_number"] for r in rows), [5, 6, 7, 8])
        reasons = [(r.line, r.reason) for r in rejected]
        self.assertEqual(reasons, [(9, "cells_14_of_21"), (10, "deal_month_outside_export")])
        self.assertIn("가상리", rejected[0].raw)
        self.assertEqual(job.rejected_share(len(rows), len(rejected)), 2 / 6)

    def test_a_second_exports_rows_after_the_first_are_rejected(self):
        rows, _, rejected = rows_and_rejected(export(factory(no=1), factory(no=2), factory(no=1, amount="7")))
        self.assertEqual(len(rows), 2)
        self.assertEqual([r.reason for r in rejected], ["row_number_1_where_3"])

    def test_an_unknown_column_is_counted(self):
        _, unknown = rows_of(export(factory() + ["x"], header=HEADER + ["새칸"]))
        self.assertEqual(unknown, ["새칸"])


class LinksPlaces(unittest.TestCase):
    SNAPSHOT = [
        ("9999900000", "합성도 합성시", "존재"),
        ("9999933021", "합성도 합성시 합성면 가상리", "존재"),
        ("9999933022", "합성도 합성시 합성면 겹친리", "존재"),
        ("9999833022", "합성도 합성시 합성면 겹친리", "존재"),
        ("9999933024", "합성도 합성시 합성면 옛리", "폐지"),
    ]

    def test_full_addresses_resolve_to_one_code_or_none(self):
        dongs = job.dong_index(self.SNAPSHOT)
        self.assertEqual(job.legal_dong_code({"address_raw": "합성도  합성시 합성면 가상리"}, dongs), ("9999933021", None))
        self.assertEqual(job.legal_dong_code({"address_raw": "합성도 합성시 합성면 옛리"}, dongs), ("9999933024", None))
        self.assertEqual(job.legal_dong_code({"address_raw": "합성도 합성시 합성면 겹친리"}, dongs),
                         (None, "legal_dong_not_found"))
        self.assertEqual(job.legal_dong_code({}, dongs), (None, "no_address"))

    def test_lots_become_candidates_unless_masked_or_absent(self):
        self.assertEqual(job.lot_of({"jibun_raw": "12-3"}), ("ok", (False, 12, 3)))
        self.assertEqual(job.lot_of({"jibun_raw": "산 7"}), ("ok", (True, 7, 0)))
        self.assertEqual(job.lot_of({"jibun_raw": "3**"}), ("jibun_masked", None))
        self.assertEqual(job.lot_of({}), ("no_jibun", None))
        self.assertEqual(job.lot_of({"jibun_raw": "가-1"}), ("unparsed", None))
        self.assertEqual(job.lot_of({"jibun_raw": "686", "bonbun": "0686", "bubun": "0000"}), ("ok", (False, 686, 0)))
        self.assertEqual(job.candidate_pnu("9999933021", (False, 12, 3)), "9999933021100120003")
        self.assertEqual(job.candidate_pnu("9999933021", (True, 7, 0)), "9999933021200070000")
        self.assertIsNone(job.candidate_pnu(None, (False, 1, 0)))

    def test_the_bound_counts_only_unmasked_lots(self):
        rows = [{"dataset_slug": DATASET, "pnu": "9999933021100120003", "legal_dong_code": "9999933021",
                 "pnu_unresolved_reason": None},
                {"dataset_slug": DATASET, "pnu": None, "legal_dong_code": "9999933021",
                 "pnu_unresolved_reason": "jibun_masked"},
                {"dataset_slug": DATASET, "pnu": None, "legal_dong_code": "9999933021",
                 "pnu_unresolved_reason": "parcel_absent"}]
        report = job.link_report(rows)[DATASET]
        self.assertEqual((report["rows"], report["pnu"], report["legal_dong_only"]), (3, 1, 2))
        self.assertEqual(job.unmasked_unresolved_share(rows), 0.5)


class ReadsPlans(unittest.TestCase):
    def plan(self, root: Path, slug: str, table="silver.real_transaction", schema=job.PLAN_SCHEMA,
             last_modified="2099-01-02T03:04:05+00:00") -> Path:
        plan_dir = root / "plan"
        (plan_dir / "objects").mkdir(parents=True)
        key = f"bronze/source={slug}/period=2098-12/scope=nationwide/export--sha256-{'a' * 64}.csv"
        (plan_dir / "objects" / "a.csv").write_bytes(export(factory()))
        (plan_dir / "plan.json").write_text(json.dumps({
            "schema_version": schema, "table": table,
            "objects": [{"object_key": key, "source_slug": slug, "local_path": "objects/a.csv",
                         "last_modified": last_modified}]}), encoding="utf-8")
        return plan_dir

    def test_a_plan_of_this_source_reads(self):
        with tempfile.TemporaryDirectory() as tmp:
            objects = job.planned_objects([self.plan(Path(tmp), f"rtmolitgokr__{DATASET}")], job.load_source_contract())
            self.assertEqual([(o["dataset"], o["month"], o["source_snapshot_id"]) for o in objects],
                             [(DATASET, "209812", "20990102T030405Z")])

    def test_another_source_table_format_or_a_plan_without_time_is_refused(self):
        contract = job.load_source_contract()
        for kwargs in ({"slug": "datagokr__real_transaction_industrial_trade"},
                       {"slug": f"rtmolitgokr__{DATASET}", "table": "silver.onbid_real_estate_item"},
                       {"slug": f"rtmolitgokr__{DATASET}", "schema": "something.else.v1"},
                       {"slug": f"rtmolitgokr__{DATASET}", "last_modified": None}):
            with tempfile.TemporaryDirectory() as tmp, self.assertRaises(ValueError):
                job.planned_objects([self.plan(Path(tmp), **kwargs)], contract)


if __name__ == "__main__":
    unittest.main()
