"""법원경매정보 Silver 적재(root ADR-0280, ADR-0281)를 합성 행으로 본다.

법원 코드·사건번호·지명은 지어낸 것이다(시도 99). 거부해야 할 것을 심는다: 키가 없는 행, 200 이 아닌 답의
행, 0 으로 적힌 매각가, 끝까지 읽지 못한 조건, 다른 형식의 계획, 이 적재가 맡지 않은 원천. Spark 가 있으면 비교 단계도 돈다: 완전한 물건 검색만 삭제
표지를 남기고, 매각결과 검색은 남기지 않는다.
"""

from __future__ import annotations

import gzip
import json
import sys
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import address_pnu  # noqa: E402
import courtauction_go_kr as job  # noqa: E402
from platform_contracts import load_lakehouse_contract  # noqa: E402

try:
    from pyspark.sql import SparkSession
    HAS_SPARK = hasattr(SparkSession, "builder")
except ImportError:
    HAS_SPARK = False

CONTRACT = job.load_source_contract()
NOW = datetime(2099, 1, 2, 3, 4, 5)
PREFIXES = {"합성": "99"}
DONGS = address_pnu.legal_dong_index([("9999910100", "합성도 가상시 가나동")])


def item(sa="20990130000001", **extra):
    base = {"boCd": "B999999", "saNo": sa, "maemulSer": "1", "mokmulSer": "1", "srnSaNo": "2099타경1",
            "gamevalAmt": "100000000", "minmaePrice": "70000000", "maeGiil": "20990115", "maeAmt": "0",
            "jinstatCd": "0002100001", "hjguSido": "합성도", "hjguSigu": "가상시", "hjguDong": "가나동", "hjguRd": "",
            "daepyoLotno": "7", "rdNm": "", "buldNo": "", "wgs84Xcordi": "127.1235", "wgs84Ycordi": "36.1235"}
    base.update(extra)
    return base


def line(items, status=200):
    body = {"status": 200, "data": {"ipcheck": True, "dma_pageInfo": {"totalCnt": str(len(items))}, "dlt_srchResult": items}}
    return {"operation": "searchControllerMain", "request": {}, "http_status": status, "body": json.dumps(body, ensure_ascii=False)}


class RowTest(unittest.TestCase):
    def test_a_row_keeps_values_and_reads_dates_and_an_unsold_amount(self) -> None:
        row = job.item_row(item())
        self.assertEqual((row["property_seq"], row["appraisal_amount_krw"], row["sale_date"]), (1, 100000000, date(2099, 1, 15)))
        self.assertIsNone(row["sale_amount_krw"], "the site writes 0 for unsold")
        self.assertEqual(job.item_row(item(maeAmt="81000000"))["sale_amount_krw"], 81000000)

    def test_the_address_offers_a_jibun_pnu_else_a_road_key(self) -> None:
        self.assertEqual(job.offer(job.item_row(item()), PREFIXES, DONGS)["jibun_pnu"], "9999910100100070000")
        road = job.offer(job.item_row(item(daepyoLotno="", rdNm="가나대로", buldNo="12")), PREFIXES, DONGS)["road"]
        self.assertEqual((road.road, road.main_no), ("가나대로", 12))

    def test_the_later_answer_wins_and_a_refused_page_gives_nothing(self) -> None:
        lines = [line([item(), item("20990130000002")]), line([item(minmaePrice="56000000")]), line([item("X")], status=429)]
        rows = job.rows_of(lines, "bronze/k.gz", "20990102T030405Z", NOW, PREFIXES, DONGS)
        self.assertEqual(len(rows), 2)
        first = next(r for r in rows if r["case_no"] == "20990130000001")
        self.assertEqual((first["minimum_bid_price_krw"], first["source_line_number"]), (56000000, 2))

    def test_a_row_without_its_key_is_refused(self) -> None:
        with self.assertRaises((ValueError, KeyError)):
            job.rows_of([line([{"boCd": "B999999"}])], "bronze/k.gz", "s", NOW, PREFIXES, DONGS)

    def test_the_province_prefixes_are_the_factory_contracts(self) -> None:
        factory = json.loads((job.CONTRACTS / "data-go-kr-registered-factory.contract.json").read_text(encoding="utf-8"))
        self.assertEqual(job.sido_prefixes(CONTRACT), factory["pnu_resolution"]["sido_prefixes"])

    def test_every_table_column_is_built(self) -> None:
        for table in (job.ITEM_TABLE, job.RESULT_TABLE):
            names, _ = job.offered_schema(load_lakehouse_contract(table))
            built = set(job.rows_of([line([item()])], "k", "s", NOW, PREFIXES, DONGS)[0]) | {"row_id"}
            self.assertLessEqual(set(names), built, table)


def asked(lines, condition, window=True):
    """Lines as the collector writes them: the condition, with the contract's date range when it has one."""

    spec = CONTRACT["item_search"]["date_window"]
    dates = {spec["start_field"]: "20980102", spec["end_field"]: "21000102"} if window else {}
    return [{**entry, "request": {"dma_srchGdsDtlSrchInfo": {**condition, **dates}}} for entry in lines]


class CompletenessTest(unittest.TestCase):
    def test_a_search_is_complete_only_when_every_condition_was_read_to_its_end(self) -> None:
        search = CONTRACT["item_search"]
        first, *rest = search["conditions"]
        ended = [entry for condition in rest for entry in asked([line([])], condition)]
        self.assertTrue(job.search_complete([*asked([line([item()])], first), *ended], search))
        self.assertFalse(job.search_complete(ended, search), "a condition never asked")
        short = line([item()])
        short["body"] = short["body"].replace('"totalCnt": "1"', '"totalCnt": "2"')
        self.assertFalse(job.search_complete([*asked([short], first), *ended], search), "stopped before totalCnt")
        refused = asked([line([item()], status=429)], first)
        self.assertFalse(job.search_complete([*refused, *ended], search), "refused")

    def test_a_pass_without_the_date_range_is_not_complete(self) -> None:
        search = CONTRACT["item_search"]
        lines = [entry for condition in search["conditions"] for entry in asked([line([])], condition, window=False)]
        self.assertFalse(job.search_complete(lines, search), "the server's sliding default range")

    def test_the_result_search_has_no_date_range(self) -> None:
        search = CONTRACT["sale_result_search"]
        lines = [{**entry, "request": {"dma_srchGdsDtlSrchInfo": condition}}
                 for condition in search["conditions"] for entry in [line([])]]
        self.assertTrue(job.search_complete(lines, search))


class PlanTest(unittest.TestCase):
    ITEM = "bronze/source=courtauctiongokr__auction_item/auction-item-20990102t030405z.gz"

    def write(self, root, key, lines, schema=job.PLAN_SCHEMA):
        slug = key.split("/")[1].removeprefix("source=")
        local = f"objects/{slug}/{key.rsplit('/', 1)[1]}"
        (root / local).parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(root / local, "wt", encoding="utf-8") as out:
            out.write("".join(json.dumps(entry, ensure_ascii=False) + "\n" for entry in lines))
        (root / "plan.json").write_text(json.dumps({"schema_version": schema, "objects": [
            {"object_key": key, "source_slug": slug, "local_path": local, "size_bytes": 1}]}), encoding="utf-8")
        return root

    def test_a_plan_gives_each_searchs_rows_and_completeness(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            [entry] = job.planned_objects([self.write(Path(tmp), self.ITEM, [line([item()])])], CONTRACT)
            self.assertEqual((entry["role"], entry["source_snapshot_id"]), ("item_search", "20990102T030405Z"))
            search = job.read_object(entry, CONTRACT, DONGS, NOW)
            self.assertFalse(search["complete"])
            self.assertEqual([r["source_record_id"] for r in search["rows"]], [self.ITEM])

    def test_a_plan_of_another_kind_or_source_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                job.planned_objects([self.write(Path(tmp) / "a", self.ITEM, [], schema="other.v1")], CONTRACT)
            other = "bronze/source=datagokr__onbid_real_estate_item/item-list-20990102t030405z.gz"
            with self.assertRaises(ValueError):
                job.planned_objects([self.write(Path(tmp) / "b", other, [])], CONTRACT)


@unittest.skipUnless(HAS_SPARK, "requires the pinned Spark runtime")
class ChangesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]").appName("synthetic-court-auction")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.parcels = cls.spark.createDataFrame([("9999910100100070000",)], "pnu string")
        cls.titles = cls.spark.createDataFrame([], "pnu string, road_address_raw string")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def changes(self, role, complete, current_rows):
        contract = load_lakehouse_contract(job.ROLE_TABLE[role])
        rows = job.rows_of([line([item(), item("20990130000002", daepyoLotno="8")])], "bronze/new.gz", "20990102T030405Z",
                           NOW, PREFIXES, DONGS)
        names = [c["name"] for c in contract["columns"]]
        schema = ", ".join(f"{c['name']} {_ddl(c['logical_type'])}" for c in contract["columns"])
        current = self.spark.createDataFrame([tuple(r.get(n) for n in names) for r in current_rows], schema)
        return job.search_changes(self.spark, role, {"rows": rows, "complete": complete}, contract, self.parcels,
                                  self.titles, current, "20990102T030405Z", NOW)

    def vanished(self):
        gone = job.item_row(item("20990130000009"))
        gone.update(pnu=None, pnu_basis=None, pnu_unresolved_reason="no_legal_dong", pnu_candidate_count=None,
                    legal_dong_code=None, source_record_id="bronze/old.gz", source_line_number=1,
                    source_snapshot_id="20990101T000000Z", ingested_at_utc=datetime(2099, 1, 1), row_state=None)
        return gone

    def test_a_complete_item_search_marks_what_left_it(self):
        staged, changes = self.changes("item_search", True, [self.vanished()])
        rows = {r.case_no: r for r in changes.collect()}
        self.assertEqual(rows["20990130000009"].row_state, "removed")
        self.assertEqual(rows["20990130000001"].pnu, "9999910100100070000")
        self.assertEqual(rows["20990130000002"].pnu_unresolved_reason, "parcel_absent")

    def test_an_incomplete_search_and_the_result_search_mark_nothing(self):
        for role, complete in (("item_search", False), ("sale_result_search", True)):
            current = [self.vanished()]
            if role == "sale_result_search":
                current[0].pop("row_state")
            _, changes = self.changes(role, complete, current)
            self.assertNotIn("20990130000009", {r.case_no for r in changes.collect()}, role)


def _ddl(logical: str) -> str:
    from platform_contracts import spark_sql_type

    return spark_sql_type(logical)


if __name__ == "__main__":
    unittest.main()
