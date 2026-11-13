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

    def test_a_row_assembles_its_source_pnu_from_its_code_and_lot(self) -> None:
        # Root ADR-0382 §1: the representative parcel's four code fields (2+3+3+2) and its lot.
        coded = dict(daepyoSidoCd="99", daepyoSiguCd="999", daepyoDongCd="101", daepyoRdCd="00")
        self.assertEqual(job.item_row(item(daepyoLotno="산 7-2", **coded))["source_code_pnu_raw"], "9999910100200070002")
        self.assertIsNone(job.item_row(item(daepyoLotno="7외 2필지", **coded))["source_code_pnu_raw"], "prose lot")
        self.assertIsNone(job.item_row(item(**{**coded, "daepyoDongCd": "10"}))["source_code_pnu_raw"], "short part")
        self.assertIsNone(job.item_row(item())["source_code_pnu_raw"], "no code fields")

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

    def test_no_contract_writes_a_province_code(self) -> None:
        # Root ADR-0403: the prefixes come from the 법정동 code table each load, not from a contract.
        self.assertNotIn("sido_prefixes", json.dumps(CONTRACT["pnu_resolution"], ensure_ascii=False).replace("sido_prefixes_from", ""))

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
            search = job.read_object(entry, CONTRACT, PREFIXES, DONGS, NOW)
            self.assertFalse(search["complete"])
            self.assertEqual([r["source_record_id"] for r in search["rows"]], [self.ITEM])

    def test_a_plan_of_another_kind_or_source_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                job.planned_objects([self.write(Path(tmp) / "a", self.ITEM, [], schema="other.v1")], CONTRACT)
            other = "bronze/source=datagokr__onbid_real_estate_item/item-list-20990102t030405z.gz"
            with self.assertRaises(ValueError):
                job.planned_objects([self.write(Path(tmp) / "b", other, [])], CONTRACT)


def detail_line(seq="1", outcome="read", photos=()):
    request = {"dma_srchGdsDtlSrch": {"cortOfcCd": "B999999", "csNo": "20990130000001", "dspslGdsSeq": seq,
                                      "pgmId": "PGJ15BM01", "srchInfo": {}}}
    body = {"data": {"dma_result": {"csPicLst": list(photos)}}}
    return {"operation": "selectAuctnCsSrchRslt", "request": request, "requested_at_utc": "2099-01-02T03:04:05+00:00",
            "http_status": 200, "list_fingerprint": "f" * 64, "read_outcome": outcome,
            "photo_count": len(photos), "body": json.dumps(body)}


def stored_photo(n, sha):
    return {"cortOfcCd": "B999999", "csNo": "20990130000001", "picFile": f"sha256:{sha}", "sha256": sha,
            "media_format": "jpg", "size_bytes": 1000 + n, "picTitlNm": f"synthetic{n}.jpg",
            "cortAuctnPicDvsCd": "000241", "cortAuctnPicSeq": str(n), "pageSeq": str(n)}


class DetailRowsTest(unittest.TestCase):
    """Root ADR-0384: a 물건상세 object gives one read row per line and the case's stored photos once."""

    def rows(self, lines):
        return job.detail_rows_of(lines, CONTRACT["item_detail"], "bronze/d.gz", "20990102T030405Z", NOW)

    def test_every_read_is_a_row_and_a_case_photo_is_one_row_however_many_properties_name_it(self):
        a, b = "a" * 64, "b" * 64
        photos = [stored_photo(1, a), stored_photo(2, b)]
        failed = {**detail_line("3", "failed"), "body": "<html>"}
        rows = self.rows([detail_line("1", photos=photos), detail_line("2", photos=photos), failed])
        self.assertEqual([(r["property_seq"], r["read_outcome"]) for r in rows["rows"]],
                         [(1, "read"), (2, "read"), (3, "failed")], "a failed read is recorded too (ADR-0352 §2)")
        self.assertEqual(rows["rows"][0]["requested_at_utc"], datetime(2099, 1, 2, 3, 4, 5))
        self.assertEqual(sorted(r["sha256"] for r in rows["photo_rows"]), [a, b], "photos are the case's, once each")
        first = next(r for r in rows["photo_rows"] if r["sha256"] == a)
        self.assertEqual((first["sequence"], first["photo_kind_code"], first["size_bytes"], first["source_line_number"]),
                         (1, "000241", 1001, 2), "the last line naming it wins")

    def test_a_photo_the_collector_could_not_store_is_not_pointed_at(self):
        broken = {"cortOfcCd": "B999999", "csNo": "20990130000001", "picFile": None, "cortAuctnPicSeq": "9"}
        self.assertEqual(self.rows([detail_line(photos=[broken])])["photo_rows"], [])

    def test_a_read_without_its_key_is_refused(self):
        line = detail_line()
        line["request"]["dma_srchGdsDtlSrch"]["csNo"] = ""
        with self.assertRaises(ValueError):
            self.rows([line])

    def test_the_detail_objects_are_a_role_of_this_load(self):
        self.assertEqual(job.search_roles(CONTRACT)["courtauctiongokr__auction_item_detail"], job.DETAIL_ROLE)


def synthetic_result(seq="1", schedule_result="002"):
    """A 물건상세 result of the shape the site answers (root ADR-0540), with made-up values and names."""

    case = {"cortOfcCd": "B999999", "csNo": "20990130000001"}
    return {
        "csBaseInfo": {**case, "cortOfcNm": "합성지방법원", "cortSptNm": "합성지방법원", "csNm": "부동산임의경매",
                       "userCsNo": "2099타경1", "csRcptYmd": "20990101", "csCmdcYmd": "20990105", "clmAmt": "150000000",
                       "jdbnCd": "1001", "cortAuctnJdbnNm": "경매1계", "jdbnTelno": "099-000-0000",
                       "jdgeAojAsstnNm": "홍길동", "ultmtDvsCd": "000", "csUltmtYmd": "", "lwstDvsCd": "1"},
        "dstrtDemnInfo": [{"orddcsDvsCd": "021", "dstrtDemnLstprdYmd": "20990301"},
                          {"orddcsDvsCd": "022", "dstrtDemnLstprdYmd": "20990315"}],
        "dspslGdsDxdyInfo": {**case, "dspslGdsSeq": seq, "auctnGdsUsgCd": "10", "aeeEvlAmt": "100000000",
                             "fstPbancLwsDspslPrc": "70000000", "flbdNcnt": "1", "dspslDxdyYmd": "20990415",
                             "fstDspslHm": "1000", "dspslPlcNm": "제1호 법정", "dspslDcsnDxdyYmd": "20990422",
                             "prchDposRate": "10", "gdsSpcfcWrtYmd": "20990401",
                             "ndstrcRghCtt": "토지: 갑구 제14번 홍길동지분전부이전청구권가등기",
                             "tprtyRnkHypthcStngDts": "2099.1.5. 근저당권(김가상 지분)",
                             "gdsSpcfcRmk": "일괄매각. 토지 지분매각", "sprfcExstcDts": "해당사항없음",
                             "dspslGdsRmk": "소유자 이합성 지분 전부", "dspslGdsSpcfcEcdocId": "SYNTHETIC"},
        "gdsDspslDxdyLst": [
            {"dxdyYmd": "20990315", "dxdyHm": "1000", "auctnDxdyKndCd": "01", "dxdyPlcNm": "제1호 법정",
             "auctnDxdyRsltCd": schedule_result, "tsLwsDspslPrc": "100000000", "dspslAmt": "0"},
            {"dxdyYmd": "20990415", "dxdyHm": "1000", "auctnDxdyKndCd": "01", "dxdyPlcNm": "제1호 법정",
             "auctnDxdyRsltCd": "", "tsLwsDspslPrc": "70000000", "dspslAmt": "0"},
        ],
        "gdsDspslObjctLst": [
            {**case, "dspslGdsSeq": seq, "dspslObjctSeq": "1", "rletDvsDts": "토지", "objctArDts": "330㎡",
             "ldcgDts": "공장용지", "aeeEvlAmt": "60000000", "rprsAdongSdCd": "99", "rprsAdongSggCd": "999",
             "rprsAdongEmdCd": "101", "rprsAdongRiCd": "00", "rprsLtnoAddr": "7", "pnuNoCtt": "9999910100100070000",
             "dspslStkCtt": "갑구 2번 홍길동 지분 2분의 1 전부", "dspslStkNmrtVal": "1", "dspslStkDnmnVal": "2",
             "stXcrd": "100", "stYcrd": "200"},
            {**case, "dspslGdsSeq": seq, "dspslObjctSeq": "2", "rletDvsDts": "일반건물",
             "pjbBuldList": "철골조 공장\n1층 200㎡", "aeeEvlAmt": "40000000", "dspslStkNmrtVal": "0"},
        ],
        "gdsNotSugtBldLsstAll": [[{**case, "dspslObjctSeq": "2", "etcUsgCtt": "창고", "bldStrcDts": "판넬조",
                                   "bldArDts": "20㎡", "evlAmt": "3000000", "sugtBsdsBldRmk": "제시외건물"}]],
        "aeeWevlMnpntLst": [{**case, "aeeWevlMnpntTbltDvsCd": "00082001", "aeeWevlMnpntItmCd": "00083001",
                             "aeeWevlMnpntDtlSeq": "1", "aeeWevlMnpntCtt": "본건은 합성도 가나동 소재"}],
        "csPicLst": [],
    }


def document_line(seq="1", **kwargs):
    line = detail_line(seq)
    line["body"] = json.dumps({"data": {"dma_result": synthetic_result(seq, **kwargs)}})
    return line


class DocumentRowsTest(unittest.TestCase):
    """Root ADR-0540: the 물건상세's documents, one Silver table each, with no person's name."""

    def documents(self, lines):
        rows = job.detail_rows_of(lines, CONTRACT["item_detail"], "bronze/d.gz", "20990102T030405Z", NOW)
        return rows["documents"]

    def test_each_part_of_the_document_is_a_table_and_a_case_is_one_row_however_many_properties_read_it(self):
        docs = self.documents([document_line("1"), document_line("2")])
        [case] = docs[job.CASE_TABLE]
        self.assertEqual((case["claim_amount_krw"], case["commencement_date"], case["dividend_demand_deadline"],
                          case["source_line_number"]), (150000000, date(2099, 1, 5), date(2099, 3, 15), 2),
                         "the latest 배당요구종기, the last line naming the case wins")
        self.assertNotIn("홍길동", json.dumps(case, default=str, ensure_ascii=False), "the 사법보좌관 stays in Bronze")
        self.assertEqual([r["property_seq"] for r in docs[job.SPEC_TABLE]], [1, 2])
        self.assertEqual([(r["property_seq"], r["schedule_seq"], r["result_code"], r["sale_amount_krw"])
                          for r in docs[job.SCHEDULE_TABLE]],
                         [(1, 1, "002", None), (1, 2, None, None), (2, 1, "002", None), (2, 2, None, None)])
        objects = {(r["property_seq"], r["object_seq"]): r for r in docs[job.OBJECT_TABLE]}
        land = objects[(1, 1)]
        self.assertEqual((land["legal_dong_code"], land["lot_raw"], land["land_category"], land["share_numerator"],
                          land["share_denominator"]), ("9999910100", "7", "공장용지", 1.0, 2.0))
        self.assertIsNone(objects[(1, 2)]["share_numerator"], "the site writes 0 for no share")
        self.assertNotIn("dspslStkCtt", json.dumps(docs[job.OBJECT_TABLE], default=str))
        self.assertNotIn("홍길동", json.dumps(docs[job.OBJECT_TABLE], default=str, ensure_ascii=False),
                         "the share text names its holders and is not read")
        self.assertEqual([(r["property_seq"], r["object_seq"], r["annex_seq"]) for r in docs[job.ANNEX_TABLE]],
                         [(1, 2, 1), (2, 2, 1)])
        [point] = docs[job.APPRAISAL_POINT_TABLE]
        self.assertEqual((point["point_seq"], point["item_code"], point["source_line_number"]), (1, "00083001", 2),
                         "a case's 감정요항표 is its last line's list")

    def test_names_in_the_free_text_are_masked(self):
        [spec] = self.documents([document_line("1")])[job.SPEC_TABLE]
        self.assertEqual(spec["assumed_rights"], "토지: 갑구 제14번 ○○○지분전부이전청구권가등기")
        self.assertEqual(spec["first_rank_setting"], "2099.1.5. 근저당권(○○○ 지분)")
        self.assertEqual(spec["item_remarks"], "소유자 ○○○ 지분 전부")
        self.assertEqual(spec["spec_remarks"], "일괄매각. 토지 지분매각", "a word that is not a name stays")

    def test_the_mask_keeps_words_that_are_not_names(self):
        for text in ("토지 지분매각", "공유 지분", "소유자 미상의 분묘", "임차인 없음", "대지권 지분 전부", "해당사항없음"):
            self.assertEqual(job.mask_names(text), text, text)
        self.assertEqual(job.mask_names("임차인 홍길동은 점유"), "임차인 ○○○은 점유")
        self.assertEqual(job.mask_names("채무자: 김가상, 소유자 이합성"), "채무자: ○○○, 소유자 ○○○")
        self.assertIsNone(job.mask_names(None))

    def test_a_failed_read_states_no_document(self):
        failed = {**detail_line("3", "failed"), "body": "<html>"}
        self.assertEqual({table: rows for table, rows in self.documents([failed]).items() if rows}, {})

    def test_every_document_table_is_a_contract_table_of_this_load(self):
        for table in job.DOCUMENT_TABLES:
            names = {c["name"] for c in load_lakehouse_contract(table)["columns"]}
            rows = self.documents([document_line("1")])[table]
            self.assertTrue(rows, table)
            self.assertLessEqual(set(rows[0]), names, f"{table} rows name only its columns")
        self.assertEqual(job.DETAIL_TABLES[0], job.PHOTO_TABLE)
        self.assertEqual(job.DETAIL_TABLES[-1], job.DETAIL_TABLE, "the read record commits last")
        self.assertEqual(job.DETAIL_TABLES[-2], job.APPRAISAL_POINT_TABLE,
                         "the load script plans against the last document table too")


class DetailPartTest(unittest.TestCase):
    def test_a_detail_part_carries_its_runs_id(self):
        key = ("bronze/source=courtauctiongokr__auction_item_detail/operation=selectAuctnCsSrchRslt/"
               "auction-item-detail-part0002-20990102t030405z.gz")
        self.assertEqual(job.snapshot_id(key), "20990102T030405Z")


class DetailPlanTest(unittest.TestCase):
    def test_an_object_two_plans_name_is_loaded_once(self):
        key = "bronze/source=courtauctiongokr__auction_item_detail/operation=selectAuctnCsSrchRslt/auction-item-detail-20990102t030405z.gz"
        with tempfile.TemporaryDirectory() as tmp:
            dirs = []
            for name in ("details", "documents"):
                plan_dir = Path(tmp) / name
                (plan_dir / "objects").mkdir(parents=True)
                (plan_dir / "objects" / "o.gz").write_bytes(gzip.compress(b""))
                (plan_dir / "plan.json").write_text(json.dumps({"schema_version": job.PLAN_SCHEMA, "objects": [
                    {"object_key": key, "source_slug": "courtauctiongokr__auction_item_detail",
                     "local_path": "objects/o.gz"}]}), encoding="utf-8")
                dirs.append(plan_dir)
            self.assertEqual(len(job.planned_objects(dirs, CONTRACT)), 1)


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

    def test_the_code_path_wins_and_carries_a_superseded_code_to_the_current_parcel(self):
        # Root ADR-0382 §1: the row's code 9999920100 was renumbered to 9999910100; its address names
        # resolve nowhere, the code path finds the current parcel.
        contract = load_lakehouse_contract(job.ITEM_TABLE)
        coded = item("20990130000003", hjguDong="없는동", daepyoSidoCd="99", daepyoSiguCd="999",
                     daepyoDongCd="201", daepyoRdCd="00")
        rows = job.rows_of([line([coded])], "bronze/new.gz", "20990102T030405Z", NOW, PREFIXES, DONGS)
        names = [c["name"] for c in contract["columns"]]
        schema = ", ".join(f"{c['name']} {_ddl(c['logical_type'])}" for c in contract["columns"])
        current = self.spark.createDataFrame([], schema).select(*names)
        _, changes = job.search_changes(self.spark, "item_search", {"rows": rows, "complete": False}, contract,
                                        self.parcels, self.titles, current, "20990102T030405Z", NOW,
                                        ({"9999920100": "9999910100"}, set()))
        [row] = changes.collect()
        self.assertEqual((row.pnu, row.pnu_basis, row.pnu_unresolved_reason, row.legal_dong_code),
                         ("9999910100100070000", "legal_dong_code_superseded", None, "9999910100"))
        self.assertEqual(row.source_code_pnu_raw, "9999920100100070000", "the source code stays as evidence")

    def test_a_list_is_appended_whole_only_when_it_changed(self):
        # Root ADR-0540 §3: an unchanged list appends nothing; a changed or new one, all its rows.
        schema = "court_office_code string, case_no string, property_seq int, schedule_seq int, result_code string"
        current = self.spark.createDataFrame([("B", "1", 1, 1, "002"), ("B", "1", 1, 2, None),
                                              ("B", "1", 2, 1, "002")], schema)
        staged = self.spark.createDataFrame([("B", "1", 1, 2, None), ("B", "1", 1, 1, "002"),
                                             ("B", "1", 2, 1, "001"), ("B", "1", 3, 1, None)], schema)
        changed = job.changed_editions(staged, current, job.PROPERTY, ["schedule_seq", "result_code"])
        self.assertEqual(sorted((r.property_seq, r.schedule_seq) for r in changed.collect()), [(2, 1), (3, 1)])

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
