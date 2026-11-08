"""data.go.kr 기타 공공 일괄 파일 적재(root ADR-0401 §3–4)를 합성 행으로 확인한다.

이름·코드는 지어낸 것이다(시도 99, 합성시·가상동). Spark 가 없으면 Spark 시험은 건너뛴다(CI 의 Spark 런타임이 돌린다).
"""

import csv
import gzip
import hashlib
import io
import json
import sys
import tempfile
import unittest
import unittest.mock
import zipfile
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import data_go_kr_file_data as job  # noqa: E402

try:
    from pyspark.sql import SparkSession
    HAS_SPARK = hasattr(SparkSession, "builder")
except ImportError:
    HAS_SPARK = False

CONTRACT = job.load_source_contract()
FILES = job.files(CONTRACT)
PREFIXES = {"합성": "99"}
DONGS = {("99", "합성시 가상동"): "9999910100"}


def csv_bytes(rows, encoding="cp949"):
    out = io.StringIO()
    csv.writer(out, lineterminator="\r\n").writerows(rows)
    return out.getvalue().encode(encoding)


class Contract(unittest.TestCase):
    def test_every_file_but_the_registered_factory_is_loaded_into_its_own_table(self):
        self.assertEqual(len(FILES), 11)
        self.assertNotIn("registered_factory", FILES)
        artifact = json.loads((job.CONTRACT_PATH.parent / "industrial_complex_lakehouse_contracts.json")
                              .read_text(encoding="utf-8"))["contracts"]
        for slug, entry in FILES.items():
            table = artifact[job.table_of(slug)]
            names = {c["name"] for c in table["columns"]}
            self.assertLessEqual(set(job.LINK_COLUMNS[job.link_kind(entry)]), names, slug)
            self.assertLessEqual({"row_key", "provider_fields_json", "row_state"}, names, slug)
            self.assertEqual(table["current_row_latest"]["keep_ties"], entry["row_key"]["kind"] == "sha256_of_row", slug)
            self.assertIn("silver_load", entry, slug)
            for column in entry["row_key"].get("columns", []):
                self.assertIn(column, job.data_spec(entry)["header"], slug)
            for column in entry["place_link"].get("address_columns", []):
                self.assertIn(column, job.data_spec(entry)["header"], slug)
            if "period_column" in entry["row_key"]:
                self.assertIn(entry["row_key"]["period_column"], entry["row_key"]["columns"], slug)

    def test_tables_lists_each_table_with_its_source(self):
        with unittest.mock.patch("builtins.print") as printed:
            job.main(["tables"])
        self.assertIn(unittest.mock.call("silver.airport datagokr__airport"), printed.call_args_list)


class Reading(unittest.TestCase):
    def entry(self, header):
        return {**FILES["airport"], "header": header}

    def test_a_csv_is_staged_as_utf8_when_its_header_is_the_contracts(self):
        entry = self.entry(["가", "나"])
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "f.csv"
            source.write_bytes(csv_bytes([["가", "나"], ["합성", "값"]]))
            [copy] = job.stage(source, entry, Path(tmp) / "out")
            self.assertEqual(copy.read_text(encoding="utf-8").splitlines(), ["2,합성,값"])

    def test_a_wrong_header_or_an_unreadable_byte_refuses(self):
        entry = self.entry(["가", "나"])
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "f.csv"
            source.write_bytes(csv_bytes([["가", "다"], ["1", "2"]]))
            with self.assertRaises(ValueError):
                job.stage(source, entry, Path(tmp) / "a")
            source.write_bytes(csv_bytes([["가", "나"]]) + b"\xff\xff\n")
            with self.assertRaises(UnicodeDecodeError):
                job.stage(source, entry, Path(tmp) / "b")
            source.write_bytes(csv_bytes([["가", "나"], ["1", "2", "3"]]))
            with self.assertRaises(ValueError):
                job.stage(source, entry, Path(tmp) / "c")

    def test_every_matching_member_of_a_zip_is_staged_and_the_readme_is_not(self):
        entry = FILES["commercial_district"]
        header = entry["member"]["header"]
        raw = io.BytesIO()
        with zipfile.ZipFile(raw, "w") as archive:
            for region in ("합성", "가상"):
                archive.writestr(f"소상공인시장진흥공단_상가(상권)정보_{region}_209901.csv",
                                 "\ufeff" + ",".join(header) + "\n" + ",".join(["x"] * len(header)) + "\n")
            archive.writestr("[필독]안내.txt", "not data")
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "f.zip"
            source.write_bytes(raw.getvalue())
            copies = job.stage(source, entry, Path(tmp) / "out")
            self.assertEqual(len(copies), 2)
            self.assertEqual(copies[0].read_text(encoding="utf-8"), "2," + ",".join(["x"] * len(header)) + "\n")

    def pass_file(self, tmp, total, pages):
        """A synthetic pass as the collector lands it: the column-list answer, then each page of rows."""
        path = Path(tmp) / "pass.gz"
        answers = [("columns", {"totalCount": total, "tableVO": {"svcTableNm": "tn_pubr_synthetic_svc"}})]
        answers += [("rows", page) for page in pages]
        with gzip.open(path, "wt", encoding="utf-8") as out:
            for kind, body in answers:
                out.write(json.dumps({"operation": "synthetic", "kind": kind, "request": [["page", "1"]],
                                      "http_status": 200, "body": json.dumps(body, ensure_ascii=False)},
                                     ensure_ascii=False) + "\n")
        return path

    def test_a_standard_data_pass_is_staged_in_header_order_and_an_unknown_field_refuses(self):
        entry = {**FILES["school_location"], "header": ["SCHOOL_ID", "SCHOOL_NM", "LNMADR", "CDDC_CODE"]}
        with tempfile.TemporaryDirectory() as tmp:
            source = self.pass_file(tmp, 2, [[{"LNMADR": "합성도 합성시 가상동 1", "SCHOOL_ID": "S1", "CDDC_CODE": 9999}],
                                             [{"SCHOOL_ID": "S2", "SCHOOL_NM": "합성학교", "LNMADR": None}]])
            [copy] = job.stage(source, entry, Path(tmp) / "out")
            self.assertEqual(copy.read_text(encoding="utf-8").splitlines(),
                             ["2,S1,,합성도 합성시 가상동 1,9999", "3,S2,합성학교,,"])
            for total, pages in ((1, [[{"SCHOOL_ID": "S1", "NEW_FIELD": "x"}]]),  # a field the contract lacks
                                 (2, [[{"SCHOOL_ID": "S1"}]]),  # fewer rows than the column list reports
                                 (1, [{"SCHOOL_ID": "S1"}])):  # a page that is not a list of rows
                with self.assertRaises(ValueError):
                    job.stage(self.pass_file(tmp, total, pages), entry, Path(tmp) / "bad")

    def test_the_lot_column_is_tried_first_then_the_road_column(self):
        lot, road = "합성도 합성시 가상동 12-3", "합성도 합성시 가상로 45"
        offer = job.address_offer([lot, road], PREFIXES, DONGS)
        self.assertEqual((offer["jibun_pnu"], offer["legal_dong_code"]), ("9999910100100120003", "9999910100"))
        self.assertEqual((offer["road"].road, offer["road"].main_no), ("가상로", 45))
        offer = job.address_offer(["", road], PREFIXES, DONGS)
        self.assertEqual((offer["jibun_pnu"], offer["road"].road, offer["reason"]), (None, "가상로", None))
        self.assertEqual(job.address_offer(["", "합성도 합성시 가상로 지하 45"], PREFIXES, DONGS)["reason"], "unparsed")
        self.assertEqual(job.address_offer(["", ""], PREFIXES, DONGS)["reason"], "no_address")

    def test_the_row_key_is_the_provider_key_or_the_rows_sha256(self):
        header = ["노선번호", "출입시설", "준공년도"]
        cells = ["9", "합성IC", "2099"]
        self.assertEqual(job.row_key(cells, header, {"kind": "columns", "columns": ["노선번호", "출입시설"]}),
                         "9\x1f합성IC")
        self.assertEqual(job.row_key(cells, header, {"kind": "sha256_of_row"}),
                         hashlib.sha256("9\x1f합성IC\x1f2099".encode()).hexdigest())

    def test_bounds_refuse_a_short_or_shrunk_edition(self):
        entry = FILES["airport"]
        floor = entry["bounds"]["min_rows"]
        job.check_bounds(floor, None, entry)
        with self.assertRaises(ValueError):
            job.check_bounds(floor - 1, None, entry)
        with self.assertRaises(ValueError):
            job.check_bounds(floor, floor * 2, entry)

    def test_names_reach_a_legal_dong_or_say_why_not(self):
        self.assertEqual(job.names_code(["합성도", "합성시", "가상동"], PREFIXES, DONGS), ("9999910100", None))
        self.assertEqual(job.names_code(["합성도", "합성시", "없는동"], PREFIXES, DONGS), (None, "no_legal_dong"))
        self.assertEqual(job.names_code(["모름도", "합성시"], PREFIXES, DONGS), (None, "unparsed"))
        self.assertEqual(job.names_code(["", " "], PREFIXES, DONGS), (None, "no_address"))

    def test_a_plan_names_only_the_sources_this_job_loads(self):
        slugs = {job.source_slug(s): s for s in FILES}
        with tempfile.TemporaryDirectory() as tmp:
            plan = Path(tmp)
            item = {"object_key": "bronze/source=datagokr__port/port-file-1.csv", "source_slug": "datagokr__port",
                    "local_path": "objects/x.csv"}
            (plan / "plan.json").write_text(json.dumps({"schema_version": job.PLAN_SCHEMA, "objects": [item]}))
            [found] = job.planned_objects([plan], slugs)
            self.assertEqual((found["dataset_slug"], found["path"]), ("port", plan / "objects/x.csv"))
            item["source_slug"] = "datagokr__registered_factory"
            (plan / "plan.json").write_text(json.dumps({"schema_version": job.PLAN_SCHEMA, "objects": [item]}))
            with self.assertRaises(ValueError):
                job.planned_objects([plan], slugs)
        self.assertEqual(job.snapshot_id_of("bronze/source=datagokr__port/port-file-1.csv"), "port-file-1")

    def test_the_last_follow_record_is_the_newest_by_its_instant(self):
        first = job.follow_record(7, datetime(2099, 1, 1))
        later = job.follow_record(3, datetime(2099, 1, 2))
        self.assertEqual(job.last_followed([later, "bronze/source=datagokr__port/x.csv", first]), 3)
        self.assertIsNone(job.last_followed(["bronze/source=datagokr__port/x.csv"]))


@unittest.skipUnless(HAS_SPARK, "requires the pinned Spark runtime")
class SparkRows(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]").appName("synthetic-data-go-kr-file-data")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    class Inputs:
        prefixes, dongs = PREFIXES, DONGS

    def rows(self, slug, data):
        entry = FILES[slug]
        header = job.data_spec(entry)["header"]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "f.csv"
            path.write_text("\n".join([",".join(header), *(",".join(r) for r in data)]) + "\n", encoding="utf-8")
            staged = job.stage(path, {**entry, "format": "csv", "encoding": "utf-8"}, Path(tmp) / "out")
            return [r.asDict() for r in job.staged_rows(self.spark, staged, entry).collect()], entry

    def test_a_row_keeps_every_cell_under_its_header_and_is_keyed_like_the_python_twin(self):
        data = [["9", "합성선", "합성도", "합성시", "가상동", "합성IC", "2099"]]
        rows, entry = self.rows("highway_access_point", data)
        header = entry["header"]
        self.assertEqual(json.loads(rows[0]["provider_fields_json"]), dict(zip(header, data[0])))
        self.assertEqual(rows[0]["row_key"], job.row_key(data[0], header, entry["row_key"]))
        self.assertEqual(rows[0]["source_line_number"], 2)

    def test_names_and_codes_link_by_the_contracts_place_link(self):
        rows, entry = self.rows("highway_access_point", [["9", "합성선", "합성도", "합성시", "가상동", "합성IC", "2099"],
                                                         ["9", "합성선", "합성도", "합성시", "없는동", "가상IC", "2099"]])
        frame = self.spark.createDataFrame(rows)
        linked = {r["row_key"]: r for r in job.link(frame, entry, self.Inputs()).collect()}
        self.assertEqual({(r["legal_dong_code"], r["legal_dong_unresolved_reason"]) for r in linked.values()},
                         {(None, "no_legal_dong"), ("9999910100", None)})
        header = FILES["household_count"]["header"]
        rows, entry = self.rows("household_count", [["9999951000", "2099-01-31", *(["합성"] * 3), *(["1"] * (len(header) - 5))]])
        [area] = job.link(self.spark.createDataFrame(rows), entry, self.Inputs()).collect()
        self.assertEqual((area["administrative_dong_code"], area["legal_dong_code"]), ("9999951000", None))
        self.assertEqual(area["row_key"], "9999951000\x1f2099-01-31")


if __name__ == "__main__":
    unittest.main()
