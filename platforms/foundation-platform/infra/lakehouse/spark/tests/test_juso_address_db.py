"""주소DB → silver.juso_road_address (root ADR-0495) 를 합성 zip 으로 본다.

주소·지번·도로명은 지어낸 것이다(시도 99, 연도 2099). 확인하는 것: 월전체는 그달 마지막 날, 일변동은 그날이 판이다;
안의 파일 가운데 주소·지번·도로명만 읽고 대표 지번만 남긴다; 계획은 월전체를 먼저, 일변동을 날 순서로 싣는다;
일변동은 바뀐 키만, 63 은 삭제 표지로, 말하지 않은 칸은 현재 행의 것으로 둔다(Spark).
"""

import json
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import juso_address_db as j  # noqa: E402

try:
    from pyspark.sql import SparkSession
    HAS_SPARK = hasattr(SparkSession, "builder")
except ImportError:
    HAS_SPARK = False

M1 = "9999910100100010000000001"
M2 = "9999910100100020000000001"
M3 = "9999910100100030000000001"


def write_zip(path: Path, members: dict[str, list[str]]) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        for name, rows in members.items():
            info = zipfile.ZipInfo(name)  # Python marks a non-ASCII name UTF-8; `_decoded` reads either
            archive.writestr(info, "\r\n".join(rows).encode("cp949") + b"\r\n")
    return path


def full_zip(path: Path) -> Path:
    return write_zip(path, {
        "주소_합성도.txt": [f"{M1}|999014100001|01|0|3|0|99999||||0", f"{M2}|999014100002|01|0|7|1|99999||||0"],
        "지번_합성도.txt": [f"{M1}|1|9999910100|합성도|가상시|가나동||0|1|0|1",
                            f"{M1}|2|9999910100|합성도|가상시|가나동||0|9|0|0",
                            f"{M2}|1|9999910100|합성도|가상시|가나동||1|2|0|1"],
        "개선_도로명코드_전체분.txt": ["999014100001|가나대로|Gana-daero|00|합성도|Hapseong|가상시|Gasang|||2||0||||",
                                     "999014100002|다라로|Dara-ro|00|합성도|Hapseong|가상시|Gasang|||2||0||||"],
        "부가정보_합성도.txt": [f"{M1}|9999951500|가나동|99999|||가나빌|가나빌|1"],
        "[가이드]주소DB 활용방법.pdf": ["not read"],
    })


def daily_zip(path: Path) -> Path:
    return write_zip(path, {
        "AlterD.JUSUMT.20991102.MATCHING_JUSO.TXT": [f"{M2}|999014100002|01|0|7|1|99999|63|||0",
                                                     f"{M3}|999014100003|01|0|5|0|99999|31|||0"],
        "AlterD.JUSUMT.20991102.MATCHING_JIBUN.TXT": [f"{M3}|1|9999910100|합성도|가상시|가나동||0|3|0|1|31",
                                                      f"{M1}|1|9999910100|합성도|가상시|가나동||0|4|0|1|34"],
        "AlterD.JUSUMT.20991102.MATCHING_ROAD.TXT": ["999014100003|마바길|Maba-gil|00|합성도|Hapseong|가상시|Gasang|||2||0||신규|20991101|"],
        "AlterD.JUSUMT.20991102.MATCHING_ADDINFO.TXT": [f"{M3}|9999951500|가나동|99999|||||0|31"],
    })


class ParseTest(unittest.TestCase):
    def test_a_full_holds_its_months_last_day_and_a_change_file_its_day(self) -> None:
        self.assertEqual(j.snapshot_of(j.FULL, "bronze/source=jusogokr__address_db_monthly_full/209902ALLMTCHG00.zip"),
                         "20990228")
        self.assertEqual(j.snapshot_of(j.DAILY, "x/20991101_dailymatchingdata.zip"), "20991101")
        with self.assertRaises(ValueError):
            j.snapshot_of(j.DAILY, "x/dailymatchingdata.zip")

    def test_only_the_address_jibun_and_road_members_are_staged_with_the_representative_jibun(self) -> None:
        contract = j.load_source_contract()
        with tempfile.TemporaryDirectory() as tmp:
            staged = j.stage(full_zip(Path(tmp) / "full.zip"), j.FULL, contract, Path(tmp) / "out")
            text = {kind: path.read_text(encoding="utf-8").splitlines() for kind, path in staged.items()}
        self.assertEqual(text["address"][0].split("\t"), [M1, "999014100001", "01", "0", "3", "0", ""])
        self.assertEqual([line.split("\t")[3] for line in text["jibun"]], ["1", "2"], "the 대표 지번 only")
        self.assertEqual(text["road_code"][1].split("\t"), ["999014100002", "다라로", "00", "합성도", "가상시"])

    def test_a_change_file_stages_its_reason(self) -> None:
        contract = j.load_source_contract()
        with tempfile.TemporaryDirectory() as tmp:
            staged = j.stage(daily_zip(Path(tmp) / "day.zip"), j.DAILY, contract, Path(tmp) / "out")
            address = staged["address"].read_text(encoding="utf-8").splitlines()
            jibun = staged["jibun"].read_text(encoding="utf-8").splitlines()
        self.assertEqual([line.split("\t")[-1] for line in address], ["63", "31"])
        self.assertEqual(jibun[1].split("\t"), [M1, "9999910100", "0", "4", "0", "1", "34"])

    def test_the_plan_loads_the_full_first_then_the_days_in_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            plan = Path(tmp)
            objects = [
                {"object_key": "bronze/source=jusogokr__address_db_daily_change/o/20991102_dailymatchingdata.zip",
                 "source_slug": "jusogokr__address_db_daily_change", "local_path": "objects/b.zip"},
                {"object_key": "bronze/source=jusogokr__address_db_daily_change/o/20991031_dailymatchingdata.zip",
                 "source_slug": "jusogokr__address_db_daily_change", "local_path": "objects/a.zip"},
                {"object_key": "bronze/source=jusogokr__address_db_monthly_full/o/209910ALLMTCHG00.zip",
                 "source_slug": "jusogokr__address_db_monthly_full", "local_path": "objects/f.zip"},
            ]
            (plan / "plan.json").write_text(json.dumps({"schema_version": "foundation-platform.silver_load_objects.v1",
                                                        "objects": objects}), encoding="utf-8")
            order = [(o["dataset_slug"], o["snapshot"]) for o in j.planned_objects([plan])]
        self.assertEqual(order, [(j.FULL, "20991031"), (j.DAILY, "20991031"), (j.DAILY, "20991102")])


@unittest.skipUnless(HAS_SPARK, "requires the pinned Spark runtime")
class RowsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]").appName("synthetic-juso-address-db")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_a_full_row_carries_its_road_names_and_its_representative_jibuns_pnu(self):
        contract = j.load_source_contract()
        with tempfile.TemporaryDirectory() as tmp:
            staged = j.stage(full_zip(Path(tmp) / "full.zip"), j.FULL, contract, Path(tmp) / "out")
            rows = {r.road_address_management_no: r for r in j.full_rows(self.spark, staged).collect()}
        self.assertEqual((rows[M1].road_name, rows[M1].sigungu_code, rows[M1].sigungu_name, rows[M1].building_main_no),
                         ("가나대로", "99901", "가상시", 3))
        self.assertEqual(rows[M1].pnu, "9999910100100010000")
        self.assertEqual(rows[M2].pnu, "9999910100200020000", "산 is 2")

    def test_a_change_file_moves_only_what_it_says_and_removes_what_it_abolishes(self):
        contract = j.load_source_contract()
        with tempfile.TemporaryDirectory() as tmp:
            full = j.stage(full_zip(Path(tmp) / "full.zip"), j.FULL, contract, Path(tmp) / "f")
            current = j.full_rows(self.spark, full).cache()
            day = j.stage(daily_zip(Path(tmp) / "day.zip"), j.DAILY, contract, Path(tmp) / "d")
            rows, removed = j.daily_rows(self.spark, day, current)
            changed = {r.road_address_management_no: r for r in rows.collect()}
            gone = [r.road_address_management_no for r in removed.collect()]
        self.assertEqual(gone, [M2])
        self.assertEqual(set(changed), {M1, M3})
        self.assertEqual((changed[M1].pnu, changed[M1].road_name, changed[M1].building_main_no),
                         ("9999910100100040000", "가나대로", 3), "a new 지번, the address as it was")
        self.assertEqual((changed[M3].road_name, changed[M3].pnu), ("마바길", "9999910100100030000"))


if __name__ == "__main__":
    unittest.main()
