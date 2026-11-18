"""실거래가 Silver 적재(root ADR-0348, ADR-0349)의 판독부를 합성 자료로 본다.

지역은 합성 99 시도를 쓰고 거래는 지어낸 것이다. 각 관문은 거부해야 할 것을 심어서 확인한다:
같은 두 거래 중 하나만 해제된 달, 고쳐진 금액, 가린 지번, 산 지번, 이름이 두 코드를 가리키는 동, 머리줄이 없는 내보내기,
칸 수가 틀린 줄, 달 밖의 계약, 이 적재가 맡지 않은 원천, 다른 형식의 계획, 시각이 없는 계획.
"""

from __future__ import annotations

import json
import os
import subprocess
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


class RunsOnTheHost(unittest.TestCase):
    def test_the_host_reads_the_tables_in_isolated_mode(self):
        # scripts/ops/rt-molit-silver-load.sh runs `python3 -I <job> tables`, and -I leaves the job's
        # own directory off sys.path: a module-level `from lakehouse_ingest import ...` failed every
        # hourly run from 2026-11-05 08:13 to 11-07 (ModuleNotFoundError), and no export loaded.
        run = subprocess.run([sys.executable, "-I", str(SPARK_DIR / "jobs" / "rt_molit_real_transaction.py"), "tables"],
                             capture_output=True, text=True, timeout=60)
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertTrue(run.stdout.startswith("silver.real_transaction "), run.stdout)


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

    def test_an_export_over_the_bound_is_quarantined_whole_and_the_run_goes_on(self):
        """ADR-0364: a mixed export loads nothing (its clean copy will); a little noise loads the rest."""

        bound = float(job.load_source_contract()["silver_load"]["max_rejected_share"])
        one = [job.Rejected(9, "x", "cells_14_of_21")]
        self.assertIsNone(job.quarantine_reason(10_000, one, False, bound))
        self.assertTrue(job.quarantine_reason(100, one, False, bound).startswith("export_quarantined"))
        self.assertEqual(job.quarantine_reason(0, [], True, bound), "export_refused")

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
        self.assertEqual(job.lot_of({"jibun_raw": "가-1"}), ("provisional_lot", None))
        self.assertEqual(job.lot_of({"jibun_raw": "12 -3"}), ("ok", (False, 12, 3)))
        self.assertEqual(job.lot_of({"jibun_raw": "1-가"}), ("unparsed", None))
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

    def test_a_block_or_a_provisional_number_is_no_lot_the_bound_counts(self):
        # Root ADR-0469, 2026-11-09: 분양권 months failed every hourly load at 30.4% and 35.2% unresolved,
        # most of it `BL-A3`-like blocks and `가-` numbers of development zones, no 지번 at all.
        for raw in ("BL-A3", "BL-3", "3BL", "지구BL B-1", "A1블록", "가-", "가-12"):
            with self.subTest(raw=raw):
                reason, lot = job.lot_of({"jibun_raw": raw})
                self.assertIn(reason, job.NOT_A_LOT)
                self.assertIsNone(lot)
        lot = {"dataset_slug": DATASET, "pnu": "9999933021100120003", "legal_dong_code": "9999933021",
               "pnu_unresolved_reason": None}
        block = {**lot, "pnu": None, "pnu_unresolved_reason": "block_lot"}
        self.assertFalse(job.below_link_bound([lot] * 60 + [block] * 400, 0.25))

    def test_a_dataset_the_contract_measured_has_its_own_bound(self):
        silver_load = job.load_source_contract()["silver_load"]
        self.assertEqual(job.link_bound(silver_load, "real_transaction_apartment_presale"), 0.5)
        self.assertEqual(job.link_bound(silver_load, "real_transaction_apartment_trade"),
                         silver_load["max_unmasked_unresolved_share"])
        for dataset in silver_load["max_unmasked_unresolved_share_by_dataset"]:
            self.assertIn(dataset, job.load_source_contract()["datasets"])

    def test_the_bound_reads_a_share_of_enough_unmasked_lots_not_of_all_rows(self):
        masked = {"dataset_slug": DATASET, "pnu": None, "legal_dong_code": "9999933021",
                  "pnu_unresolved_reason": "jibun_masked"}
        absent = {**masked, "pnu_unresolved_reason": "legal_dong_not_found"}
        # 2026-11-08: a 토지 매매 month, every lot masked but two, both unresolved.
        self.assertFalse(job.below_link_bound([masked] * 1000 + [absent] * 2, 0.25))
        self.assertTrue(job.below_link_bound([masked] * 1000 + [absent] * job.MIN_UNMASKED_LOTS, 0.25))


class ParsesInTheWorker(unittest.TestCase):
    """2026-11-11: the driver held each export's rows, shipped them to the JVM and collected their links
    back, and the small Spark's 2.5GiB was outgrown three times in 13 hours. The parse now runs in the
    Python worker (export_records) and the driver reads only counts; what it yields is what the driver
    path made of the same export."""

    ENTRY = {"dataset": DATASET, "month": "209812", "object_key": "k", "source_snapshot_id": "20990102T030405Z"}
    DONGS = {"합성도 합성시 합성면 가상리": "9999933021"}
    COLUMNS = ("row_key", "jibun_raw", "legal_dong_code", "pnu", "source_line_number")

    def records(self, data, remap=None, ambiguous=frozenset()):
        return list(job.export_records(data, self.ENTRY, CSV, NOW, self.DONGS, remap or {}, ambiguous, self.COLUMNS))

    def test_the_worker_yields_the_rows_rejected_lines_and_meta_the_driver_made(self):
        data = export(factory(1), factory(2, jibun="3**"), ["x"], factory(3, jibun="가-1"))
        records = self.records(data, remap={"9999933021": "9999933099"})
        kinds = [r[0] for r in records]
        self.assertEqual(kinds[0], job.META)
        self.assertEqual(json.loads(records[0][4]), {"unknown": [], "refused": False})
        rows, _unknown, rejected = rows_and_rejected(data)
        self.assertEqual([r[1:4] for r in records if r[0] == job.REJECTED_LINE], [tuple(x) for x in rejected])
        yielded = [r[5] for r in records if r[0] == job.ROW]
        # The row key is Spark's (ranked): the worker yields the name it ranks by, rank 1 for a name alone.
        self.assertEqual([f"{y[6]}-1" for y in yielded], [r["row_key"] for r in rows])
        self.assertEqual([y[0] for y in yielded], [None] * 3)
        # The source code's candidate, the current code under the crosswalk, and the reason a row names no lot.
        self.assertEqual([y[2:4] + y[5:6] for y in yielded],
                         [("9999933099", "9999933021100120003", None), ("9999933099", None, "jibun_masked"),
                          ("9999933099", None, "provisional_lot")])

    def test_a_refused_export_is_one_meta_and_one_rejected_line(self):
        records = self.records(b"not the provider's csv")
        self.assertEqual(json.loads(records[0][4])["refused"], True)
        (line,) = [r for r in records if r[0] == job.REJECTED_LINE]
        self.assertEqual(line[1], 0)
        self.assertTrue(line[3].startswith("export_refused"))
        self.assertFalse([r for r in records if r[0] == job.ROW])

    def test_a_code_that_splits_names_no_current_code(self):
        (row,) = [r[5] for r in self.records(export(factory(1)), ambiguous=frozenset({"9999933021"})) if r[0] == job.ROW]
        self.assertIsNone(row[2])

    def test_the_forms_asked_are_the_ones_the_resolver_asks(self):
        # The Spark join of asked_forms, done here in Python, against place_identity_resolver.candidate_pnus.
        import place_identity_resolver

        remap = {"9999933021": "9999933099", "9999933022": "9999933099", "9999933030": "9999933031"}
        split = {"9999933040"}
        forms = job.asked_forms(remap, split)
        touched = {code for code, _ in forms}
        for pnu in ("9999933021100120003", "9999933099100120003", "9999933031100010000", "9999933050100010000",
                    "9999933040100010000"):
            code = pnu[:10]
            asked = set() if code in split else ({form + pnu[10:] for c, form in forms if c == code}
                                                 if code in touched else {pnu})
            with self.subTest(pnu=pnu):
                self.assertEqual(asked, place_identity_resolver.candidate_pnus([pnu], remap, split))


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class LoadsInSpark(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("rt-molit-worker-parse").getOrCreate()
        # As the load does: the worker imports the job to parse an export.
        cls.spark.sparkContext.addPyFile(job.__file__)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_an_export_parsed_from_its_file_in_the_worker_is_the_drivers_rows(self):
        from pyspark.sql import functions as F
        from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

        # Two rows of one name (a cancellation filled in on one of them): ranked as rows_of ranks them.
        data = export(factory(1), factory(2, cancel="20990110"), factory(3, jibun="3**"), factory(4, amount="1"))
        columns = ParsesInTheWorker.COLUMNS
        row_type = StructType([StructField(c, StringType()) for c in columns[:4]]
                              + [StructField("source_line_number", IntegerType()),
                                 StructField("_reason", StringType()), StructField("_name", StringType()),
                                 StructField("_sort", ArrayType(StringType()))])
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "export.csv"
            path.write_bytes(data)
            import functools
            import os

            records = functools.partial(job.file_records, {os.path.normpath(str(path)): ParsesInTheWorker.ENTRY},
                                        CSV, NOW, ParsesInTheWorker.DONGS, {}, frozenset(), columns)
            files = self.spark.sparkContext.binaryFiles(str(path))
            frame = self.spark.createDataFrame(files.flatMap(records), StructType([
                StructField("kind", StringType()), StructField("line", IntegerType()),
                StructField("raw", StringType()), StructField("reason", StringType()), StructField("meta", StringType()),
                StructField("row", row_type)]))
            got = job.ranked(frame.where(F.col("kind") == job.ROW).select("row.*"), F)
            keys = {(r["source_line_number"], r["row_key"]) for r in got.collect()}
        rows, _ = rows_of(data)
        self.assertEqual(keys, {(r["source_line_number"], r["row_key"]) for r in rows})
        self.assertIn(2, {int(key.rsplit("-", 1)[1]) for _, key in keys}, "the two rows of one name rank 1 and 2")

    def test_the_link_counts_are_the_link_report(self):
        from pyspark.sql import functions as F

        rows = [{"dataset_slug": DATASET, "pnu": "9999933021100120003", "legal_dong_code": "9999933021",
                 "pnu_unresolved_reason": None},
                {"dataset_slug": DATASET, "pnu": None, "legal_dong_code": "9999933021",
                 "pnu_unresolved_reason": "parcel_absent"},
                {"dataset_slug": DATASET, "pnu": None, "legal_dong_code": "9999933021",
                 "pnu_unresolved_reason": "jibun_masked"},
                {"dataset_slug": DATASET, "pnu": None, "legal_dong_code": None,
                 "pnu_unresolved_reason": "legal_dong_not_found"}]
        frame = self.spark.createDataFrame(rows, "dataset_slug string, pnu string, legal_dong_code string, "
                                                 "pnu_unresolved_reason string")
        report, lots, unresolved = job.link_counts(frame, F)
        self.assertEqual(report, job.link_report(rows))
        self.assertEqual((lots, unresolved), (len(job.unmasked_lots(rows)),
                                              sum(1 for r in job.unmasked_lots(rows) if not r["pnu"])))


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


class FitsItsUnit(unittest.TestCase):
    """ADR-0300 H: one run's objects fit the unit's time limit, from measured object times."""

    def test_the_per_run_bound_fits_the_unit(self):
        import re

        load = job.load_source_contract()["silver_load"]
        unit = (SPARK_DIR.parents[1] / "systemd" / "foundation-rt-molit-silver-load.service").read_text(encoding="utf-8")
        limit = int(re.search(r"^TimeoutStartSec=(\d+)m$", unit, re.M).group(1)) * 60
        measured = load["measured_object_seconds"]
        # The objects a run takes, at p90, after the start, fit the budget at which it stops starting more.
        self.assertLessEqual(load["run_start_seconds"] + load["max_objects"] * measured["p90"],
                             load["run_time_budget_seconds"])
        # The object in flight when the budget passes still finishes inside the unit's limit.
        self.assertLess(load["run_time_budget_seconds"] + measured["max"] + 120, limit)

    def test_a_run_stops_starting_objects_at_its_deadline(self):
        now = [0.0]
        from lakehouse_ingest import Deadline  # noqa: PLC0415

        deadline = Deadline(10, clock=lambda: now[0])
        self.assertFalse(deadline.passed())
        now[0] = 10
        self.assertTrue(deadline.passed())

    def test_the_parcel_read_asks_for_both_codes_of_a_lot(self):
        dongs = {"합성도 합성시 합성면 가상리": "9999933021"}
        rows = [{"address_raw": "합성도 합성시 합성면 가상리", "jibun_raw": "12-3"},
                {"address_raw": "합성도 합성시 합성면 가상리", "jibun_raw": "3**"}]
        self.assertEqual(job.candidate_pnus(rows, dongs, {"9999933021": "9999933099"}),
                         {"9999933021100120003", "9999933099100120003"})


if __name__ == "__main__":
    unittest.main()
