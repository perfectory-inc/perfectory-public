"""Parcel editions in the source contract, and the 지번 step reading each change across its own
pair of them (root ADR-0144 §3.2, §4; ADR-0148), on synthetic data.

The codes are the synthetic 시도 97–98 and the editions are dated 2099, so nothing here is a real
place or a real release. Every refusal is proven by planting what it refuses: an object named by
two editions, an edition whose members are another's, two editions sharing a handoff prefix, a
change whose editions the contract does not hold, one the contract holds but the parcel table
does not, and the other spelling of a snapshot id.
"""

from __future__ import annotations

import copy
import json
import os
import re
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SPARK_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK_DIR / "jobs"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import code_go_kr_legal_dong as cg  # noqa: E402
import legal_dong_code_change_pairs as pairs_job  # noqa: E402
import vworld_parcel_editions as editions  # noqa: E402
from test_code_go_kr_legal_dong import CONTRACT, as_rows, pnu, row, table_html  # noqa: E402

MIN_SHARE = CONTRACT["pairing"]["jibun_overlap_min_share"]
SPRING, AUTUMN = "20990301", "20990901"


def edition_entry(name: str, extracted: str, latest: str | None = None) -> dict:
    """One synthetic edition: 시도 97 and 98, one 시군구 each, extracted on `extracted`."""

    objects = [
        {"object_key": f"bronze/source=vworldkr__parcel/{name}-{code}.zip", "bytes": 1,
         "dataset_name": f"LSMD_CONT_LDREG_{code}_{name}", "region_code": code,
         "granularity": "sido" if len(code) == 2 else "sigungu"}
        for code in ("97", "98", "97110", "98110")
    ]
    return {"provider_base_month": f"{name[:4]}-{name[4:]}",
            "extracted_on": {"earliest": extracted, "latest": latest or extracted},
            "handoff_prefix": f"silver-handoff/synthetic/edition={name}",
            "granularity_counts": {"sido": 2, "sigungu": 2}, "objects": objects}


def contract(*names: str, served: str | None = None) -> dict:
    dates = {"209902": "2099-02-10", "209906": "2099-06-10", "209910": "2099-10-10"}
    held = {name: edition_entry(name, dates[name]) for name in names}
    return {"schema_version": 2, "load_granularity": "sigungu", "snapshot_id_prefix": "vworldkr__parcel-",
            "served_edition": served or names[0], "handoff_suffix": ".jsonl.gz", "editions": held,
            "new_edition_bounds": {"max_sido_set_difference": 3, "max_sigungu_count_change_share": 0.05}}


def two_changes():
    """Two renames months apart: 갑동 → 새갑동 in 97 in spring, 병동 → 새병동 in 98 in autumn.
    The names differ, so the date + name rule settles neither; only 지번 can."""

    return as_rows(table_html([
        row("9700000000", "합성시", parent="0000000000", created="20000101"),
        row("9711000000", "합성시 가구", parent="9700000000", created="20000101"),
        row("9711010100", "합성시 가구 갑동", "폐지", "9711000000", abolished=SPRING),
        row("9711010300", "합성시 가구 새갑동", parent="9711000000", created=SPRING),
        row("9800000000", "합성도", parent="0000000000", created="20000101"),
        row("9811000000", "합성도 나구", parent="9800000000", created="20000101"),
        row("9811010100", "합성도 나구 병동", "폐지", "9811000000", abolished=AUTUMN),
        row("9811010300", "합성도 나구 새병동", parent="9811000000", created=AUTUMN),
    ]))


# Where each edition saw the parcels: 갑동's 지번 (1–20) move in spring, 병동's (101–120) in autumn.
PARCELS = {
    "209902": [pnu("9711010100", n) for n in range(1, 21)] + [pnu("9811010100", n) for n in range(101, 121)],
    "209906": [pnu("9711010300", n) for n in range(1, 21)] + [pnu("9811010100", n) for n in range(101, 121)],
    "209910": [pnu("9711010300", n) for n in range(1, 21)] + [pnu("9811010300", n) for n in range(101, 121)],
}


def table_holding(*loaded: str):
    """A parcel table holding `loaded` editions: PNUs under the asked codes, None for an edition it lacks."""

    def pnus_of(name: str, codes: set[str], lots: set[str] | None = None):
        if name not in loaded:
            return None
        return [value for value in PARCELS[name] if value[:10] in codes and (lots is None or value[10:] in lots)]

    return pnus_of


def pair_with(source: dict, *loaded: str):
    rows = two_changes()
    jibun = pairs_job.edition_evidence(rows, CONTRACT["pairing"]["floor_date"], source, table_holding(*loaded))
    return cg.pair_changes(rows, CONTRACT["pairing"]["floor_date"], jibun=jibun, min_share=MIN_SHARE), jibun


class EditionContractTest(unittest.TestCase):
    def test_the_real_contract_holds_its_editions(self):
        real = editions.load()
        self.assertIn(editions.served(real), editions.names(real))
        for name in editions.names(real):
            self.assertEqual(editions.edition_of_snapshot_id(real, editions.snapshot_id(real, name)), name)
            self.assertTrue(editions.load_objects(real, name))
        # The served edition decides the cadastral 시도, not the union of every edition held.
        served = editions.edition(real, editions.served(real))
        self.assertEqual(editions.cadastral_sido(real),
                         sorted({o["region_code"][:2] for o in served["objects"] if o["granularity"] == "sido"}))

    def test_an_edition_loads_under_one_id_and_one_validity(self):
        source = contract("209906")
        self.assertEqual(editions.snapshot_id(source, "209906"), "vworldkr__parcel-209906")
        self.assertEqual(editions.valid_from_utc("209906"), "2099-06-01T00:00:00Z")
        # 심은 위반: 같은 판의 다른 철자(2026-09 의 수작업 적재가 쓴 것)는 판이 아니다.
        with self.assertRaisesRegex(editions.EditionError, "not the snapshot id of an edition"):
            editions.edition_of_snapshot_id(source, "vworldkr__parcel:209906")
        with self.assertRaisesRegex(editions.EditionError, "no edition '209910'"):
            editions.snapshot_id(source, "209910")

    def test_a_contract_that_confuses_editions_is_refused(self):
        good = contract("209902", "209906")
        editions.validate(good)
        planted = {
            "named by editions": lambda c: c["editions"]["209906"]["objects"].append(
                copy.deepcopy(c["editions"]["209902"]["objects"][0])),
            "not edition 209906": lambda c: c["editions"]["209906"]["objects"][0].update(
                dataset_name="LSMD_CONT_LDREG_97_209902"),
            "share the handoff prefix": lambda c: c["editions"]["209906"].update(
                handoff_prefix=c["editions"]["209902"]["handoff_prefix"]),
            "counts 3 sigungu": lambda c: c["editions"]["209906"]["granularity_counts"].update(sigungu=3),
            "every province needs its districts": lambda c: (
                c["editions"]["209906"]["objects"].pop(), c["editions"]["209906"]["granularity_counts"].update(sigungu=1)),
            "served_edition": lambda c: c.update(served_edition="209910"),
            "schema_version": lambda c: c.update(schema_version=1),
            "is not YYYYMM": lambda c: c["editions"].update({"2099-6": c["editions"].pop("209906")}),
            "to an earlier": lambda c: c["editions"]["209906"]["extracted_on"].update(latest="2099-06-01"),
        }
        for message, plant in planted.items():
            bad = copy.deepcopy(good)
            plant(bad)
            with self.subTest(message), self.assertRaisesRegex(editions.EditionError, message):
                editions.validate(bad)

    def test_the_bracketing_editions_are_wholly_before_and_wholly_after(self):
        source = contract("209902", "209906", "209910")
        self.assertEqual(editions.bracketing(source, SPRING), ("209902", "209906"))
        self.assertEqual(editions.bracketing(source, "2099-09-01"), ("209906", "209910"))
        self.assertEqual(editions.bracketing(source, "20990110"), (None, "209902"))
        self.assertEqual(editions.bracketing(source, "20991201"), ("209910", None))
        # 심은 함정: 바뀐 날에 뽑은 판, 그날을 걸쳐 뽑은 판은 어느 쪽이 담겼는지 모른다. 어느 쪽도 아니다.
        self.assertEqual(editions.bracketing(source, "20990610"), ("209902", "209910"))
        source["editions"]["209906"]["extracted_on"] = {"earliest": "2099-05-30", "latest": "2099-06-02"}
        self.assertEqual(editions.bracketing(source, "20990601"), ("209902", "209910"))

    def test_the_command_line_answers_from_the_contract_and_refuses_an_unknown_edition(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "c.json"
            path.write_text(json.dumps(contract("209902", "209906")), encoding="utf-8")
            with mock.patch("sys.stdout") as out:
                self.assertEqual(editions.main(["handoff-keys", "--edition", "209906", "--contract", str(path)]), 0)
            printed = "".join(call.args[0] for call in out.write.call_args_list)
            self.assertEqual(printed.split(), [
                "silver-handoff/synthetic/edition=209906/209906-97110.jsonl.gz",
                "silver-handoff/synthetic/edition=209906/209906-98110.jsonl.gz"])
            with mock.patch("sys.stdout"), mock.patch("sys.stderr"):
                self.assertEqual(editions.main(["snapshot-id", "--edition", "209910", "--contract", str(path)]), 2)
                self.assertEqual(editions.main(["snapshot-id", "--contract", str(path)]), 2)


def inventory(*files: tuple[str, str, str]) -> dict:
    """An inventory report of the parcel dataset: (provider file name, base month, download kind)."""

    return {"jobs": [{"endpoint_slug": editions.PROVIDER_ENDPOINT, "files": [
        {"file_no": str(n), "provider_file_name": name, "base_ym": base, "updated_at": "2099-10-15",
         "download_kind": kind} for n, (name, base, kind) in enumerate(files, 1)]}]}


def measured(name: str, code: str, day: str = "2099-10-14", first: str | None = None) -> dict:
    return {"object_key": f"bronze/source=vworldkr__parcel/{name}-{code}-new.zip", "bytes": 7,
            "members": [f"LSMD_CONT_LDREG_{code}_{name}.{ext}" for ext in ("dbf", "prj", "shp", "shx")],
            "member_dates": sorted({first or day, day})}


class ANewProviderEditionTest(unittest.TestCase):
    """The daily check, collection and proposal of scripts/ops/vworld-parcel-edition-collect.sh."""

    LISTED = inventory(("a.zip", "2099-10", "single_resource_file"), ("b.zip", "2099-10", "single_resource_file"),
                       ("big-province.zip", "2099-10", "selection_archive"),
                       ("old-district.zip", "2099-06", "single_resource_file"), ("columns.hwp", "-", "single_resource_file"))

    def test_the_provider_edition_is_its_newest_base_month(self):
        # 제공자는 폐지된 시군구의 파일을 마지막 판 그대로 남겨 둔다(2026-10 에 셋). 판은 가장 새 기준월이다.
        self.assertEqual(editions.provider_edition(contract("209902", "209906"), self.LISTED), ("209910", False))
        self.assertEqual(editions.provider_edition(contract("209906", "209910"), self.LISTED), ("209910", True))

    def test_an_empty_or_older_listing_is_refused_not_read_as_nothing_new(self):
        with self.assertRaisesRegex(editions.EditionError, "lists no parcel files"):
            editions.provider_edition(contract("209906"), inventory())
        with self.assertRaisesRegex(editions.EditionError, "older than the contract's"):
            editions.provider_edition(contract("209906", "209910"), inventory(("a.zip", "2099-06", "single_resource_file")))
        with self.assertRaisesRegex(editions.EditionError, "no parcel file lists a base month"):
            editions.provider_edition(contract("209906"), inventory(("columns.hwp", "-", "single_resource_file")))

    def test_only_the_new_editions_direct_downloads_are_collected(self):
        cut = editions.select_inventory(self.LISTED, "209910")
        self.assertEqual([f["provider_file_name"] for f in cut["jobs"][0]["files"]], ["a.zip", "b.zip"])
        with self.assertRaisesRegex(editions.EditionError, "no file of edition 209912"):
            editions.select_inventory(self.LISTED, "209912")

    def test_a_proposal_is_the_contract_entry_or_nothing(self):
        source = contract("209902", "209906")
        rows = [measured("209910", "97"), measured("209910", "97110", "2099-10-13"), measured("209910", "98110")]
        entry = editions.propose(source, "209910", rows, "silver-handoff/synthetic/edition=209910")
        self.assertEqual(entry["granularity_counts"], {"sido": 1, "sigungu": 2})
        self.assertEqual(entry["extracted_on"], {"earliest": "2099-10-13", "latest": "2099-10-14"})
        self.assertEqual([o["region_code"] for o in entry["objects"]], ["97", "97110", "98110"])
        # 심은 위반: 다른 판의 파일, 시군구 없는 시도, 이미 다른 판이 가진 객체.
        planted = {
            "not one shapefile of edition 209910": rows + [measured("209906", "99110")],
            "every province needs its districts": [measured("209910", "97"), measured("209910", "98"), measured("209910", "97110")],
            "named by editions": rows + [{**measured("209910", "97120"),
                                          "object_key": source["editions"]["209906"]["objects"][0]["object_key"]}],
        }
        for message, bad in planted.items():
            with self.subTest(message), self.assertRaisesRegex(editions.EditionError, message):
                editions.propose(source, "209910", bad, "silver-handoff/synthetic/edition=209910")

    def test_a_half_updated_listing_is_refused_as_a_partial_edition(self):
        # 심은 결함: 제공자 목록이 반쯤 바뀐 날. 98 의 시군구는 아직 옛 판이라 새 판에는 97 만 있다.
        # 계약 검사만으로는 통과한다(시도 객체 없는 판은 형식상 옳다). 앞 판과 비교해 거부한다.
        source = contract("209902", "209906")
        half = [measured("209910", "97110")]
        merged = copy.deepcopy(source)
        merged["editions"]["209910"] = edition_entry("209910", "2099-10-14")
        merged["editions"]["209910"]["objects"] = [o for o in merged["editions"]["209910"]["objects"] if o["region_code"] == "97110"]
        merged["editions"]["209910"]["granularity_counts"] = {"sido": 0, "sigungu": 1}
        editions.validate(merged)  # the contract alone would take it
        with self.assertRaisesRegex(editions.EditionError, "partial collection"):
            editions.propose(source, "209910", half, "silver-handoff/synthetic/edition=209910")

    def test_the_bound_counts_districts_and_lets_a_merger_through(self):
        def districts(*codes):
            return [{"region_code": code, "granularity": "sigungu"} for code in codes]

        source = contract("209906")
        source["editions"]["209906"]["objects"] = districts(*(f"97{n:03d}" for n in range(110, 150)), "98110")
        # 시도 합병: 98 이 떠나고 99 가 나타났다. 시군구 수는 그대로다.
        editions.check_not_partial(source, "209910", districts(*(f"97{n:03d}" for n in range(110, 150)), "99110"))
        planted = {
            "a district count off by half": districts(*(f"97{n:03d}" for n in range(110, 130)), "98110"),
            "a province gone with nothing new": districts(*(f"97{n:03d}" for n in range(110, 151))),
            "too many provinces changed": districts(*(f"97{n:03d}" for n in range(110, 147)), "91110", "92110", "93110", "94110"),
        }
        for what, objects in planted.items():
            with self.subTest(what), self.assertRaisesRegex(editions.EditionError, "partial collection"):
                editions.check_not_partial(source, "209910", objects)
        with self.assertRaisesRegex(editions.EditionError, "new_edition_bounds"):
            editions.check_not_partial({k: v for k, v in source.items() if k != "new_edition_bounds"}, "209910", [])

    def test_a_file_whose_members_straddle_a_change_is_not_wholly_after_it(self):
        # 심은 함정: 한 파일의 멤버가 10-12 와 10-14 에 걸쳐 쓰였다. 10-13 의 변경 뒤에 다 뽑힌 판이 아니다.
        source = contract("209902", "209906")
        rows = [measured("209910", "97"), measured("209910", "97110", first="2099-10-12"), measured("209910", "98110")]
        entry = editions.propose(source, "209910", rows, "silver-handoff/synthetic/edition=209910")
        self.assertEqual(entry["extracted_on"], {"earliest": "2099-10-12", "latest": "2099-10-14"})
        source["editions"]["209910"] = entry
        self.assertEqual(editions.bracketing(source, "20991013"), ("209906", None))

    def test_a_re_upload_inside_a_held_edition_is_reported(self):
        source = contract("209906", "209910")  # 209910 extracted through 2099-10-10
        listed = inventory(("a.zip", "2099-10", "single_resource_file"), ("old.zip", "2099-06", "single_resource_file"))
        listed["jobs"][0]["files"][1]["updated_at"] = "2099-06-10"
        self.assertEqual(editions.held_reuploads(source, listed, "209910"), ["a.zip 2099-10-15"])
        listed["jobs"][0]["files"][0]["updated_at"] = "2099-10-10T09:00:00"
        self.assertEqual(editions.held_reuploads(source, listed, "209910"), [])

    def test_the_plan_reads_one_dataset_with_the_latest_editions_counts(self):
        catalog = {"schema_version": 1, "endpoints": [
            {"endpoint_slug": editions.PROVIDER_ENDPOINT, "provider_dataset_selector": {"svc_cde": "MK", "ds_id": "99999"}},
            {"endpoint_slug": "something-else"}]}
        self.assertEqual([e["endpoint_slug"] for e in editions.endpoint_catalog(catalog)["endpoints"]],
                         [editions.PROVIDER_ENDPOINT])
        header, row = editions.inventory_summary_csv(contract("209906"), catalog).splitlines()
        self.assertEqual(header, "module,svc_cde,ds_id,file_pages,file_count,large_file_count,listed_gib")
        self.assertEqual(row.split(",")[:6], ["vworld_dataset", "MK", "99999", "1", "4", "0"])
        with self.assertRaisesRegex(editions.EditionError, "lists 0"):
            editions.endpoint_catalog({"endpoints": []})


AREA = Path(__file__).resolve().parents[4]
COLLECT_SCRIPT = AREA / "scripts" / "ops" / "vworld-parcel-edition-collect.sh"


def measurement_keys(script: str) -> set[str]:
    """The variables the measurement container's AWS_* credentials are taken from."""

    block = script[script.index("GDAL_IMAGE="):script.index("vworld_parcel_edition_members.py <")]
    return set(re.findall(r'AWS_(?:ACCESS_KEY_ID|SECRET_ACCESS_KEY)="\$\{(FOUNDATION_PLATFORM_[A-Z0-9_]+)', block))


def awaiting_contract_notice(script: str) -> str:
    """What the exit-3 path tells Slack before it exits: the `message=` it hands `notify_slack`."""

    tail = script[script.index("# 4. 끝."):script.index('exit "${AWAITING_CONTRACT}"')]
    if 'notify_slack "${message}"' not in tail:
        return ""
    return re.search(r'^message="([^"\n]*)', tail, re.MULTILINE).group(1)


class TheEditionJobTest(unittest.TestCase):
    """The scheduled job's own promises: an edition awaiting the contract is reported as that and
    not retried, and the measurement only reads."""

    def test_an_edition_awaiting_the_contract_is_not_retried(self):
        jobs = json.loads((AREA / "orchestration" / "jobs.v1.json").read_text(encoding="utf-8"))["jobs"]
        job = next(job for job in jobs if job["id"] == "vworld_parcel_edition")
        # Airflow retries a failed run `retries` times, 1 when absent (orchestration/dags/job_specs.py).
        self.assertEqual(job.get("retries", 1), 0, "a retry downloads and proposes the same edition again")

    def test_exit_3_says_a_new_edition_waits_for_its_contract_entry(self):
        script = COLLECT_SCRIPT.read_text(encoding="utf-8")
        self.assertIn("new edition waiting for contract entry", awaiting_contract_notice(script))
        # 심은 회귀: 알림을 빼면 남는 것은 Airflow 의 일반 "실패" 뿐이다.
        self.assertEqual(awaiting_contract_notice(script.replace('notify_slack "${message}"', ":")), "")

    def test_the_measurement_container_gets_the_reader_key_only(self):
        script = COLLECT_SCRIPT.read_text(encoding="utf-8")
        self.assertEqual(measurement_keys(script), {"FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID",
                                                    "FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY"})
        # 심은 회귀: 쓰기 키를 넘기던 이전 모양은 이 검사가 잡는다.
        self.assertTrue(all("WRITER" in key for key in measurement_keys(script.replace("_READER_", "_WRITER_"))))


class EachChangeReadsItsOwnEditionsTest(unittest.TestCase):
    def test_two_changes_are_paired_across_their_own_pairs(self):
        result, _ = pair_with(contract("209902", "209906", "209910"), "209902", "209906", "209910")
        got = {p["old_code"]: (p["new_code"], p["source"]) for p in result.pairs}
        self.assertEqual(got["9711010100"],
                         ("9711010300", "derived:parcel-jibun:vworldkr__parcel-209902->vworldkr__parcel-209906"))
        self.assertEqual(got["9811010100"],
                         ("9811010300", "derived:parcel-jibun:vworldkr__parcel-209906->vworldkr__parcel-209910"))
        self.assertEqual(result.review, [])

    def test_reading_the_later_edition_for_held_jibun_only_changes_nothing(self):
        # 나중 판은 폐지된 동이 가졌던 지번만 읽는다(새로 생긴 코드 아래의 수백만 필지를 드라이버로 모으지 않으려고).
        # 그 거름이 결과를 바꾸면 안 된다: 거르지 않은 표와 같은 짝, 같은 목록이어야 한다.
        rows = two_changes() + as_rows(table_html([
            row("9811010400", "합성도 나구 딴동", parent="9811000000", created=AUTUMN)]))
        floor = CONTRACT["pairing"]["floor_date"]
        source = contract("209902", "209906", "209910")
        extra = {"209910": PARCELS["209910"] + [pnu("9811010400", n) for n in (101, 102, 900, 901)]}
        filtered_reads: list[set[str] | None] = []

        def table(name, codes, lots=None, honour=True):
            filtered_reads.append(lots)
            pnus = extra.get(name, PARCELS[name])
            return [p for p in pnus if p[:10] in codes and (not honour or lots is None or p[10:] in lots)]

        results = []
        for honour in (True, False):
            jibun = pairs_job.edition_evidence(rows, floor, source, lambda n, c, lots=None: table(n, c, lots, honour))
            results.append(cg.pair_changes(rows, floor, jibun=jibun, min_share=MIN_SHARE))
        self.assertEqual(results[0].pairs, results[1].pairs)
        self.assertEqual(results[0].review, results[1].review)
        self.assertTrue([lots for lots in filtered_reads if lots], "the later edition must have been read through the filter")

    def test_one_pair_for_both_changes_would_have_missed_one(self):
        # 지금까지의 방식: 한 쌍(봄 전 → 봄 뒤)으로 둘 다 판단하면 가을 변경은 볼 것이 없다.
        rows = two_changes()
        floor = CONTRACT["pairing"]["floor_date"]
        before, after = pairs_job.evidence_codes(rows, floor)
        one = cg.JibunEvidence(cg.jibun_sets(PARCELS["209902"], before), cg.jibun_sets(PARCELS["209906"], after), "one")
        result = cg.pair_changes(rows, floor, jibun=one, min_share=MIN_SHARE)
        self.assertNotIn("9811010100", {p["old_code"] for p in result.pairs})

    def test_a_change_whose_edition_the_contract_lacks_waits_and_names_it(self):
        result, jibun = pair_with(contract("209902", "209906"), "209902", "209906")
        self.assertIn("9711010100", {p["old_code"] for p in result.pairs})
        [item] = result.review
        self.assertEqual((item["old_code"], item["status"]), ("9811010100", "awaiting_data"))
        self.assertEqual(item["jibun"], "needs a parcel edition extracted after 20990901; the source contract holds none")
        self.assertIn("awaiting=1", pairs_job.evidence_label(jibun))
        with self.assertRaisesRegex(ValueError, "the data decides it"):
            pairs_job.steward_rows(result.review, ["9811010100=9811010300"], "steward-a", "추정", None, "s", pairs_job.datetime.now())

        result, _ = pair_with(contract("209906", "209910"), "209906", "209910")
        waiting = {i["old_code"]: i["jibun"] for i in result.review}
        self.assertEqual(waiting, {"9711010100": "needs a parcel edition extracted before 20990301; the source contract holds none"})

    def test_an_edition_in_the_contract_but_not_in_the_table_is_not_no_parcels(self):
        # 심은 누락: 계약은 가을 판을 알지만 표에는 아직 적재되지 않았다. 빈 집합으로 읽으면 '지번이 모두
        # 떠났다'가 되어 사람에게 넘어간다. 판단 대기로 남고, 필요한 판을 이름으로 말한다.
        result, _ = pair_with(contract("209902", "209906", "209910"), "209902", "209906")
        [item] = result.review
        self.assertEqual((item["old_code"], item["status"]), ("9811010100", "awaiting_data"))
        self.assertEqual(item["jibun"], "needs parcel edition 209910 (vworldkr__parcel-209910), which is not loaded into the parcel table")

    def test_a_sigungu_above_waiting_dongs_waits_too(self):
        rows = two_changes() + as_rows(table_html([
            row("9812000000", "합성도 옛구", "폐지", "9800000000", abolished=AUTUMN),
            row("9812010100", "합성도 옛구 정동", "폐지", "9812000000", abolished=AUTUMN),
        ]))
        floor = CONTRACT["pairing"]["floor_date"]
        jibun = pairs_job.edition_evidence(rows, floor, contract("209902", "209906"), table_holding("209902", "209906"))
        review = {i["old_code"]: (i["status"], i["jibun"]) for i in cg.pair_changes(rows, floor, jibun=jibun, min_share=MIN_SHARE).review}
        self.assertEqual(review["9812000000"][0], "awaiting_data")
        self.assertTrue(review["9812000000"][1].startswith("1 codes below wait: needs a parcel edition extracted after"))


class ParcelReadersDefaultToTheServedEditionTest(unittest.TestCase):
    """ADR-0148 §1: every reader that took a typed snapshot id reads the served edition unless told otherwise."""

    READERS = {  # job: (its served-id flag, the attribute, its other required arguments, its earlier-endpoint flag)
        "parcel_boundary_served_gold": ("--source-snapshot-id", "source_snapshot_id", ["--output-dir", "p"], None),
        "parcel_registry_to_silver": ("--to-snapshot-id", "to_snapshot_id", ["--to-date", "2099-09-01", "--to-sido", "97"],
                                      "--from-snapshot-id"),
        "parcel_lineage_to_silver": ("--to-snapshot-id", "to_snapshot_id",
                                     ["--from-snapshot-id", "vworldkr__parcel-209902", "--from-date", "2099-02-01",
                                      "--to-date", "2099-06-01", "--from-sido", "97", "--to-sido", "97"], "--from-snapshot-id"),
        "parcel_matching_gate": ("--snapshot-id", "snapshot_id", ["--sido", "97"], "--lineage-from-snapshot-id"),
    }

    def test_each_reader_reads_the_served_edition_and_refuses_another_without_the_flag(self):
        import importlib  # noqa: PLC0415

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "c.json"
            path.write_text(json.dumps(contract("209902", "209906", served="209906")), encoding="utf-8")
            with mock.patch.dict(os.environ, {editions.CONTRACT_ENV: str(path)}):
                for name, (flag, attr, rest, earlier) in self.READERS.items():
                    job = importlib.import_module(name)
                    with self.subTest(name):
                        self.assertEqual(getattr(job.parse_args(rest), attr), "vworldkr__parcel-209906")
                        self.assertEqual(getattr(job.parse_args(rest + [flag, "vworldkr__parcel-209906"]), attr),
                                         "vworldkr__parcel-209906")
                        # 심은 실수: 지난 판, 다른 철자. 깃발 없이는 거부한다.
                        for typed in ("vworldkr__parcel-209902", "vworldkr__parcel:209906"):
                            with self.assertRaisesRegex(editions.EditionError, "not the served edition"):
                                job.parse_args(rest + [flag, typed])
                        other = job.parse_args(rest + [flag, "vworldkr__parcel-209902", editions.ALLOW_OTHER_EDITION])
                        self.assertEqual(getattr(other, attr), "vworldkr__parcel-209902")
                        if earlier and earlier not in rest:
                            self.assertEqual(getattr(job.parse_args(rest + [earlier, "vworldkr__parcel-209902"]),
                                                     earlier[2:].replace("-", "_")), "vworldkr__parcel-209902")
                        if earlier:
                            argv = [a for a in rest if a != "vworldkr__parcel-209902" and a != earlier]
                            with self.assertRaisesRegex(editions.EditionError, "not the snapshot id of an edition"):
                                job.parse_args(argv + [earlier, "vworldkr__parcel:209902"])


class TheJobReadsEditionsFromTheContractTest(unittest.TestCase):
    def run_job(self, tmp: Path, source: dict, files: dict[str, str]) -> dict:
        (tmp / "contract.json").write_text(json.dumps(source), encoding="utf-8")
        (tmp / "table.html").write_text(table_html([
            row("9700000000", "합성시", parent="0000000000", created="20000101"),
            row("9711000000", "합성시 가구", parent="9700000000", created="20000101"),
            row("9711010100", "합성시 가구 갑동", "폐지", "9711000000", abolished=SPRING),
            row("9711010300", "합성시 가구 새갑동", parent="9711000000", created=SPRING),
            row("9800000000", "합성도", parent="0000000000", created="20000101"),
            row("9811000000", "합성도 나구", parent="9800000000", created="20000101"),
            row("9811010100", "합성도 나구 병동", "폐지", "9811000000", abolished=AUTUMN),
            row("9811010300", "합성도 나구 새병동", parent="9811000000", created=AUTUMN),
        ]), encoding="utf-8")
        argv = ["--snapshot-date", "2099-11-01", "--table-source-record-id", "k", "--validate-only",
                "--table-html", str(tmp / "table.html"), "--jibun-evidence", "editions",
                "--summary-output", str(tmp / "summary.json"), "--review-output", str(tmp / "review.json")]
        for name in files:
            (tmp / f"{name}.txt").write_text("\n".join(PARCELS[name]) + "\n", encoding="utf-8")
            argv += ["--edition-pnus", f"{name}={tmp / f'{name}.txt'}"]
        with mock.patch.dict(os.environ, {editions.CONTRACT_ENV: str(tmp / "contract.json")}), mock.patch("sys.stdout"):
            self.assertEqual(pairs_job.main(argv), 0)
        return {"summary": json.loads((tmp / "summary.json").read_text(encoding="utf-8")),
                "review": json.loads((tmp / "review.json").read_text(encoding="utf-8"))["review"]}

    def test_the_job_pairs_both_changes_and_names_what_it_waits_for(self):
        with tempfile.TemporaryDirectory() as tmp:
            done = self.run_job(Path(tmp), contract("209902", "209906", "209910"), {n: n for n in PARCELS})
            self.assertEqual(done["summary"]["pairs_by_evidence"], {"jibun": 2})
            self.assertEqual(done["summary"]["jibun_evidence"],
                             "editions[vworldkr__parcel-209902->vworldkr__parcel-209906, "
                             "vworldkr__parcel-209906->vworldkr__parcel-209910] awaiting=0")
        with tempfile.TemporaryDirectory() as tmp:
            waiting = self.run_job(Path(tmp), contract("209902", "209906", "209910"), {"209902": "", "209906": ""})
            self.assertEqual([(i["old_code"], i["status"]) for i in waiting["review"]], [("9811010100", "awaiting_data")])
            self.assertIn("209910", waiting["review"][0]["jibun"])

    def test_the_one_pair_flags_are_gone(self):
        base = ["--snapshot-date", "2099-11-01", "--table-source-record-id", "k", "--validate-only", "--table-html", "t"]
        for gone in (["--parcels-before-snapshot-id", "a", "--parcels-after-snapshot-id", "b"],
                     ["--parcels-before-pnus", "a", "--parcels-after-pnus", "b"]):
            with self.subTest(gone=gone), mock.patch("sys.stderr"), self.assertRaises(SystemExit):
                pairs_job.parse_args(base + gone)
        with mock.patch("sys.stderr"), self.assertRaises(SystemExit):  # a stand-in table without the step on
            pairs_job.parse_args(base + ["--edition-pnus", "209902=x"])
        with mock.patch("sys.stderr"), self.assertRaises(SystemExit):
            pairs_job.parse_args(base + ["--jibun-evidence", "editions", "--edition-pnus", "2099-2=x"])

    def test_validate_only_reads_the_lakehouse_when_given_no_local_table(self):
        # 운영 미리 보기(legal-dong-code-validate.sh): 표 파일 없이 --validate-only 는 레이크하우스를 읽고 쓰지 않는다.
        lake = pairs_job.parse_args(["--snapshot-date", "2099-11-01", "--table-source-record-id", "k", "--validate-only",
                                     "--jibun-evidence", "editions"])
        self.assertTrue(lake.validate_only and lake.table_html is None)
        self.assertEqual(lake.change_table, "legal_dong_code_change", "no smoke-suffix check: nothing is written")
        for local_only in (["--table-html", "t"], ["--recorded-changes", "r"],
                           ["--validate-only", "--jibun-evidence", "editions", "--edition-pnus", "209902=x"]):
            with self.subTest(local_only), mock.patch("sys.stderr"), self.assertRaises(SystemExit):
                pairs_job.parse_args(["--snapshot-date", "2099-11-01", "--table-source-record-id", "k",
                                      "--allow-non-smoke-write", *local_only])


if __name__ == "__main__":
    unittest.main()
