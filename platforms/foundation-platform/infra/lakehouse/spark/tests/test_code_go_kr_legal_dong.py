"""The code.go.kr parser, the pairing and the crosswalk derivation (root ADR-0143, ADR-0144),
on synthetic data.

Every gate here is proven by planting what it must refuse: a reordered table, a shrunken table, a
split, a 지번 share below the contract's line, a tie, a load unit the registry already holds. Codes
use the synthetic 시도 97–99 and names are made up; the one test that reproduces the real 27 seed
pairs reads their codes from the baseline fixture and invents the names.
"""

from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from xml.sax.saxutils import escape

SPARK_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK_DIR / "jobs"))

import code_go_kr_legal_dong as cg  # noqa: E402
import lakehouse_ingest  # noqa: E402
import legal_dong_code_change_pairs as pairs_job  # noqa: E402
import legal_dong_code_change_views as views  # noqa: E402
import legal_dong_code_snapshot_to_reference as loader  # noqa: E402
import vworld_parcel_editions as editions  # noqa: E402

CONTRACT = cg.load_source_contract()
SEED = json.loads((SPARK_DIR.parent / "contracts" / "sigungu-crosswalk-baseline.json").read_text(encoding="utf-8"))
CADASTRAL = pairs_job.cadastral_sido(editions.load())
NOW = datetime(2099, 1, 2, tzinfo=timezone.utc)
FLOOR = CONTRACT["pairing"]["floor_date"]
DAY = "20990701"


# --- builders ---------------------------------------------------------------------------------


def row(code, name, status="현존", parent="", created="", abolished=""):
    """One full-table row in the site's column order."""

    return [code, name, status, parent, "01", created, abolished, "", name.split()[-1], code, code]


def table_html(rows, headers=None):
    """A page shaped like the site's: a layout table around the result table, unclosed rows."""

    headers = headers or CONTRACT["full_table"]["headers"]
    head = "".join(f'<td class="h">{escape(h)}</td>' for h in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{escape(c) if c else '&nbsp'}</td>" for c in cells) for cells in rows
    )
    return (
        "<html><body><table><tr><td><table><tr><td>검색</td></tr></table></td></tr></table>"
        f"<table><thead><tr>{head}</tr></thead>{body}</table>"
        "<table><thead><tr><td>분류명</td><td>주제분류명</td></tr></thead><tr><td>공통</td><td>공통</td></tr></table>"
        "</body></html>"
    )


def as_rows(page):
    return cg.parse_full_table_html(page, CONTRACT)


def merged_table(n_extra=0):
    """시도 98 merges into 99 on DAY: one 시군구, two dongs, and filler rows to pass the floor."""

    base = [
        row("9800000000", "합성도", "폐지", "0000000000", abolished=DAY),
        row("9900000000", "합성특별시", parent="0000000000", created=DAY),
        row("9811000000", "합성도 가구", "폐지", "9800000000", abolished=DAY),
        row("9911000000", "합성특별시 가구", parent="9900000000", created=DAY),
        row("9811010100", "합성도 가구 갑동", "폐지", "9811000000", abolished=DAY),
        row("9911010100", "합성특별시 가구 갑동", parent="9911000000", created=DAY),
        row("9811010200", "합성도 가구 을동", "폐지", "9811000000", abolished=DAY),
        row("9911010200", "합성특별시 가구 을동", parent="9911000000", created=DAY),
    ]
    filler = [row(f"97{index:08d}", f"합성채움 {index}동", parent="9700000000", created="20000101") for index in range(n_extra)]
    return base + filler



# --- the full table ---------------------------------------------------------------------------


class FullTableTest(unittest.TestCase):
    def test_the_site_page_becomes_one_row_per_code(self):
        rows = as_rows(table_html(merged_table()))
        self.assertEqual(len(rows), 8)
        first = rows[0]
        self.assertEqual((first["region_cd"], first["status"], first["abolished_date"], first["created_date"]), ("9800000000", cg.ABOLISHED, DAY, ""))
        self.assertEqual(rows[1]["status"], cg.EXISTS)

    def test_a_changed_column_order_is_refused(self):
        # 심은 위반: 생성일과 폐지일 칸이 자리를 바꿨다. 이름으로 옮겨 담지 않고 거부한다.
        headers = list(CONTRACT["full_table"]["headers"])
        headers[5], headers[6] = headers[6], headers[5]
        with self.assertRaisesRegex(cg.SourceFormatError, "columns changed"):
            as_rows(table_html(merged_table(), headers))
        with self.assertRaisesRegex(cg.SourceFormatError, "columns changed"):
            as_rows(table_html(merged_table(), CONTRACT["full_table"]["headers"] + ["새 칸"]))
        with self.assertRaisesRegex(cg.SourceFormatError, "no table headed"):
            as_rows("<html><table><tr><td>점검 중</td></tr></table></html>")

    def test_a_malformed_row_refuses_the_whole_table(self):
        for bad in (
            row("98110101", "합성도 가구 갑동"),  # 10자리가 아님
            row("9811010100", "합성도 가구 갑동", status="미상"),
            row("9811010100", "합성도 가구 갑동", created="2099-07-01"),
        ):
            with self.subTest(bad=bad), self.assertRaises(cg.SourceFormatError):
                as_rows(table_html([bad]))
        with self.assertRaisesRegex(cg.SourceFormatError, "appears twice"):
            as_rows(table_html([row("9811010100", "합성도 가구 갑동")] * 2))

    def test_a_table_thirty_percent_smaller_is_refused(self):
        bounds = CONTRACT["full_table"]["bounds"]
        previous = bounds["min_rows"] * 2
        cg.check_table_size(previous, previous, CONTRACT)
        with self.assertRaisesRegex(cg.TableShrunk, "fewer than the previous"):
            cg.check_table_size(int(previous * 0.7), previous, CONTRACT)
        with self.assertRaisesRegex(cg.TableShrunk, "below the floor"):
            cg.check_table_size(bounds["min_rows"] - 1, None, CONTRACT)

    def test_the_loader_refuses_a_shrunken_table_before_writing(self):
        rows = merged_table(n_extra=CONTRACT["full_table"]["bounds"]["min_rows"])
        with tempfile.TemporaryDirectory() as tmp:
            page = Path(tmp) / "regcode.html"
            page.write_text(table_html(rows), encoding="utf-8")
            summary = Path(tmp) / "summary.json"
            argv = ["--input", str(page), "--input-format", "code-go-kr-html", "--snapshot-date", "2099-07-02",
                    "--source-record-id", loader.CODE_GO_KR_TABLE_PREFIX + "operation=regCodeL/regcode-x.html",
                    "--validate-only", "--summary-output", str(summary)]
            self.assertEqual(loader.main(argv + ["--previous-row-count", str(len(rows))]), 0)
            written = json.loads(summary.read_text(encoding="utf-8"))
            self.assertEqual((written["row_count"], written["input_format"]), (len(rows), "code-go-kr-html"))
            summary.unlink()
            with self.assertRaises(cg.TableShrunk):
                loader.main(argv + ["--previous-row-count", str(int(len(rows) / 0.7))])
            self.assertFalse(summary.exists(), "a refused table must leave no summary")
            with self.assertRaisesRegex(ValueError, "source-record-id"):
                loader.main(argv[:-4] + ["bronze/source=elsewhere/x.html", "--validate-only"])
            dated = loader.code_go_kr_snapshot_rows(page.read_text(encoding="utf-8"), NOW.date(), "k", NOW, CONTRACT)
            self.assertEqual(set(dated[0]) - set(loader.COLUMNS), set())
            self.assertEqual(set(loader.COLUMNS) - set(dated[0]), set())

    def test_the_shrink_baseline_is_one_snapshot_even_when_a_day_holds_two(self):
        # 심은 함정: 마지막 날짜에 표가 둘(재시도) 적재됐다. 그날의 행을 모두 세면 기준이 두 배가 되어
        # 다음 표가 모두 '줄었다'로 거부된다. 기준은 그날 마지막 적재 하나다.
        d1, d2 = NOW.date().replace(day=1), NOW.date()
        loads = [(d1, NOW, "bronze/k-1", 50_000), (d2, NOW, "bronze/k-2a", 51_000),
                 (d2, NOW.replace(hour=5), "bronze/k-2b", 51_100)]
        self.assertEqual(loader.latest_snapshot_row_count(loads), 51_100)
        self.assertIsNone(loader.latest_snapshot_row_count([]))
        cg.check_table_size(51_100, loader.latest_snapshot_row_count(loads), CONTRACT)
        with self.assertRaises(cg.TableShrunk):  # what the double count would have done
            cg.check_table_size(51_100, 51_000 + 51_100, CONTRACT)



# --- pairing ----------------------------------------------------------------------------------

MIN_SHARE = CONTRACT["pairing"]["jibun_overlap_min_share"]


def lots(*numbers):
    """A 지번 set: ledger kind 1, 본번 n, 부번 0 (`parcel_lineage.lot`)."""

    return {f"1{n:04d}0000" for n in numbers}


def pnu(code, main):
    """A PNU in the synthetic 시도 of `code`: ledger kind 1, 본번 `main`, 부번 0."""

    return f"{code}1{main:04d}0000"


def renamed_table():
    """시도 98 merges into 99 on DAY, and the 동 갑동 is renamed 새갑동 at the same time."""

    rows = merged_table()
    rows[5] = row("9911010100", "합성특별시 가구 새갑동", parent="9911000000", created=DAY)
    return as_rows(table_html(rows))


# 갑동's 지번 all left (renumbered), and the parcel-number history is loaded but has no link yet:
# every step had its data and none decided, so a person does (ADR-0144 §3.4).
RENUMBERED = cg.JibunEvidence({"9811010100": lots(1, 2)}, {"9911010100": lots(901, 902)}, "b->a")
# 갑동 held no parcel in the earlier snapshot: the 지번 step had its data and found nothing to weigh,
# and no other step can see it, so a person decides (ADR-0144 §3.4).
NO_PARCELS_BEFORE = cg.JibunEvidence({}, {"9911010100": lots(901)}, "b->a")


class PairingTest(unittest.TestCase):
    def test_the_rule_pairs_a_merger_top_down(self):
        result = cg.pair_changes(as_rows(table_html(merged_table())), FLOOR)
        got = {(p["old_code"], p["new_code"], p["source"].split(":")[0]) for p in result.pairs}
        self.assertEqual(got, {
            ("9800000000", "9900000000", "derived"), ("9811000000", "9911000000", "derived"),
            ("9811010100", "9911010100", "derived"), ("9811010200", "9911010200", "derived"),
        })
        self.assertEqual(result.review, [])

    def test_abolished_the_day_before_counts_as_the_same_day(self):
        rows = merged_table()
        rows[2] = row("9811000000", "합성도 가구", "폐지", "9800000000", abolished="20990630")
        result = cg.pair_changes(as_rows(table_html(rows)), FLOOR)
        self.assertIn(("9811000000", "9911000000"), {(p["old_code"], p["new_code"]) for p in result.pairs})

    def test_a_split_goes_to_the_steward(self):
        rows = merged_table()
        # 심은 분리: 갑동이 같은 이름의 두 동(가구·나구 아래)으로 나뉘었다.
        rows += [row("9912000000", "합성특별시 나구", parent="9900000000", created=DAY),
                 row("9912010100", "합성특별시 가구 갑동", parent="9911000000", created=DAY)]
        result = cg.pair_changes(as_rows(table_html(rows)), FLOOR)
        review = {item["old_code"]: item for item in result.review}
        self.assertEqual(review["9811010100"]["reason"], "ambiguous_name_match")
        self.assertEqual(review["9811010100"]["candidates"], ["9911010100", "9912010100"])
        # 시군구 단위 분리: 옛 시군구 하나가 새 시군구 둘로 → 크로스워크 한 칸이 될 수 없다.
        split = [{"old_code": "9800000000", "new_code": "9900000000", "level": "sido", "effective_date": DAY, "source": "s"},
                 {"old_code": "9811000000", "new_code": "9911000000", "level": "sigungu", "effective_date": DAY, "source": "s"},
                 {"old_code": "9811000000", "new_code": "9912000000", "level": "sigungu", "effective_date": DAY, "source": "s"}]
        crosswalk, review = cg.sigungu_crosswalk(split, ["98"])
        self.assertEqual(crosswalk["sido"][0]["old_codes"], ["98"])
        self.assertEqual(crosswalk["sigungu"], [])
        self.assertEqual({item["reason"] for item in review}, {"not_one_to_one"})

    def test_without_the_data_a_code_awaits_it_and_no_steward_may_decide_it(self):
        # ADR-0144 §4: 연속지적 두 판이 없으면 2단계는 돌지 않고, 이름 규칙으로 추정하지도 않는다.
        parsed = renamed_table()
        [item] = cg.pair_changes(parsed, FLOOR).review
        self.assertEqual((item["old_code"], item["status"], item["jibun"]), ("9811010100", "awaiting_data", "jibun evidence off"))
        # 지번이 모두 떠났는데 변동연혁이 적재되지 않았다: 역시 데이터를 기다린다.
        [item] = cg.pair_changes(parsed, FLOOR, jibun=RENUMBERED).review
        self.assertEqual(item["status"], "awaiting_data")
        [item] = cg.pair_changes(parsed, FLOOR, jibun=RENUMBERED, official_links=[]).review
        self.assertEqual(item["status"], "steward")
        plan = pairs_job.plan_derivation(parsed, [], CONTRACT, CADASTRAL, "run", NOW)
        self.assertEqual(plan["counts"]["review_by_status"], {"awaiting_data": 1})
        # The job has no parcel-number history to give (not collected, and the parcel lineage is not
        # evidence: ADR-0145 §2), so a renumbered 동 waits for that data even with both snapshots.
        renumbered = pairs_job.plan_derivation(parsed, [], CONTRACT, CADASTRAL, "run", NOW, RENUMBERED)
        self.assertEqual(renumbered["counts"]["review_by_status"], {"awaiting_data": 1})
        with self.assertRaisesRegex(ValueError, "awaiting_data"):
            pairs_job.steward_rows(plan["review"], ["9811010100=9911010100"], "steward-a", "추정",
                                   {r["region_cd"] for r in parsed}, "s", NOW)

    def test_steward_approvals_are_accepted_only_for_listed_items(self):
        parsed = renamed_table()
        plan = pairs_job.plan_derivation(parsed, [], CONTRACT, CADASTRAL, "run", NOW, NO_PARCELS_BEFORE)
        codes = {r["region_cd"] for r in parsed}
        approved = pairs_job.steward_rows(plan["review"], ["9811010100=9911010100"], "steward-a", "현장 확인", codes, "s", NOW)
        self.assertEqual((approved[0]["source"], approved[0]["kind"]), ("steward:steward-a", "pair"))
        for approvals, why in ((["9811010200=9911010200"], "not on the steward list"),
                               (["9811010100=9999999999"], "not a candidate")):
            with self.subTest(approvals), self.assertRaisesRegex(ValueError, why):
                pairs_job.steward_rows(plan["review"], approvals, "steward-a", "이유", codes, "s", NOW)
        with self.assertRaisesRegex(ValueError, "reason"):
            pairs_job.steward_rows(plan["review"], ["9811010100=9911010100"], "steward-a", " ", codes, "s", NOW)
        # 승인 행을 기록한 다음 날의 도출은 그 짝을 쓰고 목록에서 뺀다.
        next_day = pairs_job.plan_derivation(parsed, approved, CONTRACT, CADASTRAL, "run2", NOW, NO_PARCELS_BEFORE)
        self.assertEqual(next_day["review"], [])
        self.assertIn("steward", next_day["counts"]["pairs_by_evidence"])


class JibunEvidenceTest(unittest.TestCase):
    """ADR-0113 §5's 지번 overlap, the second step (ADR-0144). Polygons are never read."""

    def evidence(self, before, after):
        return cg.JibunEvidence(before, after, "before->after")

    def test_a_renamed_dong_pairs_by_the_dong_that_holds_its_jibun(self):
        table = renamed_table()
        # 새갑동이 옛 갑동의 지번 20개를 모두 갖는다. 날짜·이름 규칙은 이름이 달라 못 정한다.
        jibun = self.evidence({"9811010100": lots(*range(1, 21))}, {"9911010100": lots(*range(1, 21)), "9911010200": lots(500)})
        result = cg.pair_changes(table, FLOOR, jibun=jibun, min_share=MIN_SHARE)
        pair = {p["old_code"]: p for p in result.pairs}["9811010100"]
        self.assertEqual((pair["new_code"], pair["source"], pair["rule_verdict"]),
                         ("9911010100", "derived:parcel-jibun:before->after", "jibun"))
        self.assertEqual(pair["detail"], "jibun_share:1.0000")
        self.assertEqual(result.review, [])

    def test_a_share_below_the_contract_line_is_not_a_pair(self):
        # 심은 분리: 옛 갑동 지번 20개 중 18개(90%)만 새갑동에 있다. 계약의 하한(95%) 아래는 짝이 아니다.
        jibun = self.evidence({"9811010100": lots(*range(1, 21))}, {"9911010100": lots(*range(1, 19))})
        result = cg.pair_changes(renamed_table(), FLOOR, jibun=jibun, min_share=MIN_SHARE)
        self.assertNotIn("9811010100", {p["old_code"] for p in result.pairs})
        [item] = result.review
        self.assertEqual((item["old_code"], item["jibun"], item["candidates"]),
                         ("9811010100", "best 9911010100 share 0.9000", ["9911010100"]))
        self.assertGreater(MIN_SHARE, 0.9, "the planted share must sit below the contract's line")

    def test_a_dong_split_into_two_is_recorded_as_a_split(self):
        # ADR-0144 §3.2: 여러 동으로 나뉘면 분할이다. 어디로 몇 개 갔는지 남기고, 짝으로 쓰지 않는다.
        rows = merged_table()
        rows[5] = row("9911010100", "합성특별시 가구 새갑동", parent="9911000000", created=DAY)
        rows.append(row("9911010300", "합성특별시 가구 다른동", parent="9911000000", created=DAY))
        jibun = self.evidence({"9811010100": lots(*range(1, 11))},
                              {"9911010100": lots(*range(1, 7)), "9911010300": lots(*range(7, 11))})
        result = cg.pair_changes(as_rows(table_html(rows)), FLOOR, jibun=jibun, min_share=MIN_SHARE)
        [item] = result.review
        self.assertEqual((item["status"], item["split_into"]), ("split", {"9911010100": 6, "9911010300": 4}))
        plan = pairs_job.plan_derivation(as_rows(table_html(rows)), [], CONTRACT, CADASTRAL, "run", NOW, jibun)
        with self.assertRaisesRegex(ValueError, "split"):
            pairs_job.steward_rows(plan["review"], ["9811010100=9911010100"], "steward-a", "추정", None, "s", NOW)

    def test_two_dongs_holding_as_many_jibun_are_not_guessed_between(self):
        rows = merged_table()
        rows[5] = row("9911010100", "합성특별시 가구 새갑동", parent="9911000000", created=DAY)
        rows.append(row("9911010300", "합성특별시 가구 다른동", parent="9911000000", created=DAY))
        jibun = self.evidence({"9811010100": lots(1, 2)}, {"9911010100": lots(1, 2), "9911010300": lots(1, 2)})
        result = cg.pair_changes(as_rows(table_html(rows)), FLOOR, jibun=jibun, min_share=MIN_SHARE)
        self.assertNotIn("9811010100", {p["old_code"] for p in result.pairs})
        self.assertIn("(tied)", result.review[0]["jibun"])

    def test_a_renamed_sigungu_rolls_up_from_its_dongs(self):
        # 심은 개편: 시군구 가구가 마구로 이름을 바꿨다(시도 통합과 같은 날). 동 이름은 그대로지만 상위가
        # 이어지지 않아 날짜·이름 규칙은 아무것도 못 정한다. 지번이 동을, 동이 시군구를 잇는다.
        rows = merged_table()
        rows[3] = row("9911000000", "합성특별시 마구", parent="9900000000", created=DAY)
        table = as_rows(table_html(rows))
        self.assertEqual({i["old_code"] for i in cg.pair_changes(table, FLOOR).review},
                         {"9811000000", "9811010100", "9811010200"})
        jibun = self.evidence({"9811010100": lots(1, 2, 3), "9811010200": lots(7)},
                              {"9911010100": lots(1, 2, 3), "9911010200": lots(7)})
        result = cg.pair_changes(table, FLOOR, jibun=jibun, min_share=MIN_SHARE)
        got = {p["old_code"]: (p["new_code"], p["rule_verdict"]) for p in result.pairs}
        self.assertEqual(got["9811000000"], ("9911000000", "rollup"))
        self.assertEqual(got["9811010100"], ("9911010100", "jibun"))
        self.assertEqual(result.review, [])

    def test_the_jibun_sets_come_from_pnus_and_skip_malformed_ones(self):
        pnus = [pnu("9811010100", 1), pnu("9811010100", 2), pnu("9811010200", 7), "98110101001", "x" * 19]
        self.assertEqual(cg.jibun_sets(pnus), {"9811010100": lots(1, 2), "9811010200": lots(7)})
        self.assertEqual(cg.jibun_sets(pnus, {"9811010200"}), {"9811010200": lots(7)})

    def test_parcel_lineage_reads_the_same_line_from_the_contract(self):
        self.assertEqual(cg.pl.SPLIT_SIGNAL_OVERLAP, MIN_SHARE)

    def test_a_recorded_pair_stands_when_a_later_run_lacks_its_evidence(self):
        table = renamed_table()
        jibun = self.evidence({"9811010100": lots(1, 2)}, {"9911010100": lots(1, 2)})
        first = pairs_job.plan_derivation(table, [], CONTRACT, CADASTRAL, "run", NOW, jibun)
        later = pairs_job.plan_derivation(table, first["fresh_changes"], CONTRACT, CADASTRAL, "run2", NOW)
        self.assertEqual(later["review"], [])
        self.assertEqual(later["fresh_changes"], [], "a re-run records nothing twice")
        self.assertEqual(later["counts"]["pairs"], first["counts"]["pairs"])


class OfficialParcelHistoryTest(unittest.TestCase):
    """The third step: 지번 renumbered with the code, settled only by the official parcel-number history."""

    def test_official_links_into_one_new_dong_settle_it(self):
        # 심은 재지번: 새갑동의 지번은 옛 갑동과 하나도 겹치지 않는다. 공식 이력만이 잇는다.
        jibun = cg.JibunEvidence({"9811010100": lots(1, 2)}, {"9911010100": lots(901, 902)}, "b->a")
        links = [(pnu("9811010100", 1), pnu("9911010100", 901)), (pnu("9811010100", 2), pnu("9911010100", 902))]
        without = cg.pair_changes(renamed_table(), FLOOR, jibun=jibun, min_share=MIN_SHARE)
        self.assertEqual([i["old_code"] for i in without.review], ["9811010100"])
        result = cg.pair_changes(renamed_table(), FLOOR, jibun=jibun, official_links=links, min_share=MIN_SHARE)
        pair = {p["old_code"]: p for p in result.pairs}["9811010100"]
        self.assertEqual((pair["new_code"], pair["source"]), ("9911010100", "official:parcel-history"))

    def test_links_into_two_dongs_go_to_the_steward(self):
        rows = merged_table()
        rows[5] = row("9911010100", "합성특별시 가구 새갑동", parent="9911000000", created=DAY)
        rows.append(row("9911010300", "합성특별시 가구 다른동", parent="9911000000", created=DAY))
        links = [(pnu("9811010100", 1), pnu("9911010100", 901)), (pnu("9811010100", 2), pnu("9911010300", 902))]
        result = cg.pair_changes(as_rows(table_html(rows)), FLOOR, official_links=links, min_share=MIN_SHARE)
        self.assertNotIn("9811010100", {p["old_code"] for p in result.pairs})


# --- the handoff ------------------------------------------------------------------------------


def collect_dir(root, table_rows, name="regcode-20990702T051500Z"):
    """A collector output directory: the manifest and the table, as the Rust collector writes it."""

    root.mkdir(parents=True, exist_ok=True)
    (root / "objects").mkdir(exist_ok=True)
    raw = table_html(table_rows).encode("utf-8")
    (root / "objects" / f"{name}.html").write_bytes(raw)
    manifest = {"collection_date": "2099-07-02", "objects": [{
        "role": "full_table", "object_key": f"bronze/source=codegokr__legal_dong_code_table/{name}.html",
        "local_path": f"objects/{name}.html", "checksum_sha256": hashlib.sha256(raw).hexdigest(), "size_bytes": len(raw)}]}
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return root


class HandoffTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.state = self.root / "state"
        # The floor is the contract's business (FullTableTest); here a small one keeps the tables small.
        self.contract = json.loads(json.dumps(CONTRACT))
        self.contract["full_table"]["bounds"]["min_rows"] = 10
        self.rows = merged_table(n_extra=40)

    def tearDown(self):
        self.tmp.cleanup()

    def test_a_changed_table_hands_off_once_and_an_unchanged_day_hands_off_nothing(self):
        first = cg.stage_handoff(collect_dir(self.root / "a", self.rows), self.state, self.contract, NOW)
        self.assertEqual(first["status"], "staged")
        pending = self.state / "pending" / first["handoff"]
        handoff = json.loads((pending / "handoff.json").read_text(encoding="utf-8"))
        self.assertEqual((handoff["row_count"], handoff["snapshot_date"]), (len(self.rows), "2099-07-02"))
        self.assertTrue((pending / "objects" / f"{first['handoff']}.html").exists())
        same = cg.stage_handoff(collect_dir(self.root / "b", self.rows, name="regcode-20990703T051500Z"), self.state, self.contract, NOW)
        self.assertEqual(same["status"], "unchanged")
        self.assertEqual(sorted(p.name for p in (self.state / "pending").iterdir()), [first["handoff"]])
        with self.assertRaises(FileExistsError):
            cg.stage_handoff(collect_dir(self.root / "d", self.rows + [row("9799999999", "합성 새동")], name=first["handoff"]),
                             self.state, self.contract, NOW)

    def test_a_shrunken_or_reshaped_table_is_refused_and_hands_off_nothing(self):
        cg.stage_handoff(collect_dir(self.root / "a", self.rows), self.state, self.contract, NOW)
        before = sorted(p.name for p in (self.state / "pending").iterdir())
        shrunk = self.rows[: int(len(self.rows) * 0.7)]
        with self.assertRaises(cg.TableShrunk):
            cg.stage_handoff(collect_dir(self.root / "b", shrunk, name="regcode-20990703T051500Z"), self.state, self.contract, NOW)
        broken = collect_dir(self.root / "c", self.rows, name="regcode-20990704T051500Z")
        table = broken / "objects" / "regcode-20990704T051500Z.html"
        table.write_text(table.read_text(encoding="utf-8").replace("폐지구분", "상태"), encoding="utf-8")
        with self.assertRaises(cg.SourceFormatError):
            cg.stage_handoff(broken, self.state, self.contract, NOW)  # 내용이 Bronze 의 sha256 과도 다르다
        self.assertEqual(sorted(p.name for p in (self.state / "pending").iterdir()), before)

    def test_every_passing_run_records_when_it_checked(self):
        first = NOW.replace(day=3)
        cg.stage_handoff(collect_dir(self.root / "a", self.rows), self.state, self.contract, first)
        accepted = json.loads((self.state / "accepted.json").read_text(encoding="utf-8"))
        self.assertEqual(accepted["checked_at_utc"], first.isoformat())
        later = NOW.replace(day=9)
        same = cg.stage_handoff(collect_dir(self.root / "b", self.rows, name="regcode-20990709T051500Z"), self.state, self.contract, later)
        self.assertEqual(same["status"], "unchanged")
        after = json.loads((self.state / "accepted.json").read_text(encoding="utf-8"))
        self.assertEqual(after["checked_at_utc"], later.isoformat())
        self.assertEqual(after["table_object_key"], accepted["table_object_key"], "an unchanged run keeps the handed-off table")


# --- steward decisions and reruns -------------------------------------------------------------


class StewardOnlyRerunTest(unittest.TestCase):
    KEY = "bronze/source=codegokr__legal_dong_code_table/operation=regCodeL/regcode-x.html"

    def test_rows_a_steward_decision_unlocks_are_appended_by_a_rerun_on_the_same_table(self):
        # 시군구에 후보가 없으니 그 아래 동들도 짝을 못 찾는다. 스튜어드가 시군구를 승인하면, 같은 표로
        # 다시 도는 다음 실행이 동의 짝을 새로 얻는다. 그 행들이 적재 단위가 겹쳐 조용히 버려지면 안 된다.
        rows = merged_table()
        rows[3] = row("9911000000", "합성특별시 새가구", parent="9900000000", created=DAY)
        table = as_rows(table_html(rows))
        codes = {r["region_cd"] for r in table}
        registry, held = set(), []
        # The 지번 step ran and found nothing (no parcels under these codes), so what is left is a
        # steward's (ADR-0144 §3.4).
        data = (cg.JibunEvidence({}, {}, "b->a"),)

        def append(batch):
            units = sorted({r["derivation_run_id"] for r in batch})
            if not lakehouse_ingest.decide_whether_to_append({unit: 1 for unit in registry}, units):
                return False
            registry.update(units)
            held.extend(batch)
            return True

        day1 = NOW
        plan = pairs_job.plan_derivation(table, [], CONTRACT, CADASTRAL, pairs_job.pairing_run_id(self.KEY, day1), day1, *data)
        pairs_job.append_new_rows(append, plan["fresh_changes"], "change")
        self.assertEqual({i["old_code"] for i in plan["review"]}, {"9811000000", "9811010100", "9811010200"})

        day2 = NOW.replace(day=3)
        decision = [("20990103T000000Z-steward-a.json", {"steward": "steward-a", "reason": "현장 확인",
                                                         "approve": ["9811000000=9911000000"]})]
        before = pairs_job.plan_derivation(table, list(held), CONTRACT, CADASTRAL, pairs_job.pairing_run_id(self.KEY, day2), day2, *data)
        steward, verdicts = pairs_job.fold_steward_decisions(decision, before["review"], codes, day2)
        self.assertEqual(list(verdicts.values()), ["recorded"])
        pairs_job.append_new_rows(append, steward, "steward")
        after = pairs_job.plan_derivation(table, list(held), CONTRACT, CADASTRAL, pairs_job.pairing_run_id(self.KEY, day2), day2, *data)
        unlocked = {(r["old_code"], r["new_code"]) for r in after["fresh_changes"]}
        self.assertEqual(unlocked, {("9811010100", "9911010100"), ("9811010200", "9911010200")})
        self.assertTrue(pairs_job.append_new_rows(append, after["fresh_changes"], "change"))
        self.assertTrue(unlocked <= {(r["old_code"], r["new_code"]) for r in held})
        self.assertEqual(pairs_job.plan_derivation(table, list(held), CONTRACT, CADASTRAL, "run3", day2, *data)["review"], [])

    def test_rows_the_registry_claims_are_refused_not_dropped(self):
        # 심은 위반: 적재 단위가 이미 등록된 행을 새 행이라며 넘긴다(예전의 표 키 하나짜리 실행 id).
        with self.assertRaisesRegex(ValueError, "dropped"):
            pairs_job.append_new_rows(lambda batch: False, [{"derivation_run_id": "code-go-kr-pairing:k"}], "change")
        self.assertFalse(pairs_job.append_new_rows(lambda batch: False, [], "change"))
        self.assertNotEqual(pairs_job.pairing_run_id(self.KEY, NOW), pairs_job.pairing_run_id(self.KEY, NOW.replace(day=3)))


class StewardDecisionTest(unittest.TestCase):
    def test_a_staged_decision_is_checked_now_and_again_when_folded(self):
        parsed = renamed_table()
        plan = pairs_job.plan_derivation(parsed, [], CONTRACT, CADASTRAL, "run", NOW, NO_PARCELS_BEFORE)
        with tempfile.TemporaryDirectory() as tmp:
            review = Path(tmp) / "steward-review.json"
            review.write_text(json.dumps({"review": plan["review"]}), encoding="utf-8")
            out = Path(tmp) / "pending"
            base = ["stage-steward-decision", "--review", str(review), "--output-dir", str(out), "--steward", "steward-a", "--reason", "현장 확인"]
            with self.assertRaisesRegex(ValueError, "not on the steward list"):
                pairs_job.main(base + ["--approve", "9811010200=9911010200"])
            self.assertFalse(out.exists() and any(out.iterdir()), "a refused decision leaves no file")
            self.assertEqual(pairs_job.main(base + ["--approve", "9811010100=9911010100"]), 0)
            decisions = [(p.name, json.loads(p.read_text(encoding="utf-8"))) for p in out.glob("*.json")]
        steward, verdicts = pairs_job.fold_steward_decisions(decisions, plan["review"], {r["region_cd"] for r in parsed}, NOW)
        self.assertEqual(list(verdicts.values()), ["recorded"])
        self.assertEqual(steward[0]["source"], "steward:steward-a")
        # 그 사이 목록이 바뀌었으면(이미 짝이 생김) 기록하지 않고 그렇다고 말한다.
        _, stale = pairs_job.fold_steward_decisions(decisions, [], set(), NOW)
        self.assertTrue(list(stale.values())[0].startswith("rejected"))


class SeedReproductionTest(unittest.TestCase):
    def test_the_derived_crosswalk_reproduces_the_seed_pairs(self):
        # 씨앗의 시도·시군구 코드로 7월 1일 통합의 전체 표를 만든다(이름은 지어낸 것). 날짜·이름 규칙만으로
        # 씨앗 27쌍과 같은 크로스워크가 나와야 한다.
        merged = SEED["sido"][0]
        rows = [row(merged["new_code"] + "00000000", "합성통합시", parent="0000000000", created=DAY)]
        rows += [row(old + "00000000", f"합성옛시도{old}", "폐지", "0000000000", abolished=DAY) for old in merged["old_codes"]]
        for index, pair in enumerate(SEED["sigungu"]):
            name = f"합성구{index:02d}"
            rows.append(row(pair["new_code"] + "00000", f"합성통합시 {name}", parent=merged["new_code"] + "00000000", created=DAY))
            rows.append(row(pair["old_code"] + "00000", f"합성옛시도 {name}", "폐지", pair["old_code"][:2] + "00000000", abolished=DAY))
        parsed = as_rows(table_html(rows))
        expected = {(p["old_code"], p["new_code"]) for p in SEED["sigungu"]}
        result = cg.pair_changes(parsed, FLOOR)
        crosswalk, review = cg.sigungu_crosswalk(result.pairs, CADASTRAL)
        self.assertEqual({(e["old_code"], e["new_code"]) for e in crosswalk["sigungu"]}, expected)
        self.assertEqual(crosswalk["sido"], [{"new_code": merged["new_code"], "old_codes": sorted(merged["old_codes"]),
                                              "effective_date": DAY}])
        self.assertEqual(review, [])
        # The projection is the crosswalk view of the change table once this run's rows are in it,
        # old → new (ADR-0145): the 27 pairs come back from the table's rows alone.
        plan = pairs_job.plan_derivation(parsed, [], CONTRACT, CADASTRAL, "run", NOW)
        projection = pairs_job.projection_document(plan, "2099-07-02", "bronze/k", "1", NOW)
        self.assertEqual(projection["schema_version"], "foundation-platform.sigungu_crosswalk_projection.v2")
        self.assertEqual((projection["change_table"], projection["change_table_snapshot_id"]), ("reference.legal_dong_code_change", "1"))
        self.assertEqual({(e["old_code"], e["new_code"]) for e in projection["sigungu"]}, expected)
        self.assertEqual(projection["sido"], [{"new_code": merged["new_code"], "old_codes": sorted(merged["old_codes"]),
                                               "effective_date": DAY}])
        self.assertEqual(views.sigungu_crosswalk_view(plan["fresh_changes"], CADASTRAL)[0], plan["crosswalk"])


class TheNoticeBoardIsNotASourceTest(unittest.TestCase):
    def test_no_board_endpoint_or_parser_remains(self):
        # ADR-0144: 원천은 내려받은 데이터뿐이다. 게시판 주소가 계약으로 돌아오면 거부한다.
        self.assertFalse({"notice_list", "notice_detail", "attachment"} & set(CONTRACT))
        self.assertNotIn("bbsmng", json.dumps(CONTRACT))
        for name in ("parse_notice_list_html", "notices_to_fetch", "parse_notice_spreadsheet", "notice_official_pairs"):
            self.assertFalse(hasattr(cg, name), name)


if __name__ == "__main__":
    unittest.main()


class TheChangeTableDoesNotReadTheLineageTest(unittest.TestCase):
    def test_the_pairing_job_reads_no_parcel_lineage(self):
        # ADR-0145 §2: the lineage reads its 동 pairs from the change table, so the change table may not
        # take the lineage as evidence, or each would read the other. 심은 위반: the option or a read of
        # the lineage table coming back into the job.
        import ast

        source = (SPARK_DIR / "jobs" / "legal_dong_code_change_pairs.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        strings = [node.value for node in ast.walk(tree) if isinstance(node, ast.Constant) and isinstance(node.value, str)]
        self.assertFalse([value for value in strings if "parcel_lineage" in value or "parcel-lineage" in value])
        with self.assertRaises(SystemExit):
            pairs_job.parse_args(["--snapshot-date", "2099-07-02", "--table-source-record-id", "k", "--validate-only",
                                  "--table-html", "t.html", "--parcel-lineage-table", "silver.parcel_lineage"])
        planted = source.replace('parser.add_argument("--steward-decisions"',
                                 'parser.add_argument("--parcel-lineage-table")\n    parser.add_argument("--steward-decisions"')
        self.assertNotEqual(planted, source, "the planted option must land in the source")
        planted_strings = [node.value for node in ast.walk(ast.parse(planted))
                           if isinstance(node, ast.Constant) and isinstance(node.value, str)]
        self.assertTrue([value for value in planted_strings if "parcel-lineage" in value], "the check must see a planted read")
