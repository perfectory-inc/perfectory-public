"""The views of `reference.legal_dong_code_change` that replaced the stored copies (root ADR-0145).

Codes are synthetic (the reserved 99999 range and the synthetic 시도 97–99), except the 27 baseline
pairs, which are read from their fixture file and dressed in made-up rows.
"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

SPARK_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK_DIR / "jobs"))

import legal_dong_code_change_pairs as pairs_job  # noqa: E402
import legal_dong_code_change_views as views  # noqa: E402

BASELINE = json.loads((SPARK_DIR.parent / "contracts" / "sigungu-crosswalk-baseline.json").read_text(encoding="utf-8"))
CADASTRAL = pairs_job.cadastral_sido(json.loads(pairs_job.PARCEL_SOURCE_PATH.read_text(encoding="utf-8")))


def change(old, new, level, source="derived:code-go-kr:date+name:20990630", kind="pair"):
    return {"kind": kind, "old_code": old, "new_code": new, "level": level, "effective_date": "20990701", "source": source,
            "rule_verdict": "rule", "detail": None, "derivation_run_id": "run", "recorded_at_utc": None}


def pnu(code, main):
    return f"{code}1{main:04d}0000"


def lots(*pnus):
    out: dict[str, set[str]] = {}
    for value in pnus:
        out.setdefault(value[:10], set()).add(value[10:])
    return out


class SigunguCrosswalkViewTest(unittest.TestCase):
    def table(self):
        """The change table after a run that paired the baseline's merger: 시도, 시군구 and one 동 per 시군구."""

        merged = BASELINE["sido"][0]
        rows = [change(old + "00000000", merged["current_code"] + "00000000", "sido") for old in merged["supersedes"]]
        for pair in BASELINE["sigungu"]:
            rows.append(change(pair["superseded_code"] + "00000", pair["current_code"] + "00000", "sigungu"))
            rows.append(change(pair["superseded_code"] + "10100", pair["current_code"] + "10100", "eupmyeondong"))
        return rows

    def test_the_view_of_the_table_reproduces_the_27_baseline_pairs(self):
        crosswalk, review = views.sigungu_crosswalk_view(self.table(), CADASTRAL)
        expected = {(p["superseded_code"], p["current_code"]) for p in BASELINE["sigungu"]}
        self.assertEqual(len(expected), 27)
        self.assertEqual({(e["old_code"], e["new_code"]) for e in crosswalk["sigungu"]}, expected)
        self.assertEqual(crosswalk["sido"], [{"new_code": BASELINE["sido"][0]["current_code"],
                                              "old_codes": sorted(BASELINE["sido"][0]["supersedes"]), "effective_date": "20990701"}])
        self.assertEqual(review, [])

    def test_a_pair_recorded_twice_is_one_entry_and_other_kinds_are_not_pairs(self):
        rows = self.table()
        again = [{**row, "derivation_run_id": "run2"} for row in rows]
        noise = [{**rows[2], "kind": "note"}, {**rows[2], "old_code": None}]
        self.assertEqual(views.sigungu_crosswalk_view(rows + again + noise, CADASTRAL),
                         views.sigungu_crosswalk_view(rows, CADASTRAL))

    def test_a_split_sigungu_is_not_an_entry(self):
        # 심은 분리: 옛 시군구 하나가 두 새 시군구로 → 크로스워크 한 칸이 될 수 없다.
        rows = [change("9800000000", "9900000000", "sido"), change("9811000000", "9911000000", "sigungu"),
                change("9811000000", "9912000000", "sigungu", source="steward:a")]
        crosswalk, review = views.sigungu_crosswalk_view(rows, ["98"])
        self.assertEqual(crosswalk["sigungu"], [])
        self.assertEqual({item["reason"] for item in review}, {"not_one_to_one"})


class DongPredecessorViewTest(unittest.TestCase):
    """The administrative-boundary id's predecessor map, read from the table instead of a CLI file."""

    def test_the_view_matches_the_map_the_retired_command_wrote(self):
        # The fixture the retired command was tested on: a 면 (99999-101) with two 리, renumbered to
        # 99999-201. That command wrote {new: old} for both 리 and rolled them up to the 면.
        rows = [change("9999910121", "9999920121", "ri"), change("9999910122", "9999920122", "ri")]
        before = lots(*(pnu("9999910121", n) for n in range(1, 4)), *(pnu("9999910122", n) for n in range(1, 3)))
        retired_output = {"9999920100": "9999910100", "9999920121": "9999910121", "9999920122": "9999910122"}
        self.assertEqual(views.dong_predecessors(rows, before), retired_output)
        self.assertEqual(views.dong_predecessors(rows), retired_output, "lot counts only break ties")

    def test_a_directly_paired_myeon_is_not_overridden_by_its_ri(self):
        rows = [change("9999910121", "9999920121", "ri"), change("9999910100", "9999930100", "eupmyeondong")]
        self.assertEqual(views.dong_predecessors(rows)["9999930100"], "9999910100")
        self.assertEqual(views.dong_predecessors(rows)["9999920100"], "9999910100")

    def test_merged_dongs_leave_the_id_with_the_one_that_brought_the_most_lots(self):
        rows = [change("9999910100", "9999930100", "eupmyeondong"), change("9999910200", "9999930100", "eupmyeondong")]
        before = lots(pnu("9999910100", 1), *(pnu("9999910200", n) for n in range(1, 4)))
        self.assertEqual(views.dong_predecessors(rows, before), {"9999930100": "9999910200"})
        self.assertEqual(views.dong_predecessors(rows), {"9999930100": "9999910100"}, "without lots the smaller code keeps it")

    def test_unchanged_codes_and_other_levels_are_not_listed(self):
        rows = [change("9999910121", "9999910121", "ri"), change("9999900000", "9999800000", "sigungu"),
                change("9900000000", "9800000000", "sido")]
        self.assertEqual(views.dong_predecessors(rows), {})


if __name__ == "__main__":
    unittest.main()
