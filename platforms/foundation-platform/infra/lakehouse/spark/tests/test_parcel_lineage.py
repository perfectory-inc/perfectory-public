import sys
import unittest
from pathlib import Path


JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import parcel_lineage as pl  # noqa: E402

# Synthetic codes only (public repository): sido 99 is the "old" region, 98 the region it became.
CODE_LIST = "\n".join(
    [
        "법정동코드\t법정동명\t폐지여부",
        "9911010100\t합성도 가구 갑동\t폐지",
        "9911010200\t합성도 가구 을동\t폐지",
        "9812010100\t합성시 가구 갑동\t존재",
        "9812010200\t합성시 가구 을동\t존재",
        "9711010100\t다른도 나구 갑동\t존재",
        "9911010300\t합성도 가구 병동\t존재",
    ]
)


def pnu(dong, main, sub=0, mountain=False):
    return f"{dong}{'2' if mountain else '1'}{main:04d}{sub:04d}"


class CodeListTest(unittest.TestCase):
    def test_the_official_file_is_parsed_and_malformed_rows_are_refused(self):
        codes = pl.parse_code_list(CODE_LIST)
        self.assertEqual(codes["9812010100"], ("합성시 가구 갑동", pl.EXISTS))
        for bad in ("9911\t이름\t존재", "9911010100\t이름\t모름", "9911010100\t이름"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                pl.parse_code_list("법정동코드\t법정동명\t폐지여부\n" + bad)
        with self.assertRaisesRegex(ValueError, "header"):
            pl.parse_code_list("9911010100\t이름\t존재")


class DongPairingTest(unittest.TestCase):
    def setUp(self):
        self.codes = pl.parse_code_list(CODE_LIST)

    def test_a_renamed_dong_pairs_with_the_new_dong_of_the_same_leaf_name_only(self):
        before = {pnu("9911010100", 1), pnu("9911010200", 1), pnu("9911010300", 5)}
        after = {pnu("9812010100", 1), pnu("9812010200", 1), pnu("9911010300", 5), pnu("9711010100", 1)}
        pairing = pl.pair_legal_dongs(self.codes, pl.lots_by_dong(before), pl.lots_by_dong(before), pl.lots_by_dong(after))
        self.assertEqual(pairing.pairs["9911010100"], "9812010100")
        self.assertEqual(pairing.pairs["9911010300"], "9911010300")
        self.assertEqual(pairing.how["9911010300"], "unchanged")
        self.assertEqual(pairing.unpaired, [])

    def test_a_same_named_dong_elsewhere_is_never_a_candidate(self):
        # 다른도 나구 갑동 exists and holds parcels, but it held them before too: not new, not a candidate.
        before = {pnu("9911010100", 1), pnu("9711010100", 1)}
        after = {pnu("9812010100", 1), pnu("9711010100", 1)}
        pairing = pl.pair_legal_dongs(self.codes, pl.lots_by_dong(before), pl.lots_by_dong(before), pl.lots_by_dong(after))
        self.assertEqual(pairing.pairs["9911010100"], "9812010100")

    def test_a_low_lot_overlap_is_a_split_signal(self):
        before = {pnu("9911010100", n) for n in range(1, 11)}
        after = {pnu("9812010100", n) for n in range(1, 4)} | {pnu("9812010200", n) for n in range(50, 57)}
        pairing = pl.pair_legal_dongs(self.codes, pl.lots_by_dong(before), pl.lots_by_dong(before), pl.lots_by_dong(after))
        self.assertAlmostEqual(pairing.lot_overlap["9911010100"], 0.3)
        self.assertEqual(pairing.split_signals(), ["9911010100"])


class CarryOverTest(unittest.TestCase):
    def test_lots_that_survive_the_rename_are_code_derived_and_the_counts_balance(self):
        codes = pl.parse_code_list(CODE_LIST)
        before = {pnu("9911010100", 1), pnu("9911010100", 2), pnu("9911010300", 7)}
        after = {pnu("9812010100", 1), pnu("9812010100", 3), pnu("9911010300", 7)}
        pairing = pl.pair_legal_dongs(codes, pl.lots_by_dong(before), pl.lots_by_dong(before), pl.lots_by_dong(after))
        links, vanished, appeared = pl.carry_over(before, after, pairing)
        self.assertEqual(
            links,
            [pl.Link(pnu("9911010100", 1), pnu("9812010100", 1), "code_change", "code_derived", "same_lot_paired_dong", "sigungu+dong name")],
        )
        self.assertEqual(vanished, {pnu("9911010100", 2)})
        self.assertEqual(appeared, {pnu("9812010100", 3)})
        self.assertTrue(pl.reconcile(before, after, vanished, appeared, links).balanced)


class HistoryTextTest(unittest.TestCase):
    def event(self, p, reason, when="2099-02-01", code="10"):
        return pl.MovementEvent(p, code, reason, when, "")

    def test_merge_split_and_conversion_texts_name_the_other_lot(self):
        d = "9911010300"
        events = [
            self.event(pnu(d, 62), "88번과 합병되어 말소"),
            self.event(pnu(d, 454, 30), "454번에서 분할"),
            self.event(pnu(d, 337, 58), "산 27-1번에서 등록전환"),
            self.event(pnu(d, 9), "지목변경"),
        ]
        links = pl.history_links(events, "2099-01-01", "2099-12-31")
        by_rel = {l.relation: (l.predecessor_pnu, l.successor_pnu, l.grade) for l in links}
        self.assertEqual(by_rel["merge"], (pnu(d, 62), pnu(d, 88), "official"))
        self.assertEqual(by_rel["split"], (pnu(d, 454), pnu(d, 454, 30), "official"))
        self.assertEqual(by_rel["registration_conversion"], (pnu(d, 27, 1, mountain=True), pnu(d, 337, 58), "official"))
        self.assertEqual(len(links), 3)

    def test_events_before_the_earlier_snapshot_are_not_counted_again(self):
        events = [self.event(pnu("9911010300", 62), "88번과 합병되어 말소", when="2098-12-31")]
        self.assertEqual(pl.history_links(events, "2099-01-01", "2099-12-31"), [])

    def test_the_same_event_under_old_and_new_codes_is_one_link(self):
        codes = pl.parse_code_list(CODE_LIST)
        before = {pnu("9911010100", 62), pnu("9911010100", 88)}
        after = {pnu("9812010100", 88)}
        pairing = pl.pair_legal_dongs(codes, pl.lots_by_dong(before), pl.lots_by_dong(before), pl.lots_by_dong(after))
        events = [
            self.event(pnu("9911010100", 62), "88번과 합병되어 말소"),
            self.event(pnu("9812010100", 62), "88번과 합병되어 말소"),
        ]
        links = pl.history_links(events, "2099-01-01", "2099-12-31", pairing)
        self.assertEqual(links, [pl.Link(pnu("9911010100", 62), pnu("9812010100", 88), "merge", "official", "history_text", "88번과 합병되어 말소", "2099-02-01")])

    def test_jurisdiction_transfers_are_read_from_the_reason_code(self):
        events = [self.event(pnu("9812010200", 803), "행정관할구역변경", code="52"), self.event(pnu("9812010200", 1), "지목변경")]
        self.assertEqual(pl.transferred_in(events, "2099-01-01", "2099-12-31"), {pnu("9812010200", 803)})


class BuildingAndAttributeEvidenceTest(unittest.TestCase):
    def test_a_building_key_links_a_renumbered_lot_only_when_every_building_agrees(self):
        old_a, old_b, new_x, new_y = pnu("9911010100", 150), pnu("9911010100", 151), pnu("9812010200", 803), pnu("9812010200", 804)
        before = {"k1": old_a, "k2": old_b, "k3": old_b}
        after = {"k1": new_x, "k2": new_x, "k3": new_y}
        links = pl.building_links(before, after, {old_a, old_b}, {new_x, new_y})
        self.assertEqual([(l.predecessor_pnu, l.successor_pnu, l.grade) for l in links], [(old_a, new_x, "code_derived")])

    def test_attribute_grades(self):
        old = {
            "o1": pl.ParcelFacts("142.0", "17", "01", "0"),
            "o2": pl.ParcelFacts("61", "08", "02", "1"),
            "o3": pl.ParcelFacts("300", "05", None, None),
            "o4": pl.ParcelFacts("55", "01", "01", "0"),
            "o5": pl.ParcelFacts("55", "01", "02", "0"),
        }
        new = {
            "n1": pl.ParcelFacts("142", "17", "01", "00"),
            "n2": pl.ParcelFacts("61.0", "08", "01", "0"),
            "n3": pl.ParcelFacts("300.0", "05", "01", "0"),
            "n4": pl.ParcelFacts("55", "01", "02", "0"),
            "n5": pl.ParcelFacts("999", "01", "01", "0"),
        }
        grades = {l.successor_pnu: (l.predecessor_pnu, l.grade) for l in pl.attribute_links(old, new, "jurisdiction_transfer")}
        self.assertEqual(grades["n1"], ("o1", "evidence_strong"))
        self.assertEqual(grades["n2"], ("o2", "needs_review"))
        self.assertEqual(grades["n3"], ("o3", "evidence_weak"))
        self.assertEqual(grades["n4"], ("o5", "evidence_weak"), "ownership breaks the area+category tie")
        self.assertEqual(grades["n5"], ("", "pending"))


class EffectiveLinkTest(unittest.TestCase):
    def test_the_strongest_grade_wins_and_a_disagreeing_identity_claim_is_reported(self):
        a, b, n = pnu("9911010100", 1), pnu("9911010100", 2), pnu("9812010200", 803)
        links = [
            pl.Link(b, n, "jurisdiction_transfer", "evidence_strong", "area+category+ownership"),
            pl.Link(a, n, "jurisdiction_transfer", "code_derived", "building_register_key"),
        ]
        best, conflicts = pl.effective_links(links)
        self.assertEqual(best[n].predecessor_pnu, a)
        self.assertEqual(len(conflicts), 1)

    def test_a_merge_with_two_predecessors_is_not_a_conflict(self):
        a, b, n = pnu("9911010300", 1), pnu("9911010300", 2), pnu("9911010300", 3)
        links = [pl.Link(a, n, "merge", "official", "history_text"), pl.Link(b, n, "merge", "official", "history_text")]
        _, conflicts = pl.effective_links(links)
        self.assertEqual(conflicts, [])
        self.assertEqual(set(pl.cardinality(links).values()), {"N:1"})

    def test_polygon_only_evidence_does_not_exist_as_a_grade(self):
        self.assertNotIn("geometry", " ".join(pl.GRADES))
        self.assertEqual(pl.GRADES[-1], "pending")


class ValidityTest(unittest.TestCase):
    def test_malformed_source_pnus_are_set_aside(self):
        good, bad = pl.partition_valid({pnu("9911010300", 1), "99110103001 0010000", "9911010300100010000--"})
        self.assertEqual(good, {pnu("9911010300", 1)})
        self.assertEqual(len(bad), 2)


if __name__ == "__main__":
    unittest.main()
