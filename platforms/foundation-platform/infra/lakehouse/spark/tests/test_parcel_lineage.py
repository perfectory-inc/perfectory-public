import sys
import unittest
from pathlib import Path


JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import parcel_lineage as pl  # noqa: E402

# Synthetic codes only, in the repository-reserved 99999 range: dongs 99999-1xx are the "old"
# region, 99999-2xx the region it became.
CODE_LIST = "\n".join(
    [
        "법정동코드\t법정동명\t폐지여부",
        "9999910100\t합성도 가구 갑동\t폐지",
        "9999910200\t합성도 가구 을동\t폐지",
        "9999920100\t합성시 가구 갑동\t존재",
        "9999920200\t합성시 가구 을동\t존재",
        "9999930100\t다른도 나구 갑동\t존재",
        "9999910300\t합성도 가구 병동\t존재",
    ]
)


def pnu(dong, main, sub=0, mountain=False):
    return f"{dong}{'2' if mountain else '1'}{main:04d}{sub:04d}"


class CodeListTest(unittest.TestCase):
    def test_the_official_file_is_parsed_and_malformed_rows_are_refused(self):
        codes = pl.parse_code_list(CODE_LIST)
        self.assertEqual(codes["9999920100"], ("합성시 가구 갑동", pl.EXISTS))
        for bad in ("9911\t이름\t존재", "9999910100\t이름\t모름", "9999910100\t이름"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                pl.parse_code_list("법정동코드\t법정동명\t폐지여부\n" + bad)
        with self.assertRaisesRegex(ValueError, "header"):
            pl.parse_code_list("9999910100\t이름\t존재")


def change(old, new, level="eupmyeondong", source="derived:code-go-kr:date+name:20990630"):
    """One `reference.legal_dong_code_change` pair row."""

    return {"kind": "pair", "old_code": old, "new_code": new, "level": level, "effective_date": "20990701", "source": source}


# The change table's view of the CODE_LIST change: 갑동 and 을동 moved from 99999-1xx to 99999-2xx.
CHANGES = [change("9999910100", "9999920100"), change("9999910200", "9999920200")]


class DongPairingTest(unittest.TestCase):
    """The lineage reads its 동 pairs from the code change table (root ADR-0145 §2)."""

    def test_the_dong_pairs_are_the_change_tables(self):
        before = {pnu("9999910100", 1), pnu("9999910200", 1), pnu("9999910300", 5)}
        after = {pnu("9999920100", 1), pnu("9999920200", 1), pnu("9999910300", 5), pnu("9999930100", 1)}
        pairing = pl.dong_pairing_from_changes(CHANGES, pl.lots_by_dong(before), pl.lots_by_dong(after))
        self.assertEqual(pairing.pairs, {"9999910100": "9999920100", "9999910200": "9999920200"})
        self.assertEqual(pairing.how["9999910100"], CHANGES[0]["source"])
        self.assertEqual(pairing.to_new(pnu("9999910300", 5)), pnu("9999910300", 5), "a code the table does not move keeps its number")
        self.assertEqual(pairing.unpaired, [])

    def test_no_pair_is_made_up_without_the_table(self):
        # Same names and lots as above, but the table records nothing: the lineage pairs nothing itself.
        before = {pnu("9999910100", 1)}
        after = {pnu("9999920100", 1)}
        pairing = pl.dong_pairing_from_changes([], pl.lots_by_dong(before), pl.lots_by_dong(after))
        self.assertEqual(pairing.pairs, {})
        self.assertEqual(pairing.unpaired, ["9999910100"])

    def test_a_split_and_other_levels_and_kinds_are_not_one_pair(self):
        rows = [
            change("9999910100", "9999920100"), change("9999910100", "9999920200", source="steward:someone"),
            change("9999900000", "9999800000", level="sigungu"),
            {**change("9999910200", "9999920200"), "kind": "note"},
        ]
        pairing = pl.dong_pairing_from_changes(rows, {}, {})
        self.assertEqual(pairing.pairs, {})
        self.assertEqual(pairing.unpaired, ["9999910100"])

    def test_a_low_lot_overlap_is_a_split_signal(self):
        before = {pnu("9999910100", n) for n in range(1, 11)}
        after = {pnu("9999920100", n) for n in range(1, 4)} | {pnu("9999920200", n) for n in range(50, 57)}
        pairing = pl.dong_pairing_from_changes(CHANGES, pl.lots_by_dong(before), pl.lots_by_dong(after))
        self.assertAlmostEqual(pairing.lot_overlap["9999910100"], 0.3)
        self.assertEqual(pairing.split_signals(), ["9999910100"])


class CarryOverTest(unittest.TestCase):
    def test_lots_that_survive_the_rename_are_code_derived_and_the_counts_balance(self):
        before = {pnu("9999910100", 1), pnu("9999910100", 2), pnu("9999910300", 7)}
        after = {pnu("9999920100", 1), pnu("9999920100", 3), pnu("9999910300", 7)}
        pairing = pl.dong_pairing_from_changes(CHANGES, pl.lots_by_dong(before), pl.lots_by_dong(after))
        links, vanished, appeared = pl.carry_over(before, after, pairing)
        self.assertEqual(
            links,
            [pl.Link(pnu("9999910100", 1), pnu("9999920100", 1), "code_change", "code_derived", "same_lot_paired_dong", CHANGES[0]["source"])],
        )
        self.assertEqual(vanished, {pnu("9999910100", 2)})
        self.assertEqual(appeared, {pnu("9999920100", 3)})
        self.assertTrue(pl.reconcile(before, after, vanished, appeared, links).balanced)


class HistoryTextTest(unittest.TestCase):
    def event(self, p, reason, when="2099-02-01", code="10"):
        return pl.MovementEvent(p, code, reason, when, "")

    def test_merge_split_and_conversion_texts_name_the_other_lot(self):
        d = "9999910300"
        events = [
            self.event(pnu(d, 62), "88번과 합병되어 말소"),
            self.event(pnu(d, 454, 30), "454번에서 분할"),
            self.event(pnu(d, 337, 58), "산 27-1번에서 등록전환"),
            self.event(pnu(d, 9), "지목변경"),
        ]
        links = pl.history_links(events, "2099-01-01", "2099-12-31")
        by_rel = {link.relation: (link.predecessor_pnu, link.successor_pnu, link.grade) for link in links}
        self.assertEqual(by_rel["merge"], (pnu(d, 62), pnu(d, 88), "official"))
        self.assertEqual(by_rel["split"], (pnu(d, 454), pnu(d, 454, 30), "official"))
        self.assertEqual(by_rel["registration_conversion"], (pnu(d, 27, 1, mountain=True), pnu(d, 337, 58), "official"))
        self.assertEqual(len(links), 3)

    def test_events_before_the_earlier_snapshot_are_not_counted_again(self):
        events = [self.event(pnu("9999910300", 62), "88번과 합병되어 말소", when="2098-12-31")]
        self.assertEqual(pl.history_links(events, "2099-01-01", "2099-12-31"), [])

    def test_the_same_event_under_old_and_new_codes_is_one_link(self):
        before = {pnu("9999910100", 62), pnu("9999910100", 88)}
        after = {pnu("9999920100", 88)}
        pairing = pl.dong_pairing_from_changes(CHANGES, pl.lots_by_dong(before), pl.lots_by_dong(after))
        events = [
            self.event(pnu("9999910100", 62), "88번과 합병되어 말소"),
            self.event(pnu("9999920100", 62), "88번과 합병되어 말소"),
        ]
        links = pl.history_links(events, "2099-01-01", "2099-12-31", pairing)
        self.assertEqual(links, [pl.Link(pnu("9999910100", 62), pnu("9999920100", 88), "merge", "official", "history_text", "88번과 합병되어 말소", "2099-02-01")])

    def test_jurisdiction_transfers_are_read_from_the_reason_code(self):
        events = [self.event(pnu("9999920200", 803), "행정관할구역변경", code="52"), self.event(pnu("9999920200", 1), "지목변경")]
        self.assertEqual(pl.transferred_in(events, "2099-01-01", "2099-12-31"), {pnu("9999920200", 803)})


class BuildingAndAttributeEvidenceTest(unittest.TestCase):
    def test_a_building_key_links_a_renumbered_lot_only_when_every_building_agrees(self):
        old_a, old_b, new_x, new_y = pnu("9999910100", 150), pnu("9999910100", 151), pnu("9999920200", 803), pnu("9999920200", 804)
        before = {"k1": old_a, "k2": old_b, "k3": old_b}
        after = {"k1": new_x, "k2": new_x, "k3": new_y}
        links = pl.building_links(before, after, {old_a, old_b}, {new_x, new_y})
        self.assertEqual([(link.predecessor_pnu, link.successor_pnu, link.grade) for link in links], [(old_a, new_x, "code_derived")])

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
        grades = {link.successor_pnu: (link.predecessor_pnu, link.grade) for link in pl.attribute_links(old, new, "jurisdiction_transfer")}
        self.assertEqual(grades["n1"], ("o1", "evidence_strong"))
        self.assertEqual(grades["n2"], ("o2", "needs_review"))
        self.assertEqual(grades["n3"], ("o3", "evidence_weak"))
        self.assertEqual(grades["n4"], ("o5", "evidence_weak"), "ownership breaks the area+category tie")
        self.assertEqual(grades["n5"], ("", "pending"))


class LotOrderTest(unittest.TestCase):
    OLD, NEW = "9999910100", "9999920200"

    def facts(self, pairs):
        return {p: pl.ParcelFacts(a, c) for p, a, c in pairs}

    def test_parcels_between_anchors_pair_in_order_when_area_and_category_agree(self):
        o, n = self.OLD, self.NEW
        anchors = {pnu(n, 800): pnu(o, 10), pnu(n, 810): pnu(o, 20)}
        new = self.facts([(pnu(n, 801), "34", "16"), (pnu(n, 802), "290", "16"), (pnu(n, 803), "11", "05")])
        old = self.facts([(pnu(o, 11), "34", "16"), (pnu(o, 12), "290", "16"), (pnu(o, 13), "288", "16")])
        links = pl.lot_order_links(anchors, new, old, new, old, "jurisdiction_transfer")
        self.assertEqual(
            {(link.successor_pnu, link.predecessor_pnu) for link in links},
            {(pnu(n, 801), pnu(o, 11)), (pnu(n, 802), pnu(o, 12))},
            "the forest parcel with no counterpart stays unpaired",
        )
        self.assertEqual({link.grade for link in links}, {"evidence_strong"})

    def test_equal_gaps_pair_even_when_values_repeat(self):
        o, n = self.OLD, self.NEW
        anchors = {pnu(n, 800): pnu(o, 10), pnu(n, 810): pnu(o, 20)}
        new = self.facts([(pnu(n, 801), "50", "14"), (pnu(n, 802), "50", "14")])
        old = self.facts([(pnu(o, 11), "50", "14"), (pnu(o, 12), "50", "14")])
        links = pl.lot_order_links(anchors, new, old, new, old, "jurisdiction_transfer")
        self.assertEqual(sorted((link.successor_pnu, link.predecessor_pnu) for link in links), [(pnu(n, 801), pnu(o, 11)), (pnu(n, 802), pnu(o, 12))])

    def test_the_rule_is_off_when_anchors_break_the_order(self):
        o, n = self.OLD, self.NEW
        anchors = {pnu(n, 800): pnu(o, 20), pnu(n, 810): pnu(o, 10)}
        new = self.facts([(pnu(n, 801), "34", "16")])
        old = self.facts([(pnu(o, 15), "34", "16")])
        self.assertEqual(pl.lot_order_links(anchors, new, old, new, old, "jurisdiction_transfer"), [])

    def test_repeated_values_in_an_unequal_gap_are_not_guessed(self):
        o, n = self.OLD, self.NEW
        anchors = {pnu(n, 800): pnu(o, 10), pnu(n, 810): pnu(o, 20)}
        new = self.facts([(pnu(n, 801), "50", "14"), (pnu(n, 802), "50", "14"), (pnu(n, 803), "9", "05")])
        old = self.facts([(pnu(o, 11), "50", "14"), (pnu(o, 12), "50", "14")])
        self.assertEqual(pl.lot_order_links(anchors, new, old, new, old, "jurisdiction_transfer"), [])


class EffectiveLinkTest(unittest.TestCase):
    def test_the_strongest_grade_wins_and_a_disagreeing_identity_claim_is_reported(self):
        a, b, n = pnu("9999910100", 1), pnu("9999910100", 2), pnu("9999920200", 803)
        links = [
            pl.Link(b, n, "jurisdiction_transfer", "evidence_strong", "area+category+ownership"),
            pl.Link(a, n, "jurisdiction_transfer", "code_derived", "building_register_key"),
        ]
        best, conflicts = pl.effective_links(links)
        self.assertEqual(best[n].predecessor_pnu, a)
        self.assertEqual(len(conflicts), 1)

    def test_a_merge_with_two_predecessors_is_not_a_conflict(self):
        a, b, n = pnu("9999910300", 1), pnu("9999910300", 2), pnu("9999910300", 3)
        links = [pl.Link(a, n, "merge", "official", "history_text"), pl.Link(b, n, "merge", "official", "history_text")]
        _, conflicts = pl.effective_links(links)
        self.assertEqual(conflicts, [])
        self.assertEqual(set(pl.cardinality(links).values()), {"N:1"})

    def test_polygon_only_evidence_does_not_exist_as_a_grade(self):
        self.assertNotIn("geometry", " ".join(pl.GRADES))
        self.assertEqual(pl.GRADES[-1], "pending")


class ValidityTest(unittest.TestCase):
    def test_malformed_source_pnus_are_set_aside(self):
        good, bad = pl.partition_valid({pnu("9999910300", 1), "99999103001 0010000", "9999910300100010000--"})
        self.assertEqual(good, {pnu("9999910300", 1)})
        self.assertEqual(len(bad), 2)


if __name__ == "__main__":
    unittest.main()
