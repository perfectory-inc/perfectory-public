"""The place-identity code chain without Spark (root ADR-0288 §1).

A recorded old → new pair carries a code to the current one; a split or a cycle is left for a
steward; a superseded code reconstructs the current pnu keeping its lot. Codes are synthetic.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import place_identity_resolver as r  # noqa: E402


class CurrentCode(unittest.TestCase):
    def test_a_code_with_no_successor_is_already_current(self) -> None:
        self.assertEqual(r.current_code("9999900200", {}), ("9999900200", False))

    def test_a_chain_follows_old_to_current(self) -> None:
        pairs = {"9999900100": ["9999900150"], "9999900150": ["9999900200"]}
        self.assertEqual(r.current_code("9999900100", pairs), ("9999900200", False))

    def test_a_split_is_ambiguous_not_a_guess(self) -> None:
        self.assertEqual(r.current_code("9999900100", {"9999900100": ["9999900200", "9999900300"]}), (None, True))

    def test_a_cycle_is_ambiguous(self) -> None:
        self.assertEqual(r.current_code("A", {"A": ["B"], "B": ["A"]}), (None, True))


class Crosswalk(unittest.TestCase):
    def test_only_changed_codes_enter_and_a_split_is_flagged(self) -> None:
        # Synthetic codes (reserved 99999 namespace). The real case is a 분구: 화성시 장지동 → 동탄구.
        remap, ambiguous = r.code_crosswalk([
            ("9999900100", "9999900200"),                                 # a dong that moved to a new 구
            ("9999901000", "9999901000"),                                 # unchanged: not in the remap
            ("9999902000", "9999902001"), ("9999902000", "9999902002"),   # a split → steward
            ("", "9999900200"), ("9999900100", ""),                       # blanks are dropped
        ])
        self.assertEqual(remap, {"9999900100": "9999900200"})
        self.assertEqual(ambiguous, {"9999902000"})


class CurrentRegion(unittest.TestCase):
    """A complex's 시도·시군구·법정동 carried to the current codes (root ADR-0426). Synthetic codes: 97 and
    98 merged into 96, the way 29 and 46 merged into 12 on 2026-07-01."""

    REMAP = {"9700000000": "9600000000", "9800000000": "9600000000", "9717000000": "9630000000",
             "9889000000": "9685000000", "9717010100": "9630010100"}

    def test_a_merged_sido_and_its_sigungu_take_the_current_codes(self) -> None:
        self.assertEqual(r.current_region("97", "97170", None, self.REMAP, set()), ("96", "96300", None))
        self.assertEqual(r.current_region("98", "98890", None, self.REMAP, set()), ("96", "96850", None))

    def test_the_finest_resolved_level_names_the_levels_above_it(self) -> None:
        # A 시군구 that moved 시도 on its own pair (군위군 47720 → 27720) takes the 시도 with it.
        self.assertEqual(r.current_region("95", "95720", None, {"9572000000": "9472000000"}, set()),
                         ("94", "94720", None))
        self.assertEqual(r.current_region("97", "97170", "9717010100", self.REMAP, set()),
                         ("96", "96300", "9630010100"))

    def test_current_codes_stay_and_a_split_keeps_no_code_at_its_level(self) -> None:
        self.assertEqual(r.current_region("95", "95110", None, self.REMAP, set()), ("95", "95110", None))
        self.assertEqual(r.current_region("97", "97170", None, self.REMAP, {"9717000000"}), ("96", None, None))
        self.assertEqual(r.current_region(None, None, None, self.REMAP, set()), (None, None, None))
        self.assertEqual(r.current_region("9", "97x", None, self.REMAP, set()), (None, None, None))


class SidoNames(unittest.TestCase):
    def test_a_current_sido_is_named_by_its_own_row_else_its_smallest_current_code(self) -> None:
        names = r.sido_names([
            ("9600000000", "Southsynth City", "존재"),
            ("9630000000", "Southsynth City Westgu", "존재"),
            ("9700000000", "Oldsynth City", "폐지"),           # abolished: not a current name
            ("9512000000", "Midsynth City Eastgu", "존재"),    # no 9500000000 row (세종's shape)
            ("9511000000", "Midsynth Town", "존재"),
        ])
        self.assertEqual(names, {"95": "Midsynth", "96": "Southsynth"})


class ReconstructPnu(unittest.TestCase):
    def test_the_lot_is_kept_when_the_legal_code_moves(self) -> None:
        # The forward map changes only the 10-digit legal code; the lot (san·본번·부번) is kept.
        self.assertEqual(r.with_current_code("9999900100109120008", "9999900200"), "9999900200109120008")


class CandidatePnus(unittest.TestCase):
    def test_the_parcel_set_is_asked_about_the_current_form_and_the_earlier_forms_of_each_code(self) -> None:
        # Root ADR-0390: the run reads only these PNUs of the ~40M current parcels. Root ADR-0482 §1:
        # the served edition may predate the pair, so the source's own old form is asked too.
        asked = r.candidate_pnus(["9999900100109120008", "9999900300100010000", "9999902000100020000", None, ""],
                                 {"9999900100": "9999900200"}, {"9999902000"})
        self.assertEqual(asked, {"9999900200109120008", "9999900100109120008", "9999900300100010000"})

    def test_a_new_code_asks_about_the_old_codes_that_carry_to_it(self) -> None:
        # Synthetic merge: two old dongs (…301, …501) became one (…401). A source giving the new code asks the old forms.
        remap = {"9999930100": "9999940100", "9999950100": "9999940100"}
        self.assertEqual(r.candidate_pnus(["9999940100100070000"], remap, set()),
                         {"9999940100100070000", "9999930100100070000", "9999950100100070000"})


class ServedForms(unittest.TestCase):
    """Root ADR-0482 §1: a code's earlier forms, the source's own old code before its siblings."""

    def test_an_old_code_is_its_own_first_earlier_form_and_a_sibling_comes_second(self) -> None:
        remap = {"9999930100": "9999940100", "9999950100": "9999940100"}
        self.assertEqual(r.served_forms(remap), [
            ("9999930100", "9999930100", 1), ("9999930100", "9999950100", 2),
            ("9999940100", "9999930100", 2), ("9999940100", "9999950100", 2),
            ("9999950100", "9999950100", 1), ("9999950100", "9999930100", 2),
        ])

    def test_a_chain_lists_every_code_that_carries_to_the_current_one(self) -> None:
        remap = {"9999900100": "9999900300", "9999900200": "9999900300"}  # 100 -> 200 -> 300, flattened
        forms = {(code, form) for code, form, _ in r.served_forms(remap)}
        self.assertIn(("9999900300", "9999900100"), forms)
        self.assertIn(("9999900300", "9999900200"), forms)

    def test_no_pair_no_form(self) -> None:
        self.assertEqual(r.served_forms({}), [])


class CodeLotPnu(unittest.TestCase):
    """Root ADR-0382 §1: a source's legal-dong code and lot text assemble its pnu, prose never does."""

    def test_a_flat_and_a_mountain_lot_assemble(self) -> None:
        self.assertEqual(r.code_lot_pnu("9999910100", "66-8"), "9999910100100660008")
        self.assertEqual(r.code_lot_pnu("9999910100", "산 12"), "9999910100200120000")

    def test_prose_a_short_code_or_nothing_does_not(self) -> None:
        for code, lot in (("9999910100", "외 6필지"), ("9999910100", "31-27^31-28"), ("99999101", "1"),
                          ("99999101AB", "1"), ("9999910100", None), (None, "1")):
            self.assertIsNone(r.code_lot_pnu(code, lot), (code, lot))

    def test_the_crosswalk_of_rows_reads_only_pairs_in_force(self) -> None:
        rows = [{"kind": "pair", "old_code": "9999920100", "new_code": "9999910100", "source": "derived"},
                {"kind": "pair", "old_code": "9999930100", "new_code": "9999940100", "source": "derived"},
                {"kind": "supersession", "old_code": "9999930100", "new_code": "9999940100"}]
        self.assertEqual(r.crosswalk_of(rows), ({"9999920100": "9999910100"}, set()))


if __name__ == "__main__":
    unittest.main()
