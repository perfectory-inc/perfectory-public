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
        self.assertEqual(r.current_code("4159711100", {}), ("4159711100", False))

    def test_a_chain_follows_old_to_current(self) -> None:
        pairs = {"4159013600": ["4159011000"], "4159011000": ["4159711100"]}
        self.assertEqual(r.current_code("4159013600", pairs), ("4159711100", False))

    def test_a_split_is_ambiguous_not_a_guess(self) -> None:
        self.assertEqual(r.current_code("4159013600", {"4159013600": ["4159711100", "4159712100"]}), (None, True))

    def test_a_cycle_is_ambiguous(self) -> None:
        self.assertEqual(r.current_code("A", {"A": ["B"], "B": ["A"]}), (None, True))


class Crosswalk(unittest.TestCase):
    def test_only_changed_codes_enter_and_a_split_is_flagged(self) -> None:
        remap, ambiguous = r.code_crosswalk([
            ("4159013600", "4159711100"),                                 # 화성시 장지동 → 동탄구
            ("2638010300", "2638010300"),                                 # unchanged: not in the remap
            ("4111000000", "4111000001"), ("4111000000", "4111000002"),   # a split → steward
            ("", "4159711100"), ("4159013600", ""),                       # blanks are dropped
        ])
        self.assertEqual(remap, {"4159013600": "4159711100"})
        self.assertEqual(ambiguous, {"4111000000"})


class ReconstructPnu(unittest.TestCase):
    def test_the_lot_is_kept_when_the_legal_code_moves(self) -> None:
        # The forward map changes only the 10-digit legal code; the lot (san·본번·부번) is kept.
        self.assertEqual(r.with_current_code("4159013600109120008", "4159711100"), "4159711100109120008")


if __name__ == "__main__":
    unittest.main()
