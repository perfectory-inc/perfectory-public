"""What the bake scripts read of a lane line (by_pnu_kinds.lane_fields). Pure Python, the real contract."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import by_pnu_kinds  # noqa: E402


class LaneFieldsTest(unittest.TestCase):
    def test_a_building_kind_and_only_it_reads_the_building_links(self):
        # Root ADR-0252: a building kind's bake checks its units' links, so its script resolves the
        # runtime database; the publisher derives the same from the host.
        for lane, kind in by_pnu_kinds.kinds().items():
            with self.subTest(lane=lane):
                expected = "true" if kind.host == "building_by_pnu_gateway" else "false"
                self.assertEqual(by_pnu_kinds.lane_fields(lane)["reads_building_links"], expected)
        self.assertEqual(by_pnu_kinds.lane_fields("unit-price")["reads_building_links"], "false")

    def test_a_kind_keyed_by_a_suffix_is_one_row_per_pnu_and_suffix(self):
        self.assertEqual(by_pnu_kinds.kind("building-units").row_key, ("pnu", "building_id"))
        self.assertEqual(by_pnu_kinds.kind("building-titles").row_key, ("pnu",))


if __name__ == "__main__":
    unittest.main()
