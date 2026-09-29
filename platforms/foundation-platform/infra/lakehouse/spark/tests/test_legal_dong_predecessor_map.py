import sys
import unittest
from pathlib import Path


JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import parcel_lineage as pl  # noqa: E402
from legal_dong_predecessor_map import predecessor_map  # noqa: E402

# Reserved 99999 range. A rural 면 (99999-101) with two 리 was renumbered to 99999-201.
CODES = pl.parse_code_list(
    "\n".join(
        [
            "법정동코드\t법정동명\t폐지여부",
            "9999910100\t합성도 가군 갑면\t폐지",
            "9999910121\t합성도 가군 갑면 일리\t폐지",
            "9999910122\t합성도 가군 갑면 이리\t폐지",
            "9999920100\t합성시 가군 갑면\t존재",
            "9999920121\t합성시 가군 갑면 일리\t존재",
            "9999920122\t합성시 가군 갑면 이리\t존재",
        ]
    )
)


def pnu(code, main):
    return f"{code}1{main:04d}0000"


class PredecessorMapTest(unittest.TestCase):
    def test_ri_pairs_roll_up_to_their_myeon(self):
        before = {pnu("9999910121", n) for n in range(1, 4)} | {pnu("9999910122", n) for n in range(1, 3)}
        after = {pnu("9999920121", n) for n in range(1, 4)} | {pnu("9999920122", n) for n in range(1, 3)}
        lots_before, lots_after = pl.lots_by_dong(before), pl.lots_by_dong(after)
        pairing = pl.pair_legal_dongs(CODES, lots_before, lots_before, lots_after)
        mapping = predecessor_map(pairing, lots_before)
        self.assertEqual(mapping["9999920121"], "9999910121")
        self.assertEqual(mapping["9999920100"], "9999910100", "the 면 the boundary layer draws gets a predecessor too")

    def test_unchanged_dongs_are_not_listed(self):
        codes = pl.parse_code_list("법정동코드\t법정동명\t폐지여부\n9999910121\t합성도 가군 갑면 일리\t존재")
        before = {pnu("9999910121", 1)}
        pairing = pl.pair_legal_dongs(codes, pl.lots_by_dong(before), pl.lots_by_dong(before), pl.lots_by_dong(before))
        self.assertEqual(predecessor_map(pairing, pl.lots_by_dong(before)), {})


if __name__ == "__main__":
    unittest.main()
