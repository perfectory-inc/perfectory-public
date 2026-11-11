"""The onbid closed-owed seed (scripts/ops/onbid_closed_owed_seed.py, root ADR-0542 §3).

2026-11-11: 2,569 items had left the item list before the 물건상세 backlog reached them, and the
collector had forgotten them with the list. The seed reads them once from Silver as the collector's
item keys; these tests pin the keys it writes, that a malformed row is no key, and that
onbid-collect.sh reads it only until one run took it. Item numbers are synthetic.
"""

import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

ORCHESTRATION = pathlib.Path(__file__).resolve().parents[1]
PLATFORM = ORCHESTRATION.parent
sys.path.insert(0, str(PLATFORM / "scripts/ops"))

import onbid_closed_owed_seed as seed  # noqa: E402


class SeedTest(unittest.TestCase):
    def test_it_writes_the_collectors_item_keys(self):
        rows = [["9999-0000-000001", "77"], ["9999-0000-000002", "78"]]
        with tempfile.TemporaryDirectory() as folder, mock.patch.object(seed, "trino", return_value=rows):
            out = pathlib.Path(folder) / "seed.json"
            self.assertEqual(seed.main(["seed", str(out)]), 0)
            self.assertEqual(json.loads(out.read_text(encoding="utf-8")), [
                {"cltr_mng_no": "9999-0000-000001", "pbct_cdtn_no": 77},
                {"cltr_mng_no": "9999-0000-000002", "pbct_cdtn_no": 78},
            ])

    def test_a_row_without_both_parts_is_no_key(self):
        self.assertEqual(seed.keys([["", "1"], ["9999-0000-000003", ""], ["9999-0000-000004", "x"], ["only"]]), [])

    def test_it_asks_for_items_with_no_detail_through_their_last_round(self):
        self.assertIn("NOT EXISTS", seed.SQL)
        self.assertIn(seed.DETAILS, seed.SQL)
        self.assertIn("max_by(i.pbct_cdtn_no, i.ingested_at_utc)", seed.SQL)

    def test_the_collect_reads_it_until_one_run_took_it(self):
        script = (PLATFORM / "scripts/ops/onbid-collect.sh").read_text(encoding="utf-8")
        self.assertIn("onbid_closed_owed_seed.py", script)
        self.assertIn('if [ ! -e "${seed_done}" ]', script)
        self.assertIn("_CLOSED_OWED_SEED=", script)
        self.assertIn('> "${seed_done}"', script)


if __name__ == "__main__":
    unittest.main()
