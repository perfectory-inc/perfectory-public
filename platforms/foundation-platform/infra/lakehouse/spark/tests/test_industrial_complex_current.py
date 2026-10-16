"""One rule picks a complex's current row of the append-only `silver.industrial_complexes`.

On 2026-10-16 `complex_boundary_release` refused its supervised run: the table holds every complex
three times (the same snapshot loaded 2026-08-18 and twice on 2026-10-01), and the boundary loader
had no rule while the catalog Gold had its own. Both read `industrial_complex_current` now.
"""

import sys
import unittest
from pathlib import Path

JOBS = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS))

import industrial_complex_current as current  # noqa: E402


def row(code, ingested, valid_from="2025-06-01", valid_to=None, complex_id=None):
    return {"complex_id": complex_id or f"id-{code}", "official_complex_code": code,
            "valid_from_utc": valid_from, "valid_to_utc": valid_to, "ingested_at_utc": ingested}


class CurrentRow(unittest.TestCase):
    def test_the_latest_load_of_one_snapshot_is_current(self):
        rows = [row("990001", "2099-08-18"), row("990001", "2099-10-01T03"), row("990001", "2099-10-01T05")]
        self.assertEqual([r["ingested_at_utc"] for r in current.pick_current(rows)], ["2099-10-01T05"])

    def test_a_later_valid_from_beats_a_later_load_and_a_closed_row_is_never_current(self):
        rows = [row("990002", "2099-10-01", valid_from="2099-01-01"),
                row("990002", "2099-09-01", valid_from="2099-06-01"),
                row("990002", "2099-12-01", valid_from="2099-12-01", valid_to="2099-12-02")]
        (picked,) = current.pick_current(rows)
        self.assertEqual(picked["valid_from_utc"], "2099-06-01")

    def test_a_missing_time_sorts_last(self):
        rows = [row("990003", None), row("990003", "2099-01-01")]
        (picked,) = current.pick_current(rows)
        self.assertEqual(picked["ingested_at_utc"], "2099-01-01")

    def test_both_readers_take_the_shared_rule(self):
        for name in ("industrial_complex_silver_to_gold.py", "industrial_complex_boundaries_handoff_to_silver.py"):
            with self.subTest(job=name):
                text = (JOBS / name).read_text(encoding="utf-8")
                self.assertIn("from industrial_complex_current import current_complex_rows", text)
                self.assertIn("current_complex_rows(", text.split("import current_complex_rows", 1)[1])


if __name__ == "__main__":
    unittest.main()
