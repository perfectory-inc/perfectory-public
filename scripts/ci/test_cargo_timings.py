#!/usr/bin/env python3
"""The per-crate build-time summary (root ADR-0269)."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import cargo_timings as ct  # noqa: E402


def report(*units: tuple[str, float]) -> str:
    data = [{"i": i, "name": name, "mode": "todo", "duration": secs} for i, (name, secs) in enumerate(units)]
    return f"<script>\nconst UNIT_DATA = {json.dumps(data)};\nconst CONCURRENCY_DATA = [];\n</script>"


class SlowestTest(unittest.TestCase):
    def test_a_crates_units_add_up_across_builds_and_rank_slowest_first(self) -> None:
        clippy = report(("duckdb", 40.0), ("foundation-api", 12.5), ("serde", 3.0))
        tests = report(("foundation-api", 30.0), ("foundation-api", 2.5), ("serde", 0.5))
        self.assertEqual(
            ct.slowest([clippy, tests], top=2),
            [("foundation-api", 45.0, 3), ("duckdb", 40.0, 1)],
        )

    def test_a_report_without_unit_data_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            ct.units("<html>no data</html>")

    def test_the_table_is_markdown(self) -> None:
        text = ct.table("t", [("a", 1.5, 2)])
        self.assertIn("| `a` | 1.5 | 2 |", text)


if __name__ == "__main__":
    unittest.main()
