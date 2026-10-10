#!/usr/bin/env python3
"""Self-test for the ADR index check: each refusal is planted and must be refused (root ADR-0176)."""

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("render-adr-index.py")
SPEC = importlib.util.spec_from_file_location("render_adr_index", MODULE_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)

GOOD = "# ADR 0001: 첫 결정\n\n- Status: Accepted\n"


class AdrIndexCheckTests(unittest.TestCase):
    def check(self, files: dict[str, str]) -> tuple[list, list[str]]:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            (directory / "README.md").write_text("# 목록\n", encoding="utf-8")
            for name, text in files.items():
                (directory / name).write_text(text, encoding="utf-8")
            return MODULE.read_adrs(directory)

    def test_the_repository_passes(self) -> None:
        entries, problems = MODULE.read_adrs(MODULE.ADR_DIR)
        self.assertEqual(problems, [])
        self.assertTrue(entries)

    def test_a_well_formed_directory_passes_and_lists_titles(self) -> None:
        entries, problems = self.check(
            {
                "0001-first.md": GOOD,
                "0002-second.md": "---\nstatus: current\n---\n\n# ADR-0002 — 두 번째 결정\n",
            }
        )
        self.assertEqual(problems, [])
        self.assertEqual([(n, t) for n, t, _ in entries], [("0001", "첫 결정"), ("0002", "두 번째 결정")])

    def test_two_files_with_one_number_are_refused(self) -> None:
        # The collision two parallel PRs produce when both take the next free number.
        _, problems = self.check(
            {"0001-first.md": GOOD, "0001-another.md": GOOD.replace("첫", "다른")}
        )
        self.assertTrue(any("taken by 2 files" in problem for problem in problems), problems)

    def test_a_heading_with_another_number_is_refused(self) -> None:
        _, problems = self.check({"0002-second.md": GOOD})
        self.assertTrue(any("heading says ADR 0001" in problem for problem in problems), problems)

    def test_a_missing_heading_is_refused(self) -> None:
        _, problems = self.check({"0001-first.md": "본문만 있다\n"})
        self.assertTrue(any("first heading" in problem for problem in problems), problems)

    def test_a_badly_named_file_is_refused(self) -> None:
        _, problems = self.check({"1-First.md": GOOD})
        self.assertTrue(any("file name" in problem for problem in problems), problems)


if __name__ == "__main__":
    unittest.main()
