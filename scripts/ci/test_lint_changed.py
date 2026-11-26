#!/usr/bin/env python3
"""The ratchet of scripts/ci/lint_changed.py: what fails a file, and that the baseline only shrinks."""
import unittest

import lint_changed as lint


class Ratchet(unittest.TestCase):
    def test_findings_are_counted_per_file_and_rule(self):
        counted = lint.count([("b.sh", "SC2312"), ("a.sh", "SC2086"), ("b.sh", "SC2312"), ("b.sh", "SC2034")])
        self.assertEqual(counted, {"a.sh": {"SC2086": 1}, "b.sh": {"SC2034": 1, "SC2312": 2}})
        self.assertEqual(list(counted), ["a.sh", "b.sh"])

    def test_a_file_at_or_under_its_baseline_passes(self):
        self.assertEqual(lint.over({"a.sh": {"SC2312": 2}}, {"a.sh": {"SC2312": 2}}), [])
        self.assertEqual(lint.over({"a.sh": {"SC2312": 1}}, {"a.sh": {"SC2312": 2}}), [])

    def test_one_more_finding_of_a_rule_fails(self):
        self.assertEqual(lint.over({"a.sh": {"SC2312": 3}}, {"a.sh": {"SC2312": 2}}), ["a.sh: SC2312 x3 (baseline 2)"])

    def test_a_fixed_rule_does_not_pay_for_another(self):
        # Two SC2312 fixed and one SC2086 added is still a new finding.
        self.assertEqual(lint.over({"a.sh": {"SC2086": 1}}, {"a.sh": {"SC2312": 2}}), ["a.sh: SC2086 x1 (baseline 0)"])

    def test_a_file_the_baseline_does_not_name_must_be_clean(self):
        self.assertEqual(lint.over({"new.py": {"F401": 1}}, {}), ["new.py: F401 x1 (baseline 0)"])
        self.assertEqual(lint.over({}, {}), [])

    def test_the_baseline_may_not_gain_an_entry_or_a_count(self):
        base = {"shellcheck": {"a.sh": {"SC2312": 2}}, "ruff": {}}
        self.assertEqual(lint.grown(base, {"shellcheck": {"a.sh": {"SC2312": 1}}, "ruff": {}}), [])
        self.assertEqual(lint.grown(base, {"shellcheck": {}, "ruff": {}}), [])
        self.assertEqual(lint.grown(base, {"shellcheck": {"a.sh": {"SC2312": 3}}, "ruff": {}}),
                         ["shellcheck: a.sh: SC2312 2 -> 3"])
        self.assertEqual(lint.grown(base, {"shellcheck": {"a.sh": {"SC2312": 2}}, "ruff": {"b.py": {"F401": 1}}}),
                         ["ruff: b.py: F401 0 -> 1"])

    def test_the_change_that_brings_a_tool_in_starts_its_section(self):
        head = {"squawk": {"m.sql": {"ban-drop-column": 1}}}
        self.assertEqual(lint.grown({"shellcheck": {}, "ruff": {}}, head), [])
        self.assertEqual(lint.grown({"shellcheck": {}, "ruff": {}, "squawk": {}}, head),
                         ["squawk: m.sql: ban-drop-column 0 -> 1"])

    def test_a_checked_file_that_improved_must_lower_its_baseline(self):
        allowed = {"a.sh": {"SC2312": 2}, "untouched.sh": {"SC2312": 9}}
        self.assertEqual(lint.stale({"a.sh": {"SC2312": 1}}, allowed, ["a.sh"]), ["a.sh: SC2312 baseline 2, now 1"])
        self.assertEqual(lint.stale({}, allowed, ["a.sh"]), ["a.sh: SC2312 baseline 2, now 0"])
        # A file this run did not check says nothing about its entry.
        self.assertEqual(lint.stale({"a.sh": {"SC2312": 2}}, allowed, ["a.sh"]), [])

    def test_an_entry_for_a_file_that_is_gone_fails(self):
        baseline = {"shellcheck": {"kept.sh": {"SC2312": 1}, "deleted.sh": {"SC2312": 4}}, "ruff": {"old.py": {"F401": 1}}}
        self.assertEqual(lint.gone(baseline, {"shellcheck": ["kept.sh"], "ruff": []}),
                         ["shellcheck: deleted.sh", "ruff: old.py"])
        self.assertEqual(lint.gone(baseline, {"shellcheck": ["kept.sh", "deleted.sh"], "ruff": ["old.py"]}), [])

    def test_the_settings_pins_baseline_and_checker_are_whole_tree_inputs(self):
        for path in (".shellcheckrc", "ruff.toml", ".squawk.toml", "tools/shellcheck.env", "tools/ruff.env", "tools/squawk.env",
                     "tools/pinned-artifacts.json", "tools/ci/lint-baseline.json", "scripts/ci/lint_changed.py"):
            self.assertIn(path, lint.WHOLE_TREE_INPUTS)

    def test_a_missing_baseline_is_empty_for_every_tool(self):
        self.assertEqual(lint.load_baseline("{}"), {"shellcheck": {}, "ruff": {}, "squawk": {}})

    def test_squawk_reads_the_foundation_migrations_only(self):
        self.assertEqual(lint.MIGRATIONS, "platforms/foundation-platform/migrations/")


if __name__ == "__main__":
    unittest.main()
