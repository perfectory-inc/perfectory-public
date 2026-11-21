#!/usr/bin/env python3
"""Self-test of the daily repository drift and Renovate pulse check (root ADR-0570)."""

from __future__ import annotations

import contextlib
import copy
import io
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import repository_drift as drift  # noqa: E402
from repository_drift import POLICY, RULESETS, Unreadable, renovate_silent, ruleset_drift, settings_drift  # noqa: E402

NOW = datetime(2026, 11, 18, 12, 0, 0, tzinfo=timezone.utc)


def declared(name: str) -> dict:
    return json.loads((POLICY / name).read_text(encoding="utf-8"))


def live(policy: dict, **changes) -> dict:
    """The ruleset as the API returns it to a reader: no bypass_actors, extra read-only keys."""
    value = copy.deepcopy(policy)
    value.pop("bypass_actors", None)
    value.update({"id": 1, "source_type": "Repository", "source": "x/y", "created_at": "t", "updated_at": "t"})
    value.update(changes)
    return value


def policies() -> list[dict]:
    return [declared(name) for name in RULESETS]


def summaries(rulesets: list[dict]) -> list[dict]:
    return [{key: ruleset[key] for key in ("name", "target", "enforcement", "source_type")} for ruleset in rulesets]


class SettingsTest(unittest.TestCase):
    def readable(self) -> dict:
        settings = declared("repository-settings.json")
        return {key: settings[key] for key in drift.SETTINGS_READABLE}

    def test_issues_turned_on_is_drift(self) -> None:
        # 2026-10-22..11-18 the other way: has_issues false while repository-settings.json said true.
        # Issues stay off since 2026-11-21 (root ADR-0554 5); either difference is drift.
        found = settings_drift(declared("repository-settings.json"), {**self.readable(), "has_issues": True})
        self.assertEqual(len(found), 1)
        self.assertIn("has_issues", found[0])

    def test_a_cleared_description_is_drift(self) -> None:
        self.assertIn("description", settings_drift(declared("repository-settings.json"), {**self.readable(), "description": None})[0])

    def test_what_the_token_cannot_read_is_not_compared(self) -> None:
        self.assertEqual(settings_drift(declared("repository-settings.json"), {**self.readable(), "allow_auto_merge": None}), [])


class RulesetTest(unittest.TestCase):
    def test_the_declared_rulesets_as_a_reader_sees_them_match(self) -> None:
        rulesets = [live(policy) for policy in policies()]
        self.assertEqual(ruleset_drift(policies(), summaries(rulesets), rulesets), [])

    def test_a_merge_queue_left_on_headgreen_is_drift(self) -> None:
        # 2026-11-12..18: ADR-0552 chose ALLGREEN, the live queue stayed HEADGREEN.
        rulesets = [live(policy) for policy in policies()]
        for rule in rulesets[0]["rules"]:
            if rule["type"] == "merge_queue":
                rule["parameters"]["grouping_strategy"] = "HEADGREEN"
        self.assertEqual(
            ruleset_drift(policies(), summaries(rulesets), rulesets),
            ["ruleset `perfectory-public-main`: `rules` differs from tools/github"],
        )

    def test_an_extra_or_inherited_ruleset_is_drift(self) -> None:
        rulesets = [live(policy) for policy in policies()]
        extra = summaries(rulesets) + [{"name": "org-wide", "target": "branch", "enforcement": "active", "source_type": "Organization"}]
        self.assertIn("set of rulesets", ruleset_drift(policies(), extra, rulesets)[0])

    def test_a_missing_or_doubled_ruleset_is_drift(self) -> None:
        rulesets = [live(policy) for policy in policies()]
        self.assertTrue(any("found 0" in line for line in ruleset_drift(policies(), summaries(rulesets[1:]), rulesets[1:])))
        doubled = rulesets + [rulesets[2]]
        self.assertTrue(any("found 2" in line for line in ruleset_drift(policies(), summaries(doubled), doubled)))

    def test_enforcement_off_is_drift(self) -> None:
        rulesets = [live(policy) for policy in policies()]
        rulesets[1]["enforcement"] = "disabled"
        found = ruleset_drift(policies(), summaries(rulesets), rulesets)
        self.assertTrue(any("`enforcement`" in line for line in found))


class PulseTest(unittest.TestCase):
    def test_an_open_pull_request_is_a_pulse(self) -> None:
        self.assertIsNone(renovate_silent([{"state": "OPEN", "createdAt": "2026-11-01T00:00:00Z"}], NOW))

    def test_a_recently_opened_pull_request_is_a_pulse(self) -> None:
        self.assertIsNone(renovate_silent([{"state": "MERGED", "createdAt": "2026-11-17T13:00:00Z"}], NOW))

    def test_never_and_two_days_old_are_silence(self) -> None:
        # 2026-10-22..11-18: no Renovate pull request at all.
        self.assertIn("last opened: never", renovate_silent([], NOW) or "")
        self.assertIsNotNone(renovate_silent([{"state": "CLOSED", "createdAt": "2026-11-16T11:59:59Z"}], NOW))


class ReportTest(unittest.TestCase):
    """The job's verdict, which the host's service checks carry to Slack (issues are off, 2026-11-21)."""

    def run_report(self, result: str | None, findings: str) -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as scratch:
            summary = Path(scratch) / "summary.md"
            summary.write_text(findings, encoding="utf-8")
            if result is not None:
                summary.with_suffix(".result").write_text(result + "\n", encoding="utf-8")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                code = drift.report(summary)
            return code, out.getvalue()

    def test_findings_fail_the_job_with_a_count_and_one_annotation_each(self) -> None:
        code, out = self.run_report("failure", "- has_issues: 100% off\n- could not read rulesets: 502\n")
        self.assertEqual(code, 1)
        self.assertIn("::error title=repository-drift-count::1\n", out)
        self.assertIn("::error title=repository-drift::has_issues: 100%25 off\n", out)
        self.assertNotIn("title=repository-drift::could not read", out)

    def test_github_keeps_ten_error_annotations_a_step(self) -> None:
        _, out = self.run_report("failure", "".join(f"- finding {n}\n" for n in range(12)))
        self.assertEqual(out.count("::error title=repository-drift::"), drift.FINDING_ANNOTATIONS)
        self.assertIn("::error title=repository-drift-count::12", out)

    def test_clear_passes(self) -> None:
        self.assertEqual(self.run_report("success", "")[0], 0)

    def test_unreadable_and_missing_are_no_verdict(self) -> None:
        for result in ("unreadable", None):
            code, out = self.run_report(result, "")
            self.assertEqual(code, 1)
            self.assertNotIn("repository-drift-count", out)


class CheckTest(unittest.TestCase):
    def test_one_unreadable_signal_keeps_the_others(self) -> None:
        rulesets = [live(policy) for policy in policies()]
        rulesets[0]["enforcement"] = "evaluate"
        settings = {key: declared("repository-settings.json")[key] for key in drift.SETTINGS_READABLE}

        def fake(*args: str) -> str:
            if args[0] == "api" and args[1] == "repos/o/r":
                return json.dumps(settings)
            if args[0] == "api" and "rulesets?" in args[1]:
                return json.dumps(summaries(rulesets) and [dict(s, id=i) for i, s in enumerate(summaries(rulesets))])
            if args[0] == "api" and "/rulesets/" in args[1]:
                return json.dumps(rulesets[int(args[1].rsplit("/", 1)[1])])
            raise Unreadable("planted")

        with tempfile.TemporaryDirectory() as scratch:
            summary = Path(scratch) / "summary.md"
            with mock.patch.object(drift, "gh", fake), mock.patch.dict(os.environ, {"GH_REPO": "o/r"}):
                self.assertEqual(drift.check(summary, "", NOW), 0)
            self.assertEqual(summary.with_suffix(".result").read_text(encoding="utf-8").strip(), "failure")
            text = summary.read_text(encoding="utf-8")
            self.assertIn("`enforcement`", text)
            self.assertIn("could not read Renovate pulse", text)

    def test_a_cargo_advisory_with_a_silent_renovate_is_a_finding(self) -> None:
        def fake(*args: str) -> str:
            if args[0] == "pr":
                return "[]"
            raise Unreadable("planted")

        with tempfile.TemporaryDirectory() as scratch:
            summary = Path(scratch) / "summary.md"
            with mock.patch.object(drift, "gh", fake), mock.patch.dict(os.environ, {"GH_REPO": "o/r"}):
                drift.check(summary, "failure", NOW)
            self.assertEqual(summary.with_suffix(".result").read_text(encoding="utf-8").strip(), "failure")
            self.assertIn("last opened: never", summary.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
