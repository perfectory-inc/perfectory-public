#!/usr/bin/env python3
"""Self-test of the daily repository drift and Renovate pulse check (root ADR-0570)."""

from __future__ import annotations

import copy
import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from repository_drift import (  # noqa: E402
    POLICY,
    RULESETS,
    renovate_silent,
    ruleset_drift,
    settings_drift,
)

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


class SettingsTest(unittest.TestCase):
    def test_issues_turned_off_is_drift(self) -> None:
        # 2026-10-22..11-18: has_issues false while repository-settings.json said true.
        found = settings_drift(declared("repository-settings.json"), {"has_issues": False, "has_wiki": False})
        self.assertEqual(len(found), 1)
        self.assertIn("has_issues", found[0])

    def test_what_the_token_cannot_read_is_not_drift(self) -> None:
        # A reader gets null for the merge settings; null is not a value to compare.
        self.assertEqual(settings_drift(declared("repository-settings.json"), {"allow_auto_merge": None}), [])


class RulesetTest(unittest.TestCase):
    def test_the_declared_rulesets_as_a_reader_sees_them_match(self) -> None:
        policies = [declared(name) for name in RULESETS]
        self.assertEqual(ruleset_drift(policies, [live(policy) for policy in policies]), [])

    def test_a_merge_queue_left_on_headgreen_is_drift(self) -> None:
        # 2026-11-12..18: ADR-0552 chose ALLGREEN, the live queue stayed HEADGREEN.
        policy = declared("main-ruleset.json")
        changed = live(policy)
        for rule in changed["rules"]:
            if rule["type"] == "merge_queue":
                rule["parameters"]["grouping_strategy"] = "HEADGREEN"
        found = ruleset_drift([policy], [changed])
        self.assertEqual(found, ["ruleset `perfectory-public-main`: `rules` differs from tools/github"])

    def test_a_missing_or_doubled_ruleset_is_drift(self) -> None:
        policy = declared("tag-firewall.json")
        self.assertIn("found 0", ruleset_drift([policy], [])[0])
        self.assertIn("found 2", ruleset_drift([policy], [live(policy), live(policy)])[0])

    def test_enforcement_off_is_drift(self) -> None:
        policy = declared("non-main-branch-firewall.json")
        self.assertIn("`enforcement`", ruleset_drift([policy], [live(policy, enforcement="disabled")])[0])


class PulseTest(unittest.TestCase):
    def test_a_recent_pull_request_is_a_pulse(self) -> None:
        self.assertIsNone(renovate_silent([{"updatedAt": "2026-11-17T13:00:00Z"}], [], NOW))

    def test_a_recent_dashboard_is_a_pulse(self) -> None:
        issues = [{"title": "Dependency Dashboard", "updatedAt": "2026-11-18T11:00:00Z"}]
        self.assertIsNone(renovate_silent([], issues, NOW))

    def test_another_issue_by_renovate_is_not_its_dashboard(self) -> None:
        issues = [{"title": "Action Required: Fix Renovate Configuration", "updatedAt": "2026-11-18T11:00:00Z"}]
        self.assertIsNotNone(renovate_silent([], issues, NOW))

    def test_never_and_two_days_old_are_silence(self) -> None:
        # 2026-10-22..11-18: no Renovate pull request at all.
        self.assertIn("last: never", renovate_silent([], [], NOW) or "")
        self.assertIsNotNone(renovate_silent([{"updatedAt": "2026-11-16T11:59:59Z"}], [], NOW))


if __name__ == "__main__":
    unittest.main()
