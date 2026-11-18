#!/usr/bin/env python3
"""main is red when a job last failed on main itself, or for two pull requests in a row in the queue."""

from __future__ import annotations

import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location("main_health", pathlib.Path(__file__).with_name("main_health.py"))
mh = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mh)


def seen(at: int, outcome: str, source: str) -> dict:
    return {"at": f"2026-11-11T{at:02d}:00:00Z", "outcome": outcome, "source": source, "url": f"https://example.invalid/{at}"}


class RedTest(unittest.TestCase):
    def test_one_queue_failure_is_that_pull_requests_own(self) -> None:
        self.assertEqual(mh.red([seen(4, "fail", "pr-1")]), [])

    def test_the_same_pull_request_failing_twice_is_still_its_own(self) -> None:
        self.assertEqual(mh.red([seen(4, "fail", "pr-1"), seen(5, "fail", "pr-1")]), [])

    def test_two_pull_requests_failing_the_same_job_in_a_row_is_main(self) -> None:
        # 2026-11-11 04:51Z and 05:11Z: #1281 and #1255 both failed foundation-jobs after #1268.
        self.assertEqual(len(mh.red([seen(4, "fail", "pr-1281"), seen(5, "fail", "pr-1255")])), 2)

    def test_a_success_between_two_failures_clears_the_first(self) -> None:
        self.assertEqual(mh.red([seen(4, "fail", "pr-1"), seen(5, "pass", "pr-2"), seen(6, "fail", "pr-3")]), [])

    def test_a_later_success_clears_it(self) -> None:
        self.assertEqual(mh.red([seen(4, "fail", "pr-1"), seen(5, "fail", "pr-2"), seen(7, "pass", "pr-9")]), [])
        self.assertEqual(mh.red([seen(4, "fail", "main"), seen(7, "pass", "pr-9")]), [])

    def test_one_failure_on_main_itself_is_red(self) -> None:
        self.assertEqual(len(mh.red([seen(17, "fail", "main")])), 1)
        self.assertEqual(len(mh.red([seen(3, "pass", "pr-1"), seen(17, "fail", "main")])), 1)


class SourceTest(unittest.TestCase):
    def test_a_queue_run_speaks_for_its_pull_request(self) -> None:
        run = {"event": "merge_group", "head_branch": "gh-readonly-queue/main/pr-1281-b55864aa0000"}
        self.assertEqual(mh.source(run), "pr-1281")

    def test_the_nightly_a_manual_and_a_push_run_on_main_speak_for_main(self) -> None:
        for event in ("schedule", "workflow_dispatch", "push"):
            self.assertEqual(mh.source({"event": event, "head_branch": "main"}), "main")

    def test_a_pull_request_run_and_a_manual_run_on_a_branch_say_nothing(self) -> None:
        self.assertIsNone(mh.source({"event": "pull_request", "head_branch": "main"}))
        self.assertIsNone(mh.source({"event": "workflow_dispatch", "head_branch": "feature"}))


class FailedJobsTest(unittest.TestCase):
    def test_only_a_job_with_a_failed_step_is_a_verdict(self) -> None:
        jobs = [
            {"name": "Rust quality", "conclusion": "failure", "steps": [{"conclusion": "success"}, {"conclusion": "failure"}]},
            {"name": "Spark", "conclusion": "failure", "steps": []},  # the runner died (root ADR-0421)
            {"name": "required/foundation", "conclusion": "failure", "steps": [{"conclusion": "failure"}]},
            {"name": "Docs", "conclusion": "success", "steps": [{"conclusion": "success"}]},
        ]
        self.assertEqual(mh.failed_jobs(jobs), ["Rust quality"])


class RenderTest(unittest.TestCase):
    def test_the_issue_names_each_job_and_its_runs(self) -> None:
        body = mh.render({("Foundation Platform CI", "Rust quality"): [seen(4, "fail", "pr-1"), seen(5, "fail", "pr-2")]})
        self.assertIn("| Foundation Platform CI | Rust quality | [pr-1](https://example.invalid/4), [pr-2](https://example.invalid/5) |", body)
        self.assertIn("hotfix:main-red", body)


if __name__ == "__main__":
    unittest.main()
