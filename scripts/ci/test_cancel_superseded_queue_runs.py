#!/usr/bin/env python3
"""Only an active queue run whose branch is gone and whose commit is not on main is cancelled."""

from __future__ import annotations

import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location(
    "cancel_superseded_queue_runs", pathlib.Path(__file__).with_name("cancel_superseded_queue_runs.py")
)
cs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cs)

RUN = {
    "event": "merge_group",
    "status": "in_progress",
    "head_branch": "gh-readonly-queue/main/pr-7-0000000000000000000000000000000000000000",
    "head_sha": "1111111111111111111111111111111111111111",
}


def never(_: str) -> bool:
    raise AssertionError("not asked")


class CancelSupersededQueueRunsTest(unittest.TestCase):
    def test_a_dropped_commits_run_is_cancelled(self) -> None:
        self.assertTrue(cs.superseded(RUN, lambda _: False, lambda _: False))
        self.assertTrue(cs.superseded({**RUN, "status": "queued"}, lambda _: False, lambda _: False))

    def test_a_run_whose_queue_branch_exists_is_left_alone(self) -> None:
        self.assertFalse(cs.superseded(RUN, lambda _: True, never))

    def test_a_merged_commits_run_is_left_alone(self) -> None:
        # The host deploys a commit by the queue's check runs on it (root ADR-0167).
        self.assertFalse(cs.superseded(RUN, lambda _: False, lambda _: True))

    def test_the_branch_is_asked_before_main(self) -> None:
        asked: list[str] = []
        cs.superseded(RUN, lambda _: asked.append("branch") or False, lambda _: asked.append("main") or False)
        self.assertEqual(asked, ["branch", "main"])

    def test_other_events_and_finished_runs_are_never_asked_about(self) -> None:
        for other in ({"event": "pull_request"}, {"event": "push"}, {"status": "completed"}, {"head_branch": "main"}):
            self.assertFalse(cs.superseded({**RUN, **other}, never, never))


if __name__ == "__main__":
    unittest.main()
