#!/usr/bin/env python3
"""Only a run whose failed jobs reached no failed step is re-run, once, outside the merge queue."""

from __future__ import annotations

import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location(
    "rerun_runner_failures", pathlib.Path(__file__).with_name("rerun_runner_failures.py")
)
rr = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rr)


def job(conclusion: str, steps: list[str]) -> dict:
    return {"conclusion": conclusion, "steps": [{"conclusion": s} for s in steps]}


RUN = {"conclusion": "failure", "run_attempt": 1, "event": "pull_request"}


class RerunRunnerFailuresTest(unittest.TestCase):
    def test_a_job_that_failed_with_no_failed_step_is_re_run(self) -> None:
        self.assertTrue(rr.should_rerun(RUN, [job("failure", []), job("success", ["success"])]))

    def test_a_failed_step_is_a_real_failure(self) -> None:
        self.assertFalse(rr.should_rerun(RUN, [job("failure", ["success", "failure"])]))

    def test_one_real_failure_beside_a_runner_failure_is_not_re_run(self) -> None:
        self.assertFalse(rr.should_rerun(RUN, [job("failure", []), job("failure", ["failure"])]))

    def test_a_run_is_re_run_at_most_once(self) -> None:
        self.assertFalse(rr.should_rerun({**RUN, "run_attempt": 2}, [job("failure", [])]))

    def test_the_merge_queue_has_already_removed_the_entry(self) -> None:
        self.assertFalse(rr.should_rerun({**RUN, "event": "merge_group"}, [job("failure", [])]))

    def test_a_cancelled_or_green_run_is_left_alone(self) -> None:
        self.assertFalse(rr.should_rerun({**RUN, "conclusion": "cancelled"}, [job("failure", [])]))
        self.assertFalse(rr.should_rerun(RUN, [job("success", ["success"])]))


if __name__ == "__main__":
    unittest.main()
