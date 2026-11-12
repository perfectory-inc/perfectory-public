#!/usr/bin/env python3
"""A labelled fix goes to the front without removing anyone else; everything else queues at the back."""

from __future__ import annotations

import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location(
    "merge_queue_enqueue", pathlib.Path(__file__).with_name("merge_queue_enqueue.py")
)
mq = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mq)


def pull_request(*labels: str, entry: dict | None = None, **other) -> dict:
    return {
        "id": "PR_x",
        "state": "OPEN",
        "isDraft": False,
        "labels": {"nodes": [{"name": label} for label in labels]},
        "mergeQueueEntry": entry,
        **other,
    }


class MergeQueueEnqueueTest(unittest.TestCase):
    def test_an_unlabelled_pull_request_queues_at_the_back(self) -> None:
        self.assertEqual(mq.decide(pull_request("bug"))[0], ["enqueue"])

    def test_an_unlabelled_queued_pull_request_is_left_where_it_is(self) -> None:
        self.assertEqual(mq.decide(pull_request(entry={"jump": False, "position": 4}))[0], [])

    def test_each_front_label_queues_at_the_front(self) -> None:
        for label in mq.FRONT_LABELS:
            self.assertEqual(mq.decide(pull_request(label))[0], ["enqueue-front"])

    def test_a_labelled_pull_request_queued_at_the_back_moves_alone(self) -> None:
        steps, _ = mq.decide(pull_request("hotfix:main-red", entry={"jump": False, "position": 9}))
        self.assertEqual(steps, ["dequeue", "enqueue-front"])

    def test_a_jumped_entry_is_never_restarted(self) -> None:
        # 2026-11-11: #1293 was removed and re-added 14 seconds after its jump, restarting its build.
        self.assertEqual(mq.decide(pull_request("hotfix:deploy-stop", entry={"jump": True, "position": 1}))[0], [])

    def test_a_draft_or_closed_pull_request_is_never_queued(self) -> None:
        self.assertEqual(mq.decide(pull_request("hotfix:main-red", isDraft=True))[0], [])
        self.assertEqual(mq.decide(pull_request("hotfix:main-red", state="MERGED"))[0], [])

    def test_the_only_removal_is_of_the_pull_request_itself(self) -> None:
        source = pathlib.Path(mq.__file__).read_text(encoding="utf-8")
        self.assertEqual(source.count("dequeuePullRequest"), 1)
        self.assertEqual(source.count("graphql(DEQUEUE"), 1)
        self.assertIn('graphql(DEQUEUE, id=pull_request["id"])', source)


if __name__ == "__main__":
    unittest.main()
