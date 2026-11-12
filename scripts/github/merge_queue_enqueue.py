#!/usr/bin/env python3
"""Put a pull request in main's merge queue; one that repairs main or the deploy goes to the front
(root ADR-0553).

  merge_queue_enqueue.py <pull request number> [--dry-run] [--repository OWNER/NAME]

This is the one way a pull request enters the queue. A pull request carrying `hotfix:main-red` or
`hotfix:deploy-stop` is enqueued with `jump: true`, GitHub's own "jump the queue". Nobody else's
entry is removed: on 2026-11-11 the queue was emptied three times to put a fix first (#1316, #1337,
#1337 with #1342), each time dropping every running queue build and making up to 19 pull requests
wait for a re-enqueue.

What it does for a labelled pull request:
- not queued: enqueues it at the front;
- queued without the jump: removes THIS entry and enqueues it at the front;
- queued with the jump: nothing. Removing and re-adding a jumped entry restarts its build (on
  2026-11-11 that was done to #1293 14 seconds after its jump, which had already taken effect).
It then leaves a comment on the pull request naming the label, so a jump is on record.

An unlabelled pull request is enqueued at the back, and nothing happens when it is already queued.
GitHub itself refuses a draft, a conflicted pull request, or one whose required checks have not
passed; this script adds no bypass, and the queue still runs every required check on it.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

FRONT_LABELS = ("hotfix:main-red", "hotfix:deploy-stop")

QUERY = """
query($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      id number state isDraft
      labels(first: 50) { nodes { name } }
      mergeQueueEntry { jump position state }
    }
  }
}
"""
DEQUEUE = "mutation($id: ID!) { dequeuePullRequest(input: {id: $id}) { clientMutationId } }"
ENQUEUE = (
    "mutation($id: ID!, $jump: Boolean!) { enqueuePullRequest(input: {pullRequestId: $id, jump: $jump}) "
    "{ mergeQueueEntry { position jump } } }"
)


def front_label(pull_request: dict) -> str | None:
    """The label that sends this pull request to the front, if it carries one."""
    names = {node["name"] for node in pull_request["labels"]["nodes"]}
    return next((label for label in FRONT_LABELS if label in names), None)


def decide(pull_request: dict) -> tuple[list[str], str]:
    """The steps to take, in order, from `dequeue`, `enqueue`, `enqueue-front`; and why."""
    if pull_request["state"] != "OPEN":
        return [], f"the pull request is {pull_request['state'].lower()}"
    if pull_request["isDraft"]:
        return [], "the pull request is a draft"
    label = front_label(pull_request)
    entry = pull_request.get("mergeQueueEntry")
    if label is None:
        if entry is not None:
            return [], f"already queued at position {entry['position']}"
        return ["enqueue"], "no front label"
    if entry is None:
        return ["enqueue-front"], label
    if entry["jump"]:
        return [], f"already queued at the front ({label})"
    return ["dequeue", "enqueue-front"], f"{label}: queued at position {entry['position']} without the jump"


def graphql(query: str, **variables: str | int | bool) -> dict:
    command = ["gh", "api", "graphql", "-f", f"query={query}"]
    for key, value in variables.items():
        if isinstance(value, bool):
            command += ["-F", f"{key}={'true' if value else 'false'}"]
        elif isinstance(value, int):
            command += ["-F", f"{key}={value}"]
        else:
            command += ["-f", f"{key}={value}"]
    out = subprocess.run(command, capture_output=True, text=True, encoding="utf-8")
    if out.returncode != 0:
        raise SystemExit(f"merge-queue-enqueue: {out.stderr.strip() or out.stdout.strip()}")
    return json.loads(out.stdout)["data"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("number", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--repository", default=None, help="owner/name; default GITHUB_REPOSITORY, else this repository")
    args = parser.parse_args(argv)
    repository = args.repository or os.environ.get("GITHUB_REPOSITORY") or "perfectory-inc/perfectory-public"
    owner, name = repository.split("/", 1)
    pull_request = graphql(QUERY, owner=owner, name=name, number=args.number)["repository"]["pullRequest"]
    steps, why = decide(pull_request)
    print(f"merge-queue-enqueue: #{args.number}: {' then '.join(steps) or 'nothing'} ({why})")
    if args.dry_run:
        return 0
    for step in steps:
        if step == "dequeue":
            # The only removal this script makes: the pull request it was asked about.
            graphql(DEQUEUE, id=pull_request["id"])
            continue
        entry = graphql(ENQUEUE, id=pull_request["id"], jump=step == "enqueue-front")["enqueuePullRequest"]["mergeQueueEntry"]
        print(f"merge-queue-enqueue: #{args.number} queued at position {entry['position']} (jump {entry['jump']})")
    if "enqueue-front" in steps:
        body = (
            f"`{front_label(pull_request)}`: 이 PR 을 머지 큐의 앞에 세웠습니다"
            "(`scripts/github/merge_queue_enqueue.py`, ADR-0553). "
            "다른 PR 은 큐에서 빼지 않았고, 큐는 이 PR 에도 필수 검사를 전부 돌립니다."
        )
        subprocess.run(
            ["gh", "pr", "comment", str(args.number), "--repo", repository, "--body", body],
            check=False,
            capture_output=True,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
