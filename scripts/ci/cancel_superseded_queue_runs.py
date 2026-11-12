#!/usr/bin/env python3
"""Cancel a merge-queue run whose commit the queue has dropped (root ADR-0553).

The merge queue builds each entry on a temporary branch (``gh-readonly-queue/main/pr-N-<base>``).
When an entry ahead fails, is removed or is jumped over, the queue deletes that branch and builds
the entry again on a new one, but the workflow runs of the old commit keep their runners until they
finish: of the last 200 Foundation queue runs on 2026-11-12, 55 completed green on a commit that
could no longer merge, while the standard runners were full.

A run is cancelled only when all of these hold:
- its event is ``merge_group`` and it is queued or in progress;
- its queue branch no longer exists;
- its commit is not on main. A merged entry's branch is deleted too, and the host deploys a commit
  by the queue's check runs on it (root ADR-0167), so a merged commit's runs are never touched.

Usage: cancel_superseded_queue_runs.py [--dry-run]   (GH_TOKEN with actions: write)
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from typing import Callable

QUEUE_BRANCH_PREFIX = "gh-readonly-queue/"
ACTIVE = ("queued", "in_progress")
# `GET /compare/main...<sha>`: the commit is main's tip or behind it, so it is on main.
ON_MAIN = frozenset({"identical", "behind"})


def superseded(run: dict, branch_exists: Callable[[str], bool], on_main: Callable[[str], bool]) -> bool:
    """Whether the queue dropped this run's commit. The branch is asked first: a branch deleted by a
    merge is deleted after main moved, so the later main answer already holds that merge."""
    branch = run.get("head_branch") or ""
    if run.get("event") != "merge_group" or run.get("status") not in ACTIVE:
        return False
    if not branch.startswith(QUEUE_BRANCH_PREFIX):
        return False
    if branch_exists(branch):
        return False
    return not on_main(run["head_sha"])


def gh(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["gh", "api", *args], capture_output=True, text=True, encoding="utf-8")


def gh_json(*args: str) -> dict:
    out = gh(*args)
    if out.returncode != 0:
        raise RuntimeError(out.stderr.strip() or f"gh api {args[0]} failed")
    return json.loads(out.stdout)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--repository", default=None, help="owner/name; default GITHUB_REPOSITORY")
    args = parser.parse_args(argv)
    repo = args.repository or os.environ["GITHUB_REPOSITORY"]

    branches: dict[str, bool] = {}

    def branch_exists(branch: str) -> bool:
        if branch not in branches:
            out = gh("--include", f"repos/{repo}/git/ref/heads/{branch}")
            status = out.stdout.split("\n", 1)[0]
            if out.returncode == 0:
                branches[branch] = True
            elif " 404" in status:
                branches[branch] = False
            else:
                # Unreadable is not "gone": leave the run alone.
                raise RuntimeError(f"cannot read {branch}: {status or out.stderr.strip()}")
        return branches[branch]

    def on_main(sha: str) -> bool:
        return gh_json(f"repos/{repo}/compare/main...{sha}")["status"] in ON_MAIN

    runs: list[dict] = []
    for status in ACTIVE:
        runs += gh_json(f"repos/{repo}/actions/runs?event=merge_group&status={status}&per_page=100")["workflow_runs"]
    cancelled = 0
    for run in runs:
        try:
            if not superseded(run, branch_exists, on_main):
                continue
        except RuntimeError as error:
            print(f"cancel-superseded-queue-runs: run {run['id']} left alone: {error}", file=sys.stderr)
            continue
        print(f"cancel-superseded-queue-runs: run {run['id']} {run['name']} on {run['head_branch']} was dropped by the queue")
        if not args.dry_run:
            # A run that finished meanwhile answers 409; nothing is lost.
            gh("-X", "POST", f"repos/{repo}/actions/runs/{run['id']}/cancel")
        cancelled += 1
    print(f"cancel-superseded-queue-runs: {cancelled} run(s) cancelled, {len(runs)} active queue run(s) inspected")
    return 0


if __name__ == "__main__":
    sys.exit(main())
