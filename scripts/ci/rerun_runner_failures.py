#!/usr/bin/env python3
"""Re-run, once, a workflow run that failed only because its runner did (root ADR-0421).

A job whose conclusion is ``failure`` but none of whose steps failed never reached a verdict: the
runner died, lost its log blob (BlobNotFound) or ran out of disk before a step could report. Such a
run is retried once. A run with any failed step is a real failure and is left alone, and a run
already re-run (``run_attempt > 1``) is never retried again, so a broken runner cannot loop.

merge_group runs are skipped: the merge queue has already removed the entry when its check failed,
so a re-run would spend runners on a commit nobody will merge. Fork pull request runs are re-run
like any other, because re-running reuses the approval and the code GitHub already accepted.

Usage: rerun_runner_failures.py [--dry-run] [--since-minutes N]   (GH_TOKEN with actions: write)
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone

SKIPPED_EVENTS = frozenset({"merge_group"})


def runner_failed_only(jobs: list[dict]) -> bool:
    """True when at least one job failed and every failed job failed without a failed step."""
    failed = [job for job in jobs if job.get("conclusion") == "failure"]
    if not failed:
        return False
    return all(
        not any(step.get("conclusion") == "failure" for step in job.get("steps") or [])
        for job in failed
    )


def should_rerun(run: dict, jobs: list[dict]) -> bool:
    return (
        run.get("conclusion") == "failure"
        and run.get("run_attempt", 1) == 1
        and run.get("event") not in SKIPPED_EVENTS
        and runner_failed_only(jobs)
    )


def gh_json(*args: str) -> dict:
    out = subprocess.run(["gh", "api", *args], check=True, capture_output=True, text=True, encoding="utf-8")
    return json.loads(out.stdout)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--since-minutes", type=int, default=120)
    parser.add_argument("--repository", default=None, help="owner/name; default GITHUB_REPOSITORY")
    args = parser.parse_args(argv)
    import os

    repo = args.repository or os.environ["GITHUB_REPOSITORY"]
    since = (datetime.now(timezone.utc) - timedelta(minutes=args.since_minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")
    runs = gh_json(f"repos/{repo}/actions/runs?status=failure&per_page=100&created=%3E{since}")["workflow_runs"]
    rerun = 0
    for run in runs:
        if run.get("run_attempt", 1) != 1 or run.get("event") in SKIPPED_EVENTS:
            continue
        jobs = gh_json(f"repos/{repo}/actions/runs/{run['id']}/jobs?per_page=100")["jobs"]
        if not should_rerun(run, jobs):
            continue
        print(f"rerun-runner-failures: run {run['id']} {run['name']} ({run['event']}) failed with no failed step")
        if not args.dry_run:
            subprocess.run(["gh", "api", "-X", "POST", f"repos/{repo}/actions/runs/{run['id']}/rerun-failed-jobs"], check=True)
        rerun += 1
    print(f"rerun-runner-failures: {rerun} run(s) re-run, {len(runs)} failed run(s) inspected since {since}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
