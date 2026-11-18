#!/usr/bin/env python3
"""Say when main is red, and keep one issue open while it is (root ADR-0554).

  main_health.py [--dry-run] [--as-of TIME] [--repository OWNER/NAME]   (GH_TOKEN; issues: write to report)

A pull request and the merge queue run only what a change reaches (root ADR-0313), and a push to
main repeats nothing the queue passed. So a break that slipped past the selection shows nowhere on
main itself: on 2026-11-11 #1268 and #1302 each left a test failing on main, every push run on main
stayed green, and the only sign was other pull requests failing in the queue. The fixes were opened
57 and 115 minutes after the merge, by whoever happened to read a queue failure.

This reads the runs GitHub already made and decides, per job, from what last happened to it:

- a job of a gating workflow that last FAILED (a step failed; a runner that died is not a verdict,
  root ADR-0421) on main itself (the nightly full run, a manual run, or a push run) in the last 26
  hours is red;
- a job that last failed in the merge queue for two different pull requests in a row in the last 3
  hours, with no success of that job between or after them, is red: one queue failure is that pull
  request's own;
- a later success of the same job anywhere on main's line (queue, push, nightly) clears it. A
  skipped job says nothing.

While any job is red, one issue titled TITLE stays open with the jobs and their runs; it is edited
when the set changes and closed when it empties.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[2]
TITLE = "main 이 깨졌어요 (main-red)"
LABEL = "main-red"
# Runs that speak for main's own tree, and the queue, which speaks for main plus one pull request.
MAIN_EVENTS = frozenset({"push", "schedule", "workflow_dispatch"})
QUEUE_EVENT = "merge_group"
QUEUE_PULL_REQUEST = re.compile(r"^gh-readonly-queue/main/pr-(\d+)-")
# A queue failure counts for 3 hours: a job main still breaks fails again for the next pull request
# that reaches it, and a job nothing reaches blocks nobody. Main's own runs count past one nightly.
QUEUE_HOURS = 3
MAIN_HOURS = 26
# Failed runs are read 100 a page; 2026-11-11 had about 150 in a day.
FAILURE_PAGES = 5
# How many newer successful runs of a workflow are opened to look for the job's success: more than
# one workflow ran in any 3 hours measured (37 on 2026-11-11 21-24Z).
SUCCESS_LOOKBACK = 40
STAMP = "%Y-%m-%dT%H:%M:%SZ"


class GitHubUnreadable(Exception):
    pass


def source(run: dict) -> str | None:
    """`main`, `pr-<n>` for a queue run, or None for a run that says nothing about main."""
    if run.get("event") == QUEUE_EVENT:
        match = QUEUE_PULL_REQUEST.match(run.get("head_branch") or "")
        return f"pr-{match.group(1)}" if match else None
    if run.get("event") in MAIN_EVENTS and run.get("head_branch") == "main":
        return "main"
    return None


def failed_jobs(jobs: list[dict]) -> list[str]:
    """The jobs a step failed in. `required/*` only repeats the jobs it gathers."""
    return sorted(
        job["name"]
        for job in jobs
        if job.get("conclusion") == "failure"
        and not job["name"].startswith("required/")
        and any(step.get("conclusion") == "failure" for step in job.get("steps") or [])
    )


def red(observations: list[dict]) -> list[dict]:
    """The observations that make one job red: [] when it is not.

    Each observation is {"at": ISO time, "outcome": "fail"|"pass", "source": "main"|"pr-<n>", "url"}.
    """
    streak: list[dict] = []
    for observation in sorted(observations, key=lambda o: o["at"]):
        if observation["outcome"] == "pass":
            streak = []
        else:
            streak.append(observation)
    if any(o["source"] == "main" for o in streak):
        return streak
    if len({o["source"] for o in streak}) >= 2:
        return streak
    return []


def render(found: dict[tuple[str, str], list[dict]]) -> str:
    lines = [
        "main 에서 다음 작업이 실패하고 있어요(루트 ADR-0554). 이 이슈는 `main-health.yml` 이 열고, 같은 작업이 다시 통과하면 닫아요.",
        "",
        "| 워크플로 | 작업 | 근거 |",
        "|---|---|---|",
    ]
    for (workflow, job), streak in sorted(found.items()):
        evidence = ", ".join(f"[{o['source']}]({o['url']})" for o in streak[-4:])
        lines.append(f"| {workflow} | {job} | {evidence} |")
    lines += [
        "",
        "고치는 PR 에 `hotfix:main-red` 라벨을 붙이고 `python3 scripts/github/merge_queue_enqueue.py <PR>` 로 큐 앞에 세워요(ADR-0553).",
    ]
    return "\n".join(lines) + "\n"


def gating_workflows(root: Path = ROOT) -> frozenset[str]:
    """The workflow files that gate a pull request (tools/ci/change-scope.json lists every one). A
    schedule-only report (the cache budget, the advisories) speaks about the repository at that hour
    and keeps its own issue."""
    contract = json.loads((root / "tools/ci/change-scope.json").read_text(encoding="utf-8"))
    return frozenset(contract["workflows"]) | frozenset(contract["unscoped"])


def gh(*args: str) -> str:
    out = subprocess.run(["gh", *args], capture_output=True, text=True, encoding="utf-8")
    if out.returncode != 0:
        raise GitHubUnreadable(f"gh {' '.join(args[:3])}: {out.stderr.strip()}")
    return out.stdout


def api(path: str) -> dict:
    return json.loads(gh("api", path))


def observe(repo: str, now: datetime, bounded: bool, gating: frozenset[str]) -> dict[tuple[str, str], list[dict]]:
    """Per (workflow, job): its failures (the queue's of the last QUEUE_HOURS, main's own of the last
    MAIN_HOURS) and its successes since the oldest of them. `bounded` leaves out what finished after
    `now`: the answer a past moment would have had."""
    until = now.strftime(STAMP)
    as_of = until if bounded else "9"  # "9" sorts after every ISO time: nothing is left out
    jobs_of: dict[int, list[dict]] = {}

    def jobs(run: dict) -> list[dict]:
        if run["id"] not in jobs_of:
            jobs_of[run["id"]] = api(f"repos/{repo}/actions/runs/{run['id']}/jobs?per_page=100")["jobs"]
        return jobs_of[run["id"]]

    def counts(run: dict) -> bool:
        return source(run) is not None and PurePosixPath(run["path"]).name in gating and run["updated_at"] <= as_of

    failed: list[dict] = []
    windows = [(QUEUE_EVENT, "", QUEUE_HOURS)] + [(event, "&branch=main", MAIN_HOURS) for event in sorted(MAIN_EVENTS)]
    for event, branch, hours in windows:
        since = (now - timedelta(hours=hours)).strftime(STAMP)
        for page in range(1, FAILURE_PAGES + 1):
            listed = api(
                f"repos/{repo}/actions/runs?event={event}{branch}&status=failure&per_page=100&page={page}&created={since}..{until}"
            )["workflow_runs"]
            failed += listed
            if len(listed) < 100:
                break
    failures: dict[tuple[str, str], list[dict]] = {}
    workflow_ids: dict[str, int] = {}
    for run in failed:
        if not counts(run):
            continue
        for job in failed_jobs(jobs(run)):
            workflow_ids[run["name"]] = run["workflow_id"]
            failures.setdefault((run["name"], job), []).append(
                {"at": run["updated_at"], "outcome": "fail", "source": source(run), "url": run["html_url"]}
            )
    for (workflow, job), observations in failures.items():
        oldest = min(o["at"] for o in observations)
        latest = max(o["at"] for o in observations)
        newer = api(
            f"repos/{repo}/actions/workflows/{workflow_ids[workflow]}/runs?status=success&per_page=50&created={oldest}..{until}"
        )["workflow_runs"]
        for run in [run for run in newer if counts(run)][:SUCCESS_LOOKBACK]:  # newest first
            if any(j["name"] == job and j.get("conclusion") == "success" for j in jobs(run)):
                observations.append(
                    {"at": run["updated_at"], "outcome": "pass", "source": source(run), "url": run["html_url"]}
                )
                if run["updated_at"] > latest:
                    break  # a success after every failure: nothing older changes the answer
    return failures


def report(repo: str, found: dict[tuple[str, str], list[dict]]) -> None:
    listed = json.loads(gh("issue", "list", "--repo", repo, "--state", "open", "--label", LABEL, "--json", "number,title,body"))
    current = next((issue for issue in listed if issue["title"] == TITLE), None)
    if not found:
        if current:
            gh("issue", "close", str(current["number"]), "--repo", repo, "--comment", "실패하던 작업이 모두 다시 통과했어요.")
        return
    body = render(found)
    if current is None:
        gh("issue", "create", "--repo", repo, "--title", TITLE, "--label", LABEL, "--body", body)
    elif current["body"].strip() != body.strip():
        gh("issue", "edit", str(current["number"]), "--repo", repo, "--body", body)
        gh("issue", "comment", str(current["number"]), "--repo", repo, "--body", "실패하는 작업이 바뀌었어요.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--repository", default=None, help="owner/name; default GITHUB_REPOSITORY")
    parser.add_argument("--as-of", default=None, help="ISO time: what a past moment would have answered (reports nothing)")
    args = parser.parse_args(argv)
    repo = args.repository or os.environ["GITHUB_REPOSITORY"]
    now = datetime.fromisoformat(args.as_of.replace("Z", "+00:00")) if args.as_of else datetime.now(timezone.utc)
    try:
        observed = observe(repo, now, bool(args.as_of), gating_workflows())
        found = {key: streak for key, observations in observed.items() if (streak := red(observations))}
        for (workflow, job), streak in sorted(found.items()):
            print(f"main-health: RED {workflow} / {job}: {', '.join(o['source'] for o in streak)}")
        print(f"main-health: {len(found)} red job(s) as of {now.strftime(STAMP)}")
        if not args.dry_run and not args.as_of:
            report(repo, found)
    except GitHubUnreadable as error:
        # A 502 or a spent rate limit is not a verdict on main; the next run asks again.
        print(f"main-health: GitHub could not be read, leaving the issue as it is: {error}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
