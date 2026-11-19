#!/usr/bin/env python3
"""Measure pull request CI wall time per week against the one budget (root ADR-0203).

A pull request attempt is every `pull_request` workflow run of one head SHA. Its wall time is
max(updated_at) - min(run_started_at): what an author waits from push to the last verdict.

    measure  fetch completed runs for the last N full UTC weeks, write the trend JSON and a
             Markdown table (the workflow sends it to $GITHUB_STEP_SUMMARY).
    judge    read that JSON, warn for every attempt of the newest week over budget, and exit 1
             when that week's p50 is over budget (2 when the week measured nothing).

`measure` also reports the runner wait (root ADR-0582): for an even sample
of each week's completed runs per event, every job that took a runner contributes
started_at - created_at. The wait is reported, never judged: the budget is the wall time only.

The budget lives only in tools/ci/ci-time-budget.contract.json. Standard library only.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "tools" / "ci" / "ci-time-budget.contract.json"
API = "https://api.github.com"
# The runs API returns at most 1,000 results for a filtered query, silently.
API_RESULT_CAP = 1000
# The events whose runner wait the trend reports (root ADR-0582): what an author, the merge queue and
# main's deploy each wait for. Sampled runs per week and event: the jobs API is one request per run
# and GITHUB_TOKEN allows 1,000 requests an hour for the whole repository. Week of 2026-11-09: 2,248
# pull_request, 2,500 merge_group and 956 push runs, about 60 list pages; 4 weeks x (60 + 3 x 25 job
# reads) is about 540 requests.
QUEUE_EVENTS = ("pull_request", "merge_group", "push")
QUEUE_SAMPLE_RUNS = 25


def load_budget(contract: Path = CONTRACT) -> float:
    data = json.loads(contract.read_text(encoding="utf-8"))
    if data.get("schema_version") != 1:
        raise ValueError(f"{contract}: schema_version must be 1")
    budget = data.get("pull_request_wall_minutes")
    if isinstance(budget, bool) or not isinstance(budget, (int, float)) or budget <= 0:
        raise ValueError(f"{contract}: pull_request_wall_minutes must be a positive number")
    return float(budget)


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def week_start(moment: datetime) -> datetime:
    day = moment.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return day - timedelta(days=day.weekday())


def percentile(values: list[float], fraction: float) -> float:
    """Nearest-rank percentile; no interpolation, so every reported value was observed."""
    if not values:
        raise ValueError("percentile of no values")
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def group_attempts(runs: list[dict]) -> list[dict]:
    """One attempt per head SHA. A SHA with a cancelled run was superseded by a newer push, so its
    wall time measures the cancellation, not the wait; it is left out. Only `pull_request` runs are
    attempts (a run without `event` is one)."""
    by_sha: dict[str, list[dict]] = defaultdict(list)
    for run in runs:
        if run.get("event", "pull_request") == "pull_request":
            by_sha[run["head_sha"]].append(run)
    attempts = []
    for sha, group in by_sha.items():
        if any(run.get("conclusion") == "cancelled" for run in group):
            continue
        start = min(parse_time(run["run_started_at"]) for run in group)
        end = max(parse_time(run["updated_at"]) for run in group)
        workflows: dict[str, float] = {}
        for run in group:  # a re-run of the same workflow keeps its longest run
            took = (parse_time(run["updated_at"]) - parse_time(run["run_started_at"])).total_seconds() / 60
            workflows[run["name"]] = max(workflows.get(run["name"], 0.0), round(took, 1))
        attempts.append(
            {
                "head_sha": sha,
                # The runs API empties pull_requests once a PR closes; the branch name survives.
                "head_branch": group[0]["head_branch"],
                "started_at": start.isoformat(),
                "minutes": round((end - start).total_seconds() / 60, 1),
                "workflows": workflows,
            }
        )
    return attempts


def weekly_trend(attempts: list[dict], weeks: list[datetime]) -> list[dict]:
    rows = []
    for start in weeks:
        inside = [a for a in attempts if week_start(parse_time(a["started_at"])) == start]
        minutes = [a["minutes"] for a in inside]
        per_workflow: dict[str, list[float]] = defaultdict(list)
        for attempt in inside:
            for name, value in attempt["workflows"].items():
                per_workflow[name].append(value)
        worst = max(per_workflow, key=lambda name: (percentile(per_workflow[name], 0.5), name), default=None)
        rows.append(
            {
                "week_start": start.date().isoformat(),
                "count": len(inside),
                "p50": percentile(minutes, 0.5) if minutes else None,
                "p90": percentile(minutes, 0.9) if minutes else None,
                "max": max(minutes) if minutes else None,
                "worst_workflow": worst,
                "worst_workflow_p50": percentile(per_workflow[worst], 0.5) if worst else None,
                "attempts": sorted(
                    ({k: a[k] for k in ("head_sha", "head_branch", "minutes")} for a in inside),
                    key=lambda a: -a["minutes"],
                ),
            }
        )
    return rows


def job_waits(jobs: list[dict]) -> list[float]:
    """Seconds each job waited for a runner. A job that never took one (skipped, or cancelled before
    it started) waited for nothing and is left out."""
    waits = []
    for job in jobs:
        created, started = job.get("created_at"), job.get("started_at")
        if not created or not started or job.get("conclusion") == "skipped":
            continue
        if job.get("conclusion") == "cancelled" and not job.get("steps"):
            continue
        waits.append(max(0.0, (parse_time(started) - parse_time(created)).total_seconds()))
    return waits


def sample_evenly(runs: list[dict], count: int) -> list[dict]:
    """`count` runs spread over the week in start order, so the busy hours are in it as often as they
    happened (the newest page alone is one evening)."""
    ordered = sorted(runs, key=lambda run: (run["run_started_at"], run.get("id", 0)))
    if len(ordered) <= count:
        return ordered
    if count == 1:
        return ordered[:1]
    return [ordered[round(i * (len(ordered) - 1) / (count - 1))] for i in range(count)]


def queue_trend(samples: list[dict], weeks: list[datetime]) -> list[dict]:
    """Runner wait per week and event. A sample is one run: its event, start and job waits."""
    rows = []
    for start in weeks:
        for event in QUEUE_EVENTS:
            inside = [s for s in samples
                      if s["event"] == event and week_start(parse_time(s["run_started_at"])) == start]
            waits = [w for s in inside for w in s["waits"]]
            rows.append(
                {
                    "week_start": start.date().isoformat(),
                    "event": event,
                    "runs": len(inside),
                    "jobs": len(waits),
                    "p50": percentile(waits, 0.5) if waits else None,
                    "p90": percentile(waits, 0.9) if waits else None,
                    "p99": percentile(waits, 0.99) if waits else None,
                    "max": max(waits) if waits else None,
                }
            )
    return rows


def verdict(week: dict, budget: float) -> tuple[int, list[str]]:
    """(exit code, ::warning:: lines) for one week: 0 within budget, 1 over, 2 nothing measured."""
    if not week["count"]:
        return 2, [f"::error::{week['week_start']} 주에 측정된 PR 시도가 없다 — 판정 불가"]
    lines = []
    for attempt in week["attempts"]:
        if attempt["minutes"] > budget:
            lines.append(
                f"::warning::{attempt['head_branch']} ({attempt['head_sha'][:8]}) 벽시계 {attempt['minutes']}분 > 예산 {budget:g}분"
            )
    if week["p50"] > budget:
        lines.append(f"::error::{week['week_start']} 주 p50 {week['p50']}분 > 예산 {budget:g}분")
        return 1, lines
    return 0, lines


def markdown(trend: list[dict], budget: float, queue: list[dict] | None = None) -> str:
    def cell(value):
        return "-" if value is None else f"{value:g}"

    lines = [
        f"## PR CI 벽시계 추이 (예산 p50 ≤ {budget:g}분)",
        "",
        "| 주 시작(UTC 월) | PR 시도 | p50(분) | p90(분) | 최대(분) | 가장 느린 워크플로 (p50 분) |",
        "| --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in trend:
        worst = f"{row['worst_workflow']} ({cell(row['worst_workflow_p50'])})" if row["worst_workflow"] else "-"
        lines.append(
            f"| {row['week_start']} | {row['count']} | {cell(row['p50'])} | {cell(row['p90'])} "
            f"| {cell(row['max'])} | {worst} |"
        )
    if queue:
        lines += [
            "",
            f"## 러너 대기 (작업 started_at − created_at, 주·이벤트마다 실행 {QUEUE_SAMPLE_RUNS}개 표본, 판정 없음)",
            "",
            "| 주 시작(UTC 월) | 이벤트 | 실행 | 작업 | p50(초) | p90(초) | p99(초) | 최대(초) |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
        for row in queue:
            lines.append(
                f"| {row['week_start']} | {row['event']} | {row['runs']} | {row['jobs']} | {cell(row['p50'])} "
                f"| {cell(row['p90'])} | {cell(row['p99'])} | {cell(row['max'])} |"
            )
    return "\n".join(lines) + "\n"


def _get(url: str, token: str | None) -> dict:
    request = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def fetch_runs(
    repo: str, start: datetime, end: datetime, token: str | None, event: str = "pull_request"
) -> list[dict]:
    """Completed runs of one event created in [start, end), split until each query is under the cap."""
    runs: dict[int, dict] = {}
    pending = [(start, end)]
    while pending:
        low, high = pending.pop()
        created = f"{low:%Y-%m-%dT%H:%M:%SZ}..{high - timedelta(seconds=1):%Y-%m-%dT%H:%M:%SZ}"
        page, seen = 1, 0
        while True:
            query = urllib.parse.urlencode(
                {"event": event, "status": "completed", "created": created, "per_page": 100, "page": page}
            )
            body = _get(f"{API}/repos/{repo}/actions/runs?{query}", token)
            total = body["total_count"]
            if total > API_RESULT_CAP:
                if high - low <= timedelta(hours=1):
                    raise RuntimeError(f"{created}: {total} runs in one hour exceed the API cap")
                middle = low + (high - low) / 2
                pending += [(low, middle), (middle, high)]
                break
            for run in body["workflow_runs"]:
                runs[run["id"]] = {
                    "id": run["id"],
                    "event": event,
                    "name": run["name"],
                    "head_sha": run["head_sha"],
                    "run_started_at": run["run_started_at"],
                    "updated_at": run["updated_at"],
                    "conclusion": run["conclusion"],
                    "head_branch": run["head_branch"],
                }
            seen += len(body["workflow_runs"])
            if not body["workflow_runs"] or seen >= total:
                break
            page += 1
    return list(runs.values())


def fetch_jobs(repo: str, run_id: int, token: str | None) -> list[dict]:
    jobs, page = [], 1
    while True:
        body = _get(f"{API}/repos/{repo}/actions/runs/{run_id}/jobs?per_page=100&page={page}", token)
        jobs += body["jobs"]
        if len(jobs) >= body["total_count"] or not body["jobs"]:
            return jobs
        page += 1


def sample_queue(runs: list[dict], weeks: list[datetime], jobs_of) -> list[dict]:
    """The sampled runs of every week and event with their job waits; `jobs_of(run)` reads a run's
    jobs. A run whose jobs cannot be read (rate limit, deleted) is left out and said so."""
    samples = []
    for start in weeks:
        for event in QUEUE_EVENTS:
            inside = [r for r in runs if r.get("event", "pull_request") == event
                      and week_start(parse_time(r["run_started_at"])) == start]
            for run in sample_evenly(inside, QUEUE_SAMPLE_RUNS):
                try:
                    jobs = jobs_of(run)
                except (urllib.error.URLError, KeyError, ValueError) as error:
                    print(f"::warning::run {run.get('id')}: jobs unreadable, left out of the runner wait: {error}")
                    continue
                samples.append({"event": event, "run_started_at": run["run_started_at"], "waits": job_waits(jobs)})
    return samples


def measure(args: argparse.Namespace) -> int:
    budget = load_budget(args.contract)
    now = parse_time(args.now) if args.now else datetime.now(timezone.utc)
    current = week_start(now)
    weeks = [current - timedelta(weeks=n) for n in range(args.weeks, 0, -1)]
    if args.runs_json:
        # Fixture runs carry their jobs inline (tests).
        runs = json.loads(Path(args.runs_json).read_text(encoding="utf-8"))
        jobs_of = lambda run: run.get("jobs", [])  # noqa: E731
    else:
        repo = args.repo or os.environ.get("GITHUB_REPOSITORY")
        if not repo:
            print("FAIL ci-time-budget: pass --repo or set GITHUB_REPOSITORY", file=sys.stderr)
            return 2
        token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
        # One extra day catches runs created after midnight for an attempt that began on Sunday.
        runs = [run for event in QUEUE_EVENTS
                for run in fetch_runs(repo, weeks[0], current + timedelta(days=1), token, event)]
        jobs_of = lambda run: fetch_jobs(repo, run["id"], token)  # noqa: E731
    trend = weekly_trend(group_attempts(runs), weeks)
    queue = queue_trend(sample_queue(runs, weeks, jobs_of), weeks)
    Path(args.out).write_text(
        json.dumps({"weeks": trend, "runner_wait": queue}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    table = markdown(trend, budget, queue)
    if args.markdown:
        with open(args.markdown, "a", encoding="utf-8") as summary:
            summary.write(table)
    print(table, end="")
    return 0


def judge(args: argparse.Namespace) -> int:
    budget = load_budget(args.contract)
    weeks = json.loads(Path(args.trend).read_text(encoding="utf-8"))["weeks"]
    if not weeks:
        print("::error::추이에 주가 하나도 없다 — 판정 불가")
        return 2
    code, lines = verdict(weeks[-1], budget)
    for line in lines:
        print(line)
    return code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--contract", type=Path, default=CONTRACT)
    sub = parser.add_subparsers(dest="command", required=True)
    m = sub.add_parser("measure")
    m.add_argument("--weeks", type=int, default=4)
    m.add_argument("--repo")
    m.add_argument("--now", help="ISO time treated as now (tests)")
    m.add_argument("--runs-json", help="read runs from this file instead of the API (tests)")
    m.add_argument("--out", required=True, help="trend JSON path")
    m.add_argument("--markdown", help="append the Markdown table here")
    j = sub.add_parser("judge")
    j.add_argument("trend", help="trend JSON written by measure")
    args = parser.parse_args(argv)
    return measure(args) if args.command == "measure" else judge(args)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")  # Korean table on a non-UTF-8 console
    sys.exit(main())
