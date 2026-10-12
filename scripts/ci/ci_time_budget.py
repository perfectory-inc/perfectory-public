#!/usr/bin/env python3
"""Measure pull request CI wall time per week against the one budget (root ADR-0203).

A pull request attempt is every `pull_request` workflow run of one head SHA. Its wall time is
max(updated_at) - min(run_started_at): what an author waits from push to the last verdict.

    measure  fetch completed runs for the last N full UTC weeks, write the trend JSON and a
             Markdown table (the workflow sends it to $GITHUB_STEP_SUMMARY).
    judge    read that JSON, warn for every attempt of the newest week over budget, and exit 1
             when that week's p50 is over budget (2 when the week measured nothing).

The budget lives only in tools/ci/ci-time-budget.contract.json. Standard library only.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
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
    wall time measures the cancellation, not the wait; it is left out."""
    by_sha: dict[str, list[dict]] = defaultdict(list)
    for run in runs:
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


def markdown(trend: list[dict], budget: float) -> str:
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
    return "\n".join(lines) + "\n"


def _get(url: str, token: str | None) -> dict:
    request = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def fetch_runs(repo: str, start: datetime, end: datetime, token: str | None) -> list[dict]:
    """Completed pull_request runs created in [start, end), split until each query is under the cap."""
    runs: dict[int, dict] = {}
    pending = [(start, end)]
    while pending:
        low, high = pending.pop()
        created = f"{low:%Y-%m-%dT%H:%M:%SZ}..{high - timedelta(seconds=1):%Y-%m-%dT%H:%M:%SZ}"
        page, seen = 1, 0
        while True:
            query = urllib.parse.urlencode(
                {"event": "pull_request", "status": "completed", "created": created, "per_page": 100, "page": page}
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


def measure(args: argparse.Namespace) -> int:
    budget = load_budget(args.contract)
    now = parse_time(args.now) if args.now else datetime.now(timezone.utc)
    current = week_start(now)
    weeks = [current - timedelta(weeks=n) for n in range(args.weeks, 0, -1)]
    if args.runs_json:
        runs = json.loads(Path(args.runs_json).read_text(encoding="utf-8"))
    else:
        repo = args.repo or os.environ.get("GITHUB_REPOSITORY")
        if not repo:
            print("FAIL ci-time-budget: pass --repo or set GITHUB_REPOSITORY", file=sys.stderr)
            return 2
        token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
        # One extra day catches runs created after midnight for an attempt that began on Sunday.
        runs = fetch_runs(repo, weeks[0], current + timedelta(days=1), token)
    trend = weekly_trend(group_attempts(runs), weeks)
    Path(args.out).write_text(json.dumps({"weeks": trend}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    table = markdown(trend, budget)
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
