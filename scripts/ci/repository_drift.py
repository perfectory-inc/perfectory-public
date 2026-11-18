#!/usr/bin/env python3
"""Say when the repository is not what tools/github declares, or Renovate is silent (root ADR-0570).

  repository_drift.py check <summary.md> [--advisories-result RESULT] [--as-of TIME]   (GH_TOKEN, GH_REPO)
  repository_drift.py issue <summary.md>                                                (issues: write)

`check` writes the findings to summary.md and success|failure|unreadable to summary.result; `issue`
reads both.

Two things went unseen for weeks because nothing looked:

- the live repository drifted from `tools/github/*.json`: `has_issues` false and the non-main firewall
  without Renovate's bypass actor since 2026-10-22, the merge queue still `HEADGREEN` after ADR-0552
  (2026-11-12) chose `ALLGREEN`. `scripts/github/configure-public-repository.sh verify` reports all of
  it, but only an administrator can run it and nobody did;
- Renovate (root ADR-0271) opened no pull request for four weeks while advisories stopped every pull
  request (2026-11-13..18), because it could not create a branch.

This runs daily with the workflow token, which reads what any reader of a public repository reads. It
checks the part of `verify` that token can see, and the symptom of the part it cannot:

1. the repository settings `tools/github/repository-settings.json` declares and the API returns to this
   token (has_issues, has_projects, has_wiki, has_discussions, description);
2. each declared ruleset (main, non-main firewall, tag firewall), compared the way `verify` compares
   (`github-policy-json.py normalize-ruleset`) without `bypass_actors`, which only an administrator reads;
3. Renovate's pulse: while main has advisories (the daily Cargo advisories check failed, or the last
   scheduled OSV ratchet on main failed), Renovate must have opened or updated a pull request, or its
   dependency dashboard issue, in the last PULSE_HOURS. A Renovate that cannot write (a bypass actor
   missing, the app uninstalled) shows here.

A check that cannot read GitHub reports `unreadable`, which leaves the issue as it is (like ADR-0554).
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
POLICY = ROOT / "tools" / "github"
SETTINGS_READABLE = ("description", "has_discussions", "has_issues", "has_projects", "has_wiki")
RULESETS = ("main-ruleset.json", "non-main-branch-firewall.json", "tag-firewall.json")
TITLE = "Repository drift or Renovate silence"
RENOVATE = "renovate[bot]"
DASHBOARD = "Dependency Dashboard"
# Renovate's hosted app runs several times a day and rewrites its dashboard when anything it tracks
# changes; an advisory on main is such a change. Two days is two missed daily checks.
PULSE_HOURS = 48
STAMP = "%Y-%m-%dT%H:%M:%SZ"


class Unreadable(Exception):
    pass


def _policy_json():
    spec = importlib.util.spec_from_file_location("github_policy_json", ROOT / "scripts" / "github" / "github-policy-json.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def comparable(ruleset: dict) -> dict:
    """The ruleset as `verify` compares it, without what only an administrator reads."""
    normalized = _policy_json().normalize_ruleset(ruleset, without_status=False)
    normalized.pop("bypass_actors", None)
    return normalized


def settings_drift(declared: dict, live: dict) -> list[str]:
    return [
        f"repository setting `{key}`: declared {json.dumps(declared[key], ensure_ascii=False)}, live {json.dumps(live.get(key), ensure_ascii=False)}"
        for key in SETTINGS_READABLE
        if key in declared and live.get(key) is not None and live.get(key) != declared[key]
    ]


def ruleset_drift(declared: list[dict], live: list[dict]) -> list[str]:
    found = []
    by_name: dict[str, list[dict]] = {}
    for ruleset in live:
        by_name.setdefault(ruleset.get("name", ""), []).append(ruleset)
    for policy in declared:
        matches = by_name.get(policy["name"], [])
        if len(matches) != 1:
            found.append(f"ruleset `{policy['name']}`: expected one, found {len(matches)}")
            continue
        want, have = comparable(policy), comparable(matches[0])
        for key in sorted(set(want) | set(have)):
            if want.get(key) != have.get(key):
                found.append(f"ruleset `{policy['name']}`: `{key}` differs from tools/github")
    return found


def stamp(text: str) -> datetime:
    return datetime.strptime(text, STAMP).replace(tzinfo=timezone.utc)


def renovate_silent(pulls: list[dict], issues: list[dict], now: datetime) -> str | None:
    """None when Renovate opened or updated a pull request or its dashboard within PULSE_HOURS."""
    since = now - timedelta(hours=PULSE_HOURS)
    times = [stamp(pull["updatedAt"]) for pull in pulls]
    times += [stamp(issue["updatedAt"]) for issue in issues if issue.get("title") == DASHBOARD]
    last = max(times, default=None)
    if last is not None and last >= since:
        return None
    seen = last.strftime(STAMP) if last else "never"
    return (
        f"main has advisories, and Renovate has opened or updated no pull request or dependency dashboard "
        f"for {PULSE_HOURS} hours (last: {seen}). Check that the app is installed and can create branches "
        f"(the non-main firewall's bypass actor, `configure-public-repository.sh verify`)"
    )


def gh(*args: str) -> str:
    done = subprocess.run(["gh", *args], capture_output=True, text=True, encoding="utf-8", timeout=120)
    if done.returncode != 0:
        raise Unreadable(f"gh {' '.join(args[:2])}: {done.stderr.strip()}")
    return done.stdout


def main_has_advisories(repo: str, cargo_result: str) -> bool:
    if cargo_result == "failure":
        return True
    runs = json.loads(
        gh("run", "list", "--repo", repo, "--workflow", "monorepo-guard.yml", "--branch", "main",
           "--event", "schedule", "--limit", "1", "--json", "databaseId,conclusion")
    )
    if not runs or runs[0].get("conclusion") != "failure":
        return False
    jobs = json.loads(gh("run", "view", str(runs[0]["databaseId"]), "--repo", repo, "--json", "jobs"))["jobs"]
    return any(
        step.get("name") == "Repository OSV vulnerability ratchet" and step.get("conclusion") == "failure"
        for job in jobs
        for step in job.get("steps") or []
    )


def check(summary: Path, cargo_result: str, now: datetime) -> int:
    repo = os.environ["GH_REPO"]
    try:
        live_settings = json.loads(gh("api", f"repos/{repo}"))
        live_rulesets = [
            json.loads(gh("api", f"repos/{repo}/rulesets/{entry['id']}"))
            for entry in json.loads(gh("api", f"repos/{repo}/rulesets?per_page=100"))
            if entry.get("source_type") == "Repository"
        ]
        found = settings_drift(json.loads((POLICY / "repository-settings.json").read_text(encoding="utf-8")), live_settings)
        found += ruleset_drift(
            [json.loads((POLICY / name).read_text(encoding="utf-8")) for name in RULESETS], live_rulesets
        )
        if main_has_advisories(repo, cargo_result):
            pulls = json.loads(
                gh("pr", "list", "--repo", repo, "--state", "all", "--author", "app/renovate",
                   "--limit", "20", "--json", "number,updatedAt")
            )
            issues = json.loads(
                gh("issue", "list", "--repo", repo, "--state", "all", "--author", "app/renovate",
                   "--limit", "20", "--json", "number,title,updatedAt")
            )
            silence = renovate_silent(pulls, issues, now)
            if silence:
                found.append(silence)
    except Unreadable as error:
        print(f"repository-drift: unreadable ({error}); the issue stays as it is")
        summary.write_text("", encoding="utf-8")
        summary.with_suffix(".result").write_text("unreadable\n", encoding="utf-8")
        return 0
    lines = [f"- {line}" for line in found]
    summary.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    print("\n".join(lines) if lines else "repository-drift: none")
    summary.with_suffix(".result").write_text(("failure" if found else "success") + "\n", encoding="utf-8")
    # `issue` keeps the issue in step and then turns the run red on drift; no conditional step needed.
    return 0


def issue(summary: Path) -> int:
    repo = os.environ["GH_REPO"]
    marker = summary.with_suffix(".result")
    result = marker.read_text(encoding="utf-8").strip() if marker.exists() else "missing"
    run = os.environ.get("RUN_URL", "the daily run")
    listed = json.loads(gh("issue", "list", "--repo", repo, "--state", "open", "--search", f'"{TITLE}" in:title',
                           "--json", "number,title,body"))
    current = next((entry for entry in listed if entry["title"] == TITLE), None)
    if result == "success":
        if current:
            gh("issue", "close", str(current["number"]), "--repo", repo, "--comment", f"Cleared ({run}).")
        return 0
    if result != "failure":
        print(f"repository-drift: check result '{result}' leaves the issue as it is")
        return 0
    findings = summary.read_text(encoding="utf-8") if summary.exists() else ""
    body = (
        f"The daily repository check (root ADR-0570) found the repository unlike `tools/github`, or Renovate silent. "
        f"An administrator runs `bash scripts/github/configure-public-repository.sh verify` for the full picture. Latest run: {run}\n\n"
        + (findings or "The check stopped before it wrote a finding; read the run.\n")
    )
    if current is None:
        gh("issue", "create", "--repo", repo, "--title", TITLE, "--body", body)
    elif current["body"].split("\n", 1)[-1] != body.split("\n", 1)[-1]:
        # The run link changes every day; only a change in the findings rewrites and notifies.
        gh("issue", "edit", str(current["number"]), "--repo", repo, "--body", body)
        gh("issue", "comment", str(current["number"]), "--repo", repo, "--body", f"The findings changed ({run}).")
    print(findings, end="")
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    checking = sub.add_parser("check")
    checking.add_argument("summary", type=Path)
    checking.add_argument("--advisories-result", default="")
    checking.add_argument("--as-of")
    issuing = sub.add_parser("issue")
    issuing.add_argument("summary", type=Path)
    args = parser.parse_args(argv)
    if args.command == "check":
        now = stamp(args.as_of) if args.as_of else datetime.now(timezone.utc).replace(microsecond=0)
        return check(args.summary, args.advisories_result, now)
    return issue(args.summary)


if __name__ == "__main__":
    sys.exit(main())
