#!/usr/bin/env python3
"""Say when the repository is not what tools/github declares, or Renovate is silent (root ADR-0570).

  repository_drift.py check <summary.md> [--advisories OUTCOME] [--as-of TIME]   (GH_TOKEN, GH_REPO)
  repository_drift.py report <summary.md>

`check` writes the findings to summary.md and success|failure|unreadable to summary.result; `report`
reads both and is the job's verdict: green when clear; on findings it fails with a
`repository-drift-count` annotation and one `repository-drift` annotation per finding, which the
host's service checks carry to Slack #alerts (the `repository` part, the way root ADR-0278 carries
advisories). Until 2026-11-21 this kept a GitHub issue; issues are off in this repository.

Two things went unseen for weeks because nothing looked:

- the live repository drifted from `tools/github/*.json`: `has_issues` false and the non-main firewall
  without Renovate's bypass actor since 2026-10-22, the merge queue still `HEADGREEN` after ADR-0552
  (2026-11-12) chose `ALLGREEN`. `scripts/github/configure-public-repository.sh verify` reports all of
  it, but only an administrator can run it and nobody did;
- Renovate (root ADR-0271) opened no pull request for four weeks while advisories stopped every pull
  request (2026-11-13..18), because it could not create a branch.

This runs daily with the workflow token, which reads what any reader of a public repository reads. It
checks the part of `verify` that token can see, and the symptom of the part it cannot:

1. the repository settings `tools/github/repository-settings.json` declares that a reader is given
   (has_issues, has_projects, has_wiki, has_discussions, description);
2. the set of repository rulesets (name, target, enforcement, source; `verify`'s effective-set check)
   and each declared ruleset, compared the way `verify` compares (`github-policy-json.py`) without
   `bypass_actors`, which only an administrator reads;
3. Renovate's pulse: while main has advisories (the daily `Every Cargo workspace on main` step failed,
   or the last completed scheduled OSV ratchet on main failed), Renovate must have an open pull request
   or have opened one in the last PULSE_HOURS. Its dependency dashboard does not count: a Renovate that
   cannot create branches still edits its dashboard. An advisory no release fixes also shows here, and
   correctly: nothing is moving it (the way out is an ADR-0414 allow-list entry).

Each signal is read on its own: one that cannot be read is reported as unreadable and the others still
count. Only when nothing could be read is there no verdict: the job fails without a count, and the
verdict before it stands (like ADR-0554).
"""

from __future__ import annotations

import argparse
import functools
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
# The annotations the service checks read. GitHub keeps 10 error annotations of a step: the count,
# then at most FINDING_ANNOTATIONS findings.
COUNT_TITLE = "repository-drift-count"
FINDING_TITLE = "repository-drift"
FINDING_ANNOTATIONS = 9
# Renovate's hosted app runs several times a day, and a vulnerability fix ignores its schedule and hourly
# limit (Renovate's vulnerabilityAlerts). Two days is two missed daily checks.
PULSE_HOURS = 48
STAMP = "%Y-%m-%dT%H:%M:%SZ"


class Unreadable(Exception):
    pass


@functools.cache
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
        if key in declared and live.get(key) != declared[key]
    ]


def ruleset_drift(declared: list[dict], summaries: list[dict], live: list[dict]) -> list[str]:
    """`summaries` is the repository's ruleset list (with parents); `live` the full repository rulesets."""
    found = []
    helper = _policy_json()
    if helper.ruleset_summaries(summaries, expected=False) != helper.ruleset_summaries(declared, expected=True):
        found.append("the set of rulesets (name, target, enforcement, source) differs from tools/github")
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


def renovate_silent(pulls: list[dict], now: datetime) -> str | None:
    """None when Renovate has an open pull request or opened one within PULSE_HOURS."""
    if any(pull.get("state") == "OPEN" for pull in pulls):
        return None
    since = now - timedelta(hours=PULSE_HOURS)
    last = max((stamp(pull["createdAt"]) for pull in pulls), default=None)
    if last is not None and last >= since:
        return None
    seen = last.strftime(STAMP) if last else "never"
    return (
        f"main has advisories, and Renovate has no open pull request and opened none for {PULSE_HOURS} hours "
        f"(last opened: {seen}). Either it cannot write (the app, the non-main firewall's bypass actor: "
        f"`configure-public-repository.sh verify`), or no release fixes the advisory (an ADR-0414 allow-list entry)"
    )


def gh(*args: str) -> str:
    try:
        # check=False: the status is read below and raised as Unreadable with gh's own message.
        done = subprocess.run(
            ["gh", *args], capture_output=True, text=True, encoding="utf-8", timeout=120, check=False
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise Unreadable(f"gh {' '.join(args[:2])}: {error}") from error
    if done.returncode != 0:
        raise Unreadable(f"gh {' '.join(args[:2])}: {done.stderr.strip()}")
    return done.stdout


def read_json(*args: str):
    text = gh(*args)
    try:
        return json.loads(text)
    except json.JSONDecodeError as error:
        raise Unreadable(f"gh {' '.join(args[:2])}: not JSON ({error})") from error


def main_has_advisories(repo: str, cargo_outcome: str) -> bool | None:
    """True, False, or None when it cannot be told (then the pulse is not judged)."""
    if cargo_outcome == "failure":
        return True
    runs = read_json(
        "run", "list", "--repo", repo, "--workflow", "monorepo-guard.yml", "--branch", "main",
        "--event", "schedule", "--status", "completed", "--limit", "1", "--json", "databaseId,conclusion",
    )
    if not runs:
        return None if cargo_outcome != "success" else False
    if runs[0].get("conclusion") == "success":
        return False if cargo_outcome == "success" else None
    jobs = read_json("run", "view", str(runs[0]["databaseId"]), "--repo", repo, "--json", "jobs")["jobs"]
    outcomes = {
        step.get("conclusion")
        for job in jobs
        for step in job.get("steps") or []
        if step.get("name") == "Repository OSV vulnerability ratchet"
    }
    if "failure" in outcomes:
        return True
    return False if outcomes == {"success"} and cargo_outcome == "success" else None


def check(summary: Path, cargo_outcome: str, now: datetime) -> int:
    repo = os.environ["GH_REPO"]
    found: list[str] = []
    unreadable: list[str] = []

    def signal(name, read):
        try:
            found.extend(read())
        except (Unreadable, KeyError, TypeError) as error:
            unreadable.append(f"{name}: {error}")

    signal("settings", lambda: settings_drift(
        json.loads((POLICY / "repository-settings.json").read_text(encoding="utf-8")), read_json("api", f"repos/{repo}")
    ))

    def rulesets():
        summaries = read_json("api", f"repos/{repo}/rulesets?includes_parents=true&per_page=100")
        live = [read_json("api", f"repos/{repo}/rulesets/{entry['id']}") for entry in summaries
                if entry.get("source_type") == "Repository"]
        return ruleset_drift([json.loads((POLICY / name).read_text(encoding="utf-8")) for name in RULESETS], summaries, live)

    signal("rulesets", rulesets)

    def pulse():
        advisories = main_has_advisories(repo, cargo_outcome)
        if advisories is None:
            raise Unreadable("whether main has advisories (the advisory checks did not finish)")
        if not advisories:
            return []
        pulls = read_json("pr", "list", "--repo", repo, "--state", "all", "--author", "app/renovate",
                          "--limit", "30", "--json", "number,state,createdAt")
        silence = renovate_silent(pulls, now)
        return [silence] if silence else []

    signal("Renovate pulse", pulse)

    lines = [f"- {line}" for line in found] + [f"- could not read {line}" for line in unreadable]
    summary.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    result = "failure" if found else ("unreadable" if len(unreadable) == 3 else "success")
    summary.with_suffix(".result").write_text(result + "\n", encoding="utf-8")
    for line in unreadable:
        print(f"::warning title=repository-drift::could not read {line}")
    print("\n".join(lines) if lines else "repository-drift: none")
    return 0


def annotate(message: str) -> str:
    """A workflow-command message: GitHub reads `%`, CR and LF as escapes."""
    return message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def report(summary: Path) -> int:
    marker = summary.with_suffix(".result")
    result = marker.read_text(encoding="utf-8").strip() if marker.exists() else "missing"
    lines = summary.read_text(encoding="utf-8").splitlines() if summary.exists() else []
    print("\n".join(lines) if lines else "repository-drift: none")
    if result == "success":
        return 0
    if result != "failure":
        print(f"::warning title=repository-drift-unreadable::check result '{result}': no verdict")
        return 1
    findings = [line[2:] for line in lines if line.startswith("- ") and not line.startswith("- could not read ")]
    print(f"::error title={COUNT_TITLE}::{len(findings)}")
    for finding in findings[:FINDING_ANNOTATIONS]:
        print(f"::error title={FINDING_TITLE}::{annotate(finding)}")
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    checking = sub.add_parser("check")
    checking.add_argument("summary", type=Path)
    checking.add_argument("--advisories", default="", help="the outcome of the daily Cargo advisories step")
    checking.add_argument("--as-of")
    reporting = sub.add_parser("report")
    reporting.add_argument("summary", type=Path)
    args = parser.parse_args(argv)
    if args.command == "check":
        now = stamp(args.as_of) if args.as_of else datetime.now(timezone.utc).replace(microsecond=0)
        return check(args.summary, args.advisories, now)
    return report(args.summary)


if __name__ == "__main__":
    sys.exit(main())
