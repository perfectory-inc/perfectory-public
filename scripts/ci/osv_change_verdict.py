#!/usr/bin/env python3
"""Judge a pull request's OSV findings by what the change did (root ADR-0568).

  osv_change_verdict.py REPORT --root DIR --base SHA [--fix-pulls FILE] [--now TIME] [--repository OWNER/NAME]

`cargo xtask ci osv-gate` calls this only for a pull request or a merge group, and only after the
scanner reported findings the allow list (`osv-scanner.toml`, root ADR-0414) does not cover. A push to
main and the nightly run never come here: they judge every finding as before.

An advisory published about a package main already locks arrives with no commit. On 2026-11-13..18
it happened four times (basic-ftp #1375, quinn-udp #1379, next, then picomatch, chrono and libz-sys)
and each one failed every pull request until someone fixed main, whatever the pull request touched.
So, per finding:

- in a lockfile the change touches (`git diff --name-only BASE HEAD`): it fails. A change that brings
  or keeps a vulnerable version in the files it edits is judged as before;
- in a lockfile the change leaves as it is: it fails, unless a pull request labelled
  `fix_pull_request_label` that changes that lockfile or the allow list and names one of the finding's
  ids or aliases is open, and the FIRST such pull request (any state) was opened less than
  `grace_hours[severity]` ago (`tools/osv-gate.contract.json`). Then it is a warning: the fix is on its
  way and blocking this change does not bring it sooner. When the grace ends with the fix not merged,
  every pull request fails again, as today.

Whatever cannot be read (the report's findings, the base, the pull requests, a lockfile path) counts
as "touched" and "no fix": the verdict falls back to failing, never to passing.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "tools" / "osv-gate.contract.json"
STAMP = "%Y-%m-%dT%H:%M:%SZ"
# Labelled pull requests read at most; reaching it fails closed (the first fix could be cut off).
LIMIT = 500


@dataclass(frozen=True)
class Finding:
    lockfile: str  # relative to the repository's top level; "" when it could not be told
    package: str
    ids: tuple[str, ...]
    aliases: tuple[str, ...]
    severity: str  # the group's max_severity; "" when unscored

    def names(self) -> str:
        return ",".join(self.ids)


def load_contract(path: Path = CONTRACT) -> dict:
    contract = json.loads(path.read_text(encoding="utf-8"))
    missing = {"fix_pull_request_label", "allow_list", "critical_from", "high_from", "grace_hours"} - set(contract)
    if missing:
        raise ValueError(f"{path}: missing {sorted(missing)}")
    hours = contract["grace_hours"]
    if set(hours) != {"critical", "high", "other", "unscored"}:
        raise ValueError(f"{path}: grace_hours must name critical, high, other and unscored")
    if not all(isinstance(value, int) and 0 <= value <= 168 for value in hours.values()):
        raise ValueError(f"{path}: every grace is a whole number of hours, at most 168 (7 days)")
    if hours["unscored"] > hours["critical"]:
        raise ValueError(f"{path}: an unscored advisory (unknown severity) gets no more than a critical one")
    if not contract["critical_from"] >= contract["high_from"] > 0:
        raise ValueError(f"{path}: critical_from must be at least high_from, and both above 0")
    if not contract["fix_pull_request_label"] or not contract["allow_list"]:
        raise ValueError(f"{path}: fix_pull_request_label and allow_list must be set")
    return contract


def severity_class(score: str, contract: dict) -> str:
    try:
        value = float(score)
    except (TypeError, ValueError):
        return "unscored"
    if value >= contract["critical_from"]:
        return "critical"
    if value >= contract["high_from"]:
        return "high"
    return "other"


def toplevel(root: Path) -> Path | None:
    shown = subprocess.run(["git", "-C", str(root), "rev-parse", "--show-toplevel"], capture_output=True, text=True)
    return Path(shown.stdout.strip()) if shown.returncode == 0 and shown.stdout.strip() else None


def relative_lockfile(path: str, top: Path | None) -> str:
    """The lockfile relative to the repository's top level (what `git diff` names), or "" when it is
    not under it: a path the diff cannot name counts as touched."""
    if top is None:
        return ""
    try:
        return Path(path).resolve().relative_to(top.resolve()).as_posix()
    except ValueError:
        return ""


def findings(report: dict, top: Path | None) -> list[Finding]:
    """Every (lockfile, package, advisory group) of the report. A shape this does not know raises."""
    found = []
    for result in report["results"]:
        lockfile = relative_lockfile(result["source"]["path"], top)
        for package in result["packages"]:
            meta = package["package"]
            name = f"{meta['ecosystem']}/{meta['name']}@{meta.get('version', '')}"
            for group in package["groups"]:
                ids = set(group["ids"])
                if not ids:
                    raise ValueError(f"{lockfile}: {name} has an advisory group without ids")
                found.append(
                    Finding(
                        lockfile=lockfile,
                        package=name,
                        ids=tuple(sorted(ids)),
                        aliases=tuple(sorted(set(group.get("aliases") or []) | ids)),
                        severity=str(group.get("max_severity") or ""),
                    )
                )
    return found


def opened(pull: dict) -> datetime:
    return datetime.strptime(pull["createdAt"], STAMP).replace(tzinfo=timezone.utc)


def fixes_for(finding: Finding, pulls: list[dict], contract: dict) -> list[dict]:
    """Pull requests (any state) that count as this finding's fix: they carry the label, name one of its
    ids or aliases in the title or body (case ignored, not part of a longer id), and change the
    finding's lockfile or the allow list. A body that names an id and changes neither is not a fix."""
    label = contract["fix_pull_request_label"]
    words = [
        re.compile(rf"(?<![A-Za-z0-9-]){re.escape(name)}(?![A-Za-z0-9-])", re.IGNORECASE)
        for name in sorted({*finding.ids, *finding.aliases})
    ]
    targets = {finding.lockfile, contract["allow_list"]} - {""}
    return [
        pull
        for pull in pulls
        if label in {entry.get("name") for entry in pull.get("labels") or []}
        and any(word.search(f"{pull.get('title') or ''}\n{pull.get('body') or ''}") for word in words)
        and targets & {entry.get("path") for entry in pull.get("files") or []}
    ]


def verdict(finding: Finding, touched: set[str] | None, pulls: list[dict], contract: dict, now: datetime) -> tuple[bool, str]:
    """(passes, why) for one finding.

    | lockfile          | fix pull request                          | verdict |
    |-------------------|-------------------------------------------|---------|
    | touched / unknown | any                                       | fail    |
    | untouched         | none open                                 | fail    |
    | untouched         | one open, the first fix opened < grace    | warning |
    | untouched         | one open, the first fix opened >= grace   | fail    |

    The clock starts when the FIRST fix (open, closed or merged) was opened: closing it and opening
    another does not restart it.
    """
    if touched is None:
        return False, "the base could not be read, so every lockfile counts as touched"
    if not finding.lockfile or finding.lockfile in touched:
        return False, "this change touches the lockfile (or its path could not be told apart)"
    fixes = fixes_for(finding, pulls, contract)
    still_open = [pull for pull in fixes if pull.get("state", "OPEN") == "OPEN"]
    if not still_open:
        label = contract["fix_pull_request_label"]
        return False, f"main already locks it and no open '{label}' pull request that changes it names it"
    first = min(fixes, key=opened)
    severity = severity_class(finding.severity, contract)
    until = opened(first) + timedelta(hours=contract["grace_hours"][severity])
    if now >= until:
        return False, (
            f"its first fix #{first['number']} was opened {first['createdAt']}; "
            f"the grace ({severity}) ended {until.strftime(STAMP)}"
        )
    return True, f"main already locks it; fix #{still_open[0]['number']} is open, grace ({severity}) until {until.strftime(STAMP)}"


def touched_files(root: Path, base: str) -> set[str] | None:
    if not base or set(base) == {"0"}:
        return None
    present = subprocess.run(["git", "-C", str(root), "cat-file", "-e", f"{base}^{{commit}}"], capture_output=True)
    if present.returncode != 0:
        fetched = subprocess.run(
            ["git", "-C", str(root), "fetch", "-q", "--no-tags", "--depth=1", "origin", base], capture_output=True
        )
        if fetched.returncode != 0:
            return None
    diff = subprocess.run(["git", "-C", str(root), "diff", "-z", "--name-only", base, "HEAD"], capture_output=True)
    if diff.returncode != 0:
        return None
    return {name for name in diff.stdout.decode("utf-8", "surrogateescape").split("\0") if name}


def fix_pulls(repository: str, label: str) -> list[dict] | None:
    """Every pull request (any state) carrying the label, or None when GitHub could not be read."""
    try:
        listed = subprocess.run(
            [
                "gh", "pr", "list", "--repo", repository, "--state", "all", "--label", label,
                "--limit", str(LIMIT), "--json", "number,title,body,createdAt,labels,state,files",
            ],
            capture_output=True, text=True, encoding="utf-8", timeout=120,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        print(f"osv-change-verdict: cannot list pull requests ({error})", file=sys.stderr)
        return None
    if listed.returncode != 0:
        print(f"osv-change-verdict: gh pr list failed ({listed.stderr.strip()})", file=sys.stderr)
        return None
    pulls = json.loads(listed.stdout or "[]")
    if len(pulls) >= LIMIT:
        print(f"osv-change-verdict: {LIMIT} or more labelled pull requests; the first fix may be cut off", file=sys.stderr)
        return None
    return pulls


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("report", type=Path)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--base", default="")
    parser.add_argument("--fix-pulls", type=Path, help="a gh pr list --json file instead of asking GitHub")
    parser.add_argument("--now", help="the time to judge at (YYYY-MM-DDTHH:MM:SSZ); default now")
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY", ""))
    parser.add_argument("--contract", type=Path, default=CONTRACT)
    args = parser.parse_args(argv)

    annotate = os.environ.get("GITHUB_ACTIONS") == "true"
    contract = load_contract(args.contract)
    report = json.loads(args.report.read_text(encoding="utf-8"))
    found = findings(report, toplevel(args.root))
    if not found:
        # The scanner said there are findings (the only way here); reading none is a report shape this
        # does not know, not a clean tree.
        print("osv-change-verdict: the scanner reported findings but none could be read; failing")
        return 1
    now = (
        datetime.strptime(args.now, STAMP).replace(tzinfo=timezone.utc)
        if args.now
        else datetime.now(timezone.utc).replace(microsecond=0)
    )
    touched = touched_files(args.root, args.base)
    if args.fix_pulls:
        pulls = json.loads(args.fix_pulls.read_text(encoding="utf-8"))
    else:
        pulls = fix_pulls(args.repository, contract["fix_pull_request_label"]) if args.repository else None
        if pulls is None:
            message = "the pull requests could not be read, so no fix counts and the grace does not apply"
            print(f"osv-change-verdict: {message}")
            if annotate:
                print(f"::warning title=advisory-grace::{message}")
            pulls = []
    failed = 0
    for finding in found:
        passes, why = verdict(finding, touched, pulls, contract, now)
        where = finding.lockfile or "?"
        print(f"{'GRACE' if passes else 'FAIL'} {finding.names()} {finding.package} in {where}: {why}")
        if annotate:
            # Not `advisory`: that title is main's, which the host reads (root ADR-0278).
            title, level = ("advisory-grace", "warning") if passes else ("advisory-change", "error")
            print(f"::{level} title={title}::{finding.names()} {finding.package.rsplit('@', 1)[0]} ({where}): {why}")
        failed += not passes
    print(f"osv-change-verdict: {failed} failing finding(s) of {len(found)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
