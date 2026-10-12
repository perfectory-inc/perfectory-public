#!/usr/bin/env python3
"""Decide whether a pull-request workflow does its work, from the changed paths (root ADR-0201).

`tools/ci/change-scope.json` is the one place an area's inputs are written. Every
scoped workflow starts on every pull request and reports its required context; its
`scope` job runs `select`, and the other jobs run only when the answer is `true`.
`check` refuses a workflow that is in neither list, a scoped workflow that keeps a
`paths:` filter or skips the shared scope job, and an input glob that names nothing.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path, PurePosixPath
from typing import Iterable

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = "tools/ci/change-scope.json"
SELECTOR = "scripts/ci/change_scope.py"
WORKFLOWS = ".github/workflows"
# A change to how scope is decided can change every decision.
RUN_ALL = frozenset({CONTRACT, SELECTOR})
ALWAYS_RUN_EVENTS = frozenset({"workflow_dispatch", "schedule"})
# The verification tool every area CI runs lives outside every area (ADR-0004, ADR-0010).
XTASK_RUN = re.compile(r"cargo\s+.*xtask\s+(verify|integration)")
XTASK_INPUTS = ("tools/xtask/**", ".cargo/config.toml", "rust-toolchain.toml")
# Repository files a workflow names in its steps are its inputs without being listed.
NAMED_FILE = re.compile(r"(?<![\w./-])((?:[\w.-]+/)+[\w.-]+\.(?:sh|py|json|toml|yml|yaml))\b")
SCOPE_IF = "${{ needs.scope.outputs.affected == 'true' }}"
SCOPE_ENV = "CHANGE_SCOPE_RUN: ${{ needs.scope.outputs.affected }}"


def glob_regex(glob: str) -> re.Pattern[str]:
    """GitHub `paths` glob semantics for the forms in use: `**`, `*`, literal."""
    out, i = [], 0
    while i < len(glob):
        if glob.startswith("**/", i):
            out.append("(?:.*/)?")
            i += 3
        elif glob.startswith("**", i):
            out.append(".*")
            i += 2
        elif glob[i] == "*":
            out.append("[^/]*")
            i += 1
        else:
            out.append(re.escape(glob[i]))
            i += 1
    return re.compile("".join(out) + r"\Z")


def load_contract(root: Path = ROOT) -> dict:
    return json.loads((root / CONTRACT).read_text(encoding="utf-8"))


def named_files(root: Path, text: str, working_directory: str) -> set[str]:
    found = set()
    for literal in NAMED_FILE.findall(text):
        for candidate in (literal, f"{working_directory}/{literal}" if working_directory else literal):
            path = PurePosixPath(candidate).as_posix()
            if ".." not in PurePosixPath(path).parts and (root / path).is_file():
                found.add(path)
    return found


def default_working_directory(text: str) -> str:
    match = re.search(r"^defaults:\n  run:\n    working-directory: (\S+)$", text, re.MULTILINE)
    return match.group(1) if match else ""


def workflow_inputs(root: Path, workflow: str, globs: Iterable[str]) -> tuple[list[re.Pattern[str]], set[str]]:
    path = f"{WORKFLOWS}/{workflow}"
    text = (root / path).read_text(encoding="utf-8")
    exact = {path} | named_files(root, text, default_working_directory(text))
    return [glob_regex(g) for g in globs], exact


def decide(
    contract: dict, workflow: str, event: str, changed: set[str] | None, root: Path = ROOT
) -> tuple[bool, str]:
    entry = contract["workflows"].get(workflow)
    if entry is None:
        raise ValueError(f"{workflow} is not a scoped workflow in {CONTRACT}")
    if event in ALWAYS_RUN_EVENTS:
        return True, f"{event} runs everything"
    if event == "merge_group" and entry["merge_group"] == "full":
        return True, "merge_group: full"
    if changed is None:
        return True, "changed paths unavailable"
    if changed & RUN_ALL:
        return True, "the scope definition changed"
    patterns, exact = workflow_inputs(root, workflow, entry["paths"])
    for path in sorted(changed):
        if path in exact or any(p.match(path) for p in patterns):
            return True, f"input changed: {path}"
    return False, f"none of {len(changed)} changed paths is an input"


def git_changed_paths(root: Path, base: str, head: str) -> set[str] | None:
    if not base or not head or set(base) == {"0"}:
        return None
    # Three dots: what the head adds since it left the base, not what the base
    # gained meanwhile. Equal to two dots when the base is an ancestor (push, queue).
    command = ["git", "-C", str(root), "diff", "--name-only", "-z", "--no-renames", f"{base}...{head}"]
    try:
        out = subprocess.run(command, check=True, capture_output=True).stdout
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"change-scope: git comparison unavailable, running everything: {error}", file=sys.stderr)
        return None
    return {os.fsdecode(p) for p in out.split(b"\0") if p}


# --- check: the contract and the workflows agree -------------------------------------------------


def _jobs(text: str) -> dict[str, str]:
    """Job id -> its block, for the canonical two-space `jobs:` dialect the policy guard admits."""
    body = text.split("\njobs:\n", 1)[1] if "\njobs:\n" in text else ""
    jobs, current = {}, None
    for line in body.splitlines():
        if line and not line.startswith(" "):
            break
        match = re.match(r"^  ([A-Za-z0-9_-]+):\s*$", line)
        if match:
            current = match.group(1)
            jobs[current] = ""
        elif current:
            jobs[current] += line + "\n"
    return jobs


def _triggers_pull_request(text: str) -> bool:
    return re.search(r"^  pull_request:\s*$", text, re.MULTILINE) is not None


def check_workflow(workflow: str, text: str) -> list[str]:
    errors = []
    on_block = text.split("\njobs:\n", 1)[0]
    if re.search(r"^\s+paths(-ignore)?:", on_block, re.MULTILINE):
        errors.append(f"{workflow}: a scoped workflow keeps no `paths:` filter; its inputs live in {CONTRACT}")
    jobs = _jobs(text)
    scope = jobs.get("scope", "")
    if f"run: python3 {SELECTOR} select --workflow {workflow}\n" not in scope or "affected: ${{ steps.select.outputs.affected }}" not in scope:
        errors.append(f"{workflow}: job `scope` must run `python3 {SELECTOR} select --workflow {workflow}` and output `affected`")
    for job, block in jobs.items():
        if job == "scope":
            continue
        if re.search(r"^    name: required/", block, re.MULTILINE):
            if SCOPE_ENV not in block:
                errors.append(f"{workflow}: the required job must pass `{SCOPE_ENV}` to its result check")
            continue
        needs = re.search(r"^    needs: \[([^\]]*)\]", block, re.MULTILINE)
        if not needs or "scope" not in [n.strip() for n in needs.group(1).split(",")]:
            errors.append(f"{workflow}: job `{job}` must need `scope`")
        if f"    if: {SCOPE_IF}\n" not in block:
            errors.append(f"{workflow}: job `{job}` must run only `if: {SCOPE_IF}`")
    return errors


def check(root: Path = ROOT, tracked: Iterable[str] | None = None) -> list[str]:
    contract = load_contract(root)
    errors: list[str] = []
    scoped, unscoped = contract.get("workflows", {}), contract.get("unscoped", {})
    if tracked is None:
        tracked = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z"], check=True, capture_output=True
        ).stdout.decode().split("\0")
    tracked = [t for t in tracked if t]
    for workflow in sorted(set(scoped) & set(unscoped)):
        errors.append(f"{workflow}: both scoped and unscoped")
    for workflow, reason in unscoped.items():
        if not str(reason).strip():
            errors.append(f"{workflow}: an unscoped workflow states why")
    for name in sorted(set(scoped) | set(unscoped)):
        if not (root / WORKFLOWS / name).is_file():
            errors.append(f"{name}: listed in {CONTRACT} but no such workflow")
    for path in sorted((root / WORKFLOWS).glob("*.y*ml")):
        text = path.read_text(encoding="utf-8")
        if _triggers_pull_request(text) and path.name not in scoped and path.name not in unscoped:
            errors.append(
                f"{path.name}: a pull-request workflow must be scoped in {CONTRACT} (or listed as unscoped with a reason)"
            )
    for workflow, entry in sorted(scoped.items()):
        if entry.get("merge_group") not in ("full", "scoped"):
            errors.append(f"{workflow}: merge_group must be `full` or `scoped`")
        globs = entry.get("paths") or []
        if not globs or len(globs) != len(set(globs)):
            errors.append(f"{workflow}: paths must be non-empty and duplicate-free")
        for glob in globs:
            pattern = glob_regex(glob)
            if not any(pattern.match(t) for t in tracked):
                errors.append(f"{workflow}: input `{glob}` names no tracked file (stale)")
        if not (root / WORKFLOWS / workflow).is_file():
            continue
        text = (root / WORKFLOWS / workflow).read_text(encoding="utf-8")
        errors += check_workflow(workflow, text)
        if XTASK_RUN.search(text):
            for required in XTASK_INPUTS:
                if required not in globs:
                    errors.append(
                        f"{workflow}: runs `cargo xtask verify|integration` but does not take `{required}` as an input"
                    )
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    select = sub.add_parser("select", help="write affected=true|false for one workflow to $GITHUB_OUTPUT")
    select.add_argument("--workflow", required=True)
    paths = sub.add_parser("paths", help="print one scoped workflow's input globs")
    paths.add_argument("--workflow", required=True)
    paths.add_argument("--root", type=Path, default=ROOT)
    sub.add_parser("check", help="the contract and the workflows agree")
    args = parser.parse_args(argv)

    if args.command == "paths":
        entry = load_contract(args.root)["workflows"].get(args.workflow)
        if entry is not None:
            print("\n".join(entry["paths"]))
        return 0
    if args.command == "check":
        errors = check()
        for error in errors:
            print(f"FAIL change-scope: {error}", file=sys.stderr)
        if not errors:
            print("OK change-scope")
        return 1 if errors else 0

    event = os.environ.get("CHANGE_SCOPE_EVENT", "")
    changed = git_changed_paths(ROOT, os.environ.get("CHANGE_SCOPE_BASE", ""), os.environ.get("CHANGE_SCOPE_HEAD", ""))
    run, reason = decide(load_contract(), args.workflow, event, changed)
    print(json.dumps({"workflow": args.workflow, "event": event, "run": run, "reason": reason}, ensure_ascii=False))
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8", newline="\n") as handle:
            handle.write(f"affected={str(run).lower()}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
