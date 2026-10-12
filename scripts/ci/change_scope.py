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
# The merge queue is the last gate before main: it runs everything, whatever the scope.
ALWAYS_RUN_EVENTS = frozenset({"workflow_dispatch", "schedule", "merge_group"})
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


def matches(path: str, inputs: tuple[list[re.Pattern[str]], set[str]]) -> bool:
    patterns, exact = inputs
    return path in exact or any(p.match(path) for p in patterns)


def claimed(contract: dict, path: str, root: Path = ROOT) -> bool:
    """Whether some workflow is known to answer for this path: a scoped workflow
    takes it as an input, or an unscoped workflow (which always runs) owns it."""
    for workflow, entry in contract["workflows"].items():
        if matches(path, workflow_inputs(root, workflow, entry["paths"])):
            return True
    for workflow, entry in contract["unscoped"].items():
        if path == f"{WORKFLOWS}/{workflow}" or any(glob_regex(g).match(path) for g in entry["owns"]):
            return True
    return False


def decide(
    contract: dict, workflow: str, event: str, changed: set[str] | None, root: Path = ROOT
) -> tuple[bool, str]:
    entry = contract["workflows"].get(workflow)
    if entry is None:
        raise ValueError(f"{workflow} is not a scoped workflow in {CONTRACT}")
    if event in ALWAYS_RUN_EVENTS:
        return True, f"{event} runs everything"
    if changed is None:
        return True, "changed paths unavailable"
    if changed & RUN_ALL:
        return True, "the scope definition changed"
    inputs = workflow_inputs(root, workflow, entry["paths"])
    for path in sorted(changed):
        if matches(path, inputs):
            return True, f"input changed: {path}"
    # A path no workflow answers for may be read by anything: run everything.
    for path in sorted(changed):
        if not claimed(contract, path, root):
            return True, f"no area claims {path}"
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
    for workflow, entry in sorted(unscoped.items()):
        if not str(entry.get("reason", "")).strip():
            errors.append(f"{workflow}: an unscoped workflow states why")
        for glob in entry.get("owns", []):
            if not any(glob_regex(glob).match(t) for t in tracked):
                errors.append(f"{workflow}: owned glob `{glob}` names no tracked file (stale)")
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
        errors += unlisted_reads(root, workflow, globs, tracked)
    return errors


# --- what an area's sources read must be an input ------------------------------------------------

SOURCE_SUFFIXES = (".rs", ".ts", ".tsx", ".js", ".mjs", ".cjs", ".py", ".sh")
# A quoted relative path: what include_str!/read_to_string/readFileSync/open receive.
QUOTED_PATH = re.compile(r"""["'`]((?:\.\.?/)*[\w@.-]+(?:/[\w@.-]+)+/?)["'`]""")
# A quoted bare file name, joined onto a directory the same source names (`resolve(docs, "a.json")`).
QUOTED_NAME = re.compile(r"""["'`]([\w@.-]+\.[A-Za-z0-9]+)["'`]""")
COMMENT = re.compile(r"^\s*(//|/\*|\*|#)")
# Areas are the scanned trees. tools/xtask names every area because it dispatches
# them; a change to it already reruns every workflow that runs it.
AREA_PREFIXES = ("platforms/", "products/", "consoles/")


def _resolve(base: PurePosixPath, literal: str) -> str | None:
    """The repository-relative path `literal` names from `base`, if it stays inside the repository."""
    parts: list[str] = list(base.parts)
    for part in PurePosixPath(literal).parts:
        if part == "..":
            if not parts:
                return None
            parts.pop()
        elif part != ".":
            parts.append(part)
    return "/".join(parts) if parts else None


def source_reads(source: str, text: str, tracked: set[str], tracked_dirs: set[str]) -> set[str]:
    """Repository paths (files, or directories nothing narrower explains) a source names in quotes.

    A quoted path is resolved from the repository root, the area root and the source's
    own directory; the first resolution that is tracked counts. A directory is
    narrowed by the bare file names and relative paths the same source joins onto it.
    Comment lines are ignored.
    """
    code = "\n".join(line for line in text.splitlines() if not COMMENT.match(line))
    area = PurePosixPath(*PurePosixPath(source).parts[:2])
    bases = (PurePosixPath(), area, PurePosixPath(source).parent)
    files: set[str] = set()
    dirs: set[str] = set()
    for literal in set(QUOTED_PATH.findall(code)):
        for base in bases:
            path = _resolve(base, literal.rstrip("/"))
            if path in tracked:
                files.add(path)
                break
            if path in tracked_dirs:
                dirs.add(path)
                break
    names = set(QUOTED_NAME.findall(code)) | set(QUOTED_PATH.findall(code))
    reads = set(files)
    for directory in dirs:
        joined = {_resolve(PurePosixPath(directory), name) for name in names} & tracked
        reads |= joined if joined else {directory}
    return reads


def unlisted_reads(root: Path, workflow: str, globs: list[str], tracked: list[str]) -> list[str]:
    """Every repository path an area's sources read must be one of the workflow's inputs."""
    inputs = workflow_inputs(root, workflow, globs)
    tracked_set = set(tracked)
    tracked_dirs = {p for t in tracked for p in map(str, PurePosixPath(t).parents) if p != "."}
    roots = [
        g[: -len("/**")]
        for g in globs
        if g.endswith("/**") and "*" not in g[: -len("/**")] and g.startswith(AREA_PREFIXES)
    ]
    errors: set[str] = set()
    for source in tracked:
        if not source.endswith(SOURCE_SUFFIXES) or not any(source.startswith(r + "/") for r in roots):
            continue
        try:
            text = (root / source).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        for path in source_reads(source, text, tracked_set, tracked_dirs):
            probe = path if path in tracked_set else f"{path}/x"
            if not matches(probe, inputs):
                errors.add(f"{workflow}: {source} reads `{path}`, which is not an input; add it to `paths`")
    return sorted(errors)


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
