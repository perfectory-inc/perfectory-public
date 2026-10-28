#!/usr/bin/env python3
"""Which repository guards a pull request runs, from the changed paths (root ADR-0207).

`tools/ci/change-scope.json` (root ADR-0202) holds the one ordered list of the
guards `scripts/guard/monorepo-guard.sh` runs, under `guards`. Each entry either
names the paths whose change can change its verdict (`paths`), or runs on every
change, says why (`always`) and how long it may take on a CI runner (`budget_seconds`, root
ADR-0313; the runner fails a CI sweep in which it takes longer). A guard's own script, its `.py` twin, the
repository files its scripts name (followed through the scripts they name), and,
for `<guard>-self-test`, the scripts of `<guard>`, are inputs without being
listed. A pull request and the merge queue run the guards whose inputs they touch; every other event,
an unknown comparison, or a change to the runner, the shared fixture library, the
contract or a selector runs every guard.

`check` refuses a guard listed twice or with no script, an entry that is neither
scoped nor justified, a glob that names nothing, a guard script nothing runs or two
things run, and a repository path a scoped guard names in quotes that is not one of its inputs.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path, PurePosixPath
from typing import Iterable

import change_scope as cs

ROOT = cs.ROOT
GUARD_DIR = "scripts/guard"
RUNNER = f"{GUARD_DIR}/monorepo-guard.sh"
SELF = "scripts/ci/guard_scope.py"
# A change to how guards are chosen, or to what every guard shares, can change every verdict.
RUN_ALL = cs.RUN_ALL | {SELF, RUNNER}
RUN_ALL_GLOBS = (f"{GUARD_DIR}/lib/**",)
# A pull request and the merge queue are narrowed (root ADR-0313). main, the daily run and a manual
# run keep the whole sweep: it catches drift no diff shows.
SCOPED_EVENTS = cs.NARROWED_EVENTS
# What an `always` guard may cost: it runs on every pull request and every merge group. On
# 2026-10-28 one walked every compose file for 8.5 minutes (16 on some runners), six times per merge.
MAX_ALWAYS_BUDGET_SECONDS = 60
# Guards name more kinds of file than workflows do (env files, documents, SQL, schemas).
NAMED_FILE = re.compile(
    r"(?<![\w./-])((?:[\w.-]+/)+[\w.-]+\.(?:sh|py|json|jsonc|toml|yml|yaml|env|md|txt|sql|avsc|mjs|rs|tsv|css|sha256))\b"
)
SELF_TEST = "-self-test"
# How a guard script starts another.
CALL = re.compile(
    r"""^\s*(?:exec\s+)?(?:bash|python3?)\s+"?(?:\$\{?\w+\}?/)?(?:(?:\./)?scripts/guard/)?([\w.-]+\.(?:sh|py))\b""",
    re.MULTILINE,
)


def guards(contract: dict) -> list[dict]:
    return list(contract.get("guards", {}).get("run", []))


def guard_files(root: Path, name: str) -> list[str]:
    return [f"{GUARD_DIR}/{name}{suffix}" for suffix in (".sh", ".py") if (root / GUARD_DIR / f"{name}{suffix}").is_file()]


def named_closure(root: Path, start: Iterable[str]) -> set[str]:
    """`start` and every repository file they name, following named scripts transitively."""
    seen: set[str] = set()
    queue = [p for p in start if (root / p).is_file()]
    while queue:
        path = queue.pop()
        if path in seen:
            continue
        seen.add(path)
        if not path.endswith((".sh", ".py")):
            continue
        try:
            text = (root / path).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        for literal in NAMED_FILE.findall(text):
            candidate = PurePosixPath(literal).as_posix()
            if ".." not in PurePosixPath(candidate).parts and (root / candidate).is_file():
                queue.append(candidate)
    return seen


def _entry(contract: dict, name: str) -> dict | None:
    return next((e for e in guards(contract) if e.get("name") == name), None)


def guard_inputs(root: Path, contract: dict, name: str) -> tuple[list[re.Pattern[str]], set[str]]:
    """Patterns and exact paths whose change can change `name`'s verdict."""
    globs = list((_entry(contract, name) or {}).get("paths", []))
    files = guard_files(root, name)
    if name.endswith(SELF_TEST):
        # The guard under test is an input; the tree it reads is not, unless listed:
        # a self-test plants fixtures, and the guard itself runs when its tree changes.
        files += guard_files(root, name[: -len(SELF_TEST)])
    return [cs.glob_regex(g) for g in globs], named_closure(root, files)


def runs_everything(event: str, changed: set[str] | None) -> str | None:
    if event not in SCOPED_EVENTS:
        return f"{event or 'a local run'} runs every guard"
    if changed is None:
        return "changed paths unavailable"
    for path in sorted(changed):
        if path in RUN_ALL or any(cs.glob_regex(g).match(path) for g in RUN_ALL_GLOBS):
            return f"{path} changed"
    return None


def select(contract: dict, event: str, changed: set[str] | None, root: Path = ROOT) -> tuple[list[str], str]:
    names = [e["name"] for e in guards(contract)]
    reason = runs_everything(event, changed)
    if reason:
        return names, reason
    assert changed is not None
    chosen = []
    for entry in guards(contract):
        if "always" in entry:
            chosen.append(entry["name"])
            continue
        inputs = guard_inputs(root, contract, entry["name"])
        if any(cs.matches(path, inputs) for path in changed):
            chosen.append(entry["name"])
    return chosen, f"{len(chosen)} of {len(names)} guards read one of {len(changed)} changed paths"


# --- check ---------------------------------------------------------------------------------------


def started_by(guard_dir: Path) -> dict[str, list[str]]:
    """Guard script -> the other guard scripts that start it. A self-test running its
    guard on a synthetic fixture is not a second run on the tree, so it does not count."""
    started: dict[str, list[str]] = {}
    for path in sorted(guard_dir.glob("*.sh")):
        if path.name.endswith(f"{SELF_TEST}.sh") or path.name == "monorepo-guard.sh":
            continue
        for called in CALL.findall(path.read_text(encoding="utf-8")):
            if called != path.name:
                started.setdefault(called, []).append(path.name)
    return started


def run_elsewhere(root: Path) -> str:
    """Text of what runs guard scripts besides the list: workflows, hooks, the runner prologue."""
    parts = [p.read_text(encoding="utf-8") for p in sorted((root / cs.WORKFLOWS).glob("*.y*ml"))]
    # The runner starts its identity and legal gates by name before the list.
    for extra in ("lefthook.yml", RUNNER):
        if (root / extra).is_file():
            parts.append((root / extra).read_text(encoding="utf-8"))
    return "\n".join(parts)


def check(root: Path = ROOT, tracked: Iterable[str] | None = None) -> list[str]:
    contract = cs.load_contract(root)
    errors: list[str] = []
    if tracked is None:
        tracked = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z"], check=True, capture_output=True
        ).stdout.decode().split("\0")
    tracked = [t for t in tracked if t]
    entries = guards(contract)
    if not entries:
        return [f"{cs.CONTRACT}: `guards.run` lists no guard"]
    names = [str(e.get("name", "")) for e in entries]
    for name in sorted({n for n in names if names.count(n) > 1}):
        errors.append(f"guard {name}: listed {names.count(name)} times; each guard runs once")
    for entry in entries:
        name = str(entry.get("name", ""))
        if not (root / GUARD_DIR / f"{name}.sh").is_file():
            errors.append(f"guard {name}: no {GUARD_DIR}/{name}.sh")
            continue
        scoped, always = "paths" in entry, "always" in entry
        if scoped == always:
            errors.append(f"guard {name}: give exactly one of `paths` (what it reads) or `always` (why it reads everything)")
            continue
        if always:
            if not str(entry["always"]).strip():
                errors.append(f"guard {name}: `always` states why the guard reads every change")
            budget = entry.get("budget_seconds")
            if type(budget) is not int or not 0 < budget <= MAX_ALWAYS_BUDGET_SECONDS:
                errors.append(
                    f"guard {name}: an `always` guard declares `budget_seconds`, 1..{MAX_ALWAYS_BUDGET_SECONDS}, "
                    "the CI time it may take on every change; give it `paths` if it needs more"
                )
            continue
        if "budget_seconds" in entry:
            errors.append(f"guard {name}: `budget_seconds` belongs to an `always` guard")
        globs = entry["paths"]
        if len(globs) != len(set(globs)):
            errors.append(f"guard {name}: duplicate paths")
        for glob in globs:
            if not any(cs.glob_regex(glob).match(t) for t in tracked):
                errors.append(f"guard {name}: input `{glob}` names no tracked file (stale)")
        errors += unlisted_reads(root, contract, name, tracked)
    guard_dir = root / GUARD_DIR
    listed = {f"{n}.sh" for n in names}
    started = started_by(guard_dir)
    # Each guard runs once per sweep: from the list, or from exactly one other guard.
    # public-github-policy.sh used to re-run seven listed self-tests (root ADR-0207).
    for script, parents in sorted(started.items()):
        callers = (["the guard list"] if script in listed else []) + parents
        if len(callers) > 1:
            errors.append(f"{GUARD_DIR}/{script}: runs {len(callers)} times per sweep ({', '.join(callers)}); keep one caller")
    elsewhere = run_elsewhere(root)
    for path in sorted(guard_dir.glob("*.sh")):
        if path.name == "monorepo-guard.sh" or path.name in listed or path.name in started:
            continue
        if f"{GUARD_DIR}/{path.name}" in elsewhere:
            continue
        errors.append(f"{GUARD_DIR}/{path.name}: nothing runs it; list it under `guards.run` or delete it")
    return errors


def unlisted_reads(root: Path, contract: dict, name: str, tracked: list[str]) -> list[str]:
    """Every repository path a scoped guard's own scripts quote must be one of its inputs."""
    inputs = guard_inputs(root, contract, name)
    tracked_set = set(tracked)
    tracked_dirs = {p for t in tracked for p in map(str, PurePosixPath(t).parents) if p != "."}
    errors = set()
    for source in guard_files(root, name):
        text = (root / source).read_text(encoding="utf-8")
        for path in repository_reads(text, tracked_set, tracked_dirs):
            probe = path if path in tracked_set else f"{path}/x"
            if not cs.matches(probe, inputs):
                errors.add(f"guard {name}: {source} reads `{path}`, which is not an input; add it to its `paths`")
    return sorted(errors)


def repository_reads(text: str, tracked: set[str], tracked_dirs: set[str]) -> set[str]:
    """Repository-root paths (files or directories) a guard quotes, outside comment lines."""
    code = "\n".join(line for line in text.splitlines() if not cs.COMMENT.match(line))
    reads = set()
    for literal in set(cs.QUOTED_PATH.findall(code)):
        path = cs._resolve(PurePosixPath(), literal.rstrip("/"))
        if path in tracked or path in tracked_dirs:
            reads.add(path)
    return reads


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("select", help="print the guards to run, one per line, in contract order")
    sub.add_parser("check", help="the guard list and the guard scripts agree")
    sub.add_parser("budgets", help="print `<guard> <seconds>` for every `always` guard")
    args = parser.parse_args(argv)
    if args.command == "check":
        errors = check()
        for error in errors:
            print(f"FAIL guard-scope: {error}", file=sys.stderr)
        if not errors:
            print("OK guard-scope")
        return 1 if errors else 0
    if args.command == "budgets":
        for entry in guards(cs.load_contract()):
            if "always" in entry:
                print(f"{entry['name']} {entry.get('budget_seconds', 0)}")
        return 0
    event = os.environ.get("CHANGE_SCOPE_EVENT", "")
    verified = cs.push_verified_by_queue("monorepo-guard.yml")
    if verified:
        print(f"guard-scope: the merge queue passed this tree, no guard runs again: {verified}", file=sys.stderr)
        return 0
    changed = None
    if event in SCOPED_EVENTS:
        changed = cs.git_changed_paths(ROOT, os.environ.get("CHANGE_SCOPE_BASE", ""), os.environ.get("CHANGE_SCOPE_HEAD", ""))
    contract = cs.load_contract()
    for entry in guards(contract):
        if not (ROOT / GUARD_DIR / f"{entry.get('name', '')}.sh").is_file():
            print(f"FAIL guard-scope: {entry.get('name')} has no script", file=sys.stderr)
            return 1
    chosen, reason = select(contract, event, changed)
    print(f"guard-scope: {reason}", file=sys.stderr)
    print("\n".join(chosen))
    return 0


if __name__ == "__main__":
    sys.exit(main())
