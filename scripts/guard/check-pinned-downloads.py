#!/usr/bin/env python3
"""Prevents: a CI job losing a pull request to one 5xx from a release download (root ADR-0197).

Two rules, both read from the tree rather than from a list kept here:

1. A release asset is downloaded only by scripts/ci/fetch_pinned.py. The string
   `releases/download` may appear only in the registry (tools/pinned-artifacts.json), in the
   contracts that registry names, and in prose; a script or workflow that spells such a URL is
   downloading around the cache, the retry and the checksum.
2. Every workflow job that reaches the fetcher restores the pinned cache before it does. A job
   reaches the fetcher when a step names the fetcher, names a script that calls it, or runs
   `cargo xtask verify`, which runs the repository guard and with it actionlint. Every
   restore step is the same step, so the key and path cannot drift between jobs.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys

FETCHER = "scripts/ci/fetch_pinned.py"
REGISTRY = "tools/pinned-artifacts.json"
CACHE_PATH = "~/.cache/perfectory/pinned"
RESTORE_STEP = (
    "      - name: Restore pinned release binaries\n"
    "        uses: actions/cache/restore@55cc8345863c7cc4c66a329aec7e433d2d1c52a9 # v6.1.0\n"
    "        with:\n"
    f"          path: {CACHE_PATH}\n"
    "          key: pinned-release-binaries-v1-${{ runner.os }}\n"
    "          restore-keys: pinned-release-binaries-v1-${{ runner.os }}-\n"
)
XTASK_ENTRY = "cargo xtask verify"
# This guard and its self-test spell the forbidden shape on purpose.
GUARD_FILES = frozenset(
    {"scripts/guard/check-pinned-downloads.py", "scripts/guard/pinned-downloads-self-test.sh"}
)
SCRIPT_SUFFIXES = (".sh", ".py", ".rs", ".ps1")
JOB = re.compile(r"^  ([A-Za-z0-9_-]+):\s*$")


def tracked(root: pathlib.Path) -> list[str]:
    output = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z"], check=True, capture_output=True
    ).stdout
    return [name for name in output.decode("utf-8").split("\0") if name]


def read(root: pathlib.Path, name: str) -> str:
    try:
        return (root / name).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return ""


def allowed_url_homes(root: pathlib.Path) -> set[str]:
    import json

    registry = json.loads(read(root, REGISTRY) or "{}")
    return {REGISTRY, *registry.get("contracts", [])}


def check_no_raw_downloads(root: pathlib.Path, files: list[str]) -> list[str]:
    homes = allowed_url_homes(root)
    failures = []
    for name in files:
        if name in homes or name in GUARD_FILES or name.endswith((".md", ".mdx")) or name.startswith("docs/"):
            continue
        # Image builds run on the release host, not in a CI job; ADR-0197 lists their downloads.
        if not (name.endswith(SCRIPT_SUFFIXES) or name.startswith(".github/")):
            continue
        for number, line in enumerate(read(root, name).splitlines(), start=1):
            if "releases/download" in line:
                failures.append(
                    f"{name}:{number}: a release asset URL outside {REGISTRY}; download it with {FETCHER}"
                )
    return failures


def fetching_scripts(root: pathlib.Path, files: list[str]) -> set[str]:
    """Every tracked script that calls the fetcher itself.

    Scripts that only name one of these (a self-test stubbing it, a path selector listing it) do
    not download, so the walk stops at direct callers; the guard chain's indirect use is the
    `cargo xtask verify` entry.
    """
    return {
        name
        for name in files
        if name.endswith(SCRIPT_SUFFIXES)
        and name != FETCHER
        and "/test" not in name
        and re.search(r"fetch_pinned\.py\b|import fetch_pinned\b", read(root, name))
    }


def jobs(text: str) -> list[tuple[str, str]]:
    lines = text.splitlines(keepends=True)
    try:
        start = next(index for index, line in enumerate(lines) if line.rstrip() == "jobs:")
    except StopIteration:
        return []
    result: list[tuple[str, list[str]]] = []
    for line in lines[start + 1 :]:
        if line.strip() and not line.startswith(" "):
            break
        match = JOB.match(line)
        if match:
            result.append((match.group(1), []))
        elif result:
            result[-1][1].append(line)
    return [(name, "".join(body)) for name, body in result]


def check_jobs(root: pathlib.Path, files: list[str], workflow_dir: pathlib.Path) -> list[str]:
    entries = [XTASK_ENTRY, "fetch_pinned.py"] + sorted(
        pathlib.PurePosixPath(name).name for name in fetching_scripts(root, files)
    )
    failures = []
    for workflow in sorted([*workflow_dir.glob("*.yml"), *workflow_dir.glob("*.yaml")]):
        text = workflow.read_text(encoding="utf-8")
        for job, body in jobs(text):
            positions = [body.find(entry) for entry in entries if entry in body]
            exact = body.find(RESTORE_STEP)
            if CACHE_PATH in body and exact < 0:
                failures.append(
                    f"{workflow.name}: job {job}: a pinned-cache step differs from the one restore step "
                    "(see RESTORE_STEP in scripts/guard/check-pinned-downloads.py)"
                )
            if not positions:
                continue
            first = min(positions)
            if exact < 0 or exact > first:
                reached = sorted(entry for entry in entries if entry in body)
                failures.append(
                    f"{workflow.name}: job {job} reaches the pinned fetcher ({', '.join(reached)}) "
                    "without first restoring the pinned release cache"
                )
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[2])
    parser.add_argument("--workflows", type=pathlib.Path)
    arguments = parser.parse_args()
    root = arguments.root.resolve()
    files = tracked(root)
    failures = check_no_raw_downloads(root, files) + check_jobs(
        root, files, arguments.workflows or root / ".github" / "workflows"
    )
    for failure in failures:
        print(f"FAIL pinned-downloads: {failure}", file=sys.stderr)
    if failures:
        return 1
    print(f"OK pinned-downloads (fetching scripts={len(fetching_scripts(root, files))})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
