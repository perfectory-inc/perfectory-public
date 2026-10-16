#!/usr/bin/env python3
"""A pull request may not add an ADR number or a migration version another change already holds.

  numbering_collisions.py --repository OWNER/REPO --pull-request N --base REF

The per-tree guards (render-adr-index.py --check, migration-naming) see one tree; two open pull
requests each pass them and collide on merge. On 2026-10-16 that happened four times with ADR
numbers (0212/0248, 0255, 0259, 0265) and once with a migration version (20261016120000, #633/#636).

This reads, read-only, every open pull request's added paths (one GraphQL query with the job's own
token: the data is public) and main's tree, then fails when this pull request adds
- a root ADR number (docs/adr/NNNN-*.md) main holds or an OLDER open pull request adds, or
- a migration version (<dir>/migrations/<14 digits>_*.sql) the same directory holds that way,
naming the next free number. A newer pull request holding the same number is only named: it is the
one that moves, so two pull requests never block each other.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.request

ADR = re.compile(r"^docs/adr/(\d{4})-[^/]+\.md$")
MIGRATION = re.compile(r"^(.+/migrations)/(\d{14})_[^/]+\.sql$")

QUERY = """
query($owner: String!, $name: String!, $after: String) {
  repository(owner: $owner, name: $name) {
    pullRequests(states: OPEN, first: 50, after: $after) {
      pageInfo { hasNextPage endCursor }
      nodes { number files(first: 100) { pageInfo { hasNextPage } nodes { path changeType } } }
    }
  }
}
"""


def number_of(path: str) -> tuple[str, str] | None:
    """The numbering space and number a path takes: ("adr", "0259") or (<dir>, "<14 digits>")."""
    if found := ADR.match(path):
        return "adr", found.group(1)
    if found := MIGRATION.match(path):
        return found.group(1), found.group(2)
    return None


def next_free(space: str, number: str, taken: set[str]) -> str:
    if space == "adr":
        return f"{max(int(value) for value in taken | {number}) + 1:04d}"
    value = int(number) + 1
    while str(value) in taken:
        value += 1
    return str(value)


def collisions(mine: dict[str, str], main: list[str], others: dict[int, list[str]],
               me: int) -> tuple[list[str], list[str]]:
    """(failures, notes). `mine` maps this pull request's added paths; `main` is main's tree;
    `others` maps each other open pull request to the paths it adds."""
    holders: dict[tuple[str, str], list[str]] = {}
    for path in main:
        if (key := number_of(path)) is not None:
            holders.setdefault(key, []).append(f"main ({path})")
    by_pr: dict[tuple[str, str], list[int]] = {}
    for number, paths in sorted(others.items()):
        for path in paths:
            if (key := number_of(path)) is not None and number != me:
                by_pr.setdefault(key, []).append(number)
    taken: dict[str, set[str]] = {}
    for space, value in [*holders, *by_pr]:
        taken.setdefault(space, set()).add(value)
    failures, notes = [], []
    for path in sorted(mine):
        key = number_of(path)
        if key is None:
            continue
        space, value = key
        older = [number for number in by_pr.get(key, []) if number < me]
        newer = [number for number in by_pr.get(key, []) if number > me]
        on_main = [holder for holder in holders.get(key, []) if not holder.endswith(f"({path})")]
        if on_main or older:
            held = ", ".join(on_main + [f"#{number}" for number in older])
            failures.append(f"{path}: {value} is already held by {held}; the next free one is "
                            f"{next_free(space, value, taken.get(space, set()))}")
        for number in newer:
            notes.append(f"{path}: #{number}, opened later, adds {value} too; it is the one that renumbers")
    return failures, notes


def open_pull_request_paths(repository: str, token: str) -> dict[int, list[str]]:
    owner, name = repository.split("/", 1)
    out: dict[int, list[str]] = {}
    after = None
    while True:
        body = json.dumps({"query": QUERY, "variables": {"owner": owner, "name": name, "after": after}})
        request = urllib.request.Request("https://api.github.com/graphql", data=body.encode(), headers={
            "Authorization": f"Bearer {token}", "User-Agent": "numbering-collisions"})
        with urllib.request.urlopen(request, timeout=60) as response:
            page = json.load(response)
        if page.get("errors"):
            raise RuntimeError(f"GitHub GraphQL: {page['errors']}")
        pulls = page["data"]["repository"]["pullRequests"]
        for node in pulls["nodes"]:
            files = node["files"]
            if files is None or files["pageInfo"]["hasNextPage"]:
                # shortcut: a pull request of more than 100 files is read only to its first 100; the
                # check then names it so a collision behind it is not mistaken for none.
                print(f"numbering-collisions: #{node['number']} changes over 100 files; only its first 100 are read")
            out[node["number"]] = [file["path"] for file in (files or {}).get("nodes", [])
                                   if file["changeType"] in ("ADDED", "RENAMED", "COPIED")]
        if not pulls["pageInfo"]["hasNextPage"]:
            return out
        after = pulls["pageInfo"]["endCursor"]


def git_lines(*args: str) -> list[str]:
    return subprocess.run(["git", *args], capture_output=True, text=True, check=True).stdout.split("\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repository", required=True)
    parser.add_argument("--pull-request", default="", help="empty outside a pull request run")
    parser.add_argument("--base", default="", help="main as this run sees it")
    args = parser.parse_args()
    if not args.pull_request:
        # A push to main or a merge group: what merges is checked by its pull request's run.
        print("numbering-collisions: not a pull request; nothing to compare")
        return 0
    args.pull_request = int(args.pull_request)
    mine = [path for path in git_lines("diff", "--name-only", "--diff-filter=AR", f"{args.base}...HEAD") if path]
    main_tree = [path for path in git_lines("ls-tree", "-r", "--name-only", args.base) if path]
    others = open_pull_request_paths(args.repository, os.environ["GITHUB_TOKEN"])
    failures, notes = collisions({path: path for path in mine}, main_tree, others, args.pull_request)
    for note in notes:
        print(f"numbering-collisions: note {note}")
    for failure in failures:
        print(f"numbering-collisions: FAIL {failure}", file=sys.stderr)
    if not failures:
        numbered = sum(number_of(path) is not None for path in mine)
        print(f"numbering-collisions: OK ({numbered} numbered files added; {len(others)} open pull requests read)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
