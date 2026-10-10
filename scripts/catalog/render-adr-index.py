#!/usr/bin/env python3
"""Print the root ADR index from the ADR files themselves, and check what the index relied on.

`docs/adr/README.md` used to keep one hand-appended line per ADR, at the same place, so any two PRs
that each recorded a decision conflicted there (root ADR-0176). The list is the directory: each
file's number is in its name and its title is its first heading, so the index is derived from them
and never stored. `--check` enforces what a reader of that list depended on:

- every root ADR file is named `NNNN-<kebab-case>.md`;
- its first heading is `# ADR NNNN: <title>` with the same number as its name;
- no two files share a number. Two PRs that both take the next free number merge without a textual
  conflict now, so this is where that collision is caught — in the merge queue, before `main`.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
ADR_DIR = ROOT / "docs/adr"
FILENAME = re.compile(r"^(\d{4})-[a-z0-9]+(?:-[a-z0-9]+)*\.md$")
# `# ADR NNNN: <title>` is the template (write-adr skill); six accepted ADRs wrote
# `# ADR-NNNN — <title>` and stay as written, since an accepted ADR's text is not edited.
HEADING = re.compile(r"^# ADR[ -](\d{4})\s*(?::|—)\s*(\S.*?)\s*$")
# Files in the directory that are not decisions.
NOT_DECISIONS = {"README.md"}


def first_heading(text: str) -> str | None:
    lines = text.splitlines()
    if lines and lines[0].strip() == "---":
        for index in range(1, len(lines)):
            if lines[index].strip() == "---":
                lines = lines[index + 1 :]
                break
    for line in lines:
        if line.startswith("# "):
            return line
    return None


def read_adrs(adr_dir: Path) -> tuple[list[tuple[str, str, str]], list[str]]:
    """Return (number, title, filename) per ADR and the problems found, both in filename order."""
    entries: list[tuple[str, str, str]] = []
    problems: list[str] = []
    owners: dict[str, list[str]] = {}
    for path in sorted(adr_dir.iterdir(), key=lambda item: item.name):
        if path.name in NOT_DECISIONS or not path.is_file():
            continue
        named = FILENAME.match(path.name)
        if not named:
            problems.append(f"{path.name}: ADR file name must be NNNN-<kebab-case>.md")
            continue
        number = named.group(1)
        owners.setdefault(number, []).append(path.name)
        heading = first_heading(path.read_text(encoding="utf-8-sig"))
        titled = HEADING.match(heading or "")
        if not titled:
            problems.append(f"{path.name}: first heading must be '# ADR {number}: <title>', found {heading!r}")
            continue
        if titled.group(1) != number:
            problems.append(f"{path.name}: heading says ADR {titled.group(1)} but the file name says {number}")
            continue
        entries.append((number, titled.group(2), path.name))
    for number, names in sorted(owners.items()):
        if len(names) > 1:
            problems.append(
                f"ADR {number} is taken by {len(names)} files: {', '.join(names)} — renumber the later one"
            )
    return entries, problems


def render(entries: list[tuple[str, str, str]]) -> str:
    lines = [
        "<!-- Rendered on demand: python3 scripts/catalog/render-adr-index.py (not committed, root ADR-0176) -->",
        "",
        "# 전역 ADR 목록",
        "",
    ]
    lines += [f"- [ADR-{number} — {title}](./{name})" for number, title, name in entries]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail on a bad name, heading or duplicate number")
    parser.add_argument("--adr-dir", type=Path, default=ADR_DIR, help=argparse.SUPPRESS)
    args = parser.parse_args()
    entries, problems = read_adrs(args.adr_dir)
    sys.stderr.reconfigure(encoding="utf-8")
    if args.check:
        for problem in problems:
            print(f"adr index: {problem}", file=sys.stderr)
        return 1 if problems else 0
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stdout.write(render(entries))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
