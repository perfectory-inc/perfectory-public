#!/usr/bin/env python3
"""Keep every ADR's recorded debt reachable from the one task list that claims to own it.

`docs/roadmap/production-readiness.md` declares itself the single execution list and forbids
duplicating work into READMEs and ADRs. Eight ADRs nevertheless grew a `남은 부채` section holding
thirty-seven items between them, and the roadmap linked none of them. The rule was prose, so nothing
noticed: an ADR could record real blocking work and the document a reader consults for "what is
left" would never mention it.

This guard supplies the missing half. It does not ask the roadmap to *copy* the debt — that is the
duplication the roadmap forbids, and a copy drifts from its original the first time either side is
edited. It asks only that the roadmap *link* the ADR, so every recorded item is reachable from the
task list and an author who records new debt has to decide where it sits.

What it deliberately does not check: whether a linked item is actually scheduled, prioritised, or
correct. Those stay human judgement. It checks the one thing that can be checked — that recorded
debt cannot be invisible.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# `남은 부채` is the section heading ADR-0013 introduced and every ADR since has reused. The guard
# keys on the heading rather than on the phrase anywhere in the prose, so an ADR that merely
# discusses another ADR's debt is not treated as recording its own.
#
# Trailing text after the phrase is part of the heading, not a different heading: ADR-0010 writes
# `### 남은 부채 (이 ADR로 닫히지 **않는** 것)`, and the first version of this guard anchored on the
# end of line and silently missed it — eleven items, the largest single debt list in the tree. A
# separator is still required before that text, so a sentence-shaped heading (`남은 부채를 …`) does
# not qualify.
DEBT_HEADING = re.compile(r"^#{2,4}[ \t]*남은 부채(?:[ \t(].*)?$", re.MULTILINE)
MARKDOWN_LINK = re.compile(r"\]\(([^)\s]+)")
ROADMAP = Path("docs/roadmap/production-readiness.md")
ADR_ROOTS = ("docs", "platforms", "products")
PRUNED = {"target", "node_modules", "__pycache__", ".venv", ".git"}
EXTERNAL = ("http://", "https://", "mailto:")


def adr_documents(root: Path) -> list[Path]:
    found: list[Path] = []
    for path in root.rglob("*.md"):
        if any(part in PRUNED for part in path.parts):
            continue
        if path.parent.name == "adr" and path.name != "README.md" and path.is_file():
            found.append(path)
    return found


def read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return ""


def linked_targets(roadmap: Path) -> set[Path]:
    targets: set[Path] = set()
    for raw in MARKDOWN_LINK.findall(read(roadmap)):
        target = raw.split("#", 1)[0]
        if not target or target.startswith(EXTERNAL):
            continue
        targets.add((roadmap.parent / target).resolve())
    return targets


def main(argv: list[str]) -> int:
    repo_root = Path(argv[1]).resolve() if len(argv) > 1 else Path(__file__).resolve().parents[2]
    roadmap = repo_root / ROADMAP

    if not roadmap.is_file():
        print(
            f"FAIL roadmap-owns-recorded-debt: {ROADMAP.as_posix()} is missing; the single task "
            "list is what makes recorded debt reachable",
            file=sys.stderr,
        )
        return 1

    linked = linked_targets(roadmap)
    failures: list[str] = []
    recording = 0

    for root_name in ADR_ROOTS:
        root = repo_root / root_name
        if not root.is_dir():
            continue
        for adr in sorted(adr_documents(root)):
            if not DEBT_HEADING.search(read(adr)):
                continue
            recording += 1
            if adr.resolve() not in linked:
                failures.append(adr.relative_to(repo_root).as_posix())

    if failures:
        for path in failures:
            # The message stays ASCII on purpose. ADR-0009 makes documents Korean-first; guard
            # output is read through whatever code page the runner hands us, and Git for Windows
            # mangles the heading name into noise exactly when someone is trying to act on it.
            print(
                f"FAIL roadmap-owns-recorded-debt: {path} records remaining debt that "
                f"{ROADMAP.as_posix()} does not link; add the link so the task list reaches it, "
                "and do not copy the items across",
                file=sys.stderr,
            )
        return 1

    print(f"OK roadmap-owns-recorded-debt (recording={recording})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
