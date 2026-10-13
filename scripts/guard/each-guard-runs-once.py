#!/usr/bin/env python3
"""Each guard runs once per `monorepo-guard.sh` run.

Guards were added one per incident, and some aggregate guards started calling
other guards as well: `public-github-policy.sh` ran eight self-tests the runner
loop also ran, and `check-workflow-policy.sh` ran from two parents. A full run
executed eleven guards twice (about nine of sixty-seven seconds on CI), and the
loop stopped being the one list of what runs.

A guard is started either by the runner loop or by exactly one other guard,
never by two. Self-tests are exempt as callers: they run a guard against a
synthetic fixture to prove it fails, which is not a second run on the tree.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

RUNNER = "monorepo-guard.sh"
LOOP = re.compile(r"^for g in (.*?); do$", re.MULTILINE | re.DOTALL)
# `bash scripts/guard/x.sh`, `exec bash "$dir/x.sh"`, `python3 scripts/guard/x.py`.
CALL = re.compile(
    r"""^\s*(?:exec\s+)?(?:bash|python3?)\s+"?(?:\$\{?\w+\}?/)?(?:(?:\./)?scripts/guard/)?([\w.-]+\.(?:sh|py))\b""",
    re.MULTILINE,
)


def loop_entries(runner: str) -> list[str]:
    match = LOOP.search(runner)
    if match is None:
        return []
    return [f"{name}.sh" for name in match.group(1).replace("\\", " ").split()]


def violations(guard_dir: Path) -> list[str]:
    runner = (guard_dir / RUNNER).read_text(encoding="utf-8")
    entries = loop_entries(runner)
    if not entries:
        return [f"{RUNNER}: no `for g in ...; do` loop found"]
    callers: dict[str, list[str]] = {}
    for name in entries:
        callers.setdefault(name, []).append(f"{RUNNER} loop")
    for path in sorted(guard_dir.glob("*.sh")):
        if path.name == RUNNER or path.name.endswith("-self-test.sh"):
            continue
        for called in CALL.findall(path.read_text(encoding="utf-8")):
            if called != path.name:
                callers.setdefault(called, []).append(path.name)
    errors = [
        f"{name} runs {len(by)} times per run (started by {', '.join(by)}); keep one caller"
        for name, by in sorted(callers.items())
        if len(by) > 1
    ]
    errors += [
        f"{name} is in the {RUNNER} loop but does not exist"
        for name in entries
        if not (guard_dir / name).is_file()
    ]
    return errors


def main(argv: list[str]) -> int:
    root = Path(argv[1]) if len(argv) > 1 else Path(__file__).resolve().parents[2]
    errors = violations(root / "scripts" / "guard")
    for error in errors:
        print(f"FAIL each-guard-runs-once: {error}", file=sys.stderr)
    if errors:
        return 1
    print("OK each-guard-runs-once")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
