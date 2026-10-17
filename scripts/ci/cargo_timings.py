#!/usr/bin/env python3
"""Print the crates a verify run spent the longest compiling (root ADR-0269).

`cargo xtask verify` builds clippy and the tests with `--timings`; cargo writes one HTML report per
build under `<target>/cargo-timings/` with its units in `const UNIT_DATA = [...]`. This reads the
reports written since `--since` (Unix seconds), adds each crate's unit durations (check, build,
test, build script) and prints the slowest as a Markdown table, appended to $GITHUB_STEP_SUMMARY in
CI. Like oxidecomputer/omicron's build analysis, it reports; it never fails a run.

    cargo_timings.py summarize --workspace <dir> --since <seconds> [--top N]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

UNIT_DATA = re.compile(r"const UNIT_DATA = (\[.*?\]);\n", re.S)


def units(report: str) -> list[dict]:
    match = UNIT_DATA.search(report)
    if match is None:
        raise ValueError("no UNIT_DATA in the timing report")
    return json.loads(match.group(1))


def slowest(reports: list[str], top: int) -> list[tuple[str, float, int]]:
    """(crate, seconds over all its units, unit count), slowest first."""
    seconds: dict[str, float] = defaultdict(float)
    count: dict[str, int] = defaultdict(int)
    for report in reports:
        for unit in units(report):
            seconds[unit["name"]] += float(unit["duration"])
            count[unit["name"]] += 1
    ranked = sorted(seconds, key=lambda name: (-seconds[name], name))
    return [(name, round(seconds[name], 1), count[name]) for name in ranked[:top]]


def table(title: str, rows: list[tuple[str, float, int]]) -> str:
    lines = [f"### {title}", "", "| crate | seconds | units |", "|---|---:|---:|"]
    lines += [f"| `{name}` | {secs} | {n} |" for name, secs, n in rows]
    return "\n".join(lines) + "\n"


def target_directory(workspace: Path) -> Path:
    out = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=workspace,
        check=True,
        capture_output=True,
    ).stdout
    return Path(json.loads(out.decode("utf-8"))["target_directory"])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    summarize = sub.add_parser("summarize")
    summarize.add_argument("--workspace", type=Path, required=True)
    summarize.add_argument("--since", type=float, required=True)
    summarize.add_argument("--top", type=int, default=15)
    args = parser.parse_args(argv)

    try:
        directory = target_directory(args.workspace) / "cargo-timings"
        paths = sorted(
            p for p in directory.glob("cargo-timing-*.html") if p.stat().st_mtime >= args.since
        )
        rows = slowest([p.read_text(encoding="utf-8") for p in paths], args.top)
    except (OSError, ValueError, subprocess.CalledProcessError, json.JSONDecodeError) as error:
        print(f"cargo-timings: no build-time summary: {error}", file=sys.stderr)
        return 0
    if not rows:
        print(f"cargo-timings: no build since this run started under {directory}", file=sys.stderr)
        return 0
    text = table(f"Slowest crates to build: {args.workspace.as_posix()} ({len(paths)} builds)", rows)
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8", newline="\n") as handle:
            handle.write(text + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
