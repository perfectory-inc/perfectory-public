#!/usr/bin/env python3
"""Which Spark runtime tests one shard of Foundation CI's Spark job runs (root ADR-0365).

    foundation_spark_shards.py --shard I --shards N

Prints the shard's targets on one line (nothing: this shard has none) and the whole plan on stderr.

1. Which files: a pull request and the merge queue run the runtime-gated files their change reaches,
   by the suites' own dependency walk (`change_scope.py suites`, root ADR-0313); every other event,
   an unknown comparison, a run-all input, or a change to the runtime or this selection runs all.
2. Which shard: longest first onto the least loaded shard, by the durations the job's log measured
   (foundation-spark-runtime-durations.json). A file longer than one slot's even share is split into
   its test classes, or its test methods when it has one class, so it is not the tail by itself; a
   piece the table names by its unittest name counts its own time, else an even part of the file's.

Reads CHANGE_SCOPE_EVENT, CHANGE_SCOPE_BASE and CHANGE_SCOPE_HEAD, as the scope job does.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import statistics
import subprocess
import sys
from pathlib import Path, PurePosixPath
from typing import Callable

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))
import foundation_ci_scope  # noqa: E402

WORKSPACE = "platforms/foundation-platform"
TESTS = f"{WORKSPACE}/infra/lakehouse/spark/tests"
RUNNER = ROOT / "scripts" / "verify" / "foundation-spark-runtime-files.sh"
DURATIONS = HERE / "foundation-spark-runtime-durations.json"
NARROWED_EVENTS = frozenset({"pull_request", "merge_group"})
# The file runner runs two at a time on a 4-CPU runner (foundation-spark-runtime-files.sh).
SLOTS_PER_SHARD = 2
# A split piece starts its own JVM and Spark session (about 10 s); below this a split only adds that.
MIN_SPLIT_SECONDS = 60
TEST_CASE_BASES = frozenset({"unittest.TestCase", "TestCase"})


def selected_files(gated: list[str], changed: set[str] | None, suites_answer: Callable[[], str]) -> tuple[list[str], str]:
    """(the gated files to run, why). `changed` is None when the event does not narrow."""
    if changed is None:
        return list(gated), "every file: this event or comparison does not narrow (root ADR-0313)"
    runtime = set(foundation_ci_scope.RULES["spark-runtime"]["exact"])
    for path in sorted(changed):
        if foundation_ci_scope._runs_all(path) or path in runtime:
            return list(gated), f"every file: {path} changes the runtime or the selection"
    lines = suites_answer().splitlines()
    if not lines or lines[0].split("\t", 1)[0] != "some":
        return list(gated), f"every file: suite selection says {lines[0] if lines else 'nothing (unreadable)'}"
    reached = {
        PurePosixPath(line.split("\t", 1)[1]).name
        for line in lines[1:]
        if line.startswith("test\t") and str(PurePosixPath(line.split("\t", 1)[1]).parent) == TESTS
    }
    chosen = [name for name in gated if name in reached]
    return chosen, f"{len(chosen)} of {len(gated)} files reach the change ({lines[0].split(chr(9), 1)[-1]})"


def pieces(name: str, seconds: float, source: str) -> list[tuple[str, float]]:
    """The file as unittest targets: its classes, or its one class's methods. Whole when any class
    is not a plain TestCase (an inherited test would be missed) or the module loads its own tests."""
    tree = ast.parse(source)
    if any(isinstance(node, ast.FunctionDef) and node.name == "load_tests" for node in tree.body):
        return [(name, seconds)]
    classes = [node for node in tree.body if isinstance(node, ast.ClassDef)]
    if any(not node.bases or any(ast.unparse(base) not in TEST_CASE_BASES for base in node.bases) for node in classes):
        return [(name, seconds)]
    tested = [
        (node.name, [f.name for f in node.body if isinstance(f, (ast.FunctionDef, ast.AsyncFunctionDef)) and f.name.startswith("test")])
        for node in classes
    ]
    tested = [(cls, methods) for cls, methods in tested if methods]
    module = name.removesuffix(".py")
    if len(tested) > 1:
        targets = [f"{module}.{cls}" for cls, _ in tested]
    elif tested:
        targets = [f"{module}.{tested[0][0]}.{method}" for method in tested[0][1]]
    else:
        targets = []
    if len(targets) < 2:
        return [(name, seconds)]
    return [(target, seconds / len(targets)) for target in targets]


def plan(
    files: list[str], durations: dict[str, float], shards: int, source: Callable[[str], str]
) -> list[list[tuple[str, float]]]:
    """Each shard's (target, estimated seconds), longest first."""
    default = statistics.median(durations.values()) if durations else 1.0
    estimated = {name: float(durations.get(name, default)) for name in files}
    share = sum(estimated.values()) / (shards * SLOTS_PER_SHARD)
    work: list[tuple[str, float]] = []
    for name, seconds in estimated.items():
        split = seconds > max(share, MIN_SPLIT_SECONDS)
        parts = pieces(name, seconds, source(name)) if split else [(name, seconds)]
        # A piece measured on its own (its methods are seldom equal) keeps its own time.
        work += [(target, float(durations.get(target, part))) for target, part in parts]
    assigned: list[list[tuple[str, float]]] = [[] for _ in range(shards)]
    for target, seconds in sorted(work, key=lambda item: (-item[1], item[0])):
        least = min(range(shards), key=lambda i: (sum(s for _, s in assigned[i]), i))
        assigned[least].append((target, seconds))
    return assigned


def changed_paths() -> set[str] | None:
    event = os.environ.get("CHANGE_SCOPE_EVENT", "")
    base = os.environ.get("CHANGE_SCOPE_BASE", "")
    head = os.environ.get("CHANGE_SCOPE_HEAD", "")
    if event not in NARROWED_EVENTS or not base or not head or set(base) == {"0"}:
        return None
    try:
        return foundation_ci_scope.git_changed_paths(ROOT, base, head)
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"foundation-spark-shards: git comparison unavailable, every file runs: {error}", file=sys.stderr)
        return None


def suites_answer() -> str:
    try:
        return subprocess.run(
            [sys.executable, str(HERE / "change_scope.py"), "suites", "--workspace", WORKSPACE],
            check=True, capture_output=True, text=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"foundation-spark-shards: suite selection unreadable, every file runs: {error}", file=sys.stderr)
        return ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--shard", type=int, required=True, help="this shard, from 0 (strategy.job-index)")
    parser.add_argument("--shards", type=int, required=True, help="how many shards (strategy.job-total)")
    args = parser.parse_args(argv)
    if not 0 <= args.shard < args.shards:
        parser.error("--shard must be in [0, --shards)")
    tests = ROOT / TESTS
    gated = subprocess.run(["bash", str(RUNNER), "--list", str(tests)], check=True, capture_output=True, text=True).stdout.split()
    if not gated:
        print("foundation-spark-shards: no runtime-gated test file found", file=sys.stderr)
        return 1
    files, why = selected_files(gated, changed_paths(), suites_answer)
    durations = json.loads(DURATIONS.read_text(encoding="utf-8"))["seconds"]
    shards = plan(files, durations, args.shards, lambda name: (tests / name).read_text(encoding="utf-8"))
    print(f"foundation-spark-shards: {why}", file=sys.stderr)
    for index, shard in enumerate(shards):
        mark = " <- this shard" if index == args.shard else ""
        estimate = max(sum(s for _, s in shard) / SLOTS_PER_SHARD, max((s for _, s in shard), default=0))
        print(f"  shard {index + 1}/{args.shards}: {len(shard)} targets, ~{estimate:.0f}s{mark}", file=sys.stderr)
        for target, seconds in shard:
            print(f"    {seconds:6.0f}s  {target}", file=sys.stderr)
    print(" ".join(target for target, _ in shards[args.shard]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
