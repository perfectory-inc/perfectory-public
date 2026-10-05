#!/usr/bin/env python3
"""Keep every local git hook step inside the fast budget class and twinned in CI (ADR-0149).

Two rules per Lefthook command:

1. Budget class. Neither the command nor any script it invokes directly may start a Rust
   build, a container, the full guard sweep, a link crawl, or a JS build/test runner. Those
   are what made pre-push take 20-25 minutes and get killed on a 16 GB laptop (ADR-0098).
   `cargo fmt` is the one Cargo subcommand allowed: it formats without compiling.
2. CI twin. Every command must also run in CI: its script, or for a tool command the
   concrete tool, must be named in a workflow, the repository guard runner, or the xtask
   verification source. A check that only a hook runs is a check nobody enforces — three SP10
   panel guards ran only in pre-commit, could never match a path in the monorepo, and a
   violation sat on main unseen.

The Lefthook parser is the advisory-policy guard's, so both guards read one shape.
"""

from __future__ import annotations

import importlib.util
import re
import shlex
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

_spec = importlib.util.spec_from_file_location(
    "lefthook_advisory_policy", Path(__file__).with_name("check-lefthook-advisory-policy.py")
)
assert _spec is not None and _spec.loader is not None
_policy = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _policy  # dataclasses resolve their module through sys.modules
_spec.loader.exec_module(_policy)

# One heavy class, matched on executable lines only (comments are skipped so prose that names
# a tool does not trip the guard).
HEAVY = [
    (re.compile(r"(?:^|[^\w-])cargo\s+(?!fmt\b)[\w+-]+"), "a Cargo subcommand other than fmt"),
    (re.compile(r"(?:^|[^\w-])(?:docker|podman)(?:$|[^\w-])"), "a container runtime"),
    (re.compile(r"(?:^|[^\w-])xtask(?:$|[^\w-])"), "xtask verification"),
    (re.compile(r"monorepo-guard\.sh"), "the full repository guard sweep"),
    (re.compile(r"(?:^|[^\w-])lychee(?:$|[^\w-])"), "the link crawler"),
    (
        re.compile(r"(?:^|[^\w-])(?:pnpm|npx)\s+(?:turbo|test|build|tsc|vitest|playwright|exec|dlx)\b"),
        "a JS build or test runner",
    ),
    (re.compile(r"(?:^|[^\w-])pytest(?:$|[^\w-])"), "a Python test runner"),
]
SCRIPT_TOKEN = re.compile(r"[\w./-]+\.(?:sh|py)$")
INTERPRETERS = {"bash", "sh", "python", "python3"}
CI_SOURCES = [
    *sorted((ROOT / ".github" / "workflows").glob("*.yml")),
    ROOT / "scripts" / "guard" / "monorepo-guard.sh",
    *sorted((ROOT / "tools" / "xtask" / "src").glob("*.rs")),
]


def executable_lines(text: str) -> list[str]:
    return [line for line in text.splitlines() if not line.lstrip().startswith("#")]


def heavy_reason(text: str) -> str | None:
    for line in executable_lines(text):
        for pattern, reason in HEAVY:
            if pattern.search(line):
                return f"{reason} ({line.strip()!r})"
    return None


def run_tokens(run: str) -> list[str]:
    return shlex.split(re.sub(r"&&|\|\||;|\|", " ", run), posix=True)


def scripts_of(run: str, base: Path) -> list[Path]:
    return [base / token for token in run_tokens(run) if SCRIPT_TOKEN.search(token)]


def ci_needle(run: str) -> list[str]:
    """What CI must name: each invoked script's stem, or the concrete tool."""
    tokens = run_tokens(run)
    scripts = [Path(token).stem for token in tokens if SCRIPT_TOKEN.search(token)]
    if scripts:
        return scripts
    launcher = tokens[0]
    if launcher in {"pnpm", "cargo"} and len(tokens) > 1:
        return [tokens[1]]
    return [launcher]


def check(config: Path, ci_text: str) -> list[str]:
    problems: list[str] = []
    for command in _policy.commands(config):
        if command.run is None:
            continue
        where = f"{config}:{command.line}: {command.name}"
        base = config.parent / (command.root or "")
        reason = heavy_reason(command.run)
        if reason:
            problems.append(f"{where} runs {reason}; that belongs in CI only")
        for script in scripts_of(command.run, base):
            if not script.is_file():
                problems.append(f"{where} invokes missing script {script}")
                continue
            reason = heavy_reason(script.read_text(encoding="utf-8"))
            if reason:
                problems.append(f"{where} invokes {script.name}, which runs {reason}")
        for needle in ci_needle(command.run):
            if not re.search(rf"(?<![\w-]){re.escape(needle)}(?![\w-])", ci_text):
                problems.append(
                    f"{where} runs {needle!r}, which no CI workflow, guard runner, or xtask "
                    "source names; add it to CI first (ADR-0149)"
                )
    return problems


def main() -> int:
    config = Path(sys.argv[1] if len(sys.argv) > 1 else ROOT / "lefthook.yml")
    ci_root = Path(sys.argv[2]) if len(sys.argv) > 2 else None
    sources = sorted(ci_root.rglob("*")) if ci_root else CI_SOURCES
    ci_text = "\n".join(p.read_text(encoding="utf-8") for p in sources if p.is_file())
    try:
        problems = check(config, ci_text)
    except (OSError, ValueError) as error:
        print(f"FAIL lefthook-time-budget: {error}", file=sys.stderr)
        return 1
    for problem in problems:
        print(f"FAIL lefthook-time-budget: {problem}", file=sys.stderr)
    if problems:
        return 1
    print("OK lefthook-time-budget")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
