#!/usr/bin/env python3
"""Require advisory Lefthook commands to probe the tool they actually invoke.

`pnpm` and `cargo` are launchers. Their presence does not prove that a package
binary (Biome/markdownlint) or optional Cargo component (rustfmt/Clippy) is
installed. The repository's hooks are advisory, so those commands must skip
when the concrete subtool cannot run while CI remains authoritative.

This intentionally parses only Lefthook's canonical command shape. Unsupported
or ambiguous shapes fail closed instead of silently weakening the policy.
"""

from __future__ import annotations

import json
import re
import shlex
import sys
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath


TOP_LEVEL_RE = re.compile(r"^([A-Za-z0-9_-]+):\s*$")
COMMAND_RE = re.compile(r"^    ([A-Za-z0-9_-]+):\s*$")
ROOT_RE = re.compile(r"^      root:\s*(.+?)\s*$")
RUN_RE = re.compile(r"^      run:\s*(.+?)\s*$")
SKIP_RE = re.compile(r"^        - run:\s*(.+?)\s*$")
OPTIONAL_CARGO_SUBTOOLS = {"clippy", "fmt"}
PNPM_WORD_RE = re.compile(r"(?:^|[^A-Za-z0-9_-])pnpm(?:$|[^A-Za-z0-9_-])")
CARGO_WORD_RE = re.compile(r"(?:^|[^A-Za-z0-9_-])cargo(?:$|[^A-Za-z0-9_-])")
SHELL_COMPOUND_RE = re.compile(r"&&|\|\||[;&|`]|\$\(|[<>]\(")
PNPM_INDIRECT_SUBCOMMANDS = {"dlx", "exec", "run"}
BLOCK_SCALARS = {">", ">-", ">+", "|", "|-", "|+"}


@dataclass
class HookCommand:
    name: str
    line: int
    root: str | None = None
    run: str | None = None
    skip_runs: list[str] = field(default_factory=list)
    skip_count: int = 0


def scalar(raw: str, *, path: Path, line: int) -> str:
    if raw in BLOCK_SCALARS or raw.startswith(("*", "&")):
        raise ValueError(
            f"{path}:{line}: block, alias, and anchored scalars are unsupported"
        )
    if raw.startswith('"'):
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as error:
            raise ValueError(f"{path}:{line}: invalid quoted run scalar: {error}") from error
        if not isinstance(value, str):
            raise ValueError(f"{path}:{line}: run scalar must be a string")
        return value
    if raw.startswith("'"):
        if not raw.endswith("'"):
            raise ValueError(f"{path}:{line}: unterminated quoted run scalar")
        return raw[1:-1].replace("''", "'")
    return raw


def commands(path: Path) -> list[HookCommand]:
    parsed: list[HookCommand] = []
    current: HookCommand | None = None
    current_hook: str | None = None
    in_commands = False
    in_skip = False
    commands_seen = False
    seen_hooks: set[str] = set()
    command_names: set[str] = set()

    def finish() -> None:
        nonlocal current
        if current is not None:
            parsed.append(current)
            current = None

    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if line and not line.startswith(" "):
            finish()
            current_hook = None
            in_commands = False
            in_skip = False
            commands_seen = False
            top_level_match = TOP_LEVEL_RE.fullmatch(line)
            if top_level_match:
                current_hook = top_level_match.group(1)
                if current_hook in seen_hooks:
                    raise ValueError(
                        f"{path}:{line_number}: duplicate top-level key {current_hook}"
                    )
                seen_hooks.add(current_hook)
            continue
        if current_hook is None:
            continue
        if line.startswith("  jobs:"):
            raise ValueError(
                f"{path}:{line_number}: Lefthook jobs are unsupported; use canonical commands"
            )
        if line.startswith("  commands:"):
            if line != "  commands:":
                raise ValueError(
                    f"{path}:{line_number}: commands must use a canonical block mapping"
                )
            if commands_seen:
                raise ValueError(
                    f"{path}:{line_number}: {current_hook} has duplicate commands keys"
                )
            finish()
            commands_seen = True
            command_names.clear()
            in_commands = True
            in_skip = False
            continue
        if line.startswith("  ") and not line.startswith("    "):
            finish()
            in_commands = False
            in_skip = False
            continue
        if not in_commands:
            continue

        if line.startswith("    ") and not line.startswith("      "):
            command_match = COMMAND_RE.fullmatch(line)
            if command_match:
                finish()
                command_name = command_match.group(1)
                if command_name in command_names:
                    raise ValueError(
                        f"{path}:{line_number}: duplicate command key {command_name}"
                    )
                command_names.add(command_name)
                current = HookCommand(command_name, line_number)
                in_skip = False
                continue
            if line.strip() and not line.lstrip().startswith("#"):
                raise ValueError(
                    f"{path}:{line_number}: unsupported command mapping shape"
                )
        if current is None:
            continue

        root_match = ROOT_RE.fullmatch(line)
        if root_match:
            if current.root is not None:
                raise ValueError(
                    f"{path}:{line_number}: {current.name} has more than one root"
                )
            current.root = scalar(root_match.group(1), path=path, line=line_number)
            in_skip = False
            continue
        if line.startswith("      root:"):
            raise ValueError(f"{path}:{line_number}: unsupported root scalar shape")
        run_match = RUN_RE.fullmatch(line)
        if run_match:
            if current.run is not None:
                raise ValueError(
                    f"{path}:{line_number}: {current.name} has more than one command-level run"
                )
            current.run = scalar(run_match.group(1), path=path, line=line_number)
            in_skip = False
            continue
        if line.startswith("      run:"):
            raise ValueError(f"{path}:{line_number}: unsupported run scalar shape")
        if line == "      skip:":
            current.skip_count += 1
            if current.skip_count > 1:
                raise ValueError(
                    f"{path}:{line_number}: {current.name} has duplicate skip keys"
                )
            in_skip = True
            continue
        if line.startswith("      skip:"):
            current.skip_count += 1
            if current.skip_count > 1:
                raise ValueError(
                    f"{path}:{line_number}: {current.name} has duplicate skip keys"
                )
            # Flow-form merge/rebase skips are valid for commands that do not
            # need a run probe. A managed package-tool command will still fail
            # below because no concrete `skip.run` can be derived from this form.
            in_skip = False
            continue
        skip_match = SKIP_RE.fullmatch(line)
        if in_skip and skip_match:
            current.skip_runs.append(
                scalar(skip_match.group(1), path=path, line=line_number)
            )

    finish()
    return parsed


def command_root(command: HookCommand, path: Path) -> str | None:
    if command.root is None:
        return None
    root = PurePosixPath(command.root)
    if root.is_absolute() or ".." in root.parts:
        raise ValueError(
            f"{path}:{command.line}: {command.name} root must stay inside the repository"
        )
    normalized = str(root)
    if normalized in {"", "."}:
        return None
    on_disk = path.parent.joinpath(*root.parts)
    if not on_disk.is_dir():
        raise ValueError(
            f"{path}:{command.line}: {command.name} root does not exist: {normalized}"
        )
    return normalized


def required_probe(command: HookCommand, path: Path) -> str | None:
    assert command.run is not None
    run = command.run
    try:
        tokens = shlex.split(run, posix=True)
    except ValueError as error:
        raise ValueError(f"cannot parse command run value {run!r}: {error}") from error
    if not tokens:
        return None
    contains_package_tool = PNPM_WORD_RE.search(run) or CARGO_WORD_RE.search(run)
    if contains_package_tool and SHELL_COMPOUND_RE.search(run):
        raise ValueError(f"unsupported compound package-tool command: {run!r}")
    launcher = tokens[0]
    if launcher == "pnpm" and len(tokens) < 2:
        raise ValueError(f"unsupported pnpm command shape: {run!r}")
    if launcher == "cargo" and len(tokens) < 2:
        raise ValueError(f"unsupported cargo command shape: {run!r}")
    if len(tokens) < 2:
        return None
    subtool = tokens[1]
    if launcher == "pnpm":
        if subtool.startswith("-") or subtool in PNPM_INDIRECT_SUBCOMMANDS:
            raise ValueError(f"unsupported pnpm command shape: {run!r}")
        probe = f"pnpm {subtool} --version"
    elif launcher == "cargo":
        if subtool not in OPTIONAL_CARGO_SUBTOOLS:
            raise ValueError(
                f"unsupported host Cargo command {run!r}; Rust builds and repository "
                "guards belong in Docker-backed verification/CI"
            )
        probe = f"cargo {subtool} --version"
    else:
        if contains_package_tool:
            raise ValueError(
                f"unsupported indirect package-tool command shape: {run!r}; "
                "invoke pnpm or the optional Cargo subtool directly so its "
                "availability probe can be derived"
            )
        return None

    root = command_root(command, path)
    if root is not None:
        probe = f"(cd {shlex.quote(root)} && {probe})"
    return f"! {probe} >/dev/null 2>&1"


def main() -> int:
    path = Path(sys.argv[1] if len(sys.argv) > 1 else "lefthook.yml")
    if not path.is_file():
        print(f"FAIL lefthook-advisory-policy: missing {path}", file=sys.stderr)
        return 1

    try:
        parsed = commands(path)
        checked = 0
        for command in parsed:
            if command.run is None:
                continue
            probe = required_probe(command, path)
            if probe is None:
                continue
            checked += 1
            if probe not in command.skip_runs:
                raise ValueError(
                    f"{path}:{command.line}: {command.name} invokes {command.run!r} but "
                    f"does not skip with {probe!r}; checking only the launcher can still "
                    "fail an advisory hook when the concrete subtool is unavailable"
                )
        if checked == 0:
            raise ValueError(f"{path}: no pnpm or optional Cargo subtool commands were checked")
    except (OSError, ValueError) as error:
        print(f"FAIL lefthook-advisory-policy: {error}", file=sys.stderr)
        return 1

    print(f"OK lefthook-advisory-policy ({checked} concrete subtools)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
