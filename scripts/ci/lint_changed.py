#!/usr/bin/env python3
"""ShellCheck, ruff and squawk over the shell files, Python files and foundation migrations a change
touches, against a baseline that only shrinks.

  lint_changed.py check       the files this change touches (a pull request or the merge queue:
                              CHANGE_SCOPE_BASE...CHANGE_SCOPE_HEAD), or every tracked file
  lint_changed.py baseline    rewrite tools/ci/lint-baseline.json from every tracked file

The tree had no shell or Python linter, so a first run over it reports thousands of findings. The
baseline (tools/ci/lint-baseline.json) records them per file and per rule code as counts, the same
shape as ESLint's bulk suppressions. A checked file fails when a rule code has more findings in it
than the baseline holds for that file; a file the baseline does not name must be clean. The
baseline itself may only lose entries or lower counts against the change's base: a finding is fixed,
never added to the list.

The incident this prevents: AGENTS.md polices "an exit code a pipe or a command substitution hides"
in prose, and review missed it more than once (2026-10-29: a photo check reported 200 from piped
output). ShellCheck's SC2312 (optional check `check-extra-masked-returns`, enabled in .shellcheckrc)
and ruff's PLW1510 (`subprocess.run` without `check`) are that rule as a tool.

squawk holds a new foundation migration to expand/contract (root ADR-0616 §2-5, .squawk.toml): the rollout
migrates while the previous release still runs and a rollback moves only the code back, so a migration that
drops, renames, retypes or newly requires something breaks the release that is running. The incident:
2026-10-14 #465 merged a drop migration; the deploy's migrate step failed after the API container was
recreated and the API stayed down about 30 minutes. A contract step that may drop says so on the statement:
`-- squawk-ignore <rule>` with the release whose code no longer reads it. Migrations are never edited after
they merge, so a migration a change touches is a new one; the baseline holds the ones from before the rule.

The tools are checksum-pinned (tools/shellcheck.env, tools/ruff.env, tools/squawk.env) and fetched by
scripts/ci/fetch_pinned.py. ShellCheck publishes no per-architecture Windows asset, so on Windows
shell files are skipped with a notice unless --shellcheck names an executable; CI (Linux) judges.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BASELINE = ROOT / "tools" / "ci" / "lint-baseline.json"
FETCHER = ROOT / "scripts" / "ci" / "fetch_pinned.py"
SHELL_SUFFIXES = (".sh",)
PYTHON_SUFFIXES = (".py",)
# The migrations the expand/contract rule covers: the foundation database's (the data host and AWS).
MIGRATIONS = "platforms/foundation-platform/migrations/"
TOOLS = ("shellcheck", "ruff", "squawk")
# A change to any of these can change the verdict on a file the change does not touch, so it
# checks every file: the settings, the pins, the baseline and this checker.
WHOLE_TREE_INPUTS = frozenset({
    ".shellcheckrc", "ruff.toml", ".squawk.toml", "tools/shellcheck.env", "tools/ruff.env", "tools/squawk.env",
    "tools/pinned-artifacts.json",
    "tools/ci/lint-baseline.json", "scripts/ci/lint_changed.py", "scripts/ci/fetch_pinned.py",
})
# ShellCheck takes its files as arguments; a batch stays far under any command-line limit.
BATCH = 50

Findings = dict[str, dict[str, dict[str, int]]]  # tool -> path -> rule code -> count


def git(*arguments: str) -> str:
    return subprocess.run(["git", "-C", str(ROOT), *arguments], check=True, capture_output=True,
                          text=True, encoding="utf-8").stdout


def tracked(suffixes: tuple[str, ...]) -> list[str]:
    return sorted(path for path in git("ls-files", "-z").split("\0") if path.endswith(suffixes))


def changed_paths() -> set[str] | None:
    """The paths this change touches, or None when there is no comparison (then: every file)."""
    if os.environ.get("CHANGE_SCOPE_EVENT", "") not in ("pull_request", "merge_group"):
        return None
    base, head = os.environ.get("CHANGE_SCOPE_BASE", ""), os.environ.get("CHANGE_SCOPE_HEAD", "")
    if not base or not head:
        return None
    try:
        return set(filter(None, git("diff", "--name-only", "-z", "--no-renames", f"{base}...{head}").split("\0")))
    except subprocess.CalledProcessError:
        return None


def pinned(tool: str, directory: Path) -> Path | None:
    """The pinned executable for this platform, or None when the tool is not pinned here."""
    archive = directory / f"{tool}.archive"
    fetched = subprocess.run([sys.executable, str(FETCHER), "tool", tool, "--output", str(archive)],
                             capture_output=True, text=True, check=False)
    if fetched.returncode != 0:
        if "is not pinned for" in fetched.stderr + fetched.stdout:
            return None
        raise SystemExit(f"FAIL lint: fetching {tool}: {(fetched.stderr or fetched.stdout).strip()}")
    names = (tool, f"{tool}.exe")
    target = directory / (names[1] if os.name == "nt" else names[0])
    if tool == "squawk":
        # Published as a bare executable, not an archive.
        target.write_bytes(archive.read_bytes())
    elif zipfile.is_zipfile(archive):
        with zipfile.ZipFile(archive) as bundle:
            member = next(name for name in bundle.namelist() if Path(name).name in names)
            target.write_bytes(bundle.read(member))
    else:
        with tarfile.open(archive) as bundle:
            member = next(entry for entry in bundle.getmembers() if entry.isfile() and Path(entry.name).name in names)
            source = bundle.extractfile(member)
            assert source is not None
            target.write_bytes(source.read())
    target.chmod(0o755)
    return target


def count(findings: list[tuple[str, str]]) -> dict[str, dict[str, int]]:
    """(path, rule code) pairs as path -> code -> count, both sorted."""
    counted: dict[str, Counter[str]] = {}
    for path, code in findings:
        counted.setdefault(path, Counter())[code] += 1
    return {path: dict(sorted(codes.items())) for path, codes in sorted(counted.items())}


def run_shellcheck(executable: Path, files: list[str]) -> dict[str, dict[str, int]]:
    found: list[tuple[str, str]] = []
    for start in range(0, len(files), BATCH):
        batch = files[start:start + BATCH]
        # Exit 1 means "findings"; anything else is ShellCheck itself failing.
        result = subprocess.run([str(executable), "--format=json1", *batch], cwd=ROOT, capture_output=True,
                                text=True, encoding="utf-8", check=False)
        if result.returncode not in (0, 1):
            raise SystemExit(f"FAIL lint: shellcheck exited {result.returncode}: {result.stderr.strip()}")
        found += [(comment["file"].replace("\\", "/"), f"SC{comment['code']}")
                  for comment in json.loads(result.stdout)["comments"]]
    return count(found)


def run_ruff(executable: Path, files: list[str]) -> dict[str, dict[str, int]]:
    found: list[tuple[str, str]] = []
    for start in range(0, len(files), BATCH):
        batch = files[start:start + BATCH]
        result = subprocess.run([str(executable), "check", "--no-cache", "--force-exclude", "--output-format=json",
                                 *batch], cwd=ROOT, capture_output=True, text=True, encoding="utf-8", check=False)
        if result.returncode not in (0, 1):
            raise SystemExit(f"FAIL lint: ruff exited {result.returncode}: {result.stderr.strip()}")
        for item in json.loads(result.stdout):
            path = Path(item["filename"]).resolve().relative_to(ROOT).as_posix()
            # A file ruff cannot parse reports with no code.
            found.append((path, item.get("code") or "syntax-error"))
    return count(found)


def run_squawk(executable: Path, files: list[str]) -> dict[str, dict[str, int]]:
    found: list[tuple[str, str]] = []
    for start in range(0, len(files), BATCH):
        batch = files[start:start + BATCH]
        # Exit 1 means "findings"; anything else is squawk itself failing.
        result = subprocess.run([str(executable), "--config", ".squawk.toml", "--reporter", "json", *batch], cwd=ROOT,
                                capture_output=True, text=True, encoding="utf-8", check=False)
        if result.returncode not in (0, 1):
            raise SystemExit(f"FAIL lint: squawk exited {result.returncode}: {result.stderr.strip()}")
        found += [(item["file"].replace("\\", "/"), item["rule_name"]) for item in json.loads(result.stdout or "[]")]
    return count(found)


def over(found: dict[str, dict[str, int]], allowed: dict[str, dict[str, int]]) -> list[str]:
    """Every (file, rule) with more findings than the baseline allows that file."""
    return [f"{path}: {code} x{number} (baseline {allowed.get(path, {}).get(code, 0)})"
            for path, codes in found.items() for code, number in codes.items()
            if number > allowed.get(path, {}).get(code, 0)]


def grown(base: dict, head: Findings) -> list[str]:
    """Every baseline entry that is new or larger than at the change's base. A tool the base's baseline does
    not have at all is the change that brings that tool in: its first section is the starting point."""
    return [f"{tool}: {path}: {code} {before} -> {number}"
            for tool in TOOLS if base.get(tool) is not None
            for path, codes in head.get(tool, {}).items() for code, number in codes.items()
            if number > (before := base[tool].get(path, {}).get(code, 0))]


def stale(found: dict[str, dict[str, int]], allowed: dict[str, dict[str, int]], checked: list[str]) -> list[str]:
    """Baseline entries, for the files just checked, that are larger than what the file now has."""
    return [f"{path}: {code} baseline {number}, now {found.get(path, {}).get(code, 0)}"
            for path in checked for code, number in allowed.get(path, {}).items()
            if number > found.get(path, {}).get(code, 0)]


def load_baseline(text: str | None = None) -> Findings:
    if text is None:
        text = BASELINE.read_text(encoding="utf-8") if BASELINE.exists() else "{}"
    data = json.loads(text)
    return {tool: data.get(tool, {}) for tool in TOOLS}


def base_baseline() -> dict | None:
    """The baseline at the change's base; None when there is no base, or the base has no baseline
    (the change that introduces it). A base that cannot be read is an error, never "no baseline"."""
    base = os.environ.get("CHANGE_SCOPE_BASE", "")
    if not base:
        return None
    path = BASELINE.relative_to(ROOT).as_posix()
    listed = subprocess.run(["git", "-C", str(ROOT), "ls-tree", "--name-only", base, "--", path],
                            capture_output=True, text=True, encoding="utf-8", check=False)
    if listed.returncode != 0:
        raise SystemExit(f"FAIL lint: cannot read the base {base}: {listed.stderr.strip()}")
    if not listed.stdout.strip():
        return None
    data = json.loads(git("show", f"{base}:{path}"))
    return {tool: data.get(tool) for tool in TOOLS}


def gone(baseline: Findings, existing: dict[str, list[str]]) -> list[str]:
    """Baseline entries for files that are no longer tracked: an allowance nothing can use up."""
    return [f"{tool}: {path}" for tool in TOOLS for path in baseline.get(tool, {}) if path not in set(existing.get(tool, []))]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("command", choices=("check", "baseline"))
    parser.add_argument("--shellcheck", type=Path, help="use this executable instead of the pinned download")
    parser.add_argument("--ruff", type=Path, help="use this executable instead of the pinned download")
    parser.add_argument("--squawk", type=Path, help="use this executable instead of the pinned download")
    arguments = parser.parse_args(argv)
    changed = changed_paths() if arguments.command == "check" else None
    files = {"shellcheck": tracked(SHELL_SUFFIXES), "ruff": tracked(PYTHON_SUFFIXES),
             "squawk": [path for path in tracked((".sql",)) if path.startswith(MIGRATIONS)]}
    every = {tool: list(paths) for tool, paths in files.items()}
    if changed is not None and changed & WHOLE_TREE_INPUTS:
        changed = None
    if changed is not None:
        files = {tool: [path for path in paths if path in changed] for tool, paths in files.items()}
    runners = {"shellcheck": run_shellcheck, "ruff": run_ruff, "squawk": run_squawk}
    given = {"shellcheck": arguments.shellcheck, "ruff": arguments.ruff, "squawk": arguments.squawk}
    found: Findings = {}
    with tempfile.TemporaryDirectory(prefix="lint-") as temporary:
        for tool in TOOLS:
            if not files[tool]:
                found[tool] = {}
                continue
            executable = given[tool] or pinned(tool, Path(temporary))
            if executable is None:
                # CI is the authority: there a missing tool is a failure, not a notice.
                if arguments.command == "baseline" or os.environ.get("GITHUB_ACTIONS") == "true":
                    raise SystemExit(f"FAIL lint: {tool} is not pinned for this platform; pass --{tool}")
                print(f"lint: {tool} is not pinned for this platform; {len(files[tool])} file(s) not checked here, CI checks them")
                files[tool] = []
                found[tool] = {}
                continue
            found[tool] = runners[tool](executable, files[tool])
    if arguments.command == "baseline":
        BASELINE.write_text(json.dumps(found, indent=1, sort_keys=True) + "\n", encoding="utf-8", newline="\n")
        total = sum(number for tool in TOOLS for codes in found[tool].values() for number in codes.values())
        print(f"OK lint baseline: {total} finding(s) in {sum(len(found[tool]) for tool in TOOLS)} file(s)")
        return 0
    baseline = load_baseline()
    problems = [f"{tool}: {line}" for tool in TOOLS for line in over(found[tool], baseline[tool])]
    shrink = [f"{tool}: {line}" for tool in TOOLS for line in stale(found[tool], baseline[tool], files[tool])]
    before = base_baseline()
    growth = grown(before, baseline) if before is not None else []
    removed = gone(baseline, every)
    for line in problems:
        print(f"FAIL lint: {line}", file=sys.stderr)
    for line in shrink:
        print(f"FAIL lint: the baseline holds more than the file has; lower it: {line}", file=sys.stderr)
    for line in growth:
        print(f"FAIL lint: tools/ci/lint-baseline.json only shrinks: {line}", file=sys.stderr)
    for line in removed:
        print(f"FAIL lint: the baseline names a file that is gone; remove the entry: {line}", file=sys.stderr)
    if problems or shrink or growth or removed:
        print("FAIL lint: fix the finding, or silence one line with its reason (`# shellcheck disable=SCnnnn` / `# noqa: CODE` / "
              "`-- squawk-ignore <rule>` on a migration's contract step, root ADR-0616 §2-5). "
              "After fixing, `python3 scripts/ci/lint_changed.py baseline` lowers the baseline.", file=sys.stderr)
        return 1
    scope = "the files this change touches" if changed is not None else "every tracked file"
    print(f"OK lint: {len(files['shellcheck'])} shell, {len(files['ruff'])} Python and {len(files['squawk'])} migration "
          f"file(s), {scope}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
