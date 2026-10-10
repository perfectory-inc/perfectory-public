#!/usr/bin/env python3
"""Two branches that change unrelated documents must merge cleanly and stay green (root ADR-0176).

Before ADR-0176 almost every PR conflicted with every PR merged ahead of it, and only in files no
one edits by hand: the document catalog and audit report (a row and a total per document), the
foundation baseline (`ADR: **N개**`), and the ADR index (one line appended at the same place by
every ADR). Each conflict cost a rebase and a full CI rerun.

This test replays what a contributor does, using the tree's *own* rules rather than a copy of them:

1. From one base, branch A adds an ADR and a guide; branch B adds a different ADR and a different
   guide. The two ADRs take adjacent numbers, the worst case for any sorted list.
2. On each branch the contributor follows the documented procedure: append an index line to
   `docs/adr/README.md` if that file keeps a per-ADR list (the write-adr skill's rule while it did),
   then run the documentation commands of the pre-push hook (every `lefthook.yml` command under
   `scripts/catalog/`), regenerating with the same command minus `--check`/`--strict` whenever one
   fails. Each branch must be green on its own.
3. B merges A. The merge must produce no textual conflict, and the merged tree must pass the same
   commands without regenerating anything — a generated file that merges cleanly but is stale still
   sends the PR back for another round.

Run against another revision to see the layout it had: `--rev origin/main` (or any commit).
"""

from __future__ import annotations

import argparse
import io
import os
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
INDEX_LINE = re.compile(r"^- \[(?:ADR-)?\d{4}\b", re.MULTILINE)
# A hook exports GIT_DIR and friends; inherited, they point every git call here at the real
# repository instead of the temporary one (it happened once: a guard self-test committed to it).
CLEAN_ENV = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}


def git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=check,
        env=CLEAN_ENV,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )


def materialize(rev: str | None, dest: Path) -> None:
    """Copy the tree under test: a committed revision, or the working tree (tracked + new files)."""
    if rev:
        archive = subprocess.run(
            ["git", "-C", str(ROOT), "archive", "--format=tar", rev],
            check=True,
            capture_output=True,
        ).stdout
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
            tar.extractall(dest, filter="data")
        return
    listed = subprocess.run(
        ["git", "-C", str(ROOT), "ls-files", "-co", "--exclude-standard", "-z"],
        check=True,
        capture_output=True,
    ).stdout
    for raw in listed.split(b"\0"):
        if not raw:
            continue
        relative = raw.decode("utf-8")
        source = ROOT / relative
        if not source.is_file():
            continue
        target = dest / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)


def documentation_commands(repo: Path) -> list[str]:
    """The pre-push hook's documentation commands, read from the tree under test."""
    text = (repo / "lefthook.yml").read_text(encoding="utf-8")
    commands: list[str] = []
    for line in text.splitlines():
        match = re.match(r"^\s*run:\s*(.+?)\s*$", line)
        if not match or "scripts/catalog/" not in match.group(1):
            continue
        commands.extend(part.strip() for part in match.group(1).split("&&"))
    if not commands:
        raise AssertionError("lefthook.yml runs no scripts/catalog/ command; nothing to replay")
    return commands


def run_command(repo: Path, command: str) -> subprocess.CompletedProcess:
    argv = shlex.split(command)
    if argv and argv[0] in {"python", "python3"}:
        argv[0] = sys.executable
    return subprocess.run(
        argv, cwd=repo, env=CLEAN_ENV, capture_output=True, text=True, encoding="utf-8"
    )


def regenerate_variant(command: str) -> str:
    return " ".join(part for part in shlex.split(command) if part not in {"--check", "--strict"})


def failing(repo: Path, commands: list[str]) -> list[str]:
    return [command for command in commands if run_command(repo, command).returncode != 0]


def contribute(repo: Path, number: int, slug: str, title: str, guide: str) -> None:
    adr_name = f"{number:04d}-{slug}.md"
    (repo / "docs/adr" / adr_name).write_text(
        f"# ADR {number:04d}: {title}\n\n"
        "- Status: Accepted\n"
        "- Date: 2026-10-10\n\n"
        "## Context\n\n병합 충돌 재현을 위한 시험 결정입니다.\n\n"
        "## Decision\n\n1. 시험용 결정 하나를 기록합니다.\n\n"
        "## Consequences\n\n시험이 끝나면 임시 저장소와 함께 사라집니다.\n",
        encoding="utf-8",
        newline="\n",
    )
    (repo / "docs/guides" / f"{guide}.md").write_text(
        "---\nstatus: current\nowner: repository-maintainers\ndoc_type: guide\n"
        "last_reviewed: 2026-10-10\n---\n\n"
        f"# {title} 안내\n\n이 안내는 병합 충돌 재현 시험이 만든 문서입니다.\n",
        encoding="utf-8",
        newline="\n",
    )
    index = repo / "docs/adr/README.md"
    text = index.read_text(encoding="utf-8")
    if INDEX_LINE.search(text):
        # The rule while the index was a hand-kept list: one line per ADR, added at the end.
        index.write_text(
            text.rstrip("\n") + f"\n- [ADR-{number:04d} — {title}](./{adr_name})\n",
            encoding="utf-8",
            newline="\n",
        )
    commands = documentation_commands(repo)
    for command in failing(repo, commands):
        result = run_command(repo, regenerate_variant(command))
        if result.returncode != 0:
            raise AssertionError(f"regenerating failed: {command}\n{result.stderr}")
    still = failing(repo, commands)
    if still:
        raise AssertionError(f"branch is not green on its own after regenerating: {still}")
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "--no-verify", "-m", f"add ADR {number:04d} and {guide}")


def replay(rev: str | None) -> list[str]:
    """Return the problems found; an empty list means unrelated branches merge cleanly."""
    with tempfile.TemporaryDirectory(prefix="generated-docs-merge-") as temp:
        repo = Path(temp)
        materialize(rev, repo)
        git(repo, "init", "-q", "-b", "base")
        git(repo, "config", "user.name", "merge test")
        git(repo, "config", "user.email", "merge-test@example.invalid")
        git(repo, "config", "core.autocrlf", "false")
        git(repo, "config", "commit.gpgsign", "false")
        git(repo, "add", "-A")
        git(repo, "commit", "-q", "--no-verify", "-m", "base")

        numbers = [
            int(match.group(1))
            for path in (repo / "docs/adr").glob("[0-9]*.md")
            if (match := re.match(r"(\d{4})-", path.name))
        ]
        first = max(numbers) + 1

        git(repo, "checkout", "-q", "-b", "a", "base")
        contribute(repo, first, "merge-test-first", "첫 번째 시험 결정", "merge-test-first")
        git(repo, "checkout", "-q", "-b", "b", "base")
        contribute(repo, first + 1, "merge-test-second", "두 번째 시험 결정", "merge-test-second")

        problems: list[str] = []
        merged = git(repo, "merge", "--no-edit", "--no-verify", "-q", "a", check=False)
        if merged.returncode != 0:
            conflicted = git(repo, "diff", "--name-only", "--diff-filter=U").stdout.split()
            problems.append(f"textual conflict in: {', '.join(conflicted) or merged.stdout + merged.stderr}")
            return problems
        for command in failing(repo, documentation_commands(repo)):
            problems.append(f"merged tree fails without regenerating: {command}")
        return problems


class UnrelatedDocumentBranchesMergeTest(unittest.TestCase):
    def test_two_unrelated_adr_and_doc_branches_merge_cleanly_and_stay_green(self) -> None:
        problems = replay(None)
        self.assertEqual(problems, [], "\n".join(problems))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rev", help="replay a committed revision instead of the working tree")
    args, rest = parser.parse_known_args()
    if args.rev is None:
        return 0 if unittest.main(argv=[sys.argv[0], *rest], exit=False).result.wasSuccessful() else 1
    problems = replay(args.rev)
    for problem in problems:
        print(f"CONFLICT {problem}")
    if not problems:
        print(f"OK {args.rev}: unrelated branches merge cleanly and the merged tree stays green")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
