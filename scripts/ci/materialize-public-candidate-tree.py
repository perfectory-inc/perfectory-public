#!/usr/bin/env python3
"""Copy the Git public-candidate set into an empty, history-free directory."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path, PurePosixPath
from typing import NoReturn


def fail(message: str) -> NoReturn:
    raise SystemExit(f"FAIL public-candidate-tree: {message}")


def main() -> None:
    if len(sys.argv) != 3:
        fail("usage: materialize-public-candidate-tree.py <source> <destination>")

    source = Path(sys.argv[1]).resolve(strict=True)
    destination = Path(sys.argv[2]).resolve()
    try:
        destination.relative_to(source)
    except ValueError:
        pass
    else:
        fail("destination must be outside the source repository")

    destination.mkdir(parents=True, exist_ok=True)
    if any(destination.iterdir()):
        fail("destination must be empty")

    safe_git = os.environ.get("PERFECTORY_SAFE_GIT_TRANSPORT")
    bash_executable = os.environ.get("PERFECTORY_BASH_EXECUTABLE")
    if not safe_git or not bash_executable:
        fail("safe Git transport/Bash was not provided by the shell entry point")
    git_command = [bash_executable, safe_git]
    trusted_index = os.environ.get("PERFECTORY_TRUSTED_GIT_INDEX_FILE")
    if trusted_index:
        git_command.extend(["--trusted-index-file", trusted_index.replace("\\", "/")])
    git_command.extend(["--repository", str(source).replace("\\", "/")])

    try:
        raw_paths = subprocess.run(
            git_command
            + [
                "ls-files",
                "-z",
                "--cached",
                "--others",
                "--exclude-standard",
                "--deduplicate",
            ],
            check=True,
            stdout=subprocess.PIPE,
        ).stdout.split(b"\0")
    except subprocess.CalledProcessError:
        fail("source is not a readable Git repository")

    copied = 0
    for raw_path in raw_paths:
        if not raw_path:
            continue
        relative_text = os.fsdecode(raw_path).replace("\\", "/")
        relative = PurePosixPath(relative_text)
        if relative.is_absolute() or ".." in relative.parts:
            fail(f"unsafe candidate path: {relative_text!r}")

        input_path = source.joinpath(*relative.parts)
        # An unstaged working-tree deletion is absent from the current candidate.
        if not input_path.exists() and not input_path.is_symlink():
            continue
        if input_path.is_symlink() or not input_path.is_file():
            fail(f"only regular files are allowed: {relative_text}")
        try:
            input_path.resolve(strict=True).relative_to(source)
        except ValueError:
            fail(f"candidate escapes source repository: {relative_text}")

        output_path = destination.joinpath(*relative.parts)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(input_path, output_path)
        copied += 1

    if copied == 0:
        fail("candidate contains no regular files")
    print(f"OK public-candidate-tree files={copied}")


if __name__ == "__main__":
    main()
