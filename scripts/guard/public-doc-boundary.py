#!/usr/bin/env python3
"""Keep private direction notes and competitive roadmaps out of public Git."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path, PurePosixPath


PUBLIC_CONTRACT_MARKER = "<!-- public-repository-safety: reviewed-public-contract -->"
DATED_NOTE = re.compile(r"^\d{4}-\d{2}-\d{2}-.+")
PRIVATE_STRATEGY_DOCUMENTS = {"next-actions.md", "roadmap.md"}


def tracked_paths(root: Path) -> list[PurePosixPath]:
    try:
        output = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z"],
            check=True,
            stdout=subprocess.PIPE,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as error:
        raise RuntimeError(f"cannot enumerate tracked files: {error}") from error
    return sorted(
        PurePosixPath(raw.decode("utf-8", errors="strict"))
        for raw in output.split(b"\0")
        if raw
    )


def is_documentation_path(path: PurePosixPath) -> bool:
    return any(part.casefold() == "docs" for part in path.parts[:-1])


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <repository-root>", file=sys.stderr)
        return 2

    root = Path(sys.argv[1]).resolve()
    try:
        paths = tracked_paths(root)
    except RuntimeError as error:
        print(f"FAIL public-doc-boundary: {error}", file=sys.stderr)
        return 1

    errors: list[str] = []
    for relative in paths:
        if not is_documentation_path(relative):
            continue
        source = root.joinpath(*relative.parts)
        # An unstaged deletion is absent from the current public candidate.
        if not source.is_file():
            continue

        name = relative.name.casefold()
        if name in PRIVATE_STRATEGY_DOCUMENTS:
            errors.append(
                f"{relative}: competitive roadmap/active queue belongs in private operations"
            )

        if not DATED_NOTE.fullmatch(relative.name):
            continue
        try:
            text = source.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as error:
            errors.append(f"{relative}: cannot verify dated documentation: {error}")
            continue
        if PUBLIC_CONTRACT_MARKER not in text:
            errors.append(
                f"{relative}: dated documentation requires the reviewed-public-contract marker"
            )

    if errors:
        for error in errors:
            print(f"FAIL public-doc-boundary: {error}", file=sys.stderr)
        return 1
    print("OK public-doc-boundary")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
