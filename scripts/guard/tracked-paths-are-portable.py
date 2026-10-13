#!/usr/bin/env python3
"""Every tracked path can be checked out on Linux, macOS and Windows alike.

THE INCIDENT THIS GUARD EXISTS FOR

PR #478 committed eighteen files under platforms/foundation-platform named like
`X<U+F03A><temp>autodeploy-<random>state/lock`. A test wrote
a Windows temporary path (`X:\\<temp>\\state`) unquoted into a bash script;
bash ate the backslashes, the remaining `X:<temp>state` was relative to the
working directory, and Git for Windows (like Cygwin and WSL) stored the `:` it
cannot put in an NTFS name as the private-use character U+F03A. The result was
a lock file per test run, inside the area, and `git add -A` took them all.

A name like that is never a real file: it is a Windows path that leaked into a
POSIX one. Such a name either cannot be checked out on Windows at all (`:`,
`\\`, `<>"|?*`), or only exists because one of those characters was translated
into the U+F000-U+F0FF range. Both are refused here, along with the other names
Windows cannot create: control characters, a component ending in a dot or a
space, and the reserved device names.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

# Characters Windows refuses in a file name. `/` is the separator Git uses.
RESERVED_CHARACTERS = frozenset('\\:*?"<>|')
# Cygwin, MSYS2 (Git for Windows) and WSL store each reserved character as
# U+F000 + its code point, so `:` arrives as U+F03A.
TRANSLATED_RESERVED = range(0xF000, 0xF100)
RESERVED_DEVICE_NAMES = frozenset(
    {"CON", "PRN", "AUX", "NUL"}
    | {f"COM{n}" for n in range(1, 10)}
    | {f"LPT{n}" for n in range(1, 10)}
)


def component_problem(component: str) -> str | None:
    for character in component:
        if character in RESERVED_CHARACTERS:
            return f"contains {character!r}, which Windows cannot store"
        if ord(character) < 0x20:
            return "contains a control character"
        if ord(character) in TRANSLATED_RESERVED:
            return (
                f"contains U+{ord(character):04X}, a Windows-reserved character as "
                "Git for Windows/Cygwin/WSL store it (a Windows path leaked into a POSIX one)"
            )
    if component.endswith((".", " ")) and component not in {".", ".."}:
        return "ends in a dot or a space, which Windows drops"
    if component.split(".", 1)[0].upper() in RESERVED_DEVICE_NAMES:
        return "is a reserved Windows device name"
    return None


def violations(paths: list[str]) -> list[str]:
    found = []
    for path in paths:
        for component in path.split("/"):
            problem = component_problem(component)
            if problem is not None:
                found.append(f"{path!a}: {component!a} {problem}")
                break
    return found


def tracked_paths(root: Path) -> list[str]:
    output = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z"],
        check=True,
        capture_output=True,
    ).stdout
    return [path.decode("utf-8", "surrogateescape") for path in output.split(b"\0") if path]


def main(argv: list[str]) -> int:
    root = Path(argv[1]) if len(argv) > 1 else Path(__file__).resolve().parents[2]
    found = violations(tracked_paths(root))
    if found:
        print("FAIL tracked-paths-are-portable: these tracked paths cannot be checked out everywhere:", file=sys.stderr)
        for line in found:
            print(f"  {line}", file=sys.stderr)
        return 1
    print("OK tracked-paths-are-portable")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
