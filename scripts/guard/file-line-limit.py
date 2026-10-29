#!/usr/bin/env python3
"""No tracked source or document file grows past the line limit without a recorded reason.

What failing this prevents: on 2026-10-28 thirteen tracked files were over the 1500-line rule
that foundation's and gongzzang's AGENTS.md state (xtask's main.rs at 3625 lines), because only
gongzzang's `repo-guard` enforced it and only inside products/gongzzang. Every other area grew
past it one commit at a time until splitting took a series of pull requests.

A file over the limit fails unless `tools/ci/file-line-limit.exceptions.json` names it with a
reason and a ceiling; past its ceiling it fails again. Generated files match `generated` and are
not counted (their size belongs to their source). An exception whose file is gone, back under the
limit, or under its ceiling is reported so the next change lowers or deletes it — a warning, not a
failure, so the split pull requests in flight do not each have to edit this one file.
"""

from __future__ import annotations

import fnmatch
import json
import subprocess
import sys
from pathlib import Path

SUFFIXES = (".md", ".rs", ".ts", ".tsx", ".sql", ".py", ".sh", ".js", ".mjs")


def main(root: Path, contract_path: Path) -> int:
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    limit = int(contract["limit"])
    generated = contract["generated"]
    exceptions = contract["exceptions"]
    listed = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z"], check=True, capture_output=True
    ).stdout.decode("utf-8").split("\0")
    errors, notes = [], []
    counted = 0
    sizes: dict[str, int] = {}
    for path in listed:
        if not path.endswith(SUFFIXES) or any(fnmatch.fnmatch(path, g) for g in generated):
            continue
        try:
            sizes[path] = (root / path).read_bytes().count(b"\n")
        except OSError:  # listed but deleted in the working tree
            continue
        counted += 1
    for path, lines in sorted(sizes.items()):
        if lines <= limit:
            continue
        entry = exceptions.get(path)
        if entry is None:
            errors.append(f"{path}: {lines} lines, over {limit}; split it (or record an exception with a reason)")
        elif lines > int(entry["lines"]):
            errors.append(f"{path}: {lines} lines, past its recorded ceiling {entry['lines']}; it may only shrink")
    for path, entry in sorted(exceptions.items()):
        if not str(entry.get("reason", "")).strip():
            errors.append(f"{path}: an exception needs a one-line reason")
        lines = sizes.get(path)
        if lines is None or lines <= limit:
            notes.append(f"{path}: no longer over {limit}; delete its exception")
        elif lines < int(entry["lines"]):
            notes.append(f"{path}: now {lines} lines; lower its ceiling from {entry['lines']}")
    for note in notes:
        print(f"warning: file-line-limit: {note}", file=sys.stderr)
    if errors:
        print("FAIL file-line-limit:", file=sys.stderr)
        for error in errors:
            print(f"    {error}", file=sys.stderr)
        return 1
    print(f"OK file-line-limit (files={counted}, exceptions={len(exceptions)})")
    return 0


if __name__ == "__main__":
    repo = Path(sys.argv[1])
    raise SystemExit(main(repo, Path(sys.argv[2]) if len(sys.argv) > 2 else repo / "tools/ci/file-line-limit.exceptions.json"))
