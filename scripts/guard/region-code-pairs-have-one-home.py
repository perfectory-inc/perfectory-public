#!/usr/bin/env python3
"""Region code pairs have one home, parcel pairs another (root ADR-0145 §4).

What failing this prevents: on 2026-10-04 the same fact, "region code X became Y", was found in 13
places. One stored crosswalk had two writers with different idempotency keys, two algorithms paired
dongs, the change table and the parcel lineage read each other, and the directions disagreed.
ADR-0145 kept one home for code pairs and one for parcel pairs and retired the rest. This check
keeps them retired: the next copy would look entirely reasonable to whoever adds it.

Everything it knows comes from the definition file
`platforms/foundation-platform/infra/lakehouse/contracts/region-code-holders.json`: the homes, the
column pairs that make a table a pair store, the retired holders and the paths that may still name
one. It refuses

1. a lakehouse table, other than the home and the derived tables the file names with a reason, whose
   columns hold both halves of a pair shape;
2. a code file naming a retired holder outside the paths listed for it;
3. a retired path that exists again.

Documentation (`docs` directories, Markdown) is not code and is not read.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

DEFINITION = "platforms/foundation-platform/infra/lakehouse/contracts/region-code-holders.json"


def table_problems(root: Path, definition: dict) -> list[str]:
    homes = definition["homes"]
    derived = definition["derived_tables"]
    problems = []
    for relative in definition["lakehouse_contracts"]:
        contracts = json.loads((root / relative).read_text(encoding="utf-8"))["contracts"]
        for name, contract in sorted(contracts.items()):
            columns = {column["name"] for column in contract.get("columns", [])}
            for kind, shapes in definition["pair_shapes"].items():
                for shape in shapes:
                    if set(shape) <= columns and name != homes[kind] and name not in derived:
                        problems.append(
                            f"{relative}: {name} stores {kind} ({' -> '.join(shape)}); their home is {homes[kind]}"
                        )
    return problems


def code_files(root: Path, definition: dict):
    skipped = set(definition["skipped_directories"])
    suffixes = tuple(definition["code_suffixes"])
    stack = [root]
    while stack:
        directory = stack.pop()
        for entry in directory.iterdir():
            if entry.is_dir():
                if entry.name not in skipped and not entry.is_symlink():
                    stack.append(entry)
            elif entry.name.endswith(suffixes):
                yield entry


def name_problems(root: Path, definition: dict) -> list[str]:
    retired = [
        (re.compile(re.escape(holder["name"]) + r"(?![A-Za-z0-9_])"), holder["name"], set(holder["allowed_paths"]))
        for holder in definition["retired"]
    ]
    problems = []
    for path in code_files(root, definition):
        relative = path.relative_to(root).as_posix()
        if relative == DEFINITION:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for pattern, name, allowed in retired:
            if relative in allowed:
                continue
            for number, line in enumerate(text.splitlines(), start=1):
                if pattern.search(line):
                    problems.append(f"{relative}:{number}: names the retired holder {name}")
                    break
    return problems


def path_problems(root: Path, definition: dict) -> list[str]:
    return [f"{relative}: a retired holder is back" for relative in definition["retired_paths"] if (root / relative).exists()]


def main(argv: list[str]) -> int:
    root = Path(argv[1] if len(argv) > 1 else Path(__file__).resolve().parents[2]).resolve()
    definition = json.loads((root / DEFINITION).read_text(encoding="utf-8"))
    problems = table_problems(root, definition) + name_problems(root, definition) + path_problems(root, definition)
    if problems:
        for problem in problems:
            print(f"FAIL region-code-pairs-have-one-home: {problem}", file=sys.stderr)
        print(f"    Code pairs live in {definition['homes']['code_pairs']}, parcel pairs in "
              f"{definition['homes']['parcel_pairs']}; read a view of them instead. Definition: {DEFINITION}. "
              "root ADR-0145", file=sys.stderr)
        return 1
    print("OK region-code-pairs-have-one-home")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
