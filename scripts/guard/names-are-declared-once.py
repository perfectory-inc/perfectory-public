#!/usr/bin/env python3
"""G4 (root ADR-0626 §7): a table name written in code is a name the contracts or the rename work list declare.

Every string literal shaped like a catalog table, `"<namespace>.<table>"` with a namespace the contracts use
(`silver`, `gold`, and the two the switches fold into silver, `reference` and `intelligence`), in a tracked
.rs/.py/.sql/.ts/.tsx file, resolves to one of:
  - a table of the Rust contracts (their committed artifact,
    `platforms/foundation-platform/infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json`);
  - an old or a new name on the naming registry's rename work list (`naming.renames`);
  - a generated companion `<table>__<role>` of one of those.
Test code is not read (a test's own synthetic names are its input), nor migrations (append-only history),
nor comment and docstring lines, nor the Lakekeeper dev rig's probes (`infra/aws-lakekeeper/probes/`: they make
the dev warehouse's smoke tables, the one place root ADR-0626 rule 7 lets a smoke table live). A literal that
is a name prefix of declared tables (`"silver.land_"`, `"silver.industrial_land_sale_notice"` for its
`_attachment`) is a prefix, not a table. A literal that only looks like a table (an authorization resource)
carries `names-are-declared-once: <why>` on its line or anywhere above it in the same block (no blank line
between).

What failing this prevents: a table was spelled by hand in 117 stage constants over 47 Rust files besides
its contract (root ADR-0626 F3), so a rename that misses one reads a table nothing writes any more. The
switch PR that renames a table must move every reader in the same PR, and this is the check that it did.
"""

from __future__ import annotations

import importlib.util
import json
import pathlib
import re
import subprocess
import sys

NAMING = "platforms/foundation-platform/config/naming.contract.json"
CONTRACTS = "platforms/foundation-platform/infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json"
NAMESPACES = ("silver", "gold", "reference", "intelligence")
LITERAL = re.compile(r"""["'`]((?:%s)\.([a-z][a-z0-9_]*))["'`]""" % "|".join(NAMESPACES))
# `<namespace>.<extension>`: a file name, not a table.
FILE_SUFFIXES = {"csv", "json", "jsonl", "md", "parquet", "py", "sql", "txt", "yaml", "yml"}
MARKER = "names-are-declared-once:"
SUFFIXES = (".rs", ".py", ".sql", ".ts", ".tsx")
TEST_PATH = re.compile(r"(^|/)(tests?|__tests__|fixtures?)/|(^|/)test_[^/]*\.py$|_test\.py$|\.test\.tsx?$|(^|/|_)tests?\.rs$")
COMMENT = re.compile(r"^\s*(//|#|--|\*|/\*|\"\"\"|''')")
DEV_RIG = "platforms/foundation-platform/infra/aws-lakekeeper/probes/"


def registry(root: pathlib.Path) -> dict:
    """The generated naming registry, or generated here from the definitions when the tree has not run
    `declared_definitions.py generate` (root ADR-0418)."""
    path = root / NAMING
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    spec = importlib.util.spec_from_file_location("declared_definitions", root / "scripts/catalog/declared_definitions.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return json.loads(module.artifacts(root)[NAMING])


def declared(root: pathlib.Path) -> set[str]:
    naming = registry(root)
    tables = set(json.loads((root / CONTRACTS).read_text(encoding="utf-8"))["contracts"])
    renames = naming["renames"]
    tables |= set(renames["tables"]) | set(renames["tables"].values()) | set(renames["not_recreated"])
    return tables | {f"{table}__{role}" for table in tables for role in naming["role_suffixes"]}


def tracked(root: pathlib.Path) -> list[str]:
    listed = subprocess.run(["git", "-C", str(root), "ls-files", "-z", *[f"*{s}" for s in SUFFIXES]],
                            capture_output=True, check=True).stdout.decode("utf-8")
    return [path for path in listed.split("\0") if path]


def production_lines(path: str, text: str) -> list[tuple[int, str]]:
    lines = text.splitlines()
    if path.endswith(".rs"):
        for index, line in enumerate(lines):
            if re.match(r"\s*#\[cfg\(test\)\]", line):
                lines = lines[:index]
                break
    return list(enumerate(lines, 1))


def marked(lines: list[tuple[int, str]], number: int) -> bool:
    """The marker on the line, or on a line above it in the same block (stopping at a blank line)."""
    at = number - 1
    while at >= 0 and lines[at][1].strip():
        if MARKER in lines[at][1]:
            return True
        at -= 1
    return False


def problems(root: pathlib.Path, paths: list[str] | None = None) -> list[str]:
    known = declared(root)
    found = []
    for path in paths if paths is not None else tracked(root):
        if TEST_PATH.search(path) or "/migrations/" in path or path.startswith(DEV_RIG):
            continue
        try:
            text = (root / path).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        lines = production_lines(path, text)
        for number, line in lines:
            if COMMENT.match(line):
                continue
            for match in LITERAL.finditer(line):
                name, table = match.group(1), match.group(2)
                if name in known or table.endswith("_") or table in FILE_SUFFIXES:
                    continue
                if any(other.startswith(f"{name}_") for other in known):
                    continue
                if marked(lines, number):
                    continue
                found.append(f"{path}:{number}: {name} is no table the contracts or naming.renames declare "
                             "(root ADR-0626 §7 G4); name a declared table, or mark a literal that is not a table "
                             f"with `{MARKER} <why>`")
    return found


def main(argv: list[str]) -> int:
    root = pathlib.Path(argv[1] if len(argv) > 1 else pathlib.Path(__file__).resolve().parents[2]).resolve()
    found = problems(root)
    for problem in found:
        print(f"FAIL names-are-declared-once: {problem}", file=sys.stderr)
    if not found:
        print("OK names-are-declared-once")
    return 1 if found else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
