#!/usr/bin/env python3
"""No Spark job takes the current rows of `silver.parcel_boundaries` but through the served reader.

What failing this prevents (root ADR-0291 §5): on 2026-10-21 the 공매 kind producer built its parcel
universe from `current_rows` of the whole table. Every cadastral edition's rows are current, so it
attached 632 sales to parcels editions 202609/202610 hold and the served 202606 map does not, and the
by-PNU serving gate refused the release. `address_pnu.lakehouse_inputs` and the real-transaction
load had the same read. The one served-universe reader is `vworld_parcel_editions.served_rows`
(and `parcel_panel_silver_to_gold.read_served_parcels`, built on it).

The rule: in `platforms/foundation-platform/infra/lakehouse/spark/jobs/*.py`, a call of
`current_rows`, `current_rows_of_versioned` or `current_row_latest` whose arguments name
`parcel_boundaries` — literally, or through a name the module assigns from text naming it — fails.
A reader of one named edition filters `source_snapshot_id` itself and does not call these.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

JOBS = Path("platforms/foundation-platform/infra/lakehouse/spark/jobs")
CANONICAL = {"vworld_parcel_editions.py"}
CURRENT = {"current_rows", "current_rows_of_versioned", "current_row_latest"}
TABLE = "parcel_boundaries"


def _called(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def violations(path: Path) -> list[str]:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    tainted: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None:
            if TABLE in (ast.get_source_segment(source, node.value) or ""):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                tainted |= {target.id for target in targets if isinstance(target, ast.Name)}
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or _called(node) not in CURRENT:
            continue
        arguments = [*node.args, *(keyword.value for keyword in node.keywords)]
        text = " ".join(ast.get_source_segment(source, argument) or "" for argument in arguments)
        names = {sub.id for argument in arguments for sub in ast.walk(argument) if isinstance(sub, ast.Name)}
        if TABLE in text or names & tainted:
            found.append(f"{path.as_posix()}:{node.lineno}: {_called(node)} over {TABLE}")
    return found


def main(root: Path) -> int:
    jobs = root / JOBS
    if not jobs.is_dir():
        print(f"FAIL parcel-reads-the-served-edition: missing {jobs}", file=sys.stderr)
        return 1
    found = [line for path in sorted(jobs.glob("*.py")) if path.name not in CANONICAL for line in violations(path)]
    for line in found:
        print(f"FAIL parcel-reads-the-served-edition: {line}; read the served parcels through "
              "vworld_parcel_editions.served_rows (root ADR-0291, section 5)", file=sys.stderr)
    if found:
        return 1
    print("OK parcel-reads-the-served-edition")
    return 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parents[2]))
