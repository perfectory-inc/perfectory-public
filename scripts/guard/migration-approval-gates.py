#!/usr/bin/env python3
"""Every migration approval gate is declared, with the setting production already holds (ADR-0259 §4).

A migration whose top-level `DO` block reads `current_setting('<name>', true)` refuses to run until
an operator has set `<name>` on the production database. The server applies migrations on its own
when main moves, so merging such a migration is not its approval: the setting must be there first.
tools/ci/migration-approval-gates.contract.json names each gate and the day the setting was set in
production. A gate the contract does not name, an entry that does not say the setting is set, and
an entry whose migration no longer holds that gate all fail.

What real incident does failing this prevent? On 2026-10-14 #465 merged a drop migration guarded by
`foundation.adr_0194_drop_approved`; nobody had set it, the deploy's migrate step failed after the
API container had been recreated, the API stayed down about 30 minutes and every DAG stayed paused.
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import date
from pathlib import Path

CONTRACT = "tools/ci/migration-approval-gates.contract.json"
SKIPPED = {".git", "node_modules", "target"}
DO_BLOCK = re.compile(r"\bDO\s+(\$\w*\$)(.*?)\1", re.IGNORECASE | re.DOTALL)
SETTING = re.compile(r"current_setting\(\s*'([^']+)'")


def gates(root: Path) -> set[tuple[str, str]]:
    """(migration path, setting) for every setting a migration's `DO` block reads."""
    found = set()
    for directory, names, files in os.walk(root):
        names[:] = sorted(name for name in names if name not in SKIPPED)
        here = Path(directory)
        if "migrations" not in here.relative_to(root).parts:
            continue
        for file in files:
            if not file.endswith(".sql"):
                continue
            path = here / file
            text = path.read_text(encoding="utf-8")
            for block in DO_BLOCK.finditer(text):
                for setting in SETTING.findall(block.group(2)):
                    found.add((path.relative_to(root).as_posix(), setting))
    return found


def problems(root: Path) -> list[str]:
    contract = json.loads((root / CONTRACT).read_text(encoding="utf-8"))
    declared: dict[tuple[str, str], dict] = {}
    out = []
    for entry in contract["gates"]:
        key = (entry.get("migration", ""), entry.get("setting", ""))
        declared[key] = entry
        set_on = entry.get("set_in_production")
        try:
            date.fromisoformat(set_on)
        except (TypeError, ValueError):
            out.append(f"{key[0]}: {key[1]} has no set_in_production date; set it on the production "
                       "database (after the owner's approval) before this merges, then record the day")
        if not str(entry.get("value", "")).strip() or not str(entry.get("database", "")).strip():
            out.append(f"{key[0]}: {key[1]} must name its database and value")
    found = gates(root)
    for migration, setting in sorted(found - set(declared)):
        out.append(f"{migration}: reads {setting} in a DO block, an approval gate {CONTRACT} does not declare")
    for migration, setting in sorted(set(declared) - found):
        out.append(f"{CONTRACT}: {migration} holds no gate on {setting}; remove the entry")
    return out


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else Path(__file__).resolve().parents[2])
    found = problems(root)
    for problem in found:
        print(f"FAIL migration-approval-gates: {problem}", file=sys.stderr)
    if found:
        return 1
    print("OK migration-approval-gates")
    return 0


if __name__ == "__main__":
    sys.exit(main())
