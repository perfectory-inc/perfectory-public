#!/usr/bin/env python3
"""Before a deploy touches anything: the approval settings its pending migrations wait for are set.

  migration-approval-preflight.py <commit tree> -- <psql command...>

<commit tree> holds the commit being deployed: its Foundation migrations, the approval gate checker
(scripts/guard/migration-approval-gates.py, which finds the gates) and its contract (which names the
value each gate waits for). The psql command reads one statement on stdin and prints unaligned rows
from the production database. For each gate whose migration that database has not applied, the live
value of its setting must be the declared one, or the deploy stops here: one line per setting.

foundation-deploy.sh runs this before step 0, so a refusal pauses no DAG and restarts no container.
On 2026-10-14 #465's migration refused in step 4, after the API container had been recreated: the
API stayed down about 30 minutes and every DAG stayed paused (root ADR-0259 §4).

It fails when it cannot look: a database that does not answer is not one whose settings are set.
"""

from __future__ import annotations

import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

FOUNDATION_MIGRATIONS = "platforms/foundation-platform/migrations/"
SETTING_NAME = re.compile(r"[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*")


def load_checker(tree: Path):
    path = tree / "scripts/guard/migration-approval-gates.py"
    spec = importlib.util.spec_from_file_location("migration_approval_gates", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def refusals(tree: Path, query) -> list[str]:
    """Every pending gate whose setting the database does not hold; `query(sql)` returns rows."""
    checker = load_checker(tree)
    gates = sorted(gate for gate in checker.gates(tree) if gate[0].startswith(FOUNDATION_MIGRATIONS))
    if not gates:
        return []
    contract = json.loads((tree / checker.CONTRACT).read_text(encoding="utf-8"))
    declared = {(entry["migration"], entry["setting"]): entry for entry in contract["gates"]}
    applied = set(query("select version from _sqlx_migrations where success"))
    if not applied:
        return ["the production database reports no applied migration; its ledger could not be read"]
    out = []
    for migration, setting in gates:
        version = Path(migration).name.split("_", 1)[0]
        if version in applied:
            continue
        entry = declared.get((migration, setting))
        if entry is None or not SETTING_NAME.fullmatch(setting):
            out.append(f"{migration} waits for {setting}, which {checker.CONTRACT} does not declare")
            continue
        live = (query(f"select coalesce(current_setting('{setting}', true), '')") or [""])[0]
        if live != entry["value"]:
            out.append(
                f"{Path(migration).name} waits for {setting} = '{entry['value']}' on database "
                f"{entry['database']} (it is '{live}'). After the owner's approval: "
                f"ALTER DATABASE {entry['database']} SET {setting} = '{entry['value']}'; then deploy again")
    return out


def main(argv: list[str]) -> int:
    if len(argv) < 4 or argv[2] != "--":
        print("usage: migration-approval-preflight.py <commit tree> -- <psql command...>", file=sys.stderr)
        return 64
    tree, psql = Path(argv[1]), argv[3:]

    def query(sql: str) -> list[str]:
        result = subprocess.run(psql, input=sql + "\n", capture_output=True, text=True, timeout=120, check=False)
        if result.returncode != 0:
            raise RuntimeError(f"psql exited {result.returncode}: {result.stderr.strip()[-300:]}")
        return [line.strip() for line in result.stdout.splitlines() if line.strip()]

    try:
        found = refusals(tree, query)
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
        found = [f"the production database could not be asked: {error}"]
    for line in found:
        print(f"foundation-deploy: REFUSED before anything was paused or restarted: {line}", file=sys.stderr)
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
