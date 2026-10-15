#!/usr/bin/env python3
"""The committed production-shape ledgers still describe production (root ADR-0259 §5).

CI gates every pull request on two ledgers the release ships: the lakehouse's
(infra/lakehouse/contracts/lakehouse-production-shape.json) and Postgres's
(config/postgres-production-shape.json). Production moves away from them when a deploy migrates
a table, a load creates a table or its first rows, or a rewrite retires a file shape. This job
captures both again, read-only, right after each deploy (started_once_after_deploy) and daily
after the loads, and fails when either differs from the release's, one line per table. The fresh
ledgers are left in the state directory for the refresh pull request:
  /var/lib/foundation-shape-ledger/ (the unit's StateDirectory)
Only the shape is compared, not when it was captured.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

RELEASE = Path(os.environ.get("FOUNDATION_SHAPE_LEDGER_RELEASE", "/opt/foundation-platform/current"))
STATE = Path(os.environ.get("FOUNDATION_SHAPE_LEDGER_STATE", "/var/lib/foundation-shape-ledger"))
LAKEHOUSE = "infra/lakehouse/contracts/lakehouse-production-shape.json"
POSTGRES = "config/postgres-production-shape.json"


def differences(committed: dict, live: dict, key: str) -> list[str]:
    """Each table whose shape differs between the two ledgers, by name. `key` is what holds the
    shapes: a mapping of table to shape, or a list of table names."""
    old, new = committed[key], live[key]
    if isinstance(old, list):
        return ([f"{table}: now holds rows" for table in sorted(set(new) - set(old))]
                + [f"{table}: no longer holds rows" for table in sorted(set(old) - set(new))])
    return ([f"{table}: new in production" for table in sorted(set(new) - set(old))]
            + [f"{table}: gone from production" for table in sorted(set(old) - set(new))]
            + [f"{table}: shape changed" for table in sorted(set(old) & set(new)) if old[table] != new[table]])


def load_capture():
    path = RELEASE / "scripts/ops/lakehouse-production-shape.py"
    spec = importlib.util.spec_from_file_location("lakehouse_production_shape", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    STATE.mkdir(parents=True, exist_ok=True)
    capture = load_capture()
    lakehouse = capture.capture()
    (STATE / Path(LAKEHOUSE).name).write_text(capture.render(lakehouse), encoding="utf-8")
    postgres_text = subprocess.run(["bash", str(RELEASE / "scripts/ops/postgres-production-shape.sh")],
                                   capture_output=True, text=True, check=True, timeout=600).stdout
    (STATE / Path(POSTGRES).name).write_text(postgres_text, encoding="utf-8")
    found = [f"lakehouse {line}" for line in differences(
        json.loads((RELEASE / LAKEHOUSE).read_text(encoding="utf-8")), lakehouse, "tables")]
    found += [f"postgres {line}" for line in differences(
        json.loads((RELEASE / POSTGRES).read_text(encoding="utf-8")), json.loads(postgres_text), "tables_with_rows")]
    if not found:
        print("shape-ledger-check: production holds the shapes the release's ledgers record")
        return 0
    for line in found:
        print(f"shape-ledger-check: STALE {line}")
    print(f"shape-ledger-check: CI gates pull requests on a shape production no longer holds. Copy "
          f"{STATE}/{Path(LAKEHOUSE).name} to platforms/foundation-platform/{LAKEHOUSE} and "
          f"{STATE}/{Path(POSTGRES).name} to platforms/foundation-platform/{POSTGRES} in one pull request.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
