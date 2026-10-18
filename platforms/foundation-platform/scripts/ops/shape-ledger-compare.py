#!/usr/bin/env python3
"""Compare the shape ledgers production last captured with main's, for the job summary (root ADR-0279).

The shape ledger check on the host puts both captures in the private ledger bucket; the scheduled
workflow shape-ledger-refresh runs this in the protected environment shape-ledger, whose read-only
key reads that bucket only. Without the key it says so and skips. It prints Markdown and always
exits 0: it gates nothing, and a stale ledger is a pull request to take (root ADR-0259 §5), which
the prepared-PR App opens once it exists.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from pathlib import Path

PLATFORM = Path(__file__).resolve().parents[2]
READ_ENV = ("FOUNDATION_PLATFORM_R2_SHAPE_LEDGER_ENDPOINT_HOST",
            "FOUNDATION_PLATFORM_SHAPE_LEDGER_READER_ACCESS_KEY_ID",
            "FOUNDATION_PLATFORM_SHAPE_LEDGER_READER_SECRET_ACCESS_KEY")


def module(name: str, file: str):
    spec = importlib.util.spec_from_file_location(name, PLATFORM / "scripts/ops" / file)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


def bucket() -> str:
    """The ledger bucket, as the R2 connection contract pins it."""
    contract = json.loads((PLATFORM / "config/r2-connections.contract.json").read_text(encoding="utf-8"))
    return contract["connections"]["shape_ledger"]["expected_values"]["FOUNDATION_PLATFORM_R2_SHAPE_LEDGER_BUCKET"]


def report(captured: dict[str, bytes | None]) -> list[str]:
    """Markdown lines: per ledger, what production holds that main's ledger does not say."""
    check = module("shape_ledger_check", "shape-ledger-check.py")
    lines, stale = ["## Production shape ledgers", ""], False
    for relative, key in ((check.LAKEHOUSE, "tables"), (check.POSTGRES, "tables_with_rows")):
        name = Path(relative).name
        raw = captured.get(name)
        if raw is None:
            lines.append(f"- `{name}`: no capture in the ledger bucket yet")
            continue
        live = json.loads(raw)
        found = check.differences(json.loads((PLATFORM / relative).read_text(encoding="utf-8")), live, key)
        stale = stale or bool(found)
        lines.append(f"- `{name}` (captured {live.get('captured_at', '?')}): "
                     + ("matches main" if not found else f"{len(found)} tables differ"))
        lines += [f"  - {line}" for line in found]
    lines += ["", ("A refresh pull request is due: copy the captures unchanged (root ADR-0259 §5)."
                   if stale else "Nothing to refresh.")]
    return lines


def main() -> int:
    values = [os.environ.get(name, "") for name in READ_ENV]
    if not all(values):
        missing = [name for name, value in zip(READ_ENV, values) if not value]
        print("## Production shape ledgers\n\nSkipped: the environment `shape-ledger` holds no "
              f"{', '.join(missing)} yet (owner steps: release-artifact-bucket.md §8).")
        return 0
    host, key, secret = values
    r2 = module("r2_object", "r2_object.py")
    names = ("lakehouse-production-shape.json", "postgres-production-shape.json")
    captured = {name: r2.get(f"https://{host}", bucket(), "shape-ledger/" + name, key, secret) for name in names}
    print("\n".join(report(captured)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
