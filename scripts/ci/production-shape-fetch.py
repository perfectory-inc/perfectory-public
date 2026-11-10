#!/usr/bin/env python3
"""Puts production's published table shapes where the CI gates read them (root ADR-0476).

  production-shape-fetch.py [--root DIR]

The host's shape ledger check publishes each shape it captures to the ledger bucket, append-only, and
the head shape-ledger/latest.json names the current pair. This reads the head and the two objects it
names from the bucket's public read domain (the R2 connection contract's shape_ledger public base
URL; no key: the ledgers hold table and column names this repository has always carried), checks
each object's shape SHA-256 against the head, and writes them over the in-tree ledgers
(lakehouse-production-shape.json, postgres-production-shape.json), so every pull request is judged
against production as the host last saw it, not against a copy someone committed.

It says which head it used, and warns when production last confirmed it more than STALE_HOURS ago
(the host check then fails after 48 hours and pages). A head whose object does not match its hash
fails the step. Until the first head exists (the bucket is new) the committed ledgers stand and it
says so; root ADR-0476 §5 removes that fallback once the first head is published.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

AREA = "platforms/foundation-platform"
STALE_HOURS = 72
ATTEMPTS = 3


def check_module(root: Path):
    spec = importlib.util.spec_from_file_location("shape_ledger_check", root / AREA / "scripts/ops/shape-ledger-check.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def base_url(root: Path) -> str:
    contract = json.loads((root / AREA / "config/r2-connections.contract.json").read_text(encoding="utf-8"))
    return contract["connections"]["shape_ledger"]["expected_values"]["FOUNDATION_PLATFORM_R2_SHAPE_LEDGER_PUBLIC_BASE_URL"]


def fetch(url: str) -> bytes | None:
    """The object's bytes, None on 404; retries a transient failure, then raises."""
    for attempt in range(1, ATTEMPTS + 1):
        request = urllib.request.Request(url, headers={"User-Agent": "perfectory-ci-production-shape-fetch",
                                                       "Cache-Control": "no-cache"})
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None
            if attempt == ATTEMPTS or error.code < 500:
                raise
        except OSError:
            if attempt == ATTEMPTS:
                raise
    return None


def place(root: Path, head: dict, objects: dict[str, bytes], check) -> list[str]:
    """Writes each object over its in-tree ledger after checking its shape hash; the summary lines."""
    paths = {Path(relative).name: root / AREA / relative for relative in (check.LAKEHOUSE, check.POSTGRES)}
    if set(head["ledgers"]) != set(paths):
        raise ValueError(f"the head names {sorted(head['ledgers'])}, not {sorted(paths)}")
    lines = []
    for name, entry in sorted(head["ledgers"].items()):
        text = objects[name].decode("utf-8")
        if check.shape_digest(text) != entry["shape_sha256"]:
            raise ValueError(f"{entry['key']} does not hash to the head's {entry['shape_sha256']}")
        paths[name].write_text(text, encoding="utf-8", newline="\n")
        lines.append(f"- `{name}`: `{entry['key']}`")
    return lines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args(argv)
    check = check_module(args.root)
    base = base_url(args.root).rstrip("/")
    run = os.environ.get("GITHUB_RUN_ID", "local")
    try:
        raw = fetch(f"{base}/{check.HEAD}?run={run}")
    except OSError as error:
        raw, why = None, f"unreachable ({error})"
    else:
        why = "no head published yet"
    if raw is None:
        print(f"::warning::production shape: {why} at {base}; the committed ledgers stand (root ADR-0476 §5)")
        return 0
    head = json.loads(raw)
    if head.get("schema_version") != check.HEAD_SCHEMA:
        print(f"::error::production shape: {check.HEAD} is not {check.HEAD_SCHEMA}")
        return 1
    try:
        objects = {}
        for name, entry in head["ledgers"].items():
            body = fetch(f"{base}/{entry['key']}")
            if body is None:
                raise ValueError(f"{entry['key']} named by the head does not exist")
            objects[name] = body
        lines = place(args.root, head, objects, check)
    except (OSError, ValueError, KeyError) as error:
        print(f"::error::production shape: {error}")
        return 1
    checked = datetime.strptime(head["checked_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    hours = (datetime.now(timezone.utc) - checked).total_seconds() / 3600
    summary = ["### Production shape (root ADR-0476)", "",
               f"Judged against the head production confirmed at {head['checked_at']} "
               f"({hours:.0f} h ago, release `{head.get('release', '?')}`):", "", *lines]
    print("\n".join(summary))
    if hours > STALE_HOURS:
        print(f"::warning::production shape: the head is {hours:.0f} h old; the host's shape ledger check is not publishing")
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as handle:
            handle.write("\n".join(summary) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
