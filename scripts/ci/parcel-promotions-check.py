#!/usr/bin/env python3
"""Judges the parcel source contract against production's promotions ledger (root ADR-0490 §5).

  parcel-promotions-check.py [--root DIR]

On the host every reader lays the host promotions ledger over the source contract, and a ledger that
does not continue the contract stops all of them (vworld_parcel_editions.validate): the served map, the
by-PNU lanes and the parcel readers would refuse to start on the first deploy of such a contract. The
readiness job copies that ledger to the ledger bucket (scripts/ops/parcel-promotions-publish.py, the
way the shape ledger check publishes production's table shapes, root ADR-0476). This reads the head
parcel-editions/latest.json and the ledger it names from the bucket's public read domain (no key: the
ledger holds edition names, instants and the controller's or a staff id, which a reviewed copy into the
contract would carry too), checks the ledger's SHA-256 against the head, and loads this tree's contract
over it, so a pull request whose contract the host ledger does not continue fails here, not after its
deploy. Until the first head exists it says so and passes.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

AREA = "platforms/foundation-platform"
HEAD = "parcel-editions/latest.json"
HEAD_SCHEMA = "foundation-platform.parcel_promotions_head.v1"
ATTEMPTS = 3


def fetch(url: str) -> bytes | None:
    """The object's bytes, None on 404; retries a transient failure, then raises."""
    for attempt in range(1, ATTEMPTS + 1):
        request = urllib.request.Request(url, headers={"User-Agent": "perfectory-ci-parcel-promotions-check",
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


def editions_module(root: Path):
    jobs = root / AREA / "infra/lakehouse/spark/jobs"
    spec = importlib.util.spec_from_file_location("vworld_parcel_editions", jobs / "vworld_parcel_editions.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def base_url(root: Path) -> str:
    contract = json.loads((root / AREA / "config/r2-connections.contract.json").read_text(encoding="utf-8"))
    return contract["connections"]["shape_ledger"]["expected_values"]["FOUNDATION_PLATFORM_R2_SHAPE_LEDGER_PUBLIC_BASE_URL"]


def judge(root: Path, head: dict, ledger: bytes) -> str:
    """The served edition this tree's contract has over production's ledger; raises ValueError when the
    ledger is not the head's or the contract does not continue it."""
    if head.get("schema_version") != HEAD_SCHEMA:
        raise ValueError(f"{HEAD} is not {HEAD_SCHEMA}")
    if hashlib.sha256(ledger).hexdigest() != head["sha256"]:
        raise ValueError(f"{head['key']} does not hash to the head's {head['sha256']}")
    editions = editions_module(root)
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "served-ledger.jsonl"
        path.write_bytes(ledger)
        try:
            contract = editions.load(editions.CONTRACT_PATH, path)
        except editions.EditionError as error:
            raise ValueError(f"production's promotions ledger does not continue this contract: {error}") from error
    return editions.served(contract)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args(argv)
    base = base_url(args.root).rstrip("/")
    try:
        raw = fetch(f"{base}/{HEAD}")
    except OSError as error:
        print(f"::warning::parcel promotions: {base} unreachable ({error}); the contract is judged alone")
        return 0
    if raw is None:
        print(f"::warning::parcel promotions: no head published yet at {base}/{HEAD}; the contract is judged alone")
        return 0
    try:
        head = json.loads(raw)
        ledger = fetch(f"{base}/{head['key']}")
        if ledger is None:
            raise ValueError(f"{head['key']} named by the head does not exist")
        served = judge(args.root, head, ledger)
    except (OSError, ValueError, KeyError) as error:
        print(f"::error::parcel promotions: {error}")
        return 1
    print(f"parcel promotions: this contract continues production's ledger ({head['lines']} host promotions, "
          f"confirmed {head['checked_at']}, release {head.get('release', '?')}); served edition {served}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
