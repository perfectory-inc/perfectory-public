#!/usr/bin/env python3
"""The pinned OSV-Scanner reads every YAML document of a pnpm lockfile (root ADR-0414 amendment, W85).

pnpm 12 writes `pnpm-lock.yaml` as two documents when the project declares `packageManager`: a boot
document (pnpm's own version and binaries) and then the project's dependency graph. Measured
2026-11-18 on a lockfile pnpm 12.21.0 wrote: OSV-Scanner 2.5.0 read only the boot document (16
packages instead of 162 for the tile gateway; a planted `lodash@4.17.20` in the project document went
unreported, exit 0), 2.7.1 reads both (exit 1, lodash reported). A scanner that reads the first document
only would pass every npm tree whatever it locks, and nothing else would say so.

This scans `tools/osv-gate/planted-two-document-pnpm-lock.yaml.fixture` (a real pnpm 12 lockfile:
boot document, then `lodash@4.17.20`, GHSA-35jh-r3h4-6jhm / CVE-2021-23337) with the pinned scanner
and fails unless the scanner reports lodash. It runs before the repository ratchet in
`required/repository`.
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tools" / "osv-gate" / "planted-two-document-pnpm-lock.yaml.fixture"


def main() -> int:
    text = FIXTURE.read_text(encoding="utf-8")
    documents = [part for part in text.split("\n---\n") if part.strip()]
    if not text.startswith("---\n") or len(documents) != 2 or "lodash@4.17.20" not in documents[1]:
        print("FAIL osv-reads-every-document: the fixture must be two documents with lodash@4.17.20 in the second")
        return 1
    with tempfile.TemporaryDirectory() as scratch:
        work = Path(scratch)
        binary = work / ("osv-scanner.exe" if os.name == "nt" else "osv-scanner")
        fetched = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "ci" / "fetch_pinned.py"), "tool", "osv-scanner", "--output", str(binary)]
        )
        if fetched.returncode != 0:
            print("FAIL osv-reads-every-document: the pinned scanner could not be fetched")
            return 1
        binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
        lockfile = work / "planted" / "pnpm-lock.yaml"
        lockfile.parent.mkdir()
        shutil.copyfile(FIXTURE, lockfile)
        report = work / "report.json"
        scan = subprocess.run(
            [str(binary), "scan", "--format", "json", "--verbosity", "error", "--output-file", str(report), "-L", str(lockfile)]
        )
        found = set()
        if report.exists():
            data = json.loads(report.read_text(encoding="utf-8"))
            found = {
                (package["package"]["name"], package["package"]["version"])
                for result in data.get("results") or []
                for package in result.get("packages") or []
            }
        if scan.returncode != 1 or ("lodash", "4.17.20") not in found:
            print(
                "FAIL osv-reads-every-document: the pinned OSV-Scanner did not report lodash@4.17.20 from the second "
                f"document of a pnpm 12 lockfile (exit {scan.returncode}, found {sorted(found)}); it would pass every npm tree"
            )
            return 1
    print("OK osv-reads-every-document: the pinned scanner reads the project document of a two-document pnpm lockfile")
    return 0


if __name__ == "__main__":
    sys.exit(main())
