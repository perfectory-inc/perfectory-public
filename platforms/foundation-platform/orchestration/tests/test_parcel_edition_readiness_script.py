"""The edition readiness job (scripts/ops/parcel-edition-readiness.sh, root ADR-0379 §2) on a fake docker.

The fake answers like the Trino CLI does: `--execute` runs its one statement, and `--file` refuses a
statement with no terminating `;` ("Non-terminated statement"). From 2026-11-04 to 2026-11-11 the job sent
the partition query through `--file` without one, so every run left Silver unmeasured and the newest
edition never passed its Silver stage. The row counts are synthetic.
"""

import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts" / "ops" / "parcel-edition-readiness.sh"
CONTRACT = PLATFORM / "infra" / "lakehouse" / "contracts" / "vworld-parcel-source-objects.json"

FAKE_DOCKER = r'''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
open(os.environ["FAKE_LOG"], "a").write(json.dumps(args) + "\n")
if "--file" in args:
    statement = sys.stdin.read().strip()
    if not statement.endswith(";"):
        sys.exit("Non-terminated statement: " + statement)
elif "--execute" not in args:
    sys.exit("fake docker: neither --execute nor --file")
for line in json.loads(os.environ["FAKE_PARTITIONS"]):
    print(line)
'''


@unittest.skipUnless(shutil.which("bash"), "needs bash")
class ReadinessScriptTest(unittest.TestCase):
    def run_job(self, partitions: list[str]) -> tuple[subprocess.CompletedProcess, dict, list]:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            (bin_dir / "docker").write_text(FAKE_DOCKER, encoding="utf-8")
            (bin_dir / "docker").chmod(0o755)
            log = root / "docker.log"
            out = root / "editions" / "readiness.json"
            env = {
                **os.environ,
                "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                "FAKE_LOG": str(log),
                "FAKE_PARTITIONS": json.dumps(partitions),
                "FOUNDATION_PLATFORM_RELEASE_ROOT": str(PLATFORM),
                "FOUNDATION_PARCEL_EDITION_READINESS_PATH": str(out),
                "FOUNDATION_VWORLD_PARCEL_EDITION_STATE_ROOT": str(root / "absent"),
                "FOUNDATION_PARCEL_TILE_BAKE_STATE_ROOT": str(root / "absent"),
                "FOUNDATION_PLATFORM_JOB_SWITCHES_DIR": str(root / "switches"),
            }
            done = subprocess.run([shutil.which("bash"), SCRIPT.as_posix()], env=env, capture_output=True, text=True, timeout=120)
            report = json.loads(out.read_text(encoding="utf-8")) if out.is_file() else {}
            calls = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()] if log.is_file() else []
            return done, report, calls

    def test_silver_is_measured_from_the_partition_metadata(self):
        contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
        served = contract["served_edition"]
        prefix = contract["snapshot_id_prefix"]
        done, report, calls = self.run_job([f'"{prefix}{name}","{1000 + i}"'
                                            for i, name in enumerate(sorted(contract["editions"]))])
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertNotIn("silver not measured", done.stderr)
        self.assertEqual(len(calls), 1)
        self.assertIn("--execute", calls[0])
        self.assertNotIn("-i", calls[0])
        served_row = next(row for row in report["editions"] if row["edition"] == served)
        self.assertTrue(served_row["stages"]["silver"]["passed"], served_row["stages"]["silver"])

    def test_an_unmeasured_silver_is_said_and_not_ready(self):
        done, report, _ = self.run_job([])
        self.assertEqual(done.returncode, 0, done.stderr)
        for row in report["editions"]:
            self.assertFalse(row["stages"]["silver"]["passed"])


if __name__ == "__main__":
    unittest.main()
