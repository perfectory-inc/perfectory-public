"""The edition readiness job (scripts/ops/parcel-edition-readiness.sh, root ADR-0379 §2) on a stand-in publisher.

Silver's rows per edition come from the publisher's `lakehouse-partitions` (root ADR-0565; it asked Trino's
`$partitions`, and from 2026-11-04 to 2026-11-11 a statement Trino's CLI refused left every run's Silver
unmeasured). The stand-in answers as the command does, one JSON line per table asked; a partition of
another field (`sigungu_code`) splits an edition's rows, which the judgement adds up. Row counts are synthetic.
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

FAKE_PUBLISHER = r'''#!/usr/bin/env python3
import json, os, sys
asked = sys.stdin.read().split()
open(os.environ["FAKE_LOG"], "a").write(json.dumps({"args": sys.argv[1:], "asked": asked}) + "\n")
if os.environ.get("FAKE_FAIL"):
    sys.exit("the catalog did not answer")
for table in asked:
    print(json.dumps({"table": table, "snapshot_id": 7, "partitions": json.loads(os.environ["FAKE_PARTITIONS"])}))
'''


@unittest.skipUnless(shutil.which("bash"), "needs bash")
class ReadinessScriptTest(unittest.TestCase):
    def run_job(self, partitions: list[dict], fail: bool = False) -> tuple[subprocess.CompletedProcess, dict, list]:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            publisher = root / "publisher"
            publisher.write_text(FAKE_PUBLISHER, encoding="utf-8")
            publisher.chmod(0o755)
            runtime = root / "runtime.sh"
            runtime.write_text(f'PUBLISHER_BIN={publisher.as_posix()!r}\n', encoding="utf-8")
            log = root / "publisher.log"
            out = root / "editions" / "readiness.json"
            env = {
                **os.environ,
                "FAKE_LOG": str(log),
                "FAKE_PARTITIONS": json.dumps(partitions),
                "FOUNDATION_PARCEL_EDITION_ADMITTED_RUNTIME": runtime.as_posix(),
                "FOUNDATION_PLATFORM_RELEASE_ROOT": str(PLATFORM),
                "FOUNDATION_PARCEL_EDITION_READINESS_PATH": str(out),
                "FOUNDATION_VWORLD_PARCEL_EDITION_STATE_ROOT": str(root / "absent"),
                "FOUNDATION_PARCEL_TILE_BAKE_STATE_ROOT": str(root / "absent"),
                "FOUNDATION_PLATFORM_JOB_SWITCHES_DIR": str(root / "switches"),
                "VWORLD_PARCEL_PROMOTIONS_LEDGER": str(root / "editions" / "served-ledger.jsonl"),
            }
            if fail:
                env["FAKE_FAIL"] = "1"
            done = subprocess.run([shutil.which("bash"), SCRIPT.as_posix()], env=env, capture_output=True, text=True, timeout=120)
            report = json.loads(out.read_text(encoding="utf-8")) if out.is_file() else {}
            calls = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()] if log.is_file() else []
            return done, report, calls

    def test_silver_is_measured_from_the_manifests_partitions(self):
        contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
        served = contract["served_edition"]
        prefix = contract["snapshot_id_prefix"]
        partitions = [{"partition": {"source_snapshot_id": f"{prefix}{name}", "sigungu_code": code},
                       "record_count": 500 + i, "files": 1}
                      for i, name in enumerate(sorted(contract["editions"])) for code in (None, "99999")]
        done, report, calls = self.run_job(partitions)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertNotIn("silver not measured", done.stderr)
        self.assertEqual(calls, [{"args": ["lakehouse-partitions"], "asked": ["silver.parcel_boundaries"]}])
        served_row = next(row for row in report["editions"] if row["edition"] == served)
        self.assertTrue(served_row["stages"]["silver"]["passed"], served_row["stages"]["silver"])

    def test_an_unmeasured_silver_is_said_and_not_ready(self):
        for partitions, fail in (([], False), ([{"partition": {"source_snapshot_id": None}, "record_count": 1, "files": 1}], False),
                                 ([], True)):
            with self.subTest(fail=fail, partitions=partitions):
                done, report, _ = self.run_job(partitions, fail)
                self.assertEqual(done.returncode, 0, done.stderr)
                for row in report["editions"]:
                    self.assertFalse(row["stages"]["silver"]["passed"])
                if fail:
                    self.assertIn("silver not measured: the catalog did not answer", done.stderr)


if __name__ == "__main__":
    unittest.main()
