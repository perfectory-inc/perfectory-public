"""The parcel id registry's first registration (scripts/ops/parcel-registry-cycle.sh, root ADR-0243) on a fake docker.

The Spark stand-in answers the probe with the regions the registry lacks and records each region's
bootstrap. The real script reads the served edition and its date from the release's source contract. The
release layout is the host's (root ADR-0134). Region codes are synthetic.
"""

import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
RELEASE_ID = "c" * 40

FAKE_DOCKER = r'''#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
if args[:1] == ["rm"]:
    sys.exit(0)
assert args[0] == "compose" and "spark-submit" in args, args
submit = args[args.index("spark-submit"):]
script = next(a for a in submit if a.endswith(".py"))
job_args = submit[submit.index(script) + 1:]
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps({"job": pathlib.Path(script).name, "args": job_args,
                          "service": args[args.index("spark-submit") - 1],
                          "memory": submit[submit.index("--driver-memory") + 1]}) + "\n")
if "--probe-missing" in job_args:
    print("parcel-registry-probe " + json.dumps({"to_snapshot_id": "x", "missing": json.loads(os.environ["FAKE_MISSING"])}))
elif os.environ.get("FAKE_FAIL_REGION") and os.environ["FAKE_FAIL_REGION"] in job_args:
    sys.exit("refusing to register")
'''


class ParcelRegistryCycle(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="parcel-registry-")
        self.addCleanup(temp.cleanup)
        self.root = pathlib.Path(temp.name)
        base = self.root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        for part in ("infra/lakehouse/spark/jobs", "infra/lakehouse/contracts"):
            shutil.copytree(PLATFORM / part, release / part, ignore=shutil.ignore_patterns("__pycache__"))
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("parcel-registry-cycle.sh", "admitted-writer-runtime.sh", "database-url.sh", "job-journal.sh"):
            shutil.copy(PLATFORM / "scripts/ops" / name, release / "scripts/ops" / name)
            (release / "scripts/ops" / name).chmod(0o755)
        (base / "current").symlink_to(pathlib.Path("releases") / RELEASE_ID)
        artifacts = base / "artifacts" / RELEASE_ID
        artifacts.mkdir(parents=True)
        publisher = artifacts / "foundation-outbox-publisher"
        # The publisher answers the ledger's editions: the served edition, synthetic 시도 97.
        served = json.loads((PLATFORM / "infra/lakehouse/contracts/vworld-parcel-source-objects.json")
                            .read_text(encoding="utf-8"))["served_edition"]
        self.editions = self.root / "editions.json"
        self.editions.write_text(json.dumps({"editions": {served: {
            "provider_base_month": f"{served[:4]}-{served[4:]}",
            "extracted_on": {"earliest": "2099-01-01", "latest": "2099-01-01"},
            "handoff_prefix": "silver-handoff/synthetic", "granularity_counts": {"sido": 1, "sigungu": 1},
            "objects": [{"object_key": f"bronze/source=vworldkr__parcel/{served}-{code}.zip", "bytes": 1,
                         "dataset_name": f"LSMD_CONT_LDREG_{code}_{served}", "region_code": code,
                         "granularity": "sido" if len(code) == 2 else "sigungu"} for code in ("97", "97110")]}}}))
        publisher.write_text(f"#!/bin/sh\ncat '{self.editions}'\n")
        publisher.chmod(0o555)
        (artifacts / "build.json").write_text(json.dumps({
            "source": RELEASE_ID, "publisher_image": "sha256:" + "a" * 64, "tippecanoe_image": "sha256:" + "d" * 64,
            "files": {"foundation-outbox-publisher": hashlib.sha256(publisher.read_bytes()).hexdigest(),
                      "jars/fixture.jar": "0" * 64}}))
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        (bin_dir / "docker").write_text(FAKE_DOCKER)
        (bin_dir / "docker").chmod(0o755)
        self.log = self.root / "calls.jsonl"
        self.state = self.root / "state"
        self.script = base / "current/scripts/ops/parcel-registry-cycle.sh"
        self.env = {"PATH": f"{bin_dir}:{os.environ['PATH']}", "FAKE_LOG": str(self.log),
                    "FOUNDATION_PARCEL_REGISTRY_STATE_ROOT": str(self.state),
                    "FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT": str(self.root / "lakehouse"),
                    "DATABASE_URL": "postgres://fixture@127.0.0.1:1/fixture"}

    def run_job(self, missing, **env):
        result = subprocess.run(["bash", str(self.script)], env={**self.env, "FAKE_MISSING": json.dumps(missing), **env},
                                capture_output=True, text=True, timeout=120)
        calls = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        return result, calls

    def test_each_missing_region_is_bootstrapped_in_the_order_given_on_the_served_editions_date(self):
        result, calls = self.run_job([["98", 10], ["99", 20]])
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("--probe-missing", calls[0]["args"])
        boots = calls[1:]
        self.assertEqual([c["args"][c["args"].index("--to-sido") + 1] for c in boots], ["98", "99"])
        cli = [sys.executable, str(PLATFORM / "infra/lakehouse/spark/jobs/vworld_parcel_editions.py")]
        served = subprocess.run([*cli, "served"], capture_output=True, text=True, check=True).stdout.strip()
        date = subprocess.run([*cli, "valid-from", "--edition", served, "--editions", str(self.editions)],
                              capture_output=True, text=True, check=True).stdout.strip()[:10]
        for call in boots:
            self.assertIn("--bootstrap", call["args"])
            self.assertIn("--allow-non-smoke-write", call["args"])
            self.assertEqual(call["args"][call["args"].index("--to-date") + 1], date)
        # The largest region needs the 20g Spark, not the small one.
        self.assertEqual({(c["service"], c["memory"]) for c in calls}, {("spark", "16g")})
        lines = result.stdout.strip().splitlines()
        self.assertEqual(lines[-1], "foundation-job-outcome changed")
        self.assertIn("registered=98,99 remaining=0", lines[-2])
        journal = (self.state / "journal.log").read_text()
        self.assertIn("region=98 parcels=10 seconds=", journal)

    def test_nothing_missing_is_unchanged(self):
        result, calls = self.run_job([])
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(len(calls), 1)
        self.assertIn("outcome=unchanged reason=all_registered", result.stdout)

    def test_no_region_starts_after_the_budget(self):
        result, calls = self.run_job([["98", 10]], FOUNDATION_PARCEL_REGISTRY_START_BUDGET_MINUTES="0")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(len(calls), 1)
        self.assertIn("outcome=unchanged reason=budget_spent", result.stdout)
        self.assertIn("remaining=1", result.stdout)

    def test_a_refused_region_fails_the_run_and_starts_no_later_one(self):
        result, calls = self.run_job([["98", 10], ["99", 20]], FAKE_FAIL_REGION="98")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(calls), 2)
        self.assertNotIn("foundation-job-outcome", result.stdout)


if __name__ == "__main__":
    unittest.main()
