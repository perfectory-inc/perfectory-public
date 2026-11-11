"""The 주소DB job's real command line (scripts/ops/juso-address-db-cycle.sh, root ADR-0495).

The script runs from an installed release layout (as in test_juso_electronic_map_command.py). The publisher is a
stand-in that answers the collect command with a manifest. What runs for real is the script: the collector is run
live and a landed file is a change; a failed collection fails the run with the collector's summary in the journal;
and a missing variable stops the run before anything reaches the provider.
"""

import hashlib
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

ORCHESTRATION = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ORCHESTRATION / "dags"))

import job_specs  # noqa: E402

PLATFORM = job_specs.PLATFORM_ROOT
sys.path.insert(0, str(PLATFORM / "scripts/deploy"))
import runtime_secrets  # noqa: E402

UNIT = "foundation-juso-address-db.service"
SCRIPT = PLATFORM / "scripts/ops/juso-address-db-cycle.sh"
NEEDS = sorted(runtime_secrets.load().consumer(UNIT).needs)
REQUIRED = sorted(runtime_secrets.script_requirements(SCRIPT))
RELEASE_ID = "c" * 40

FAKE_PUBLISHER = r"""#!/usr/bin/env python3
import json, os, sys
prefix = "FOUNDATION_PLATFORM_JUSO_ADDRESS_DB_"
open(os.path.join(os.environ["FAKE_STATE"], "calls.log"), "a", encoding="utf-8").write(sys.argv[1] + "\n")
assert sys.argv[1] == "collect-juso-address-db", sys.argv
assert os.environ[prefix + "LIVE_WRITE"] == "1"
assert os.environ[prefix + "STATE_DIR"].endswith("/collector")
out = os.environ[prefix + "OUTPUT_DIR"]
landed = [{"kind": "address_db_daily_change", "period": "20990101"}] if os.environ.get("FAKE_LANDED") == "1" else []
failed = "the 주소DB download of 20990102 answered HTTP 500" if os.environ.get("FAKE_FAILED") == "1" else None
json.dump({"requests": 2, "bytes": 0, "files": landed, "base_month": "209812", "next_day": "2099-01-02",
           "stop": None if failed else "Done", "failed": failed},
          open(os.path.join(out, "manifest.json"), "w", encoding="utf-8"))
sys.exit(1 if failed else 0)
"""


class JusoAddressDbCommand(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="juso-address-db-")
        self.addCleanup(self.temp.cleanup)
        root = pathlib.Path(self.temp.name)
        base = root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("juso-address-db-cycle.sh", "admitted-writer-runtime.sh", "job-journal.sh", "database-url.sh"):
            (release / "scripts/ops" / name).write_bytes((PLATFORM / "scripts/ops" / name).read_bytes())
            (release / "scripts/ops" / name).chmod(0o755)
        (base / "current").symlink_to(pathlib.Path("releases") / RELEASE_ID)
        artifacts = base / "artifacts" / RELEASE_ID
        artifacts.mkdir(parents=True)
        publisher = artifacts / "foundation-outbox-publisher"
        publisher.write_text(FAKE_PUBLISHER)
        publisher.chmod(0o555)
        (artifacts / "build.json").write_text(json.dumps({
            "source": RELEASE_ID, "publisher_image": "sha256:" + "a" * 64, "tippecanoe_image": "sha256:" + "d" * 64,
            "files": {"foundation-outbox-publisher": hashlib.sha256(publisher.read_bytes()).hexdigest(),
                      "jars/fixture.jar": "0" * 64}}))
        self.script = base / "current/scripts/ops/juso-address-db-cycle.sh"
        self.state = root / "state"
        self.fake = root / "fake"
        self.fake.mkdir()
        self.env = {
            "PATH": os.environ["PATH"], "FOUNDATION_JUSO_ADDRESS_DB_STATE_ROOT": str(self.state),
            "FAKE_STATE": str(self.fake),
            **{name: "planted-" + name.lower() for name in NEEDS},
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT": "https://r2.invalid",
        }

    def run_job(self, env=None):
        return subprocess.run(["bash", str(self.script)], env=env or self.env, capture_output=True, text=True, timeout=120)

    def test_the_collector_runs_live_and_a_landed_file_is_a_change(self):
        for landed, outcome in (("0", "unchanged"), ("1", "changed")):
            with self.subTest(landed=landed):
                result = self.run_job({**self.env, "FAKE_LANDED": landed})
                self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
                self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome " + outcome)
        self.assertEqual((self.fake / "calls.log").read_text().splitlines(), ["collect-juso-address-db"] * 2)

    def test_a_failed_collection_fails_the_run_and_the_journal_names_it(self):
        result = self.run_job({**self.env, "FAKE_FAILED": "1"})
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("foundation-job-outcome", result.stdout)
        journal = (self.state / "journal.log").read_text(encoding="utf-8")
        self.assertIn("COLLECT FAILED", journal)
        self.assertIn("20990102", journal)

    def test_a_missing_variable_stops_the_run_before_anything_reaches_the_provider(self):
        self.assertLessEqual(set(REQUIRED), set(NEEDS))
        for name in REQUIRED:
            with self.subTest(name):
                env = dict(self.env)
                del env[name]
                result = self.run_job(env)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(name, result.stderr)
                self.assertFalse((self.fake / "calls.log").exists())
                self.assertFalse(self.state.exists(), "no state was written")


if __name__ == "__main__":
    unittest.main()
