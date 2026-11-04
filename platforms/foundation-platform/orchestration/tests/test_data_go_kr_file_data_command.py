"""The data.go.kr file-data job's real command line (scripts/ops/data-go-kr-file-data-cycle.sh, root ADR-0401).

The script runs from an installed release layout (as in test_registered_factory_command.py). The publisher is a
stand-in that records what it was asked and answers like the collector: a manifest of the files it landed, and the
outcome line. What runs for real is the script: it names every file of the contract but the registered factory
(its own job collects it), passes the outcome on, removes the copies whether the collector succeeds or not, and
stops before the portal when a variable is missing.
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

UNIT = "foundation-data-go-kr-file-data.service"
SCRIPT = PLATFORM / "scripts/ops/data-go-kr-file-data-cycle.sh"
CONTRACT = PLATFORM / "infra/lakehouse/contracts/data-go-kr-file-data.contract.json"
NEEDS = sorted(runtime_secrets.load().consumer(UNIT).needs)
REQUIRED = sorted(runtime_secrets.script_requirements(SCRIPT))
RELEASE_ID = "c" * 40

FAKE_PUBLISHER = r"""#!/usr/bin/env python3
import json, os, sys
prefix = "FOUNDATION_PLATFORM_DATA_GO_KR_FILE_DATA_"
with open(os.path.join(os.environ["FAKE_STATE"], "calls.log"), "a", encoding="utf-8") as log:
    log.write(sys.argv[1] + " " + os.environ.get(prefix + "DATASETS", "") + "\n")
assert sys.argv[1] == "collect-data-go-kr-file-data", sys.argv
assert os.environ[prefix + "LIVE_WRITE"] == "1"
out = os.environ[prefix + "OUTPUT_DIR"]
os.makedirs(os.path.join(out, "objects"), exist_ok=True)
landed = [s for s in os.environ.get("FAKE_LANDED", "").split(",") if s]
for slug in landed:
    open(os.path.join(out, "objects", slug + ".csv"), "wb").write(b"x")
json.dump({"files": [{"dataset_slug": s} for s in landed], "failed": []},
          open(os.path.join(out, "manifest.json"), "w", encoding="utf-8"))
if os.environ.get("FAKE_FAIL") == "1":
    sys.exit(1)
print("foundation-job-outcome " + ("changed" if landed else "unchanged"))
"""


class DataGoKrFileDataCommand(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="data-go-kr-file-data-")
        self.addCleanup(self.temp.cleanup)
        root = pathlib.Path(self.temp.name)
        base = root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("data-go-kr-file-data-cycle.sh", "admitted-writer-runtime.sh", "job-journal.sh", "database-url.sh"):
            (release / "scripts/ops" / name).write_bytes((PLATFORM / "scripts/ops" / name).read_bytes())
            (release / "scripts/ops" / name).chmod(0o755)
        contracts = release / "infra/lakehouse/contracts"
        contracts.mkdir(parents=True)
        (contracts / CONTRACT.name).write_bytes(CONTRACT.read_bytes())
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
        self.script = base / "current/scripts/ops/data-go-kr-file-data-cycle.sh"
        self.state = root / "state"
        self.fake = root / "fake"
        self.fake.mkdir()
        self.env = {
            "PATH": os.environ["PATH"], "FOUNDATION_DATA_GO_KR_FILE_DATA_STATE_ROOT": str(self.state),
            "FAKE_STATE": str(self.fake),
            **{name: "planted-" + name.lower() for name in NEEDS},
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT": "https://r2.invalid",
        }

    def run_job(self, env=None):
        return subprocess.run(["bash", str(self.script)], env=env or self.env, capture_output=True, text=True, timeout=120)

    def test_every_file_but_the_registered_factory_is_asked_and_the_outcome_is_passed_on(self):
        files = json.loads(CONTRACT.read_text(encoding="utf-8"))["files"]
        same = self.run_job()
        self.assertEqual(same.returncode, 0, same.stderr + same.stdout)
        self.assertEqual(same.stdout.splitlines()[-1], "foundation-job-outcome unchanged")
        command, datasets = (self.fake / "calls.log").read_text().splitlines()[-1].split(" ")
        self.assertEqual(datasets.split(","), [slug for slug in files if slug != "registered_factory"])
        self.assertEqual(len(datasets.split(",")), 8)
        self.env["FAKE_LANDED"] = "airport,port"
        new = self.run_job()
        self.assertEqual(new.returncode, 0, new.stderr + new.stdout)
        self.assertEqual(new.stdout.splitlines()[-1], "foundation-job-outcome changed")
        self.assertIn("landed=[airport port]", (self.state / "journal.log").read_text(encoding="utf-8"))
        self.assertEqual(sorted(self.state.rglob("*.csv")), [])

    def test_a_failed_collection_fails_the_run_and_leaves_no_copy(self):
        self.env.update(FAKE_LANDED="airport", FAKE_FAIL="1")
        failed = self.run_job()
        self.assertNotEqual(failed.returncode, 0)
        self.assertNotIn("foundation-job-outcome", failed.stdout)
        self.assertIn("FAILED", (self.state / "journal.log").read_text(encoding="utf-8"))
        self.assertEqual(sorted(self.state.rglob("*.csv")), [])

    def test_a_missing_variable_stops_the_run_before_anything_reaches_the_portal(self):
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
