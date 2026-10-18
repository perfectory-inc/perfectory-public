"""The court auction jobs' real command lines (scripts/ops/court-auction-collect.sh and
court-auction-silver-load.sh, root ADR-0280, ADR-0281).

The scripts run from an installed release layout (as in test_parcel_number_change_collect_command.py).
The publisher and docker are stand-ins: the publisher writes the manifest a night would write and
exits as the collector does (0, or 1 after a refusal); docker records that it was called. What runs
for real is the scripts: a night that landed objects hands them off and says `changed`; a refused
night still hands off what it landed and then fails; a missing contact stops the night before any
request; the load with nothing pending ends without starting Spark.
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

UNIT = "foundation-court-auction-collect.service"
SCRIPT = PLATFORM / "scripts/ops/court-auction-collect.sh"
NEEDS = sorted(runtime_secrets.load().consumer(UNIT).needs)
REQUIRED = sorted(runtime_secrets.script_requirements(SCRIPT))
RELEASE_ID = "b" * 40

FAKE_PUBLISHER = r"""#!/usr/bin/env python3
import json, os, sys
state = os.environ["FAKE_STATE"]
with open(os.path.join(state, "calls.log"), "a", encoding="utf-8") as log:
    log.write(sys.argv[1] + "\n")
assert sys.argv[1] == "collect-courtauction-go-kr", sys.argv
out = os.environ["FOUNDATION_PLATFORM_COURT_AUCTION_OUTPUT_DIR"]
written = os.environ["FAKE_WRITTEN"] == "1"
objects = [{"role": "item_search", "object_key": "bronze/x.gz", "local_path": "objects/x.gz", "written": True,
            "complete": False}] if written else []
json.dump({"schema_version": "foundation-platform.courtauction_go_kr_collect.v1", "objects": objects},
          open(os.path.join(out, "manifest.json"), "w", encoding="utf-8"), indent=1)
if os.environ["FAKE_REFUSED"] == "1":
    print("Error: courtauction.go.kr refused the run (HTTP 429)")
    sys.exit(1)
"""

FAKE_DOCKER = r"""#!/usr/bin/env python3
import os
open(os.path.join(os.environ["FAKE_STATE"], "docker.log"), "a").write("called\n")
"""


class CourtAuctionCommands(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="court-auction-")
        self.addCleanup(self.temp.cleanup)
        root = pathlib.Path(self.temp.name)
        base = root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("court-auction-collect.sh", "court-auction-silver-load.sh", "admitted-writer-runtime.sh",
                     "job-journal.sh", "database-url.sh"):
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
        bin_dir = root / "bin"
        bin_dir.mkdir()
        (bin_dir / "docker").write_text(FAKE_DOCKER)
        (bin_dir / "docker").chmod(0o755)
        self.collect = base / "current/scripts/ops/court-auction-collect.sh"
        self.load = base / "current/scripts/ops/court-auction-silver-load.sh"
        self.state = root / "state"
        self.fake = root / "fake"
        self.fake.mkdir()
        self.env = {
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}", "FOUNDATION_COURT_AUCTION_STATE_ROOT": str(self.state),
            "FAKE_STATE": str(self.fake), "FAKE_WRITTEN": "1", "FAKE_REFUSED": "0",
            **{name: "planted-" + name.lower() for name in NEEDS},
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT": "https://r2.invalid",
        }

    def run_script(self, script, env=None):
        return subprocess.run(["bash", str(script)], env=env or self.env, capture_output=True, text=True, timeout=120)

    def pending(self):
        pending = self.state / "lakehouse/pending"
        return sorted(pending.iterdir()) if pending.exists() else []

    def test_a_night_that_landed_objects_hands_them_off(self):
        result = self.run_script(self.collect)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome changed")
        self.assertEqual(len(self.pending()), 1)

    def test_a_refused_night_hands_off_what_it_landed_and_fails(self):
        self.env["FAKE_REFUSED"] = "1"
        result = self.run_script(self.collect)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertNotIn("foundation-job-outcome", result.stdout, "a stopped night never says it ended well")
        self.assertIn("STOPPED", (self.state / "journal.log").read_text(encoding="utf-8"))
        self.assertEqual(len(self.pending()), 1, "what was collected before the refusal is still loaded")

    def test_a_missing_contact_stops_the_night_before_any_request(self):
        self.assertIn("FOUNDATION_PLATFORM_COURT_AUCTION_CONTACT", REQUIRED)
        self.assertLessEqual(set(REQUIRED), set(NEEDS))
        for name in REQUIRED:
            with self.subTest(name):
                env = dict(self.env)
                del env[name]
                result = self.run_script(self.collect, env)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(name, result.stderr)
                self.assertFalse((self.fake / "calls.log").exists())
                self.assertFalse(self.state.exists(), "no state was written")

    def test_the_load_with_nothing_pending_ends_without_spark(self):
        result = self.run_script(self.load)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome unchanged")
        self.assertFalse((self.fake / "docker.log").exists())


if __name__ == "__main__":
    unittest.main()
