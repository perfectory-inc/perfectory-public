"""The Onbid public-sale jobs' real command lines (scripts/ops/onbid-collect.sh and onbid-silver-load.sh,
root ADR-0280).

The scripts run from an installed release layout (as in test_parcel_number_change_collect_command.py).
The publisher and docker are stand-ins: the publisher writes the manifest a run would write, with or
without loadable objects; docker records that it was called. What runs for real is the scripts: a
run with item or result objects hands off once and says `changed`, a run with only announcements
hands off nothing, a missing key stops the run before anything reaches the provider, and the load
with nothing pending ends without starting Spark.
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

UNIT = "foundation-onbid-collect.service"
SCRIPT = PLATFORM / "scripts/ops/onbid-collect.sh"
NEEDS = sorted(runtime_secrets.load().consumer(UNIT).needs)
REQUIRED = sorted(runtime_secrets.script_requirements(SCRIPT))
RELEASE_ID = "f" * 40

FAKE_PUBLISHER = r"""#!/usr/bin/env python3
import json, os, sys
state = os.environ["FAKE_STATE"]
with open(os.path.join(state, "calls.log"), "a", encoding="utf-8") as log:
    log.write(sys.argv[1] + "\n")
assert sys.argv[1] == "collect-data-go-kr-onbid", sys.argv
assert os.environ["FOUNDATION_PLATFORM_DATA_GO_KR_ONBID_LIVE_WRITE"] == "1"
roles = os.environ["FAKE_ROLES"].split(",")
out = os.environ["FOUNDATION_PLATFORM_DATA_GO_KR_ONBID_OUTPUT_DIR"]
json.dump({"schema_version": "foundation-platform.data_go_kr_onbid_collect.v1", "source_snapshot_id": "20990101T000000Z",
           "report": {"item_pass_complete": True},
           "objects": [{"role": role, "written": True, "local_path": "objects/" + role + ".gz"} for role in roles if role]},
          open(os.path.join(out, "manifest.json"), "w", encoding="utf-8"))
print("foundation-job-outcome changed")
"""

FAKE_DOCKER = r"""#!/usr/bin/env python3
import os, sys
open(os.path.join(os.environ["FAKE_STATE"], "docker.log"), "a").write("called\n")
if os.environ.get("FAKE_DOCKER_FAIL") == "1":
    sys.exit(1)
print('data-go-kr-onbid-summary-json {"status": "loaded"}')
"""


class OnbidCommands(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="onbid-")
        self.addCleanup(self.temp.cleanup)
        root = pathlib.Path(self.temp.name)
        base = root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("onbid-collect.sh", "onbid-silver-load.sh", "admitted-writer-runtime.sh", "job-journal.sh",
                     "database-url.sh"):
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
        self.collect = base / "current/scripts/ops/onbid-collect.sh"
        self.load = base / "current/scripts/ops/onbid-silver-load.sh"
        self.state = root / "state"
        self.fake = root / "fake"
        self.fake.mkdir()
        self.env = {
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}", "FOUNDATION_ONBID_STATE_ROOT": str(self.state),
            "FAKE_STATE": str(self.fake), "FAKE_ROLES": "announcement_list,item_list",
            **{name: "planted-" + name.lower() for name in NEEDS},
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT": "https://r2.invalid",
        }

    def run_script(self, script, env=None):
        return subprocess.run(["bash", str(script)], env=env or self.env, capture_output=True, text=True, timeout=120)

    def pending(self):
        pending = self.state / "lakehouse/pending"
        return sorted(pending.iterdir()) if pending.exists() else []

    def test_items_hand_off_once_and_announcements_alone_hand_off_nothing(self):
        first = self.run_script(self.collect)
        self.assertEqual(first.returncode, 0, first.stderr + first.stdout)
        [handoff] = self.pending()
        self.assertTrue((handoff / "manifest.json").exists())
        self.assertEqual(first.stdout.splitlines()[-1], "foundation-job-outcome changed")
        self.env["FAKE_ROLES"] = "announcement_list"
        second = self.run_script(self.collect)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(second.stdout.splitlines()[-1], "foundation-job-outcome unchanged")
        self.assertEqual(len(self.pending()), 1)

    def test_a_missing_key_stops_the_run_before_anything_reaches_the_provider(self):
        self.assertIn("DATA_GO_KR_SERVICE_KEY", REQUIRED)
        self.assertLessEqual(set(REQUIRED), set(NEEDS))
        env = dict(self.env)
        del env["DATA_GO_KR_SERVICE_KEY"]
        self.assertEqual(self.run_script(self.collect, env).returncode, 78, "EX_CONFIG: the owner has not placed the key")
        for name in REQUIRED:
            with self.subTest(name):
                env = dict(self.env)
                del env[name]
                result = self.run_script(self.collect, env)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(name, result.stderr)
                self.assertFalse((self.fake / "calls.log").exists())
                self.assertFalse(self.state.exists(), "no state was written")

    def test_the_host_moves_a_loaded_handoff_and_keeps_a_failed_one(self):
        # The Spark container runs as another user and only reads the handoffs (2026-10-19: it could
        # not remove one the host made); the script that owns them moves them after the load.
        self.run_script(self.collect)
        [handoff] = self.pending()
        self.env["FAKE_DOCKER_FAIL"] = "1"
        failed = self.run_script(self.load)
        self.assertNotEqual(failed.returncode, 0)
        self.assertEqual(self.pending(), [handoff], "a failed load leaves the handoff for the next run")
        self.env["FAKE_DOCKER_FAIL"] = "0"
        loaded = self.run_script(self.load)
        self.assertEqual(loaded.returncode, 0, loaded.stderr)
        self.assertEqual(self.pending(), [])
        self.assertTrue((self.state / "lakehouse/loaded" / handoff.name / "manifest.json").exists())
        self.assertEqual(loaded.stdout.splitlines()[-1], "foundation-job-outcome changed")

    def test_the_load_with_nothing_pending_ends_without_spark(self):
        result = self.run_script(self.load)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome unchanged")
        self.assertFalse((self.fake / "docker.log").exists())


if __name__ == "__main__":
    unittest.main()
