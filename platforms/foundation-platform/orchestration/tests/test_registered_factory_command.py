"""The registered-factory job's real command line (scripts/ops/registered-factory-cycle.sh, root ADR-0280).

The script runs from an installed release layout (as in test_parcel_number_change_collect_command.py).
The publisher and docker are stand-ins: the publisher writes the manifest a run would write, with a
new edition or without one; docker records that it was called. What runs for real is the script: a
day the file is the same ends `unchanged` without starting Spark, a new edition is handed off and
loaded once, and a missing variable stops the run before anything reaches the portal.
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

UNIT = "foundation-registered-factory.service"
SCRIPT = PLATFORM / "scripts/ops/registered-factory-cycle.sh"
NEEDS = sorted(runtime_secrets.load().consumer(UNIT).needs)
REQUIRED = sorted(runtime_secrets.script_requirements(SCRIPT))
RELEASE_ID = "c" * 40

FAKE_PUBLISHER = r"""#!/usr/bin/env python3
import json, os, sys
state = os.environ["FAKE_STATE"]
with open(os.path.join(state, "calls.log"), "a", encoding="utf-8") as log:
    log.write(sys.argv[1] + "\n")
assert sys.argv[1] == "collect-data-go-kr-registered-factory", sys.argv
out = os.environ["FOUNDATION_PLATFORM_DATA_GO_KR_REGISTERED_FACTORY_OUTPUT_DIR"]
new = os.environ["FAKE_NEW"] == "1"
objects = [{"object_key": "bronze/source=datagokr__registered_factory/x.zip", "local_path": "objects/x.zip", "written": True}] if new else []
json.dump({"schema_version": "foundation-platform.data_go_kr_registered_factory_collect.v1", "objects": objects},
          open(os.path.join(out, "manifest.json"), "w", encoding="utf-8"), indent=1)
"""

# docker compose ... run spark spark-submit ... load --state-dir <container path>: move the pending
# handoffs to loaded/ as the load would, and print its summary line.
FAKE_DOCKER = r"""#!/usr/bin/env python3
import os, shutil, sys
state = os.environ["FAKE_STATE"]
open(os.path.join(state, "docker.log"), "a").write(" ".join(sys.argv[1:]) + "\n")
if "spark-submit" in sys.argv:
    root = os.environ["FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT"]
    os.makedirs(os.path.join(root, "loaded"), exist_ok=True)
    for name in os.listdir(os.path.join(root, "pending")):
        shutil.move(os.path.join(root, "pending", name), os.path.join(root, "loaded", name))
    print('registered-factory-summary-json {"status": "loaded"}')
"""


class RegisteredFactoryCommand(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="registered-factory-")
        self.addCleanup(self.temp.cleanup)
        root = pathlib.Path(self.temp.name)
        base = root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("registered-factory-cycle.sh", "admitted-writer-runtime.sh", "job-journal.sh", "database-url.sh"):
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
        self.script = base / "current/scripts/ops/registered-factory-cycle.sh"
        self.state = root / "state"
        self.fake = root / "fake"
        self.fake.mkdir()
        self.env = {
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}", "FOUNDATION_REGISTERED_FACTORY_STATE_ROOT": str(self.state),
            "FAKE_STATE": str(self.fake), "FAKE_NEW": "0",
            **{name: "planted-" + name.lower() for name in NEEDS},
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT": "https://r2.invalid",
        }

    def run_job(self, env=None):
        return subprocess.run(["bash", str(self.script)], env=env or self.env, capture_output=True, text=True, timeout=120)

    def test_the_same_file_ends_unchanged_without_spark_and_a_new_one_loads_once(self):
        same = self.run_job()
        self.assertEqual(same.returncode, 0, same.stderr + same.stdout)
        self.assertEqual(same.stdout.splitlines()[-1], "foundation-job-outcome unchanged")
        self.assertFalse((self.fake / "docker.log").exists(), "no Spark on a day the file is the same")
        self.env["FAKE_NEW"] = "1"
        new = self.run_job()
        self.assertEqual(new.returncode, 0, new.stderr + new.stdout)
        self.assertEqual(new.stdout.splitlines()[-1], "foundation-job-outcome changed")
        self.assertIn("spark-submit", (self.fake / "docker.log").read_text())
        self.assertEqual(len(list((self.state / "lakehouse/loaded").iterdir())), 1)
        self.assertEqual(list((self.state / "lakehouse/pending").iterdir()), [])

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
