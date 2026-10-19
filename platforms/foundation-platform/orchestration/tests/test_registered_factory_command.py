"""The registered-factory job's real command line (scripts/ops/registered-factory-cycle.sh, root ADR-0280).

The script runs from an installed release layout (as in test_parcel_number_change_collect_command.py).
The publisher and docker are stand-ins: the publisher answers the collect command and the plan command
with a plan of FAKE_OWED zips; docker records its arguments. What runs for real is the script: a day
nothing is owed ends `unchanged` without starting Spark, an owed edition is loaded from the plan
directory and the copies are removed whether the load succeeds or not, and a missing variable stops
the run before anything reaches the portal.
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
if sys.argv[1] == "collect-data-go-kr-registered-factory":
    out = os.environ["FOUNDATION_PLATFORM_DATA_GO_KR_REGISTERED_FACTORY_OUTPUT_DIR"]
    json.dump({"objects": []}, open(os.path.join(out, "manifest.json"), "w", encoding="utf-8"))
    sys.exit(0)
assert sys.argv[1] == "plan-silver-load-objects", sys.argv
prefix = "FOUNDATION_PLATFORM_SILVER_LOAD_"
assert os.environ[prefix + "TABLE"] == "silver.registered_factory"
assert os.environ[prefix + "SOURCE_SLUGS"] == "datagokr__registered_factory"
assert (os.environ[prefix + "WINDOW_DAYS"], os.environ[prefix + "MAX_OBJECTS"]) == ("7", "1")
out = os.environ[prefix + "OUTPUT_DIR"]
os.makedirs(os.path.join(out, "objects/datagokr__registered_factory"), exist_ok=True)
objects = []
for n in range(int(os.environ["FAKE_OWED"])):
    open(os.path.join(out, "objects/datagokr__registered_factory/x.zip"), "wb").write(b"x")
    objects.append({"object_key": "bronze/source=datagokr__registered_factory/x.zip",
                    "source_slug": "datagokr__registered_factory",
                    "local_path": "objects/datagokr__registered_factory/x.zip", "size_bytes": 1})
json.dump({"schema_version": "foundation-platform.silver_load_objects.v1", "objects": objects},
          open(os.path.join(out, "plan.json"), "w", encoding="utf-8"))
"""

FAKE_DOCKER = r"""#!/usr/bin/env python3
import os, sys
open(os.path.join(os.environ["FAKE_STATE"], "docker.log"), "a").write(" ".join(sys.argv[1:]) + "\n")
if os.environ.get("FAKE_DOCKER_FAIL") == "1":
    sys.exit(1)
if "spark-submit" in sys.argv:
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
        contracts = release / "infra/lakehouse/contracts"
        contracts.mkdir(parents=True)
        (contracts / "data-go-kr-registered-factory.contract.json").write_bytes(
            (PLATFORM / "infra/lakehouse/contracts/data-go-kr-registered-factory.contract.json").read_bytes())
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
            "FAKE_STATE": str(self.fake), "FAKE_OWED": "0",
            **{name: "planted-" + name.lower() for name in NEEDS},
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT": "https://r2.invalid",
        }

    def run_job(self, env=None):
        return subprocess.run(["bash", str(self.script)], env=env or self.env, capture_output=True, text=True, timeout=120)

    def copies(self):
        return sorted(self.state.rglob("*.zip"))

    def test_nothing_owed_ends_unchanged_without_spark_and_an_owed_edition_loads_from_its_plan(self):
        same = self.run_job()
        self.assertEqual(same.returncode, 0, same.stderr + same.stdout)
        self.assertEqual(same.stdout.splitlines()[-1], "foundation-job-outcome unchanged")
        self.assertFalse((self.fake / "docker.log").exists(), "no Spark when the table records every zip")
        self.env["FAKE_OWED"] = "1"
        new = self.run_job()
        self.assertEqual(new.returncode, 0, new.stderr + new.stdout)
        self.assertEqual(new.stdout.splitlines()[-1], "foundation-job-outcome changed")
        call = (self.fake / "docker.log").read_text().splitlines()[-1].split()
        self.assertEqual(call[call.index("--plan-dir") + 1].rsplit("/", 1)[1], "plan")
        self.assertEqual(self.copies(), [])

    def test_a_failed_load_removes_its_copy_and_leaves_nothing_to_hand_on(self):
        # The zip has no commit record, so the next run's plan fetches it again from R2.
        self.env.update(FAKE_OWED="1", FAKE_DOCKER_FAIL="1")
        failed = self.run_job()
        self.assertNotEqual(failed.returncode, 0)
        self.assertEqual(self.copies(), [])
        self.assertFalse((self.state / "lakehouse/pending").exists())

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
