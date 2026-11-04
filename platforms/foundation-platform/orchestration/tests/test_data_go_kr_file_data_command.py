"""The data.go.kr file-data job's real command line (scripts/ops/data-go-kr-file-data-cycle.sh, root ADR-0401).

The script runs from an installed release layout (as in test_registered_factory_command.py). The publisher and docker
are stand-ins: the publisher answers the collect command with a manifest and each table's plan command with a plan of
FAKE_OWED; docker records its arguments. What runs for real is the script: it names every file of the contract but the
registered factory (its own job collects it), plans every table, starts Spark every run (the code-change follow-up
needs it even when nothing is owed), removes the copies whether the run succeeds or not, and stops before the portal
when a variable is missing.
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
    log.write(sys.argv[1] + " " + os.environ.get(prefix + "DATASETS", os.environ.get("FOUNDATION_PLATFORM_SILVER_LOAD_TABLE", "")) + "\n")
if sys.argv[1] == "collect-data-go-kr-file-data":
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
    sys.exit(0)
assert sys.argv[1] == "plan-silver-load-objects", sys.argv
p = "FOUNDATION_PLATFORM_SILVER_LOAD_"
table = os.environ[p + "TABLE"]
assert os.environ[p + "SOURCE_SLUGS"] == "datagokr__" + table[len("silver."):]
assert (os.environ[p + "WINDOW_DAYS"], os.environ[p + "MAX_OBJECTS"]) == ("7", "1")
out = os.environ[p + "OUTPUT_DIR"]
os.makedirs(os.path.join(out, "objects"), exist_ok=True)
objects = []
if table in os.environ.get("FAKE_OWED", "").split(","):
    open(os.path.join(out, "objects/x.csv"), "wb").write(b"x")
    objects.append({"object_key": "bronze/source=x/x.csv", "source_slug": os.environ[p + "SOURCE_SLUGS"],
                    "local_path": "objects/x.csv", "size_bytes": 1})
json.dump({"schema_version": "foundation-platform.silver_load_objects.v1", "objects": objects, "selection": {"late": []}},
          open(os.path.join(out, "plan.json"), "w", encoding="utf-8"))
"""

FAKE_DOCKER = r"""#!/usr/bin/env python3
import json, os, sys
open(os.path.join(os.environ["FAKE_STATE"], "docker.log"), "a").write(" ".join(sys.argv[1:]) + "\n")
if os.environ.get("FAKE_DOCKER_FAIL") == "1":
    sys.exit(1)
if "spark-submit" in sys.argv:
    appended = int(os.environ.get("FAKE_APPENDED", "0"))
    print("data-go-kr-file-data-summary-json " + json.dumps({"editions": [], "follows": [{"appended": appended}]}))
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
        bin_dir = root / "bin"
        bin_dir.mkdir()
        (bin_dir / "docker").write_text(FAKE_DOCKER)
        (bin_dir / "docker").chmod(0o755)
        (release / "infra/lakehouse/spark/jobs").mkdir(parents=True)
        for name in ("data_go_kr_file_data.py", "address_pnu.py"):
            (release / "infra/lakehouse/spark/jobs" / name).write_bytes((PLATFORM / "infra/lakehouse/spark/jobs" / name).read_bytes())
        self.script = base / "current/scripts/ops/data-go-kr-file-data-cycle.sh"
        self.state = root / "state"
        self.fake = root / "fake"
        self.fake.mkdir()
        self.env = {
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}", "FOUNDATION_DATA_GO_KR_FILE_DATA_STATE_ROOT": str(self.state),
            "FAKE_STATE": str(self.fake),
            **{name: "planted-" + name.lower() for name in NEEDS},
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT": "https://r2.invalid",
        }

    def run_job(self, env=None):
        return subprocess.run(["bash", str(self.script)], env=env or self.env, capture_output=True, text=True, timeout=120)

    def test_every_file_is_asked_every_table_planned_and_spark_follows_code_changes_even_when_nothing_is_owed(self):
        files = json.loads(CONTRACT.read_text(encoding="utf-8"))["files"]
        same = self.run_job()
        self.assertEqual(same.returncode, 0, same.stderr + same.stdout)
        self.assertEqual(same.stdout.splitlines()[-1], "foundation-job-outcome unchanged")
        calls = (self.fake / "calls.log").read_text().splitlines()
        command, datasets = calls[0].split(" ")
        expected = [slug for slug in files if slug != "registered_factory"]
        self.assertEqual((command, datasets.split(",")), ("collect-data-go-kr-file-data", expected))
        self.assertEqual([c.split(" ")[1] for c in calls[1:]], ["silver." + slug for slug in expected])
        call = (self.fake / "docker.log").read_text().splitlines()[-1].split()
        self.assertEqual(call.count("--plan-dir"), 8, "one Spark run for every table, for the code-change follow-up too")
        self.assertEqual(sorted(self.state.rglob("*.csv")), [])

    def test_a_landed_owed_or_followed_run_is_a_change(self):
        for env in ({"FAKE_LANDED": "airport"}, {"FAKE_OWED": "silver.port"}, {"FAKE_APPENDED": "3"}):
            with self.subTest(env):
                result = self.run_job({**self.env, **env})
                self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
                self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome changed")
        self.assertIn("landed=[airport]", (self.state / "journal.log").read_text(encoding="utf-8"))
        self.assertEqual(sorted(self.state.rglob("*.csv")), [])

    def test_a_failed_collection_or_load_fails_the_run_and_leaves_no_copy(self):
        for env in ({"FAKE_LANDED": "airport", "FAKE_FAIL": "1"}, {"FAKE_OWED": "silver.port", "FAKE_DOCKER_FAIL": "1"}):
            with self.subTest(env):
                failed = self.run_job({**self.env, **env})
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
