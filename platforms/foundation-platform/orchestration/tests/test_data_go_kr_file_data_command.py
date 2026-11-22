"""The data.go.kr file-data job's real command line (scripts/ops/data-go-kr-file-data-cycle.sh, root ADR-0401).

The script runs from an installed release layout (as in test_registered_factory_command.py). The publisher is a
stand-in: it answers the collect command with a manifest, each table's plan command with a plan of FAKE_OWED, and
the load command (load-data-go-kr-file-data-silver, DuckDB inside the publisher, root ADR-0559) with a summary line,
recording the plans it was given. What runs for real is the script: it names every file of the contract but the
registered factory (its own job collects it), plans every table, starts the load every run (the code-change
follow-up needs it even when nothing is owed), removes the copies whether the run succeeds or not, and stops before
the portal when a variable is missing. No Spark and no docker.
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
    failed = ["school_location: SERVICE_KEY_IS_NOT_REGISTERED_ERROR"] if os.environ.get("FAKE_FAIL") == "1" else []
    json.dump({"files": [{"dataset_slug": s, "size_bytes": 1} for s in landed], "failed": failed},
              open(os.path.join(out, "manifest.json"), "w", encoding="utf-8"))
    if os.environ.get("FAKE_FAIL") == "1":
        sys.exit(1)
    print("foundation-job-outcome " + ("changed" if landed else "unchanged"))
    sys.exit(0)
if sys.argv[1] == "load-data-go-kr-file-data-silver":
    root = os.environ[prefix + "PLAN_ROOT"]
    plans = sorted(name for name in os.listdir(root) if os.path.isfile(os.path.join(root, name, "plan.json")))
    with open(os.path.join(os.environ["FAKE_STATE"], "load.log"), "a", encoding="utf-8") as log:
        log.write(" ".join(plans) + "\n")
    if os.environ.get("FAKE_LOAD_FAIL") == "1":
        sys.exit(1)
    appended = int(os.environ.get("FAKE_APPENDED", "0"))
    print("data-go-kr-file-data-summary-json " + json.dumps({"editions": [], "follows": [{"appended": appended}]}))
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
"""


class DataGoKrFileDataCommand(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="data-go-kr-file-data-")
        self.addCleanup(self.temp.cleanup)
        root = pathlib.Path(self.temp.name)
        base = root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("data-go-kr-file-data-cycle.sh", "data-go-kr-file-collect.sh", "admitted-writer-runtime.sh",
                     "job-journal.sh", "database-url.sh"):
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

    def test_every_file_is_asked_every_table_planned_and_the_load_follows_code_changes_even_when_nothing_is_owed(self):
        files = json.loads(CONTRACT.read_text(encoding="utf-8"))["files"]
        same = self.run_job()
        self.assertEqual(same.returncode, 0, same.stderr + same.stdout)
        self.assertEqual(same.stdout.splitlines()[-1], "foundation-job-outcome unchanged")
        calls = (self.fake / "calls.log").read_text().splitlines()
        command, datasets = calls[0].split(" ")
        expected = [slug for slug, entry in files.items() if "own_job" not in entry]
        self.assertNotIn("registered_factory", expected)
        self.assertNotIn("pension_workplace", expected, "its own job collects it (root ADR-0604)")
        self.assertEqual((command, datasets.split(",")), ("collect-data-go-kr-file-data", expected))
        self.assertEqual([c.split(" ")[1] for c in calls[1:-1]], ["silver." + slug for slug in expected])
        self.assertEqual(calls[-1].split(" ")[0], "load-data-go-kr-file-data-silver")
        plans = (self.fake / "load.log").read_text(encoding="utf-8").splitlines()[-1].split()
        self.assertEqual(plans, sorted(expected), "one load over every table's plan, for the code-change follow-up too")
        self.assertFalse((self.fake / "docker.log").exists(), "the load starts no container")
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
        for env in ({"FAKE_LANDED": "airport", "FAKE_FAIL": "1"}, {"FAKE_OWED": "silver.port", "FAKE_LOAD_FAIL": "1"}):
            with self.subTest(env):
                failed = self.run_job({**self.env, **env})
                self.assertNotEqual(failed.returncode, 0)
                self.assertNotIn("foundation-job-outcome", failed.stdout)
                self.assertIn("FAILED", (self.state / "journal.log").read_text(encoding="utf-8"))
                self.assertEqual(sorted(self.state.rglob("*.csv")), [])

    def test_one_files_failed_collection_still_loads_what_landed_then_fails(self):
        failed = self.run_job({**self.env, "FAKE_LANDED": "airport", "FAKE_FAIL": "1"})
        self.assertNotEqual(failed.returncode, 0)
        self.assertTrue((self.fake / "load.log").exists(), "the files that landed are loaded")
        journal = (self.state / "journal.log").read_text(encoding="utf-8")
        self.assertIn("COLLECT FAILED", journal)
        self.assertIn("school_location: SERVICE_KEY_IS_NOT_REGISTERED_ERROR", journal)

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


class DataGoKrFileCollectCommand(unittest.TestCase):
    """scripts/ops/data-go-kr-file-collect.sh: one file that names its own job, collected only (root ADR-0604)."""

    def setUp(self):
        DataGoKrFileDataCommand.setUp(self)
        self.script = self.script.with_name("data-go-kr-file-collect.sh")
        self.env["FOUNDATION_DATA_GO_KR_FILE_COLLECT_STATE_ROOT"] = str(self.state)

    def collect(self, dataset, env=None):
        return subprocess.run(["bash", str(self.script), dataset], env={**self.env, **(env or {})},
                              capture_output=True, text=True, timeout=120)

    def test_it_collects_only_the_file_it_is_named_and_says_what_landed(self):
        same = self.collect("pension-workplace")  # the unit's instance: a slug with hyphens
        self.assertEqual(same.returncode, 0, same.stderr + same.stdout)
        self.assertEqual(same.stdout.splitlines()[-1], "foundation-job-outcome unchanged")
        self.assertEqual((self.fake / "calls.log").read_text().splitlines(),
                         ["collect-data-go-kr-file-data pension_workplace"], "no plan and no load: Bronze only")
        landed = self.collect("pension-workplace", {"FAKE_LANDED": "pension_workplace"})
        self.assertEqual(landed.stdout.splitlines()[-1], "foundation-job-outcome changed")
        self.assertIn("landed=1 past=0 bytes=1", (self.state / "journal.log").read_text(encoding="utf-8"))
        self.assertEqual(sorted(self.state.rglob("*.csv")), [])

    def test_a_file_of_the_daily_cycle_or_one_whose_job_loads_it_is_refused(self):
        for dataset in ("airport", "registered_factory", "", "no_such_file"):
            with self.subTest(dataset):
                refused = self.collect(dataset)
                self.assertEqual(refused.returncode, 64, refused.stderr)
                self.assertFalse((self.fake / "calls.log").exists())

    def test_every_job_of_this_script_names_a_file_whose_own_job_it_is(self):
        files = json.loads(CONTRACT.read_text(encoding="utf-8"))["files"]
        jobs = {spec.job_id: spec.systemd_service for spec in job_specs.load_specs()
                if spec.systemd_service.startswith("foundation-data-go-kr-file-collect@")}
        self.assertEqual(sorted(jobs), ["employment_insurance_workplace_collect", "pension_workplace_collect"])
        for job, unit in jobs.items():
            dataset = unit[len("foundation-data-go-kr-file-collect@"):-len(".service")].replace("-", "_")
            self.assertEqual(files[dataset]["own_job"], job)
            self.assertNotIn("silver_load", files[dataset])


if __name__ == "__main__":
    unittest.main()
