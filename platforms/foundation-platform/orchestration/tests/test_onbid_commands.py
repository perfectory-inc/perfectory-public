"""The Onbid public-sale jobs' real command lines (scripts/ops/onbid-collect.sh and onbid-silver-load.sh,
root ADR-0280).

The scripts run from an installed release layout (as in test_parcel_number_change_collect_command.py).
The publisher and docker are stand-ins: the publisher answers the collect command with an outcome and
the plan command with a plan of FAKE_OWED objects per table; docker records its arguments. What runs
for real is the scripts: the collect passes its outcome on and keeps no copy, a missing key stops the
run before anything reaches the provider, the load with nothing owed ends without starting Spark,
and a load hands Spark one plan directory per table and removes the copies whether it succeeds or not.
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
if sys.argv[1] == "collect-data-go-kr-onbid":
    assert os.environ["FOUNDATION_PLATFORM_DATA_GO_KR_ONBID_LIVE_WRITE"] == "1"
    out = os.environ["FOUNDATION_PLATFORM_DATA_GO_KR_ONBID_OUTPUT_DIR"]
    os.makedirs(os.path.join(out, "objects"))
    open(os.path.join(out, "objects", "item-list.gz"), "wb").write(b"x")
    print("foundation-job-outcome " + os.environ["FAKE_OUTCOME"])
    sys.exit(0)
assert sys.argv[1] == "plan-silver-load-objects", sys.argv
prefix = "FOUNDATION_PLATFORM_SILVER_LOAD_"
table, slug = os.environ[prefix + "TABLE"], os.environ[prefix + "SOURCE_SLUGS"]
assert {table: slug} in ({"silver.onbid_real_estate_item": "datagokr__onbid_real_estate_item"},
                         {"silver.onbid_item_bid_result": "datagokr__onbid_item_bid_result"}), (table, slug)
assert (os.environ[prefix + "WINDOW_DAYS"], os.environ[prefix + "MAX_OBJECTS"]) == ("7", "24")
out = os.environ[prefix + "OUTPUT_DIR"]
os.makedirs(out, exist_ok=True)
objects = []
for n in range(int(os.environ["FAKE_OWED"])):
    local = "objects/" + slug + "/o" + str(n) + ".gz"
    os.makedirs(os.path.dirname(os.path.join(out, local)), exist_ok=True)
    open(os.path.join(out, local), "wb").write(b"x")
    objects.append({"object_key": "bronze/source=" + slug + "/o" + str(n) + ".gz", "source_slug": slug,
                    "local_path": local, "size_bytes": 1})
late = [o["object_key"] for o in objects] if os.environ.get("FAKE_LATE") == "1" else []
json.dump({"schema_version": "foundation-platform.silver_load_objects.v1", "table": table, "objects": objects,
           "selection": {"owed": [o["object_key"] for o in objects], "late": late}},
          open(os.path.join(out, "plan.json"), "w", encoding="utf-8"))
"""

FAKE_DOCKER = r"""#!/usr/bin/env python3
import os, sys
open(os.path.join(os.environ["FAKE_STATE"], "docker.log"), "a").write(" ".join(sys.argv[1:]) + "\n")
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
        contracts = release / "infra/lakehouse/contracts"
        contracts.mkdir(parents=True)
        (contracts / "data-go-kr-onbid.contract.json").write_bytes(
            (PLATFORM / "infra/lakehouse/contracts/data-go-kr-onbid.contract.json").read_bytes())
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
            "FAKE_STATE": str(self.fake), "FAKE_OUTCOME": "changed", "FAKE_OWED": "1",
            **{name: "planted-" + name.lower() for name in NEEDS},
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT": "https://r2.invalid",
        }

    def run_script(self, script, env=None):
        return subprocess.run(["bash", str(script)], env=env or self.env, capture_output=True, text=True, timeout=120)

    def copies(self):
        return sorted(path for path in self.state.rglob("*.gz"))

    def test_the_collect_passes_its_outcome_on_and_keeps_no_copy(self):
        for outcome in ("changed", "unchanged"):
            self.env["FAKE_OUTCOME"] = outcome
            result = self.run_script(self.collect)
            self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
            self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome " + outcome)
        self.assertEqual(self.copies(), [], "the objects are in R2; the load fetches what it owes")
        self.assertFalse((self.state / "lakehouse").exists(), "nothing is handed to the load")

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

    def test_a_load_hands_spark_one_plan_per_table_and_removes_the_copies(self):
        self.env["FAKE_DOCKER_FAIL"] = "1"
        failed = self.run_script(self.load)
        self.assertNotEqual(failed.returncode, 0)
        self.assertEqual(self.copies(), [], "an unloaded object has no commit record, so the next plan fetches it again")
        self.env["FAKE_DOCKER_FAIL"] = "0"
        loaded = self.run_script(self.load)
        self.assertEqual(loaded.returncode, 0, loaded.stderr)
        self.assertEqual(loaded.stdout.splitlines()[-1], "foundation-job-outcome changed")
        self.assertEqual(self.copies(), [])
        call = (self.fake / "docker.log").read_text().splitlines()[-1].split()
        plans = [call[i + 1].rsplit("/", 1)[1] for i, arg in enumerate(call) if arg == "--plan-dir"]
        self.assertEqual(plans, ["items", "results"])

    def test_a_load_of_late_objects_loads_them_and_fails(self):
        # An object no load recorded for longer than the window means loads have been failing; it is
        # loaded, and the run fails so the gap raises an alert instead of a log line.
        self.env["FAKE_LATE"] = "1"
        result = self.run_script(self.load)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("spark-submit", (self.fake / "docker.log").read_text())
        self.assertIn("waited more than 7 days", result.stderr)
        self.assertNotIn("foundation-job-outcome", result.stdout)
        self.assertIn("LATE", (self.state / "journal.log").read_text(encoding="utf-8"))

    def test_the_load_with_nothing_owed_ends_without_spark(self):
        self.env["FAKE_OWED"] = "0"
        result = self.run_script(self.load)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome unchanged")
        self.assertFalse((self.fake / "docker.log").exists())
        self.assertEqual((self.fake / "calls.log").read_text().split(), ["plan-silver-load-objects"] * 2)


if __name__ == "__main__":
    unittest.main()
