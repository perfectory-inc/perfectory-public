"""The 주소DB job's real command line (scripts/ops/juso-address-db-cycle.sh, root ADR-0495).

The script runs from an installed release layout (as in test_data_go_kr_file_data_command.py). The publisher is a
stand-in that answers the collect command with a manifest and the plan command with the objects FAKE_OWED names;
docker records its arguments. What runs for real is the script: the collector is run live; the plan covers both
sources of silver.juso_road_address and Spark runs only when something is owed; a failed collection still loads
what landed and fails the run with the collector's summary in the journal; and a missing variable stops the run
before anything reaches the provider.
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
CONTRACT = PLATFORM / "infra/lakehouse/contracts/juso-address-db.contract.json"
NEEDS = sorted(runtime_secrets.load().consumer(UNIT).needs)
REQUIRED = sorted(runtime_secrets.script_requirements(SCRIPT))
RELEASE_ID = "c" * 40

FAKE_PUBLISHER = r"""#!/usr/bin/env python3
import json, os, sys
prefix = "FOUNDATION_PLATFORM_JUSO_ADDRESS_DB_"
open(os.path.join(os.environ["FAKE_STATE"], "calls.log"), "a", encoding="utf-8").write(sys.argv[1] + "\n")
if sys.argv[1] == "plan-silver-load-objects":
    p = "FOUNDATION_PLATFORM_SILVER_LOAD_"
    assert os.environ[p + "TABLE"] == "silver.juso_road_address"
    assert os.environ[p + "SOURCE_SLUGS"] == "jusogokr__address_db_monthly_full,jusogokr__address_db_daily_change"
    assert (os.environ[p + "WINDOW_DAYS"], os.environ[p + "MAX_OBJECTS"]) == ("2", "70")
    out = os.environ[p + "OUTPUT_DIR"]
    os.makedirs(out, exist_ok=True)
    owed = os.environ.get("FAKE_OWED") == "1"
    objects = [{"object_key": "bronze/source=x/20990101_dailymatchingdata.zip",
                "source_slug": "jusogokr__address_db_daily_change", "local_path": "objects/x.zip"}] if owed else []
    json.dump({"schema_version": "foundation-platform.silver_load_objects.v1", "objects": objects,
               "selection": {"late": []}}, open(os.path.join(out, "plan.json"), "w", encoding="utf-8"))
    sys.exit(0)
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

FAKE_DOCKER = r"""#!/usr/bin/env python3
import json, os, sys
open(os.path.join(os.environ["FAKE_STATE"], "docker.log"), "a").write(" ".join(sys.argv[1:]) + "\n")
if "spark-submit" in sys.argv:
    print("juso-address-db-summary-json " + json.dumps({"objects": [{"appended": 1}]}))
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
        self.script = base / "current/scripts/ops/juso-address-db-cycle.sh"
        self.state = root / "state"
        self.fake = root / "fake"
        self.fake.mkdir()
        self.env = {
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}", "FOUNDATION_JUSO_ADDRESS_DB_STATE_ROOT": str(self.state),
            "FAKE_STATE": str(self.fake),
            **{name: "planted-" + name.lower() for name in NEEDS},
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT": "https://r2.invalid",
        }

    def run_job(self, env=None):
        return subprocess.run(["bash", str(self.script)], env=env or self.env, capture_output=True, text=True, timeout=120)

    def docker_calls(self):
        log = self.fake / "docker.log"
        return log.read_text().splitlines() if log.exists() else []

    def test_the_collector_runs_live_and_spark_runs_only_when_something_is_owed(self):
        for landed, outcome in (("0", "unchanged"), ("1", "changed")):
            with self.subTest(landed=landed):
                result = self.run_job({**self.env, "FAKE_LANDED": landed, "FAKE_OWED": landed})
                self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
                self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome " + outcome)
        self.assertEqual((self.fake / "calls.log").read_text().splitlines(),
                         ["collect-juso-address-db", "plan-silver-load-objects"] * 2)
        spark = [line for line in self.docker_calls() if "spark-submit" in line]
        self.assertEqual(len(spark), 1, "Spark only for the run that owed")
        self.assertIn("juso_address_db.py load", spark[0])

    def test_a_failed_collection_loads_what_landed_and_fails_the_run(self):
        result = self.run_job({**self.env, "FAKE_FAILED": "1", "FAKE_OWED": "1"})
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("foundation-job-outcome", result.stdout)
        journal = (self.state / "journal.log").read_text(encoding="utf-8")
        self.assertIn("COLLECT FAILED", journal)
        self.assertIn("20990102", journal)
        self.assertTrue(any("spark-submit" in line for line in self.docker_calls()), "what landed is loaded")

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
