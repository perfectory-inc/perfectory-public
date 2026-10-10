"""The staging smoke (scripts/ops/staging-smoke.sh, root ADR-0177) against stand-ins.

The script runs from an installed release that is not `current` (the deploy runs it before the
switch). The publisher and docker are stand-ins: the publisher answers each command the way a
scenario file says and records the environment it was given, docker records what it was asked.
What runs for real is the script, the step bounds and sample rules of config/staging-gate.contract.json,
database-url.sh and the daily sweep's VWorld lane settings: every publisher call runs as staging
against foundation_staging, the sample is the two smallest files plus one large enough to go
multipart, and a step that lands or measures less than it should fails the smoke.
"""

import hashlib
import json
import os
import pathlib
import shutil
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

RUN = "staging-smoke"
CONTRACT = PLATFORM / "config/staging-gate.contract.json"
NAMING = PLATFORM / "config/environment-variable-naming.contract.json"
CATALOG = PLATFORM / "docs/catalog/public-source-endpoint-catalog.v1.json"
NEEDS = sorted(runtime_secrets.load().consumer(RUN).needs)
VWORLD_LOGIN = json.loads(NAMING.read_text(encoding="utf-8"))[
    "compatibility_migrations"]["foundation-vworld-credentials"]["credentials"]
SAMPLE = json.loads(CONTRACT.read_text(encoding="utf-8"))["sample"]
RELEASE_ID = "e" * 40
CURRENT_ID = "c" * 40
MIB = 1024 * 1024

FAKE_PUBLISHER = r"""#!/usr/bin/env python3
import json, os, sys
state = os.environ["FAKE_STATE"]
command = sys.argv[1]
scenario = json.load(open(os.path.join(state, "scenario.json"), encoding="utf-8"))
env = os.environ
seen = {"command": command, "runtime_env": env.get("FOUNDATION_PLATFORM_RUNTIME_ENV"),
        "database": env.get("DATABASE_URL", "").rsplit("/", 1)[-1],
        "threshold": env.get("FOUNDATION_PLATFORM_R2_STAGING_MULTIPART_THRESHOLD_BYTES"),
        "writer": bool(env.get("FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID")),
        "inventory": env.get("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INVENTORY_PATH"),
        "bronze_key": env.get("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_BRONZE_KEY"),
        "sources": env.get("FOUNDATION_PLATFORM_BRONZE_MEMBER_SOURCES")}
with open(os.path.join(state, "calls.jsonl"), "a", encoding="utf-8") as log:
    log.write(json.dumps(seen) + "\n")
if command == scenario.get("die"):
    sys.exit(command + " died")
if command == "clear-staging-namespace":
    print("staging-namespace cleared prefix=staging/ objects=0 bytes=0")
elif command == "plan-building-hub-bulk-collection":
    json.dump({"jobs": scenario.get("hub_jobs", [{"endpoint_slug": "hub"}])},
              open(env["FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_PLAN_PATH"], "w"))
elif command == "plan-vworld-dataset-collection":
    json.dump({"status": "ready", "job_count": 1, "jobs": [{"endpoint_slug": "v"}]},
              open(env["FOUNDATION_PLATFORM_VWORLD_DATASET_COLLECTION_PLAN_PATH"], "w"))
elif command == "inventory-vworld-dataset-files":
    json.dump({"jobs": scenario["inventory"]}, open(env["FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INVENTORY_PATH"], "w"))
elif command == "ingest-vworld-dataset-files":
    sample = json.load(open(env["FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INVENTORY_PATH"], encoding="utf-8"))
    files = [f for job in sample["jobs"] for f in job["files"]]
    json.dump({"sampled": [f["file_no"] for f in files]}, open(os.path.join(state, "sample.json"), "w"))
    failed = scenario.get("ingest_failed", 0)
    json.dump({"status": "blocked" if failed else "ready", "selected_file_count": len(files),
               "succeeded_file_count": len(files) - failed, "failed_file_count": failed},
              open(env["FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INGEST_EVIDENCE_PATH"], "w"))
elif command == "measure-bronze-object-members":
    landed = len(json.load(open(os.path.join(state, "sample.json")))["sampled"])
    measured = scenario.get("measured", landed)
    print("bronze-object-members-json " + json.dumps({"measured": measured, "failed": 0}))
else:
    sys.exit("unexpected command " + command)
"""

# docker: `ps` names one postgres, `exec ... psql` records its database, role and statements and
# swallows the SQL on stdin, `build` and `run` record themselves.
FAKE_DOCKER = r"""#!/usr/bin/env python3
import json, os, sys
state = os.environ["FAKE_STATE"]
args = sys.argv[1:]
record = {"verb": args[0]}
if args[0] == "ps":
    print("pgcontainer")
elif args[0] == "exec":
    record["database"] = args[args.index("-d") + 1]
    record["role"] = args[args.index("-U") + 1]
    record["statements"] = [args[i + 1] for i, a in enumerate(args) if a == "-c"]
    if "-f" in args:
        record["sql"] = sys.stdin.read().splitlines()[0]
elif args[0] == "run":
    record["migrator_database"] = os.environ["FOUNDATION_MIGRATOR_DATABASE_URL"].rsplit("/", 1)[-1]
    record["migrator_role"] = os.environ["FOUNDATION_MIGRATOR_DATABASE_URL"].split("//", 1)[1].split(":", 1)[0]
    record["image"] = args[-1]
elif args[0] == "build":
    record["tag"] = args[args.index("--tag") + 1]
with open(os.path.join(state, "docker.jsonl"), "a", encoding="utf-8") as log:
    log.write(json.dumps(record) + "\n")
"""


def vfile(file_no, mib, kind="single_resource_file"):
    return {"download_ds_id": "9991", "file_no": file_no, "size_kib": mib * 1024, "download_kind": kind}


INVENTORY = [{"source_slug": "vworldkr__synthetic", "endpoint_slug": "v", "files": [
    vfile("tiny", 1), vfile("small", 2), vfile("middle", 8),
    vfile("large", SAMPLE["multipart_threshold_bytes"] // MIB + 4),
    vfile("larger", SAMPLE["multipart_threshold_bytes"] // MIB + 40),
    vfile("archive", 0, kind="selection_archive"),
]}]


class StagingSmoke(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="staging-smoke-")
        self.addCleanup(temp.cleanup)
        root = pathlib.Path(temp.name)
        base = root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("staging-smoke.sh", "admitted-writer-runtime.sh", "database-url.sh", "vworld-login.sh",
                     "vworld-sweep-lane.sh", "job-journal.sh"):
            (release / "scripts/ops" / name).write_bytes((PLATFORM / "scripts/ops" / name).read_bytes())
            (release / "scripts/ops" / name).chmod(0o755)
        (release / "config").mkdir()
        shutil.copy(CONTRACT, release / "config")
        shutil.copy(NAMING, release / "config")
        (release / "docs/catalog").mkdir(parents=True)
        shutil.copy(CATALOG, release / "docs/catalog")
        (release / "infra/compose").mkdir(parents=True)
        for sql in ("bootstrap-foundation.sql", "grant-foundation-runtime.sql", "finalize-foundation.sql"):
            shutil.copy(PLATFORM / "infra/compose" / sql, release / "infra/compose")
        # Another release is current: the smoke runs the installed one before the switch.
        (base / "releases" / CURRENT_ID).mkdir()
        (base / "current").symlink_to(pathlib.Path("releases") / CURRENT_ID)
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
        self.script = release / "scripts/ops/staging-smoke.sh"
        self.state = root / "state"
        self.fake = root / "fake"
        self.fake.mkdir()
        self.env = {
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}", "FAKE_STATE": str(self.fake),
            "FOUNDATION_STAGING_SMOKE_STATE_ROOT": str(self.state),
            **{name: "planted-" + name.lower() for name in NEEDS},
            # What source-sweep.env carries in production; the smoke must override it.
            "FOUNDATION_PLATFORM_RUNTIME_ENV": "production",
            VWORLD_LOGIN["username"]["canonical"]: "planted-user", VWORLD_LOGIN["password"]["canonical"]: "planted-pass",
        }
        self.scenario()

    def scenario(self, **fields):
        (self.fake / "scenario.json").write_text(json.dumps({"inventory": INVENTORY, **fields}), encoding="utf-8")

    def run_smoke(self, env=None):
        return subprocess.run(["bash", str(self.script)], env=env or self.env, capture_output=True, text=True,
                              timeout=120)

    def lines(self, name):
        path = self.fake / name
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()] if path.exists() else []

    def test_a_good_release_passes_every_step_as_staging(self):
        result = self.run_smoke()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(f"staging-smoke: passed release={RELEASE_ID}", result.stdout)
        calls = self.lines("calls.jsonl")
        self.assertEqual([call["command"] for call in calls], [
            "clear-staging-namespace", "plan-building-hub-bulk-collection", "plan-vworld-dataset-collection",
            "inventory-vworld-dataset-files", "ingest-vworld-dataset-files", "measure-bronze-object-members"])
        for call in calls:
            with self.subTest(call["command"]):
                self.assertEqual(call["runtime_env"], "staging", "every publisher call runs in the staging namespace")
                self.assertEqual(call["database"], "foundation_staging", "and against the staging database")
        ingest = calls[4]
        self.assertEqual(ingest["threshold"], str(SAMPLE["multipart_threshold_bytes"]))
        self.assertEqual(ingest["bronze_key"], "content_addressed", "the sweep's own lane settings")
        self.assertTrue(ingest["inventory"].endswith("vworld-sample-inventory.json"))
        self.assertFalse(calls[5]["writer"], "the measure reads with the read-only pair only")
        self.assertEqual(calls[5]["sources"], "vworldkr__synthetic")
        # The two smallest files and the smallest one from the multipart threshold; never an archive.
        sampled = json.loads((self.fake / "sample.json").read_text())["sampled"]
        self.assertEqual(sampled, ["tiny", "small", "large"])

    def test_the_schema_is_built_from_empty_on_the_staging_database_only(self):
        self.assertEqual(self.run_smoke().returncode, 0)
        docker = [call for call in self.lines("docker.jsonl") if call["verb"] != "ps"]
        self.assertEqual([call["verb"] for call in docker], ["build", "exec", "exec", "run", "exec", "exec"])
        build, recreate, bootstrap, migrate, grants, finalize = docker
        self.assertEqual(build["tag"], migrate["image"])
        self.assertNotEqual(build["tag"], "foundation-platform-runtime:local", "the production image tag is not moved")
        self.assertEqual(recreate["statements"], ["DROP DATABASE IF EXISTS foundation_staging WITH (FORCE)",
                                                  "CREATE DATABASE foundation_staging"])
        self.assertEqual((migrate["migrator_role"], migrate["migrator_database"]),
                         ("foundation_migrator", "foundation_staging"))
        for step, role in ((bootstrap, "foundation_admin"), (grants, "foundation_migrator"),
                           (finalize, "foundation_admin")):
            self.assertEqual((step["database"], step["role"]), ("foundation_staging", role))
        self.assertNotIn("foundation", [call.get("database") for call in docker])

    def test_a_sample_that_does_not_land_whole_refuses_the_release(self):
        self.scenario(ingest_failed=1)
        result = self.run_smoke()
        self.assertEqual(result.returncode, 1)
        self.assertIn("ingest FAILED", result.stderr)
        self.assertNotIn("measure-bronze-object-members", [c["command"] for c in self.lines("calls.jsonl")])

    def test_a_measure_that_measures_less_than_landed_refuses_the_release(self):
        self.scenario(measured=0)
        result = self.run_smoke()
        self.assertEqual(result.returncode, 1)
        self.assertIn("measure FAILED", result.stderr)

    def test_an_inventory_without_a_file_for_the_multipart_path_refuses_the_release(self):
        small_only = [{**INVENTORY[0], "files": [vfile("tiny", 1), vfile("small", 2), vfile("middle", 8)]}]
        self.scenario(inventory=small_only)
        result = self.run_smoke()
        self.assertEqual(result.returncode, 1)
        self.assertIn("sample FAILED", result.stderr)
        self.assertNotIn("ingest-vworld-dataset-files", [c["command"] for c in self.lines("calls.jsonl")])

    def test_a_provider_that_lists_nothing_refuses_the_release(self):
        self.scenario(hub_jobs=[])
        result = self.run_smoke()
        self.assertEqual(result.returncode, 1)
        self.assertIn("plan FAILED", result.stderr)

    def test_a_step_that_dies_names_itself(self):
        self.scenario(die="clear-staging-namespace")
        result = self.run_smoke()
        self.assertEqual(result.returncode, 1)
        self.assertIn("clear FAILED", result.stderr)
        self.assertEqual(self.lines("docker.jsonl"), [], "nothing after the failed step ran")

    def test_a_missing_setting_refuses_before_any_side_effect(self):
        env = dict(self.env)
        del env["FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID"]
        result = self.run_smoke(env)
        self.assertEqual(result.returncode, 78)
        self.assertIn("FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID", result.stderr)
        self.assertEqual((self.lines("calls.jsonl"), self.lines("docker.jsonl")), ([], []))

    def test_no_secret_reaches_the_output(self):
        result = self.run_smoke()
        for name in NEEDS:
            if "PASSWORD" in name or "SECRET" in name:
                self.assertNotIn("planted-" + name.lower(), result.stdout + result.stderr)


class TheStagingDatabaseName(unittest.TestCase):
    """scripts/ops/database-url.sh: the one place a runtime environment names its database."""

    def ask(self, *call, **env):
        script = f'source "{PLATFORM / "scripts/ops/database-url.sh"}"; {" ".join(call)}'
        base = {"PATH": os.environ["PATH"], "FOUNDATION_ADMIN_PASSWORD": "p@ss/word",
                "FOUNDATION_MIGRATOR_PASSWORD": "m", "FOUNDATION_DB_PORT": "15434"}
        return subprocess.run(["bash", "-c", script], env={**base, **env}, capture_output=True, text=True, timeout=30)

    def test_staging_names_foundation_staging_and_production_names_foundation(self):
        staging = self.ask("foundation_database_url", FOUNDATION_PLATFORM_RUNTIME_ENV="staging")
        self.assertEqual(staging.stdout.strip(),
                         "postgres://foundation_admin:p%40ss%2Fword@127.0.0.1:15434/foundation_staging")
        # Only `staging` selects staging, as in the R2 client: no production job's address moves.
        for env in ({"FOUNDATION_PLATFORM_RUNTIME_ENV": "production"}, {},
                    {"FOUNDATION_PLATFORM_RUNTIME_ENV": "planted-anything"}):
            with self.subTest(env):
                self.assertEqual(self.ask("foundation_database_url", **env).stdout.strip(),
                                 "postgres://foundation_admin:p%40ss%2Fword@127.0.0.1:15434/foundation")
        migrator = self.ask("foundation_database_url", "migrator", FOUNDATION_PLATFORM_RUNTIME_ENV="staging")
        self.assertEqual(migrator.stdout.strip(), "postgres://foundation_migrator:m@127.0.0.1:15434/foundation_staging")

    def test_a_url_needs_its_roles_password(self):
        result = self.ask("foundation_database_url", "migrator", FOUNDATION_MIGRATOR_PASSWORD="")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")


if __name__ == "__main__":
    unittest.main()
