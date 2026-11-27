"""The staging smoke (scripts/ops/staging-smoke.sh, root ADR-0177) against stand-ins.

The script runs from an installed release that is not `current` (the deploy runs it before the
switch). The publisher and docker are stand-ins: the publisher records the command and environment it
was given, docker records what it was asked. What runs for real is the script, the step bounds of
config/staging-gate.contract.json and database-url.sh: every publisher call runs as staging against
foundation_staging, the schema is built from empty, and no provider is asked (root ADR-0363, amended
2026-11-27): 2026-11-27 09:35-09:44Z VWorld's scheduled maintenance refused five releases in a row
through the smoke's live inventory read.
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

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

PLATFORM = job_specs.PLATFORM_ROOT
sys.path.insert(0, str(PLATFORM / "scripts/deploy"))
import runtime_secrets  # noqa: E402

RUN = "staging-smoke"
CONTRACT = PLATFORM / "config/staging-gate.contract.json"
NAMING = PLATFORM / "config/environment-variable-naming.contract.json"
NEEDS = sorted(runtime_secrets.load().consumer(RUN).needs)
RELEASE_ID = "e" * 40
CURRENT_ID = "c" * 40
# Every command a provider answers; the smoke runs none of them.
PROVIDER_COMMANDS = ("plan-building-hub-bulk-collection", "plan-vworld-dataset-collection",
                     "inventory-vworld-dataset-files", "ingest-vworld-dataset-files")

FAKE_PUBLISHER = r"""#!/usr/bin/env python3
import json, os, sys
state = os.environ["FAKE_STATE"]
command = sys.argv[1]
scenario = json.load(open(os.path.join(state, "scenario.json"), encoding="utf-8"))
env = os.environ
seen = {"command": command, "runtime_env": env.get("FOUNDATION_PLATFORM_RUNTIME_ENV"),
        "database": env.get("DATABASE_URL", "").rsplit("/", 1)[-1],
        "writer": bool(env.get("FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID"))}
with open(os.path.join(state, "calls.jsonl"), "a", encoding="utf-8") as log:
    log.write(json.dumps(seen) + "\n")
if command == scenario.get("die"):
    sys.exit(command + " died")
if command == "clear-staging-namespace":
    print("staging-namespace cleared prefix=staging/ objects=0 bytes=0")
else:
    sys.exit("unexpected command " + command)
"""

# docker: `ps` names one postgres, `exec ... psql` records its database, role and statements and
# swallows the SQL on stdin, `build` and `run` record themselves.
FAKE_DOCKER = r"""#!/usr/bin/env python3
import json, os, sys
state = os.environ["FAKE_STATE"]
args = sys.argv[1:]
record = {"verb": args[0], "docker_config": os.environ.get("DOCKER_CONFIG", "")}
if not os.path.isdir(record["docker_config"]):
    sys.exit("mkdir /nonexistent: permission denied")  # what the real CLI says without a config dir
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


class StagingSmoke(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="staging-smoke-")
        self.addCleanup(temp.cleanup)
        root = pathlib.Path(temp.name)
        base = root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("staging-smoke.sh", "admitted-writer-runtime.sh", "database-url.sh", "job-journal.sh"):
            (release / "scripts/ops" / name).write_bytes((PLATFORM / "scripts/ops" / name).read_bytes())
            (release / "scripts/ops" / name).chmod(0o755)
        (release / "config").mkdir()
        shutil.copy(CONTRACT, release / "config")
        shutil.copy(NAMING, release / "config")
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
            # The service account's home in production; Docker cannot keep its state there.
            "HOME": "/nonexistent", "DOCKER_CONFIG": "/nonexistent/.docker",
        }
        self.scenario()

    def scenario(self, **fields):
        (self.fake / "scenario.json").write_text(json.dumps(fields), encoding="utf-8")

    def run_smoke(self, env=None):
        return subprocess.run(["bash", str(self.script)], env=env or self.env, capture_output=True, text=True,
                              timeout=120)

    def lines(self, name):
        path = self.fake / name
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()] if path.exists() else []

    def test_a_good_release_passes_every_step_as_staging_and_asks_no_provider(self):
        result = self.run_smoke()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(f"staging-smoke: passed release={RELEASE_ID}", result.stdout)
        calls = self.lines("calls.jsonl")
        self.assertEqual([call["command"] for call in calls], ["clear-staging-namespace"])
        self.assertFalse(set(call["command"] for call in calls) & set(PROVIDER_COMMANDS))
        (clear,) = calls
        self.assertEqual(clear["runtime_env"], "staging", "the publisher call runs in the staging namespace")
        self.assertEqual(clear["database"], "foundation_staging", "and against the staging database")
        self.assertTrue(clear["writer"], "the clear step empties staging/ with the writer pair")

    def test_the_gate_has_no_step_that_reads_a_provider(self):
        steps = json.loads(CONTRACT.read_text(encoding="utf-8"))["steps"]
        self.assertEqual(sorted(steps), ["clear", "database", "image"])
        script = self.script.read_text(encoding="utf-8")
        for command in PROVIDER_COMMANDS:
            self.assertNotIn(command, script)
        self.assertNotIn("vworld-login.sh", script)

    def test_docker_keeps_its_state_in_the_smoke_directory(self):
        # 2026-10-10, the first real smoke: the image step failed with "mkdir /nonexistent: permission
        # denied" because the service account has no home (root ADR-0177).
        result = self.run_smoke()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        configs = {call["docker_config"] for call in self.lines("docker.jsonl")}
        self.assertEqual(configs, {str(self.state / "docker-config")})

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

    def test_a_step_that_dies_names_itself(self):
        self.scenario(die="clear-staging-namespace")
        result = self.run_smoke()
        self.assertEqual(result.returncode, 1)
        self.assertIn("clear FAILED", result.stderr)
        self.assertEqual(self.lines("docker.jsonl"), [], "nothing after the failed step ran")

    def test_a_missing_setting_refuses_before_any_side_effect(self):
        env = dict(self.env)
        del env["FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID"]
        result = self.run_smoke(env)
        self.assertEqual(result.returncode, 78)
        self.assertIn("FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID", result.stderr)
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
