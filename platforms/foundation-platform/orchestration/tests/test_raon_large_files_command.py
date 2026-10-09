"""The large-file lane's real command line (scripts/ops/raon-large-files.sh, root ADR-0170).

The script runs from an installed release layout (root ADR-0134), as the daily sweep does
(test_daily_source_sweep_command.py). The publisher and docker are stand-ins: the publisher writes
the provider acquisition plan the way a scenario says, and docker records what it is asked to build
and run and answers as the scenario says. What runs for real is the script, the Python it calls,
the pinned package contract, the endpoint catalog's budget and the worker files it builds from:
a missing prerequisite stops it before anything happens, the image is named after its content and
built only once with the pinned package URL and sha256, the release's publisher is mounted
read-only, settings pass by name only, and a run over its budget fetches nothing.
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

RUN = "raon-large-files"
SCRIPT = PLATFORM / "scripts/ops/raon-large-files.sh"
CATALOG = PLATFORM / "docs/catalog/public-source-endpoint-catalog.v1.json"
NAMING = PLATFORM / "config/environment-variable-naming.contract.json"
PACKAGES = PLATFORM / "config/provider-agent-packages.contract.json"
WORKER = "services/foundation-provider-acquisition-worker"
NEEDS = sorted(runtime_secrets.load().consumer(RUN).needs)
REQUIRED = sorted(runtime_secrets.script_requirements(SCRIPT))
VWORLD_LOGIN = json.loads(NAMING.read_text(encoding="utf-8"))[
    "compatibility_migrations"]["foundation-vworld-credentials"]["credentials"]
RELEASE_ID = "e" * 40

FAKE_PUBLISHER = r"""#!/usr/bin/env python3
import json, os, sys
state = os.environ["FAKE_STATE"]
scenario = json.load(open(os.path.join(state, "scenario.json"), encoding="utf-8"))
assert sys.argv[1] == "plan-provider-acquisition-jobs", sys.argv
names = ("BLOCKED_EVIDENCE_PATH", "PLAN_OUTPUT_PATH", "NEW_BYTES_BUDGET", "MAX_FILES")
seen = {name: os.environ.get("FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_" + name) for name in names}
with open(os.path.join(state, "publisher.log"), "a", encoding="utf-8") as log:
    log.write(json.dumps(seen) + "\n")
jobs = scenario["plan"]["jobs"]
budget = int(seen["NEW_BYTES_BUDGET"])
listed = sum(job["listed_bytes"] for job in jobs)
status = "blocked_new_bytes_budget" if listed > budget else "ready"
json.dump({"status": status, "job_count": len(jobs), "candidate_count": len(jobs), "listed_bytes_total": listed,
           "jobs": jobs}, open(seen["PLAN_OUTPUT_PATH"], "w"))
sys.exit(1 if status != "ready" else 0)
"""

FAKE_DOCKER = r"""#!/usr/bin/env python3
import hashlib, json, os, sys
state = os.environ["FAKE_STATE"]
scenario = json.load(open(os.path.join(state, "scenario.json"), encoding="utf-8"))
args = sys.argv[1:]
def log(entry):
    with open(os.path.join(state, "docker.log"), "a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry) + "\n")
images = os.path.join(state, "images.txt")
known = open(images, encoding="utf-8").read().split() if os.path.exists(images) else []
if args[0] == "info":
    sys.exit(scenario.get("docker_info_rc", 0))
if args[:2] == ["image", "inspect"]:
    log({"command": "inspect", "image": args[2]})
    sys.exit(0 if args[2] in known else 1)
if args[0] == "build":
    context = sorted(os.path.relpath(os.path.join(d, f), ".") for d, _, fs in os.walk(".") for f in fs)
    log({"command": "build", "args": args, "context": context,
         "dockerfile_present": os.path.exists(args[args.index("-f") + 1]),
         "naming_contract_present": os.path.exists("config/environment-variable-naming.contract.json")})
    open(images, "a", encoding="utf-8").write(args[args.index("-t") + 1] + "\n")
    sys.exit(scenario.get("build_rc", 0))
if args[0] == "run":
    env_file = args[args.index("--env-file") + 1]
    lines = open(env_file, encoding="utf-8").read().split()
    mounts = [args[i + 1] for i, arg in enumerate(args) if arg == "-v"]
    env = dict(arg.split("=", 1) for i, arg in enumerate(args) if i and args[i - 1] == "-e" and "=" in arg)
    log({"command": "run", "args": args, "env_file_lines": lines, "image": args[-1]})
    run_dir = next(m.split(":")[0] for m in mounts if m.endswith(":/work/run"))
    batch = os.path.join(run_dir, "batch", env["BATCH_ID"])
    os.makedirs(batch, exist_ok=True)
    plan = json.load(open(os.path.join(run_dir, "plan.json"), encoding="utf-8"))
    failed = scenario.get("batch_failed", 0)
    results = [{"source_slug": job["source_slug"], "provider_file_id": job["provider_file_id"],
                "status": "failed" if index < failed else "committed",
                "bronze_object_key": "bronze/source=x/" + job["provider_file_id"] + "--sha256-" + "a" * 64 + ".zip"}
               for index, job in enumerate(plan["jobs"])]
    json.dump({"committed_count": len(results) - failed, "failed_count": failed, "results": results},
              open(os.path.join(batch, "summary.json"), "w"))
    sys.exit(2 if failed else 0)
sys.exit("unexpected docker " + " ".join(args))
"""


def job(file_no, listed_bytes=600 * 2**20):
    return {"source_slug": "vworldkr__synthetic", "provider_file_id": f"20991231DS99991-{file_no}",
            "download_ds_id": "20991231DS99991", "file_no": file_no, "listed_bytes": listed_bytes}


class RaonLargeFiles(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="raon-large-files-")
        self.addCleanup(self.temp.cleanup)
        root = pathlib.Path(self.temp.name)
        base = root / "opt/foundation-platform"
        self.release = release = base / "releases" / RELEASE_ID
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("raon-large-files.sh", "admitted-writer-runtime.sh", "vworld-login.sh", "job-journal.sh"):
            (release / "scripts/ops" / name).write_bytes((PLATFORM / "scripts/ops" / name).read_bytes())
            (release / "scripts/ops" / name).chmod(0o755)
        (release / "docs/catalog").mkdir(parents=True)
        shutil.copy(CATALOG, release / "docs/catalog")
        (release / "config").mkdir()
        shutil.copy(NAMING, release / "config")
        worker = release / WORKER
        worker.mkdir(parents=True)
        for name in ("Dockerfile.raon-batch", "pyproject.toml", "requirements.lock"):
            shutil.copy(PLATFORM / WORKER / name, worker)
        for name in ("src", "scripts"):
            shutil.copytree(PLATFORM / WORKER / name, worker / name,
                            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        # The pinned package as the repository pins it.
        shutil.copy(PACKAGES, release / "config")
        self.pin = json.loads(PACKAGES.read_text(encoding="utf-8"))["packages"]["raonk-2018"]
        (base / "current").symlink_to(pathlib.Path("releases") / RELEASE_ID)
        artifacts = base / "artifacts" / RELEASE_ID
        artifacts.mkdir(parents=True)
        self.publisher = artifacts / "foundation-outbox-publisher"
        self.publisher.write_text(FAKE_PUBLISHER)
        self.publisher.chmod(0o555)
        (artifacts / "build.json").write_text(json.dumps({
            "source": RELEASE_ID, "publisher_image": "sha256:" + "a" * 64, "tippecanoe_image": "sha256:" + "d" * 64,
            "files": {"foundation-outbox-publisher": hashlib.sha256(self.publisher.read_bytes()).hexdigest(),
                      "jars/fixture.jar": "0" * 64}}))
        bin_dir = root / "bin"
        bin_dir.mkdir()
        (bin_dir / "docker").write_text(FAKE_DOCKER)
        (bin_dir / "docker").chmod(0o755)
        self.script = base / "current/scripts/ops/raon-large-files.sh"
        self.fake = root / "fake"
        self.fake.mkdir()
        self.work = root / "data/source-sweep/raon"
        self.evidence = root / "vworld-evidence.json"
        self.evidence.write_text("{}")
        self.env = {
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}", "FAKE_STATE": str(self.fake),
            "FOUNDATION_RAON_LARGE_FILES_ROOT": str(self.work),
            **{name: "planted-" + name.lower() for name in NEEDS},
            VWORLD_LOGIN["username"]["canonical"]: "planted-user", VWORLD_LOGIN["password"]["canonical"]: "planted-pass",
        }

    def write_pin(self, **override):
        contract = json.loads(PACKAGES.read_text(encoding="utf-8"))
        contract["packages"]["raonk-2018"].update(override)
        (self.release / "config/provider-agent-packages.contract.json").write_text(json.dumps(contract))

    def scenario(self, jobs=(), **extra):
        (self.fake / "scenario.json").write_text(json.dumps({"plan": {"jobs": list(jobs)}, **extra}))

    def run_script(self, *args, env=None):
        return subprocess.run(["bash", str(self.script), *args], env=env or self.env, capture_output=True,
                              text=True, timeout=120)

    def log(self, name):
        path = self.fake / name
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()] if path.exists() else []

    def summary(self):
        runs = sorted((self.work / "runs").iterdir())
        return json.loads((runs[-1] / "summary.json").read_text(encoding="utf-8"))

    def docker(self, command):
        return [entry for entry in self.log("docker.log") if entry["command"] == command]

    def assert_refused_before_anything(self, result, reason):
        self.assertEqual(result.returncode, 78, result.stderr)
        self.assertIn(reason, result.stderr)
        self.assertEqual(self.log("publisher.log"), [], "nothing was planned")
        self.assertEqual(self.docker("build") + self.docker("run"), [], "nothing was built or run")
        self.assertFalse(self.work.exists(), "no state was written")

    # --- prerequisites: loud, before anything ---

    def test_a_missing_or_malformed_package_pin_stops_the_lane_before_anything(self):
        self.scenario([job("70")])
        for override in ({"sha256": ""}, {"sha256": "F" * 64}, {"url": "http://example.invalid/agent.deb"}):
            with self.subTest(override):
                self.write_pin(**override)
                self.assert_refused_before_anything(self.run_script("run", str(self.evidence)), "package pin")
        (self.release / "config/provider-agent-packages.contract.json").unlink()
        self.assert_refused_before_anything(self.run_script("run", str(self.evidence)), "package pin")

    def test_a_silent_docker_daemon_stops_the_lane(self):
        self.scenario([job("70")], docker_info_rc=1)
        self.assert_refused_before_anything(self.run_script("run", str(self.evidence)), "docker daemon")

    def test_a_missing_setting_stops_the_lane(self):
        self.assertLessEqual(set(REQUIRED), set(NEEDS), "the script requires only what the contract gives the run")
        self.scenario([job("70")])
        logins = [VWORLD_LOGIN[role]["canonical"] for role in ("username", "password")]
        for name in [*REQUIRED, *logins]:
            with self.subTest(name):
                env = dict(self.env)
                del env[name]
                self.assert_refused_before_anything(self.run_script("run", str(self.evidence), env=env), name)

    def test_missing_evidence_stops_the_lane(self):
        self.scenario([job("70")])
        self.assert_refused_before_anything(self.run_script("run", str(self.evidence) + ".absent"), "no VWorld")

    # --- the run ---

    def test_the_catalogs_budget_prices_the_plan(self):
        self.scenario([])
        self.assertEqual(self.run_script("plan", str(self.evidence)).returncode, 0)
        plan_call = self.log("publisher.log")[0]
        self.assertEqual(plan_call["BLOCKED_EVIDENCE_PATH"], str(self.evidence))
        catalog = json.loads(CATALOG.read_text(encoding="utf-8"))
        self.assertEqual(int(plan_call["NEW_BYTES_BUDGET"]),
                         catalog["daily_collections"]["source_sweep"]["selection_archive_new_bytes_budget"])
        self.assertFalse(plan_call["MAX_FILES"], "a daily run takes every file its budget allows")
        self.assertNotIn("budget_override", (self.work / "journal.log").read_text(encoding="utf-8"))

    def test_a_run_builds_its_image_once_from_the_checked_package_and_lands_the_plan(self):
        self.scenario([job("70"), job("71")])
        self.env["FOUNDATION_RAON_LARGE_FILES_NEW_BYTES_BUDGET"] = str(2**40)
        result = self.run_script("run", str(self.evidence))
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [build] = self.docker("build")
        self.assertTrue(build["dockerfile_present"] and build["naming_contract_present"])
        # The package comes from the pin, as build arguments the Dockerfile has no defaults for.
        self.assertIn(f"RAON_DEB_URL={self.pin['url']}", build["args"])
        self.assertIn(f"RAON_DEB_SHA256={self.pin['sha256']}", build["args"])
        self.assertTrue(all(path.startswith(("services/", "config/")) for path in build["context"]),
                        "the context is the hashed inputs and nothing else")
        self.assertEqual(build["args"][-1], ".", "one literal context, as the container policy requires")
        [run] = self.docker("run")
        tag = build["args"][build["args"].index("-t") + 1]
        self.assertEqual(run["image"], tag)
        self.assertRegex(tag, r"^foundation-platform/raon-batch-[0-9a-f]{16}:local$")
        self.assertIn(f"{self.publisher}:/usr/local/bin/foundation-outbox-publisher:ro", run["args"],
                      "the release's own publisher, read-only")
        self.assertIn("host", run["args"][run["args"].index("--network") + 1])
        self.assertIn("DATABASE_URL", run["env_file_lines"])
        self.assertIn("FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY", run["env_file_lines"])
        self.assertIn(VWORLD_LOGIN["password"]["canonical"], run["env_file_lines"])
        self.assertTrue(all("=" not in line for line in run["env_file_lines"]), "names only, never values")
        everything = json.dumps(self.log("docker.log")) + result.stdout + result.stderr
        self.assertNotIn("planted-pass", everything)
        self.assertNotIn("planted-foundation_platform_r2_lakehouse_writer_secret_access_key", everything)
        summary = self.summary()
        self.assertEqual((summary["status"], summary["planned"], summary["committed"]), ("ready", 2, 2))
        journal = (self.work / "journal.log").read_text(encoding="utf-8")
        self.assertIn("raon planned=2 committed=2 failed=0", journal)
        self.assertFalse((self.work / "context").exists(), "the build context is removed")

        # The same content is the same image: the next run builds nothing.
        self.assertEqual(self.run_script("run", str(self.evidence)).returncode, 0)
        self.assertEqual(len(self.docker("build")), 1)
        self.assertEqual(len(self.docker("run")), 2)

    def test_the_image_name_follows_the_worker_and_the_package_pin(self):
        self.scenario()
        self.assertEqual(self.run_script("build").returncode, 0)
        self.assertEqual(self.run_script("build").returncode, 0)
        (self.release / WORKER / "src/foundation_provider_acquisition/__init__.py").write_text("# changed\n")
        self.assertEqual(self.run_script("build").returncode, 0)
        self.write_pin(sha256="0" * 64)
        self.assertEqual(self.run_script("build").returncode, 0)
        tags = [b["args"][b["args"].index("-t") + 1] for b in self.docker("build")]
        self.assertEqual(len(tags), 3, "the same content is not built twice")
        self.assertEqual(len(set(tags)), 3, "a changed worker or package pin is a new image")

    def test_a_dry_plan_fetches_nothing(self):
        self.scenario([job("70")])
        self.env["FOUNDATION_RAON_LARGE_FILES_NEW_BYTES_BUDGET"] = str(2**40)
        result = self.run_script("plan", str(self.evidence))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.docker("build") + self.docker("run"), [])
        self.assertEqual(self.summary()["status"], "planned")

    def test_nothing_deferred_is_nothing_to_fetch(self):
        self.scenario([])
        result = self.run_script("run", str(self.evidence))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.docker("build") + self.docker("run"), [], "no image for no work")
        self.assertEqual(self.summary()["status"], "nothing-to-fetch")

    def test_a_run_over_its_budget_fetches_nothing_and_fails(self):
        self.scenario([job(str(n), 2**30) for n in range(70, 95)])
        self.env["FOUNDATION_RAON_LARGE_FILES_NEW_BYTES_BUDGET"] = str(24 * 2**30)
        result = self.run_script("run", str(self.evidence))
        self.assertEqual(result.returncode, 1)
        self.assertEqual(self.docker("run"), [])
        self.assertEqual(self.summary()["status"], "blocked_new_bytes_budget")

    def test_an_operator_takes_one_file_with_a_raised_budget(self):
        self.scenario([job("70")])
        env = dict(self.env, FOUNDATION_RAON_LARGE_FILES_NEW_BYTES_BUDGET=str(2**40),
                   FOUNDATION_RAON_LARGE_FILES_MAX_FILES="1")
        result = self.run_script("run", str(self.evidence), env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        plan_call = self.log("publisher.log")[0]
        self.assertEqual((plan_call["NEW_BYTES_BUDGET"], plan_call["MAX_FILES"]), (str(2**40), "1"))
        self.assertIn("budget_override=1", (self.work / "journal.log").read_text(encoding="utf-8"))
        env["FOUNDATION_RAON_LARGE_FILES_MAX_FILES"] = "0"
        self.assertEqual(self.run_script("run", str(self.evidence), env=env).returncode, 78)

    def test_a_failed_file_fails_the_run(self):
        self.scenario([job("70"), job("71")], batch_failed=1)
        self.env["FOUNDATION_RAON_LARGE_FILES_NEW_BYTES_BUDGET"] = str(2**40)
        result = self.run_script("run", str(self.evidence))
        self.assertEqual(result.returncode, 1)
        summary = self.summary()
        self.assertEqual((summary["status"], summary["committed"], summary["failed"]), ("failed", 1, 1))

    def test_a_killed_runs_staged_bytes_are_cleared(self):
        staged = self.work / "runs/20991231T000000Z/batch/raon-x/job-1/staging"
        staged.mkdir(parents=True)
        (staged / "replay.zip").write_bytes(b"partial")
        kept = self.work / "runs/20991231T000000Z/batch/raon-x/job-1/raon-agent-proof.json"
        kept.write_text("{}")
        self.scenario([])
        self.assertEqual(self.run_script("run", str(self.evidence)).returncode, 0)
        self.assertFalse(staged.exists())
        self.assertTrue(kept.exists(), "only staged provider bytes are removed")


class PinnedPackage(unittest.TestCase):
    """The package is pinned once, in the release's config (root ADR-0170)."""

    def test_the_pin_names_a_secure_source_and_its_bytes(self):
        package = json.loads(PACKAGES.read_text(encoding="utf-8"))["packages"]["raonk-2018"]
        self.assertTrue(package["url"].startswith("https://"))
        self.assertRegex(package["sha256"], r"^[0-9a-f]{64}$")
        self.assertGreater(package["size_bytes"], 0)

    def test_no_other_file_restates_the_url_or_the_checksum(self):
        package = json.loads(PACKAGES.read_text(encoding="utf-8"))["packages"]["raonk-2018"]
        skipped = {".git", "target", "node_modules", "__pycache__", ".pytest_cache"}
        for value in (package["url"], package["sha256"]):
            with self.subTest(value):
                holders = []
                for directory, subdirectories, files in os.walk(PLATFORM):
                    subdirectories[:] = [name for name in subdirectories if name not in skipped]
                    for name in files:
                        path = pathlib.Path(directory) / name
                        try:
                            if value.encode() in path.read_bytes():
                                holders.append(path.relative_to(PLATFORM).as_posix())
                        except OSError:
                            continue
                self.assertEqual(holders, ["config/provider-agent-packages.contract.json"])

    def test_the_dockerfiles_take_the_package_only_as_arguments(self):
        for name in ("Dockerfile.raon-batch", "Dockerfile.raon-agent-proof"):
            text = (PLATFORM / WORKER / name).read_text(encoding="utf-8")
            with self.subTest(name):
                self.assertIn("\nARG RAON_DEB_URL\n", text, "no default URL")
                self.assertIn("\nARG RAON_DEB_SHA256\n", text, "no default checksum")


if __name__ == "__main__":
    unittest.main()
