"""The scheduled lakehouse maintenance (scripts/ops/lakehouse-maintenance.sh) against a fake docker.

The fake stands in for `docker compose ... run spark spark-submit lakehouse_maintenance.py`: it
records what it was asked to run and prints what the job prints. The release layout is the host's
(root ADR-0134), as in test_gold_panel_rebuild.py.
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
sys.path.insert(0, str(PLATFORM / "infra/lakehouse/spark/jobs"))
import lakehouse_maintenance  # noqa: E402

RELEASE_ID = "e" * 40
INVOCATION = "c" * 32

FAKE_DOCKER = r'''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
if args[:1] == ["rm"]:
    sys.exit(0)
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps({"args": args, "project": args[args.index("-p") + 1]}) + "\n")
print("maintenance-done step=snapshot_expiry data_files=3 manifests=1 older_than=x")
print("spark noise that is not a maintenance line")
print("maintenance-summary tables=2 done=2 failed=0 deferred=0")
print("foundation-job-outcome unchanged")
sys.exit(int(os.environ.get("FAKE_EXIT", "0")))
'''


class LakehouseMaintenance(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="lakehouse-maintenance-")
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        base = self.root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("lakehouse-maintenance.sh", "admitted-writer-runtime.sh"):
            (release / "scripts/ops" / name).write_bytes((PLATFORM / "scripts/ops" / name).read_bytes())
            (release / "scripts/ops" / name).chmod(0o755)
        for part in ("infra/lakehouse/contracts", "infra/lakehouse/spark/jobs"):
            shutil.copytree(PLATFORM / part, release / part, ignore=shutil.ignore_patterns("__pycache__"))
        (base / "current").symlink_to(pathlib.Path("releases") / RELEASE_ID)
        artifacts = base / "artifacts" / RELEASE_ID
        artifacts.mkdir(parents=True)
        publisher = artifacts / "foundation-outbox-publisher"
        publisher.write_text('#!/bin/sh\nprintf "publisher %s\\n" "$*"\n')
        publisher.chmod(0o555)
        (artifacts / "build.json").write_text(json.dumps({
            "source": RELEASE_ID, "publisher_image": "sha256:" + "a" * 64, "tippecanoe_image": "sha256:" + "d" * 64,
            "files": {"foundation-outbox-publisher": hashlib.sha256(publisher.read_bytes()).hexdigest(),
                      "jars/fixture.jar": "0" * 64}}))
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        (bin_dir / "docker").write_text(FAKE_DOCKER)
        (bin_dir / "docker").chmod(0o755)
        self.script = base / "current/scripts/ops/lakehouse-maintenance.sh"
        self.log = self.root / "calls.jsonl"
        self.state = self.root / "state"
        self.env = {
            "INVOCATION_ID": INVOCATION,
            "PATH": f"{bin_dir}:{os.environ['PATH']}", "FAKE_LOG": str(self.log),
            "FOUNDATION_LAKEHOUSE_MAINTENANCE_STATE_ROOT": str(self.state),
            "FOUNDATION_LAKEHOUSE_MAINTENANCE_SCRATCH": str(self.root / "scratch"),
        }

    def run_script(self, *args, **env):
        result = subprocess.run(["bash", str(self.script), *args], env={**self.env, **env},
                                capture_output=True, text=True, timeout=120)
        calls = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        return result, calls

    def test_one_spark_run_takes_every_table_under_the_contracts_size_in_this_invocations_project(self):
        result, [call] = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(call["project"], "foundation-lakehouse-maintenance-" + INVOCATION)
        submit = call["args"][call["args"].index("spark-submit"):]
        spark = lakehouse_maintenance.load_maintenance_contract()["scheduled"]["spark"]
        self.assertEqual(submit[submit.index("--master") + 1], spark["master"])
        self.assertEqual(submit[submit.index("--driver-memory") + 1], spark["driver_memory"])
        job = submit.index("/workspace/infra/lakehouse/spark/jobs/lakehouse_maintenance.py")
        self.assertEqual(submit[job + 1:], ["--all-tables", "--state-file", "/workspace/target/lakehouse/rotation.json"])
        # The job's own lines reach the unit's journal; Spark's noise stays in the run's log.
        self.assertIn("foundation-job-outcome unchanged\n", result.stdout)
        self.assertIn("maintenance-summary tables=2", result.stdout)
        self.assertNotIn("spark noise", result.stdout)

    def test_a_rotation_file_the_container_owns_does_not_stop_the_run(self):
        """2026-10-15 05:04: rotation.json belonged to the container's uid 185; chmod-ing it failed the run."""
        self.state.mkdir(exist_ok=True)
        (self.state / "rotation.json").write_text("{}")
        bin_dir = pathlib.Path(self.env["PATH"].split(":")[0])
        (bin_dir / "chmod").write_text(
            '#!/usr/bin/env bash\n'
            'for a in "$@"; do [[ "$a" == */rotation.json ]] && { echo "chmod: Operation not permitted" >&2; exit 1; }; done\n'
            'exec /bin/chmod "$@"\n')
        (bin_dir / "chmod").chmod(0o755)
        result, calls = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(len(calls), 1)

    def test_a_failed_table_fails_the_unit(self):
        result, _ = self.run_script(FAKE_EXIT="1")
        self.assertEqual(result.returncode, 1)
        self.assertIn("FAILED (exit 1", result.stdout)

    def test_a_dry_run_is_passed_through_and_nothing_else_is_accepted(self):
        result, [call] = self.run_script("--dry-run")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(call["args"][-1], "--dry-run")
        refused, _ = self.run_script("--table")
        self.assertEqual(refused.returncode, 64)

    def test_cleanup_has_the_release_publisher_stop_this_invocations_containers(self):
        result, _ = self.run_script("cleanup")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "publisher stop-lakehouse-maintenance\n")
        refused = subprocess.run(["bash", str(self.script), "cleanup"],
                                 env={k: v for k, v in self.env.items() if k != "INVOCATION_ID"},
                                 capture_output=True, text=True, timeout=60)
        self.assertEqual(refused.returncode, 64, refused.stderr)


if __name__ == "__main__":
    unittest.main()
