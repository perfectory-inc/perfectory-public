"""The deploy steps that ran on every deploy say why, or skip while their inputs are unchanged (root ADR-0308).

2026-10-27: the deploy of 77198c1f spent 1 min 51 s bringing 48 lakehouse tables to contracts that
release_changes.py had just reported unchanged; the same bytes had been migrated by the deploy before.
2026-10-29 review: the skip recorded which bytes ran, not the catalog it ran against; a restored or
re-pointed catalog, other jars or drift in production must run it again, and a record another
account could write must not be trusted.
"""

import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
CONTRACT = PLATFORM / "config/deploy-steps.contract.json"
STEPS = PLATFORM / "scripts/deploy/deploy_steps.py"
JOBS = PLATFORM / "orchestration/jobs.v1.json"
RUNTIME_SECRETS = PLATFORM / "config/runtime-secrets.contract.json"
# Where each step is run, and so where its contract entry is read.
CALLERS = {
    "lakehouse-migrate": PLATFORM / "scripts/deploy/foundation-release.sh",
    "postgres-chain": PLATFORM / "scripts/deploy/foundation-release.sh",
}
START_ONCE = PLATFORM / "scripts/deploy/deploy-start-once.sh"
SECRET = "do-not-print-0f3a"


class TheContract(unittest.TestCase):
    def test_every_step_has_inputs_or_a_reason_and_a_budget(self):
        entries = json.loads(CONTRACT.read_text(encoding="utf-8"))["steps"]
        once = {f"start-once.{job['id']}" for job in json.loads(JOBS.read_text(encoding="utf-8"))["jobs"]
                if job["enabled"] and job.get("started_once_after_deploy")}
        self.assertEqual(set(entries), set(CALLERS) | once,
                         "every job a deploy starts once says what it runs, and nothing else is listed")
        for step in once:
            self.assertIn("inputs", entries[step], "a started-once job starts only when what it runs changed")
        for step, entry in entries.items():
            with self.subTest(step):
                if "inputs" in entry:
                    self.assertNotIn("always", entry)
                    self.assertTrue(entry["inputs"])
                    for item in entry["inputs"]:
                        self.assertTrue((PLATFORM / item).exists(), f"{item} names nothing")
                    for item in entry.get("per_table", {}):
                        self.assertTrue(any(item == i or item.startswith(i + "/") for i in entry["inputs"]),
                                        f"{item} is read table by table but is not an input")
                else:
                    self.assertTrue(entry["always"].strip(), "a step that always runs says why")
                    self.assertGreater(entry["budget_seconds"], 0)

    def test_the_migration_runs_against_the_catalog_and_jars_and_is_reverified(self):
        entry = json.loads(CONTRACT.read_text(encoding="utf-8"))["steps"]["lakehouse-migrate"]
        self.assertTrue(entry["release_jars"])
        self.assertTrue((PLATFORM / entry["environment"]["unit"]).is_file())
        self.assertTrue((PLATFORM / entry["reverify"]["contracts"]).is_file())
        # The settings it hashes are names the migration's environment group holds, none a credential.
        groups = json.loads(RUNTIME_SECRETS.read_text(encoding="utf-8"))["groups"]
        unit = (PLATFORM / entry["environment"]["unit"]).read_text(encoding="utf-8")
        loaded = [group for group in groups if f"EnvironmentFile={group['path']}" in unit]
        self.assertTrue(loaded)
        held = {name for group in loaded for name in group.get("holds", [])}
        for name in entry["environment"]["names"]:
            self.assertIn(name, held)
            self.assertNotRegex(name, r"TOKEN|SECRET|PASSWORD|KEY")
        self.assertTrue(json.loads(CONTRACT.read_text(encoding="utf-8"))["state_directory"].startswith("/var/lib/perfectory/"),
                        "the records live in the deploy's root-owned state, not the jobs' account's")

    def test_the_deploy_asks_about_each_step_by_its_name(self):
        for step, caller in CALLERS.items():
            with self.subTest(step):
                self.assertIn(f" {step} ", caller.read_text(encoding="utf-8").replace('"', " "))
        self.assertIn('should-run "start-once.${id}"', START_ONCE.read_text(encoding="utf-8"))
        self.assertIn('ran "start-once.${id}"', START_ONCE.read_text(encoding="utf-8"))

    def test_only_the_deploy_skips_and_a_run_by_hand_always_runs(self):
        release = (PLATFORM / "scripts/deploy/foundation-release.sh").read_text(encoding="utf-8")
        deploy = (PLATFORM / "scripts/deploy/foundation-deploy.sh").read_text(encoding="utf-8")
        self.assertIn('"${release}" migrate --skip-unchanged\n', deploy)
        self.assertIn("1:) migrate_runtime run ;;", release)
        body = re.search(r"^migrate_lakehouse\(\) \{\n(.*?)^\}", release, re.M | re.S).group(1)
        # The record goes before a run and comes back only after it succeeded.
        self.assertLess(body.index("forget"), body.index("systemctl start foundation-lakehouse-migrate"))
        self.assertLess(body.index("systemctl start foundation-lakehouse-migrate"), body.index(" ran "))
        self.assertIn("install -d -o root -g root -m 0700", release)


class TheGate(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="deploy-steps-")
        self.addCleanup(temp.cleanup)
        top = pathlib.Path(temp.name)
        self.host = top / "host"
        (self.host / "var/lib/perfectory").mkdir(parents=True)
        (self.host / "var/lib/perfectory").chmod(0o755)
        (self.host / "etc").mkdir()
        # An installed release: releases/<sha> beside artifacts/<sha>/build.json.
        sha = "a" * 40
        self.release = top / "opt/releases" / sha
        self.build = top / "opt/artifacts" / sha / "build.json"
        self.build.parent.mkdir(parents=True)
        self.build.write_text(json.dumps({"files": {"jars/iceberg.jar": "1" * 64, "foundation-outbox-publisher": "2" * 64}}))
        (self.release / "config").mkdir(parents=True)
        (self.release / "config/deploy-steps.contract.json").write_text(json.dumps({
            "state_directory": "/var/lib/perfectory/deploy-steps",
            "steps": {
                "migrate": {"inputs": ["contracts", "job.py", "migrate.service"],
                            "environment": {"unit": "migrate.service", "names": ["CATALOG_URI", "WAREHOUSE"]},
                            "release_jars": True,
                            "reverify": {"production_shape": "/var/lib/shape/capture.json",
                                         "contracts": "contracts/tables.json"}},
                "plain": {"inputs": ["job.py"]},
                "chain": {"always": "its image is the release's own", "budget_seconds": 60},
            }}))
        (self.release / "contracts").mkdir()
        (self.release / "contracts/tables.json").write_text(json.dumps(
            {"contracts": {"silver.a": {"columns": [{"name": "k"}, {"name": "v"}]}}}))
        (self.release / "job.py").write_text("print()")
        (self.release / "migrate.service").write_text(
            "[Service]\nEnvironmentFile=/etc/catalog.env\nEnvironmentFile=-/etc/optional.env\n")
        self.env(f"CATALOG_URI=https://catalog.invalid/one\nWAREHOUSE=w\nCATALOG_TOKEN={SECRET}\n")

    def env(self, text, name="catalog.env"):
        (self.host / "etc" / name).write_text(text)

    def steps(self, *args):
        return subprocess.run([sys.executable, "-I", "-B", str(STEPS), *map(str, args)],
                              env={**os.environ, "FOUNDATION_DEPLOY_STEPS_HOST_ROOT": str(self.host)},
                              capture_output=True, text=True, check=False, timeout=60)

    def should_run(self, step="migrate"):
        result = self.steps("should-run", self.release, step)
        self.assertNotIn(SECRET, result.stdout + result.stderr, "a value is never printed")
        self.assertNotIn("catalog.invalid", result.stdout + result.stderr, "a value is never printed")
        self.last = result.stdout + result.stderr
        return result.returncode

    def ran(self, step="migrate"):
        result = self.steps("ran", self.release, step)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_a_step_never_run_here_runs_and_once_it_ran_the_same_bytes_skip_it(self):
        self.assertEqual(self.should_run(), 0)
        self.ran()
        skipped = self.steps("should-run", self.release, "migrate")
        self.assertEqual(skipped.returncode, 3, skipped.stdout + skipped.stderr)
        self.assertIn("skipped", skipped.stdout)
        state = self.host / "var/lib/perfectory/deploy-steps"
        self.assertEqual(oct(state.stat().st_mode & 0o777), "0o700")

    def test_a_changed_input_runs_it_again(self):
        self.ran()
        (self.release / "contracts/tables.json").write_text(json.dumps({"contracts": {}}))
        self.assertEqual(self.should_run(), 0)

    def test_a_readme_is_not_an_input(self):
        self.ran()
        (self.release / "contracts/README.md").write_text("prose")
        self.assertEqual(self.should_run(), 3)

    def test_a_catalog_pointed_elsewhere_runs_it_again_and_only_the_named_settings_count(self):
        self.ran()
        self.env(f"CATALOG_URI=https://catalog.invalid/one\nWAREHOUSE=w\nCATALOG_TOKEN=rotated-{SECRET}\n")
        self.assertEqual(self.should_run(), 3, "a rotated credential is not the catalog's identity")
        self.env("CATALOG_URI='https://catalog.invalid/one'\nWAREHOUSE=w\n")
        self.assertEqual(self.should_run(), 3, "quotes are systemd's, not the value's")
        self.env("WAREHOUSE=elsewhere\n", "optional.env")
        self.assertEqual(self.should_run(), 0, "a later file wins, as in systemd")
        (self.host / "etc/optional.env").unlink()
        self.env("CATALOG_URI=https://catalog.invalid/restored\nWAREHOUSE=w\n")
        self.assertEqual(self.should_run(), 0)

    def test_other_jars_run_it_again(self):
        self.ran()
        build = json.loads(self.build.read_text())
        build["files"]["foundation-outbox-publisher"] = "3" * 64
        self.build.write_text(json.dumps(build))
        self.assertEqual(self.should_run(), 3, "only the jars count")
        build["files"]["jars/iceberg.jar"] = "4" * 64
        self.build.write_text(json.dumps(build))
        self.assertEqual(self.should_run(), 0)

    def test_production_lacking_a_contract_column_runs_it_whatever_the_record_says(self):
        self.ran()
        capture = self.host / "var/lib/shape/capture.json"
        capture.parent.mkdir(parents=True)
        capture.write_text(json.dumps({"tables": {"silver.a": {"fields": [[1, "k", "string", True], [2, "v", "string", False]]}}}))
        self.assertEqual(self.should_run(), 3, "production holds every contract column")
        capture.write_text(json.dumps({"tables": {"silver.a": {"fields": [[1, "k", "string", True]]}}}))
        result = self.steps("should-run", self.release, "migrate")
        self.assertEqual(result.returncode, 0)
        self.assertIn("silver.a", result.stdout)

    def test_a_credential_like_name_is_never_read(self):
        contract = json.loads((self.release / "config/deploy-steps.contract.json").read_text())
        contract["steps"]["migrate"]["environment"]["names"].append("CATALOG_TOKEN")
        (self.release / "config/deploy-steps.contract.json").write_text(json.dumps(contract))
        result = self.steps("should-run", self.release, "migrate")
        self.assertEqual(result.returncode, 1)
        self.assertIn("never read", result.stderr)
        self.assertNotIn(SECRET, result.stdout + result.stderr)

    def test_forget_makes_the_next_deploy_run_it(self):
        self.ran()
        self.assertEqual(self.steps("forget", self.release, "migrate").returncode, 0)
        self.assertEqual(self.should_run(), 0)
        self.assertEqual(self.steps("forget", self.release, "migrate").returncode, 0, "forgetting twice is fine")

    def test_a_record_another_account_could_write_is_not_trusted(self):
        self.ran()
        (self.host / "var/lib/perfectory").chmod(0o777)
        self.assertEqual(self.should_run(), 1, "a parent others may write: run it, never skip")
        self.assertNotEqual(self.steps("ran", self.release, "migrate").returncode, 0)
        (self.host / "var/lib/perfectory").chmod(0o755)
        (self.host / "var/lib/perfectory/deploy-steps").chmod(0o755)
        self.assertEqual(self.should_run(), 1, "a record directory others may read or enter: run it")

    def test_a_step_without_host_settings_keeps_its_plain_fingerprint(self):
        self.ran("plain")
        self.env("CATALOG_URI=https://catalog.invalid/elsewhere\n")
        self.assertEqual(self.should_run("plain"), 3, self.last)

    def test_a_missing_input_or_contract_never_skips(self):
        self.ran()
        (self.release / "job.py").unlink()
        self.assertNotIn(self.should_run(), (0, 3))
        shutil.rmtree(self.release / "config")
        self.assertNotIn(self.should_run(), (0, 3))

    def test_a_step_that_always_runs_runs_and_says_when_it_is_over_its_budget(self):
        self.assertEqual(self.should_run("chain"), 0)
        within = self.steps("budget", self.release, "chain", 60)
        self.assertEqual((within.returncode, within.stderr), (0, ""))
        over = self.steps("budget", self.release, "chain", 61)
        self.assertEqual(over.returncode, 0)
        self.assertIn("chain took 61 s, over its budget of 60 s", over.stderr)


if __name__ == "__main__":
    unittest.main()
