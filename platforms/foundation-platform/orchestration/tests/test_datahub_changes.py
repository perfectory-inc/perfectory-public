"""Which DataHub services the release recreates (scripts/deploy/datahub_changes.py, foundation-release.sh datahub)."""

from __future__ import annotations

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
sys.path.insert(0, str(PLATFORM / "scripts/deploy"))

import datahub_changes  # noqa: E402
import datahub_state  # noqa: E402

CONFIG = """system-update-quickstart aaa
datahub-gms-quickstart bbb
frontend-quickstart ccc
"""


class ChangedServices(unittest.TestCase):
    def test_nothing_changed_touches_nothing(self):
        self.assertEqual(datahub_changes.changed_services(CONFIG, CONFIG), [])

    def test_only_the_service_whose_config_changed_is_recreated(self):
        running = CONFIG.replace("bbb", "old")
        self.assertEqual(datahub_changes.changed_services(CONFIG, running), ["datahub-gms-quickstart"])

    def test_a_service_with_no_container_is_started(self):
        running = "system-update-quickstart aaa\ndatahub-gms-quickstart bbb\n"
        self.assertEqual(datahub_changes.changed_services(CONFIG, running), ["frontend-quickstart"])

    def test_an_exited_one_shot_with_its_config_unchanged_is_not_run_again(self):
        # docker ps -a lists the exited system-update with its label; same hash, so it is not named
        # (compose still runs it first when a service that depends on it is recreated).
        self.assertNotIn("system-update-quickstart", datahub_changes.changed_services(CONFIG, CONFIG.replace("ccc", "x")))


class TheReleaseStep(unittest.TestCase):
    release = (PLATFORM / "scripts/deploy/foundation-release.sh").read_text(encoding="utf-8")

    def body(self):
        return re.search(r"(?ms)^apply_datahub\(\) \{\n(.*?)^\}", self.release).group(1)

    def test_it_compares_composes_own_hashes_and_recreates_only_those(self):
        body = self.body()
        self.assertIn("config --hash '*'", body)
        self.assertIn('{{.Label "com.docker.compose.config-hash"}}', body)
        self.assertIn("datahub_changes.py", body)
        self.assertRegex(body, r'up -d \$\{changed\}')
        self.assertNotIn("--no-deps", body, "compose owns the order (the-deploy-does-not-restate-the-compose-chain)")
        self.assertIn("return 0", body, "nothing changed: nothing is touched")

    def test_the_release_and_the_deploy_name_the_step(self):
        self.assertRegex(self.release, r'(?m)^  datahub\)\n.*\n    apply_datahub$')
        deploy = (PLATFORM / "scripts/deploy/foundation-deploy.sh").read_text(encoding="utf-8")
        self.assertGreater(deploy.index('"${release}" datahub'), deploy.index("phase=done"),
                           "after the DAGs are unpaused, so a catalog failure holds no job")


class TheStateIsOneFact(unittest.TestCase):
    """2026-10-16: the live state sat in DATAHUB_RUNTIME_DIR=/home/perfectory/datahub-state and root,
    using the script default, found no runtime.env. The path is the contract's, and nothing overrides it."""

    def test_the_runtime_reads_the_contract_and_no_variable(self):
        runtime = (PLATFORM / "scripts/deploy/datahub-runtime.sh").read_text(encoding="utf-8")
        self.assertNotIn("DATAHUB_RUNTIME_DIR", runtime)
        self.assertIn('runtime_dir="$(python3 -I -B "${script_dir}/datahub_state.py" dir)"', runtime)
        self.assertTrue(datahub_state.load()["state_dir"].startswith("/var/lib/"))

    def test_the_release_adopts_before_it_asks_compose(self):
        body = re.search(r"(?ms)^apply_datahub\(\) \{\n(.*?)^\}", TheReleaseStep.release).group(1)
        self.assertLess(body.index('datahub_state.py" adopt'), body.index("config --hash"))

    @unittest.skipUnless(shutil.which("docker"), "docker compose renders the config (present on the CI runner)")
    def test_the_rendered_frontend_mounts_user_props_from_the_contracts_state_dir(self):
        runtime = (PLATFORM / "scripts/deploy/datahub-runtime.sh").read_text(encoding="utf-8")
        self.assertIn('user_props="${runtime_dir}/user.props"', runtime)
        self.assertIn('DATAHUB_USER_PROPS_FILE="${user_props}" docker compose', runtime)
        compose = PLATFORM / "infra/datahub/compose.yml"
        user_props = f"{datahub_state.load()['state_dir']}/user.props"
        secrets = {name: "/placeholder" for name in set(re.findall(r"\$\{([A-Z0-9_]+):\?", compose.read_text(encoding="utf-8")))}
        rendered = json.loads(subprocess.run(
            ["docker", "compose", "-p", "datahub-config-check", "--project-directory", str(compose.parent),
             "-f", str(compose), "config", "--format", "json"],
            capture_output=True, text=True, check=True,
            env={**os.environ, **secrets, "DATAHUB_USER_PROPS_FILE": user_props}).stdout)
        mounts = {v["target"]: v["source"] for v in rendered["services"]["frontend-quickstart"]["volumes"]}
        self.assertEqual(mounts["/datahub-frontend/conf/user.props"], user_props)

    def adopt(self, state, old):
        return datahub_state.adopt(state, [old], owner=None)

    def test_missing_files_are_copied_once_and_the_old_directory_is_only_read(self):
        with tempfile.TemporaryDirectory() as directory:
            old, state = pathlib.Path(directory, "old"), pathlib.Path(directory, "state")
            old.mkdir()
            (old / "runtime.env").write_text("A=1\n")
            (old / "user.props").write_text("datahub:x\n")
            self.assertEqual(sorted(self.adopt(state, old)),
                             [f"runtime.env from {old}", f"user.props from {old}"])
            self.assertEqual((state / "runtime.env").read_text(), "A=1\n")
            self.assertEqual(sorted(p.name for p in old.iterdir()), ["runtime.env", "user.props"])
            if os.name == "posix":
                self.assertEqual((state / "runtime.env").stat().st_mode & 0o777, 0o600)
                self.assertEqual((state / "user.props").stat().st_mode & 0o777, 0o644)
                self.assertEqual(state.stat().st_mode & 0o777, 0o700)
            (old / "runtime.env").write_text("A=changed\n")
            self.assertEqual(self.adopt(state, old), [], "a second run copies nothing")
            self.assertEqual((state / "runtime.env").read_text(), "A=1\n", "the state it has is never replaced")

    def test_a_host_installed_at_the_state_dir_adopts_nothing(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory, "state")
            state.mkdir()
            (state / "runtime.env").write_text("A=1\n")
            (state / "user.props").write_text("datahub:x\n")
            self.assertEqual(self.adopt(state, pathlib.Path(directory, "absent")), [])


if __name__ == "__main__":
    unittest.main()
