"""Which DataHub services the release recreates (scripts/deploy/datahub_changes.py, foundation-release.sh datahub)."""

from __future__ import annotations

import pathlib
import re
import sys
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "scripts/deploy"))

import datahub_changes  # noqa: E402

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
        # docker ps -a lists the exited system-update with its label; same hash, so it is left alone.
        self.assertNotIn("system-update-quickstart", datahub_changes.changed_services(CONFIG, CONFIG.replace("ccc", "x")))


class TheReleaseStep(unittest.TestCase):
    release = (PLATFORM / "scripts/deploy/foundation-release.sh").read_text(encoding="utf-8")

    def body(self):
        return re.search(r"(?ms)^apply_datahub\(\) \{\n(.*?)^\}", self.release).group(1)

    def test_it_compares_composes_own_hashes_and_recreates_only_those_without_their_dependencies(self):
        body = self.body()
        self.assertIn("config --hash '*'", body)
        self.assertIn('{{.Label "com.docker.compose.config-hash"}}', body)
        self.assertIn("datahub_changes.py", body)
        self.assertRegex(body, r'up -d --no-deps \$\{changed\}')
        self.assertIn("return 0", body, "nothing changed: nothing is touched")

    def test_the_release_and_the_deploy_name_the_step(self):
        self.assertRegex(self.release, r'(?m)^  datahub\)\n.*\n    apply_datahub$')
        deploy = (PLATFORM / "scripts/deploy/foundation-deploy.sh").read_text(encoding="utf-8")
        self.assertGreater(deploy.index('"${release}" datahub'), deploy.index("phase=done"),
                           "after the DAGs are unpaused, so a catalog failure holds no job")


if __name__ == "__main__":
    unittest.main()
