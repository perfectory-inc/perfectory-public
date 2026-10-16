"""DataHub GMS writes no debug log files into its container layer (infra/datahub/compose.yml).

The GMS image's logback has two rolling debug appenders whose cap did not hold: on 2026-10-15 about
50 files of 100MB filled the system disk. Their level filters read `logging.appender.debug_file.level`
and `logging.appender.graphql_debug_file.level`; the rendered compose config must set both to OFF.
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import shutil
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
COMPOSE = PLATFORM / "infra" / "datahub" / "compose.yml"
OFF = {"logging.appender.debug_file.level": "OFF", "logging.appender.graphql_debug_file.level": "OFF"}


def gms_environment(compose: pathlib.Path) -> dict:
    """The GMS service's environment as `docker compose config` renders it."""
    text = compose.read_text(encoding="utf-8")
    # An absolute path, because one of them is a bind source.
    placeholders = {name: "/placeholder" for name in set(re.findall(r"\$\{([A-Z0-9_]+):\?", text))}
    rendered = subprocess.run(
        ["docker", "compose", "-p", "datahub-config-check", "--project-directory", str(COMPOSE.parent),
         "-f", str(compose), "config", "--format", "json"],
        capture_output=True, text=True, env={**os.environ, **placeholders}, check=True)
    return json.loads(rendered.stdout)["services"]["datahub-gms-quickstart"]["environment"]


def debug_logs_left_on(environment: dict) -> list[str]:
    return sorted(name for name, value in OFF.items() if environment.get(name) != value)


@unittest.skipUnless(shutil.which("docker"), "docker compose renders the config (present on the CI runner)")
class GmsDebugLogs(unittest.TestCase):
    def test_both_debug_appenders_are_off_in_the_rendered_config(self):
        self.assertEqual(debug_logs_left_on(gms_environment(COMPOSE)), [])

    def test_a_compose_without_them_is_caught(self):
        text = COMPOSE.read_text(encoding="utf-8")
        planted = re.sub(r"(?m)^ +logging\.appender\.debug_file\.level: .*\n", "", text)
        self.assertNotEqual(planted, text)
        with tempfile.TemporaryDirectory() as directory:
            copy = pathlib.Path(directory) / "compose.yml"
            copy.write_text(planted, encoding="utf-8")
            self.assertEqual(debug_logs_left_on(gms_environment(copy)), ["logging.appender.debug_file.level"])


if __name__ == "__main__":
    unittest.main()
