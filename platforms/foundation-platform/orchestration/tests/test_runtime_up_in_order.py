"""A refused migration leaves the running API serving (root ADR-0245), against real docker compose.

A small compose project has the runtime's shape: a database, a migrate one-shot, a finalize
one-shot and an API that waits for both (`service_completed_successfully`), as docker-compose.yml
chains postgres, bootstrap, migrate, grants, finalize and foundation-api. A release is a new value
of the API's configuration, so compose replaces the API container on `up`; a planted migration
exits 1.

- The deploy's order (scripts/deploy/runtime-up-in-order.sh, then `up -d --wait` of the API): the
  refused migration stops it and the previous API container still answers with the previous
  release.
- The order the deploy used before (`up -d --wait` of the API alone) with the same planted
  failure takes the API down. Kept as the control: if compose ever stopped doing that, this test
  would say so instead of passing for the wrong reason.
"""

import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest
import uuid

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
HELPER = PLATFORM / "scripts/deploy/runtime-up-in-order.sh"
# tools/technology-versions.contract.json pins it.
IMAGE = "busybox:1.36@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662"

COMPOSE = f"""
services:
  db:
    image: {IMAGE}
    command: ["sh", "-c", "sleep 3600"]
    healthcheck: {{test: ["CMD", "true"], interval: 1s, retries: 30}}
  migrate:
    image: {IMAGE}
    restart: "no"
    command: ["sh", "-c", "exit ${{MIGRATE_EXIT:-0}}"]
    depends_on:
      db:
        condition: service_healthy
  finalize:
    image: {IMAGE}
    restart: "no"
    command: ["true"]
    depends_on:
      migrate:
        condition: service_completed_successfully
  api:
    image: {IMAGE}
    restart: unless-stopped
    environment: {{RELEASE: "${{RELEASE:-none}}"}}
    command: ["sh", "-c", "mkdir -p /www && echo $$RELEASE > /www/index.html && exec httpd -f -p 8080 -h /www"]
    healthcheck: {{test: ["CMD", "wget", "-qO-", "http://127.0.0.1:8080/"], interval: 1s, retries: 30}}
    depends_on:
      db:
        condition: service_healthy
      finalize:
        condition: service_completed_successfully
"""


def docker_ready() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=30).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


@unittest.skipUnless(docker_ready(), "requires a Docker Engine with compose")
class RefusedMigration(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="runtime-order-")
        self.addCleanup(self.temp.cleanup)
        root = pathlib.Path(self.temp.name)
        (root / "compose.yml").write_text(COMPOSE, encoding="utf-8")
        self.project = f"runtime-order-{uuid.uuid4().hex[:8]}"
        self.runtime = root / "runtime.sh"
        self.runtime.write_text(
            f'#!/usr/bin/env bash\nexec docker compose -p {self.project} -f "{(root / "compose.yml").as_posix()}" "$@"\n',
            encoding="utf-8")
        self.runtime.chmod(0o755)
        self.addCleanup(self.compose, "down", "-v", "--remove-orphans", "--timeout", "1")

    def compose(self, *args, release="r1", migrate_exit=0):
        env = {**os.environ, "RELEASE": release, "MIGRATE_EXIT": str(migrate_exit)}
        return subprocess.run([str(self.runtime), *args], env=env, capture_output=True, text=True, timeout=300)

    def deploy(self, release, migrate_exit):
        """What foundation-release.sh `migrate` does: the chain, then the API, stopping at a failure."""
        env = {**os.environ, "RELEASE": release, "MIGRATE_EXIT": str(migrate_exit), "RUNTIME": str(self.runtime)}
        script = (f'set -e; source "{HELPER.as_posix()}"; runtime_chain_before "$RUNTIME" api; '
                  '"$RUNTIME" up -d --wait api')
        return subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True, timeout=300)

    def api(self):
        """(container id, running, what it answers) of the API container compose has now."""
        ids = self.compose("ps", "-a", "-q", "api").stdout.split()
        if not ids:
            return None, False, ""
        running = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", ids[0]],
                                 capture_output=True, text=True).stdout.strip() == "true"
        answer = subprocess.run(["docker", "exec", ids[0], "wget", "-qO-", "http://127.0.0.1:8080/"],
                                capture_output=True, text=True).stdout.strip() if running else ""
        return ids[0], running, answer

    def test_a_refused_migration_leaves_the_previous_api_answering(self):
        first = self.deploy("r1", 0)
        self.assertEqual(first.returncode, 0, first.stderr)
        before, running, answer = self.api()
        self.assertTrue(running)
        self.assertEqual(answer, "r1")

        refused = self.deploy("r2", 1)
        self.assertNotEqual(refused.returncode, 0, "a refused migration let the deploy go on")
        self.assertIn("keeps serving", refused.stderr)
        after, running, answer = self.api()
        self.assertEqual(after, before, "the API container was replaced although the migration was refused")
        self.assertTrue(running, "the API went down on a refused migration")
        self.assertEqual(answer, "r1")

        # Once the migration passes, the deploy replaces the API.
        recovered = self.deploy("r3", 0)
        self.assertEqual(recovered.returncode, 0, recovered.stderr)
        self.assertEqual(self.api()[2], "r3")

    def test_the_previous_order_took_the_api_down_on_the_same_failure(self):
        first = self.deploy("r1", 0)
        self.assertEqual(first.returncode, 0, first.stderr)
        old_order = self.compose("up", "-d", "--wait", "api", release="r2", migrate_exit=1)
        self.assertNotEqual(old_order.returncode, 0)
        _, running, answer = self.api()
        self.assertFalse(running and answer == "r1",
                         "the old order kept the API serving: this test no longer shows the 2026-10-14 failure")


if __name__ == "__main__":
    unittest.main()
