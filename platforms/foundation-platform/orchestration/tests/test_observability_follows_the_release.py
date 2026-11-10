"""How the monitoring containers follow a release (compose.observability.yml, foundation-runtime.sh,
foundation-release.sh observability).

2026-11-10 (040dfe39): the deploy recreated Prometheus and Alertmanager inside `migrate`, as every
deploy did; Prometheus took 140 s from start to ready under a bake's IO, its health check called it
unhealthy at 122 s, and every DAG the deploy held stayed paused until the next deploy. 2026-11-08:
the same recreate of Alertmanager refused three unit recoveries' posts.
"""

from __future__ import annotations

import os
import pathlib
import re
import shutil
import subprocess
import tempfile
import unittest

# The whole deploy against stand-ins, as its own tests run it: its methods, not its tests.
import test_foundation_autodeploy as autodeploy

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
COMPOSE = PLATFORM / "compose.observability.yml"
RUNTIME = PLATFORM / "scripts/deploy/foundation-runtime.sh"
RELEASE = PLATFORM / "scripts/deploy/foundation-release.sh"

# Measured on the host on 2026-11-10 (docker inspect, docker logs, the deploy's journal).
STARTED_TO_READY_SECONDS = 140
REPLAY_SECONDS = 10.8
TSDB_BYTES, TSDB_CAP_BYTES = 237 * 2**20, 2**30


def service(name: str) -> str:
    """The service's block of compose.observability.yml, up to the next service."""
    text = COMPOSE.read_text(encoding="utf-8")
    start = text.index(f"\n  {name}:\n")
    following = re.search(r"\n  [a-z-]+:\n", text[start + 1:])
    return text[start: start + 1 + following.start()] if following else text[start:]


def seconds(value: str) -> int:
    return sum(int(n) * {"h": 3600, "m": 60, "s": 1}[unit] for n, unit in re.findall(r"(\d+)([hms])", value))


class TheHealthCheckWaitsOutAStart(unittest.TestCase):
    def test_prometheus_start_period_covers_the_measured_start_with_replay_at_the_tsdb_cap(self):
        block = service("prometheus")
        start = seconds(re.search(r"start_period: (\S+)", block).group(1))
        self.assertIn("--storage.tsdb.retention.size=1GB", block, "the cap the budget is computed from")
        # Replay grows with the TSDB, which retention caps; the rest of the start is the host's.
        budget = STARTED_TO_READY_SECONDS - REPLAY_SECONDS + REPLAY_SECONDS * TSDB_CAP_BYTES / TSDB_BYTES
        self.assertGreaterEqual(start, budget * 1.5)
        # Inside the period the probe runs often, so a ready Prometheus is healthy within seconds.
        self.assertLessEqual(seconds(re.search(r"start_interval: (\S+)", block).group(1)), 5)

    def test_the_incident_start_would_have_passed(self):
        for name in ("prometheus", "alertmanager"):
            with self.subTest(name):
                block = service(name)
                start = seconds(re.search(r"start_period: (\S+)", block).group(1))
                interval = seconds(re.search(r"\n      interval: (\S+)", block).group(1))
                retries = int(re.search(r"retries: (\d+)", block).group(1))
                self.assertGreater(start + interval * retries, STARTED_TO_READY_SECONDS)

    def test_prometheus_is_given_time_to_write_its_head_out(self):
        # The default 10 s and a SIGKILL left its m-mapped chunks out of order on 2026-11-10.
        self.assertGreaterEqual(seconds(re.search(r"stop_grace_period: (\S+)", service("prometheus")).group(1)), 60)


class TheyAreRecreatedOnlyWhenTheirFilesChange(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="observability-")
        self.addCleanup(temp.cleanup)
        self.root = pathlib.Path(temp.name)
        (self.root / "scripts/deploy").mkdir(parents=True)
        shutil.copy(RUNTIME, self.root / "scripts/deploy/foundation-runtime.sh")
        shutil.copytree(PLATFORM / "infra/observability", self.root / "infra/observability")
        (self.root / "recovery.env").write_text("")
        # docker: `network inspect` answers that the network exists; `compose` prints the digests.
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        (bin_dir / "docker").write_text(
            '#!/usr/bin/env bash\n[[ "$1" != network ]] || exit 0\n'
            'echo "$FOUNDATION_PROMETHEUS_CONFIG_DIGEST $FOUNDATION_ALERTMANAGER_CONFIG_DIGEST"\n')
        (bin_dir / "docker").chmod(0o755)
        self.env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                    "FOUNDATION_PLATFORM_ENV_FILE": str(self.root / "recovery.env")}

    def digests(self) -> list[str]:
        result = subprocess.run(["bash", str(self.root / "scripts/deploy/foundation-runtime.sh"), "ps"],
                                env=self.env, capture_output=True, text=True, check=True, timeout=60)
        return result.stdout.split()

    def test_the_compose_labels_carry_the_digests_the_wrapper_computes(self):
        self.assertIn("perfectory.config-digest: ${FOUNDATION_PROMETHEUS_CONFIG_DIGEST:", service("prometheus"))
        self.assertIn("perfectory.config-digest: ${FOUNDATION_ALERTMANAGER_CONFIG_DIGEST:", service("alertmanager"))

    def test_a_release_that_changes_neither_keeps_both_digests(self):
        before = self.digests()
        self.assertEqual(len(before), 2)
        self.assertEqual(self.digests(), before)
        # A rule test is promtool's, not the server's.
        test = next((self.root / "infra/observability/prometheus").glob("*.rules.test.yml"))
        test.write_text(test.read_text(encoding="utf-8") + "\n# edited\n", encoding="utf-8")
        self.assertEqual(self.digests(), before)

    def test_a_changed_rule_moves_only_prometheus_and_a_changed_route_only_alertmanager(self):
        prometheus, alertmanager = self.digests()
        rules = self.root / "infra/observability/prometheus/foundation-api.rules.yml"
        rules.write_text(rules.read_text(encoding="utf-8") + "\n# edited\n", encoding="utf-8")
        moved_prometheus, same_alertmanager = self.digests()
        self.assertNotEqual(moved_prometheus, prometheus)
        self.assertEqual(same_alertmanager, alertmanager)
        routes = self.root / "infra/observability/alertmanager/alertmanager.yml"
        routes.write_text(routes.read_text(encoding="utf-8") + "\n# edited\n", encoding="utf-8")
        self.assertEqual(self.digests()[0], moved_prometheus)
        self.assertNotEqual(self.digests()[1], alertmanager)

    def test_the_release_does_not_force_a_recreate_and_migrate_leaves_monitoring_alone(self):
        text = RELEASE.read_text(encoding="utf-8")
        commands = [line for line in text.splitlines() if not line.lstrip().startswith("#")]
        self.assertEqual([line for line in commands if "--force-recreate" in line], [])
        body = text[text.index("apply_observability() {"):]
        body = body[: body.index("\n}\n")]
        self.assertIn("up -d --wait alertmanager prometheus node-exporter cadvisor", body)
        migrate = text[text.index("migrate_runtime() {"):]
        self.assertNotIn("prometheus", migrate[: migrate.index("\n}\n")])


class AFailedObservabilityStepHoldsNoDag(unittest.TestCase):
    setUp = autodeploy.TheWholeDeploy.setUp
    deploy = autodeploy.TheWholeDeploy.deploy
    dags = autodeploy.TheWholeDeploy.dags
    released = autodeploy.TheWholeDeploy.released
    commit_tells_what_it_changes = autodeploy.TheWholeDeploy.commit_tells_what_it_changes
    ENABLED, DISABLED = autodeploy.TheWholeDeploy.ENABLED, autodeploy.TheWholeDeploy.DISABLED

    def test_the_2026_11_10_prometheus_that_outlived_its_health_check_leaves_no_dag_paused(self):
        # 040dfe39 replayed: the step that brings the monitoring containers up fails, with a table
        # changed so the deploy holds its writers. It runs after the schedule is back, so the deploy
        # fails and alerts and no DAG stays paused for it.
        self.commit_tells_what_it_changes("silver.building_register_floors")
        result = self.deploy(FIXTURE_FAIL_AT="observability")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.released()[-2:], ["status", "observability"], "the catalog is not reached")
        self.assertEqual(self.dags(), {**{job: "unpause" for job in self.ENABLED},
                                       **{job: "pause" for job in self.DISABLED}})
        self.assertNotIn("STAY PAUSED", result.stderr)
        self.assertNotIn("DONE: production runs", result.stdout)
        self.assertLess(result.stdout.index("7. unpause the enabled DAGs"),
                        result.stdout.index("the monitoring containers follow the release"))


if __name__ == "__main__":
    unittest.main()
