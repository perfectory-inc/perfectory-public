"""The Docker move's first steps (scripts/ops/host-docker-data-root.sh, root ADR-0241), sourced with fakes.

The 2026-10-15 12:03 run as root stopped right after writing `healthy-before`: the healthy-container
list ended its loop with `[[ … ]] && echo`, which returns 1 when the last container is not healthy,
and under pipefail and `set -e` that ended the run. autodeploy.off had already been created and
the cleanup was not in place yet, so autodeploy stayed off. These run the script's own functions
with a fake `docker`, `id` and `sudo` on PATH; nothing touches the host.
"""

import json
import os
import pathlib
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts/ops/host-docker-data-root.sh"

FAKE_DOCKER = r'''#!/usr/bin/env bash
case "$1" in
  ps) printf 'api\nmartin\n' ;;
  inspect) [[ "${@: -1}" == api ]] && echo healthy || echo unhealthy ;;
  *) exit 0 ;;
esac
'''
FAKE_ID = '#!/usr/bin/env bash\n[[ "$1" == -u ]] && echo 0 || /usr/bin/id "$@"\n'
# Records how it was asked and runs the command after `-u <user> -H` as the caller.
FAKE_SUDO = '#!/usr/bin/env bash\nprintf "%s\\n" "$*" >>"$SUDO_LOG"\nshift 3\nexec "$@"\n'
# Stands in for scripts/deploy/airflow-runtime.sh: lists DAGs, or fails when told to.
FAKE_AIRFLOW_RUNTIME = r'''#!/usr/bin/env bash
printf '%s\n' "$*" >>"$AIRFLOW_LOG"
[[ -z "${AIRFLOW_FAILS:-}" ]] || exit 1
[[ -t 0 ]] && { echo "stdin is a terminal" >&2; exit 9; }
echo '[{"dag_id": "foundation_source_sweep", "is_paused": "False"}, {"dag_id": "foundation_x", "is_paused": "True"}]'
'''


class Fakes(unittest.TestCase):
    """The fakes and the sourced script; no tests of its own."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="docker-move-")
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        for name, body in (("docker", FAKE_DOCKER), ("id", FAKE_ID), ("sudo", FAKE_SUDO)):
            (self.bin / name).write_text(body, encoding="utf-8")
            (self.bin / name).chmod(0o755)
        self.runtime = self.root / "airflow-runtime.sh"
        self.runtime.write_text(FAKE_AIRFLOW_RUNTIME, encoding="utf-8")
        self.runtime.chmod(0o755)
        self.conf = self.root / "release-deploy.conf"
        self.conf.write_text("FOUNDATION_DEPLOYER=deployer\n", encoding="utf-8")
        self.state = self.root / "state"
        self.state.mkdir()
        self.off = self.root / "autodeploy.off"
        self.toml = self.root / "config.toml"
        self.toml.write_text('disabled_plugins = ["cri"]\n', encoding="utf-8")

    def run_bash(self, body, **env):
        prelude = (
            "set -euo pipefail\n"
            f'source "{SCRIPT.as_posix()}"\n'
            f'AIRFLOW_RUNTIME="{self.runtime.as_posix()}"\n'
            f'RELEASE_DEPLOY_CONF="{self.conf.as_posix()}"\n'
            f'AUTODEPLOY_OFF="{self.off.as_posix()}"\n'
            f'DAEMON_JSON="{(self.root / "daemon.json").as_posix()}"\n'
            f'CTRD_TOML="{self.toml.as_posix()}"\n'
        )
        environment = {**os.environ, "PATH": f"{self.bin}:{os.environ['PATH']}",
                       "SUDO_LOG": str(self.root / "sudo.log"), "AIRFLOW_LOG": str(self.root / "airflow.log"),
                       **env}
        return subprocess.run(["bash", "-c", prelude + body], env=environment, capture_output=True,
                              text=True, timeout=60)


class FirstSteps(Fakes):
    def test_an_unhealthy_last_container_does_not_stop_the_caller(self):
        result = self.run_bash('healthy_containers >"$OUT"; echo reached', OUT=str(self.root / "healthy"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("reached", result.stdout)
        self.assertEqual((self.root / "healthy").read_text().split(), ["api"])

    def test_as_root_airflow_runs_as_the_deployer_without_a_terminal(self):
        run = self.state / "run"
        run.mkdir()
        result = self.run_bash(f'record_state "{run.as_posix()}"; echo recorded')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("recorded", result.stdout)
        sudo = (self.root / "sudo.log").read_text().splitlines()
        self.assertEqual(len(sudo), 1)
        self.assertTrue(sudo[0].startswith("-u deployer -H bash "), sudo[0])
        self.assertIn(f"{self.runtime.as_posix()} exec -T airflow-scheduler airflow dags list -o json", sudo[0])
        self.assertEqual((run / "unpaused-dags").read_text().split(), ["foundation_source_sweep"])
        json.loads((run / "dags-before.json").read_text())

    def test_a_failure_while_recording_leaves_no_autodeploy_off(self):
        run = self.state / "run"
        run.mkdir()
        result = self.run_bash(f'begin_run "{run.as_posix()}"; echo past', AIRFLOW_FAILS="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("past", result.stdout)
        self.assertFalse(self.off.exists(), "a failed run left autodeploy off")
        self.assertTrue((run / "autodeploy-off-created").exists())

    def test_an_autodeploy_off_the_operator_set_stays(self):
        self.off.write_text("")
        run = self.state / "run"
        run.mkdir()
        result = self.run_bash(f'begin_run "{run.as_posix()}"', AIRFLOW_FAILS="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(self.off.exists(), "the run removed an autodeploy.off it did not create")


FAKE_SYSTEMCTL = r'''#!/usr/bin/env bash
printf '%s\n' "$*" >>"$SYSTEMCTL_LOG"
# Keeps systemd's one rule that matters here: a masked unit refuses every start, a dependency's too.
masks="${SYSTEMCTL_LOG}.masked"
case "$1" in
  mask) shift; [[ "$1" == --runtime ]] && shift; printf '%s\n' "$@" >>"$masks" ;;
  unmask) shift; [[ "$1" == --runtime ]] && shift; for u in "$@"; do sed -i "/^${u}$/d" "$masks" 2>/dev/null; done ;;
  start) shift; for u in "$@"; do if grep -qx "$u" "$masks" 2>/dev/null; then echo "Failed to start $u: Unit $u is masked." >&2; exit 1; fi; done ;;
esac
if [[ "$1" == list-timers ]]; then
  echo "Thu 2026-10-15 14:06:11 UTC 47s Thu 2026-10-15 14:05:11 UTC 12s ago foundation-autodeploy.timer foundation-autodeploy.service"
  echo "Fri 2026-10-16 01:45:00 UTC 11h - - foundation-identity-backup.timer foundation-identity-backup.service"
fi
'''
# Lists DAGs as AIRFLOW_LISTS says; refuses `unpause` until it was asked UNPAUSE_REFUSALS times.
FAKE_AIRFLOW_FLAKY = r'''#!/usr/bin/env bash
printf '%s\n' "$*" >>"$AIRFLOW_LOG"
case "$*" in
  *"dags list"*) cat "$AIRFLOW_LISTS" ;;
  *"dags unpause"*)
    n=$(( $(cat "$COUNTER" 2>/dev/null || echo 0) + 1 )); echo "$n" >"$COUNTER"
    (( n > ${UNPAUSE_REFUSALS:-0} )) || exit 1 ;;
esac
'''


class TheEngineStaysStopped(Fakes):
    """While the copy runs on a stopped engine, nothing may start it (2026-10-15 14:05:16)."""

    def setUp(self):
        super().setUp()
        (self.bin / "systemctl").write_text(FAKE_SYSTEMCTL, encoding="utf-8")
        (self.bin / "systemctl").chmod(0o755)
        self.calls = self.root / "systemctl.log"
        self.run_dir = self.state / "run"
        self.run_dir.mkdir()

    def systemctl(self, body):
        result = self.run_bash(body, SYSTEMCTL_LOG=str(self.calls))
        self.assertEqual(result.returncode, 0, result.stderr)
        return self.calls.read_text().splitlines()

    def test_the_timers_stop_then_the_engine_stops_and_is_masked(self):
        calls = self.systemctl(f'stop_engine "{self.run_dir.as_posix()}"')
        self.assertEqual(calls[1:], [
            "stop foundation-autodeploy.timer foundation-identity-backup.timer",
            "stop docker.socket docker.service containerd.service",
            "mask --runtime docker.socket docker.service containerd.service",
        ])
        self.assertEqual((self.run_dir / "timers-stopped").read_text().split(),
                         ["foundation-autodeploy.timer", "foundation-identity-backup.timer"])

    def test_a_start_from_outside_during_the_stop_is_refused_and_the_runs_own_start_is_not(self):
        """What the autodeploy timer's Requires=docker.service did at 14:05:16: start the engine."""
        result = self.run_bash(
            f'stop_engine "{self.run_dir.as_posix()}"\n'
            'if systemctl start docker.service; then echo OUTSIDE-STARTED; else echo OUTSIDE-REFUSED; fi\n'
            'start_engine; echo RUN-STARTED',
            SYSTEMCTL_LOG=str(self.calls))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("OUTSIDE-REFUSED", result.stdout)
        self.assertNotIn("OUTSIDE-STARTED", result.stdout)
        self.assertIn("RUN-STARTED", result.stdout)

    def test_an_engine_unit_running_before_the_stopped_copy_stops_the_run(self):
        fake = self.root / "job_units.py"
        fake.write_text("import os\nprint(os.environ.get('RUNNING', ''), end='')\n", encoding="utf-8")
        body = f'JOB_UNITS="{fake.as_posix()}"; if engine_is_down; then echo DOWN; else echo UP; fi'
        self.assertIn("DOWN", self.run_bash(body).stdout)
        refused = self.run_bash(body, RUNNING="docker.service activating\n")
        self.assertIn("UP", refused.stdout)
        self.assertIn("runs again although it was stopped", refused.stdout)

    def test_the_engine_is_unmasked_before_it_starts_and_the_timers_come_back(self):
        (self.run_dir / "timers-stopped").write_text("foundation-autodeploy.timer\n")
        calls = self.systemctl(f'start_engine; restart_timers "{self.run_dir.as_posix()}"')
        self.assertEqual(calls, [
            "unmask --runtime docker.socket docker.service containerd.service",
            "start containerd.service docker.socket docker.service",
            "start foundation-autodeploy.timer",
        ])


class TheUnpause(Fakes):
    def setUp(self):
        super().setUp()
        self.runtime.write_text(FAKE_AIRFLOW_FLAKY, encoding="utf-8")
        self.run_dir = self.state / "run"
        self.run_dir.mkdir()
        (self.run_dir / "unpaused-dags").write_text("foundation_source_sweep\nfoundation_data_quality\n")
        self.lists = self.root / "lists.json"

    def unpause(self, listed, refusals):
        self.lists.write_text(json.dumps(listed))
        return self.run_bash(f'UNPAUSE_RETRY_SECONDS=0; unpause_recorded "{self.run_dir.as_posix()}"',
                             AIRFLOW_LISTS=str(self.lists), COUNTER=str(self.root / "counter"),
                             UNPAUSE_REFUSALS=str(refusals))

    def test_a_refused_unpause_is_tried_again_and_checked_against_airflow(self):
        result = self.unpause([{"dag_id": "foundation_source_sweep", "is_paused": "False"},
                               {"dag_id": "foundation_data_quality", "is_paused": "False"}], refusals=2)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("attempt 1 failed", result.stdout)
        self.assertIn("lists all of them unpaused", result.stdout)
        self.assertFalse((self.run_dir / "unpause-failed").exists())

    def test_a_dag_airflow_still_lists_paused_is_named_and_fails_the_call(self):
        result = self.unpause([{"dag_id": "foundation_source_sweep", "is_paused": "True"},
                               {"dag_id": "foundation_data_quality", "is_paused": "False"}], refusals=0)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("STILL PAUSED", result.stdout)
        self.assertEqual((self.run_dir / "unpause-failed").read_text().split(), ["foundation_source_sweep"])


if __name__ == "__main__":
    unittest.main()
