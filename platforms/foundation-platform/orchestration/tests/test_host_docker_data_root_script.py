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


class FirstSteps(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
