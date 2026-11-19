#!/usr/bin/env python3
"""scripts/ci/spark-packages.sh against a recorded docker: which resolutions it runs, in what order.

2026-11-19: Maven Central refused the runners, every Ivy resolution in CI failed with "module not
found", and the merge queue merged nothing for two and a half hours. The script resolves with no
network first, asks the network at most three times, and never leaves a directory that still needs it.
"""
import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent / "spark-packages.sh"

# Each spark-submit call takes the next word of the plan: `ok` writes a jar and succeeds, `fail` fails.
DOCKER = r"""#!/usr/bin/env bash
set -euo pipefail
network="" source="" submit=0
previous=""
for argument in "$@"; do
  case "$previous" in --network) network="$argument" ;; esac
  case "$argument" in
    type=bind,src=*,dst=/resolve) source="${argument#type=bind,src=}"; source="${source%,dst=/resolve}" ;;
    /opt/spark/bin/spark-submit) submit=1 ;;
  esac
  previous="$argument"
done
if ((!submit)); then
  exit 0
fi
echo "$network" >>"$FAKE_STATE/calls"
outcome="$(head -n 1 "$FAKE_STATE/plan")"
tail -n +2 "$FAKE_STATE/plan" >"$FAKE_STATE/plan.next" && mv "$FAKE_STATE/plan.next" "$FAKE_STATE/plan"
if [ "$outcome" != ok ]; then
  exit 1
fi
mkdir -p "$source/jars"
echo jar >"$source/jars/resolved.jar"
"""
SLEEP = '#!/usr/bin/env bash\necho "$1" >>"$FAKE_STATE/sleeps"\n'


class SparkPackages(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.state = self.root / "state"
        self.bin = self.root / "bin"
        self.ivy = self.root / "ivy"
        self.state.mkdir()
        self.bin.mkdir()
        for name, body in (("docker", DOCKER), ("sleep", SLEEP)):
            path = self.bin / name
            path.write_text(body, encoding="utf-8", newline="\n")
            path.chmod(path.stat().st_mode | stat.S_IEXEC)
        self.github_env = self.root / "github.env"
        self.github_env.write_text("", encoding="utf-8")

    def resolve(self, *plan: str) -> subprocess.CompletedProcess:
        (self.state / "plan").write_text("".join(f"{word}\n" for word in plan), encoding="utf-8", newline="\n")
        # POSIX spellings: bash reads a backslash in a Windows path as an escape.
        environment = {**os.environ, "PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}",
                       "FAKE_STATE": self.state.as_posix(), "GITHUB_ENV": self.github_env.as_posix()}
        return subprocess.run([shutil.which("bash") or "bash", SCRIPT.as_posix(), self.ivy.as_posix(), "spark@sha256:0", "group:artifact:1"],
                              env=environment, text=True, capture_output=True, check=False)

    def lines(self, name: str) -> list[str]:
        path = self.state / name
        return path.read_text(encoding="utf-8").split() if path.exists() else []

    def test_a_directory_that_holds_the_packages_resolves_with_no_network_and_asks_for_no_save(self):
        result = self.resolve("ok")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.lines("calls"), ["none"])
        self.assertEqual(self.github_env.read_text(encoding="utf-8"), "")

    def test_an_empty_directory_is_filled_from_the_network_proved_offline_and_asks_for_a_save(self):
        result = self.resolve("fail", "ok", "ok")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.lines("calls"), ["none", "bridge", "none"])
        self.assertEqual(self.github_env.read_text(encoding="utf-8"), "PERFECTORY_SPARK_PACKAGES_SAVE=1\n")

    def test_the_network_is_asked_three_times_with_a_longer_pause_each_time(self):
        result = self.resolve("fail", "fail", "fail", "ok", "ok")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.lines("calls"), ["none", "bridge", "bridge", "bridge", "none"])
        self.assertEqual(self.lines("sleeps"), ["30", "60"])

    def test_a_network_that_fails_three_times_fails_the_run_and_saves_nothing(self):
        result = self.resolve("fail", "fail", "fail", "fail", "ok")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(self.lines("calls"), ["none", "bridge", "bridge", "bridge"])
        self.assertEqual(self.github_env.read_text(encoding="utf-8"), "")
        self.assertIn("failed 3 times", result.stderr)

    def test_a_filled_directory_that_still_needs_the_network_fails_and_saves_nothing(self):
        result = self.resolve("fail", "ok", "fail")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(self.github_env.read_text(encoding="utf-8"), "")
        self.assertIn("does not resolve with no network", result.stderr)

    def test_a_jar_the_directory_held_before_does_not_survive(self):
        (self.ivy / "jars").mkdir(parents=True)
        (self.ivy / "jars" / "previous.jar").write_text("old", encoding="utf-8")
        result = self.resolve("ok")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(sorted(path.name for path in (self.ivy / "jars").iterdir()), ["resolved.jar"])


if __name__ == "__main__":
    unittest.main()
