"""The one-off that removes the two hand-started Martin containers (scripts/ops/retire-hand-started-martin.sh).

2026-11-18: martin-static and martin-complex, started with `docker run` in September 2026, were
unhealthy for four weeks (their health check asks port 3000; they listen on 3111 and 3110) and served
nothing: no connection, and every served tile unit addressed to tiles.perfectory.io. Root ADR-0112 §10
and ADR-0343 §5 decided to take them down. These tests pin that only that container is removed: one
Compose declares, one of another image, or one with a connection is kept and the line says why.
"""

import os
import pathlib
import shutil
import stat
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts/ops/retire-hand-started-martin.sh"

DOCKER = """#!/usr/bin/env bash
# A stub docker: containers are files under $STUB/containers/<name> holding "<project>|<image>".
set -u
if [[ "$1 $2" == "container inspect" ]]; then
  if [[ "$3" == -f ]]; then
    IFS='|' read -r project image < "$STUB/containers/$5"
    case "$4" in *compose.project*) echo "$project" ;; *Image*) echo "$image" ;; esac
    exit 0
  fi
  [[ -f "$STUB/containers/$3" ]]
  exit
fi
if [[ "$1" == rm ]]; then
  rm "$STUB/containers/$3" && echo "$3" >> "$STUB/removed"
  exit
fi
exit 99
"""

SS = """#!/usr/bin/env bash
# A stub ss: $STUB/connections/<port> holds one line per established connection.
for arg in "$@"; do
  if [[ "$arg" =~ sport\\ =\\ :([0-9]+) ]]; then cat "$STUB/connections/${BASH_REMATCH[1]}" 2>/dev/null; fi
done
exit 0
"""

MARTIN = "ghcr.io/maplibre/martin:1.12.0"


@unittest.skipUnless(shutil.which("bash") and os.name == "posix", "the script is bash on the host")
class RetireHandStartedMartin(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.stub = pathlib.Path(self.tmp.name)
        (self.stub / "bin").mkdir()
        (self.stub / "containers").mkdir()
        (self.stub / "connections").mkdir()
        for name, text in (("docker", DOCKER), ("ss", SS)):
            path = self.stub / "bin" / name
            path.write_text(text, encoding="utf-8")
            path.chmod(path.stat().st_mode | stat.S_IEXEC)

    def tearDown(self):
        self.tmp.cleanup()

    def container(self, name, project="", image=MARTIN):
        (self.stub / "containers" / name).write_text(f"{project}|{image}\n", encoding="utf-8")

    def run_script(self):
        env = dict(os.environ, STUB=str(self.stub), PATH=f"{self.stub / 'bin'}:{os.environ['PATH']}")
        return subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, env=env, check=False)

    def removed(self):
        path = self.stub / "removed"
        return path.read_text(encoding="utf-8").split() if path.exists() else []

    def test_the_two_hand_started_containers_with_no_connection_are_removed(self):
        self.container("martin-static")
        self.container("martin-complex")
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(sorted(self.removed()), ["martin-complex", "martin-static"])
        self.assertIn("martin-static: removed (no connection on :3111)", result.stdout)

    def test_a_declared_another_image_or_a_connected_container_is_kept_and_gone_is_said(self):
        self.container("martin-static", project="foundation-platform-tiles")
        self.container("martin-complex")
        (self.stub / "connections" / "3110").write_text("ESTAB 0 0 127.0.0.1:3110 127.0.0.1:5555\n", encoding="utf-8")
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.removed(), [])
        self.assertIn("martin-static: kept, it belongs to Compose project foundation-platform-tiles", result.stdout)
        self.assertIn("martin-complex: kept, 1 established connection(s) on :3110", result.stdout)

        (self.stub / "containers" / "martin-static").unlink()
        self.container("martin-complex", image="nginx:1")
        result = self.run_script()
        self.assertIn("martin-static: gone", result.stdout)
        self.assertIn("martin-complex: kept, its image is nginx:1, not Martin", result.stdout)


if __name__ == "__main__":
    unittest.main()
