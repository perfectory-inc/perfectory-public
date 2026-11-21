"""The release installer's rehearsal (scripts/deploy/foundation-release-test.sh) runs here, in the Python
suite, and not inside a Rust contract test.

It installs, activates, migrates and rolls back about 270 fake releases on a fake host, each step starting
several Python processes: 49-94 s on CI (measured 2026-11-19, eight runs). Inside the foundation-api
`deploy_contract` binary it had its own nextest slow limit; on 2026-11-20 21:16Z it ran past it in #1564's
merge group (passed on retry at 92.5 s) and the flaky-fails policy dropped that pull request from the
queue. A contract test reads files; a rehearsal that runs the installer is a suite of its own.
"""

import os
import pathlib
import shutil
import subprocess
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
REHEARSAL = PLATFORM / "scripts/deploy/foundation-release-test.sh"


@unittest.skipUnless(os.name == "posix" and shutil.which("bash"), "the rehearsal runs on Linux, as the host does")
class TheReleaseInstallerRehearsal(unittest.TestCase):
    def test_every_installer_path_passes_on_a_fake_host(self):
        # Ten times the slowest measured run: a hang fails by name instead of holding the suite.
        result = subprocess.run(["bash", str(REHEARSAL)], cwd=PLATFORM, capture_output=True, text=True,
                                timeout=900, check=False)
        self.assertEqual(result.returncode, 0, f"stdout:\n{result.stdout[-6000:]}\nstderr:\n{result.stderr[-6000:]}")
        self.assertIn("foundation-release-test=pass", result.stdout)


if __name__ == "__main__":
    unittest.main()
