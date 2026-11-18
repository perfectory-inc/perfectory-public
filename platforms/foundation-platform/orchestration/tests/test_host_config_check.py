"""The host configuration a release installs applies on a root shaped like the host (root ADR-0555).

`scripts/deploy/host-config-check.sh` runs systemd-tmpfiles and systemd-analyze, the host's own tools
in the host's version, over `infra/tmpfiles`, `infra/host/tmpfiles.d` and `infra/systemd`. On
2026-11-11 15:34Z a state directory two levels into the operator's `/data/foundation-platform` made
systemd-tmpfiles exit 73 on the host and every deploy failed for 6.5 hours; nothing had run the
file through systemd-tmpfiles before production did.

The check needs Linux, systemd and root. On a CI runner it must run: a check that skips there is
the missing check again. Elsewhere (a laptop) it is skipped, and CI judges.
"""

import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
CHECK = PLATFORM / "scripts/deploy/host-config-check.sh"
LAYOUT = PLATFORM / "scripts/deploy/host-layout.measured"
# What the check reads; a copy of these is what a planted fault is written into.
READ = ("infra/tmpfiles", "infra/host/tmpfiles.d", "infra/sysusers", "infra/systemd")
IN_CI = os.environ.get("GITHUB_ACTIONS") == "true"


def runner() -> list[str] | None:
    """The command prefix that runs the check as root, or None where it cannot run."""
    if not sys.platform.startswith("linux") or not shutil.which("systemd-tmpfiles"):
        return None
    if os.geteuid() == 0:
        return ["bash"]
    if shutil.which("sudo") and subprocess.run(["sudo", "-n", "true"], capture_output=True).returncode == 0:
        return ["sudo", "-n", "bash"]
    return None


class HostConfigCheckTest(unittest.TestCase):
    def setUp(self) -> None:
        self.prefix = runner()
        if self.prefix is None:
            if IN_CI:
                self.fail("the CI runner cannot run host-config-check.sh (Linux, systemd-tmpfiles and root are needed)")
            self.skipTest("needs Linux, systemd-tmpfiles and root; CI runs it")

    def check(self, platform: pathlib.Path) -> subprocess.CompletedProcess:
        return subprocess.run([*self.prefix, str(CHECK), str(platform)], capture_output=True, text=True)

    def copy(self) -> pathlib.Path:
        root = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        for directory in READ:
            shutil.copytree(PLATFORM / directory, root / directory)
        (root / "scripts/deploy").mkdir(parents=True)
        shutil.copy(LAYOUT, root / "scripts/deploy/host-layout.measured")
        return root

    def test_the_committed_configuration_applies(self) -> None:
        result = self.check(PLATFORM)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("OK host-config-check", result.stdout)

    def test_the_directory_that_stopped_every_deploy_is_refused(self) -> None:
        root = self.copy()
        with (root / "infra/tmpfiles/foundation-job-state.conf").open("a", encoding="utf-8") as conf:
            conf.write("d /data/foundation-platform/intelligence-documents/lakehouse 0777 foundation-platform foundation-platform -\n")
        result = self.check(root)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("unsafe path transition", result.stderr)

    def test_a_unit_line_systemd_would_ignore_is_refused(self) -> None:
        root = self.copy()
        with (root / "infra/systemd/foundation-data-quality.service").open("a", encoding="utf-8") as unit:
            unit.write("\n[Service]\nMemoryMax=lots\n")
        result = self.check(root)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("Invalid memory limit", result.stderr)


if __name__ == "__main__":
    unittest.main()
