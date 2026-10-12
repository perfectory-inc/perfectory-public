"""What ages out on the host, and what never may (root ADR-0198).

The tmpfiles rule must clean the scratch tree and nothing that holds production state; the
journal must be capped; the daily prune must bound the build cache without touching tagged
images or volumes; and the release must install every file it ships under infra/host.
"""

from __future__ import annotations

import os
import pathlib
import shutil
import subprocess
import tempfile
import time
import unittest

AREA = pathlib.Path(__file__).resolve().parents[2]
HOST = AREA / "infra" / "host"
SCRATCH = "/data/foundation-platform/claude-scratch"
# Production state on the host. A rule may not name one of these, sit inside one, or contain one.
PROTECTED = (
    "/var/lib/foundation-platform",
    "/var/lib/foundation-scheduler",
    "/var/lib/foundation-gold-panel-rebuild",
    "/var/lib/foundation-platform/recovery",
    "/data/foundation-platform/by-pnu-bake",
    "/data/foundation-platform/lakehouse",
    "/data/foundation-platform/source-sweep",
    "/data/foundation-platform/silver-refresh",
    "/opt/foundation-platform",
    "/etc/foundation-platform",
)
DAY = 24 * 3600


def tmpfiles_rules() -> list[list[str]]:
    rules = []
    for conf in sorted((HOST / "tmpfiles.d").glob("*.conf")):
        for line in conf.read_text(encoding="utf-8").splitlines():
            if line.strip() and not line.lstrip().startswith("#"):
                rules.append(line.split())
    return rules


def age_days(age: str) -> float:
    value = age.split(":", 1)[-1]
    units = {"d": DAY, "h": 3600, "m": 60, "w": 7 * DAY}
    return int(value[:-1]) * units[value[-1]] / DAY


def within(path: str, root: str) -> bool:
    return path == root or path.startswith(root.rstrip("/") + "/")


class TheScratchRule(unittest.TestCase):
    def test_it_ages_out_only_the_scratch_tree_within_three_days(self):
        rules = tmpfiles_rules()
        self.assertEqual([rule[1] for rule in rules], [SCRATCH])
        kind, path, mode, user, group, age = rules[0]
        # `e` cleans an existing directory and never creates, chowns or removes it.
        self.assertEqual((kind, mode, user, group), ("e", "-", "-", "-"))
        self.assertTrue(age.startswith("mM:"), "age by modification time, not by the last read")
        self.assertLessEqual(age_days(age), 3)

    def test_no_rule_reaches_production_state(self):
        for rule in tmpfiles_rules():
            for protected in PROTECTED:
                with self.subTest(rule=rule[1], protected=protected):
                    self.assertFalse(within(rule[1], protected), "a rule inside production state")
                    self.assertFalse(within(protected, rule[1]), "a rule containing production state")

    @unittest.skipUnless(shutil.which("systemd-tmpfiles"), "systemd-tmpfiles runs on the Linux CI runner")
    def test_systemd_tmpfiles_removes_old_scratch_and_keeps_new(self):
        with tempfile.TemporaryDirectory(prefix="host-retention-") as temporary:
            root = pathlib.Path(temporary)
            scratch = root / "claude-scratch"
            (scratch / "old-run").mkdir(parents=True)
            old = scratch / "old-run" / "measurement.json"
            new = scratch / "today.json"
            old.write_text("{}")
            new.write_text("{}")
            stale = time.time() - 4 * DAY
            os.utime(old, (time.time(), stale))  # read today, written four days ago
            os.utime(scratch / "old-run", (stale, stale))
            conf = root / "scratch.conf"
            rule = (HOST / "tmpfiles.d" / "foundation-scratch.conf").read_text(encoding="utf-8")
            conf.write_text(rule.replace(SCRATCH, str(scratch)), encoding="utf-8")
            subprocess.run(["systemd-tmpfiles", "--clean", str(conf)], check=True)
            self.assertFalse(old.exists(), "a file not written for four days survived")
            self.assertTrue(new.exists(), "a file written today was removed")
            self.assertTrue(scratch.is_dir(), "the scratch directory itself was removed")


class TheJournalCap(unittest.TestCase):
    def test_the_journal_is_capped_with_free_space_kept(self):
        settings = {}
        for conf in (HOST / "journald.conf.d").glob("*.conf"):
            lines = conf.read_text(encoding="utf-8").splitlines()
            self.assertIn("[Journal]", lines)
            for line in lines:
                key, separator, value = line.partition("=")
                if separator and not line.startswith("#"):
                    settings[key.strip()] = value.strip()
        self.assertTrue(settings.get("SystemMaxUse", "").endswith("G"))
        self.assertLessEqual(int(settings["SystemMaxUse"][:-1]), 4)
        self.assertIn("SystemKeepFree", settings)


FAKE_DOCKER = """#!/bin/sh
printf '%s\\n' "$*" >>"$DOCKER_CALLS"
"""
FAKE_DF = """#!/bin/sh
printf 'Avail\\n%s\\n' "$(cat "$DF_ANSWER")"
"""


class TheDailyPrune(unittest.TestCase):
    script = AREA / "scripts" / "ops" / "host-retention.sh"

    def run_prune(self) -> list[str]:
        with tempfile.TemporaryDirectory(prefix="host-retention-") as temporary:
            root = pathlib.Path(temporary)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            for name, body in (("docker", FAKE_DOCKER), ("df", FAKE_DF)):
                (bin_dir / name).write_text(body, encoding="utf-8")
                (bin_dir / name).chmod(0o755)
            (root / "df").write_text("1073741824")
            env = dict(os.environ, PATH=f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                       DOCKER_CALLS=str(root / "calls"), DF_ANSWER=str(root / "df"))
            result = subprocess.run(["bash", str(self.script)], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("host-retention-ok", result.stdout)
            return (root / "calls").read_text().splitlines()

    @unittest.skipUnless(shutil.which("bash") and os.name != "nt", "the unit runs bash on the Linux host")
    def test_it_bounds_the_build_cache_and_removes_only_dangling_images(self):
        calls = self.run_prune()
        self.assertEqual(len(calls), 2)
        self.assertRegex(calls[0], r"^builder prune --force --max-used-space [0-9]+GB$")
        self.assertEqual(calls[1], "image prune --force")

    def test_it_never_removes_tagged_images_volumes_or_everything(self):
        text = self.script.read_text(encoding="utf-8")
        commands = [line for line in text.splitlines() if line.startswith("docker ")]
        for command in commands:
            self.assertNotRegex(command, r"--all\b|\s-a\b|system prune|volume prune|container prune")

    def test_the_unit_runs_the_script_daily(self):
        unit = (AREA / "infra" / "systemd" / "foundation-host-retention.service").read_text()
        timer = (AREA / "infra" / "systemd" / "foundation-host-retention.timer").read_text()
        self.assertIn("ExecStart=/opt/foundation-platform/current/scripts/ops/host-retention.sh\n", unit)
        self.assertIn("Unit=foundation-host-retention.service\n", timer)
        self.assertIn("OnCalendar=", timer)


class TheRelease(unittest.TestCase):
    release = (AREA / "scripts" / "deploy" / "foundation-release.sh").read_text(encoding="utf-8")

    def test_every_host_directory_the_release_ships_is_installed(self):
        directories = sorted(path.name for path in HOST.iterdir() if path.is_dir())
        self.assertEqual(directories, ["journald.conf.d", "tmpfiles.d"])
        for directory in directories:
            self.assertIn(f'"${{host}}"/{directory}/*.conf', self.release, directory)
        self.assertIn("    install_host_retention\n", self.release)

    def test_the_release_enables_the_daily_prune(self):
        self.assertIn("systemctl enable --now foundation-host-retention.timer", self.release)


if __name__ == "__main__":
    unittest.main()
