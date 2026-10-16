"""What ages out on the host, and what never may (root ADR-0198).

The tmpfiles rule must clean the scratch tree and nothing that holds production state; the
journal must be capped; the daily prune must bound the build cache without touching tagged
images or volumes; and the release must install every file it ships under infra/host.
"""

from __future__ import annotations

import io
import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import sys
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

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


sys.path.insert(0, str(AREA / "scripts" / "ops"))
import host_retention  # noqa: E402

NOW = 1_800_000_000.0
OLD = NOW - 30 * DAY
PINNED_DIGEST = "a" * 64


def image(name: str, *, tags=(), digests=(), created=OLD, size=100) -> host_retention.Image:
    return host_retention.Image(f"sha256:{name}", tuple(tags), tuple(digests), created, size)


class TheImagePrune(unittest.TestCase):
    """Only images nothing needs go: not in use, not young, not the release's, not pinned."""

    def choose(self, *images):
        return [found.image_id for found in host_retention.removable(
            images, used={"sha256:in-use"}, referenced_tags={"postgres:17-alpine"},
            referenced_digests={PINNED_DIGEST}, now=NOW, min_age_days=7)]

    def test_each_reason_to_keep_keeps(self):
        chosen = self.choose(
            image("in-use", tags=["agent/old:1"]),
            image("young", tags=["agent/new:1"], created=NOW - DAY),
            image("release", tags=["foundation-tippecanoe:0123abcd"]),
            image("emergency", digests=[f"foundation-outbox-publisher@sha256:{'b' * 64}"]),
            image("pinned-tag", tags=["postgres:17-alpine"]),
            image("pinned-digest", digests=[f"apache/airflow@sha256:{PINNED_DIGEST}"]),
            image("leftover", tags=["perfectory-verify:task74"]),
            image("untagged", digests=[f"redis@sha256:{'c' * 64}"]),
        )
        self.assertEqual(chosen, ["sha256:leftover", "sha256:untagged"])

    def test_references_come_from_the_deployed_tree(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            (root / "infra").mkdir()
            (root / "infra" / "compose.yaml").write_text(
                f"image: postgres:17-alpine@sha256:{PINNED_DIGEST}\n", encoding="utf-8")
            (root / "tools.env").write_text("SPARK_IMAGE=apache/spark:3.5.6\n", encoding="utf-8")
            (root / "node_modules").mkdir()
            (root / "node_modules" / "x.json").write_text('"busybox:1.36"', encoding="utf-8")
            tags, digests = host_retention.references(root)
        self.assertIn("postgres:17-alpine", tags)
        self.assertIn("apache/spark:3.5.6", tags)
        self.assertNotIn("busybox:1.36", tags)
        self.assertEqual(digests, {PINNED_DIGEST})

    def run_main(self, *arguments):
        calls = []
        inspected = [
            {"Id": "sha256:leftover", "RepoTags": ["agent/a:1", "agent/b:1"], "RepoDigests": [],
             "Created": "2026-01-01T00:00:00.123Z", "Size": 10},
            {"Id": "sha256:untagged", "RepoTags": [], "RepoDigests": [], "Created": "2026-01-01T00:00:00Z",
             "Size": 5},
        ]

        def runner(command):
            calls.append(list(command))
            if command[:3] == ["docker", "image", "ls"]:
                return "sha256:leftover\nsha256:untagged\n"
            if command[:3] == ["docker", "image", "inspect"]:
                return json.dumps(inspected)
            return ""

        with tempfile.TemporaryDirectory() as temporary, redirect_stdout(io.StringIO()) as out:
            code = host_retention.main(["--reference-root", temporary, *arguments], runner=runner)
        return code, calls, out.getvalue()

    def test_a_run_removes_by_every_tag_or_by_id_and_prunes_only_aged_cache(self):
        code, calls, out = self.run_main()
        self.assertEqual(code, 0)
        removals = [call[3] for call in calls if call[:3] == ["docker", "image", "rm"]]
        self.assertEqual(removals, ["agent/a:1", "agent/b:1", "sha256:untagged"])
        self.assertIn(["docker", "builder", "prune", "--force", "--filter", "until=168h"], calls)
        self.assertIn("host-retention-ok images=2 image_bytes=15", out)

    def test_a_dry_run_removes_nothing(self):
        code, calls, out = self.run_main("--dry-run")
        self.assertEqual(code, 0)
        self.assertFalse([call for call in calls if call[1:3] in (["image", "rm"], ["builder", "prune"])])
        self.assertIn("would-remove agent/a:1,agent/b:1", out)

    def test_without_the_pinned_list_nothing_is_judged(self):
        with redirect_stderr(io.StringIO()):
            code = host_retention.main(["--reference-root", "/nonexistent/reference"],
                                       runner=lambda command: self.fail(f"ran {command}"))
        self.assertEqual(code, 65)

    def test_it_only_removes_chosen_images_and_aged_cache(self):
        _, calls, _ = self.run_main()
        writes = [call for call in calls if call[1:3] not in (
            ["image", "ls"], ["image", "inspect"], ["ps", "--all"], ["container", "inspect"])]
        for call in writes:
            self.assertIn(call[1:3], (["image", "rm"], ["builder", "prune"]), call)
            self.assertNotIn("--all", call)
            self.assertNotIn("-a", call)

    def test_the_unit_runs_the_script_daily_with_a_writable_docker_config(self):
        unit = (AREA / "infra" / "systemd" / "foundation-host-retention.service").read_text()
        timer = (AREA / "infra" / "systemd" / "foundation-host-retention.timer").read_text()
        self.assertIn("scripts/ops/host_retention.py --reference-root /opt/perfectory-control/current", unit)
        self.assertIn("Environment=DOCKER_CONFIG=/tmp/", unit)
        self.assertIn("PrivateTmp=true", unit)
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


class TheImageStore(unittest.TestCase):
    """Free space is measured where the images are (root ADR-0241), not on the system disk."""

    def layout(self, directory: str, moved: bool) -> pathlib.Path:
        root = pathlib.Path(directory) / "data-containerd"
        if moved:
            root.mkdir()
        path = pathlib.Path(directory) / "layout.json"
        path.write_text(json.dumps({"containerd": {"root": str(root), "legacy_root": "/var/lib/containerd"}}))
        return path

    def test_a_moved_host_is_measured_on_the_data_disk(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(host_retention.image_store(self.layout(directory, True)),
                             pathlib.Path(directory) / "data-containerd")

    def test_a_host_not_moved_yet_is_measured_where_containerd_still_is(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(host_retention.image_store(self.layout(directory, False)),
                             pathlib.Path("/var/lib/containerd"))

    def test_a_path_not_made_yet_is_measured_on_its_parent(self):
        # A fixed answer, not a second live reading: free space moves between two calls (CI saw 4096
        # bytes on #585). What is under test is which path gets measured.
        asked = []

        def disk_usage(path):
            asked.append(pathlib.Path(path))
            return shutil._ntuple_diskusage(total=1000, used=400, free=600)

        with tempfile.TemporaryDirectory() as directory:
            missing = pathlib.Path(directory) / "not" / "made"
            with mock.patch.object(host_retention.shutil, "disk_usage", disk_usage):
                self.assertEqual(host_retention.free_bytes(missing), 600)
            self.assertEqual(asked, [pathlib.Path(directory)])


if __name__ == "__main__":
    unittest.main()
