"""A job's cleanup runs from the release its run started with (root ADR-0270 §3, deploy D1).

The deploy is to stop waiting for running jobs, so a run may outlive a switch of `current`. Its
ExecStopPost then must not run the new release's cleanup against the old run: the run records its
release (admitted-writer-runtime.sh, in the unit's RuntimeDirectory) and
scripts/ops/pinned-cleanup.sh runs that release's `<script> cleanup`.
"""

import json
import os
import pathlib
import re
import shutil
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
JOBS = PLATFORM / "orchestration/jobs.v1.json"
UNITS = PLATFORM / "infra/systemd"
CURRENT_OPS = "/opt/foundation-platform/current/scripts/ops/"
PINNED = CURRENT_OPS + "pinned-cleanup.sh"


def unit_file(service):
    return UNITS / re.sub(r"@[^.]+\.service$", "@.service", service)


def cleanup_problems(read=lambda path: path.read_text(encoding="utf-8")):
    problems = []
    services = {job["systemd_service"] for job in json.loads(JOBS.read_text(encoding="utf-8"))["jobs"]}
    for path in sorted({unit_file(service) for service in services}):
        unit = read(path)
        for line in re.findall(r"^ExecStopPost=(.*)$", unit, flags=re.MULTILINE):
            if line.startswith(CURRENT_OPS) and not line.startswith(PINNED + " "):
                problems.append(f"{path.name}: ExecStopPost runs {line} from `current`, not the run's release")
            if line.startswith(PINNED + " ") and not re.search(r"^RuntimeDirectory=\S", unit, flags=re.MULTILINE):
                problems.append(f"{path.name}: pinned-cleanup.sh needs the unit's RuntimeDirectory")
    return problems


CLEANUP_RS = PLATFORM / "crates/publisher/foundation-publisher-silver/src/remote_lakehouse_job/invocation_cleanup.rs"
# The project name as a script passes it (`-p "…"`, lakehouse_spark_run "…", a …_PROJECT="…").
PER_RUN_PROJECT = re.compile(r'"(foundation-[a-z-]+)-\$\{INVOCATION_ID\}"')


def per_run_project_problems(read=lambda path: path.read_text(encoding="utf-8")):
    """Every Compose project a job script names after its systemd invocation is one the publisher's
    invocation cleanup removes (its containers and its network), and every job unit running that
    script has it as its ExecStopPost. 2026-10-20..28: ten per-run networks were left behind, of the
    31 Docker's address pools hold; a project nothing cleans would leave one on every run."""
    cleaned = set(re.findall(r'=> "(foundation-[a-z-]+)",', read(CLEANUP_RS)))
    problems, scripts = [], set()
    for script in sorted((PLATFORM / "scripts/ops").glob("*.sh")):
        for prefix in PER_RUN_PROJECT.findall(read(script)):
            scripts.add(script.name)
            if prefix not in cleaned:
                problems.append(f"{script.name}: project {prefix}-<invocation> is not one invocation_cleanup.rs removes")
    services = {job["systemd_service"] for job in json.loads(JOBS.read_text(encoding="utf-8"))["jobs"]}
    for path in sorted({unit_file(service) for service in services}):
        unit = read(path)
        for script in re.findall(r"^ExecStart=\S*/scripts/ops/(\S+\.sh)", unit, flags=re.MULTILINE):
            if script in scripts and f"ExecStopPost={PINNED} {script}" not in unit:
                problems.append(f"{path.name}: runs {script}, whose per-run Compose project only its cleanup removes, "
                                f"without ExecStopPost={PINNED} {script}")
    return problems, scripts


class EveryPerRunComposeProjectIsCleanedUp(unittest.TestCase):
    def test_every_per_run_project_has_its_cleanup(self):
        problems, scripts = per_run_project_problems()
        self.assertEqual(problems, [])
        self.assertTrue({"gold-panel-rebuild.sh", "lakehouse-maintenance.sh", "parcel-tile-bake.sh"} <= scripts, scripts)

    def test_a_unit_without_the_cleanup_or_an_unknown_prefix_is_refused(self):
        unit = UNITS / "foundation-gold-unit-price-rebuild.service"
        stripped = re.sub(r"(?m)^ExecStopPost=.*\n", "", unit.read_text(encoding="utf-8"))
        problems, _ = per_run_project_problems(lambda path: stripped if path == unit else path.read_text(encoding="utf-8"))
        self.assertEqual(len(problems), 1, problems)
        script = PLATFORM / "scripts/ops/lakehouse-maintenance.sh"
        renamed = script.read_text(encoding="utf-8").replace('"foundation-lakehouse-maintenance-${INVOCATION_ID}"',
                                                            '"foundation-other-${INVOCATION_ID}"')
        problems, _ = per_run_project_problems(lambda path: renamed if path == script else path.read_text(encoding="utf-8"))
        self.assertEqual(len(problems), 1, problems)


class EveryJobCleansUpWithItsOwnRelease(unittest.TestCase):
    def test_no_job_unit_cleans_up_through_current(self):
        self.assertEqual(cleanup_problems(), [])

    def test_a_cleanup_through_current_is_refused(self):
        unit = UNITS / "foundation-lakehouse-maintenance.service"
        planted = unit.read_text(encoding="utf-8").replace(
            PINNED + " lakehouse-maintenance.sh", CURRENT_OPS + "lakehouse-maintenance.sh cleanup")
        self.assertNotEqual(planted, unit.read_text(encoding="utf-8"))
        problems = cleanup_problems(lambda path: planted if path == unit else path.read_text(encoding="utf-8"))
        self.assertEqual(len(problems), 1, problems)
        unpinned = unit.read_text(encoding="utf-8").replace("RuntimeDirectory=%N\n", "")
        problems = cleanup_problems(lambda path: unpinned if path == unit else path.read_text(encoding="utf-8"))
        self.assertTrue(any("RuntimeDirectory" in problem for problem in problems), problems)


@unittest.skipUnless(shutil.which("bash") and os.name == "posix", "bash on the Linux CI runner")
class PinnedCleanup(unittest.TestCase):
    OLD, NEW = "a" * 40, "b" * 40

    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="pinned-cleanup-")
        self.addCleanup(temp.cleanup)
        self.base = pathlib.Path(temp.name) / "opt/foundation-platform"
        for release in (self.OLD, self.NEW):
            ops = self.base / "releases" / release / "scripts/ops"
            ops.mkdir(parents=True)
            shutil.copy(PLATFORM / "scripts/ops/pinned-cleanup.sh", ops / "pinned-cleanup.sh")
            (ops / "probe.sh").write_text(f'#!/usr/bin/env bash\necho "cleanup by {release} $*"\n')
            for name in ("pinned-cleanup.sh", "probe.sh"):
                (ops / name).chmod(0o755)
        (self.base / "current").symlink_to(pathlib.Path("releases") / self.NEW)
        self.runtime = pathlib.Path(temp.name) / "run"
        self.runtime.mkdir()

    def cleanup(self, *args):
        return subprocess.run(["bash", str(self.base / "current/scripts/ops/pinned-cleanup.sh"), *args],
                              env={"PATH": os.environ["PATH"], "RUNTIME_DIRECTORY": str(self.runtime)},
                              capture_output=True, text=True, timeout=30)

    def test_a_run_that_outlived_a_switch_is_cleaned_up_by_its_own_release(self):
        (self.runtime / "release").write_text(f"{self.base.resolve()}/releases/{self.OLD}\n")
        result = self.cleanup("probe.sh")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, f"cleanup by {self.OLD} cleanup\n")

    def test_a_run_that_recorded_nothing_is_cleaned_up_by_current(self):
        result = self.cleanup("probe.sh")
        self.assertEqual(result.stdout, f"cleanup by {self.NEW} cleanup\n")

    def test_a_recorded_path_outside_the_releases_or_a_bad_script_name_is_refused(self):
        (self.runtime / "release").write_text("/tmp/elsewhere\n")
        self.assertEqual(self.cleanup("probe.sh").returncode, 65)
        (self.runtime / "release").unlink()
        self.assertEqual(self.cleanup("../probe.sh").returncode, 64)
        self.assertEqual(self.cleanup().returncode, 64)


if __name__ == "__main__":
    unittest.main()
