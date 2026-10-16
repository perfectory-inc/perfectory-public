"""The host's one answer to "which jobs are registered" and "is this unit running" (root ADR-0184).

scripts/deploy/job_units.py is asked by release admission, the deploy (through admission), the
autodeploy, the operator script and the scheduler's dispatcher. A stand-in systemctl answers its
one question; every other script is checked not to ask systemd the question itself.
"""

import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
REPOSITORY = PLATFORM.parents[1]
JOB_UNITS = PLATFORM / "scripts/deploy/job_units.py"
JOBS = PLATFORM / "orchestration/jobs.v1.json"

# Answers `list-units` from FAKE_UNITS ("unit=state" pairs, each listed when a pattern matches it),
# prints a status glyph before a failed unit as some systemd versions do, and fails under FAKE_DOWN.
FAKE_SYSTEMCTL = r'''#!/usr/bin/env python3
import fnmatch, json, os, sys
args = sys.argv[1:]
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps(args) + "\n")
if os.environ.get("FAKE_DOWN"):
    sys.exit("Failed to connect to bus")
units = dict(pair.split("=") for pair in os.environ.get("FAKE_UNITS", "").split(",") if pair)
patterns = [arg for arg in args[1:] if not arg.startswith("-")]
for unit, state in units.items():
    if any(fnmatch.fnmatch(unit, pattern) for pattern in patterns):
        print(("● " if state == "failed" else "") + f"{unit} loaded {state} x fixture unit")
'''


class TheOneDefinition(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="job-units-")
        self.addCleanup(temp.cleanup)
        self.root = pathlib.Path(temp.name)
        (self.root / "systemctl").write_text(FAKE_SYSTEMCTL)
        (self.root / "systemctl").chmod(0o755)
        self.log = self.root / "systemctl.log"

    def running(self, *patterns, units=None, **env):
        return subprocess.run(
            [sys.executable, "-I", "-B", str(JOB_UNITS), "running", *patterns],
            env={**os.environ, "PATH": f"{self.root}{os.pathsep}{os.environ['PATH']}", "FAKE_LOG": str(self.log),
                 "FAKE_UNITS": ",".join(f"{unit}={state}" for unit, state in (units or {}).items()), **env},
            capture_output=True, text=True, timeout=30)

    def test_every_state_but_inactive_and_failed_runs(self):
        # A oneshot is `activating` for its whole run and never `active` (2026-10-10).
        units = {f"foundation-{state}.service": state
                 for state in ("activating", "active", "deactivating", "reloading", "refreshing", "maintenance",
                               "inactive", "failed")}
        result = self.running("foundation-*", units=units)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), sorted(
            f"foundation-{state}.service {state}"
            for state in ("activating", "active", "deactivating", "reloading", "refreshing", "maintenance")))

    def test_it_asks_one_listing_of_services_by_name_or_pattern(self):
        result = self.running("foundation-a.service", "foundation-silver-refresh@*.service",
                              units={"foundation-a.service": "activating",
                                     "foundation-silver-refresh@x.service": "deactivating",
                                     "foundation-b.service": "active"})
        self.assertEqual(result.stdout.splitlines(),
                         ["foundation-a.service activating", "foundation-silver-refresh@x.service deactivating"])
        asked = [json.loads(line) for line in self.log.read_text().splitlines()]
        self.assertEqual(len(asked), 1)
        self.assertEqual(asked[0][0], "list-units")
        # --all: a unit is listed whatever its state, so the rule above decides, not systemd's filter.
        self.assertLessEqual({"--all", "--type=service", "--plain", "--no-legend", "--full"}, set(asked[0]))

    def test_nothing_running_is_an_empty_answer_and_no_answer_is_not(self):
        result = self.running("foundation-a.service", units={"foundation-a.service": "inactive"})
        self.assertEqual((result.returncode, result.stdout), (0, ""))
        result = self.running("foundation-a.service", FAKE_DOWN="1")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertIn("systemd cannot report", result.stderr)

    def test_the_registered_services_are_every_jobs_disabled_ones_too(self):
        # 2026-10-11: the deploy waited only for the enabled jobs and met a disabled lane running.
        result = subprocess.run([sys.executable, "-I", "-B", str(JOB_UNITS), "services", str(JOBS)],
                                capture_output=True, text=True, check=True)
        jobs = json.loads(JOBS.read_text(encoding="utf-8"))["jobs"]
        self.assertEqual(result.stdout.split(), sorted({job["systemd_service"] for job in jobs}))
        self.assertTrue(any(not job["enabled"] for job in jobs), "the registry has a disabled job to prove this on")


# Forms that ask systemd "does it run" without job_units.py. `systemctl is-active <unit>` without
# --quiet only prints the state for a person (the operator's `status`), so it is not one of them.
ASKS_SYSTEMD_ITSELF = re.compile(
    r"list-units|is-active\s+(--quiet|-q)\b|(-p|--property=?)\s*ActiveState|\bstate ActiveState\b"
    r"|[\"']is-active[\"']|[\"']ActiveState[\"']|[\"']--property=ActiveState[\"']")


def code_lines(path):
    """Lines that run: shell and Python comments are skipped, so prose may name what it forbids."""
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.lstrip().startswith("#"):
            yield number, line


class NothingElseAsks(unittest.TestCase):
    """Every host script that decides whether a unit runs goes through job_units.py."""

    SCANNED = (PLATFORM / "scripts", PLATFORM / "orchestration/dags", PLATFORM / "infra", REPOSITORY / "scripts/deploy")

    def offenders(self, roots):
        found = []
        for root in roots:
            for path in sorted(root.rglob("*")):
                if path.suffix not in (".sh", ".py", ".service", ".conf") or path == JOB_UNITS or not path.is_file():
                    continue
                found += [f"{path}:{number}: {line.strip()}" for number, line in code_lines(path)
                          if ASKS_SYSTEMD_ITSELF.search(line)]
        return found

    def test_no_script_asks_systemd_whether_a_unit_runs_on_its_own(self):
        self.assertEqual(self.offenders(self.SCANNED), [])

    def test_the_scan_finds_each_form_it_forbids(self):
        planted = tempfile.TemporaryDirectory(prefix="job-units-scan-")
        self.addCleanup(planted.cleanup)
        root = pathlib.Path(planted.name)
        forms = ['systemctl is-active --quiet "${unit}" && refuse',
                 'systemctl list-units --state=active,activating "${unit}"',
                 'state="$(systemctl show -p ActiveState --value "${unit}")"',
                 'while [[ "$(state ActiveState)" == activating ]]; do',
                 'subprocess.run(["/usr/bin/systemctl", "show", "--property=ActiveState", "--value", unit])']
        for index, form in enumerate(forms):
            (root / f"planted-{index}.sh").write_text(f"# prose may say list-units\n{form}\n")
        self.assertEqual(len(self.offenders([root])), len(forms))


class TheDeferredJobs(unittest.TestCase):
    """Root ADR-0260: where the runner records a job a release build kept from starting, and which."""

    def ask(self, *argv, directory):
        return subprocess.run([sys.executable, "-I", str(JOB_UNITS), *argv], capture_output=True, text=True,
                              env={**os.environ, "FOUNDATION_SCHEDULED_DEFERRED_DIR": str(directory)}, check=True)

    def test_the_runner_and_the_deploy_read_one_place(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(self.ask("deferred-dir", directory=directory).stdout.strip(), directory)
            self.assertEqual(self.ask("deferred", directory=pathlib.Path(directory) / "never-made").stdout.strip(), "")
            for name in ("map_edit_fold_complex", "outbox_publish", "not a job", ".hidden"):
                (pathlib.Path(directory) / name).write_text("0\n")
            self.assertEqual(self.ask("deferred", directory=directory).stdout.split(),
                             ["map_edit_fold_complex", "outbox_publish"])


if __name__ == "__main__":
    unittest.main()
