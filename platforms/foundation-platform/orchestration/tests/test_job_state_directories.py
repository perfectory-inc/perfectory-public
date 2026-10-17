"""The release makes every job state directory from one generated file, before anything mounts it
(root ADR-0270 §1; the 2026-10-15 class, #625).

The file is rendered from orchestration/jobs.v1.json `state_directories` by the crate foundation-jobs,
whose test refuses a committed file that differs. These check the release script's side: it applies
the file as soon as `current` switches (before `migrate` starts a container that mounts one) and in
`timers`, and no state directory is made by hand anywhere else in it.
"""

import json
import pathlib
import re
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
RELEASE = PLATFORM / "scripts/deploy/foundation-release.sh"
JOBS = PLATFORM / "orchestration/jobs.v1.json"
CONF = PLATFORM / "infra/tmpfiles/foundation-job-state.conf"


def function_body(script, name):
    match = re.search(rf"^{name}\(\) \{{\n(.*?)^\}}\n", script, flags=re.MULTILINE | re.DOTALL)
    return match.group(1) if match else ""


def timers_branch(script):
    match = re.search(r"^  timers\)\n(.*?)^    ;;\n", script, flags=re.MULTILINE | re.DOTALL)
    return match.group(1) if match else ""


def applied_after_the_switch(script):
    body = function_body(script, "activate_release")
    switch = body.find('atomic_link "${next_target}" "${release_root}/current"')
    return switch >= 0 and body.find("prepare_job_state_directories", switch) > switch


def hand_made(script):
    directories = [entry["path"] for entry in json.loads(JOBS.read_text(encoding="utf-8"))["state_directories"]]
    found = []
    for line in script.splitlines():
        if re.search(r"\b(install -d|mkdir)\b", line):
            found.extend(path for path in directories if re.search(rf"{re.escape(path)}(\s|$|\\)", line))
    return found


class TheReleaseMakesTheJobStateDirectories(unittest.TestCase):
    def setUp(self):
        self.script = RELEASE.read_text(encoding="utf-8")

    def test_the_generated_file_is_what_the_release_applies(self):
        body = function_body(self.script, "prepare_job_state_directories")
        self.assertIn("infra/tmpfiles/foundation-job-state.conf", body)
        self.assertIn("systemd-tmpfiles --create /etc/tmpfiles.d/foundation-job-state.conf", body)
        self.assertTrue(CONF.is_file())

    def test_activate_makes_them_as_soon_as_current_switches(self):
        self.assertTrue(applied_after_the_switch(self.script))
        planted = self.script.replace(
            'atomic_link "${next_target}" "${release_root}/current"\n  prepare_job_state_directories\n',
            'atomic_link "${next_target}" "${release_root}/current"\n', 1)
        self.assertNotEqual(planted, self.script)
        self.assertFalse(applied_after_the_switch(planted))

    def test_timers_makes_them_too(self):
        self.assertIn("prepare_job_state_directories", timers_branch(self.script))

    def test_no_state_directory_is_made_by_hand(self):
        self.assertEqual(hand_made(self.script), [])
        planted = self.script.replace(
            "    prepare_job_state_directories\n",
            "    prepare_job_state_directories\n"
            "    install -d -o foundation-platform -g foundation-platform /data/foundation-platform/tile-bake\n", 1)
        self.assertNotEqual(planted, self.script)
        self.assertEqual(hand_made(planted), ["/data/foundation-platform/tile-bake"])


if __name__ == "__main__":
    unittest.main()
