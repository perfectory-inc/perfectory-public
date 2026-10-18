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


def applied_before_the_switch(script):
    body = function_body(script, "activate_release")
    switch = body.find('atomic_link "${next_target}" "${release_root}/current"')
    made = body.find('prepare_job_state_directories "$(release_path "${release_id}")"')
    return 0 <= made < switch


def hand_made(script):
    directories = [entry["path"] for entry in json.loads(JOBS.read_text(encoding="utf-8"))["state_directories"]]
    found = []
    for line in script.splitlines():
        if re.search(r"\b(install -d|mkdir)\b", line):
            found.extend(path for path in directories if re.search(rf"{re.escape(path)}(\s|$|\\)", line))
    return found


IVY_FROM_ITS_ENV_FILE = {"building-register-floor-cycle.sh"}


def ivy_setting(script):
    return (PLATFORM / "scripts/ops" / script).read_text(encoding="utf-8")


def ivy_cache_problems(replaced=None):
    replaced = replaced or {}
    problems = []
    for path in sorted((PLATFORM / "scripts/ops").glob("*.sh")):
        text = replaced.get(path.name, path.read_text(encoding="utf-8"))
        for line in text.splitlines():
            if "FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE=" in line and path.name not in IVY_FROM_ITS_ENV_FILE:
                if 'IVY_CACHE="${RELEASE_JARS_DIR}"' not in line or "IVY_MODE=ro" not in line:
                    problems.append(f"{path.name}: an Ivy cache other than the release's jars, read-only")
    for job in json.loads(JOBS.read_text(encoding="utf-8"))["jobs"]:
        problems.extend(f"{job['id']}: {state} is an Ivy cache" for state in job["state_paths"] if "ivy" in state)
    return problems


class TheReleaseMakesTheJobStateDirectories(unittest.TestCase):
    def setUp(self):
        self.script = RELEASE.read_text(encoding="utf-8")

    def test_the_generated_file_is_what_the_release_applies(self):
        body = function_body(self.script, "prepare_job_state_directories")
        self.assertIn("infra/tmpfiles/foundation-job-state.conf", body)
        self.assertIn("systemd-tmpfiles --create /etc/tmpfiles.d/foundation-job-state.conf", body)
        self.assertTrue(CONF.is_file())

    def test_activate_makes_them_from_the_new_release_before_current_switches(self):
        self.assertTrue(applied_before_the_switch(self.script))
        planted = self.script.replace(
            '  prepare_job_state_directories "$(release_path "${release_id}")"\n', '', 1)
        self.assertNotEqual(planted, self.script)
        self.assertFalse(applied_before_the_switch(planted))

    def test_the_spark_account_is_made_first_with_the_containers_uid(self):
        body = function_body(self.script, "prepare_job_state_directories")
        self.assertLess(body.index("systemd-sysusers"), body.index("systemd-tmpfiles --create"))
        self.assertIn('== "185:185" ]]', body)
        accounts = (PLATFORM / "infra/sysusers/foundation-spark.conf").read_text(encoding="utf-8")
        rule = next(line.split() for line in accounts.splitlines() if line.startswith("u "))
        self.assertEqual(rule[1:3], ["foundation-spark", "185:185"])
        # The group first: `u name uid:gid` does not make a missing group, and sysusers exits 0 anyway.
        group = next(line.split() for line in accounts.splitlines() if line.startswith("g "))
        self.assertEqual(group[1:3], ["foundation-spark", "185"])
        # The uid the Spark containers run as: compose.lakehouse.yml's default for every service.
        compose = (PLATFORM / "compose.lakehouse.yml").read_text(encoding="utf-8")
        users = set(re.findall(r'user: "\$\{FOUNDATION_PLATFORM_LAKEHOUSE_UID:-(\d+)\}:\$\{FOUNDATION_PLATFORM_LAKEHOUSE_GID:-(\d+)\}"', compose))
        self.assertEqual(users, {("185", "185")})

    def test_no_job_keeps_an_ivy_cache_of_its_own(self):
        # Every Spark run mounts its release's frozen jars read-only as its Ivy cache (root
        # ADR-0134), so no job state path is an Ivy cache: one left in state_paths only widened the
        # sandbox (lakehouse-ivy, until 2026-10-18). FLOOR names its cache in its own env file.
        self.assertEqual(ivy_cache_problems(), [])
        planted = {"map-edit-fold.sh": ivy_setting("map-edit-fold.sh").replace("IVY_MODE=ro", "IVY_MODE=rw")}
        self.assertEqual(len(ivy_cache_problems(planted)), 1)

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
