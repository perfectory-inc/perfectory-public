"""The job list builds the DAGs the scheduler runs (root ADR-0122); these pin what it may say."""

import copy
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import unittest

ORCHESTRATION = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ORCHESTRATION / "dags"))

import job_specs  # noqa: E402


def real_inputs():
    return (
        json.loads(job_specs.JOBS.read_text(encoding="utf-8")),
        json.loads(job_specs.GRAPH.read_text(encoding="utf-8")),
    )


class TheRealJobList(unittest.TestCase):
    def setUp(self):
        self.specs = job_specs.load_specs()

    def test_every_job_builds_a_dag_with_lineage(self):
        self.assertTrue(self.specs, "the job list builds no DAG")
        for spec in self.specs:
            with self.subTest(spec.dag_id):
                self.assertTrue(spec.inputs, "a job must report what it reads")

    def test_floor_cycle_owns_one_complete_spark_job_and_only_its_real_edge(self):
        jobs, _ = real_inputs()
        floor = next(job for job in jobs["jobs"] if job["id"] == "building_register_floor")
        self.assertTrue(floor["enabled"])
        self.assertEqual(floor["pool"], "spark")
        self.assertEqual(floor["pipeline_graph_edges"], [
            "source-building-hub-bulk-to-silver-building-register-floors"
        ])
        release = (job_specs.PLATFORM_ROOT / "scripts/deploy/foundation-release.sh").read_text()
        self.assertIn('"${release_root}"/current/infra/systemd/*.service', release)
        self.assertTrue(job_specs.service_unit_file(floor["systemd_service"]).is_file())
        unit = job_specs.service_unit_file(floor["systemd_service"]).read_text()
        self.assertIn("User=foundation-platform\n", unit)
        for environment_file in [
            "/etc/foundation-platform/recovery.env",
            "/etc/foundation-platform/source-sweep.env",
            "/etc/foundation-platform/map-edit-fold.env",
        ]:
            self.assertIn(f"EnvironmentFile={environment_file}\n", unit)
        self.assertIn("ProtectSystem=strict\n", unit)
        self.assertNotRegex(unit, r"(?m)^ReadWritePaths=", "the installer owns the dedicated namespace policy")
        self.assertIn("UMask=0007\n", unit)
        self.assertIn("RuntimeDirectory=foundation-building-register-floor\n", unit)
        self.assertIn("RuntimeDirectoryMode=0700\n", unit)
        self.assertNotIn("LoadCredential=", unit)
        self.assertNotIn("EnvironmentFile=/opt/foundation-platform/current/", unit)
        self.assertIn('building-register-floor-cycle.sh" cleanup\'', unit)
        self.assertIn("TimeoutStopSec=120\n", unit)

    def test_a_job_runs_in_exactly_one_place(self):
        # ADR-0122 §4: an enabled job's systemd timer no longer ships; a job still on systemd keeps
        # its timer. Otherwise it would run twice, or nowhere.
        timers = {path.name for path in job_specs.SYSTEMD.glob("*.timer")}
        for spec in self.specs:
            if spec.systemd_timer is None:
                self.assertTrue(spec.enabled, f"{spec.dag_id} has no timer, so only Airflow can run it")
                continue
            with self.subTest(spec.dag_id):
                self.assertEqual(
                    spec.systemd_timer in timers,
                    not spec.enabled,
                    "enabled in Airflow and still a systemd timer, or neither",
                )

    def test_a_shipped_timer_starts_the_service_its_job_names(self):
        for spec in self.specs:
            if spec.systemd_timer is None:
                continue
            timer = job_specs.SYSTEMD / spec.systemd_timer
            if not timer.is_file():
                continue
            with self.subTest(spec.dag_id):
                units = re.findall(r"^Unit=(\S+)$", timer.read_text(encoding="utf-8"), flags=re.MULTILINE)
                self.assertEqual(units, [spec.systemd_service])

    def test_the_timeout_outlasts_the_service_own_limit(self):
        # Airflow stopping a job systemd would still let finish turns a slow success into a failure.
        for spec in self.specs:
            unit = job_specs.service_unit_file(spec.systemd_service).read_text(encoding="utf-8")
            limit = re.search(r"^TimeoutStartSec=(\d+)(m|h)$", unit, flags=re.MULTILINE)
            with self.subTest(spec.dag_id):
                self.assertIsNotNone(limit, "the service states its own time limit")
                minutes = int(limit.group(1)) * (60 if limit.group(2) == "h" else 1)
                self.assertGreater(spec.timeout_minutes, minutes)

    def test_what_each_service_runs_is_executable_in_the_repository(self):
        # systemd answers 203/EXEC when ExecStart is not executable. The stewardship cycle shipped
        # without its executable bit on 2026-09-30 and never ran once; the timer hid it until
        # Airflow showed the failure (2026-10-01). Read from git, so a Windows checkout cannot pass
        # a file that a Linux host would refuse to execute.
        release = job_specs.RELEASE_PREFIX
        for spec in self.specs:
            unit = job_specs.service_unit_file(spec.systemd_service).read_text(encoding="utf-8")
            exec_start = re.search(r"^ExecStart=(.+)$", unit, flags=re.MULTILINE).group(1)
            if "RuntimeDirectory=foundation-building-register-floor" in unit:
                # The runtime snapshot binds run AND cleanup to the physical release.
                launcher = re.search(r'exec "\$\$\{release\}/([^"]+)"', exec_start)
                self.assertIsNotNone(launcher)
                exec_start = release + launcher.group(1)
            else:
                exec_start = exec_start.split()[0]
            with self.subTest(spec.dag_id):
                self.assertTrue(exec_start.startswith(release), f"{exec_start} is not in the release")
                relative = exec_start[len(release):]
                listed = subprocess.run(
                    ["git", "ls-files", "-s", "--", relative],
                    cwd=job_specs.PLATFORM_ROOT, capture_output=True, text=True, check=True,
                ).stdout.split()
                self.assertTrue(listed, f"{relative} is not tracked")
                self.assertEqual(listed[0], "100755", f"{relative} is not executable in git")

    def test_dag_ids_are_unique(self):
        ids = [spec.dag_id for spec in self.specs]
        self.assertEqual(len(ids), len(set(ids)))


class WhatTheJobListMayNotSay(unittest.TestCase):
    def refused(self, mutate):
        jobs, graph = real_inputs()
        jobs = copy.deepcopy(jobs)
        mutate(jobs)
        with self.assertRaises(job_specs.JobListError):
            job_specs.load_specs(jobs, graph)

    def test_an_edge_the_pipeline_graph_does_not_have(self):
        self.refused(lambda jobs: jobs["jobs"][0]["pipeline_graph_edges"].append("no-such-edge"))

    def test_a_job_that_names_no_edge(self):
        self.refused(lambda jobs: jobs["jobs"][0].update(pipeline_graph_edges=[]))

    def test_a_pool_the_scheduler_does_not_have(self):
        self.refused(lambda jobs: jobs["jobs"][0].update(pool="big"))

    def test_a_service_the_release_does_not_ship(self):
        self.refused(lambda jobs: jobs["jobs"][0].update(systemd_service="foundation-no-such-job.service"))

    def test_a_service_name_outside_the_platform(self):
        self.refused(lambda jobs: jobs["jobs"][0].update(systemd_service="ssh.service"))

    def test_two_jobs_on_one_service(self):
        self.refused(lambda jobs: jobs["jobs"][1].update(systemd_service=jobs["jobs"][0]["systemd_service"]))

    def test_an_enabled_flag_that_is_not_a_boolean(self):
        self.refused(lambda jobs: jobs["jobs"][0].update(enabled="yes"))

    def test_a_job_that_both_moves_data_and_reads_the_contracts(self):
        self.refused(lambda jobs: jobs["jobs"][0].update(reads_data_contracts=True))

    def test_a_duplicate_job_id(self):
        self.refused(lambda jobs: jobs["jobs"].append(copy.deepcopy(jobs["jobs"][0])))


class FloorCycleAdapter(unittest.TestCase):
    def release_wrapper(self, root, publisher_source):
        release = root / "release"
        ops = release / "scripts/ops"
        ops.mkdir(parents=True)
        for name in ["building-register-floor-cycle.sh", "runtime-database-url.py"]:
            (ops / name).write_bytes((job_specs.PLATFORM_ROOT / "scripts/ops" / name).read_bytes())
        (release / "bin").mkdir()
        binary = release / "bin/foundation-outbox-publisher"
        binary.write_text(publisher_source)
        binary.chmod(0o755)
        return ops / "building-register-floor-cycle.sh", release

    def test_run_and_cleanup_use_the_same_captured_configuration_after_current_switches(self):
        unit = (job_specs.SYSTEMD / "foundation-building-register-floor.service").read_text()
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            releases = [root / name for name in ["a", "b"]]
            for release in releases:
                wrapper = release / "scripts/ops/building-register-floor-cycle.sh"
                wrapper.parent.mkdir(parents=True)
                wrapper.write_text('#!/bin/sh\nprintf "%s:%s" "' + release.name + '" "${1:-run}"\n')
                wrapper.chmod(0o755)
            current = root / "current"
            current.symlink_to(releases[0], target_is_directory=True)
            runtime = root / "runtime"
            runtime.mkdir()
            for release in releases:
                (release / ".foundation-floor.env").write_text(
                    "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT=" + str(release) + "\n")
            invocation = "a" * 32
            environment = {"PATH": os.environ["PATH"], "RUNTIME_DIRECTORY": str(runtime),
                           "INVOCATION_ID": invocation}
            for phase, expected in [("ExecStart", "a:run"), ("ExecStopPost", "a:cleanup")]:
                command = re.search(r"^" + phase + r"=/bin/bash -ec '(.+)'$", unit, flags=re.MULTILINE)
                self.assertIsNotNone(command)
                script = command.group(1).replace("$$", "$").replace(
                    "/opt/foundation-platform/current", str(current))
                result = subprocess.run(["bash", "-ec", script], env=environment,
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, expected)
                current.unlink()
                current.symlink_to(releases[1], target_is_directory=True)
            snapshot = runtime / (invocation + ".env")
            captured = snapshot.read_bytes()
            start = re.search(r"^ExecStart=/bin/bash -ec '(.+)'$", unit, re.MULTILINE).group(1)
            start = start.replace("$$", "$").replace("/opt/foundation-platform/current", str(current))
            collision = subprocess.run(["bash", "-ec", start], env=environment, capture_output=True)
            self.assertNotEqual(collision.returncode, 0)
            self.assertEqual(collision.stdout, b"")
            self.assertEqual(snapshot.read_bytes(), captured)
            snapshot.unlink()
            stop = re.search(r"^ExecStopPost=/bin/bash -ec '(.+)'$", unit, re.MULTILINE).group(1).replace("$$", "$")
            missing = subprocess.run(["bash", "-ec", stop], env=environment, capture_output=True)
            self.assertNotEqual(missing.returncode, 0)
            self.assertEqual(missing.stdout, b"")
            snapshot.symlink_to(releases[1] / ".foundation-floor.env")
            linked = subprocess.run(["bash", "-ec", stop], env=environment, capture_output=True)
            self.assertNotEqual(linked.returncode, 0)
            self.assertEqual(linked.stdout, b"")
            snapshot.unlink()
            # A copied config must not redirect this invocation away from the captured current.
            (releases[1] / ".foundation-floor.env").write_text(
                "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT=" + str(releases[0]) + "\n")
            redirected = subprocess.run(["bash", "-ec", start], env=environment, capture_output=True)
            self.assertNotEqual(redirected.returncode, 0)
            self.assertEqual(redirected.stdout, b"")
            self.assertFalse(snapshot.exists(), "failed start must not redirect ExecStopPost either")

    def test_compose_connection_is_projected_without_copying_credentials(self):
        source_url = "postgres://reader:encoded%40password@postgres:5433/catalogue?sslmode=disable"
        config = {"services": {
            "foundation-api": {"environment": {"DATABASE_URL": source_url}},
            "postgres": {"ports": [{"host_ip": "127.0.0.1", "published": "15439", "target": 5433, "protocol": "tcp"}]},
        }}
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            docker = root / "docker"
            docker.write_text("#!/bin/sh\ncat <<'JSON'\n" + json.dumps(config) + "\nJSON\n")
            docker.chmod(0o755)
            script, release = self.release_wrapper(root, '#!/bin/sh\nprintf "%s" "$DATABASE_URL"\n')
            env = {
                "PATH": str(root) + os.pathsep + os.environ["PATH"],
                "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT": str(release),
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH": "/fixture/history.json",
                "FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE": "sha256:" + "a" * 64,
                "FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT": "/fixture/state",
                "FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE": "/fixture/ivy",
            }
            result = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, source_url.replace("postgres:5433", "127.0.0.1:15439"))
            self.assertEqual(result.stderr, "")
            # A failed Compose command must not be masked by a successful JSON consumer.
            docker.write_text(docker.read_text() + "exit 19\n")
            result = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stdout, "")
            self.assertNotIn("password", result.stderr)

    def test_publisher_default_follows_the_physical_release_for_run_and_cleanup(self):
        script = job_specs.PLATFORM_ROOT / "scripts/ops/building-register-floor-cycle.sh"
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            release = root / "release"
            (release / "scripts/ops").mkdir(parents=True)
            (release / "bin").mkdir()
            copied = release / "scripts/ops" / script.name
            copied.write_bytes(script.read_bytes())
            publisher = release / "bin/foundation-outbox-publisher"
            publisher.write_text('#!/bin/sh\nprintf "%s" "$1"\n')
            publisher.chmod(0o755)
            (root / "current").symlink_to(release, target_is_directory=True)
            env = {
                "PATH": os.environ["PATH"], "INVOCATION_ID": "a" * 32,
                "DATABASE_URL": "postgres://fixture/fixture",
                "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT": str(release),
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH": "/fixture/history.json",
                "FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE": "sha256:" + "a" * 64,
                "FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT": str(root / "state"),
                "FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE": str(root / "ivy"),
                "FOUNDATION_BUILDING_REGISTER_FLOOR_PUBLISHER_BIN": "/not-the-release/publisher",
            }
            for args, command in [([], "run-building-register-floor-cycle"), (["cleanup"], "stop-building-register-floor-cycle")]:
                result = subprocess.run(["bash", str(root / "current/scripts/ops" / script.name), *args],
                                        env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, command)

    def test_cleanup_needs_only_invocation_and_rejects_unknown_arguments(self):
        with tempfile.TemporaryDirectory() as directory:
            script, _ = self.release_wrapper(pathlib.Path(directory), '#!/usr/bin/env bash\n[[ "$#" == 1 && "$1" == stop-building-register-floor-cycle ]] || exit 99\nprintf cleaned\n')
            env = {"PATH": os.environ["PATH"], "INVOCATION_ID": "a" * 32}
            result = subprocess.run(["bash", str(script), "cleanup"], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0)
            self.assertEqual(result.stdout, "cleaned")
            for args in [["unknown"], ["cleanup", "extra"], ["run", "extra"]]:
                result = subprocess.run(["bash", str(script), *args], env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, 64)
                self.assertEqual(result.stdout, "")
            del env["INVOCATION_ID"]
            result = subprocess.run(["bash", str(script), "cleanup"], env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stdout, "")

    def test_required_environment_and_child_exit_are_preserved(self):
        required = {
            "DATABASE_URL": "postgres://fixture/fixture",
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH": "/fixture/history.json",
            "FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE": "sha256:" + "a" * 64,
            "FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT": "/fixture/state",
            "FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE": "/fixture/ivy",
        }
        with tempfile.TemporaryDirectory() as directory:
            script, release = self.release_wrapper(pathlib.Path(directory), '#!/usr/bin/env bash\n[[ "$#" == 1 && "$1" == run-building-register-floor-cycle ]] || exit 99\nprintf "%s" "$DATABASE_URL"\nexit 23\n')
            required["FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT"] = str(release)
            env = {"PATH": os.environ["PATH"], **required}
            result = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 23)
            self.assertEqual(result.stdout, required["DATABASE_URL"])
            for name in required:
                with self.subTest(name=name):
                    missing = {key: value for key, value in env.items() if key != name}
                    result = subprocess.run(["bash", str(script)], env=missing, capture_output=True, text=True)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(result.stdout, "")
            redirected = subprocess.run(["bash", str(script)], env={**env,
                "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT": "/not-the-running-release"}, capture_output=True, text=True)
            self.assertNotEqual(redirected.returncode, 23)
            self.assertNotEqual(redirected.returncode, 0)
            self.assertEqual(redirected.stdout, "")


if __name__ == "__main__":
    unittest.main()
