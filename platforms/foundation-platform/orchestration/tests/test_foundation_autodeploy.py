"""The host deploys main by itself (scripts/deploy/foundation-autodeploy.sh, root ADR-0159).

A test copy of the script points its fixed paths at a fixture tree: the control checkout's Git
transport answers main's head, its release_checks.py answers a verdict, and its foundation-deploy.sh
records what it was asked to deploy and exits as told. release_checks.py itself is tested against
a local stand-in for the GitHub API.
"""

import getpass
import http.server
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import threading
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts/deploy/foundation-autodeploy.sh"
CHECKS = PLATFORM / "scripts/deploy/release_checks.py"
DEPLOY = PLATFORM / "scripts/deploy/foundation-deploy.sh"
START_ONCE = PLATFORM / "scripts/deploy/deploy-start-once.sh"
JOBS = PLATFORM / "orchestration/jobs.v1.json"
CURRENT = "a" * 40
HEAD = "b" * 40

sys.path.insert(0, str(CHECKS.parent))
import release_checks  # noqa: E402


def run(name, status="completed", conclusion="success", suite=1):
    return {"name": name, "status": status, "conclusion": conclusion, "check_suite": {"id": suite}}


def queue_run(suite, status="completed", conclusion="success"):
    return {"check_suite_id": suite, "event": "merge_group", "status": status, "conclusion": conclusion}


class TheVerdict(unittest.TestCase):
    def test_every_run_passed_deploys(self):
        decision, _ = release_checks.verdict([run("ci"), run("docs", conclusion="skipped"), run("x", conclusion="neutral")])
        self.assertEqual(decision, "deploy")

    def test_no_run_or_one_still_running_waits(self):
        self.assertEqual(release_checks.verdict([])[0], "wait")
        self.assertEqual(release_checks.verdict([run("ci"), run("slow", status="in_progress", conclusion=None)])[0], "wait")

    def test_any_failure_refuses_even_while_others_run(self):
        for conclusion in ("failure", "cancelled", "timed_out", "action_required", "stale", None):
            with self.subTest(conclusion):
                decision, reason = release_checks.verdict(
                    [run("ci", conclusion=conclusion), run("slow", status="queued", conclusion=None)]
                )
                self.assertEqual(decision, "refuse")
                self.assertIn("ci", reason)


class TheMergeQueueRunDecides(unittest.TestCase):
    """main's commit is the tree the merge queue tested (root ADR-0167)."""

    def test_a_passed_queue_deploys_while_mains_second_run_still_runs(self):
        decision, reason = release_checks.verdict(
            [run("ci", suite=7), run("ci", status="in_progress", conclusion=None, suite=8)], [queue_run(7)]
        )
        self.assertEqual(decision, "deploy")
        self.assertIn("merge queue", reason)

    def test_a_queue_still_running_waits(self):
        for queue in ([queue_run(7, status="in_progress", conclusion=None)], [queue_run(7), queue_run(9, "queued", None)]):
            with self.subTest(queue):
                decision, _ = release_checks.verdict(
                    [run("ci", suite=7), run("slow", status="in_progress", conclusion=None, suite=8)], queue
                )
                self.assertEqual(decision, "wait")

    def test_a_failure_in_mains_second_run_still_refuses(self):
        decision, _ = release_checks.verdict(
            [run("ci", suite=7), run("ci", conclusion="failure", suite=8)], [queue_run(7)]
        )
        self.assertEqual(decision, "refuse")

    def test_a_commit_the_queue_never_ran_waits_for_every_run(self):
        decision, _ = release_checks.verdict([run("ci"), run("slow", status="queued", conclusion=None)], [])
        self.assertEqual(decision, "wait")


class TheChecksAreReadPageByPage(unittest.TestCase):
    def serve(self, runs, queue=()):
        class Handler(http.server.BaseHTTPRequestHandler):
            asked = []

            def do_GET(self):
                Handler.asked.append(self.path)
                page = int(self.path.rsplit("page=", 1)[1])
                key, items = ("workflow_runs", queue) if "/actions/runs?" in self.path else ("check_runs", runs)
                body = json.dumps({"total_count": len(items), key: items[(page - 1) * 100:page * 100]})
                self.send_response(200)
                self.end_headers()
                self.wfile.write(body.encode())

            def log_message(self, *_):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.shutdown)
        return f"http://127.0.0.1:{server.server_port}", Handler.asked

    def test_a_failure_on_the_second_page_refuses(self):
        api, asked = self.serve([run(f"ok{i}") for i in range(130)] + [run("late", conclusion="failure")])
        result = subprocess.run([sys.executable, str(CHECKS), api, "owner/repo", HEAD],
                                capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(result.stdout.startswith("refuse "), result.stdout)
        self.assertEqual(len([path for path in asked if "/check-runs?" in path]), 2)

    def test_the_queue_runs_are_asked_for_this_commit_and_event(self):
        api, asked = self.serve(
            [run("ci", suite=7), run("ci", status="in_progress", conclusion=None, suite=8)], [queue_run(7)]
        )
        result = subprocess.run([sys.executable, str(CHECKS), api, "owner/repo", HEAD],
                                capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(result.stdout.startswith("deploy "), result.stdout)
        self.assertIn(f"/repos/owner/repo/actions/runs?head_sha={HEAD}&event=merge_group&", "".join(asked))

    def test_an_unreachable_api_exits_2(self):
        result = subprocess.run([sys.executable, str(CHECKS), "http://127.0.0.1:9", "owner/repo", HEAD],
                                capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 2)


class TheHostDeploysMainByItself(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="autodeploy-")
        self.addCleanup(self.temp.cleanup)
        root = pathlib.Path(self.temp.name)
        self.root = root
        control = root / "control"
        deploy = control / "platforms/foundation-platform/scripts/deploy"
        deploy.mkdir(parents=True)
        (control / "tools/github").mkdir(parents=True)
        (control / "tools/github/repository-identity.json").write_text('{"full_name": "owner/repo"}')
        (control / "scripts/github").mkdir(parents=True)
        (control / "scripts/github/safe-git-transport.sh").write_text(
            '[ -n "$FIXTURE_NO_HEAD" ] && exit 128\nprintf "%s\\trefs/heads/main\\n" "$FIXTURE_HEAD"\n')
        (deploy / "release_checks.py").write_text(
            "import os, sys\nprint(os.environ['FIXTURE_CHECKS'])\n")
        (deploy / "foundation-deploy.sh").write_text(
            'printf "%s\\n" "$1" >> "$FIXTURE_ROOT/deployed.log"\nexit "${FIXTURE_DEPLOY_EXIT:-0}"\n')
        releases = root / "opt/releases"
        releases.mkdir(parents=True)
        (releases / CURRENT).mkdir()
        (root / "opt/current").symlink_to(releases / CURRENT)
        self.state = root / "state"
        self.off = root / "autodeploy.off"
        text = SCRIPT.read_text()
        for name, value in (("control_root", control), ("release_root", root / "opt"),
                            ("state", self.state), ("off_switch", self.off), ("api", "http://127.0.0.1:9")):
            line = next(line for line in text.splitlines() if line.startswith(f"{name}="))
            text = text.replace(line, f"{name}={value}", 1)
        self.script = root / "foundation-autodeploy.sh"
        self.script.write_text(text)
        # systemctl answers from FIXTURE_UNITS ("unit:transient" pairs, transient yes|no), the Worker
        # deploy's state from FIXTURE_WORKER_STATE, and records what it is asked to start.
        self.bin = root / "bin"
        self.bin.mkdir()
        systemctl = self.bin / "systemctl"
        systemctl.write_text(
            '#!/usr/bin/env bash\n'
            'if [[ "$1" == start ]]; then printf "%s\\n" "$*" >> "$FIXTURE_ROOT/started.log"; exit 0; fi\n'
            'if [[ "$*" == *ActiveState* && "${!#}" == foundation-worker-autodeploy.service ]]; then\n'
            '  printf "%s\\n" "${FIXTURE_WORKER_STATE:-inactive}"; exit 0\n'
            'fi\n'
            'for pair in ${FIXTURE_UNITS:-}; do\n'
            '  unit="${pair%%:*}"; transient="${pair#*:}"\n'
            '  if [[ "$1" == list-units ]]; then printf "%s loaded active running x\\n" "${unit}"\n'
            '  elif [[ "$1" == show && "${!#}" == "${unit}" ]]; then printf "%s\\n" "${transient}"; fi\n'
            'done\n')
        systemctl.chmod(0o755)

    def tick(self, head=HEAD, checks="deploy 49 check runs passed", **env):
        result = subprocess.run(
            ["bash", str(self.script)],
            env={**os.environ, "PATH": f"{self.bin}:{os.environ['PATH']}", "FIXTURE_ROOT": str(self.root),
                 "FIXTURE_HEAD": head, "FIXTURE_CHECKS": checks, **env},
            capture_output=True, text=True, check=False,
        )
        return result

    def deployed(self):
        log = self.root / "deployed.log"
        return log.read_text().split() if log.exists() else []

    def started(self):
        log = self.root / "started.log"
        return log.read_text().splitlines() if log.exists() else []

    def test_a_host_already_on_main_does_nothing(self):
        result = self.tick(head=CURRENT)
        self.assertEqual((result.returncode, result.stdout), (0, ""))
        self.assertEqual(self.deployed(), [])

    def test_a_host_on_main_lets_the_workers_catch_up(self):
        # Root ADR-0175: the Worker deploy retries a Worker still behind on the ticks after a deploy.
        self.assertEqual(self.tick(head=CURRENT).returncode, 0)
        self.assertEqual(self.started(), ["start --no-block foundation-worker-autodeploy.service"])

    def test_a_deploy_that_succeeded_starts_the_worker_deploy_without_waiting_for_it(self):
        self.assertEqual(self.tick().returncode, 0)
        self.assertEqual(self.started(), ["start --no-block foundation-worker-autodeploy.service"])

    def test_a_deploy_that_failed_does_not_start_the_worker_deploy(self):
        self.assertEqual(self.tick(FIXTURE_DEPLOY_EXIT="1").returncode, 1)
        self.assertEqual(self.started(), [])

    def test_a_worker_deploy_under_way_holds_the_next_host_deploy(self):
        result = self.tick(FIXTURE_WORKER_STATE="activating")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("waiting for the Worker deploy", result.stdout)
        self.assertEqual(self.deployed(), [])
        self.assertEqual(self.tick().returncode, 0)
        self.assertEqual(self.deployed(), [HEAD])

    def test_a_new_commit_whose_checks_passed_is_deployed_once(self):
        result = self.tick()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.deployed(), [HEAD])
        self.assertTrue((self.state / "deployed" / HEAD).exists())

    def test_an_operators_unit_holds_the_deploy_until_it_ends(self):
        result = self.tick(FIXTURE_UNITS="foundation-gold-panel-rebuild-unconditional.service:yes")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("waiting for the operator's foundation-gold-panel-rebuild-unconditional", result.stdout)
        self.assertEqual(self.deployed(), [])
        self.assertEqual(self.tick().returncode, 0)
        self.assertEqual(self.deployed(), [HEAD])

    def test_a_registered_job_running_is_left_to_the_deploys_own_wait(self):
        result = self.tick(FIXTURE_UNITS="foundation-map-edit-fold.service:no")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.deployed(), [HEAD])

    def test_checks_still_running_wait_without_deploying(self):
        result = self.tick(checks="wait still running: ci")
        self.assertEqual(result.returncode, 0)
        self.assertIn("still running", result.stdout)
        self.assertEqual(self.deployed(), [])

    def test_a_refused_commit_is_reported_once_and_never_deployed(self):
        first = self.tick(checks="refuse failed: ci=failure")
        self.assertEqual(first.returncode, 1, "a refusal must reach OnFailure")
        again = self.tick(checks="deploy 49 check runs passed")
        self.assertEqual(again.returncode, 0)
        self.assertIn("refused before", again.stdout)
        self.assertEqual(self.deployed(), [])

    def test_a_failed_deploy_is_reported_once_and_not_retried(self):
        first = self.tick(FIXTURE_DEPLOY_EXIT="1")
        self.assertEqual(first.returncode, 1)
        self.assertTrue((self.state / "failed" / HEAD).exists())
        again = self.tick()
        self.assertEqual(again.returncode, 0)
        self.assertEqual(self.deployed(), [HEAD], "a failed commit was deployed a second time")
        # A newer commit on main is the way past it.
        newer = "c" * 40
        self.assertEqual(self.tick(head=newer).returncode, 0)
        self.assertEqual(self.deployed(), [HEAD, newer])

    def test_the_off_switch_stops_everything(self):
        self.off.touch()
        result = self.tick()
        self.assertEqual(result.returncode, 0)
        self.assertEqual(self.deployed(), [])

    def test_an_unreadable_head_waits(self):
        result = self.tick(FIXTURE_NO_HEAD="1")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(self.deployed(), [])


class TheWholeDeploy(unittest.TestCase):
    """foundation-deploy.sh end to end against stand-ins (root ADR-0159 as amended by ADR-0173).

    A test copy points the script's fixed paths at a fixture host: the release now running (its job
    registry, its Airflow runtime script), the control checkout, and the commit's checkout holding a
    stand-in foundation-release.sh that fails at the step FIXTURE_FAIL_AT names. sudo runs the
    command as this user, systemctl answers from FIXTURE_FAILING, git and chown do nothing. What runs
    for real is the script's order of steps and what it does to the DAGs when one of them fails.
    """

    ENABLED = ("source_sweep", "outbox_publish", "map_edit_fold_admin")
    DISABLED = ("vworld_parcel_edition",)

    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="foundation-deploy-")
        self.addCleanup(temp.cleanup)
        root = self.root = pathlib.Path(temp.name)
        old, old_control = "c" * 40, "d" * 40
        release_root, control_root = root / "opt/foundation-platform", root / "opt/perfectory-control"
        running = release_root / "releases" / old
        (running / "orchestration").mkdir(parents=True)
        (running / "scripts/deploy").mkdir(parents=True)
        once = {"source_sweep": 5, "outbox_publish": 1}
        jobs = [{"id": job, "enabled": job in self.ENABLED, "systemd_service": f"foundation-{job}.service",
                 "timeout_minutes": once.get(job, 30), "started_once_after_deploy": job in once}
                for job in self.ENABLED + self.DISABLED]
        (running / "orchestration/jobs.v1.json").write_text(json.dumps({"jobs": jobs}))
        # airflow-runtime.sh exec airflow-scheduler airflow <args>: record <args>.
        (running / "scripts/deploy/airflow-runtime.sh").write_text(
            'if [[ "$1" == exec ]]; then shift 3; printf "%s\\n" "$*" >> "$FIXTURE_ROOT/airflow.log"\n'
            '  [[ -z "${FIXTURE_AIRFLOW_FAILS:-}" || "$*" != *"$FIXTURE_AIRFLOW_FAILS"* ]]; fi\n')
        (release_root / "current").symlink_to(pathlib.Path("releases") / old)
        (release_root / "config" / old).mkdir(parents=True)
        (release_root / "config" / old / "building-register-floor.env").write_text(
            "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT=x\nFOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE=y\n")
        (release_root / "artifacts" / HEAD).mkdir(parents=True)
        (release_root / "artifacts" / HEAD / "build.json").write_text('{"publisher_image": "sha256:' + "e" * 64 + '"}')
        trusted = control_root / "releases" / old_control
        (trusted / "tools/github").mkdir(parents=True)
        (trusted / "tools/github/repository-identity.json").write_text('{"full_name": "owner/repo"}')
        (trusted / "scripts/github").mkdir(parents=True)
        (trusted / "scripts/github/safe-git-transport.sh").write_text("exit 0\n")
        (control_root / "current").symlink_to(pathlib.Path("releases") / old_control)
        # The commit's checkout exists already, so step 1 only moves `current` to it.
        new = control_root / "releases" / HEAD / "platforms/foundation-platform/scripts/deploy"
        new.mkdir(parents=True)
        (new / "foundation-release.sh").write_text(
            '#!/usr/bin/env bash\nprintf "%s\\n" "$1" >> "$FIXTURE_ROOT/release.log"\n'
            '[[ "$1" != "${FIXTURE_FAIL_AT:-}" ]]\n')
        (new / "foundation-release.sh").chmod(0o755)
        (root / "var").mkdir()
        conf = root / "release-deploy.conf"
        conf.write_text(f"FOUNDATION_DEPLOYER={getpass.getuser()}\n")
        text = DEPLOY.read_text(encoding="utf-8")
        text = text.replace('[[ ${EUID} == 0 ]] || { echo "foundation-deploy: run as root" >&2; exit 64; }\n', "", 1)
        for name, value in (("host_conf", conf), ("release_root", release_root), ("control_root", control_root),
                            ("mirror", root / "var/control-source.git")):
            line = next(line for line in text.splitlines() if line.startswith(f"{name}="))
            text = text.replace(line, f"{name}={value}", 1)
        text = text.replace("mktemp /var/lib/perfectory/", f"mktemp {root}/var/")
        self.assertNotIn("/var/lib/perfectory", text)
        scripts = root / "scripts"
        scripts.mkdir()
        (scripts / "foundation-deploy.sh").write_text(text)
        (scripts / START_ONCE.name).write_bytes(START_ONCE.read_bytes())
        self.script = scripts / "foundation-deploy.sh"
        self.bin = root / "bin"
        self.bin.mkdir()
        for name, body in (
            ("sudo", 'while [[ "$1" == -* ]]; do [[ "$1" == -u ]] && shift; shift; done\nexec "$@"\n'),
            ("git", 'if [[ "$*" == *" -o "* ]]; then : > "${@: -2:1}"; fi\nexit 0\n'),
            ("chown", "exit 0\n"),
            ("systemctl", 'printf "%s\\n" "$*" >> "$FIXTURE_ROOT/systemctl.log"\n'
                          'service="${!#}"; failing=" ${FIXTURE_FAILING:-} "\n'
                          'case "$*" in\n'
                          '  start*) [[ "${failing}" != *" ${service} "* ]] ;;\n'
                          '  *ActiveState*) echo inactive ;;\n'
                          '  *Result*) if [[ "${failing}" == *" ${service} "* ]]; then echo exit-code; else echo success; fi ;;\n'
                          'esac\n'),
        ):
            (self.bin / name).write_text("#!/usr/bin/env bash\n" + body)
            (self.bin / name).chmod(0o755)

    def deploy(self, **env):
        return subprocess.run(
            ["bash", str(self.script), HEAD],
            env={**os.environ, "PATH": f"{self.bin}:{os.environ['PATH']}", "FIXTURE_ROOT": str(self.root), **env},
            capture_output=True, text=True, check=False, timeout=120,
        )

    def dags(self):
        """Each DAG's state after the deploy, from the last pause or unpause Airflow was told."""
        state = {}
        path = self.root / "airflow.log"
        for line in (path.read_text().splitlines() if path.exists() else []):
            verb, _, dag = line.removeprefix("dags ").partition(" ")
            if verb in ("pause", "unpause"):
                state[dag.removeprefix("foundation_")] = verb
        return state

    def released(self):
        path = self.root / "release.log"
        return path.read_text().split() if path.exists() else []

    def test_a_clean_deploy_ends_with_every_enabled_dag_unpaused(self):
        result = self.deploy()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.released(), ["prepare", "floor-config", "activate", "migrate", "timers", "status"])
        self.assertEqual(self.dags(), {**{job: "unpause" for job in self.ENABLED},
                                       **{job: "pause" for job in self.DISABLED}})

    def test_a_failed_post_deploy_run_leaves_every_enabled_dag_unpaused(self):
        # 2026-10-09, three deploys in a row: the sweep's post-deploy run failed, the deploy exited at
        # step 6 before step 7, and all 17 DAGs stayed paused for about three hours.
        result = self.deploy(FIXTURE_FAILING="foundation-source_sweep.service")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("DONE: production runs", result.stdout)
        self.assertEqual(self.dags(), {**{job: "unpause" for job in self.ENABLED},
                                       **{job: "pause" for job in self.DISABLED}})
        self.assertIn(f"the post-deploy run did not succeed under {HEAD}: foundation-source_sweep.service",
                      result.stderr)
        started = [line for line in (self.root / "systemctl.log").read_text().splitlines() if line.startswith("start")]
        self.assertEqual(started, ["start foundation-source_sweep.service", "start foundation-outbox_publish.service"],
                         "a failed run does not stop the next one from being started")

    def test_a_deploy_that_stops_before_the_switch_gives_the_running_release_its_schedule_back(self):
        result = self.deploy(FIXTURE_FAIL_AT="prepare")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.released(), ["prepare"], "nothing of the new release was activated")
        self.assertEqual(self.dags(), {**{job: "unpause" for job in self.ENABLED},
                                       **{job: "pause" for job in self.DISABLED}})
        self.assertIn("stopped (exit 1) before the release switch", result.stderr)

    def test_a_deploy_that_stops_during_the_switch_keeps_the_dags_paused_and_says_so(self):
        result = self.deploy(FIXTURE_FAIL_AT="migrate")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.dags(), {job: "pause" for job in self.ENABLED + self.DISABLED},
                         "jobs must not run on a release that is activated but not migrated")
        self.assertIn("THE DAGS STAY PAUSED", result.stderr)

    def test_a_dag_that_cannot_be_unpaused_fails_the_deploy_loudly(self):
        result = self.deploy(FIXTURE_AIRFLOW_FAILS="unpause foundation_outbox_publish")
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("DONE: production runs", result.stdout)
        self.assertIn("DAGS MAY STILL BE PAUSED", result.stderr)


class TheJobsADeployStartsOnce(unittest.TestCase):
    def test_they_are_enabled_and_carry_the_limit_the_deploy_waits_for(self):
        jobs = json.loads(JOBS.read_text(encoding="utf-8"))["jobs"]
        once = [job for job in jobs if job.get("started_once_after_deploy")]
        self.assertTrue(once, "a deploy that starts nothing proves nothing about the new release")
        for job in once:
            with self.subTest(job["id"]):
                self.assertTrue(job["enabled"], "a disabled job is never started by the deploy")
                # foundation-deploy.sh waits each one's own timeout_minutes.
                self.assertIsInstance(job["timeout_minutes"], int)
                self.assertGreater(job["timeout_minutes"], 0)

    def test_systemd_never_stops_a_deploy_halfway(self):
        jobs = json.loads(JOBS.read_text(encoding="utf-8"))["jobs"]
        once = sum(job["timeout_minutes"] + 1 for job in jobs if job.get("started_once_after_deploy"))
        unit = (PLATFORM / "infra/systemd/foundation-autodeploy.service").read_text()
        limit = next(line for line in unit.splitlines() if line.startswith("TimeoutStartSec="))
        hours = int(limit.removeprefix("TimeoutStartSec=").removesuffix("h"))
        # 5h waiting for running jobs and an hour to build and migrate, then the started jobs.
        self.assertGreaterEqual(hours * 60, 5 * 60 + 60 + once, "a deploy can outlive its unit's limit")


if __name__ == "__main__":
    unittest.main()
