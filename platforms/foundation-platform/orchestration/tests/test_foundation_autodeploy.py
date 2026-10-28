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
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts/deploy/foundation-autodeploy.sh"
CHECKS = PLATFORM / "scripts/deploy/release_checks.py"
DEPLOY = PLATFORM / "scripts/deploy/foundation-deploy.sh"
JOB_UNITS = PLATFORM / "scripts/deploy/job_units.py"
START_ONCE = PLATFORM / "scripts/deploy/deploy-start-once.sh"
JOBS = PLATFORM / "orchestration/jobs.v1.json"
CURRENT = "a" * 40
HEAD = "b" * 40
PREFLIGHT = PLATFORM / "scripts/deploy/migration-approval-preflight.py"
GATES_CHECKER = PLATFORM.parents[1] / "scripts/guard/migration-approval-gates.py"
GATES_CONTRACT = PLATFORM.parents[1] / "tools/ci/migration-approval-gates.contract.json"
GATED_MIGRATION = PLATFORM / "migrations/20261014021000_drop_the_catalog_parcel_building_and_unit_copies.sql"
PLAIN_MIGRATION = PLATFORM / "migrations/20260719000001_foundation_platform_schema.sql"
RECHECK_SECONDS = int(next(line for line in SCRIPT.read_text().splitlines()
                           if line.startswith("recheck_seconds=")).removeprefix("recheck_seconds="))

def version(migration):
    return migration.name.split("_", 1)[0]


sys.path.insert(0, str(CHECKS.parent))
import release_checks  # noqa: E402
sys.path.insert(0, str(PLATFORM / "orchestration/dags"))
import job_specs  # noqa: E402


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


class ReportsDoNotDecide(unittest.TestCase):
    """A schedule's or a dispatch's run reports on the repository, not on this commit (2026-10-19:
    a cache budget over its line refused fc61aa36)."""

    def report(self, suite, event="schedule"):
        return {"check_suite_id": suite, "event": event, "status": "completed", "conclusion": "failure"}

    def test_a_failed_report_neither_refuses_nor_hides(self):
        for event in ("schedule", "workflow_dispatch"):
            with self.subTest(event):
                decision, reason = release_checks.verdict(
                    [run("ci", suite=7), run("Actions cache budget", conclusion="failure", suite=5)],
                    [queue_run(7), self.report(5, event)],
                )
                self.assertEqual(decision, "deploy")
                self.assertIn("Actions cache budget=failure", reason)

    def test_a_running_report_is_not_waited_for(self):
        decision, _ = release_checks.verdict(
            [run("ci", suite=7), run("Actions cache budget", status="in_progress", conclusion=None, suite=5)],
            [{"check_suite_id": 7, "event": "push", "status": "completed", "conclusion": "success"}, self.report(5)],
        )
        self.assertEqual(decision, "deploy")

    def test_reports_alone_are_no_verdict(self):
        decision, _ = release_checks.verdict([run("Actions cache budget", suite=5)], [self.report(5)])
        self.assertEqual(decision, "wait")

    def test_the_same_check_from_a_push_still_refuses(self):
        decision, _ = release_checks.verdict(
            [run("ci", suite=7), run("guard", conclusion="failure", suite=6)],
            [queue_run(7), {"check_suite_id": 6, "event": "push", "status": "completed", "conclusion": "failure"}],
        )
        self.assertEqual(decision, "refuse")


class ARegistryReleaseIsWaitedFor(unittest.TestCase):
    """Root ADR-0276 R3: admission installs the release CI published, so the deploy waits for it."""

    RELEASE = "Build and attest the release"

    def test_the_queue_alone_waits_for_the_release(self):
        runs = [run("ci", suite=7), run(self.RELEASE, status="in_progress", conclusion=None, suite=8)]
        self.assertEqual(release_checks.verdict(runs, [queue_run(7)])[0], "deploy", "server-build: as before")
        decision, reason = release_checks.verdict(runs, [queue_run(7)], self.RELEASE)
        self.assertEqual(decision, "wait")
        self.assertIn(self.RELEASE, reason)

    def test_a_published_release_deploys_and_a_failed_one_refuses(self):
        self.assertEqual(release_checks.verdict([run("ci", suite=7), run(self.RELEASE, suite=8)], [queue_run(7)],
                                                self.RELEASE)[0], "deploy")
        self.assertEqual(release_checks.verdict([run("ci", suite=7), run(self.RELEASE, conclusion="failure", suite=8)],
                                                [queue_run(7)], self.RELEASE)[0], "refuse")
        self.assertEqual(release_checks.verdict([run("ci", suite=7)], [queue_run(7)], self.RELEASE)[0], "wait",
                         "no release run yet")

    def test_the_contract_names_the_release_check_only_in_registry_mode(self):
        with tempfile.TemporaryDirectory() as temporary:
            contract = pathlib.Path(temporary) / "release-build.contract.json"
            contract.write_text(json.dumps({"artifacts_from": "server-build", "registry": {"check_run": self.RELEASE}}))
            self.assertIsNone(release_checks.required_release_check(contract))
            contract.write_text(json.dumps({"artifacts_from": "registry", "registry": {"check_run": self.RELEASE}}))
            self.assertEqual(release_checks.required_release_check(contract), self.RELEASE)


class TheChecksAreReadPageByPage(unittest.TestCase):
    def serve(self, runs, queue=(), published=()):
        class Handler(http.server.BaseHTTPRequestHandler):
            asked = []

            def do_HEAD(self):
                # GitHub's container registry: the release tags that are in the package.
                Handler.asked.append(f"HEAD {self.path}")
                found = self.headers.get("Authorization") == "Bearer anonymous" and self.path in {
                    f"/v2/owner/foundation-release/manifests/release-{commit}" for commit in published}
                self.send_response(200 if found else 404)
                self.end_headers()

            def do_GET(self):
                Handler.asked.append(self.path)
                if self.path.startswith("/token?scope=repository:owner/foundation-release:pull"):
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b'{"token": "anonymous"}')
                    return
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

    def test_the_commits_workflow_runs_are_asked_for_every_event(self):
        # Every event: the merge queue's runs decide early, a schedule's or a dispatch's do not decide.
        # The release the contract may require has published (ARegistryReleaseIsWaitedFor).
        api, asked = self.serve(
            [run("ci", suite=7), run("ci", status="in_progress", conclusion=None, suite=8),
             run("Build and attest the release", suite=9)], [queue_run(7)], published=[HEAD]
        )
        result = subprocess.run([sys.executable, str(CHECKS), api, "owner/repo", HEAD, api],
                                capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(result.stdout.startswith("deploy "), result.stdout)
        self.assertIn(f"/repos/owner/repo/actions/runs?head_sha={HEAD}&per_page=", "".join(asked))

    def test_a_release_not_in_the_registry_yet_waits_and_is_never_deployed(self):
        # 2026-10-19: e6fb99e6 was deployed with every check passed and refused at step 2 with 404.
        # In registry mode (the contract) the tag admission pulls first must be there.
        runs = [run("ci"), run("Build and attest the release")]
        for published, expected in (([], "wait "), (["0" * 40], "wait "), ([HEAD], "deploy ")):
            with self.subTest(published=published):
                api, asked = self.serve(runs, published=published)
                result = subprocess.run([sys.executable, str(CHECKS), api, "owner/repo", HEAD, api],
                                        capture_output=True, text=True, check=False)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertTrue(result.stdout.startswith(expected), result.stdout)
                self.assertIn(f"HEAD /v2/owner/foundation-release/manifests/release-{HEAD}", asked)
        self.assertIn(f"release-{HEAD} is not in owner/foundation-release yet", self.check([], runs))

    def check(self, published, runs):
        api, _ = self.serve(runs, published=published)
        return subprocess.run([sys.executable, str(CHECKS), api, "owner/repo", HEAD, api],
                              capture_output=True, text=True, check=False).stdout

    def test_newest_released_names_the_first_published_of_the_commits_newest_first(self):
        building, released, older = HEAD, "1" * 40, "2" * 40
        for published, expected in (([released, older], released), ([older], older), ([], "")):
            with self.subTest(published=published):
                api, _ = self.serve([], published=published)
                result = subprocess.run([sys.executable, str(CHECKS), "newest-released", "owner/repo",
                                         building, released, older, "--registry", api],
                                        capture_output=True, text=True, check=False)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.strip(), expected)
        result = subprocess.run([sys.executable, str(CHECKS), "newest-released", "owner/repo", building,
                                 "--registry", "http://127.0.0.1:9"], capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 2)

    def test_an_unreadable_registry_exits_2_to_ask_again(self):
        api, _ = self.serve([run("ci"), run("Build and attest the release")])
        result = subprocess.run([sys.executable, str(CHECKS), api, "owner/repo", HEAD, "http://127.0.0.1:9"],
                                capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 2)
        self.assertIn("cannot read whether", result.stderr)

    def test_an_unreachable_api_exits_2(self):
        result = subprocess.run([sys.executable, str(CHECKS), "http://127.0.0.1:9", "owner/repo", HEAD],
                                capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 2)


def bash_assignment(name, value):
    r"""`name=value` as bash reads it back. An unquoted Windows path loses its
    backslashes to bash and turns relative: `state=X:\<temp>\state` became
    `X:<temp>state` under the working directory, and PR #478 committed those
    lock files (scripts/guard/tracked-paths-are-portable.py)."""
    if isinstance(value, pathlib.PurePath):
        value = value.as_posix()
    return f"{name}={shlex.quote(str(value))}"


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
        # Answers FIXTURE_CHECKS and records each time it is asked (each is GitHub API requests).
        # `newest-released` answers the first candidate FIXTURE_RELEASED names (unset: every one).
        (deploy / "release_checks.py").write_text(
            "import os, sys\n"
            "root = os.environ['FIXTURE_ROOT']\n"
            "if sys.argv[1] == 'newest-released':\n"
            "    open(os.path.join(root, 'candidates.log'), 'a').write(' '.join(sys.argv[3:]) + '\\n')\n"
            "    released = os.environ.get('FIXTURE_RELEASED')\n"
            "    print(next((c for c in sys.argv[3:] if released is None or c in released.split()), ''))\n"
            "    sys.exit(0)\n"
            "open(os.path.join(root, 'asked.log'), 'a').write(sys.argv[3] + '\\n')\n"
            "print(os.environ['FIXTURE_CHECKS'])\n")
        (deploy / "foundation-deploy.sh").write_text(
            'printf "%s\\n" "$1" >> "$FIXTURE_ROOT/deployed.log"\nexit "${FIXTURE_DEPLOY_EXIT:-0}"\n')
        # Whether a unit runs is the real definition (root ADR-0184), asking the systemctl below.
        (deploy / JOB_UNITS.name).write_bytes(JOB_UNITS.read_bytes())
        # The pacing the host trusts: the real contract (its quiet window).
        (control / "platforms/foundation-platform/config").mkdir(parents=True)
        self.pacing = control / "platforms/foundation-platform/config/autodeploy.contract.json"
        self.pacing.write_bytes((PLATFORM / "config/autodeploy.contract.json").read_bytes())
        releases = root / "opt/releases"
        releases.mkdir(parents=True)
        (releases / CURRENT).mkdir()
        (root / "opt/current").symlink_to(releases / CURRENT)
        self.state = root / "state"
        self.off = root / "autodeploy.off"
        text = SCRIPT.read_text()
        for name, value in (("control_root", control), ("release_root", root / "opt"),
                            ("state", self.state), ("off_switch", self.off), ("api", "http://127.0.0.1:9"),
                            ("mirror", root / "mirror.git")):
            line = next(line for line in text.splitlines() if line.startswith(f"{name}="))
            text = text.replace(line, bash_assignment(name, value), 1)
        self.script = root / "foundation-autodeploy.sh"
        self.script.write_text(text)
        # systemctl answers from FIXTURE_UNITS ("unit:transient[:ActiveState]", transient yes|no, the
        # state active by default), the Worker deploy's state from FIXTURE_WORKER_STATE, fails every
        # listing under FIXTURE_SYSTEMD_DOWN, and records what it is asked to start.
        self.bin = root / "bin"
        self.bin.mkdir()
        systemctl = self.bin / "systemctl"
        systemctl.write_text(
            '#!/usr/bin/env bash\n'
            'if [[ "$1" == start ]]; then printf "%s\\n" "$*" >> "$FIXTURE_ROOT/started.log"; exit 0; fi\n'
            'entries="foundation-worker-autodeploy.service:no:${FIXTURE_WORKER_STATE:-inactive} ${FIXTURE_UNITS:-}"\n'
            'if [[ "$1" == list-units ]]; then\n'
            '  [[ -z "${FIXTURE_SYSTEMD_DOWN:-}" ]] || exit 1\n'
            '  for entry in ${entries}; do\n'
            '    IFS=: read -r unit transient state <<<"${entry}"\n'
            '    for pattern in "${@:2}"; do\n'
            '      [[ "${pattern}" != -* && "${unit}" == ${pattern} ]] || continue\n'
            '      printf "%s loaded %s x fixture\\n" "${unit}" "${state:-active}"; break\n'
            '    done\n'
            '  done\n'
            'elif [[ "$1" == show ]]; then\n'
            '  for entry in ${entries}; do\n'
            '    IFS=: read -r unit transient _ <<<"${entry}"\n'
            '    [[ "${!#}" != "${unit}" ]] || printf "%s\\n" "${transient}"\n'
            '  done\n'
            'fi\n')
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

    def ended(self, outcome, minutes_ago):
        """A deploy of another commit that ended `minutes_ago`, deployed or failed."""
        (self.state / outcome).mkdir(parents=True, exist_ok=True)
        path = self.state / outcome / ("d" * 40)
        path.write_text("synthetic\n")
        when = time.time() - minutes_ago * 60
        os.utime(path, (when, when))

    def test_a_deploy_waits_for_the_quiet_window_after_the_last_one(self):
        # Proposal 2 (2026-10-19): 45 deploys in 33 hours, 72% of the time inside one.
        quiet = json.loads(self.pacing.read_text(encoding="utf-8"))["quiet_minutes_after_a_deploy"]
        self.assertEqual(quiet, 15)
        self.ended("deployed", minutes_ago=quiet - 1)
        result = self.tick()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("waiting for the quiet window after the last deploy", result.stdout)
        self.assertEqual(self.deployed(), [])
        shutil.rmtree(self.state / "deployed")
        self.ended("deployed", minutes_ago=quiet + 1)
        result = self.tick()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.deployed(), [HEAD])

    def test_the_fix_for_a_failed_deploy_does_not_wait(self):
        # A failed deploy left the running release as it was.
        self.ended("failed", minutes_ago=1)
        result = self.tick()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.deployed(), [HEAD])

    def test_a_host_without_the_pacing_contract_deploys_without_a_quiet_window(self):
        # The first deploy that ships the contract runs under a control checkout that has none.
        self.pacing.unlink()
        self.ended("deployed", minutes_ago=1)
        result = self.tick()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("no quiet window", result.stdout)
        self.assertEqual(self.deployed(), [HEAD])

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

    def test_an_operators_oneshot_or_stopping_unit_holds_the_deploy_and_a_failed_one_does_not(self):
        # A oneshot is `activating` for its whole run (root ADR-0184): job_units.py's definition.
        for state in ("activating", "deactivating", "reloading"):
            with self.subTest(state=state):
                result = self.tick(FIXTURE_UNITS=f"foundation-raon-large-files-operator.service:yes:{state}")
                self.assertIn("waiting for the operator's foundation-raon-large-files-operator", result.stdout)
                self.assertEqual(self.deployed(), [])
        result = self.tick(FIXTURE_UNITS="foundation-raon-large-files-operator.service:yes:failed")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.deployed(), [HEAD])

    def test_a_systemd_that_cannot_answer_holds_the_deploy_and_says_so(self):
        result = self.tick(FIXTURE_SYSTEMD_DOWN="1")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("systemd cannot tell which units run", result.stderr)
        self.assertEqual(self.deployed(), [])

    def test_a_registered_job_running_is_left_to_the_deploys_own_wait(self):
        result = self.tick(FIXTURE_UNITS="foundation-map-edit-fold.service:no")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.deployed(), [HEAD])

    def test_checks_still_running_wait_without_deploying(self):
        result = self.tick(checks="wait still running: ci")
        self.assertEqual(result.returncode, 0)
        self.assertIn("still running", result.stdout)
        self.assertEqual(self.deployed(), [])

    def test_a_release_not_published_yet_is_waited_for_and_is_no_failure(self):
        result = self.tick(checks=f"wait release-{HEAD} is not in owner/foundation-release yet")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.deployed(), [], "no deploy is attempted")
        for outcome in ("failed", "refused"):
            self.assertFalse((self.state / outcome / HEAD).exists(), f"a missing release is not {outcome}")
        asked = time.time() - 301  # the next ask, recheck_seconds later
        os.utime(self.state / "asked" / HEAD, (asked, asked))
        self.assertEqual(self.tick(checks="deploy 49 check runs passed").returncode, 0)
        self.assertEqual(self.deployed(), [HEAD], "deployed once it is published")

    def main_after_current(self, count):
        """A mirror whose main is the running release and then `count` commits; those, newest first."""
        git = ["git", "-c", "user.name=fixture", "-c", "user.email=fixture@example.test"]
        work = self.root / "work"
        subprocess.run([*git, "init", "-q", "-b", "main", str(work)], check=True)
        commits = []
        for index in range(count + 1):
            subprocess.run([*git, "-C", str(work), "commit", "-q", "--allow-empty", "-m", f"c{index}"], check=True)
            commits.append(subprocess.run(["git", "-C", str(work), "rev-parse", "HEAD"], capture_output=True,
                                          text=True, check=True).stdout.strip())
        subprocess.run(["git", "clone", "-q", "--bare", str(work), str(self.root / "mirror.git")], check=True)
        (self.root / "opt/releases" / commits[0]).mkdir()
        (self.root / "opt/current").unlink()
        (self.root / "opt/current").symlink_to(self.root / "opt/releases" / commits[0])
        return list(reversed(commits[1:]))

    def test_the_newest_released_commit_is_deployed_while_newer_ones_still_build(self):
        # 2026-10-19: 13e6d650's release was published while the tick waited on b375edaa, still
        # building; merges came faster than the release build, so nothing deployed.
        head, released, older = self.main_after_current(3)
        result = self.tick(head=head, FIXTURE_RELEASED=f"{released} {older}")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.deployed(), [released], "the newest released commit, not main's head")
        self.assertEqual((self.root / "candidates.log").read_text().split(), [head, released, older])
        self.assertEqual((self.root / "asked.log").read_text().split(), [released], "only the target's checks are asked")

    def test_nothing_released_after_current_deploys_nothing_and_marks_nothing(self):
        head, *_ = self.main_after_current(2)
        result = self.tick(head=head, FIXTURE_RELEASED="")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("has a published release yet", result.stdout)
        self.assertEqual(self.deployed(), [])
        self.assertFalse((self.root / "asked.log").exists(), "no check run is asked for")
        self.assertFalse((self.state / "failed").exists() or (self.state / "refused").exists())

    def test_a_failed_commit_is_passed_over_for_the_next_released_one(self):
        head, failed, older = self.main_after_current(3)
        (self.state / "failed").mkdir(parents=True)
        (self.state / "failed" / failed).write_text("synthetic\n")
        self.assertEqual(self.tick(head=head, FIXTURE_RELEASED=f"{failed} {older}").returncode, 0)
        self.assertEqual(self.deployed(), [older])

    def test_a_refused_commit_is_reported_once_and_never_deployed(self):
        first = self.tick(checks="refuse failed: ci=failure")
        self.assertEqual(first.returncode, 1, "a refusal must reach OnFailure")
        again = self.tick(checks="deploy 49 check runs passed")
        self.assertEqual(again.returncode, 0)
        self.assertIn("was refused or failed before", again.stdout)
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

    def asked(self):
        log = self.root / "asked.log"
        return log.read_text().split() if log.exists() else []

    def age_the_last_ask(self, head=HEAD, seconds=RECHECK_SECONDS):
        asked = self.state / "asked" / head
        os.utime(asked, (asked.stat().st_mtime - seconds,) * 2)

    def test_a_commit_without_a_verdict_is_asked_again_only_after_the_interval(self):
        # ADR-0187: a tick every minute must not spend the anonymous API budget on one commit.
        for _ in range(3):
            self.assertEqual(self.tick(checks="wait still running: ci").returncode, 0)
        self.assertEqual(self.asked(), [HEAD])
        self.age_the_last_ask()
        self.assertEqual(self.tick().returncode, 0)
        self.assertEqual(self.asked(), [HEAD, HEAD])
        self.assertEqual(self.deployed(), [HEAD])

    def test_a_new_head_is_asked_at_once(self):
        self.assertEqual(self.tick(checks="wait still running: ci").returncode, 0)
        newer = "c" * 40
        self.assertEqual(self.tick(head=newer).returncode, 0)
        self.assertEqual(self.asked(), [HEAD, newer])
        self.assertEqual(self.deployed(), [newer])

    def test_a_passed_verdict_is_kept_while_the_deploy_waits(self):
        unit = "foundation-gold-panel-rebuild-unconditional.service:yes"
        for _ in range(3):
            self.assertIn("waiting for the operator's", self.tick(FIXTURE_UNITS=unit).stdout)
        # The operator's unit ended: deployed on the next tick, not an interval later, with one ask.
        self.assertEqual(self.tick(checks="refuse never asked again").returncode, 0)
        self.assertEqual(self.deployed(), [HEAD])
        self.assertEqual(self.asked(), [HEAD])

    def test_the_asking_fits_the_anonymous_api_budget(self):
        # GitHub allows an unauthenticated client 60 REST requests an hour; release_checks.py
        # spends at least two per answer (check runs, then the merge queue's runs). Keep half the
        # budget for everything else the host reads anonymously.
        timer = (PLATFORM / "infra/systemd/foundation-autodeploy.timer").read_text()
        tick = next(line for line in timer.splitlines() if line.startswith("OnUnitInactiveSec="))
        self.assertLessEqual(int(tick.removeprefix("OnUnitInactiveSec=").removesuffix("min")) * 60,
                             RECHECK_SECONDS, "asking on every tick")
        self.assertLessEqual(3600 // RECHECK_SECONDS * 2, 60 // 2)

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
                 "timeout_minutes": once.get(job, 30), "started_once_after_deploy": job in once,
                 "interruptible": job == "source_sweep"}
                for job in self.ENABLED + self.DISABLED]
        (running / "orchestration/jobs.v1.json").write_text(json.dumps({"jobs": jobs}))
        # airflow-runtime.sh exec airflow-scheduler airflow <args>: record <args>; `dags list` answers
        # each DAG's state from what it was told (an enabled one starts unpaused, a disabled one paused).
        (running / "scripts/deploy/airflow-runtime.sh").write_text(
            'if [[ "$1" == exec ]]; then shift 3; printf "%s\\n" "$*" >> "$FIXTURE_ROOT/airflow.log"\n'
            '  [[ "$*" != "dags list -o json" ]] || python3 "$FIXTURE_ROOT/dags_list.py"\n'
            '  [[ -z "${FIXTURE_AIRFLOW_FAILS:-}" || "$*" != *"$FIXTURE_AIRFLOW_FAILS"* ]]; fi\n')
        (root / "dags_list.py").write_text(
            "import json, os\n"
            f"state = {{'foundation_' + job: job in {self.DISABLED!r} for job in {self.ENABLED + self.DISABLED!r}}}\n"
            "for line in open(os.path.join(os.environ['FIXTURE_ROOT'], 'airflow.log')):\n"
            "    words = line.split()\n"
            "    if words[:2] in (['dags', 'pause'], ['dags', 'unpause']):\n"
            "        state[words[2]] = words[1] == 'pause'\n"
            # A DAG Airflow has not parsed yet is not listed (FIXTURE_UNLISTED).
            "unlisted = {'foundation_' + job for job in os.environ.get('FIXTURE_UNLISTED', '').split()}\n"
            "print(json.dumps([{'dag_id': dag, 'is_paused': str(paused)} for dag, paused in state.items()\n"
            "                  if dag not in unlisted]))\n")
        # The running release's compose wrapper stands in for psql on the production database: it
        # answers the ledger from FIXTURE_ROOT/applied and a setting from FIXTURE_SETTING.
        (running / "scripts/deploy/foundation-runtime.sh").write_text(
            '#!/usr/bin/env bash\n[[ -z "${FIXTURE_DB_DOWN:-}" ]] || exit 2\nsql="$(cat)"\n'
            'case "$sql" in *_sqlx_migrations*) cat "$FIXTURE_ROOT/applied" ;;\n'
            '  *current_setting*) echo "${FIXTURE_SETTING:-}" ;; esac\n')
        (running / "scripts/deploy/foundation-runtime.sh").chmod(0o755)
        # The commit's tree: its approval gate checker and contract, its pre-flight, and the gated
        # migration of #465 with one ordinary one. The database has applied both unless a test says.
        commit = root / "commit"
        for path in (GATES_CHECKER, GATES_CONTRACT, PREFLIGHT, GATED_MIGRATION, PLAIN_MIGRATION):
            relative = path.relative_to(PLATFORM.parents[1])
            (commit / relative).parent.mkdir(parents=True, exist_ok=True)
            (commit / relative).write_bytes(path.read_bytes())
        (root / "applied").write_text(f"{version(PLAIN_MIGRATION)}\n{version(GATED_MIGRATION)}\n")
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
        # The control checkout's release admission answers step 0's `running-jobs`: 75 (a job runs)
        # FIXTURE_BUSY_ANSWERS times, then 0; FIXTURE_NO_ANSWER makes it fail as admission does (65).
        running_jobs = trusted / "scripts/deploy/foundation-release-admission.py"
        running_jobs.parent.mkdir(parents=True)
        running_jobs.write_text(
            '#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "$FIXTURE_ROOT/admission.log"\n'
            '[[ "$1" == running-jobs ]] || exit 64\n'
            '[[ -z "${FIXTURE_NO_ANSWER:-}" ]] || exit 65\n'
            'count="$(grep -c running-jobs "$FIXTURE_ROOT/admission.log")"\n'
            'if ((count <= ${FIXTURE_BUSY_ANSWERS:-0})); then\n'
            '  echo "${FIXTURE_BUSY_UNIT:-foundation-source_sweep.service} activating"; exit 75; fi\n')
        running_jobs.chmod(0o755)
        # The commit's checkout exists already, so step 1 only moves `current` to it.
        new = control_root / "releases" / HEAD / "platforms/foundation-platform/scripts/deploy"
        new.mkdir(parents=True)
        (new / "foundation-release.sh").write_text(
            '#!/usr/bin/env bash\nprintf "%s\\n" "$1" >> "$FIXTURE_ROOT/release.log"\n'
            # What the deploy holds while this step runs (proposal 1, deploy D5).
            '[[ ! -f "$FIXTURE_ROOT/deploy-hold" ]] || cp "$FIXTURE_ROOT/deploy-hold" "$FIXTURE_ROOT/hold-at-$1"\n'
            '[[ "$1" != "${FIXTURE_FAIL_AT:-}" ]]\n')
        (new / "foundation-release.sh").chmod(0o755)
        # The host's release admission: here it refuses what the real one refuses first after a root
        # import, bytecode in the release (its own tests cover the rest).
        admission = control_root / "releases" / HEAD / "scripts/deploy/foundation-release-admission.py"
        admission.parent.mkdir(parents=True)
        admission.write_text('#!/usr/bin/env bash\n[[ "$1" == verify-current ]] || exit 64\n'
                             f'[[ -z "$(find {release_root}/current/ -name __pycache__)" ]]\n')
        admission.chmod(0o755)
        (root / "var").mkdir()
        # The deploy's own root-owned state (config/deploy-steps.contract.json state_directory's parent).
        (root / "var/lib/perfectory").mkdir(parents=True)
        (root / "var/lib/perfectory").chmod(0o755)
        conf = root / "release-deploy.conf"
        conf.write_text(f"FOUNDATION_DEPLOYER={getpass.getuser()}\n")
        text = DEPLOY.read_text(encoding="utf-8")
        text = text.replace('[[ ${EUID} == 0 ]] || { echo "foundation-deploy: run as root" >&2; exit 64; }\n', "", 1)
        for name, value in (("host_conf", conf), ("release_root", release_root), ("control_root", control_root),
                            ("mirror", root / "var/control-source.git"), ("deploy_hold", root / "deploy-hold")):
            line = next(line for line in text.splitlines() if line.startswith(f"{name}="))
            text = text.replace(line, bash_assignment(name, value), 1)
        text = text.replace("mktemp /var/lib/perfectory/", f"mktemp {root.as_posix()}/var/")
        text = text.replace("mktemp -d /var/lib/perfectory/", f"mktemp -d {root.as_posix()}/var/")
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
            # `git archive <sha> <paths>` to stdout: the commit's tree (the pre-flight's input).
            ("git", 'if [[ "$*" == *" -o "* ]]; then : > "${@: -2:1}"\n'
                    'elif [[ "$2" == archive ]]; then tar -C "$FIXTURE_ROOT/commit" -cf - .; fi\nexit 0\n'),
            ("chown", "exit 0\n"),
            ("sleep", 'printf "%s\\n" "$*" >> "$FIXTURE_ROOT/sleep.log"\n'),
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
            env={**os.environ, "PATH": f"{self.bin}:{os.environ['PATH']}", "FIXTURE_ROOT": str(self.root),
                 "FOUNDATION_DEPLOY_STEPS_HOST_ROOT": str(self.root), **env},
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

    def test_a_deploy_writes_no_bytecode_into_the_release_it_imports_from(self):
        # 2026-10-10: the unpause order imported job_specs from the running release as root, Python
        # wrote __pycache__ into it, and release admission refused every job as "writable".
        dags = self.root / "opt/foundation-platform/current/orchestration/dags"
        dags.mkdir()
        (dags / "job_specs.py").write_text(
            "def unpause_order():\n    return %r\n" % list(reversed(self.ENABLED)))
        env = {k: v for k, v in os.environ.items() if k != "PYTHONDONTWRITEBYTECODE"}
        result = subprocess.run(
            ["bash", str(self.script), HEAD],
            env={**env, "PATH": f"{self.bin}:{env['PATH']}", "FIXTURE_ROOT": str(self.root)},
            capture_output=True, text=True, check=False, timeout=120)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(list((self.root / "opt").rglob("__pycache__")), [])
        unpaused = [line.split()[-1].removeprefix("foundation_")
                    for line in (self.root / "airflow.log").read_text().splitlines() if line.startswith("dags unpause")]
        self.assertEqual(unpaused[-len(self.ENABLED):], list(reversed(self.ENABLED)))

    def test_the_deploy_waits_on_release_admissions_own_list_of_running_jobs(self):
        # 2026-10-11: the deploy kept its own list of the jobs to wait for (the enabled ones), reached
        # the release build beside a running disabled lane, and admission refused the build. Step 0 now
        # asks admission the question its build refuses on (root ADR-0184); systemd is not asked here.
        result = self.deploy(FIXTURE_BUSY_ANSWERS="2")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual((self.root / "admission.log").read_text().splitlines()[:3], ["running-jobs"] * 3)
        self.assertEqual((self.root / "sleep.log").read_text().split(), ["10", "10"])
        self.assertNotIn("ActiveState", (self.root / "systemctl.log").read_text())
        self.assertIn("no registered job is running", result.stdout)
        self.assertEqual(self.released()[0], "prepare")

    def commit_tells_what_it_changes(self, changed_table=None, artifacts_from=None):
        """The commit's release_changes.py and its inputs; the running release has the same contracts.
        With artifacts_from, the commit's release build contract says where its release comes from."""
        if artifacts_from:
            contract = self.root / "commit/tools/release-build.contract.json"
            contract.parent.mkdir(parents=True, exist_ok=True)
            contract.write_text(json.dumps({"artifacts_from": artifacts_from}))
        old = self.root / "opt/foundation-platform/current"
        new = self.root / "commit/platforms/foundation-platform"
        steps = json.loads((PLATFORM / "config/deploy-steps.contract.json").read_text(encoding="utf-8"))
        # The migration's inputs, its contract and the programs that read them, in both releases.
        shared = ("scripts/deploy/deploy_steps.py", "scripts/deploy/worker_autodeploy.py",
                  "config/deploy-steps.contract.json", *steps["steps"]["lakehouse-migrate"]["inputs"])
        for relative in ("scripts/deploy/release_changes.py", "orchestration/jobs.v1.json",
                         "docs/catalog/pipeline-graph.v1.json", *shared):
            for tree in (new, old) if relative in shared else (new,):
                (tree / relative).parent.mkdir(parents=True, exist_ok=True)
                if (PLATFORM / relative).is_dir():
                    # Bytecode another test left in the repository is not part of a release.
                    shutil.copytree(PLATFORM / relative, tree / relative, dirs_exist_ok=True,
                                    ignore=shutil.ignore_patterns("__pycache__"))
                else:
                    (tree / relative).write_bytes((PLATFORM / relative).read_bytes())
        # This host migrated the running release (its record), so the running release is the baseline.
        subprocess.run([sys.executable, "-I", "-B", str(PLATFORM / "scripts/deploy/deploy_steps.py"), "ran", str(old),
                        "lakehouse-migrate"], env={**os.environ, "FOUNDATION_DEPLOY_STEPS_HOST_ROOT": str(self.root)},
                       check=True, timeout=60)
        # The running release has applied the migrations the commit carries: none is new.
        shutil.copytree(self.root / "commit/platforms/foundation-platform/migrations", old / "migrations")
        if changed_table:
            path = (self.root / "commit/platforms/foundation-platform/infra/lakehouse/contracts"
                    / "industrial_complex_lakehouse_contracts.json")
            contracts = json.loads(path.read_text(encoding="utf-8"))
            contracts["contracts"][changed_table]["columns"].append(
                {"name": "added", "logical_type": "string", "required": False})
            path.write_text(json.dumps(contracts), encoding="utf-8")

    def test_the_deploy_logs_what_the_release_changes_before_it_pauses_anything(self):
        # Deploy D3 (root ADR-0270): the tables a release changes and their writer jobs.
        self.commit_tells_what_it_changes()
        result = self.deploy()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("lakehouse tables this release changes: none", result.stdout)
        self.assertLess(result.stdout.index("lakehouse tables this release changes"), result.stdout.index("0. pause"))

    def test_a_release_that_changes_no_table_gives_every_job_its_schedule_back_after_the_build(self):
        # Deploy D5 (root ADR-0270 §3): nothing is held, so a deploy that then stops while migrating
        # leaves every enabled job running, and says only the held ones (none) stay paused.
        self.commit_tells_what_it_changes()
        result = self.deploy(FIXTURE_FAIL_AT="migrate")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("jobs the deploy holds until the release is live: none", result.stdout)
        self.assertIn(f"unpaused {len(self.ENABLED)} DAGs; held until the release is live: none", result.stdout)
        self.assertEqual(self.dags(), {**{job: "unpause" for job in self.ENABLED},
                                       **{job: "pause" for job in self.DISABLED}})
        self.assertIn("THE HELD DAGS STAY PAUSED", result.stderr)
        self.assertLess(result.stdout.index("2b. the schedule comes back"), result.stdout.index("4. activate"))

    def test_a_changed_table_holds_its_writers_and_the_jobs_the_graph_cannot_place(self):
        # silver.building_register_floors: its writer is not on this fixture host; source_sweep and
        # outbox_publish name no table in the graph, so they are held; the fold writes another table.
        self.commit_tells_what_it_changes("silver.building_register_floors")
        result = self.deploy(FIXTURE_FAIL_AT="migrate")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.dags(), {"map_edit_fold_admin": "unpause", "source_sweep": "pause",
                                       "outbox_publish": "pause", "vworld_parcel_edition": "pause"})

    def test_the_held_jobs_get_their_schedule_back_once_the_release_is_live(self):
        self.commit_tells_what_it_changes("silver.building_register_floors")
        result = self.deploy()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("unpaused 1 DAGs; held until the release is live: ", result.stdout)
        self.assertEqual({job: state for job, state in self.dags().items() if job in self.ENABLED},
                         {job: "unpause" for job in self.ENABLED}, "step 7 gives the held jobs their schedule back")

    def test_the_deploy_holds_every_job_while_it_builds_then_only_what_it_holds_and_then_nothing(self):
        # Proposal 1: the operator's starts read this; it names the deploy's pid first.
        self.commit_tells_what_it_changes("silver.building_register_floors")
        result = self.deploy()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        building = (self.root / "hold-at-prepare").read_text().splitlines()
        live = (self.root / "hold-at-activate").read_text().splitlines()
        self.assertRegex(building[0], r"^pid [0-9]+$")
        self.assertEqual(building[1:], ["*"])
        self.assertEqual(live[0], building[0])
        self.assertIn("building_register_floor", live[1:])
        self.assertNotIn("*", live[1:])
        self.assertFalse((self.root / "deploy-hold").exists(), "a finished deploy holds nothing")

    # Root ADR-0308: a release from the registry is pulled, not built, so step 0 drains only what the
    # release holds. 2026-10-26: a release that held nothing paused all 33 DAGs and waited 1.5 h on
    # the by-PNU bake (ADR-0301); 2026-10-27: pausing and unpausing 33 DAGs twice took 2 min 50 s.

    def airflow_told(self, verb):
        return [line.split()[-1].removeprefix("foundation_")
                for line in (self.root / "airflow.log").read_text().splitlines() if line.startswith(f"dags {verb} ")]

    def test_a_registry_release_that_holds_nothing_touches_no_running_job(self):
        self.commit_tells_what_it_changes(artifacts_from="registry")
        result = self.deploy(FIXTURE_BUSY_ANSWERS="1000")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(f"{HEAD} holds no job: nothing is paused, waited for or stopped", result.stdout)
        self.assertEqual(self.airflow_told("pause"), [])
        self.assertEqual(self.airflow_told("unpause"), [], "nothing was paused, so nothing is unpaused")
        self.assertFalse((self.root / "admission.log").exists(), "nothing is asked what runs")
        self.assertFalse((self.root / "sleep.log").exists())
        self.assertNotIn("stop", (self.root / "systemctl.log").read_text())
        self.assertEqual((self.root / "hold-at-prepare").read_text().splitlines()[1:], [])
        self.assertIn("no unheld DAG was paused", result.stdout)

    def test_a_registry_release_pauses_only_what_it_holds_and_never_waits_for_another_job(self):
        # source_sweep and outbox_publish are held (the graph cannot place them); the fold is not,
        # and its run goes on through the whole deploy.
        self.commit_tells_what_it_changes("silver.building_register_floors", artifacts_from="registry")
        result = self.deploy(FIXTURE_BUSY_ANSWERS="1000", FIXTURE_BUSY_UNIT="foundation-map_edit_fold_admin.service")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(sorted(self.airflow_told("pause")), ["outbox_publish", "source_sweep"])
        self.assertIn("no held job is running", result.stdout)
        self.assertFalse((self.root / "sleep.log").exists(), "an unheld job is not waited for")
        self.assertEqual(sorted(self.airflow_told("unpause")), ["outbox_publish", "source_sweep"])
        self.assertEqual(self.dags(), {"outbox_publish": "unpause", "source_sweep": "unpause"})
        self.assertNotIn("dags pause foundation_vworld_parcel_edition", (self.root / "airflow.log").read_text())

    def test_a_held_job_that_is_not_interruptible_is_waited_for(self):
        self.commit_tells_what_it_changes("silver.building_register_floors", artifacts_from="registry")
        result = self.deploy(FIXTURE_BUSY_ANSWERS="2", FIXTURE_BUSY_UNIT="foundation-outbox_publish.service")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual((self.root / "sleep.log").read_text().split(), ["10", "10"])
        self.assertNotIn("stop", (self.root / "systemctl.log").read_text())

    def owed_runs_are_recorded(self):
        (self.root / "opt/foundation-platform/current/scripts/deploy/job_units.py").write_bytes(
            (PLATFORM / "scripts/deploy/job_units.py").read_bytes())
        owed = self.root / "scheduler/deferred"
        owed.parent.mkdir()
        return owed

    def test_a_held_interruptible_job_is_stopped_once_not_waited_for_for_hours(self):
        # Root ADR-0301 §2: 2026-10-26 an operator stopped the bake by hand after the drain had waited 1.5 h.
        self.commit_tells_what_it_changes("silver.building_register_floors", artifacts_from="registry")
        owed = self.owed_runs_are_recorded()
        result = self.deploy(FIXTURE_BUSY_ANSWERS="3", FOUNDATION_SCHEDULED_DEFERRED_DIR=str(owed))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        stops = [line for line in (self.root / "systemctl.log").read_text().splitlines() if line.startswith("stop")]
        self.assertEqual(stops, ["stop --no-block foundation-source_sweep.service"])
        self.assertIn("stopped foundation-source_sweep.service", result.stdout)

    def test_a_stopped_job_is_owed_a_run_and_started_when_the_schedule_comes_back(self):
        # 2026-10-29 review: a `*` deploy (a Postgres migration) stops every interruptible job, and one
        # with retries 0 (a 20 h bake) was run again by nothing. It is recorded before it is stopped and
        # step 7 starts it, the way a job a release build deferred is (root ADR-0260).
        owed = self.owed_runs_are_recorded()
        result = self.deploy(FIXTURE_BUSY_ANSWERS="3", FOUNDATION_SCHEDULED_DEFERRED_DIR=str(owed))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(sorted(path.name for path in owed.iterdir()), ["source_sweep"],
                         "the record stays until the runner starts it")
        self.assertIn("it is owed a run, started when the deploy gives the schedule back", result.stdout)
        log = (self.root / "airflow.log").read_text().splitlines()
        self.assertLess(log.index("dags unpause foundation_source_sweep"), log.index("dags trigger foundation_source_sweep"))
        self.assertIn("started foundation_source_sweep", result.stdout)

    def test_a_stopped_job_is_started_again_when_the_deploy_stops_before_the_switch(self):
        owed = self.owed_runs_are_recorded()
        result = self.deploy(FIXTURE_BUSY_ANSWERS="3", FOUNDATION_SCHEDULED_DEFERRED_DIR=str(owed), FIXTURE_FAIL_AT="prepare")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("dags trigger foundation_source_sweep", (self.root / "airflow.log").read_text())

    def test_a_job_that_cannot_be_recorded_as_owed_is_waited_for_not_stopped(self):
        # The running release has no job_units.py: nowhere to record the run it would lose.
        result = self.deploy(FIXTURE_BUSY_ANSWERS="3")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn("stop", (self.root / "systemctl.log").read_text())
        self.assertIn("waiting for it instead of stopping it", result.stderr)
        self.assertEqual((self.root / "sleep.log").read_text().split(), ["10", "10", "10"])

    def test_a_dag_airflow_has_not_listed_yet_is_unpaused(self):
        # 2026-10-29 review: a new job's DAG not parsed yet is created paused by Airflow; a filter that
        # unpaused only the DAGs listed as paused left it paused for good.
        self.commit_tells_what_it_changes(artifacts_from="registry")
        result = self.deploy(FIXTURE_UNLISTED="outbox_publish")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.airflow_told("unpause"), ["outbox_publish"])

    def test_a_release_the_host_builds_drains_every_job_as_before(self):
        # ADR-0137: the 22g builder is the host's one-shot job; by the memory budget nothing fits beside it.
        self.commit_tells_what_it_changes(artifacts_from="server-build")
        result = self.deploy()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(sorted(self.airflow_told("pause")), sorted(self.ENABLED + self.DISABLED))
        self.assertIn("no registered job is running", result.stdout)
        self.assertEqual((self.root / "hold-at-prepare").read_text().splitlines()[1:], ["*"])

    def test_a_job_started_once_starts_again_only_when_the_release_changes_what_it_runs(self):
        # 2026-10-28: about ten legal-dong collections a day (55 MB each) came from step 6, one from the
        # schedule. A job starts once when it never succeeded here or its inputs changed (ADR-0308).
        current = self.root / "opt/foundation-platform/current"
        for name in ("deploy_steps.py", "worker_autodeploy.py"):
            (current / "scripts/deploy" / name).write_bytes((PLATFORM / "scripts/deploy" / name).read_bytes())
        (current / "config").mkdir()
        (current / "config/deploy-steps.contract.json").write_text(json.dumps({
            "state_directory": "/var/lib/perfectory/deploy-steps", "steps": {
            "start-once.source_sweep": {"inputs": ["orchestration/jobs.v1.json"]},
            "start-once.outbox_publish": {"inputs": ["scripts/deploy/airflow-runtime.sh"]}}}))
        starts = lambda: [line for line in (self.root / "systemctl.log").read_text().splitlines()  # noqa: E731
                          if line.startswith("start")]
        control = self.root / "opt/perfectory-control/current"

        def again(**env):
            # Step 1 moved the control checkout to the commit; the next deploy starts from the trusted one.
            control.unlink()
            control.symlink_to(pathlib.Path("releases") / ("d" * 40))
            return self.deploy(**env)

        result = self.deploy(FIXTURE_FAILING="foundation-outbox_publish.service")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(starts()), 2, "the first deploy here starts both")
        result = again()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(starts()[2:], ["start foundation-outbox_publish.service"],
                         "the one that succeeded is skipped; the one that failed starts again")
        self.assertRegex(result.stdout, r"foundation-source_sweep\.service +skipped")
        (current / "orchestration/jobs.v1.json").write_text((current / "orchestration/jobs.v1.json").read_text() + "\n")
        result = again()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("start foundation-source_sweep.service", starts()[3:], "a changed input starts it again")

    def test_a_deploy_that_stops_leaves_no_hold_behind(self):
        result = self.deploy(FIXTURE_FAIL_AT="prepare")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual((self.root / "hold-at-prepare").read_text().splitlines()[1:], ["*"])
        self.assertFalse((self.root / "deploy-hold").exists())

    def test_a_deploy_that_cannot_tell_what_the_release_changes_goes_on(self):
        # Nothing reads the answer yet (D3): the fixture's commit holds no release_changes.py.
        result = self.deploy()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("cannot tell what", result.stderr)
        self.assertEqual(self.released()[0], "prepare")

    def test_a_deploy_that_cannot_tell_which_jobs_run_does_not_build(self):
        # No answer is not "nothing runs": the deploy stops before the release build.
        result = self.deploy(FIXTURE_NO_ANSWER="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("cannot tell which registered jobs run (exit 65)", result.stderr)
        self.assertEqual(self.released(), [])
        self.assertEqual(self.dags(), {**{job: "unpause" for job in self.ENABLED},
                                       **{job: "pause" for job in self.DISABLED}})

    def unapplied(self, migration):
        applied = (self.root / "applied").read_text().split()
        (self.root / "applied").write_text("\n".join(v for v in applied if v != version(migration)) + "\n")

    def assert_refused_before_anything(self, result, text):
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("REFUSED before anything was paused or restarted", result.stderr)
        self.assertIn(text, result.stderr)
        self.assertEqual(self.released(), [])  # nothing prepared, activated or migrated
        self.assertFalse((self.root / "airflow.log").exists())  # no DAG paused

    def test_a_pending_approval_migration_whose_setting_is_unset_stops_the_deploy_before_step_0(self):
        # The replay of 2026-10-14 (#465): its drop migration waited for a setting nobody had set.
        self.unapplied(GATED_MIGRATION)
        result = self.deploy()
        self.assert_refused_before_anything(result, "waits for foundation.adr_0194_drop_approved = 'yes'")
        self.assertIn("ALTER DATABASE foundation SET foundation.adr_0194_drop_approved = 'yes'", result.stderr)

    def test_a_pending_approval_migration_whose_setting_is_set_deploys(self):
        self.unapplied(GATED_MIGRATION)
        result = self.deploy(FIXTURE_SETTING="yes")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.released()[0], "prepare")

    def test_an_applied_approval_migration_needs_its_setting_no_longer(self):
        result = self.deploy(FIXTURE_SETTING="")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_a_database_that_cannot_be_asked_stops_the_deploy_before_step_0(self):
        self.assert_refused_before_anything(self.deploy(FIXTURE_DB_DOWN="1"), "could not be asked")

    def test_a_job_its_runner_deferred_while_the_release_built_is_started_once_unpaused(self):
        # Root ADR-0260: a job due during the build was not started (start-scheduled-job.sh); nothing
        # else would start it before its next scheduled slot, an hour or a day later.
        deploy = self.root / "opt/foundation-platform/current/scripts/deploy"
        (deploy / "job_units.py").write_bytes((PLATFORM / "scripts/deploy/job_units.py").read_bytes())
        deferred = self.root / "deferred"
        deferred.mkdir()
        for job in ("map_edit_fold_admin", "vworld_parcel_edition"):  # the second is switched off
            (deferred / job).write_text("0\n")
        result = self.deploy(FOUNDATION_SCHEDULED_DEFERRED_DIR=str(deferred))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        log = (self.root / "airflow.log").read_text().splitlines()
        self.assertEqual([line for line in log if line.startswith("dags trigger")],
                         ["dags trigger foundation_map_edit_fold_admin"])
        self.assertLess(log.index("dags unpause foundation_map_edit_fold_admin"),
                        log.index("dags trigger foundation_map_edit_fold_admin"))
        self.assertIn("started foundation_map_edit_fold_admin", result.stdout)

    def test_a_clean_deploy_ends_with_every_enabled_dag_unpaused(self):
        result = self.deploy()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        # The staging smoke runs on the installed release before anything of it is activated (root
        # ADR-0177); a smoke that passed lets the deploy go on to the switch.
        self.assertEqual(self.released(),
                         ["prepare", "floor-config", "staging-smoke", "activate", "migrate", "timers", "status",
                          "datahub"])
        self.assertEqual(self.dags(), {**{job: "unpause" for job in self.ENABLED},
                                       **{job: "pause" for job in self.DISABLED}})
        # Nothing was deferred, so nothing is started out of its schedule (root ADR-0260).
        self.assertNotIn("dags trigger", (self.root / "airflow.log").read_text())

    def test_a_catalog_that_does_not_come_up_fails_the_deploy_after_the_dags_are_back(self):
        # The release applies infra/datahub itself (2026-10-16: #628 merged and waited for sudo).
        # It runs last, so a catalog that fails alerts without holding any job.
        result = self.deploy(FIXTURE_FAIL_AT="datahub")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.released()[-2:], ["status", "datahub"])
        self.assertEqual(self.dags(), {**{job: "unpause" for job in self.ENABLED},
                                       **{job: "pause" for job in self.DISABLED}})
        self.assertNotIn("DONE: production runs", result.stdout)

    def test_a_failed_staging_smoke_refuses_the_release_and_leaves_the_schedule_as_it_was(self):
        # Root ADR-0177: the three incidents of 2026-10-09/10 each passed CI and failed on the first
        # real input. A smoke that fails stops the deploy before the switch: nothing of the new
        # release is activated or migrated, and the running release gets its DAGs back.
        result = self.deploy(FIXTURE_FAIL_AT="staging-smoke")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.released(), ["prepare", "floor-config", "staging-smoke"])
        self.assertEqual(self.dags(), {**{job: "unpause" for job in self.ENABLED},
                                       **{job: "pause" for job in self.DISABLED}})
        self.assertIn("stopped (exit 1) before the release switch", result.stderr)
        self.assertNotIn("DONE: production runs", result.stdout)

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
        self.assertIn("THE HELD DAGS STAY PAUSED", result.stderr)

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
                # foundation-deploy.sh waits each one's own limit for one start.
                self.assertGreater(job_specs.run_limit_minutes(job), job["time_limit_minutes"])

    def test_the_deploy_waits_the_limit_job_specs_gives_each_start(self):
        # foundation-deploy.sh adds time_limit_minutes and stop_grace_minutes itself (the release it
        # reads from may not carry job_specs); it must add them as job_specs does (root ADR-0270 §1).
        # The deploy starts the unit itself, so the start never waits: a run's limit, not the scheduler's.
        script = (PLATFORM / "scripts/deploy/foundation-deploy.sh").read_text(encoding="utf-8")
        program = script.split("jobs_of() {\n", 1)[1].split("<<'PY'\n", 1)[1].split("\nPY\n", 1)[0]
        waited = subprocess.run([sys.executable, "-c", program, "started-once", str(JOBS)],
                                capture_output=True, text=True, check=True).stdout.split()
        jobs = json.loads(JOBS.read_text(encoding="utf-8"))["jobs"]
        expected = []
        for job in jobs:
            if job["enabled"] and job.get("started_once_after_deploy"):
                expected += [job["systemd_service"], str(job_specs.run_limit_minutes(job)), job["id"]]
        self.assertTrue(expected)
        self.assertEqual(waited, expected)

    def test_systemd_never_stops_a_deploy_halfway(self):
        jobs = json.loads(JOBS.read_text(encoding="utf-8"))["jobs"]
        once = sum(job_specs.run_limit_minutes(job) + 1 for job in jobs if job.get("started_once_after_deploy"))
        unit = (PLATFORM / "infra/systemd/foundation-autodeploy.service").read_text()
        limit = next(line for line in unit.splitlines() if line.startswith("TimeoutStartSec="))
        hours = int(limit.removeprefix("TimeoutStartSec=").removesuffix("h"))
        # The staging smoke's own bound (root ADR-0177), read where foundation-release.sh reads it.
        smoke = json.loads((PLATFORM / "config/staging-gate.contract.json").read_text(encoding="utf-8"))
        smoke_minutes = -(-smoke["unit_limit_seconds"] // 60)
        # 5h waiting for running jobs, the staging smoke, an hour to build and migrate, then the
        # started jobs.
        self.assertGreaterEqual(hours * 60, 5 * 60 + smoke_minutes + 60 + once,
                                "a deploy can outlive its unit's limit")


if __name__ == "__main__":
    unittest.main()
