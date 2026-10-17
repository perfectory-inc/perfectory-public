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
import subprocess
import sys
import tempfile
import threading
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
        (deploy / "release_checks.py").write_text(
            "import os, sys\n"
            "open(os.path.join(os.environ['FIXTURE_ROOT'], 'asked.log'), 'a').write(sys.argv[3] + '\\n')\n"
            "print(os.environ['FIXTURE_CHECKS'])\n")
        (deploy / "foundation-deploy.sh").write_text(
            'printf "%s\\n" "$1" >> "$FIXTURE_ROOT/deployed.log"\nexit "${FIXTURE_DEPLOY_EXIT:-0}"\n')
        # Whether a unit runs is the real definition (root ADR-0184), asking the systemctl below.
        (deploy / JOB_UNITS.name).write_bytes(JOB_UNITS.read_bytes())
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
                 "timeout_minutes": once.get(job, 30), "started_once_after_deploy": job in once}
                for job in self.ENABLED + self.DISABLED]
        (running / "orchestration/jobs.v1.json").write_text(json.dumps({"jobs": jobs}))
        # airflow-runtime.sh exec airflow-scheduler airflow <args>: record <args>.
        (running / "scripts/deploy/airflow-runtime.sh").write_text(
            'if [[ "$1" == exec ]]; then shift 3; printf "%s\\n" "$*" >> "$FIXTURE_ROOT/airflow.log"\n'
            '  [[ -z "${FIXTURE_AIRFLOW_FAILS:-}" || "$*" != *"$FIXTURE_AIRFLOW_FAILS"* ]]; fi\n')
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
            'if ((count <= ${FIXTURE_BUSY_ANSWERS:-0})); then echo "foundation-source_sweep.service activating"; exit 75; fi\n')
        running_jobs.chmod(0o755)
        # The commit's checkout exists already, so step 1 only moves `current` to it.
        new = control_root / "releases" / HEAD / "platforms/foundation-platform/scripts/deploy"
        new.mkdir(parents=True)
        (new / "foundation-release.sh").write_text(
            '#!/usr/bin/env bash\nprintf "%s\\n" "$1" >> "$FIXTURE_ROOT/release.log"\n'
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
        conf = root / "release-deploy.conf"
        conf.write_text(f"FOUNDATION_DEPLOYER={getpass.getuser()}\n")
        text = DEPLOY.read_text(encoding="utf-8")
        text = text.replace('[[ ${EUID} == 0 ]] || { echo "foundation-deploy: run as root" >&2; exit 64; }\n', "", 1)
        for name, value in (("host_conf", conf), ("release_root", release_root), ("control_root", control_root),
                            ("mirror", root / "var/control-source.git")):
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
                # foundation-deploy.sh waits each one's own limit for one start.
                self.assertGreater(job_specs.scheduler_timeout_minutes(job), job["time_limit_minutes"])

    def test_the_deploy_waits_the_limit_job_specs_gives_each_start(self):
        # foundation-deploy.sh adds time_limit_minutes and stop_grace_minutes itself (the release it
        # reads from may not carry job_specs); it must add them as job_specs does (root ADR-0270 §1).
        script = (PLATFORM / "scripts/deploy/foundation-deploy.sh").read_text(encoding="utf-8")
        program = script.split("jobs_of() {\n", 1)[1].split("<<'PY'\n", 1)[1].split("\nPY\n", 1)[0]
        waited = subprocess.run([sys.executable, "-c", program, "started-once", str(JOBS)],
                                capture_output=True, text=True, check=True).stdout.split()
        jobs = json.loads(JOBS.read_text(encoding="utf-8"))["jobs"]
        expected = []
        for job in jobs:
            if job["enabled"] and job.get("started_once_after_deploy"):
                expected += [job["systemd_service"], str(job_specs.scheduler_timeout_minutes(job))]
        self.assertTrue(expected)
        self.assertEqual(waited, expected)

    def test_systemd_never_stops_a_deploy_halfway(self):
        jobs = json.loads(JOBS.read_text(encoding="utf-8"))["jobs"]
        once = sum(job_specs.scheduler_timeout_minutes(job) + 1 for job in jobs if job.get("started_once_after_deploy"))
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
