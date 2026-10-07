"""The host deploys main by itself (scripts/deploy/foundation-autodeploy.sh, root ADR-0159).

A test copy of the script points its fixed paths at a fixture tree: the control checkout's Git
transport answers main's head, its release_checks.py answers a verdict, and its foundation-deploy.sh
records what it was asked to deploy and exits as told. release_checks.py itself is tested against
a local stand-in for the GitHub API.
"""

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
JOBS = PLATFORM / "orchestration/jobs.v1.json"
CURRENT = "a" * 40
HEAD = "b" * 40

sys.path.insert(0, str(CHECKS.parent))
import release_checks  # noqa: E402


def run(name, status="completed", conclusion="success"):
    return {"name": name, "status": status, "conclusion": conclusion}


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


class TheChecksAreReadPageByPage(unittest.TestCase):
    def serve(self, runs):
        class Handler(http.server.BaseHTTPRequestHandler):
            asked = []

            def do_GET(self):
                Handler.asked.append(self.path)
                page = int(self.path.rsplit("page=", 1)[1])
                body = json.dumps({"total_count": len(runs), "check_runs": runs[(page - 1) * 100:page * 100]})
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
        self.assertEqual(len(asked), 2)

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

    def tick(self, head=HEAD, checks="deploy 49 check runs passed", **env):
        result = subprocess.run(
            ["bash", str(self.script)],
            env={**os.environ, "FIXTURE_ROOT": str(self.root), "FIXTURE_HEAD": head, "FIXTURE_CHECKS": checks, **env},
            capture_output=True, text=True, check=False,
        )
        return result

    def deployed(self):
        log = self.root / "deployed.log"
        return log.read_text().split() if log.exists() else []

    def test_a_host_already_on_main_does_nothing(self):
        result = self.tick(head=CURRENT)
        self.assertEqual((result.returncode, result.stdout), (0, ""))
        self.assertEqual(self.deployed(), [])

    def test_a_new_commit_whose_checks_passed_is_deployed_once(self):
        result = self.tick()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.deployed(), [HEAD])
        self.assertTrue((self.state / "deployed" / HEAD).exists())

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
