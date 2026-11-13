"""The release verdict the host's autodeploy asks for (scripts/deploy/release_checks.py, root ADR-0159).

release_checks.py is tested against a local stand-in for the GitHub API. Split from
test_foundation_autodeploy.py, which tests the autodeploy and the deploy that use its verdict.
"""

import http.server
import json
import pathlib
import subprocess
import sys
import tempfile
import threading
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
CHECKS = PLATFORM / "scripts/deploy/release_checks.py"
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


if __name__ == "__main__":
    unittest.main()
