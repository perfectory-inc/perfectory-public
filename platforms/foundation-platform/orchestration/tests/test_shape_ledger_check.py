"""The shape ledger check fails on every way production can leave a committed ledger (root ADR-0259)."""

import hashlib
import importlib.util
import tempfile
import unittest.mock
import json
import pathlib
import unittest
from datetime import datetime, timezone

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("shape_ledger_check", PLATFORM / "scripts/ops/shape-ledger-check.py")
check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check)


class Differences(unittest.TestCase):
    SHAPE = {"has_rows": True, "fields": [[1, "a", "string", True]], "columns": [1], "earlier": []}

    def lakehouse(self, **tables):
        return {"captured_at": "then", "tables": tables}

    def test_the_same_shape_captured_later_is_no_difference(self):
        self.assertEqual(check.differences(self.lakehouse(t=self.SHAPE),
                                           {**self.lakehouse(t=self.SHAPE), "captured_at": "now"}, "tables"), [])

    def test_a_new_table_a_gone_table_and_a_changed_shape_are_each_named(self):
        rewritten = {**self.SHAPE, "earlier": [[1, 2]]}
        found = check.differences(self.lakehouse(kept=self.SHAPE, gone=self.SHAPE),
                                  self.lakehouse(kept=rewritten, new=self.SHAPE), "tables")
        self.assertEqual(found, ["new: new in production", "gone: gone from production", "kept: shape changed"])

    def test_postgres_tables_that_gained_or_lost_rows_are_named(self):
        found = check.differences({"tables_with_rows": ["a", "b"]}, {"tables_with_rows": ["b", "c"]},
                                  "tables_with_rows")
        self.assertEqual(found, ["c: now holds rows", "a: no longer holds rows"])

    def test_the_committed_ledgers_are_where_the_check_reads_them(self):
        for relative, key in ((check.LAKEHOUSE, "tables"), (check.POSTGRES, "tables_with_rows")):
            self.assertIn(key, json.loads((PLATFORM / relative).read_text(encoding="utf-8")))

    def test_it_runs_after_every_deploy_and_daily(self):
        jobs = json.loads((PLATFORM / "orchestration/jobs.v1.json").read_text(encoding="utf-8"))["jobs"]
        job = next(job for job in jobs if job["id"] == "shape_ledger_check")
        self.assertTrue(job["enabled"] and job["started_once_after_deploy"])
        self.assertEqual(job["systemd_service"], "foundation-shape-ledger-check.service")


class WaitingThenFailed(unittest.TestCase):
    """A stale ledger waits on its refresh pull request, and pages once it has waited 48 hours."""

    NOW = check.datetime(2026, 10, 16, 12, 0, tzinfo=check.timezone.utc)

    def test_within_48_hours_the_run_waits_and_names_the_pull_request(self):
        first = self.NOW - check.timedelta(hours=47)
        line, status = check.verdict(self.NOW, first, 2, "https://github.com/o/r/pull/9")
        self.assertEqual(status, 0)
        self.assertTrue(line.startswith("foundation-job-outcome waiting shape ledgers stale for 2 tables"))
        self.assertIn("pull/9", line)

    def test_after_48_hours_the_run_fails(self):
        line, status = check.verdict(self.NOW, self.NOW - check.timedelta(hours=48), 2, "pr")
        self.assertEqual(status, 1)
        self.assertTrue(line.startswith("foundation-job-outcome failed"))

    def test_first_seen_is_kept_across_runs_and_cleared_when_current(self):
        with tempfile.TemporaryDirectory() as state:
            with unittest.mock.patch.object(check, "STATE", pathlib.Path(state)):
                first = check.first_seen_at(self.NOW, True)
                later = check.first_seen_at(self.NOW + check.timedelta(hours=5), True)
                self.assertEqual(first, later)
                self.assertIsNone(check.first_seen_at(self.NOW, False))
                self.assertFalse((pathlib.Path(state) / check.FIRST_SEEN).exists())


class FakeGitHub:
    """Answers the calls refresh_pull_request makes; a tree's sha is its content."""

    def __init__(self, branch_tree=None, open_pr=False):
        self.calls, self.branch_tree, self.open_pr = [], branch_tree, open_pr

    def __call__(self, token, method, path, body=None):
        self.calls.append((method, path.split("?")[0]))
        if path.endswith("/git/ref/heads/main"):
            return {"object": {"sha": "main"}}
        if path.endswith(f"/git/ref/heads/{check.BRANCH}"):
            return {"object": {"sha": "branch"}} if self.branch_tree else None
        if "/git/commits/" in path:
            return {"tree": {"sha": "base" if path.endswith("/main") else self.branch_tree}}
        if path.endswith("/git/trees"):
            return {"sha": json.dumps(body["tree"], sort_keys=True)}
        if path.endswith("/git/commits"):
            return {"sha": "new"}
        if "/pulls" in path and method == "GET":
            return [{"number": 7, "html_url": "https://github.com/o/r/pull/7"}] if self.open_pr else []
        if path.endswith("/pulls"):
            return {"html_url": "https://github.com/o/r/pull/8"}
        return {}


class TheRefreshPullRequest(unittest.TestCase):
    FILES = {"platforms/foundation-platform/config/postgres-production-shape.json": "{}\n"}

    def run_with(self, fake):
        with unittest.mock.patch.object(check, "github", fake):
            return check.refresh_pull_request("t", "o/r", self.FILES, ["postgres x: now holds rows"])

    def test_a_first_stale_run_creates_the_branch_and_opens_the_pull_request(self):
        fake = FakeGitHub()
        self.assertEqual(self.run_with(fake), "https://github.com/o/r/pull/8")
        self.assertIn(("POST", "/repos/o/r/git/refs"), fake.calls)
        self.assertIn(("POST", "/repos/o/r/pulls"), fake.calls)

    def test_the_same_capture_again_adds_no_commit_and_updates_the_open_pull_request(self):
        tree = json.dumps([{"path": path, "mode": "100644", "type": "blob", "content": content}
                           for path, content in self.FILES.items()], sort_keys=True)
        fake = FakeGitHub(branch_tree=tree, open_pr=True)
        self.assertEqual(self.run_with(fake), "https://github.com/o/r/pull/7")
        self.assertNotIn(("POST", "/repos/o/r/git/commits"), fake.calls)
        self.assertIn(("PATCH", "/repos/o/r/pulls/7"), fake.calls)

    def test_a_new_capture_force_moves_the_one_branch(self):
        fake = FakeGitHub(branch_tree="older", open_pr=True)
        self.run_with(fake)
        self.assertIn(("PATCH", f"/repos/o/r/git/refs/heads/{check.BRANCH}"), fake.calls)


class Publish(unittest.TestCase):
    """Root ADR-0279: the host puts its captures in the private ledger bucket; CI reads them there."""

    KEY = dict(zip(check.PUBLISH_ENV, ("ledger-bucket", "account.example.invalid", "key-id", "key-secret")))
    FILES = {"lakehouse-production-shape.json": "{}\n", "postgres-production-shape.json": "{}\n"}

    def publish(self, environment, r2=None):
        with unittest.mock.patch.dict(check.os.environ, environment, clear=True), \
                unittest.mock.patch.object(check, "release_module", return_value=r2) as module:
            return check.publish(self.FILES), module

    def test_without_the_key_nothing_is_sent_and_the_files_to_copy_are_named(self):
        said, module = self.publish({})
        module.assert_not_called()
        self.assertIn("no ledger bucket key", said)

    def test_each_capture_goes_under_the_prefix_by_its_ledger_file_name(self):
        r2 = unittest.mock.Mock()
        said, _ = self.publish(self.KEY, r2)
        self.assertEqual([c.args[:3] for c in r2.put.call_args_list],
                         [("https://account.example.invalid", "ledger-bucket", "shape-ledger/" + name)
                          for name in self.FILES])
        self.assertIn("published to ledger-bucket/shape-ledger/", said)

    def test_a_failed_upload_is_said_and_does_not_fail_the_run(self):
        r2 = unittest.mock.Mock()
        r2.put.side_effect = OSError("connection reset")
        said, _ = self.publish(self.KEY, r2)
        self.assertIn("not published (connection reset)", said)


r2_spec = importlib.util.spec_from_file_location("r2_object", PLATFORM / "scripts/ops/r2_object.py")
r2_object = importlib.util.module_from_spec(r2_spec)
r2_spec.loader.exec_module(r2_object)


class SignatureVersion4(unittest.TestCase):
    def test_the_signature_is_the_s3_documentation_get_object_example(self):
        # "Example: GET Object" of the S3 Signature Version 4 documentation, its signature included.
        empty = hashlib.sha256(b"").hexdigest()
        request, signed = r2_object.canonical_request("GET", "/test.txt", {
            "host": "examplebucket.s3.amazonaws.com", "range": "bytes=0-9",
            "x-amz-content-sha256": empty, "x-amz-date": "20130524T000000Z"}, empty)
        self.assertEqual(signed, "host;range;x-amz-content-sha256;x-amz-date")
        when = datetime(2013, 5, 24, tzinfo=timezone.utc)
        self.assertEqual(r2_object.signature("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", when, "us-east-1", request),
                         "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")

    def test_a_put_carries_its_body_hash_and_a_path_style_url(self):
        when = datetime(2026, 10, 19, tzinfo=timezone.utc)
        request = r2_object.signed_request("PUT", "https://account.example.invalid", "bucket", "shape-ledger/a.json",
                                           "key-id", "key-secret", b"{}", when)
        self.assertEqual(request.full_url, "https://account.example.invalid/bucket/shape-ledger/a.json")
        self.assertEqual(request.get_header("X-amz-content-sha256"), hashlib.sha256(b"{}").hexdigest())
        self.assertTrue(request.get_header("Authorization").startswith(
            "AWS4-HMAC-SHA256 Credential=key-id/20261019/auto/s3/aws4_request, "))


if __name__ == "__main__":
    unittest.main()
