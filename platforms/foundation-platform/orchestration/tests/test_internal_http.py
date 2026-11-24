"""The daily jobs' one retry for the services they call (scripts/ops/internal_http.py).

foundation-data-quality.service failed a whole day on one `HTTP Error 503` from DataHub GMS. A
transient answer must be asked again with growing waits and a bound; a final answer (a 4xx, a
query error) must fail at once, and the data-quality job must reach both services through it.
"""

from __future__ import annotations

import importlib.util
import io
import pathlib
import subprocess
import sys
import tempfile
import types
import unittest
import urllib.error
import urllib.request
from contextlib import redirect_stderr
from unittest import mock

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "scripts/ops"))

import internal_http  # noqa: E402

FAST = internal_http.RetryPolicy(attempts=4, min_wait=1.0, max_wait=3.0)


def http_error(code):
    return urllib.error.HTTPError("http://gms/api/graphql", code, "x", {}, io.BytesIO(b""))


def failing(errors, answer="ok"):
    """An operation that raises each of `errors` in turn, then answers."""
    calls = []

    def operation():
        calls.append(None)
        if len(calls) <= len(errors):
            raise errors[len(calls) - 1]
        return answer

    return operation, calls


class CallTests(unittest.TestCase):
    def run_call(self, operation, transient=internal_http.http_transient):
        waits = []
        with redirect_stderr(io.StringIO()):
            answer = internal_http.call(operation, transient=transient, describe="t", policy=FAST,
                                        sleep=waits.append, unit=lambda: 1.0)
        return answer, waits

    def test_a_transient_answer_is_asked_again_with_growing_bounded_waits(self):
        operation, calls = failing([http_error(503), urllib.error.URLError("refused"), TimeoutError()])
        answer, waits = self.run_call(operation)
        self.assertEqual((answer, len(calls)), ("ok", 4))
        self.assertEqual(waits, [1.0, 2.0, 3.0])

    def test_the_bound_raises_the_last_failure(self):
        operation, calls = failing([http_error(503)] * 10)
        with self.assertRaises(urllib.error.HTTPError):
            self.run_call(operation)
        self.assertEqual(len(calls), FAST.attempts)

    def test_a_final_answer_is_not_asked_again(self):
        for error in (http_error(404), http_error(400), http_error(500), ValueError("bad json")):
            operation, calls = failing([error])
            with self.assertRaises(type(error)):
                self.run_call(operation)
            self.assertEqual(len(calls), 1, error)

    def test_equal_jitter_keeps_half_the_ceiling(self):
        self.assertEqual(internal_http.DAILY_JOB_POLICY.delay(1, 0.0), 0.5)
        self.assertEqual(internal_http.DAILY_JOB_POLICY.delay(10, 0.0), 15.0)
        self.assertEqual(internal_http.DAILY_JOB_POLICY.delay(10, 1.0), 30.0)


def load_quality_job():
    sys.modules.setdefault("yaml", types.ModuleType("yaml"))
    spec = importlib.util.spec_from_file_location("data_quality_check", PLATFORM / "scripts/ops/data-quality-check.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class DataQualityJobTests(unittest.TestCase):
    def setUp(self):
        self.job = load_quality_job()
        # The unit's StateDirectory on the host; here a directory of the test's own.
        state = tempfile.TemporaryDirectory()
        self.addCleanup(state.cleanup)
        for name, path in (("WORK", "duckdb"), ("FIRST_CHECKED", "first-checked.json")):
            setattr(self.job, name, pathlib.Path(state.name) / path)
        patcher = mock.patch.object(internal_http.time, "sleep")
        self.sleep = patcher.start()
        self.addCleanup(patcher.stop)
        quiet = mock.patch.object(sys, "stderr", io.StringIO())
        quiet.start()
        self.addCleanup(quiet.stop)

    def test_one_gms_503_does_not_fail_the_day(self):
        # The one question the job still asks DataHub, once per contract (root ADR-0609 §2).
        answers = [http_error(503), FakeResponse(b'{"data": {"dataset": null}}')]

        def urlopen(request, timeout):
            answer = answers.pop(0)
            if isinstance(answer, Exception):
                raise answer
            return answer

        with mock.patch.object(urllib.request, "urlopen", urlopen):
            self.assertFalse(self.job.checked_in_datahub("contract"))
        self.assertEqual(self.sleep.call_count, 1)

    def publisher_answers(self, *exits):
        """subprocess.run as the publisher's `lakehouse-query` would exit, in turn."""
        def run(argv, **_options):
            status, output = exits[len(calls)]
            calls.append(argv)
            if status:
                raise subprocess.CalledProcessError(status, argv, output, "the catalog did not answer")
            return subprocess.CompletedProcess(argv, 0, output, "")
        calls = []
        return run, calls

    def test_a_publisher_run_whose_catalog_did_not_answer_is_run_again(self):
        answer = '{"table": "silver.t", "snapshot_id": 7, "rows": []}\n'
        run, calls = self.publisher_answers((1, ""), (0, answer))
        with mock.patch.object(self.job.subprocess, "run", run):
            answers = self.job.lakehouse([("silver.t", "DESCRIBE SELECT * FROM {source}", None)])
        self.assertEqual(answers, [{"table": "silver.t", "snapshot_id": 7, "rows": []}])
        self.assertEqual(len(calls), 2)
        self.assertIn("lakehouse-query", calls[0][2], "the admitted release's publisher, through its runtime")

    def test_a_publisher_that_stays_down_fails_the_run_instead_of_hiding_tables(self):
        run, calls = self.publisher_answers(*[(1, "")] * internal_http.DAILY_JOB_POLICY.attempts)
        with mock.patch.object(self.job.subprocess, "run", run):
            with self.assertRaises(subprocess.CalledProcessError):
                self.job.lakehouse([("silver.t", "DESCRIBE SELECT * FROM {source}", None)])
        self.assertEqual(len(calls), internal_http.DAILY_JOB_POLICY.attempts)

    def test_an_answer_short_of_the_queries_fails_at_once(self):
        run, calls = self.publisher_answers((0, '{"table": "silver.t", "snapshot_id": 7, "rows": []}\n'))
        with mock.patch.object(self.job.subprocess, "run", run):
            with self.assertRaises(RuntimeError):
                self.job.lakehouse([("silver.t", "SELECT 1 FROM {source}", None), ("silver.u", "SELECT 1 FROM {source}", None)])
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
