"""The stewardship cycle skips the queue's Spark run while the lineage has not moved (root ADR-0277's shape)."""

import contextlib
import importlib.util
import io
import pathlib
import re
import subprocess
import tempfile
import unittest
import unittest.mock

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts/ops/lineage-stewardship-cycle.sh"
spec = importlib.util.spec_from_file_location("lineage_queue_check", PLATFORM / "scripts/ops/lineage-queue-check.py")
check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check)


def queue(recorded):
    summary = {} if recorded is None else {check.PROPERTY: recorded}
    return {"current-snapshot-id": 7, "snapshots": [{"snapshot-id": 6, "summary": {check.PROPERTY: "41"}},
                                                    {"snapshot-id": 7, "summary": summary}]}


class Verdict(unittest.TestCase):
    def test_the_queue_built_from_the_current_lineage_is_unchanged(self):
        self.assertEqual(check.verdict({"current-snapshot-id": 42}, queue("42")), "unchanged 42")

    def test_a_lineage_appended_since_or_a_queue_that_recorded_nothing_would_change(self):
        self.assertEqual(check.verdict({"current-snapshot-id": 43}, queue("42")), "would_change")
        self.assertEqual(check.verdict({"current-snapshot-id": 42}, queue(None)), "would_change")
        # Only the queue's current commit counts, not an older one that happened to record it.
        self.assertEqual(check.verdict({"current-snapshot-id": 41}, queue("42")), "would_change")

    def test_an_empty_lineage_is_never_unchanged(self):
        self.assertEqual(check.verdict({"current-snapshot-id": -1}, queue("-1")), "would_change")

    def test_a_check_that_cannot_reach_the_catalog_lets_the_run_happen(self):
        out = io.StringIO()
        with unittest.mock.patch.dict(check.os.environ, {}, clear=True), contextlib.redirect_stdout(out):
            self.assertEqual(check.main(), 0)
        self.assertTrue(out.getvalue().startswith("would_change ("))

    def test_the_job_records_the_property_the_check_reads(self):
        job = (PLATFORM / "infra/lakehouse/spark/jobs/lineage_review_queue_to_gold.py").read_text(encoding="utf-8")
        self.assertIn(f'LINEAGE_SNAPSHOT_PROPERTY = "{check.PROPERTY}"', job)


def queue_unchanged_function() -> str:
    match = re.search(r"^queue_unchanged\(\) \{\n.*?^\}\n", SCRIPT.read_text(encoding="utf-8"), re.M | re.S)
    if match is None:
        raise AssertionError("lineage-stewardship-cycle.sh defines no queue_unchanged function")
    return match.group(0)


class TheCycleSkips(unittest.TestCase):
    def skips(self, verdict: str, summary: str | None) -> bool:
        with tempfile.TemporaryDirectory(prefix="lineage-queue-") as directory:
            path = pathlib.Path(directory) / "last-queue-summary.json"
            if summary is not None:
                path.write_text(summary, encoding="utf-8")
            result = subprocess.run(["bash", "-c", f'{queue_unchanged_function()}queue_unchanged "$1" "$2"', "-",
                                     verdict, path.as_posix()], capture_output=True, text=True, timeout=60, check=False)
        return result.returncode == 0

    def test_only_an_unchanged_lineage_with_a_kept_summary_skips(self):
        self.assertTrue(self.skips("unchanged 42", '{"open": 857}'))
        self.assertFalse(self.skips("unchanged 42", None), "no successful load kept: run")
        self.assertFalse(self.skips("unchanged 42", ""), "an empty summary is no proof: run")
        self.assertFalse(self.skips("would_change", '{"open": 857}'))
        self.assertFalse(self.skips("would_change (URLError: down)", '{"open": 857}'))

    def test_the_summary_is_kept_only_after_the_load(self):
        text = SCRIPT.read_text(encoding="utf-8")
        branch = text[text.index('if queue_unchanged "${queue_check}"'):]
        branch = branch[:branch.index("\nfi\n")]
        self.assertLess(branch.index("load-lineage-review-items"),
                        branch.index('cp "${work}/queue-summary.json" "${last_queue_summary}"'))
        self.assertLess(branch.index("lineage_review_queue_to_gold.py"), branch.index("load-lineage-review-items"))


if __name__ == "__main__":
    unittest.main()
