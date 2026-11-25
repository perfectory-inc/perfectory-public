"""The stewardship cycle's own lineage alert names what a person acts on (root ADR-0113, ADR-0145 §2).

The unit's failure notice says only that it failed. When the lineage refuses a sido group because
the code change table does not pair some 동 codes, the cycle posts one line per refused group with
those codes, so the alert is actionable without reading the state directory (which an operator
cannot). The function is read out of the script and run, on synthetic codes.
"""

import json
import pathlib
import re
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts/ops/lineage-stewardship-cycle.sh"


def refusal_function() -> str:
    text = SCRIPT.read_text(encoding="utf-8")
    match = re.search(r"^lineage_refusal_text\(\) \{\n.*?^\}\n", text, re.M | re.S)
    if match is None:
        raise AssertionError("lineage-stewardship-cycle.sh defines no lineage_refusal_text function")
    return match.group(0)


class TheRefusalAlert(unittest.TestCase):
    def text(self, summary) -> str:
        with tempfile.TemporaryDirectory(prefix="lineage-cycle-") as directory:
            path = pathlib.Path(directory) / "derive-summary.json"
            path.write_text(json.dumps(summary), encoding="utf-8")
            result = subprocess.run(["bash", "-c", f'{refusal_function()}lineage_refusal_text "$1"', "-", str(path)],
                                    capture_output=True, text=True, encoding="utf-8", timeout=60, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout

    def test_every_refused_group_and_its_unpaired_codes_are_named(self):
        out = self.text({"pairs": [{"from": "209906", "to": "209909", "refused": [
            {"from_sido": "98", "to_sido": "98", "reason": "no pair", "unpaired_codes": ["9999910200", "9999910800"]},
            {"from_sido": "97", "to_sido": "97", "reason": "no parcels for one side", "unpaired_codes": []},
        ]}]})
        self.assertIn("시도 98→98: 9999910200, 9999910800", out)
        # A refusal without codes still says why.
        self.assertIn("시도 97→97: (코드 없음: no parcels for one side)", out)

    def test_the_cycle_posts_it_before_it_fails(self):
        text = SCRIPT.read_text(encoding="utf-8")
        block = text[text.index('if [ "${derive_refused}" != 0 ]; then'):]
        self.assertLess(block.index("lineage_refusal_text"), block.index("exit 1"))


class TheDerivationRunsInThePublisher(unittest.TestCase):
    """Root ADR-0559: the scheduled lineage derivation is the publisher's `derive-parcel-lineage`
    (DuckDB and the publisher's own Iceberg append), not a Spark submission. 2026-11-11: one Spark
    pool held 18 jobs waiting while JVM jobs failed on memory."""

    def test_the_cycle_calls_the_publisher_and_submits_no_lineage_job_to_spark(self):
        text = SCRIPT.read_text(encoding="utf-8")
        self.assertIn('"${PUBLISHER_BIN}" derive-parcel-lineage', text)
        self.assertNotIn("spark parcel_lineage", text)
        # The summary the later steps read is the one the command writes.
        start = text.index("FOUNDATION_PLATFORM_PARCEL_LINEAGE_SUMMARY=")
        call = text[start:text.index('"${PUBLISHER_BIN}" derive-parcel-lineage', start)]
        self.assertIn('"${work}/derive-summary.json"', call)
        self.assertIn("FOUNDATION_PLATFORM_PARCEL_LINEAGE_DEADLINE_MINUTES=", call)

    def test_the_journal_line_names_the_engine_the_run_derived_with(self):
        text = SCRIPT.read_text(encoding="utf-8")
        self.assertIn('lineage derive engine=$(json_field "${work}/derive-summary.json" \'["engine"]\')', text)


class QueueTakenOver(unittest.TestCase):
    """The steward queue is the publisher's table (root ADR-0564 §6-9): the cycle builds it after the
    fold and loads each unit's handoff from it; no queue job goes to Spark."""

    def test_the_cycle_builds_the_queue_after_the_fold_and_loads_both_units(self):
        text = SCRIPT.read_text(encoding="utf-8")
        for gone in ("lineage_review_queue_to_gold", "place_reference_queue_to_gold", "lineage-queue-check"):
            self.assertNotIn(gone, text)
        build = text.index('"${PUBLISHER_BIN}" build-lineage-review-queue-versions')
        self.assertLess(text.index('"${PUBLISHER_BIN}" append-lineage-steward-fold'), build)
        self.assertIn('FOUNDATION_PLATFORM_LINEAGE_REVIEW_HANDOFF_DIR="${queue_dir}"', text[:build])
        load = text[build:]
        self.assertIn("for unit in parcel place_reference; do", load)
        self.assertIn('FOUNDATION_PLATFORM_LINEAGE_REVIEW_HANDOFF_INPUT="${queue_dir}/${unit}.json"', load)

    def test_a_cycle_before_the_lineage_exists_waits(self):
        text = SCRIPT.read_text(encoding="utf-8")
        build = text.index('"${PUBLISHER_BIN}" build-lineage-review-queue-versions')
        waiting = text.index('[ "${queue_outcome}" = waiting ]', build)
        self.assertIn("foundation-job-outcome waiting silver.parcel_lineage does not exist yet", text[waiting:])


if __name__ == "__main__":
    unittest.main()
