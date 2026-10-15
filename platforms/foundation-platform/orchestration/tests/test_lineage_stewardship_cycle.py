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


if __name__ == "__main__":
    unittest.main()
