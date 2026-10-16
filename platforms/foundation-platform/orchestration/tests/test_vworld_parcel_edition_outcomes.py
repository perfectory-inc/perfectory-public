"""The cadastral edition job says how each run ended, and a new edition is not a failure (root ADR-0148, ADR-0171).

2026-10-15: the supervised run found edition 202610, collected it and proposed its contract entry, which
the job reports with exit 3 by design (since root ADR-0224: the edition awaits its Silver load); the unit
had no SuccessExitStatus, so systemd recorded a failure, paged through OnFailure, and the operations map would have shown a red run every day until the contract
PR. The held path printed no outcome line either, which ADR-0171 reads as "changed".
"""

import pathlib
import re
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = (PLATFORM / "scripts/ops/vworld-parcel-edition-collect.sh").read_text(encoding="utf-8")
UNIT = (PLATFORM / "infra/systemd/foundation-vworld-parcel-edition.service").read_text(encoding="utf-8")


class EditionJobOutcomes(unittest.TestCase):
    def test_awaiting_the_silver_load_is_a_success_status_of_the_unit(self):
        awaiting = re.search(r"^AWAITING_LOAD=(\d+)$", SCRIPT, re.MULTILINE)
        self.assertIsNotNone(awaiting)
        statuses = re.findall(r"^SuccessExitStatus=(.+)$", UNIT, re.MULTILINE)
        self.assertIn(awaiting.group(1), " ".join(statuses).split())

    def test_every_ending_that_is_not_a_failure_says_its_outcome_first(self):
        lines = SCRIPT.splitlines()
        endings = [i for i, line in enumerate(lines)
                   if re.fullmatch(r'\s*exit (0|"\$\{AWAITING_LOAD\}")\s*', line)]
        self.assertEqual(len(endings), 2, "the held and the new-edition endings")
        for index in endings:
            with self.subTest(line=lines[index].strip()):
                self.assertIn("foundation-job-outcome unchanged", lines[index - 1])


if __name__ == "__main__":
    unittest.main()
