"""The cadastral edition job says how each run ended, and a new edition is not a failure (root ADR-0148, ADR-0171).

2026-10-15: the supervised run found edition 202610, collected it and proposed its contract entry, which
the job reports with exit 3 by design; the unit had no SuccessExitStatus, so systemd recorded a failure,
paged through OnFailure, and the operations map would have shown a red run every day until the contract
PR. The held path printed no outcome line either, which ADR-0171 reads as "changed".
"""

import pathlib
import re
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = (PLATFORM / "scripts/ops/vworld-parcel-edition-collect.sh").read_text(encoding="utf-8")
UNIT = (PLATFORM / "infra/systemd/foundation-vworld-parcel-edition.service").read_text(encoding="utf-8")


class EditionJobOutcomes(unittest.TestCase):
    def test_awaiting_the_contract_is_a_success_status_of_the_unit(self):
        awaiting = re.search(r"^AWAITING_CONTRACT=(\d+)$", SCRIPT, re.MULTILINE)
        self.assertIsNotNone(awaiting)
        statuses = re.findall(r"^SuccessExitStatus=(.+)$", UNIT, re.MULTILINE)
        self.assertIn(awaiting.group(1), " ".join(statuses).split())

    def test_every_ending_that_is_not_a_failure_says_its_outcome_first(self):
        lines = SCRIPT.splitlines()
        endings = [i for i, line in enumerate(lines)
                   if re.fullmatch(r'\s*exit (0|"\$\{AWAITING_CONTRACT\}")\s*', line)]
        self.assertEqual(len(endings), 3, "the held, the account-waiting and the new-edition endings")
        for index in endings:
            with self.subTest(line=lines[index].strip()):
                self.assertRegex(lines[index - 1], r"foundation-job-outcome (unchanged|waiting)\b")

    def test_a_refused_account_waits_for_its_owner_with_the_action(self):
        # 2026-10-30: VWorld answered the login useChk (new terms). The run ends waiting on the owner,
        # with the action provider_alert.py paged, never as an opaque login error (root ADR-0363).
        waiting = re.search(r'if \[ "\$\{ingest_rc\}" = 77 \] && \[ -n "\$\{account_action\}" \]; then\n(.*\n){2}\s*exit 0',
                            SCRIPT)
        self.assertIsNotNone(waiting, "exit 77 with a paged action ends waiting")
        self.assertIn("foundation-job-outcome waiting provider account action required", waiting.group(0))
        self.assertIn("provider_alert.py\" accounts --source vworld_parcel_edition", SCRIPT)


if __name__ == "__main__":
    unittest.main()
