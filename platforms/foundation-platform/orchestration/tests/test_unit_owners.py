"""Every host unit that reports its failure names who acts on it and how (root ADR-0444).

2026-11-01..07: nine units on ai-server stayed failed for hours to six days (the identity restore
drill from 11-01, the court auction collector, the 법정동 stewardship, the zoning tile bake, ...).
Each had posted one Slack notice when it failed; nobody was named, most named no runbook, and
nothing said it again. Two more were leftovers nobody could act on: the 공매 bake whose file a
release removed on 11-05 (systemd kept its failed state, not-found) and two operator rehearsals of
the parcel tile bake, failed on 10-15/16. These tests hold the unit files to an owner and a runbook,
and the deploy to clearing a failed state that no declared unit will ever run again.
"""

import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "scripts/deploy"))

import job_units  # noqa: E402

UNITS = PLATFORM / "infra/systemd"
CONTRACT = PLATFORM / "config/unit-owners.contract.json"
RUNBOOK_PREFIX = "file:///opt/foundation-platform/current/"


def unit_lines(path):
    return path.read_text(encoding="utf-8").split("[Service]", 1)[0].splitlines()


class EveryFailingUnitHasAnOwnerAndARunbook(unittest.TestCase):
    def test_each_unit_that_reports_its_failure_names_a_listed_owner_and_a_runbook_that_exists(self):
        owners = set(json.loads(CONTRACT.read_text(encoding="utf-8"))["owners"])
        checked, problems = 0, []
        for path in sorted(UNITS.glob("*.service")):
            lines = unit_lines(path)
            if "OnFailure=foundation-unit-failed@%n.service" not in lines:
                continue
            checked += 1
            owner = [line.split("=", 1)[1] for line in lines if line.startswith("X-Foundation-Owner=")]
            if len(owner) != 1 or owner[0] not in owners:
                problems.append(f"{path.name}: X-Foundation-Owner= must name one of {sorted(owners)}, has {owner}")
            runbooks = [line.split("=", 1)[1] for line in lines if line.startswith("Documentation=")]
            if not runbooks or not runbooks[0].startswith(RUNBOOK_PREFIX + "docs/runbooks/"):
                problems.append(f"{path.name}: Documentation= must name a runbook under {RUNBOOK_PREFIX}docs/runbooks/")
            elif not (PLATFORM / runbooks[0].removeprefix(RUNBOOK_PREFIX)).is_file():
                problems.append(f"{path.name}: its runbook {runbooks[0]} is not in the release")
        self.assertGreater(checked, 40, "the scan found the units")
        self.assertEqual(problems, [])

    def test_every_listed_owner_owns_a_unit(self):
        owners = set(json.loads(CONTRACT.read_text(encoding="utf-8"))["owners"])
        named = {line.split("=", 1)[1] for path in UNITS.glob("*.service") for line in unit_lines(path)
                 if line.startswith("X-Foundation-Owner=")}
        self.assertEqual(owners - named, set(), "an owner nobody names is a name nobody is called by")


# Answers `list-units --output=json` from FAKE_FAILED (a JSON list) and `show` from FAKE_SINCE
# ({unit: epoch}), the two questions job_units.leftover_failed asks.
FAKE_SYSTEMCTL = r'''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
if args[0] == "list-units":
    print(os.environ["FAKE_FAILED"])
elif args[0] == "show":
    since = json.loads(os.environ["FAKE_SINCE"])
    units = args[args.index("--") + 1:]
    print("\n\n".join(f"Id={unit}\nActiveState=failed\nStateChangeTimestamp=@{since[unit]}" for unit in units))
'''


@unittest.skipUnless(sys.platform != "win32", "a stand-in systemctl script runs on the Linux CI runner")
class TheDeployClearsOnlyLeftovers(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root)
        self.systemctl = self.root / "systemctl"
        self.systemctl.write_text(FAKE_SYSTEMCTL, encoding="utf-8", newline="\n")
        self.systemctl.chmod(0o755)
        self.jobs = self.root / "jobs.v1.json"
        self.jobs.write_text(json.dumps({"jobs": [
            {"systemd_service": "foundation-parcel-tile-bake@gold.service"},
            {"systemd_service": "foundation-silver-refresh@land-ownership.service"},
            {"systemd_service": "foundation-zoning-tile-bake.service"},
        ]}), encoding="utf-8")

    def leftovers(self, failed, since, now):
        os.environ["FAKE_FAILED"] = json.dumps(failed)
        os.environ["FAKE_SINCE"] = json.dumps(since)
        self.addCleanup(os.environ.pop, "FAKE_FAILED", None)
        self.addCleanup(os.environ.pop, "FAKE_SINCE", None)
        return job_units.leftover_failed(self.jobs, str(self.systemctl), now=now)

    def test_a_removed_unit_and_a_day_old_rehearsal_are_cleared_and_declared_units_are_not(self):
        day = job_units.LEFTOVER_AFTER_SECONDS
        failed = [
            {"unit": "foundation-by-pnu-public-auction-bake.service", "load": "not-found"},
            {"unit": "foundation-parcel-tile-bake@rehearsal-bake.service", "load": "loaded"},
            {"unit": "foundation-parcel-tile-bake@rehearsal-gold-209901.service", "load": "loaded"},
            # Declared, failed for days: a real failure the owner is called for, never cleared here.
            {"unit": "foundation-parcel-tile-bake@gold.service", "load": "loaded"},
            {"unit": "foundation-silver-refresh@land-ownership.service", "load": "loaded"},
            {"unit": "foundation-zoning-tile-bake.service", "load": "loaded"},
            # A template no job instantiates (its instances are declared by their timers).
            {"unit": "foundation-by-pnu-serving-monitor@parcel.service", "load": "loaded"},
        ]
        since = {"foundation-parcel-tile-bake@rehearsal-bake.service": 0,
                 "foundation-parcel-tile-bake@rehearsal-gold-209901.service": 2 * day - 60}
        self.assertEqual(self.leftovers(failed, since, now=2 * day),
                         ["foundation-by-pnu-public-auction-bake.service",
                          "foundation-parcel-tile-bake@rehearsal-bake.service"])

    def test_the_deploy_clears_them_after_the_reload(self):
        release = (PLATFORM / "scripts/deploy/foundation-release.sh").read_text(encoding="utf-8")
        clear = release.index('job_units.py" leftover-failed "${jobs_file}"')
        self.assertIn('systemctl reset-failed "${leftover}"', release[clear - 600:clear])
        self.assertLess(release.rindex("systemctl daemon-reload", 0, clear), clear)


if __name__ == "__main__":
    unittest.main()
