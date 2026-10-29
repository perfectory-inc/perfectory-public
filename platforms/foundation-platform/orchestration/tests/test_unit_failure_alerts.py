"""A unit that reports its successes is one Alertmanager alert, not a Slack notice per failed run.

2026-10-28/29: all ten `foundation-by-pnu-serving-monitor@<lane>` instances failed every hour, and
each failure posted its own notice through `foundation-unit-failed@` — about 240 a day, so a new
failure could not be told from the old ones. scripts/ops/notify-unit-failure.sh now raises a unit
that names `OnSuccess=foundation-unit-recovered@%n.service` as one alert per unit in the platform's
Alertmanager (one notice when it starts, one when a run succeeds, a reminder a day), and still
posts to Slack when Alertmanager does not take it. A unit without that line is unchanged.
"""

import datetime
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

AREA = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = AREA / "scripts" / "ops" / "notify-unit-failure.sh"
UNIT = "foundation-by-pnu-serving-monitor@parcel.service"

SYSTEMCTL = """#!/usr/bin/env bash
# systemctl show -p <Property> --value <unit>: the value from STUB_<Property>.
var="STUB_$3"
printf '%s\\n' "${!var:-}"
"""
CURL = """#!/usr/bin/env python3
import json, os, sys
with open(os.environ["CALLS"], "a", encoding="utf-8") as calls:
    calls.write(json.dumps(sys.argv[1:]) + "\\n")
if any("api/v2/alerts" in arg for arg in sys.argv[1:]):
    sys.exit(22 if os.environ.get("STUB_AM_FAIL") else 0)
print('{"ok": true}')
"""


@unittest.skipUnless(sys.platform != "win32" and shutil.which("bash") and shutil.which("python3"),
                     "the host's bash and python3 run the script (the Linux CI runner)")
class NotifyUnitFailure(unittest.TestCase):
    def setUp(self):
        self.dir = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir)
        bin_dir = self.dir / "bin"
        bin_dir.mkdir()
        for name, body in (("systemctl", SYSTEMCTL), ("curl", CURL), ("hostname", "#!/usr/bin/env bash\necho host\n")):
            (bin_dir / name).write_text(body, encoding="utf-8", newline="\n")
            (bin_dir / name).chmod(0o755)
        token = self.dir / "token"
        token.write_text("synthetic-token\n", encoding="utf-8")
        self.calls = self.dir / "calls"
        self.env = {
            **os.environ,
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "CALLS": str(self.calls),
            "FOUNDATION_SOURCE_SWEEP_SLACK_TOKEN_FILE": str(token),
            "STUB_Result": "exit-code",
            "STUB_Documentation": "file:///runbook.md",
        }

    def run_script(self, *args, **stubs):
        result = subprocess.run(["bash", str(SCRIPT), *args], env={**self.env, **stubs},
                                capture_output=True, text=True, encoding="utf-8")
        self.assertEqual(result.returncode, 0, result.stderr)
        return [json.loads(line) for line in self.calls.read_text(encoding="utf-8").splitlines()]

    @staticmethod
    def alert_of(call):
        [alert] = json.loads(call[call.index("-d") + 1])
        return alert

    def test_a_unit_that_reports_success_raises_one_alert_and_no_slack_post(self):
        calls = self.run_script(UNIT, STUB_OnSuccess=f"foundation-unit-recovered@{UNIT}.service")
        self.assertEqual(len(calls), 1, calls)
        self.assertTrue(calls[0][-1].endswith("/api/v2/alerts"), calls)
        alert = self.alert_of(calls[0])
        self.assertEqual(alert["labels"], {"alertname": "FoundationUnitFailed", "service": "unit-failure",
                                           "environment": "prelaunch", "unit": UNIT})
        ends = datetime.datetime.fromisoformat(alert["endsAt"].replace("Z", "+00:00"))
        self.assertGreater(ends, datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2))
        self.assertIn(UNIT, alert["annotations"]["summary"])

    def test_a_success_ends_the_alert(self):
        calls = self.run_script("--recovered", UNIT, STUB_OnSuccess=f"foundation-unit-recovered@{UNIT}.service")
        [call] = calls
        ends = datetime.datetime.fromisoformat(self.alert_of(call)["endsAt"].replace("Z", "+00:00"))
        self.assertLessEqual(ends, datetime.datetime.now(datetime.timezone.utc))
        self.assertEqual(self.alert_of(call)["labels"]["unit"], UNIT)

    def test_without_alertmanager_the_failure_still_reaches_slack(self):
        calls = self.run_script(UNIT, STUB_OnSuccess=f"foundation-unit-recovered@{UNIT}.service", STUB_AM_FAIL="1")
        self.assertEqual([call[-1] for call in calls][-1], "https://slack.com/api/chat.postMessage")

    def test_a_unit_without_onsuccess_posts_to_slack_as_before(self):
        calls = self.run_script("foundation-database-backup.service")
        [call] = calls
        self.assertEqual(call[-1], "https://slack.com/api/chat.postMessage")
        self.assertIn("foundation-database-backup.service", json.loads(call[call.index("-d") + 1])["text"])


class TheMonitorIsOneAlertPerLane(unittest.TestCase):
    def test_the_monitor_reports_its_successes(self):
        unit = (AREA / "infra/systemd/foundation-by-pnu-serving-monitor@.service").read_text(encoding="utf-8")
        lines = unit.split("[Service]", 1)[0].splitlines()
        self.assertIn("OnFailure=foundation-unit-failed@%n.service", lines)
        self.assertIn("OnSuccess=foundation-unit-recovered@%n.service", lines)
        recovered = (AREA / "infra/systemd/foundation-unit-recovered@.service").read_text(encoding="utf-8")
        self.assertIn("scripts/ops/notify-unit-failure.sh --recovered %i", recovered)

    def test_alertmanager_groups_unit_failures_by_unit(self):
        routes = (AREA / "infra/observability/alertmanager/alertmanager.yml").read_text(encoding="utf-8")
        route = re.search(r'- service = "unit-failure"\n(?:\s{6}.*\n)+', routes)
        self.assertIsNotNone(route, "Alertmanager has no route for unit failures")
        self.assertIn("- unit", route.group(0))
        self.assertIn("repeat_interval: 24h", route.group(0))


if __name__ == "__main__":
    unittest.main()
