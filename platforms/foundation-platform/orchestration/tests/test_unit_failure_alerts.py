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

    def test_the_notice_names_the_units_owner(self):
        # Root ADR-0445: 2026-11-01..07 nine failures were posted with nobody named, and sat for days.
        fragment = self.dir / "foundation-identity-restore-drill.service"
        fragment.write_text("[Unit]\nX-Foundation-Owner=platform\n", encoding="utf-8")
        [call] = self.run_script("foundation-identity-restore-drill.service", STUB_FragmentPath=str(fragment))
        self.assertIn("주인: platform", json.loads(call[call.index("-d") + 1])["text"])

    def test_a_post_to_alertmanager_waits_out_its_restart(self):
        # 2026-11-08 19:18:04-17Z: the deploy recreated Alertmanager and three recoveries failed on
        # "connection refused" at once; each stayed failed until its lane's next hourly success.
        [call] = self.run_script("--recovered", UNIT, STUB_OnSuccess=f"foundation-unit-recovered@{UNIT}.service")
        self.assertIn("--retry-connrefused", call)
        self.assertGreaterEqual(int(call[call.index("--retry") + 1]) * int(call[call.index("--retry-delay") + 1]), 30)

    def test_a_unit_without_onsuccess_posts_to_slack_as_before(self):
        calls = self.run_script("foundation-database-backup.service")
        [call] = calls
        self.assertEqual(call[-1], "https://slack.com/api/chat.postMessage")
        self.assertIn("foundation-database-backup.service", json.loads(call[call.index("-d") + 1])["text"])


class TheReportersPostWhereComposePublishesAlertmanager(unittest.TestCase):
    # The host scripts run without the compose env file, so their default port must be the one
    # compose.observability.yml publishes by default; 11-08's refusals were not a wrong port, and this
    # keeps it so.
    def test_the_default_port_is_the_published_one(self):
        compose = (AREA / "compose.observability.yml").read_text(encoding="utf-8")
        [published] = re.findall(r'"127\.0\.0\.1:\$\{FOUNDATION_ALERTMANAGER_PORT:-(\d+)\}:9093"', compose)
        script = SCRIPT.read_text(encoding="utf-8")
        self.assertIn(f"http://127.0.0.1:${{FOUNDATION_ALERTMANAGER_PORT:-{published}}}/api/v2/alerts", script)
        provider = (AREA / "scripts/ops/provider_alert.py").read_text(encoding="utf-8")
        self.assertIn(f'os.environ.get("FOUNDATION_ALERTMANAGER_PORT", "{published}")', provider)


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


class TheRecoveryJobsTryAgain(unittest.TestCase):
    # Root ADR-0445 §5, 2026-11-08: the host OOM cut the Sunday full backup and the next try was the next
    # night's timer; both restore drills were fixed that morning and would have stayed failed a week.
    UNITS = {"foundation-postgres-backup": "2h", "foundation-identity-backup": "2h",
             "foundation-postgres-restore-drill": "1d", "foundation-identity-restore-drill": "1d"}

    def test_a_failed_run_retries_a_bounded_number_of_times_and_its_success_resolves_the_alert(self):
        for unit, longest in self.UNITS.items():
            text = (AREA / f"infra/systemd/{unit}.service").read_text(encoding="utf-8")
            head, service = text.split("[Service]", 1)
            head, service = head.splitlines(), service.splitlines()
            self.assertIn("OnFailure=foundation-unit-failed@%n.service", head, unit)
            self.assertIn("OnSuccess=foundation-unit-recovered@%n.service", head, unit)
            self.assertIn("Restart=on-failure", service, unit)
            self.assertIn(f"RestartMaxDelaySec={longest}", service, unit)
            # Without a burst cap a broken run would restart forever.
            self.assertTrue(any(line.startswith("StartLimitBurst=") for line in head), unit)
            self.assertTrue(any(line.startswith("StartLimitIntervalSec=") for line in head), unit)


if __name__ == "__main__":
    unittest.main()
