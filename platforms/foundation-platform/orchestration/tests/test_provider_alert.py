"""A provider that refuses our account pages its owner with the action it wants (root ADR-0363).

2026-10-30 13:07Z VWorld answered the collection account's login `useChk` (new terms to accept). The
2026-10-30 deploy stopped at the staging smoke and every VWorld collection failed with "failed to
acquire VWorld dataset login session"; nothing said that only the owner could clear it, or how.
scripts/ops/provider_alert.py turns the publisher's `foundation-provider-outcome` lines into one
Alertmanager alert per provider, with the owner action config/provider-accounts.contract.json gives.
"""

import datetime
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from alertmanager_stub import AlertmanagerStub  # noqa: E402

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts/ops/provider_alert.py"
CONTRACT = json.loads((PLATFORM / "config/provider-accounts.contract.json").read_text(encoding="utf-8"))
NAMING = json.loads((PLATFORM / "config/environment-variable-naming.contract.json").read_text(encoding="utf-8"))
PREFIX = "foundation-provider-outcome "
TERMS = {"outcome": "provider_account_action_required", "provider": "vworld.kr", "reason": "useChk",
         "message": "VWorld dataset login failed result=useChk message=이용약관 변경안내 페이지로 이동합니다."}


def now():
    return datetime.datetime.now(datetime.timezone.utc)


def ends(alert):
    return datetime.datetime.fromisoformat(alert["endsAt"].replace("Z", "+00:00"))


class ProviderAlert(unittest.TestCase):
    def setUp(self):
        self.alertmanager = AlertmanagerStub()
        self.addCleanup(self.alertmanager.close)
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)

    def run_log(self, *outcomes, noise="Error: failed to acquire VWorld dataset login session"):
        path = pathlib.Path(self.dir.name) / f"run-{len(list(pathlib.Path(self.dir.name).iterdir()))}.log"
        lines = [noise] + [PREFIX + json.dumps(o, ensure_ascii=False) for o in outcomes]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return str(path)

    def alert(self, *args):
        return subprocess.run([sys.executable, str(SCRIPT), *args], capture_output=True, text=True, encoding="utf-8",
                              env={**os.environ, "FOUNDATION_ALERTMANAGER_PORT": self.alertmanager.port}, timeout=60)

    def test_the_terms_change_pages_the_owner_action_for_a_day_and_more(self):
        result = self.alert("accounts", "--source", "source_sweep", self.run_log(TERMS))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("log into vworld.kr with the collection account and accept the new terms", result.stdout)
        [alert] = self.alertmanager.alerts
        self.assertEqual(alert["labels"]["alertname"], CONTRACT["alert"]["alertname"])
        self.assertEqual(alert["labels"]["provider"], "vworld.kr")
        self.assertIn("result=useChk", alert["annotations"]["summary"])
        self.assertGreater(ends(alert), now() + datetime.timedelta(hours=CONTRACT["alert"]["hold_hours"] - 1))

    def test_the_last_word_per_provider_wins_and_a_login_that_works_resolves(self):
        ok = {"outcome": "ok", "provider": "vworld.kr"}
        result = self.alert("accounts", "--source", "staging-smoke", self.run_log(TERMS), self.run_log(ok))
        self.assertEqual((result.returncode, result.stdout.strip()), (0, ""), result.stderr)
        [alert] = self.alertmanager.alerts
        self.assertLessEqual(ends(alert), now())

    def test_an_unknown_reason_or_provider_still_names_an_action(self):
        result = self.alert("accounts", "--source", "x", self.run_log(
            {**TERMS, "reason": "lockAcct"}, {**TERMS, "provider": "example.invalid", "reason": "x"}))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("vworld.kr: vworld.kr 에 수집 계정으로 로그인해 사이트가 요구하는 조치", result.stdout)
        self.assertIn("example.invalid: example.invalid 에 수집 계정으로 로그인", result.stdout)

    def test_a_run_without_an_account_word_posts_nothing(self):
        unavailable = {"outcome": "provider_unavailable", "message": "HTTP 503"}
        result = self.alert("accounts", "--source", "x", self.run_log(unavailable))
        self.assertEqual((result.returncode, result.stdout.strip(), self.alertmanager.alerts), (0, "", []))

    def test_an_alert_alertmanager_does_not_take_is_a_failure_for_the_caller(self):
        self.alertmanager.close()
        result = self.alert("accounts", "--source", "x", self.run_log(TERMS))
        self.assertEqual(result.returncode, 1)
        self.assertIn("Alertmanager did not take the alert", result.stderr)

    def test_the_smoke_pages_what_it_skipped_and_a_clean_smoke_resolves_it(self):
        line = PREFIX + json.dumps(TERMS, ensure_ascii=False)
        self.assertEqual(self.alert("smoke", "--release", "e" * 40, "--skipped", f"vworld-ingest={line}").returncode, 0)
        self.assertEqual(self.alert("smoke", "--release", "f" * 40).returncode, 0)
        skipped, resolved = self.alertmanager.alerts
        self.assertEqual(skipped["labels"], resolved["labels"])
        self.assertIn("vworld-ingest (provider_account_action_required: useChk)", skipped["annotations"]["summary"])
        self.assertGreater(ends(skipped), now())
        self.assertLessEqual(ends(resolved), now())


class EveryLoginIsAContractProvider(unittest.TestCase):
    """A login client that reports an account refusal must name a provider the contract holds."""

    def test_every_account_refusal_names_a_contract_provider(self):
        callers = [path for path in (PLATFORM / "crates").rglob("*.rs")
                   if "provider_account_action_required(" in path.read_text(encoding="utf-8")
                   and "outbound-http-infrastructure" not in path.parts]
        self.assertTrue(callers, "the VWorld login reports account refusals")
        for path in callers:
            with self.subTest(path=str(path.relative_to(PLATFORM))):
                provider = re.search(r'const ACCOUNT_PROVIDER: &str = "([^"]+)";', path.read_text(encoding="utf-8"))
                self.assertIsNotNone(provider, "a login names its provider in ACCOUNT_PROVIDER")
                self.assertIn(provider.group(1), CONTRACT["providers"])

    def test_alertmanager_routes_the_page_once_per_provider_and_reminds_daily(self):
        routes = (PLATFORM / "infra/observability/alertmanager/alertmanager.yml").read_text(encoding="utf-8")
        route = re.search(r'- service = "provider-account"\n(?:\s{6}.*\n)+', routes)
        self.assertIsNotNone(route, "Alertmanager has no route for provider accounts")
        self.assertEqual(CONTRACT["alert"]["service"], "provider-account")
        self.assertIn("- provider", route.group(0))
        self.assertIn("repeat_interval: 24h", route.group(0))

    def test_each_contract_provider_is_whole(self):
        groups = NAMING["compatibility_migrations"]
        for name, provider in CONTRACT["providers"].items():
            with self.subTest(provider=name):
                self.assertIn(provider["credentials_group"], groups)
                self.assertTrue((PLATFORM / provider["login_client"].split(" ", 1)[0]).is_file())
                self.assertIn("*", provider["owner_actions"])
        self.assertIn("accept the new terms", CONTRACT["providers"]["vworld.kr"]["owner_actions"]["useChk"])
        jobs = {job["id"] for job in json.loads(
            (PLATFORM / "orchestration/jobs.v1.json").read_text(encoding="utf-8"))["jobs"]}
        for provider in CONTRACT["providers"].values():
            self.assertLessEqual(set(provider["jobs"]), jobs)


if __name__ == "__main__":
    unittest.main()
