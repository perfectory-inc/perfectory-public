"""The building gateway's gradual rollout (scripts/ops/building-gateway-canary.sh, root ADR-0151).

A fake `npx` stands in for wrangler and records every call; a fake health command passes or fails
a step. The steps come from the contract, a breach rolls every request back to the old version and
stops, and a dry run deploys nothing.
"""

import json
import os
import pathlib
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts" / "ops" / "building-gateway-canary.sh"
CONTRACT = json.loads((PLATFORM / "config" / "r2-connections.contract.json").read_text(encoding="utf-8"))
CANARY = CONTRACT["building_by_pnu_gateway"]["section_packs"]["canary"]
SECRETS = json.loads((PLATFORM / "config" / "runtime-secrets.contract.json").read_text(encoding="utf-8"))
ENV_FILE = next(g["path"] for g in SECRETS["groups"] if g["name"] == "cloudflare-analytics")
HEALTH = PLATFORM / "scripts" / "ops" / "building-gateway-health.sh"
OLD = "11111111-1111-4111-8111-111111111111"
NEW = "22222222-2222-4222-8222-222222222222"

FAKE_NPX = r'''#!/usr/bin/env bash
printf '%s\n' "$*" >> "${FAKE_LOG}"
case "$*" in
  "wrangler deployments status"*"--json"*)
    printf '{"versions":[{"version_id":"%s","percentage":100}]}\n' "${FAKE_SERVING:-${FAKE_OLD}}" ;;
  "wrangler deployments status"*)
    printf 'split: %s\n' "${FAKE_OLD}" ;;
  "wrangler versions upload"*)
    if [[ -n "${FAKE_NO_ID:-}" ]]; then printf 'Uploaded\n'; else printf 'Uploaded\nWorker Version ID: %s\n' "${FAKE_NEW}"; fi ;;
  "wrangler versions deploy"*"canary rollback"*)
    [[ -z "${FAKE_FAIL_ROLLBACK:-}" ]] || exit 1 ;;
  "wrangler versions deploy"*)
    [[ -z "${FAKE_FAIL_DEPLOY_AT:-}" || "$*" != *"canary ${FAKE_FAIL_DEPLOY_AT}%"* ]] || exit 1 ;;
esac
'''

# Fails the judgement whose step deployed the failing percentage.
FAKE_HEALTH = r'''#!/usr/bin/env bash
printf 'health %s %s\n' "$1" "$2" >> "${FAKE_LOG}"
if [[ "$1" == --preflight ]]; then
  [[ -z "${FAKE_NO_ANALYTICS:-}" ]] || exit 78
  exit 0
fi
last="$(grep 'versions deploy' "${FAKE_LOG}" | tail -1)"
[[ -z "${FAIL_AT:-}" || "${last}" != *"canary ${FAIL_AT}%"* ]]
'''


class Canary(unittest.TestCase):
    def setUp(self):
        self.work = pathlib.Path(tempfile.mkdtemp())
        bin_dir = self.work / "bin"
        bin_dir.mkdir()
        for name, body in (("npx", FAKE_NPX), ("health", FAKE_HEALTH)):
            path = bin_dir / name
            path.write_text(body, encoding="utf-8")
            path.chmod(0o755)
        self.log = self.work / "calls.log"
        self.env = {
            **os.environ,
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "FAKE_LOG": str(self.log),
            "FAKE_OLD": OLD,
            "FAKE_NEW": NEW,
            "CANARY_HOLD_SECONDS": "0",
            "CANARY_HEALTH_COMMAND": f"{bin_dir / 'health'} @NEW@ @OLD@",
            "CORS_ALLOWED_ORIGINS": "https://app.example.test",
        }

    def run_script(self, *args, **env):
        return subprocess.run(
            ["bash", str(SCRIPT), *args],
            env={**self.env, **env},
            capture_output=True,
            text=True,
            check=False,
        )

    def calls(self):
        return self.log.read_text(encoding="utf-8").splitlines() if self.log.exists() else []

    def test_the_code_phase_walks_the_contract_steps_and_judges_each(self):
        result = self.run_script("--execute", "code")
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls()
        upload = next(call for call in calls if call.startswith("wrangler versions upload"))
        self.assertIn("FOUNDATION_PLATFORM_BUILDING_PACK_SERVING:off", upload)
        deploys = [call for call in calls if call.startswith("wrangler versions deploy")]
        steps = CANARY["steps_percent"]
        self.assertEqual(len(deploys), len(steps))
        for deploy, percent in zip(deploys, steps):
            if percent < 100:
                self.assertIn(f"{OLD}@{100 - percent}% {NEW}@{percent}%", deploy)
            else:
                self.assertIn(f"{NEW}@100%", deploy)
        # Every split step is judged against the old version; at 100% the old version is no
        # longer deployed, so the last step is judged on the new version alone (2026-10-06: a
        # comparison against an undeployed version failed a healthy rollout at 100%).
        split = [percent for percent in steps if percent < 100]
        self.assertEqual(calls.count(f"health {NEW} {OLD}"), len(split))
        self.assertEqual(sum(call.strip() == f"health {NEW}" for call in calls), len(steps) - len(split))

    def test_a_breach_rolls_every_request_back_and_stops(self):
        result = self.run_script("--execute", "packs", OLD, FAIL_AT=str(CANARY["steps_percent"][1]))
        self.assertEqual(result.returncode, 1, result.stderr)
        calls = self.calls()
        upload = next(call for call in calls if call.startswith("wrangler versions upload"))
        self.assertIn("FOUNDATION_PLATFORM_BUILDING_PACK_SERVING:on", upload)
        deploys = [call for call in calls if call.startswith("wrangler versions deploy")]
        self.assertIn(f"{OLD}@100%", deploys[-1])
        self.assertEqual(len(deploys), 3, "it went on past the breach")

    def test_without_analytics_nothing_is_uploaded(self):
        for phase in (["code"], ["packs", OLD]):
            self.log.unlink(missing_ok=True)
            result = self.run_script("--execute", *phase, FAKE_NO_ANALYTICS="1")
            self.assertEqual(result.returncode, 78, result.stderr)
            self.assertIn("cannot read Cloudflare analytics", result.stderr)
            self.assertEqual(self.calls(), ["health --preflight "], phase)

    @unittest.skipIf(pathlib.Path(ENV_FILE).exists(), "this host holds the analytics file")
    def test_the_health_check_names_the_missing_analytics_file(self):
        result = subprocess.run(
            ["bash", str(HEALTH), "--preflight"], capture_output=True, text=True, check=False
        )
        self.assertEqual(result.returncode, 78, result.stderr)
        self.assertIn(f"{ENV_FILE} does not exist", result.stderr)

    def test_an_upload_without_a_version_id_deploys_nothing(self):
        result = self.run_script("--execute", "code", FAKE_NO_ID="1")
        self.assertEqual(result.returncode, 70, result.stderr)
        self.assertIn("printed no version id", result.stderr)
        self.assertFalse([call for call in self.calls() if "versions deploy" in call])

    def test_the_packs_phase_starts_only_from_the_version_at_100_percent(self):
        result = self.run_script("--execute", "packs", OLD, FAKE_SERVING=NEW)
        self.assertEqual(result.returncode, 65, result.stderr)
        self.assertIn(f"{OLD} is not the version at 100%", result.stderr)
        self.assertFalse([call for call in self.calls() if call.startswith("wrangler versions")])

    def test_a_failed_step_deployment_shows_the_split_and_rolls_back(self):
        result = self.run_script("--execute", "code", FAKE_FAIL_DEPLOY_AT=str(CANARY["steps_percent"][1]))
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("traffic may be split", result.stderr)
        calls = self.calls()
        deploys = [call for call in calls if call.startswith("wrangler versions deploy")]
        self.assertIn(f"{OLD}@100%", deploys[-1])
        self.assertIn("canary rollback", deploys[-1])
        self.assertIn(
            "wrangler deployments status --name foundation-building-gateway --config wrangler.building.jsonc", calls
        )
        # No step after the failed one was judged.
        self.assertEqual(calls.count(f"health {NEW} {OLD}"), 1)

    def test_a_failed_rollback_is_shouted_with_the_command_to_finish_it(self):
        result = self.run_script(
            "--execute", "code", FAKE_FAIL_DEPLOY_AT=str(CANARY["steps_percent"][0]), FAKE_FAIL_ROLLBACK="1"
        )
        self.assertEqual(result.returncode, 3, result.stderr)
        self.assertIn("THE ROLLBACK FAILED TOO", result.stderr)
        self.assertIn(f"--execute rollback {OLD}", result.stderr)
        rolled = self.run_script("--execute", "rollback", OLD, FAKE_FAIL_ROLLBACK="1")
        self.assertEqual(rolled.returncode, 3, rolled.stderr)
        self.assertIn("deployment is shown above", rolled.stderr)

    def test_a_dry_run_deploys_nothing(self):
        result = self.run_script("code")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.calls(), [])
        self.assertIn("versions deploy", result.stderr)


if __name__ == "__main__":
    unittest.main()
