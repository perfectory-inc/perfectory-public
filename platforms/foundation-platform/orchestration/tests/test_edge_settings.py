"""The data host applies the console's edge settings by itself (scripts/deploy/edge_settings.py, root
ADR-0436).

A fixture host holds a control checkout (the real contracts and module inputs), the release it runs
and the token's file. Stand-ins answer for the rest: the staff list, the Cloudflare API, OpenTofu,
the runtime-secrets writer (a plain file here) and the console start. The front door (infra/cdn,
root ADR-0461) applies after the console under its own records once the aws-cdn group holds a key.
"""

import contextlib
import hashlib
import importlib.util
import io
import json
import os
import pathlib
import shutil
import sys
import tempfile
import unittest
from unittest import mock

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
ROOT = PLATFORM.parents[1]
SCRIPT = PLATFORM / "scripts/deploy/edge_settings.py"
COMMIT = "a" * 40

spec = importlib.util.spec_from_file_location("edge_settings", SCRIPT)
edge = importlib.util.module_from_spec(spec)
sys.modules["edge_settings"] = edge
spec.loader.exec_module(edge)


class EdgeSettingsTest(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root)
        self.tree = self.root / "control"
        for relative in (*edge.INPUTS, *edge.TOOL_FILES, *edge.FRONT_DOOR_INPUTS,
                         f"{edge.PLATFORM}/config/r2-connections.contract.json",
                         f"{edge.PLATFORM}/config/runtime-secrets.contract.json",
                         f"{edge.PLATFORM}/config/worker-deploys.contract.json",
                         f"{edge.PLATFORM}/scripts/deploy/runtime_secrets.py"):
            source, target = ROOT / relative, self.tree / relative
            if source.is_dir():
                shutil.copytree(source, target)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(source, target)
        (self.tree / ".perfectory-control-commit").write_text(COMMIT + "\n")
        release = self.root / "release"
        (release / "releases" / COMMIT).mkdir(parents=True)
        (release / "current").symlink_to(release / "releases" / COMMIT)
        self.secrets = self.root / "host"
        self.host = edge.Host(control_root=self.tree, release_root=release, state=self.root / "state",
                              secrets_root=self.secrets, off_switches=(self.root / "off",))
        self.token_file = edge.on_host(self.host, edge.group_path(self.tree, "cloudflare-deploy"))
        self.token_file.parent.mkdir(parents=True)
        self.token_file.write_text("CLOUDFLARE_ACCOUNT_ID=acct\nFOUNDATION_PLATFORM_WORKERS_WRITER_TOKEN=tok\n")
        self.env = {"CLOUDFLARE_ACCOUNT_ID": "acct", "FOUNDATION_PLATFORM_WORKERS_WRITER_TOKEN": "token-value"}
        self.staff = ["owner@example.com"]
        self.buckets = set()
        self.tofu_calls = []
        self.tofu_fails = False
        self.console_starts = 0
        self.front_calls = []
        self.front_fails = False

    # --- stand-ins ------------------------------------------------------------------------------

    def cloudflare(self, token, method, path, body=None):
        self.assertEqual(token, "token-value")
        if path == "/user/tokens/verify":
            return 200, {"result": {"id": "token-id"}}
        if path == "/accounts/acct/workers/subdomain":
            return 200, {"result": {"subdomain": "example"}}
        bucket = path.rsplit("/", 1)[-1]
        if method == "GET":
            return (200, {}) if bucket in self.buckets else (404, {})
        self.buckets.add(body["name"])
        return 200, {}

    def tofu(self, host, tree, env, *args, module=edge.MODULE, work="work"):
        self.assertNotIn("token-value", " ".join(args), "a value never reaches a command line")
        self.assertNotIn("aws-secret", " ".join(args), "a value never reaches a command line")
        if module == edge.FRONT_DOOR_MODULE:
            self.assertEqual(work, "work-cdn")
            self.front_calls.append((args[0], env))
            if self.front_fails and args[0] == "apply":
                raise RuntimeError("front door apply failed")
            return '{"tiles": "d111.cloudfront.net"}' if args[0] == "output" else ""
        self.tofu_calls.append((args[0], env))
        if self.tofu_fails and args[0] == "apply":
            raise RuntimeError("apply failed")
        return "tunnel-token\n" if args[0] == "output" else ""

    def place_aws_key(self):
        path = edge.on_host(self.host, edge.group_path(self.tree, edge.FRONT_DOOR_GROUP))
        path.write_text(f"{edge.FRONT_DOOR_KEY_NAMES[0]}=AKIAEXAMPLE\n{edge.FRONT_DOOR_KEY_NAMES[1]}=aws-secret\n")

    def set_group(self, host, tree, group, values):
        path = edge.on_host(host, edge.group_path(tree, group))
        kept = {**edge.values_in(path), **values}
        path.write_text("".join(f"{k}={v}\n" for k, v in kept.items()))

    def console(self, *args, **kwargs):
        self.console_starts += 1

    def run_edge(self):
        out = io.StringIO()
        with mock.patch.dict(os.environ, self.env), \
                mock.patch.object(edge, "staff_emails", lambda: list(self.staff)), \
                mock.patch.object(edge, "cloudflare", self.cloudflare), \
                mock.patch.object(edge, "tofu", self.tofu), \
                mock.patch.object(edge, "set_group", self.set_group), \
                mock.patch.object(edge.subprocess, "run", self.console), \
                contextlib.redirect_stdout(out):
            status = edge.run(self.host)
        return status, out.getvalue()

    def applies(self):
        return [call for call, _ in self.tofu_calls if call == "apply"]

    # --- tests ----------------------------------------------------------------------------------

    def test_the_first_apply_makes_the_bucket_and_passphrase_writes_the_tunnel_token_and_starts_the_console(self):
        status, out = self.run_edge()
        self.assertEqual(status, 0, out)
        self.assertEqual([call for call, _ in self.tofu_calls], ["init", "apply", "output"])
        self.assertEqual(self.buckets, {edge.STATE_BUCKET})
        tunnel = edge.on_host(self.host, edge.group_path(self.tree, "console-tunnel"))
        self.assertEqual(edge.values_in(tunnel), {"TUNNEL_TOKEN": "tunnel-token"})
        passphrase = edge.values_in(edge.on_host(self.host, edge.group_path(self.tree, "edge-settings")))
        self.assertGreaterEqual(len(passphrase[edge.PASSPHRASE_NAME]), 48)
        env = self.tofu_calls[1][1]
        self.assertEqual(json.loads(env["TF_VAR_staff_emails"]), self.staff)
        self.assertEqual(env["TF_VAR_state_passphrase"], passphrase[edge.PASSPHRASE_NAME])
        self.assertEqual((env["AWS_ACCESS_KEY_ID"], env["AWS_SECRET_ACCESS_KEY"]),
                         ("token-id", hashlib.sha256(b"token-value").hexdigest()))
        self.assertEqual(self.console_starts, 1)
        self.assertNotIn("tok\n", out)
        self.assertNotIn("owner@example.com", out, "the log names counts, never an address")

    def test_nothing_changed_nothing_runs_until_a_day_has_passed(self):
        self.run_edge()
        self.tofu_calls.clear()
        self.assertEqual(self.run_edge()[0], 0)
        self.assertEqual(self.tofu_calls, [])
        record = edge.read_json(self.host.state / "applied.json")
        record["at"] -= edge.DRIFT_SECONDS + 1
        edge.write_json(self.host.state / "applied.json", record)
        self.assertEqual(self.run_edge()[0], 0)
        self.assertEqual(self.applies(), ["apply"])
        self.assertEqual(self.console_starts, 1, "the same tunnel token does not restart the console")

    def test_a_new_staff_member_applies_again(self):
        self.run_edge()
        self.tofu_calls.clear()
        self.staff.append("new@example.com")
        self.assertEqual(self.run_edge()[0], 0)
        self.assertEqual(self.applies(), ["apply"])

    def test_an_empty_staff_list_is_refused_before_anything_runs(self):
        self.staff.clear()
        status, out = self.run_edge()
        self.assertEqual(status, 78)
        self.assertIn("let nobody in", out)
        self.assertEqual(self.tofu_calls, [])

    def test_a_failed_apply_is_retried_then_waits_for_a_change(self):
        self.tofu_fails = True
        for _ in range(edge.ATTEMPTS):
            self.assertEqual(self.run_edge()[0], 1)
        self.tofu_calls.clear()
        status, out = self.run_edge()
        self.assertEqual(status, 0)
        self.assertIn("gave up", out)
        self.assertEqual(self.tofu_calls, [])
        self.staff.append("new@example.com")
        self.assertEqual(self.run_edge()[0], 1, "a change gets fresh attempts")

    def test_off_without_the_token_file_or_with_a_switch_or_while_a_deploy_is_under_way(self):
        (self.root / "off").touch()
        self.assertEqual(self.run_edge()[0], 0)
        (self.root / "off").unlink()
        self.token_file.unlink()
        self.assertIn("off:", self.run_edge()[1])
        self.token_file.write_text("x=1\n")
        (self.tree / ".perfectory-control-commit").write_text("b" * 40 + "\n")
        self.assertIn("waiting for the host deploy", self.run_edge()[1])
        self.assertEqual(self.tofu_calls, [])

    def test_the_front_door_is_off_until_the_aws_key_is_placed(self):
        status, out = self.run_edge()
        self.assertEqual(status, 0, out)
        self.assertIn("front door off", out)
        self.assertEqual(self.front_calls, [])

    def test_with_the_aws_key_the_front_door_applies_with_it_and_the_account_token(self):
        self.place_aws_key()
        status, out = self.run_edge()
        self.assertEqual(status, 0, out)
        self.assertEqual([call for call, _ in self.front_calls], ["init", "apply", "output"])
        env = self.front_calls[1][1]
        self.assertEqual((env["TF_VAR_aws_access_key_id"], env["TF_VAR_aws_secret_access_key"]),
                         ("AKIAEXAMPLE", "aws-secret"))
        self.assertEqual(env["CLOUDFLARE_API_TOKEN"], "token-value")
        self.assertEqual(env["AWS_ACCESS_KEY_ID"], "token-id", "the AWS_ names stay the R2 state bucket's")
        self.assertEqual(env["TF_VAR_workers_subdomain"], "example")
        self.assertEqual(json.loads(env["TF_VAR_alert_emails"]), self.staff)
        self.assertIn('key = "cdn/terraform.tfstate"', (self.host.state / "work-cdn/backend.hcl").read_text())
        self.assertIn("front door tiles: d111.cloudfront.net", out)
        self.assertNotIn("aws-secret", out)
        self.front_calls.clear()
        self.assertEqual(self.run_edge()[0], 0)
        self.assertEqual(self.front_calls, [], "nothing changed, nothing runs")

    def test_a_failed_front_door_reports_and_does_not_hold_the_console_back(self):
        self.place_aws_key()
        self.front_fails = True
        self.assertEqual(self.run_edge()[0], 1)
        self.assertEqual(self.applies(), ["apply"])
        self.assertTrue((self.host.state / "applied.json").exists())
        self.assertTrue((self.host.state / "cdn/failed.json").exists())


if __name__ == "__main__":
    unittest.main()
