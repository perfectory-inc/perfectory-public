"""The operator's root steps of a section pack cut-over (scripts/ops/by-pnu-pack-operator.sh, root ADR-0161).

sudo grants this one script without a password, so what it must never do matters as much as what
it does: it takes a lane, an action and a generation or version id, refuses anything else before
running a thing, and runs only the admitted release's publisher with the environment files the
runtime-secrets contract names. A fake systemd-run records each unit it would start.
"""

import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts" / "ops" / "by-pnu-pack-operator.sh"
CONTRACT = json.loads((PLATFORM / "config" / "r2-connections.contract.json").read_text(encoding="utf-8"))
RELEASE_ID = "c" * 40
VERSION = "33333333-3333-4333-8333-333333333333"
OLD = "44444444-4444-4444-8444-444444444444"

FAKE_SYSTEMD_RUN = r'''#!/usr/bin/env python3
import json, os, sys
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps({"tool": "systemd-run", "args": sys.argv[1:]}) + "\n")
'''

FAKE_SYSTEMCTL = "#!/usr/bin/env bash\necho inactive\nexit 3\n"

FAKE_HEALTH = r'''#!/usr/bin/env python3
import json, os, sys
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps({"tool": "health", "args": sys.argv[1:]}) + "\n")
'''

FAKE_PUBLISHER = "#!/usr/bin/env bash\nexit 0\n"


class PackOperator(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="pack-operator-")
        self.addCleanup(temp.cleanup)
        self.root = pathlib.Path(temp.name)
        base = self.root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        # What the operator reads from the release: its contracts, its secrets tool, its admission.
        for part in ("config", "scripts/deploy", "infra/systemd"):
            shutil.copytree(PLATFORM / part, release / part)
        (release / "scripts/ops").mkdir(parents=True)
        # The granted copy hands every action to the release's own copy of itself.
        for name in ("admitted-writer-runtime.sh", "by-pnu-pack-operator.sh"):
            shutil.copy(PLATFORM / "scripts/ops" / name, release / "scripts/ops")
        health = release / "scripts/ops/by-pnu-gateway-health.sh"
        health.write_text(FAKE_HEALTH)
        health.chmod(0o755)
        (base / "current").symlink_to(pathlib.Path("releases") / RELEASE_ID)
        artifacts = base / "artifacts" / RELEASE_ID
        artifacts.mkdir(parents=True)
        self.publisher = artifacts / "foundation-outbox-publisher"
        self.publisher.write_text(FAKE_PUBLISHER)
        self.publisher.chmod(0o555)
        (artifacts / "build.json").write_text(json.dumps({
            "source": RELEASE_ID, "publisher_image": "sha256:" + "a" * 64,
            "tippecanoe_image": "sha256:" + "d" * 64,
            "files": {"foundation-outbox-publisher": hashlib.sha256(FAKE_PUBLISHER.encode()).hexdigest(),
                      "jars/fixture.jar": "0" * 64}}))
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        for name, body in (("systemd-run", FAKE_SYSTEMD_RUN), ("systemctl", FAKE_SYSTEMCTL)):
            (bin_dir / name).write_text(body)
            (bin_dir / name).chmod(0o755)
        self.bake = self.root / "data/by-pnu-bake"
        self.etc = self.root / "etc"
        self.etc.mkdir()
        self.log = self.root / "calls.jsonl"
        owner = subprocess.run(["id", "-un"], capture_output=True, text=True, check=True).stdout.strip()
        group = subprocess.run(["id", "-gn"], capture_output=True, text=True, check=True).stdout.strip()
        self.env = {
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "FAKE_LOG": str(self.log),
            "FOUNDATION_OPERATOR_RELEASE_ROOT": str(base / "current"),
            "FOUNDATION_OPERATOR_SYSTEMD_RUN": str(bin_dir / "systemd-run"),
            "FOUNDATION_OPERATOR_BAKE_ROOT": str(self.bake),
            "FOUNDATION_OPERATOR_ETC": str(self.etc),
            "FOUNDATION_OPERATOR_SERVICE_OWNER": owner,
            "FOUNDATION_OPERATOR_ROOT_OWNER": owner,
            "FOUNDATION_OPERATOR_SERVICE_GROUP": group,
        }

    def work(self, lane="parcel", generation=1):
        work = self.bake / f"{lane}-pack-g{generation}"
        (work / "summaries").mkdir(parents=True, exist_ok=True)
        (work / "summaries/shard-11.json").write_text(json.dumps({"gold_iceberg_snapshot_id": "777"}))
        return work

    def run_operator(self, *args):
        result = subprocess.run(["bash", str(SCRIPT), *args], env=self.env,
                                capture_output=True, text=True, timeout=60)
        calls = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        return result, calls

    def unit(self, calls):
        (call,) = [call for call in calls if call["tool"] == "systemd-run"]
        args = call["args"]
        env = dict(args[index + 1].split("=", 1) for index, arg in enumerate(args) if arg == "-E")
        props = [args[index + 1] for index, arg in enumerate(args) if arg == "-p"]
        return args, env, props

    def test_the_latency_gate_runs_the_release_publisher_against_the_lane_preview(self):
        work = self.work()
        (work / "equality.json").write_text(json.dumps({"passed": True}))
        (work / "latency.json").write_text("{}")
        result, calls = self.run_operator("parcel", "latency", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        args, env, props = self.unit(calls)
        self.assertIn("--unit=foundation-parcel-pack-latency-g1", args)
        self.assertEqual(args[-1], "probe-parcel-by-pnu-section-pack-latency")
        self.assertEqual(pathlib.Path(args[-2]).resolve(), self.publisher.resolve())
        preview = CONTRACT["parcel_by_pnu_gateway"]["section_packs"]["preview_worker"]["public_hostname"]
        prefix = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_"
        self.assertEqual(env[prefix + "PACK_PREVIEW_BASE_URL"], f"https://{preview}")
        self.assertEqual(env[prefix + "PACK_LATENCY_EVIDENCE_PATH"], f"{work}/latency.json")
        self.assertFalse([name for name in env if "BUILDING" in name], env)
        # The environment files are the contract's for this run, not a list kept in the script.
        secrets = json.loads((PLATFORM / "config/runtime-secrets.contract.json").read_text(encoding="utf-8"))
        analytics = next(g["path"] for g in secrets["groups"] if g["name"] == "cloudflare-analytics")
        self.assertIn(f"EnvironmentFile={analytics}", props)
        # The earlier evidence is kept, never overwritten.
        self.assertEqual(len(list(work.glob("latency-*.json"))), 1)

    def test_publish_refuses_until_both_gates_passed_and_pins_the_bake_snapshot(self):
        work = self.work()
        (work / "equality.json").write_text(json.dumps({"passed": True}))
        (work / "latency.json").write_text(json.dumps({"passed": False}))
        result, calls = self.run_operator("parcel", "publish", "1")
        self.assertEqual(result.returncode, 65, result.stderr)
        self.assertIn("latency.json did not pass", result.stderr)
        self.assertEqual(calls, [])
        (work / "latency.json").write_text(json.dumps({"passed": True}))
        result, calls = self.run_operator("parcel", "publish", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        args, env, _ = self.unit(calls)
        self.assertEqual(args[-1], "publish-parcel-by-pnu-section-packs")
        self.assertEqual(env["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID"], "777")
        self.assertEqual(env["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_CONFIRM_PACK_PUBLISH"], "true")

    def test_anything_but_a_lane_an_action_and_an_id_is_refused_before_anything_runs(self):
        self.work()
        for args in (
            ("buildings", "latency", "1"),
            ("parcel", "latency", "1;id"),
            ("parcel", "latency", "../1"),
            ("parcel", "latency", "0"),
            ("parcel", "shell", "1"),
            ("parcel", "health", "not-a-version"),
            ("parcel", "health", VERSION, "$(id)"),
            ("parcel",),
        ):
            result, calls = self.run_operator(*args)
            self.assertEqual(result.returncode, 64, (args, result.stderr))
            self.assertEqual(calls, [], args)

    def test_health_hands_the_release_check_its_lane_and_versions(self):
        result, calls = self.run_operator("parcel", "health", VERSION, OLD)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, [{"tool": "health", "args": ["parcel", VERSION, OLD]}])

    def test_the_monitor_sample_names_this_generations_equality_evidence(self):
        work = self.work()
        result, _ = self.run_operator("parcel", "monitor-sample", "1")
        self.assertEqual(result.returncode, 65, result.stderr)
        (work / "equality.json").write_text(json.dumps({"passed": True}))
        result, _ = self.run_operator("parcel", "monitor-sample", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            (self.etc / "parcel-serving-monitor.env").read_text(),
            f"FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_MONITOR_SAMPLE_PATH={work}/equality.json\n",
        )


if __name__ == "__main__":
    unittest.main()
