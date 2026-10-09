"""The operator's building section pack measurement (scripts/ops/measure-building-section-packs.sh).

A fake publisher stands in for `export-building-by-pnu-section-packs` and records the environment
of every call; a fake time tool stands in for GNU time. The release layout is the host's (root
ADR-0134), as in test_by_pnu_serving_bake.py. Planted failures prove the script refuses to write
anywhere but the data disk, to write over an earlier measurement, and to let a write reach R2.
"""

import hashlib
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

ORCHESTRATION = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ORCHESTRATION / "dags"))

import job_specs  # noqa: E402

sys.path.insert(0, str(job_specs.PLATFORM_ROOT / "scripts/deploy"))
import runtime_secrets  # noqa: E402

OPS = job_specs.PLATFORM_ROOT / "scripts/ops"
# What the run's environment files must carry, from the one contract (root ADR-0153); the fake
# publisher refuses without any of it, as the real Gold read does.
NEEDS = sorted(runtime_secrets.load().consumer("measure-building-section-packs").needs)
RELEASE_ID = "e" * 40
PREFIX = "FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_"

FAKE_PUBLISHER = r'''#!/usr/bin/env python3
import json, os, sys
# The real export reads Gold with the R2 key pair before it bakes anything (there is no
# read-only pair). The names are the contract's (FAKE_REQUIRED), not a list kept here.
for key in os.environ["FAKE_REQUIRED"].split(","):
    if not os.environ.get(key):
        sys.exit("failed to configure lakehouse R2 reads: " + key + " environment variable is required")
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps({"command": sys.argv[1], "env": dict(os.environ)}) + "\n")
prefix = os.environ["FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_PNU_PREFIX"]
snapshot = os.environ.get("FAKE_SNAPSHOT_" + prefix, "999990000000000001")
total = {"packs": 2, "documents": 10, "tombstones": 0, "bytes": 5000, "head_bytes": 700,
         "largest_pack_bytes": 3000 + int(prefix), "largest_head_bytes": 400}
with open(os.environ["FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_PACK_SUMMARY_PATH"], "w") as out:
    json.dump({"gold_iceberg_snapshot_id": snapshot, "exported_row_count": 10,
               "totals": {name: total for name in ("buildings", "floors", "units", "unit_prices")}}, out)
'''

FAKE_TIME = r'''#!/usr/bin/env python3
import subprocess, sys
args = sys.argv[1:]
output = args[args.index("-o") + 1]
command = args[args.index("-o") + 2:]
code = subprocess.run(command).returncode
with open(output, "w") as out:
    out.write("12.5 204800\n")
sys.exit(code)
'''


class MeasureBuildingSectionPacks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="measure-packs-")
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        base = self.root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("measure-building-section-packs.sh", "admitted-writer-runtime.sh", "by-pnu-bake-shards.sh"):
            (release / "scripts/ops" / name).write_bytes((OPS / name).read_bytes())
            (release / "scripts/ops" / name).chmod(0o755)
        (base / "current").symlink_to(pathlib.Path("releases") / RELEASE_ID)
        artifacts = base / "artifacts" / RELEASE_ID
        artifacts.mkdir(parents=True)
        publisher = artifacts / "foundation-outbox-publisher"
        publisher.write_text(FAKE_PUBLISHER)
        publisher.chmod(0o555)
        (artifacts / "build.json").write_text(json.dumps({
            "source": RELEASE_ID, "publisher_image": "sha256:" + "a" * 64,
            "tippecanoe_image": "sha256:" + "d" * 64,
            "files": {"foundation-outbox-publisher": hashlib.sha256(FAKE_PUBLISHER.encode()).hexdigest(),
                      "jars/fixture.jar": "0" * 64}}))
        time_tool = self.root / "time"
        time_tool.write_text(FAKE_TIME)
        time_tool.chmod(0o755)
        self.data = self.root / "data"
        state = self.data / "by-pnu-bake/building"
        state.mkdir(parents=True)
        (state / "shard-plan.txt").write_text("1\n2\n")
        self.script = base / "current/scripts/ops/measure-building-section-packs.sh"
        self.log = self.root / "calls.jsonl"
        self.env = {
            "PATH": os.environ["PATH"], "FAKE_LOG": str(self.log),
            "FOUNDATION_PACK_MEASURE_DATA_ROOT": f"{self.data}/",
            "FOUNDATION_PACK_MEASURE_TIME_BIN": str(time_tool),
            "FOUNDATION_BY_PNU_BAKE_STATE_ROOT": str(self.data / "by-pnu-bake"),
            "DATABASE_URL": "postgresql://fixture@127.0.0.1:1/fixture",
            "FAKE_REQUIRED": ",".join(NEEDS),
            # What the run's environment files carry (every name the contract gives it), and an R2
            # output the measurement must not pass on.
            **{name: "planted-" + name.lower() for name in NEEDS},
            PREFIX + "OUTPUT_STORAGE_DRIVER": "r2",
        }

    def measure(self, out, **env):
        result = subprocess.run(["bash", str(self.script), str(out)], env={**self.env, **env},
                                capture_output=True, text=True, timeout=60)
        calls = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        return result, calls

    def test_a_measurement_bakes_every_shard_locally_and_sums_it(self):
        out = self.data / "measure"
        result, calls = self.measure(out)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual([call["env"][PREFIX + "PNU_PREFIX"] for call in calls], ["1", "2"])
        for call in calls:
            env = call["env"]
            self.assertEqual(call["command"], "export-building-by-pnu-section-packs")
            self.assertEqual(env[PREFIX + "OUTPUT_STORAGE_DRIVER"], "local")
            self.assertEqual(env[PREFIX + "OUTPUT_ROOT"], f"{out}/packs")
            for name in NEEDS:
                self.assertEqual(env[name], "planted-" + name.lower())
        # Every shard after the first is held to the first shard's Gold snapshot.
        self.assertNotIn(PREFIX + "EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID", calls[0]["env"])
        self.assertEqual(calls[1]["env"][PREFIX + "EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID"], "999990000000000001")
        summary = json.loads((out / "measurement.json").read_text())
        self.assertEqual(summary["documents"], 20)
        self.assertEqual(summary["packs"], 16)
        self.assertEqual(summary["projected_r2_class_a_writes"], 18)
        self.assertEqual(summary["sections"]["floors"]["largest_pack_bytes"], 3002)
        self.assertEqual(summary["bake_seconds"], 25.0)
        self.assertEqual(summary["peak_resident_bytes"], 204800 * 1024)
        self.assertNotIn("planted-", result.stdout + result.stderr)

    def test_an_earlier_measurement_or_the_root_disk_is_refused(self):
        out = self.data / "measure"
        out.mkdir()
        (out / "left-over").write_text("x")
        result, calls = self.measure(out)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("is not empty", result.stdout)
        self.assertEqual(calls, [])
        result, calls = self.measure(self.root / "elsewhere")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("data disk", result.stdout)
        self.assertEqual(calls, [])

    def test_a_measurement_without_a_needed_name_is_refused_before_any_shard(self):
        # Planted: the environment without one name the contract gives the run. The first released
        # script ran without the Gold read's key pair and failed at shard 1 (2026-10-04); now it
        # refuses before the first shard and writes nothing.
        self.assertTrue(NEEDS, "the contract gives the run no names")
        for name in NEEDS:
            with self.subTest(name):
                env = {k: v for k, v in self.env.items() if k != name}
                out = self.data / "measure"
                result = subprocess.run(["bash", str(self.script), str(out)], env=env,
                                        capture_output=True, text=True, timeout=60)
                self.assertEqual(result.returncode, 78, result.stdout + result.stderr)
                self.assertIn(name, result.stdout)
                self.assertFalse(self.log.exists(), "no shard started")
                self.assertFalse((out / "measurement.json").exists())

    def test_shards_of_two_snapshots_are_not_one_measurement(self):
        result, _ = self.measure(self.data / "measure", FAKE_SNAPSHOT_2="999990000000000002")
        self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
