"""The scheduled by-PNU bake (scripts/ops/by-pnu-serving-bake.sh) against a fake publisher.

The fake stands in for the three publisher commands and keeps their contracts: the state command
writes the lane state, the export refuses a shard over its row cap with the same words the real
one uses, and every call's environment is recorded. The release layout is the host's (root
ADR-0134): the script and its runtime helper in releases/<sha>, the fake in artifacts/<sha> with a
build.json that seals it. PNUs are synthetic (99999...).
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

OPS = job_specs.PLATFORM_ROOT / "scripts/ops"
RELEASE_ID = "f" * 40
# Twelve synthetic parcels under one 6-digit prefix: the shard plan has to split down to them.
PNUS = ["999991" + digit + "000000000001" for digit in "0123456789"] + [
    "9999910000000000002", "9999910000000000003"]

FAKE_PUBLISHER = r'''#!/usr/bin/env python3
import json, os, sys
command = sys.argv[1]
env = {key: value for key, value in os.environ.items() if "_BY_PNU_SERVING_" in key}
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps({"command": command, "env": env}) + "\n")
lane = "PARCEL" if "parcel" in command else "BUILDING"
prefix_env = f"FOUNDATION_PLATFORM_{lane}_BY_PNU_SERVING_"
if command.startswith("show-"):
    with open(os.environ["FOUNDATION_PLATFORM_BY_PNU_SERVING_STATE_PATH"], "w") as out:
        json.dump({"schema_version": "foundation-platform.by_pnu_serving_state.v1",
                   "gold_iceberg_snapshot_id": os.environ["FAKE_GOLD"] or None,
                   "published": {"current_generation": int(os.environ["FAKE_PUBLISHED_GENERATION"]),
                                 "gold_iceberg_snapshot_id": os.environ["FAKE_PUBLISHED_SNAPSHOT"],
                                 "object_count": 1}}, out)
elif command.startswith("export-"):
    prefix = os.environ[prefix_env + "PNU_PREFIX"]
    pnus = json.loads(os.environ["FAKE_PNUS"])
    kept = [pnu for pnu in pnus if pnu.startswith(prefix)]
    crash = os.environ.get("FAKE_CRASH_ONCE_PREFIX")
    marker = os.environ["FAKE_LOG"] + ".crashed"
    if crash == prefix and not os.path.exists(marker):
        open(marker, "w").close()
        sys.exit("Error: R2 answered 429 Reduce your concurrent request rate")
    if len(kept) > int(os.environ["FAKE_CAP"]):
        sys.exit(f"Error: snapshot keeps {len(kept)} rows in memory; this export refuses more than "
                 f"{os.environ['FAKE_CAP']} — shard the run with {prefix_env}PNU_PREFIX, not a bigger heap")
    exported = len(kept) - (1 if os.environ.get("FAKE_SHORT_PREFIX") == prefix else 0)
    snapshot = os.environ.get("FAKE_MOVED_SNAPSHOT") if os.environ.get("FAKE_MOVED_PREFIX") == prefix else None
    with open(os.environ[prefix_env + "SUMMARY_PATH"], "w") as out:
        json.dump({"gold_iceberg_snapshot_id": snapshot or os.environ["FAKE_GOLD"],
                   "target_generation": int(os.environ[prefix_env + "TARGET_GENERATION"]),
                   "pnu_prefix": prefix, "scanned_row_count": len(pnus), "exported_row_count": exported,
                   "overwritten_object_count": 0, "artifacts": [{"pnu": pnu} for pnu in kept]}, out)
elif command.startswith("publish-"):
    pass
else:
    sys.exit(f"unexpected command {command}")
'''


class ByPnuServingBake(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="by-pnu-bake-")
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        base = self.root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("by-pnu-serving-bake.sh", "admitted-writer-runtime.sh"):
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
        self.script = base / "current/scripts/ops/by-pnu-serving-bake.sh"
        self.state_root = self.root / "data/by-pnu-bake"
        self.log = self.root / "calls.jsonl"
        self.env = {
            "PATH": os.environ["PATH"], "FAKE_LOG": str(self.log), "FAKE_PNUS": json.dumps(PNUS),
            "FAKE_CAP": "4", "FAKE_GOLD": "202", "FAKE_PUBLISHED_GENERATION": "2",
            "FAKE_PUBLISHED_SNAPSHOT": "101",
            "FOUNDATION_BY_PNU_BAKE_STATE_ROOT": str(self.state_root),
            "FOUNDATION_BY_PNU_BAKE_RETRY_SECONDS": "0",
        }

    def bake(self, unit="parcel", **env):
        result = subprocess.run(["bash", str(self.script), unit], env={**self.env, **env},
                                capture_output=True, text=True, timeout=120)
        calls = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        return result, calls

    def published(self, calls):
        return [call for call in calls if call["command"].startswith("publish-")]

    def test_a_snapshot_the_manifest_already_serves_is_nothing_to_do(self):
        result, calls = self.bake(FAKE_GOLD="101")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("nothing to do: generation 2 already serves Gold snapshot 101", result.stdout)
        self.assertEqual([call["command"] for call in calls], ["show-parcel-by-pnu-serving-state"])

    def test_a_complete_bake_publishes_the_next_generation_with_the_gold_row_count(self):
        result, calls = self.bake()
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [publish] = self.published(calls)
        env = publish["env"]
        self.assertEqual(publish["command"], "publish-parcel-by-pnu-serving-manifest")
        self.assertEqual(env["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_TARGET_GENERATION"], "3")
        self.assertEqual(env["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_EXPECTED_OBJECT_COUNT"], str(len(PNUS)))
        self.assertEqual(env["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID"], "202")
        self.assertEqual(env["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_PUBLISH_FROM_LISTING"], "true")
        # The plan split 9 → 99 → ... → 999991 → its ten children and remembers the leaves. They
        # partition the PNU space: every parcel falls under exactly one, none is a split parent.
        plan = (self.state_root / "parcel/shard-plan.txt").read_text().split()
        self.assertTrue({"999991" + digit for digit in "0123456789"} <= set(plan))
        self.assertFalse({"9", "99", "999991"} & set(plan))
        for pnu in PNUS:
            self.assertEqual(sum(pnu.startswith(prefix) for prefix in plan), 1, pnu)
        self.assertEqual(sum(len(prefix) == 1 for prefix in plan), 8)
        self.assertFalse((self.state_root / "parcel/in-progress.json").exists())
        summaries = list((self.state_root / "parcel/runs/202-g3").glob("shard-*.json"))
        self.assertTrue(summaries)
        self.assertTrue(all("artifacts" not in json.loads(path.read_text()) for path in summaries))
        # The next run starts from the learned leaves and has nothing to do once published.
        self.log.unlink()
        result, calls = self.bake(FAKE_GOLD="202", FAKE_PUBLISHED_GENERATION="3", FAKE_PUBLISHED_SNAPSHOT="202")
        self.assertIn("nothing to do", result.stdout)

    def test_an_incomplete_bake_is_not_published(self):
        result, calls = self.bake(FAKE_SHORT_PREFIX="9999913")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(f"incomplete bake: shards exported {len(PNUS) - 1} of the Gold snapshot's {len(PNUS)} rows",
                      result.stderr)
        self.assertEqual(self.published(calls), [])
        # A rerun of the same snapshot resumes the same generation, not a new one.
        self.assertEqual(json.loads((self.state_root / "parcel/in-progress.json").read_text())["target_generation"], 3)

    def test_gold_moving_during_the_bake_is_not_published(self):
        result, calls = self.bake(FAKE_MOVED_PREFIX="9999915", FAKE_MOVED_SNAPSHOT="303")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("the table moved during the bake", result.stderr)
        self.assertEqual(self.published(calls), [])

    def test_objects_are_create_only_whatever_the_environment_says(self):
        planted = {f"FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_{name}": "true"
                   for name in ("ALLOW_OVERWRITE", "ALLOW_REPOINT", "FIRST_PUBLICATION")}
        result, calls = self.bake("building", **planted)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        for call in calls:
            with self.subTest(call=call["command"]):
                for name in planted:
                    self.assertNotIn(name, call["env"])
        exports = [call for call in calls if call["command"] == "export-building-by-pnu-serving"]
        self.assertTrue(exports)
        for call in exports:
            self.assertEqual(call["env"]["FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_RESUME_FROM_LISTING"], "true")
            self.assertEqual(call["env"]["FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_CONFIRM_EXPORT"], "true")

    def test_a_crashed_shard_is_retried_and_resumes(self):
        result, calls = self.bake(FAKE_CRASH_ONCE_PREFIX="9999917")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        attempts = [call for call in calls if call["env"].get("FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_PNU_PREFIX") == "9999917"]
        self.assertEqual(len(attempts), 2)
        self.assertEqual(len(self.published(calls)), 1)

    def test_a_shard_that_never_bakes_fails_the_job_without_publishing(self):
        result, calls = self.bake(FAKE_CAP="0")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("full PNU length and still too large", result.stdout)
        self.assertEqual(self.published(calls), [])

    def test_a_snapshot_change_after_a_half_bake_starts_a_new_generation(self):
        self.bake(FAKE_SHORT_PREFIX="9999913")
        self.log.unlink()
        result, calls = self.bake(FAKE_GOLD="404")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [publish] = self.published(calls)
        self.assertEqual(publish["env"]["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_TARGET_GENERATION"], "4")

    def test_an_unknown_lane_is_refused(self):
        result, calls = self.bake("tiles")
        self.assertEqual(result.returncode, 64)
        self.assertEqual(calls, [])

    def test_work_files_stay_on_the_data_disk(self):
        unit = (job_specs.SYSTEMD / "foundation-by-pnu-serving-bake@.service").read_text(encoding="utf-8")
        writable = [line.split("=", 1)[1].split() for line in unit.splitlines() if line.startswith("ReadWritePaths=")]
        self.assertEqual(writable, [["/data/foundation-platform/by-pnu-bake"]])
        self.assertIn("ProtectSystem=strict\n", unit)
        script = (OPS / "by-pnu-serving-bake.sh").read_text(encoding="utf-8")
        self.assertIn('STATE_ROOT="${FOUNDATION_BY_PNU_BAKE_STATE_ROOT:-/data/foundation-platform/by-pnu-bake}', script)


if __name__ == "__main__":
    unittest.main()
