"""The operator's section pack generation bake (scripts/ops/by-pnu-pack-bake.sh, root ADR-0166).

The dispatcher (`by-pnu-pack-operator.sh <lane> bake <generation>`) starts it in a transient unit;
here it runs directly against a fake publisher in the host's release layout (root ADR-0134), as in
test_by_pnu_serving_bake.py. The fake export keeps the real refusals the bake reacts to: a shard
over the row cap ("shard the run with ..._PNU_PREFIX"), a Gold table that moved away from the
pinned snapshot ("moved during the bake"), and transient read failures. PNUs and snapshot ids are
synthetic.
"""

import fcntl
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
RELEASE_ID = "b" * 40
PREFIX = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_"
# Twelve synthetic parcels (the repository's reserved 99999 range) under one 6-digit prefix: the
# plan has to split 9 down to its children, and shards 1..8 are baked empty.
PNUS = ["999991" + digit + "000000000001" for digit in "0123456789"] + [
    "9999910000000000002", "9999910000000000003"]
SPLIT = ["9", "99", "999", "9999", "99999", "999991"]
LEAVES = ["1", "2", "3", "4", "5", "6", "7", "8", "90", "91", "92", "93", "94", "95", "96", "97", "98",
          "990", "991", "992", "993", "994", "995", "996", "997", "998",
          "9990", "9991", "9992", "9993", "9994", "9995", "9996", "9997", "9998",
          "99990", "99991", "99992", "99993", "99994", "99995", "99996", "99997", "99998",
          "999990", "999992", "999993", "999994", "999995", "999996", "999997", "999998", "999999",
          *("999991" + digit for digit in "0123456789")]

FAKE_PUBLISHER = r'''#!/usr/bin/env python3
import json, os, sys, time
command = sys.argv[1]
P = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_"
env = {key: value for key, value in os.environ.items() if key.startswith(P) or key == "DATABASE_URL"}
prefix = os.environ.get(P + "PNU_PREFIX", "")
started = time.monotonic()
def record(outcome):
    with open(os.environ["FAKE_LOG"], "a") as log:
        log.write(json.dumps({"command": command, "env": env, "prefix": prefix, "outcome": outcome,
                              "started": started, "ended": time.monotonic()}) + "\n")
assert command == "export-parcel-by-pnu-section-packs", command
assert os.environ.get(P + "CONFIRM_PACK_EXPORT") == "true" and os.environ.get(P + "OUTPUT_STORAGE_DRIVER") == "r2"
# Long enough that shards run side by side when the bake lets them.
time.sleep(float(os.environ.get("FAKE_SECONDS", "0.3")))
gold_file = os.environ["FAKE_GOLD_FILE"]
gold = open(gold_file).read().strip()
expected = os.environ.get(P + "EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID")
if expected and expected != gold:
    record("moved")
    sys.exit(f"Error: gold.parcel_panel moved during the bake: this bake is of snapshot {expected} but the table is now at {gold}")
failures = json.loads(os.environ.get("FAKE_FAIL", "{}"))
counter = os.environ["FAKE_LOG"] + ".attempts-" + prefix
attempts = int(open(counter).read()) if os.path.exists(counter) else 0
open(counter, "w").write(str(attempts + 1))
if attempts < failures.get(prefix, 0):
    record("transient")
    sys.exit("Error: failed to read the body of a Gold data file: connection reset by peer")
pnus = json.loads(os.environ["FAKE_PNUS"])
kept = [pnu for pnu in pnus if pnu.startswith(prefix)]
if len(kept) > int(os.environ["FAKE_CAP"]):
    record("cap")
    sys.exit(f"Error: this shard keeps more than {os.environ['FAKE_CAP']} rows; shard the run with {P}PNU_PREFIX, not a bigger heap")
with open(os.environ[P + "PACK_SUMMARY_PATH"], "w") as out:
    json.dump({"schema_version": "foundation-platform.parcel_by_pnu_section_pack_export_summary.v1",
               "gold_iceberg_snapshot_id": gold, "generation": int(os.environ[P + "PACK_GENERATION"]),
               "section_generations": {}, "patch": None, "pnu_prefix": prefix,
               "gold_record_count": len(pnus), "exported_row_count": len(kept), "tombstone_count": 0}, out)
record("baked")
# The Gold table commits a new snapshot once the first shard is baked.
if os.environ.get("FAKE_MOVE_TO"):
    open(gold_file, "w").write(os.environ["FAKE_MOVE_TO"])
'''


class PackBake(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="pack-bake-")
        self.addCleanup(temp.cleanup)
        self.root = pathlib.Path(temp.name)
        base = self.root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("by-pnu-pack-bake.sh", "by-pnu-bake-shards.sh", "admitted-writer-runtime.sh"):
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
        self.script = base / "current/scripts/ops/by-pnu-pack-bake.sh"
        self.state_root = self.root / "data/by-pnu-bake"
        self.work = self.state_root / "parcel-pack-g2"
        self.gold = self.root / "gold"
        self.gold.write_text("202")
        self.log = self.root / "calls.jsonl"
        self.env = {
            "PATH": os.environ["PATH"], "FAKE_LOG": str(self.log), "FAKE_PNUS": json.dumps(PNUS),
            "FAKE_CAP": "4", "FAKE_GOLD_FILE": str(self.gold),
            "FOUNDATION_BY_PNU_BAKE_STATE_ROOT": str(self.state_root),
            "FOUNDATION_BY_PNU_BAKE_RETRY_SECONDS": "0",
            # A lane variable an environment file might carry never reaches the export.
            PREFIX + "TARGET_PATCH": "7",
        }

    def run_bake(self, *args, **env):
        result = subprocess.run(["bash", str(self.script), *(args or ("parcel", "2"))], env={**self.env, **env},
                                capture_output=True, text=True, timeout=300)
        calls = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        return result, calls

    def summaries(self):
        return sorted(path.name[len("shard-"):-len(".json")] for path in (self.work / "summaries").glob("shard-*.json"))

    def test_a_generation_is_baked_shard_by_shard_splitting_what_the_row_cap_refuses(self):
        result, calls = self.run_bake()
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        # Every PNU falls under exactly one baked shard; no split parent has a summary.
        self.assertEqual(self.summaries(), sorted(LEAVES))
        for pnu in PNUS:
            self.assertEqual(sum(pnu.startswith(shard) for shard in LEAVES), 1, pnu)
        self.assertEqual(sorted(path.name for path in (self.work / "shards").glob("*.split")),
                         sorted(f"shard-{shard}.split" for shard in SPLIT))
        self.assertIn(f"complete: generation 2 holds {len(PNUS)} documents of Gold snapshot 202 in {len(LEAVES)} shards",
                      result.stdout)
        self.assertIn("shard 999991 holds more rows than one run may keep; split into 9999910..9999919", result.stdout)
        # Only the export runs; the publish stays a separate dispatcher action.
        self.assertEqual({call["command"] for call in calls}, {"export-parcel-by-pnu-section-packs"})
        for call in calls:
            self.assertEqual(call["env"][PREFIX + "PACK_GENERATION"], "2")
            self.assertNotIn(PREFIX + "TARGET_PATCH", call["env"])
        # The first shard runs alone and unpinned; every later one is pinned to its snapshot.
        first, *rest = sorted(calls, key=lambda call: call["started"])
        self.assertEqual(first["prefix"], "1")
        self.assertNotIn(PREFIX + "EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID", first["env"])
        self.assertEqual({call["env"].get(PREFIX + "EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID") for call in rest}, {"202"})
        self.assertTrue(all(call["started"] >= first["ended"] for call in rest))
        # Then the workers run side by side, never more than the constant allows.
        events = sorted([(call["started"], 1) for call in rest] + [(call["ended"], -1) for call in rest])
        running = widest = 0
        for _, step in events:
            running += step
            widest = max(widest, running)
        self.assertGreater(widest, 1)
        self.assertLessEqual(widest, 4)

    def test_a_rerun_resumes_baked_shards_and_goes_straight_to_split_children(self):
        # A leaf of the deepest split fails every attempt: by then every split has happened.
        result, calls = self.run_bake(FAKE_FAIL=json.dumps({"9999913": 99}))
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("FAILED: shard 9999913 did not bake in 3 attempts", result.stdout)
        self.assertIn("nothing was published; the same action resumes", result.stdout.lower())
        self.assertEqual(sum(call["prefix"] == "9999913" for call in calls), 3)
        self.assertNotIn("9999913", self.summaries())
        baked_before = set(self.summaries())
        self.assertTrue(baked_before)
        self.log.unlink()
        for counter in self.root.glob("calls.jsonl.attempts-*"):
            counter.unlink()
        result, calls = self.run_bake()
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("resuming generation 2 of Gold snapshot 202", result.stdout)
        # Nothing baked is exported again, and a split parent is not tried again.
        self.assertFalse({call["prefix"] for call in calls} & baked_before)
        self.assertFalse({call["prefix"] for call in calls} & set(SPLIT))
        self.assertEqual(self.summaries(), sorted(LEAVES))
        # The pin comes from the summaries already there, for the first shard of this run too.
        self.assertEqual({call["env"].get(PREFIX + "EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID") for call in calls}, {"202"})

    def test_a_shard_that_fails_transiently_is_retried(self):
        result, calls = self.run_bake(FAKE_FAIL=json.dumps({"3": 2}))
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual([call["outcome"] for call in calls if call["prefix"] == "3"], ["transient", "transient", "baked"])
        self.assertIn("shard 3 attempt 2/3 failed", result.stdout)
        self.assertIn("shard 3 baked: 0 documents of Gold snapshot 202", result.stdout)

    def test_a_gold_table_that_moves_stops_the_bake_and_never_mixes_snapshots(self):
        result, calls = self.run_bake(FAKE_MOVE_TO="303")
        self.assertEqual(result.returncode, 1, result.stdout)
        # Every shard after the first was pinned, so the moved table refused it at once (no retry).
        moved = [call for call in calls if call["outcome"] == "moved"]
        self.assertTrue(moved)
        self.assertEqual(len(moved), len({call["prefix"] for call in moved}))
        self.assertEqual({json.loads((self.work / "summaries" / f"shard-{shard}.json").read_text())
                          ["gold_iceberg_snapshot_id"] for shard in self.summaries()}, {"202"})
        # Summaries of two snapshots are never resumed.
        (self.work / "summaries/shard-2.json").write_text(json.dumps({
            "gold_iceberg_snapshot_id": "303", "generation": 2, "patch": None, "pnu_prefix": "2"}))
        self.log.unlink()
        result, calls = self.run_bake()
        self.assertEqual(result.returncode, 65, result.stdout)
        self.assertIn("more than one Gold snapshot", result.stderr)
        self.assertEqual(calls, [])

    def test_the_lane_lock_and_the_arguments_are_checked_before_anything_runs(self):
        (self.state_root / "parcel").mkdir(parents=True)
        with open(self.state_root / "parcel/lane.lock", "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result, calls = self.run_bake()
        self.assertEqual(result.returncode, 75, result.stdout)
        self.assertIn("parcel/lane.lock", result.stdout)
        self.assertEqual(calls, [])
        for args in (("parcel",), ("parcel", "0"), ("parcel", "2", "3"), ("lot", "2"), ("parcel", "../2")):
            with self.subTest(args=args):
                result, calls = self.run_bake(*args)
                self.assertEqual(result.returncode, 64, result.stderr)
                self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
