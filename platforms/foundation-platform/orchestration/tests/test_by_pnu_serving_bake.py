"""The scheduled by-PNU bake (scripts/ops/by-pnu-serving-bake.sh) against a fake publisher.

The fake stands in for the three publisher commands and keeps their contracts: the state command
writes the lane state (v2: the served base, its patches, the reflected snapshot and the contract
bounds), the export refuses a shard over its row cap with the same words the real one uses, and
every call's environment is recorded. A fake `docker` stands in for the change-set Spark job
(`by_pnu_panel_delta.py`) and writes its three files where the container would. The release
layout is the host's (root ADR-0134): the script and its runtime helper in releases/<sha>, the fake
in artifacts/<sha> with a build.json that seals it. PNUs are synthetic (99999...).

Most tests drive the full path: their base was baked with another document schema, which only a
full bake can serve (root ADR-0141 §5). `PatchPath` drives the patch, reflect and choice paths.
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
        json.dump({"schema_version": "foundation-platform.by_pnu_serving_state.v2",
                   "gold_iceberg_snapshot_id": os.environ["FAKE_GOLD"] or None,
                   "document_schema_version": "doc.v2",
                   "published": {"manifest_schema_version": 2,
                                 "base_generation": int(os.environ["FAKE_PUBLISHED_GENERATION"]),
                                 "base_object_count": int(os.environ.get("FAKE_BASE_OBJECTS", "100")),
                                 "document_schema_version": os.environ.get("FAKE_SERVED_SCHEMA", "doc.v1"),
                                 "gold_iceberg_snapshot_id": os.environ["FAKE_PUBLISHED_SNAPSHOT"],
                                 "reflected_gold_iceberg_snapshot_id": os.environ.get(
                                     "FAKE_REFLECTED", os.environ["FAKE_PUBLISHED_SNAPSHOT"]),
                                 "patch_count": int(os.environ.get("FAKE_PATCH_COUNT", "0")),
                                 "newest_patch": int(os.environ.get("FAKE_NEWEST_PATCH", "0")),
                                 "cumulative_changes": int(os.environ.get("FAKE_CUMULATIVE", "0")),
                                 "object_count": 100,
                                 "pnu_prefix_length": int(os.environ.get("FAKE_SERVED_PREFIX_LENGTH", "5"))},
                   "generations_with_objects": json.loads(os.environ.get("FAKE_LISTED", "[]")),
                   "patches_with_objects": json.loads(os.environ.get("FAKE_PATCHES_LISTED", "[]")),
                   "policy": {"max_patches": 7, "max_cumulative_change_ratio": 0.05,
                              "max_delta_fraction": 0.5, "pnu_prefix_length": 5}}, out)
elif command.startswith("export-"):
    prefix = os.environ.get(prefix_env + "PNU_PREFIX", "")
    target = int(os.environ[prefix_env + "TARGET_GENERATION"])
    patch = os.environ.get(prefix_env + "TARGET_PATCH")
    def listed(name):
        path = os.environ.get(prefix_env + name)
        return None if path is None else open(path).read().split()
    allow, deletes = listed("PNU_ALLOWLIST_PATH"), listed("DELETE_LIST_PATH") or []
    moved = os.environ.get("FAKE_MOVED_SNAPSHOT") if os.environ.get("FAKE_MOVED_PREFIX") == prefix else None
    expected = os.environ.get(prefix_env + "EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID")
    if moved and expected and expected != moved and not os.environ.get("FAKE_UNCHECKED_SNAPSHOT"):
        sys.exit(f"Error: gold.panel moved during the bake: this bake is of snapshot {expected} but the table is now at {moved}")
    pnus = json.loads(os.environ["FAKE_PNUS"])
    kept = [pnu for pnu in pnus if pnu.startswith(prefix) and (allow is None or pnu in allow)]
    tombstones = [pnu for pnu in deletes if pnu.startswith(prefix)]
    if len(kept) > int(os.environ["FAKE_CAP"]):
        sys.exit(f"Error: snapshot keeps {len(kept)} rows in memory; this export refuses more than "
                 f"{os.environ['FAKE_CAP']} — shard the run with {prefix_env}PNU_PREFIX, not a bigger heap")
    # A crash during the scan: the empty-range check never ran.
    before = os.environ["FAKE_LOG"] + ".crashed-before-check"
    if os.environ.get("FAKE_CRASH_BEFORE_CHECK_ONCE_PREFIX") == prefix and not os.path.exists(before):
        open(before, "w").close()
        sys.exit("Error: connection reset while reading a Gold data file")
    # Objects an unrecorded bake left in this generation (what the real export lists).
    if os.environ.get(prefix_env + "FRESH_GENERATION") == "true":
        if os.environ.get("FAKE_CLAIMED_GENERATION") == str(target) and prefix.startswith(os.environ.get("FAKE_CLAIMED_PREFIX", "")):
            sys.exit(f"Error: generation {target} already holds 3 objects under shard {prefix}, and this run did not start them")
        with open(os.environ[prefix_env + "FRESH_CHECK_MARKER_PATH"], "w") as checked:
            json.dump({"target_generation": target, "pnu_prefix": prefix, "listed_object_count": 0}, checked)
    # A crash while writing, after the check passed.
    crash = os.environ.get("FAKE_CRASH_ONCE_PREFIX")
    marker = os.environ["FAKE_LOG"] + ".crashed"
    if crash == prefix and not os.path.exists(marker):
        open(marker, "w").close()
        sys.exit("Error: R2 answered 429 Reduce your concurrent request rate")
    short = os.environ.get("FAKE_SHORT_PREFIX") == prefix and os.environ.get("FAKE_SHORT_LANE", lane) == lane
    exported = len(kept) - (1 if short else 0)
    lost_tombstone = 1 if tombstones and os.environ.get("FAKE_LOSE_TOMBSTONE") else 0
    snapshot = moved
    with open(os.environ[prefix_env + "SUMMARY_PATH"], "w") as out:
        json.dump({"gold_iceberg_snapshot_id": snapshot or os.environ["FAKE_GOLD"],
                   "target_generation": target, "target_patch": int(patch) if patch else None,
                   "pnu_prefix": prefix or None, "scanned_row_count": len(pnus),
                   "exported_row_count": exported, "tombstone_count": len(tombstones) - lost_tombstone,
                   "artifacts": [{"pnu": pnu} for pnu in kept + tombstones]}, out)
elif command.startswith("verify-"):
    # The verified re-base (root ADR-0146 §1): reads every served object, writes a change set.
    work = os.environ[prefix_env + "REBASE_WORK_DIR"]
    os.makedirs(work, exist_ok=True)
    # Like the real one, a work directory belongs to one run over one served state.
    newest, count = int(os.environ.get("FAKE_NEWEST_PATCH", "0")), int(os.environ.get("FAKE_PATCH_COUNT", "0"))
    state = {"run_id": os.environ[prefix_env + "REBASE_RUN_ID"],
             "reflected_gold_iceberg_snapshot_id": os.environ.get("FAKE_REFLECTED", os.environ["FAKE_PUBLISHED_SNAPSHOT"]),
             "base_generation": int(os.environ["FAKE_PUBLISHED_GENERATION"]),
             "patches": [newest - n for n in range(count)]}
    state_path = os.path.join(work, "state.json")
    if os.path.exists(state_path):
        if json.load(open(state_path)) != state:
            sys.exit(f"Error: {work} holds another run's work; a re-base resumes only its own run")
    else:
        json.dump(state, open(state_path, "w"))
    if os.environ.get("FAKE_REBASE_FAIL"):
        sys.exit("Error: " + os.environ["FAKE_REBASE_FAIL"])
    upserts = json.loads(os.environ.get("FAKE_REBASE_UPSERTS", "[]"))
    deletes = json.loads(os.environ.get("FAKE_REBASE_DELETES", "[]"))
    new = int(os.environ.get("FAKE_REBASE_NEW", "0"))
    for name, pnus in (("upserts.txt", upserts), ("deletes.txt", deletes)):
        with open(os.path.join(work, name), "w") as out:
            out.writelines(pnu + "\n" for pnu in pnus)
    read = 100
    with open(os.path.join(work, "change-set.json"), "w") as out:
        json.dump({"job_name": "by_pnu_serving_rebase_verify",
                   "quality_metrics": {"upsert_count": len(upserts), "delete_count": len(deletes),
                                       "new_count": new},
                   "input": {"baseline_snapshot_id": os.environ.get("FAKE_REFLECTED", "101"),
                             "current_snapshot_id": os.environ[prefix_env + "EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID"]},
                   "verification": {"run_id": os.environ[prefix_env + "REBASE_RUN_ID"],
                                    "reason": os.environ[prefix_env + "REBASE_REASON"],
                                    "served_objects_read": read,
                                    "equal": read - (len(upserts) - new) - len(deletes),
                                    "changed": len(upserts) - new, "only_served": len(deletes),
                                    "only_gold": new}}, out)
elif command.startswith("publish-"):
    pass
else:
    sys.exit(f"unexpected command {command}")
'''

# The change-set Spark job, as `docker compose ... run spark-small spark-submit ...` runs it. It
# writes where the container would: /workspace/target/lakehouse is the lane's state root.
FAKE_DOCKER = r'''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
if args[:1] == ["rm"]:
    sys.exit(0)
# Like compose's lakehouse-target-init: the container runs as uid 185, so the state root it mounts
# must be writable by anyone, not just by the job's own user.
state_root = os.environ.get("FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT", "")
if not state_root or not os.stat(state_root).st_mode & 0o002:
    print('service "lakehouse-target-init" didn\'t complete successfully: exit 1', file=sys.stderr)
    sys.exit(1)
job = next(i for i, arg in enumerate(args) if arg.endswith("by_pnu_panel_delta.py"))
options = dict(zip(args[job + 1::2], args[job + 2::2]))
service = args[args.index("spark-submit") - 1]
memory = args[args.index("--driver-memory") + 1]
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps({"command": "delta", "env": {}, "options": options,
                          "service": service, "driver_memory": memory}) + "\n")
code = int(os.environ.get("FAKE_DELTA_EXIT", "0"))
if code:
    print("by_pnu_panel_delta-refused: planted")
    sys.exit(code)
root = os.environ["FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT"]
host = lambda path: root + path.removeprefix("/workspace/target/lakehouse")
upserts = json.loads(os.environ.get("FAKE_DELTA_UPSERTS", "[]"))
deletes = json.loads(os.environ.get("FAKE_DELTA_DELETES", "[]"))
for name, pnus in (("--upsert-output", upserts), ("--delete-output", deletes)):
    with open(host(options[name]), "w") as out:
        out.writelines(pnu + "\n" for pnu in pnus)
with open(host(options["--summary-output"]), "w") as out:
    json.dump({"job_name": "by_pnu_panel_delta",
               "quality_metrics": {"upsert_count": len(upserts), "delete_count": len(deletes),
                                   "new_count": int(os.environ.get("FAKE_DELTA_NEW", "0"))},
               "input": {"baseline_snapshot_id": options["--baseline-snapshot-id"],
                         "current_snapshot_id": options["--current-snapshot-id"]}}, out)
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
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        (bin_dir / "docker").write_text(FAKE_DOCKER)
        (bin_dir / "docker").chmod(0o755)
        (artifacts / "build.json").write_text(json.dumps({
            "source": RELEASE_ID, "publisher_image": "sha256:" + "a" * 64,
            "tippecanoe_image": "sha256:" + "d" * 64,
            "files": {"foundation-outbox-publisher": hashlib.sha256(FAKE_PUBLISHER.encode()).hexdigest(),
                      "jars/fixture.jar": "0" * 64}}))
        self.script = base / "current/scripts/ops/by-pnu-serving-bake.sh"
        self.state_root = self.root / "data/by-pnu-bake"
        self.log = self.root / "calls.jsonl"
        self.env = {
            "PATH": f"{bin_dir}:{os.environ['PATH']}", "FAKE_LOG": str(self.log), "FAKE_PNUS": json.dumps(PNUS),
            "FAKE_CAP": "4", "FAKE_GOLD": "202", "FAKE_PUBLISHED_GENERATION": "2",
            "FAKE_PUBLISHED_SNAPSHOT": "101",
            "FOUNDATION_BY_PNU_BAKE_STATE_ROOT": str(self.state_root),
            "FOUNDATION_BY_PNU_BAKE_RETRY_SECONDS": "0",
            # The building export reads the runtime database; production resolves it from compose.
            "DATABASE_URL": "postgresql://fixture@127.0.0.1:1/fixture",
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
        self.assertIn("nothing to do: the served state (generation 2, 0 patches) already reflects Gold snapshot 101",
                      result.stdout)
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
        self.assertEqual(json.loads((self.state_root / "parcel/in-progress.json").read_text())["target"], 3)

    def exports(self, calls):
        return [call for call in calls if call["command"].startswith("export-")]

    def test_gold_moving_during_the_bake_stops_it_at_that_shard(self):
        result, calls = self.bake(FAKE_MOVED_PREFIX="9999915", FAKE_MOVED_SNAPSHOT="303")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("the Gold table moved off snapshot 202 during the bake", result.stdout)
        # The half-written generation is named, so its manual cleanup can find it.
        self.assertIn("abandoned: generation 3 holds the objects this bake wrote for Gold snapshot 202", result.stdout)
        self.assertEqual([call for call in calls if call["command"] == "delta"], [],
                         "a base of another document schema was weighed as a patch")
        self.assertEqual(self.published(calls), [])
        exports = self.exports(calls)
        for call in exports:
            self.assertEqual(call["env"]["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID"], "202")
        # Not retried, and no shard after it was baked: the bake stopped there, not ~20h later.
        prefix = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_PNU_PREFIX"
        self.assertEqual([call["env"][prefix] for call in exports].count("9999915"), 1)
        self.assertEqual(exports[-1]["env"][prefix], "9999915")

    def test_a_shard_summary_of_another_snapshot_is_still_not_published(self):
        # Defence behind the export's own check: the summaries are compared before publishing.
        result, calls = self.bake(FAKE_MOVED_PREFIX="9999915", FAKE_MOVED_SNAPSHOT="303", FAKE_UNCHECKED_SNAPSHOT="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("the table moved during the bake", result.stderr)
        self.assertEqual(self.published(calls), [])

    def test_a_half_written_generation_without_a_record_is_skipped_past(self):
        # The hand-kept script left generation 3 (and 5) half written from an older snapshot, and
        # this state root has no in-progress.json. Resuming 3 would count its objects as done.
        result, calls = self.bake(FAKE_LISTED="[1, 2, 3, 5]")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [publish] = self.published(calls)
        self.assertEqual(publish["env"]["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_TARGET_GENERATION"], "6")
        for call in self.exports(calls):
            self.assertEqual(call["env"]["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_TARGET_GENERATION"], "6")
            self.assertEqual(call["env"]["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_FRESH_GENERATION"], "true")

    def test_a_retry_after_a_crash_before_the_check_still_refuses_foreign_keys(self):
        # Attempt 1 crashes during the scan, before the empty-range check; another writer's keys
        # sit in the range. Attempt 2 must still demand an empty range, and refuse.
        result, calls = self.bake(FAKE_CRASH_BEFORE_CHECK_ONCE_PREFIX="1", FAKE_CLAIMED_GENERATION="3")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("generation 3 already holds objects this run did not write", result.stdout)
        exports = self.exports(calls)
        self.assertEqual(len(exports), 2)
        for call in exports:
            self.assertEqual(call["env"]["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_FRESH_GENERATION"], "true")
        self.assertEqual(self.published(calls), [])

    def test_a_retry_after_the_check_passed_resumes_its_own_writes(self):
        result, calls = self.bake(FAKE_CRASH_ONCE_PREFIX="9999917")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        flags = [call["env"]["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_FRESH_GENERATION"]
                 for call in self.exports(calls)
                 if call["env"]["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_PNU_PREFIX"] == "9999917"]
        self.assertEqual(flags, ["true", "false"])

    def test_a_refused_generation_is_never_resumed_the_next_day(self):
        # Another writer holds generation 3: the run is refused and must leave no record of it.
        result, _ = self.bake(FAKE_CLAIMED_GENERATION="3")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.state_root / "parcel/in-progress.json").exists())
        self.log.unlink()
        # The next day the bucket lists generation 3; the bake goes above it, fresh.
        result, calls = self.bake(FAKE_LISTED="[2, 3]")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [publish] = self.published(calls)
        self.assertEqual(publish["env"]["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_TARGET_GENERATION"], "4")
        self.assertIn("new generation: true", result.stdout)

    def test_a_refusal_after_other_shards_passed_removes_the_record(self):
        # Shards 1..8 pass their checks (the generation is recorded); a later shard finds another
        # writer's keys. The record must go, or the next run would resume generation 3 as its own.
        result, _ = self.bake(FAKE_CLAIMED_GENERATION="3", FAKE_CLAIMED_PREFIX="9999915")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("generation 3 already holds objects this run did not write", result.stdout)
        self.assertFalse((self.state_root / "parcel/in-progress.json").exists())
        self.log.unlink()
        result, calls = self.bake(FAKE_LISTED="[2, 3]")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [publish] = self.published(calls)
        self.assertEqual(publish["env"]["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_TARGET_GENERATION"], "4")

    def test_a_generation_is_recorded_only_after_a_shard_passed_its_check(self):
        # The first shard is refused before its check (Gold moved): nothing is recorded to resume.
        result, calls = self.bake(FAKE_MOVED_PREFIX="1", FAKE_MOVED_SNAPSHOT="303")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(self.exports(calls)), 1)
        self.assertFalse((self.state_root / "parcel/in-progress.json").exists())
        # Once a shard has passed its check, the generation is this lane's.
        self.log.unlink()
        result, _ = self.bake(FAKE_MOVED_PREFIX="2", FAKE_MOVED_SNAPSHOT="303", FAKE_GOLD="404")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(json.loads((self.state_root / "parcel/in-progress.json").read_text()),
                         {"gold_iceberg_snapshot_id": "404", "mode": "full", "base_generation": 2, "target": 3})

    def test_a_new_generation_that_already_holds_objects_is_refused(self):
        # The state listing missed them (another writer after it was read): the export refuses
        # the first shard, and the bake stops without retrying or publishing.
        result, calls = self.bake(FAKE_CLAIMED_GENERATION="3")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("generation 3 already holds objects this run did not write", result.stdout)
        self.assertEqual(self.published(calls), [])
        self.assertEqual(len(self.exports(calls)), 1)

    def test_the_run_that_recorded_a_generation_resumes_it(self):
        # A first run of snapshot 202 starts generation 3, checks shards empty and writes, then
        # fails on one shard after its check passed.
        result, _ = self.bake(FAKE_CRASH_ONCE_PREFIX="9999917", FOUNDATION_BY_PNU_BAKE_ATTEMPTS="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(json.loads((self.state_root / "parcel/in-progress.json").read_text())["target"], 3)
        self.log.unlink()
        # The next run finds generation 3 in the bucket and its own record of it: it resumes 3,
        # and its exports do not demand empty key ranges.
        result, calls = self.bake(FAKE_LISTED="[2, 3]", FAKE_CLAIMED_GENERATION="3")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [publish] = self.published(calls)
        self.assertEqual(publish["env"]["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_TARGET_GENERATION"], "3")
        for call in self.exports(calls):
            self.assertEqual(call["env"]["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_FRESH_GENERATION"], "false")

    def test_objects_are_create_only_whatever_the_environment_says(self):
        planted = {f"FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_{name}": "true"
                   for name in ("ALLOW_OVERWRITE", "ALLOW_REPOINT", "FIRST_PUBLICATION",
                                "ROLLBACK_TO_MANIFEST_KEY", "PUBLISH_PATCH")}
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

    def test_the_building_lane_without_a_database_connection_is_refused_before_baking(self):
        env = {key: value for key, value in self.env.items() if key != "DATABASE_URL"}
        result = subprocess.run(["bash", str(self.script), "building"], env=env,
                                capture_output=True, text=True, timeout=120)
        calls = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        self.assertEqual(result.returncode, 78, result.stderr + result.stdout)
        self.assertIn("cannot resolve the runtime database connection", result.stdout)
        self.assertEqual(calls, [])

    def test_all_bakes_parcel_then_building_and_one_failure_does_not_stop_the_other(self):
        result, calls = self.bake("all")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        commands = [call["command"] for call in calls]
        self.assertEqual([command for command in commands if command.startswith("publish-")],
                         ["publish-parcel-by-pnu-serving-manifest", "publish-building-by-pnu-serving-manifest"])
        self.assertLess(max(i for i, c in enumerate(commands) if "parcel" in c),
                        min(i for i, c in enumerate(commands) if "building" in c),
                        "the lanes ran side by side; the memory budget fits one at a time")
        # A parcel lane that fails still lets the building lane bake, and the job reports the failure.
        for path in self.state_root.glob("*/in-progress.json"):
            path.unlink()
        self.log.unlink()
        result, calls = self.bake("all", FAKE_PUBLISHED_GENERATION="3", FAKE_SHORT_PREFIX="9999913",
                                  FAKE_SHORT_LANE="PARCEL")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual([call["command"] for call in self.published(calls)],
                         ["publish-building-by-pnu-serving-manifest"])

    def test_an_unknown_lane_is_refused(self):
        result, calls = self.bake("tiles")
        self.assertEqual(result.returncode, 64)
        self.assertEqual(calls, [])

    def test_work_files_stay_on_the_data_disk(self):
        unit = (job_specs.SYSTEMD / "foundation-by-pnu-serving-bake.service").read_text(encoding="utf-8")
        writable = [line.split("=", 1)[1].split() for line in unit.splitlines() if line.startswith("ReadWritePaths=")]
        self.assertEqual(writable, [["/data/foundation-platform/by-pnu-bake"]])
        self.assertIn("ProtectSystem=strict\n", unit)
        script = (OPS / "by-pnu-serving-bake.sh").read_text(encoding="utf-8")
        self.assertIn('STATE_ROOT="${FOUNDATION_BY_PNU_BAKE_STATE_ROOT:-/data/foundation-platform/by-pnu-bake}', script)

    # The patch, reflect and choice paths (root ADR-0141 §5, §8). The base holds the document schema
    # the export bakes now, so the change set decides.
    def patch_bake(self, unit="parcel", **env):
        return self.bake(unit, FAKE_SERVED_SCHEMA="doc.v2", **env)

    def run_summary(self, mode, unit="parcel"):
        summaries = [json.loads(path.read_text())
                     for path in (self.state_root / unit).glob("runs/202-*/run-summary.json")]
        [summary] = [summary for summary in summaries if summary["mode"] == mode]
        return summary

    def test_a_small_change_set_is_published_as_the_next_patch(self):
        result, calls = self.patch_bake(FAKE_DELTA_UPSERTS=json.dumps(PNUS[:2]), FAKE_DELTA_NEW="1",
                                        FAKE_DELTA_DELETES=json.dumps(["9999920000000000001"]),
                                        FAKE_NEWEST_PATCH="2", FAKE_PATCH_COUNT="2", FAKE_PATCHES_LISTED="[1, 2, 4]")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [delta] = [call for call in calls if call["command"] == "delta"]
        self.assertEqual(delta["options"]["--baseline-snapshot-id"], "101")
        self.assertEqual(delta["options"]["--current-snapshot-id"], "202")
        self.assertEqual(delta["options"]["--max-delta-fraction"], "0.5")
        self.assertEqual((delta["service"], delta["driver_memory"]), ("spark-small", "1500m"))
        prefix = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_"
        # One shard over the whole table: the scan keeps only the change set's rows.
        [export] = self.exports(calls)
        self.assertNotIn(prefix + "PNU_PREFIX", export["env"])
        # Above the newest served patch and every patch directory holding objects.
        self.assertEqual(export["env"][prefix + "TARGET_PATCH"], "5")
        self.assertEqual(export["env"][prefix + "TARGET_GENERATION"], "2")
        self.assertEqual(export["env"][prefix + "FRESH_GENERATION"], "true")
        [publish] = self.published(calls)
        env = publish["env"]
        self.assertEqual(env[prefix + "PUBLISH_PATCH"], "true")
        self.assertEqual((env[prefix + "TARGET_GENERATION"], env[prefix + "TARGET_PATCH"]), ("2", "5"))
        self.assertNotIn(prefix + "PUBLISH_FROM_LISTING", env)
        self.assertEqual(open(env[prefix + "UPSERT_LIST_PATH"]).read().split(), PNUS[:2])
        summary = self.run_summary("patch")
        self.assertEqual((summary["mode"], summary["upserts"], summary["deletes"], summary["target_patch"]),
                         ("patch", 2, 1, 5))
        self.assertEqual((summary["documents"], summary["tombstones"], summary["published"]), (2, 1, "patch"))

    def test_an_empty_change_set_only_advances_the_reflected_snapshot(self):
        result, calls = self.patch_bake()
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(self.exports(calls), [])
        [publish] = self.published(calls)
        prefix = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_"
        self.assertEqual(publish["env"][prefix + "PUBLISH_PATCH"], "true")
        self.assertNotIn(prefix + "TARGET_PATCH", publish["env"])
        self.assertEqual(self.run_summary("reflect")["mode"], "reflect")

    def test_the_patch_limit_sends_the_bake_to_a_full_compaction(self):
        result, calls = self.patch_bake(FAKE_PATCH_COUNT="7", FAKE_NEWEST_PATCH="7")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual([call for call in calls if call["command"] == "delta"], [])
        [publish] = self.published(calls)
        self.assertEqual(publish["env"]["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_PUBLISH_FROM_LISTING"], "true")
        summary = self.run_summary("full")
        self.assertIn("max_patches", summary["reason"])
        self.assertEqual(summary["target_generation"], 3)

    def test_a_lowered_patch_limit_still_sends_the_bake_to_a_full_compaction(self):
        # The base carries more patches than the contract now allows.
        result, calls = self.patch_bake(FAKE_PATCH_COUNT="9", FAKE_NEWEST_PATCH="9")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual([call for call in calls if call["command"] == "delta"], [])
        self.assertIn("max_patches", self.run_summary("full")["reason"])

    def test_a_changed_prefix_length_sends_a_patched_base_to_a_full_compaction(self):
        result, calls = self.patch_bake(FAKE_PATCH_COUNT="1", FAKE_NEWEST_PATCH="1",
                                        FAKE_SERVED_PREFIX_LENGTH="4")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual([call for call in calls if call["command"] == "delta"], [])
        [publish] = self.published(calls)
        self.assertEqual(publish["env"]["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_PUBLISH_FROM_LISTING"], "true")
        self.assertIn("pnu_prefix_length", self.run_summary("full")["reason"])
        # With no patch to carry forward, the next patch is simply listed by the new length.
        for path in self.state_root.glob("parcel/*.json"):
            path.unlink()
        self.log.unlink()
        result, calls = self.patch_bake(FAKE_SERVED_PREFIX_LENGTH="4", FAKE_DELTA_UPSERTS=json.dumps(PNUS[:1]))
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(self.run_summary("patch")["mode"], "patch")

    def test_a_change_set_over_the_cumulative_ratio_goes_to_the_full_path(self):
        # 4 earlier changes + 2 now = 6 of 100 base objects, over the contract's 5%.
        result, calls = self.patch_bake(FAKE_CUMULATIVE="4", FAKE_PATCH_COUNT="1", FAKE_NEWEST_PATCH="1",
                                        FAKE_DELTA_UPSERTS=json.dumps(PNUS[:2]))
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [publish] = self.published(calls)
        self.assertEqual(publish["env"]["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_PUBLISH_FROM_LISTING"], "true")
        self.assertNotIn("FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_TARGET_PATCH", publish["env"])
        summary = self.run_summary("full")
        self.assertIn("max_cumulative_change_ratio", summary["reason"])
        self.assertEqual((summary["upserts"], summary["deletes"]), (2, 0))
        # At the bound it is still a patch: 3 + 2 = 5 of 100.
        for path in self.state_root.glob("parcel/*.json"):
            path.unlink()
        self.log.unlink()
        result, calls = self.patch_bake(FAKE_CUMULATIVE="3", FAKE_PATCH_COUNT="1", FAKE_NEWEST_PATCH="1",
                                        FAKE_DELTA_UPSERTS=json.dumps(PNUS[:2]))
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(self.run_summary("patch")["mode"], "patch")

    def test_a_change_set_over_half_the_table_is_refused_not_baked(self):
        result, calls = self.patch_bake(FAKE_DELTA_EXIT="4")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not a delta; nothing was published", result.stdout)
        self.assertEqual(self.published(calls), [])
        self.assertEqual(self.exports(calls), [])

    def refusal(self):
        return json.loads((self.state_root / "parcel/runs/202-delta/run-summary.json").read_text())["refused"]

    def test_no_comparison_snapshot_is_a_refusal_not_no_change(self):
        result, calls = self.patch_bake(FAKE_DELTA_EXIT="3")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Gold no longer holds the reflected snapshot 101 (expired, or never existed)", result.stdout)
        self.assertIn("FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE", result.stdout)
        self.assertEqual(self.refusal(), "no_comparison_snapshot")
        self.assertEqual(self.published(calls), [])

    def test_a_digestless_comparison_snapshot_is_named_as_such(self):
        # The 2026-10-04 refusal: the snapshot was there, only its row_digest was not.
        result, calls = self.patch_bake(FAKE_DELTA_EXIT="5")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("the reflected snapshot 101 has rows without row_digest", result.stdout)
        self.assertNotIn("expired", result.stdout)
        self.assertEqual(self.refusal(), "comparison_snapshot_has_no_row_digest")
        self.assertEqual(self.published(calls), [])

    # The verified re-base (root ADR-0146 §1).
    REBASE = {"FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE": "true",
              "FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE_REASON": "the reflected snapshot has no row_digest"}

    def test_a_verified_rebase_needs_a_reason_one_answer_and_the_parcel_lane(self):
        for unit, env, said in (
            ("parcel", {"FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE": "true"},
             "needs FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE_REASON"),
            ("parcel", {**self.REBASE, "FOUNDATION_BY_PNU_BAKE_FORCE_FULL": "true",
                        "FOUNDATION_BY_PNU_BAKE_FORCE_FULL_REASON": "x"}, "state one"),
            ("building", self.REBASE, "the building lane re-bases with a full bake"),
            ("parcel", {"FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE": "yes"}, "must be true or false"),
        ):
            with self.subTest(unit=unit, said=said):
                result, calls = self.patch_bake(unit, **env)
                self.assertEqual(result.returncode, 64, result.stdout)
                self.assertIn(said, result.stdout)
                self.assertEqual(calls, [])

    def test_a_verified_rebase_that_finds_nothing_changed_only_reflects(self):
        result, calls = self.patch_bake(**self.REBASE)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertNotIn("delta", [call["command"] for call in calls], "the change-set job ran beside the re-base")
        self.assertEqual(self.exports(calls), [])
        [verify] = [call for call in calls if call["command"] == "verify-parcel-by-pnu-serving-rebase"]
        prefix = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_"
        self.assertEqual(verify["env"][prefix + "EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID"], "202")
        self.assertEqual(verify["env"][prefix + "REBASE_REASON"], "the reflected snapshot has no row_digest")
        [publish] = self.published(calls)
        self.assertEqual(publish["env"][prefix + "PUBLISH_PATCH"], "true")
        self.assertNotIn(prefix + "TARGET_PATCH", publish["env"])
        change = json.loads(open(publish["env"][prefix + "CHANGE_SET_SUMMARY_PATH"]).read())
        self.assertEqual(change["job_name"], "by_pnu_serving_rebase_verify")
        summary = self.run_summary("reflect")
        self.assertEqual(summary["verified_rebase_run_id"], verify["env"][prefix + "REBASE_RUN_ID"])
        self.assertEqual((summary["rebase_served_objects_read"], summary["rebase_equal"]), (100, 100))
        self.assertIn("found every one equal", summary["reason"])

    def test_a_verified_rebase_that_finds_a_changed_document_writes_a_patch(self):
        result, calls = self.patch_bake(FAKE_REBASE_UPSERTS=json.dumps(PNUS[:2]), FAKE_REBASE_NEW="1",
                                        FAKE_REBASE_DELETES=json.dumps(["9999920000000000001"]), **self.REBASE)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        prefix = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_"
        [export] = self.exports(calls)
        self.assertEqual(export["env"][prefix + "TARGET_PATCH"], "1")
        [publish] = self.published(calls)
        self.assertEqual(publish["env"][prefix + "TARGET_PATCH"], "1")
        self.assertEqual(open(publish["env"][prefix + "UPSERT_LIST_PATH"]).read().split(), PNUS[:2])
        summary = self.run_summary("patch")
        self.assertEqual((summary["upserts"], summary["deletes"], summary["rebase_changed"],
                          summary["rebase_only_gold"], summary["rebase_only_served"]), (2, 1, 1, 1, 1))

    def test_an_incomplete_verified_rebase_publishes_nothing_and_the_rerun_resumes_it(self):
        result, calls = self.patch_bake(FAKE_REBASE_FAIL="served object v1/x.json could not be read in 3 attempts",
                                        **self.REBASE)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("the verified re-base did not complete", result.stdout)
        self.assertEqual(self.published(calls), [])
        refused = json.loads((self.state_root / "parcel/runs/202-rebase/run-summary.json").read_text())
        self.assertEqual(refused["refused"], "verified_rebase_incomplete")
        first_id = refused["verified_rebase_run_id"]
        self.log.unlink()
        result, calls = self.patch_bake(**self.REBASE)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [verify] = [call for call in calls if call["command"].startswith("verify-")]
        self.assertEqual(verify["env"]["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_REBASE_RUN_ID"], first_id,
                         "the rerun started another re-base instead of resuming its own")

    def test_a_rebase_left_over_another_served_state_is_kept_aside_and_a_new_one_starts(self):
        result, _ = self.patch_bake(FAKE_REBASE_FAIL="served object v1/x.json could not be read in 3 attempts",
                                    **self.REBASE)
        self.assertNotEqual(result.returncode, 0)
        work = self.state_root / "parcel/runs/202-rebase"
        first_id = (work / "run-id").read_text().strip()
        # The manifest moved since (a patch published, or a rollback): the old work can never resume.
        self.log.unlink()
        moved = dict(FAKE_NEWEST_PATCH="1", FAKE_PATCH_COUNT="1", FAKE_PATCHES_LISTED="[1]")
        result, calls = self.patch_bake(**moved, **self.REBASE)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("compared another served state", result.stdout)
        [verify] = [call for call in calls if call["command"].startswith("verify-")]
        self.assertNotEqual(verify["env"]["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_REBASE_RUN_ID"], first_id)
        [kept] = (self.state_root / "parcel/superseded").iterdir()
        self.assertEqual((kept / "run-id").read_text().strip(), first_id, "the old work was not kept")
        # The same served state again resumes the new run, and moves nothing aside.
        self.log.unlink()
        result, _ = self.patch_bake(**moved, **self.REBASE)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertNotIn("compared another served state", result.stdout)
        self.assertEqual(len(list((self.state_root / "parcel/superseded").iterdir())), 1)

    def test_one_run_of_a_lane_at_a_time(self):
        import fcntl
        lock = self.state_root / "parcel/lane.lock"
        lock.parent.mkdir(parents=True)
        with open(lock, "a") as held:
            fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result, calls = self.patch_bake()
        self.assertEqual(result.returncode, 75, result.stdout)
        self.assertIn("another run of the parcel lane", result.stdout)
        self.assertEqual(calls, [], "a second run of the lane went ahead")
        result, calls = self.patch_bake()
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)

    def test_a_verified_rebase_over_half_the_table_is_not_a_delta(self):
        result, calls = self.patch_bake(FAKE_REBASE_FAIL="900 of 1000 PNUs differ: not a delta", **self.REBASE)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not a delta; nothing was published", result.stdout)
        self.assertEqual(self.published(calls), [])

    def test_the_registered_job_passes_a_verified_rebase_to_the_parcel_lane_only(self):
        result, calls = self.patch_bake("all", **self.REBASE)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        commands = [call["command"] for call in calls]
        self.assertIn("verify-parcel-by-pnu-serving-rebase", commands)
        self.assertNotIn("verify-building-by-pnu-serving-rebase", commands)
        self.assertEqual([command for command in commands if command.startswith("publish-")],
                         ["publish-parcel-by-pnu-serving-manifest", "publish-building-by-pnu-serving-manifest"])

    def test_a_verified_rebase_cannot_stand_in_for_a_needed_full_bake(self):
        # The base holds another document schema (the default fixture): only a full bake serves it.
        result, calls = self.bake(**self.REBASE)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("a verified re-base cannot stand in for it", result.stdout)
        self.assertEqual([call["command"] for call in calls], ["show-parcel-by-pnu-serving-state"])

    def test_a_patch_missing_a_tombstone_is_not_published(self):
        result, calls = self.patch_bake(FAKE_DELTA_UPSERTS=json.dumps(PNUS[:1]),
                                        FAKE_DELTA_DELETES=json.dumps(["9999920000000000001"]),
                                        FAKE_LOSE_TOMBSTONE="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("incomplete patch: shards wrote 1 of 1 upserts and 0 of 1 tombstones", result.stderr)
        self.assertEqual(self.published(calls), [])

    def test_a_forced_full_bake_needs_a_reason_and_records_it(self):
        result, calls = self.patch_bake(FOUNDATION_BY_PNU_BAKE_FORCE_FULL="true")
        self.assertEqual(result.returncode, 64)
        self.assertIn("needs FOUNDATION_BY_PNU_BAKE_FORCE_FULL_REASON", result.stdout)
        self.assertEqual(calls, [])
        result, calls = self.patch_bake(FOUNDATION_BY_PNU_BAKE_FORCE_FULL="true",
                                        FOUNDATION_BY_PNU_BAKE_FORCE_FULL_REASON="re-base after a fingerprint fix")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual([call for call in calls if call["command"] == "delta"], [])
        self.assertEqual(self.run_summary("full")["forced_full_reason"], "re-base after a fingerprint fix")

    def test_a_half_written_patch_of_the_same_snapshot_is_resumed(self):
        changes = dict(FAKE_DELTA_UPSERTS=json.dumps(PNUS[:2]), FAKE_NEWEST_PATCH="1", FAKE_PATCH_COUNT="1")
        result, _ = self.patch_bake(FAKE_CRASH_ONCE_PREFIX="", FOUNDATION_BY_PNU_BAKE_ATTEMPTS="1", **changes)
        self.assertNotEqual(result.returncode, 0)
        record = json.loads((self.state_root / "parcel/in-progress.json").read_text())
        self.assertEqual((record["mode"], record["target"]), ("patch", 2))
        self.log.unlink()
        result, calls = self.patch_bake(FAKE_PATCHES_LISTED="[1, 2]", **changes)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [export] = self.exports(calls)
        self.assertEqual(export["env"]["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_TARGET_PATCH"], "2")
        self.assertEqual(export["env"]["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_FRESH_GENERATION"], "false")


if __name__ == "__main__":
    unittest.main()
