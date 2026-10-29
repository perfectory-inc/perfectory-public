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
`SectionPackBake` drives the building lane once its manifest names section packs (root ADR-0147):
the same choices, made against the packs, with the pack export and publish commands.
"""

import hashlib
import json
import os
import pathlib
import shutil
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
if os.environ.get("FAKE_EXIT_COMMAND") == command:
    sys.exit(int(os.environ["FAKE_EXIT_CODE"]))
# The lane a command names (`export-unit-price-by-pnu-section-packs`), as the publisher derives it.
import re
named = re.match(r"^[a-z]+-([a-z-]+?)-(?:by-pnu|superseded)", command)
lane = (named.group(1) if named else "parcel" if "parcel" in command else "building").upper().replace("-", "_")
prefix_env = f"FOUNDATION_PLATFORM_{lane}_BY_PNU_SERVING_"
def expired(pinned, gone="FAKE_EXPIRED"):
    # Like the catalog, a pinned snapshot expiry removed is refused in these words (root ADR-0310).
    if pinned and pinned == os.environ.get(gone):
        sys.exit(f"Error: failed to resolve the gold.panel snapshot to bake\n\nCaused by:\n    gold.panel metadata no longer holds snapshot {pinned}")
def schema(prefix):
    # The document schema the export bakes; FAKE_SCHEMA_PREFIX bakes another one for one shard.
    if os.environ.get("FAKE_SCHEMA_PREFIX") == prefix:
        return "doc.v9"
    return os.environ.get("FAKE_EXPORT_SCHEMA", "doc.v2")
if command.startswith("show-") and os.environ.get("FAKE_NOT_CUT_OVER") == command:
    # A pack-only lane before its first publish (root ADR-0190) says only that.
    with open(os.environ["FOUNDATION_PLATFORM_BY_PNU_SERVING_STATE_PATH"], "w") as out:
        json.dump({"schema_version": "foundation-platform.by_pnu_serving_state.v2",
                   "unit": command.removeprefix("show-").removesuffix("-serving-state"), "cut_over": False}, out)
elif command.startswith("show-"):
    with open(os.environ["FOUNDATION_PLATFORM_BY_PNU_SERVING_STATE_PATH"], "w") as out:
        json.dump({"schema_version": "foundation-platform.by_pnu_serving_state.v2",
                   "gold_iceberg_snapshot_id": os.environ["FAKE_GOLD"] or None,
                   "document_schema_version": "doc.v2",
                   "readable_document_schema_versions": json.loads(os.environ.get("FAKE_READABLE", '["doc.v2"]')),
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
                   "section_packs": json.loads(os.environ["FAKE_PACKS"]) if os.environ.get("FAKE_PACKS") else None,
                   "policy": {"max_patches": 7, "max_cumulative_change_ratio": 0.05,
                              "max_delta_fraction": 0.5, "pnu_prefix_length": 5}}, out)
elif command.startswith("export-") and command.endswith("-section-packs"):
    # The pack export: a base names its generation, a patch its change set and no generation.
    prefix = os.environ.get(prefix_env + "PNU_PREFIX", "")
    generation = os.environ.get(prefix_env + "PACK_GENERATION")
    patch = os.environ.get(prefix_env + "TARGET_PATCH")
    if (generation is None) == (patch is None) or os.environ.get(prefix_env + "CONFIRM_PACK_EXPORT") != "true":
        sys.exit("Error: a base names its generation; a patch names none")
    def listed(name):
        path = os.environ.get(prefix_env + name)
        return None if path is None else open(path).read().split()
    allow, deletes = listed("PNU_ALLOWLIST_PATH"), listed("DELETE_LIST_PATH") or []
    expected = os.environ.get(prefix_env + "EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID")
    expired(expected)
    if os.environ.get("FAKE_FOREIGN_PACKS"):
        sys.exit(f"Error: serving/buildings/packs/floors/g2/p{patch}/9999910000.pack holds packs of Gold snapshot 909 but this run is of {expected}; a new snapshot goes into a new generation or patch")
    pnus = json.loads(os.environ["FAKE_PNUS"])
    kept = [pnu for pnu in pnus if pnu.startswith(prefix) and (allow is None or pnu in allow)]
    tombstones = [pnu for pnu in deletes if pnu.startswith(prefix)]
    if len(kept) > int(os.environ["FAKE_CAP"]):
        sys.exit(f"Error: this shard keeps more than {os.environ['FAKE_CAP']} rows; shard the run with {prefix_env}PNU_PREFIX, not a bigger heap")
    short = os.environ.get("FAKE_SHORT_PREFIX") == prefix
    lost_tombstone = 1 if tombstones and os.environ.get("FAKE_LOSE_TOMBSTONE") else 0
    with open(os.environ[prefix_env + "PACK_SUMMARY_PATH"], "w") as out:
        json.dump({"schema_version": "foundation-platform.building_by_pnu_section_pack_export_summary.v1",
                   "gold_iceberg_snapshot_id": expected or os.environ["FAKE_GOLD"],
                   "document_schema_version": schema(prefix),
                   "generation": int(generation) if generation else 1,
                   "section_generations": {"buildings": 1, "floors": 2} if patch else {},
                   "patch": int(patch) if patch else None, "pnu_prefix": prefix or None,
                   "gold_record_count": len(pnus), "exported_row_count": len(kept) - (1 if short else 0),
                   "tombstone_count": len(tombstones) - lost_tombstone,
                   "read": {"data_files": 4, "skipped_data_files": 0, "bytes_read": 4096},
                   "withheld": [{"key": pnu, "reason": "document_too_large: planted"} for pnu in kept
                                if pnu in json.loads(os.environ.get("FAKE_WITHHELD", "[]"))],
                   "packs": [{"unit": pnu[:10], "bytes": 100} for pnu in kept + tombstones]}, out)
elif command.startswith("export-"):
    prefix = os.environ.get(prefix_env + "PNU_PREFIX", "")
    target = int(os.environ[prefix_env + "TARGET_GENERATION"])
    patch = os.environ.get(prefix_env + "TARGET_PATCH")
    def listed(name):
        path = os.environ.get(prefix_env + name)
        return None if path is None else open(path).read().split()
    allow, deletes = listed("PNU_ALLOWLIST_PATH"), listed("DELETE_LIST_PATH") or []
    # A broken export that wrote a summary of another snapshot (the completeness check's defence).
    foreign = os.environ.get("FAKE_FOREIGN_SNAPSHOT") if os.environ.get("FAKE_FOREIGN_PREFIX") == prefix else None
    expected = os.environ.get(prefix_env + "EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID")
    expired(expected)
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
    with open(os.environ[prefix_env + "SUMMARY_PATH"], "w") as out:
        json.dump({"gold_iceberg_snapshot_id": foreign or expected or os.environ["FAKE_GOLD"],
                   "document_schema_version": schema(prefix),
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
    expired(os.environ.get(prefix_env + "EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID"), "FAKE_PUBLISH_EXPIRED")
elif command.startswith("retire-"):
    if os.environ.get("FAKE_RETIRE_FAIL") == command:
        sys.exit("a delete failed; rerun to finish the retirement")
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
        for name in ("by-pnu-serving-bake.sh", "admitted-writer-runtime.sh", "by-pnu-bake-shards.sh", "held-lock.sh"):
            (release / "scripts/ops" / name).write_bytes((OPS / name).read_bytes())
            (release / "scripts/ops" / name).chmod(0o755)
        # A lane's derived fields (the lane it follows) are read through it (root ADR-0193 §9).
        (release / "infra/lakehouse/spark/jobs").mkdir(parents=True)
        (release / "infra/lakehouse/spark/jobs/by_pnu_kinds.py").write_bytes(
            (job_specs.PLATFORM_ROOT / "infra/lakehouse/spark/jobs/by_pnu_kinds.py").read_bytes())
        # Which job bakes a lane (`all` leaves a lane another job bakes to it) is read from the job
        # list and the graph (root ADR-0303, ADR-0333).
        for relative in ("orchestration/dags/job_specs.py", "orchestration/jobs.v1.json",
                         "docs/catalog/pipeline-graph.v1.json", "infra/datahub/dataset_names.py",
                         "scripts/ops/lane_follow_gate.py", "scripts/ops/iceberg_metadata.py"):
            (release / relative).parent.mkdir(parents=True, exist_ok=True)
            (release / relative).write_bytes((job_specs.PLATFORM_ROOT / relative).read_bytes())
        # The follow gate reads the Gold rebuild contract through the planner (lane_follow_gate.py).
        for part in ("infra/lakehouse/contracts", "infra/lakehouse/spark/jobs"):
            shutil.copytree(job_specs.PLATFORM_ROOT / part, release / part, dirs_exist_ok=True,
                            ignore=shutil.ignore_patterns("__pycache__", "tests"))
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
        # The registered job (both lanes): nothing published, so the run says it changed nothing.
        result, _ = self.bake("all", FAKE_GOLD="101")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(result.stdout.count("nothing to do"), 2, result.stdout)
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome unchanged")

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

    P = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_"

    def stopped_half_way(self):
        """A full bake of 202 into generation 3 that a deploy stopped after some shards (ADR-0301):
        the shard summaries written, the target recorded. Returns the shards already baked."""
        result, _ = self.bake(FAKE_CRASH_ONCE_PREFIX="9999917", FOUNDATION_BY_PNU_BAKE_ATTEMPTS="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(json.loads((self.state_root / "parcel/in-progress.json").read_text()),
                         {"gold_iceberg_snapshot_id": "202", "mode": "full", "base_generation": 2, "target": 3})
        done = {path.name[len("shard-"):-len(".json")]
                for path in (self.state_root / "parcel/runs/202-g3").glob("shard-*.json")}
        self.assertTrue(done)
        self.log.unlink()
        return done

    def test_a_full_bake_gold_moved_under_finishes_at_its_pin_then_patches_forward(self):
        # Root ADR-0310. 2026-10-24: the parcel lane's generation 5 was thrown away after 15 h
        # because Gold moved mid-bake, and generation 6 started from zero.
        done = self.stopped_half_way()
        # Gold has moved on to 404. The next run finishes generation 3 at 202, the snapshot it pinned:
        # no change set, no new generation, no finished shard baked again.
        result, calls = self.bake(FAKE_GOLD="404", FAKE_LISTED="[2, 3]", FAKE_CLAIMED_GENERATION="3")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("finishing the full bake pinned to Gold snapshot 202 (Gold is now at 404)", result.stdout)
        self.assertEqual([call for call in calls if call["command"] == "delta"], [])
        exports = self.exports(calls)
        self.assertTrue(exports)
        self.assertFalse({call["env"].get(self.P + "PNU_PREFIX") for call in exports} & done)
        for call in exports:
            self.assertEqual(call["env"][self.P + "EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID"], "202")
            self.assertEqual(call["env"][self.P + "TARGET_GENERATION"], "3")
            self.assertEqual(call["env"][self.P + "FRESH_GENERATION"], "false")
        [publish] = self.published(calls)
        self.assertEqual(publish["env"][self.P + "TARGET_GENERATION"], "3")
        self.assertEqual(publish["env"][self.P + "EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID"], "202")
        summary = json.loads((self.state_root / "parcel/runs/202-g3/run-summary.json").read_text())
        self.assertEqual((summary["gold_iceberg_snapshot_id"], summary["gold_now_iceberg_snapshot_id"],
                          summary["published"]), ("202", "404", "full"))
        self.assertFalse((self.state_root / "parcel/in-progress.json").exists())
        # The run after it patches generation 3 forward from 202 to 404 (root ADR-0274).
        self.log.unlink()
        result, calls = self.bake(FAKE_GOLD="404", FAKE_PUBLISHED_GENERATION="3", FAKE_PUBLISHED_SNAPSHOT="202",
                                  FAKE_SERVED_SCHEMA="doc.v2", FAKE_LISTED="[2, 3]",
                                  FAKE_DELTA_UPSERTS=json.dumps(PNUS[:1]))
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [delta] = [call for call in calls if call["command"] == "delta"]
        self.assertEqual((delta["options"]["--baseline-snapshot-id"], delta["options"]["--current-snapshot-id"]),
                         ("202", "404"))
        [publish] = self.published(calls)
        self.assertEqual((publish["env"][self.P + "TARGET_GENERATION"], publish["env"][self.P + "TARGET_PATCH"],
                          publish["env"][self.P + "EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID"]), ("3", "1", "404"))

    def test_a_pinned_bake_of_another_document_schema_is_not_finished(self):
        # One generation is one schema: shards baked by a release with another schema are left
        # unserved, and the new full bake goes above them, of the current snapshot.
        self.bake(FAKE_CRASH_ONCE_PREFIX="9999917", FOUNDATION_BY_PNU_BAKE_ATTEMPTS="1", FAKE_EXPORT_SCHEMA="doc.v1")
        self.log.unlink()
        result, calls = self.bake(FAKE_GOLD="404", FAKE_LISTED="[2, 3]")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [publish] = self.published(calls)
        self.assertEqual((publish["env"][self.P + "TARGET_GENERATION"],
                          publish["env"][self.P + "EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID"]), ("4", "404"))

    def test_an_expired_pin_abandons_its_generation_and_the_next_run_starts_over(self):
        # Expiry removed 202 while generation 3 waited: the export refuses it, nothing of the
        # current snapshot is mixed in, and the record goes (no retry can bring 202 back).
        self.stopped_half_way()
        result, calls = self.bake(FAKE_GOLD="404", FAKE_LISTED="[2, 3]", FAKE_EXPIRED="202")
        self.assertEqual(result.returncode, 1, result.stderr + result.stdout)
        self.assertIn("Gold no longer holds snapshot 202, which generation 3 is pinned to (expired)", result.stdout)
        self.assertIn("abandoned: generation 3 holds what this bake wrote for Gold snapshot 202", result.stdout)
        self.assertEqual(len(self.exports(calls)), 1)
        self.assertEqual(self.published(calls), [])
        self.assertFalse((self.state_root / "parcel/in-progress.json").exists())
        self.log.unlink()
        result, calls = self.bake(FAKE_GOLD="404", FAKE_LISTED="[2, 3]")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [publish] = self.published(calls)
        self.assertEqual((publish["env"][self.P + "TARGET_GENERATION"],
                          publish["env"][self.P + "EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID"]), ("4", "404"))

    def test_a_publish_whose_pin_expired_abandons_its_generation(self):
        result, calls = self.bake(FAKE_PUBLISH_EXPIRED="202")
        # The exports read 202 before it expired; the publish finds it gone.
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("abandoned: generation 3 holds what this bake wrote for Gold snapshot 202", result.stdout)
        self.assertFalse((self.state_root / "parcel/in-progress.json").exists())

    def test_a_shard_summary_of_another_snapshot_is_still_not_published(self):
        # Defence behind the export's pin: the summaries are compared before publishing.
        result, calls = self.bake(FAKE_FOREIGN_PREFIX="9999915", FAKE_FOREIGN_SNAPSHOT="303")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("a bake of two snapshots is never published", result.stderr)
        self.assertEqual(self.published(calls), [])

    def test_shards_of_two_document_schemas_are_not_published(self):
        result, calls = self.bake(FAKE_SCHEMA_PREFIX="9999915")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("shards baked different document schemas", result.stderr)
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
        # The first shard fails before its check: nothing is recorded to resume.
        result, calls = self.bake(FAKE_CRASH_BEFORE_CHECK_ONCE_PREFIX="1", FOUNDATION_BY_PNU_BAKE_ATTEMPTS="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(self.exports(calls)), 1)
        self.assertFalse((self.state_root / "parcel/in-progress.json").exists())
        # Once a shard has passed its check, the generation is this lane's.
        self.log.unlink()
        result, _ = self.bake(FAKE_CRASH_ONCE_PREFIX="2", FOUNDATION_BY_PNU_BAKE_ATTEMPTS="1", FAKE_GOLD="404")
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

    def test_a_snapshot_change_after_a_refused_bake_starts_a_new_generation(self):
        # Every shard baked and the bake was refused as incomplete: resuming it at its pin would
        # change nothing (root ADR-0310), so once Gold moves the next full bake goes above it.
        self.bake(FAKE_SHORT_PREFIX="9999913")
        self.assertTrue(json.loads((self.state_root / "parcel/in-progress.json").read_text())["refused_incomplete"])
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
        # The bakes, apart from the retirement that follows them all (root ADR-0242).
        commands = [call["command"] for call in calls if not call["command"].startswith("retire-")]
        self.assertEqual([command for command in commands if command.startswith("publish-")],
                         ["publish-parcel-by-pnu-serving-manifest", "publish-building-by-pnu-serving-manifest"])
        self.assertLess(max(i for i, c in enumerate(commands) if "parcel" in c),
                        min(i for i, c in enumerate(commands) if "building" in c),
                        "the lanes ran side by side; the memory budget fits one at a time")
        # A lane published: the job's last line says so (root ADR-0171).
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome changed")
        # A parcel lane that fails still lets the building lane bake, and the job reports the failure.
        for path in self.state_root.glob("*/in-progress.json"):
            path.unlink()
        self.log.unlink()
        result, calls = self.bake("all", FAKE_PUBLISHED_GENERATION="3", FAKE_SHORT_PREFIX="9999913",
                                  FAKE_SHORT_LANE="PARCEL")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual([call["command"] for call in self.published(calls)],
                         ["publish-building-by-pnu-serving-manifest"])
        # 2026-10-16: this run said `changed` and exited 1. The last word now matches the exit.
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome failed parcel; published: building")
        self.assertEqual(job_specs.job_outcome(result.stdout), "failed")
        # `all` decides the run's outcome alone: its lanes add no line of their own.
        self.assertEqual(result.stdout.count("foundation-job-outcome"), 1, result.stdout)

    def test_a_lane_run_on_its_own_ends_with_one_outcome_line(self):
        # Root ADR-0260: a run without the line is failed, whichever way the lane ended.
        result, _ = self.bake(FAKE_GOLD="101")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome unchanged")
        self.log.unlink()
        result, calls = self.bake()
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(len(self.published(calls)), 1)
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome changed")
        self.assertEqual(result.stdout.count("foundation-job-outcome"), 1, result.stdout)
        self.log.unlink()
        result, _ = self.bake(FAKE_GOLD="404", FAKE_CAP="0")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout.splitlines()[-1],
                         f"foundation-job-outcome failed the parcel lane exited {result.returncode}")
        self.assertEqual(job_specs.job_outcome(result.stdout), "failed")
        # A lane another run holds waits, and says so once (held-lock.sh).
        import fcntl
        with open(self.state_root / "parcel/lane.lock", "a") as held:
            fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result, _ = self.bake(FOUNDATION_JOB_SERVICE="1")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome waiting the parcel lane is held by another run")
        self.assertEqual(result.stdout.count("foundation-job-outcome"), 1, result.stdout)

    def test_every_registered_job_of_this_script_ends_with_one_outcome_line(self):
        # Root ADR-0260: a run without the line is failed. by_pnu_serving_bake runs `all`, and the
        # own-producer public-auction bake one lane as its whole program (root ADR-0303), which must
        # end with the line itself, exactly once.
        shutil.copytree(job_specs.PLATFORM_ROOT / "config", self.script.parents[2] / "config")
        programs = {}
        for spec in job_specs.load_specs():
            unit = job_specs.service_unit_file(spec.systemd_service).read_text(encoding="utf-8")
            for line in unit.splitlines():
                if line.startswith("ExecStart=") and "/scripts/ops/by-pnu-serving-bake.sh " in line:
                    programs[spec.job_id] = line.split("by-pnu-serving-bake.sh ", 1)[1].split()
        self.assertEqual(programs.get("by_pnu_public_auction_bake"), ["parcel-public-auction"])
        self.assertEqual(programs.get("by_pnu_unit_price_bake"), ["unit-price"])
        for job_id, args in programs.items():
            with self.subTest(job_id):
                self.log.unlink(missing_ok=True)
                result, _ = self.bake(*args, FAKE_GOLD="101")
                self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
                self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome unchanged")
                self.assertEqual(result.stdout.count("foundation-job-outcome"), 1, result.stdout)

    def test_only_a_pack_only_lane_that_says_it_is_not_cut_over_waits_for_the_operator(self):
        # Root ADR-0190. The release ships the contract that names the pack-only lanes.
        shutil.copytree(job_specs.PLATFORM_ROOT / "config", self.script.parents[2] / "config")
        result, calls = self.bake("all", FAKE_GOLD="101", FAKE_NOT_CUT_OVER="show-parcel-zoning-by-pnu-serving-state")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        # Every lane of the contract in its order; a kind line (root ADR-0193 §4) is baked like the
        # others, EXCEPT a lane another job bakes: the own-producer kind (root ADR-0303) and the unit
        # price lane (root ADR-0333, one job per asset), which `all` skips. Their superseded
        # generations are still retired below, like every lane's.
        lanes = json.loads((job_specs.PLATFORM_ROOT / "config/r2-connections.contract.json").read_text(encoding="utf-8"))["by_pnu_lanes"]
        elsewhere = {lane: job_specs.writer_of(f"{lane}-by-pnu-section-packs") for lane in lanes}
        elsewhere = {lane: job for lane, job in elsewhere.items() if job != "by_pnu_serving_bake"}
        self.assertEqual(elsewhere, {"parcel-public-auction": "by_pnu_public_auction_bake",
                                     "unit-price": "by_pnu_unit_price_bake"})
        baked = [lane for lane in lanes if lane not in elsewhere]
        self.assertEqual([call["command"] for call in calls if call["command"].startswith("show-")],
                         ["show-parcel-by-pnu-serving-state", "show-building-by-pnu-serving-state",
                          *(f"show-{lane}-by-pnu-serving-state" for lane in baked)])
        skipped = [line for line in result.stdout.splitlines() if "bakes in its own job" in line]
        self.assertEqual(skipped, [f"by-pnu-serving-bake {lane}: skipped: the lane bakes in its own job {job} "
                                   "(root ADR-0303, ADR-0333)" for lane, job in elsewhere.items()])
        # Then every lane retires its superseded pack generations past the window (root ADR-0242).
        retired = [call for call in calls if call["command"].startswith("retire-")]
        self.assertEqual([call["command"] for call in retired],
                         ["retire-parcel-superseded-pack-generations", "retire-building-superseded-pack-generations",
                          *(f"retire-{lane}-superseded-pack-generations" for lane in lanes)])
        for call, lane in zip(retired, ["parcel", "building", *lanes]):
            prefix = "FOUNDATION_PLATFORM_" + lane.upper().replace("-", "_") + "_BY_PNU_SERVING_"
            self.assertEqual(call["env"].get(prefix + "PACK_RETIRE_EXECUTE"), "true", call)
            self.assertEqual(call["env"].get(prefix + "OUTPUT_STORAGE_DRIVER"), "r2", call)
        waiting = [line for line in result.stdout.splitlines() if "is not cut over yet" in line]
        self.assertEqual(waiting, ["by-pnu-serving-bake parcel-zoning: nothing to do: the pack-only parcel-zoning lane is "
                                   "not cut over yet; its first publish is the operator's pack-only gate (root ADR-0190)"])
        self.assertEqual(result.stdout.count("already reflects Gold snapshot 101"), 1 + len(baked), result.stdout)
        # Each lane that serves a Gold snapshot records which one it read (root ADR-0333 §7).
        self.assertEqual(json.loads((self.state_root / "building/reads.jsonl").read_text().splitlines()[-1])["read_snapshot_id"],
                         "101")
        # Once cut over its state is a served lane's, and it is baked like the others.
        self.log.unlink()
        result, calls = self.bake("unit-price", FAKE_GOLD="101")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertNotIn("not cut over", result.stdout)
        self.assertIn("already reflects Gold snapshot 101", result.stdout)
        self.assertIn("foundation-asset-read unit-price gold=101", result.stdout)
        # The own-producer lane runs as a single lane (its dedicated job, root ADR-0303); only `all`
        # skips it. Called directly it reads its state and bakes like any other.
        self.log.unlink()
        result, calls = self.bake("parcel-public-auction", FAKE_GOLD="101")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertNotIn("an own-producer lane bakes in its own job", result.stdout)
        self.assertIn("show-parcel-public-auction-by-pnu-serving-state", [call["command"] for call in calls])
        self.assertIn("already reflects Gold snapshot 101", result.stdout)

    def test_a_failed_retirement_fails_the_job_and_still_retires_the_other_lanes(self):
        # Root ADR-0242: a lane whose superseded generations could not be deleted is retried by the
        # next run, and the job says it failed instead of leaving a second copy unseen.
        result, calls = self.bake("all", FAKE_GOLD="101", FAKE_RETIRE_FAIL="retire-parcel-superseded-pack-generations")
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("by-pnu-serving-bake parcel: retiring superseded pack generations failed", result.stdout)
        self.assertIn("retire-building-superseded-pack-generations", [call["command"] for call in calls])

    def test_a_lane_whose_documents_follow_the_parcel_lane_waits_when_that_lane_failed(self):
        # Root ADR-0193 §9: a kind's documents are its host's section; a run whose parcel lane
        # failed does not publish them ahead of it.
        shutil.copytree(job_specs.PLATFORM_ROOT / "config", self.script.parents[2] / "config")
        result, calls = self.bake("all", FAKE_SHORT_PREFIX="9999913", FAKE_SHORT_LANE="PARCEL",
                                  FAKE_NOT_CUT_OVER="show-parcel-zoning-by-pnu-serving-state")
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("show-parcel-zoning-by-pnu-serving-state", [call["command"] for call in calls])
        self.assertIn("by-pnu-serving-bake parcel-zoning: skipped: the parcel lane it follows failed in this run",
                      result.stdout)

    def test_the_unit_price_lane_bakes_only_what_the_building_lane_serves(self):
        # Root ADR-0333: its own job now, so no run order keeps the unit price documents behind the
        # building documents; the gate (lane_follow_gate.py, its own test) compares the registers both
        # Golds were built from. Waiting is recorded, and the lane is baked once the gate says ok.
        shutil.copytree(job_specs.PLATFORM_ROOT / "config", self.script.parents[2] / "config")
        gate = self.script.parent / "lane_follow_gate.py"
        gate.write_text("import os, sys\n"
                        "print('building' if sys.argv[2:] == ['--follows'] else os.environ['FAKE_GATE'])\n")
        result, calls = self.bake("unit-price", FAKE_GATE="waiting 101 the building lane serves another register build")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("show-building-by-pnu-serving-state", [call["command"] for call in calls])
        self.assertEqual(self.published(calls), [])
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome waiting the building lane it follows "
                         "does not serve what Gold snapshot 202 was built from")
        self.assertEqual(job_specs.job_outcome(result.stdout), "waiting")
        [record] = [json.loads(line) for line in (self.state_root / "unit-price/reads.jsonl").read_text().splitlines()]
        self.assertEqual({key: record[key] for key in ("lane", "read_snapshot_id", "outcome", "gold_snapshot_id", "waiting_on")},
                         {"lane": "unit-price", "read_snapshot_id": "101", "outcome": "waiting", "gold_snapshot_id": "202",
                          "waiting_on": {"lane": "building", "read_snapshot_id": "101"}})
        self.log.unlink()
        result, calls = self.bake("unit-price", FAKE_GATE="ok")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(len(self.published(calls)), 1)
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome changed")
        last = json.loads((self.state_root / "unit-price/reads.jsonl").read_text().splitlines()[-1])
        self.assertEqual((last["read_snapshot_id"], last["outcome"]), ("202", "served"))
        # A gate that cannot tell refuses the bake rather than publish ahead of the building documents.
        self.log.unlink()
        gate.write_text("import sys\nsys.exit(0 if sys.argv[2:] == ['--follows'] and print('building') is None else 65)\n")
        result, calls = self.bake("unit-price", FAKE_GOLD="203")
        self.assertEqual(result.returncode, 65, result.stderr + result.stdout)
        self.assertEqual(self.published(calls), [])

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

    def test_a_scheduled_run_waits_for_a_lane_another_run_holds_and_still_fails_on_anything_else(self):
        # 2026-10-19: an operator's attended full parcel bake held the lane; the scheduled run beside it
        # exited 75, showed red and sent a failure notice. Waiting for that run is not a failure.
        import fcntl
        shutil.copytree(job_specs.PLATFORM_ROOT / "config", self.script.parents[2] / "config")
        for lane in ("parcel", "building"):
            with self.subTest(held=lane):
                lock = self.state_root / lane / "lane.lock"
                lock.parent.mkdir(parents=True, exist_ok=True)
                with open(lock, "a") as held:
                    fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self.log.unlink(missing_ok=True)
                    by_hand, _ = self.bake("all", FAKE_GOLD="101")
                    self.log.unlink(missing_ok=True)
                    result, calls = self.bake("all", FAKE_GOLD="101", FOUNDATION_JOB_SERVICE="1")
                # By hand the run stops there (75), so a chain that started it does not go on.
                self.assertEqual(by_hand.returncode, 75, by_hand.stdout)
                self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
                last = result.stdout.splitlines()[-1]
                self.assertTrue(last.startswith(f"foundation-job-outcome waiting the lanes {lane}"), last)
                self.assertTrue(last.endswith(" is held by another run"), last)
                self.assertEqual(job_specs.job_outcome(result.stdout), "waiting")
                self.assertNotIn(f"show-{lane}-by-pnu-serving-state", [call["command"] for call in calls])
                if lane == "parcel":
                    parcel_held, parcel_calls = result, calls
        # A lane whose documents follow the held one waits with it (root ADR-0190, ADR-0193 §9). The
        # unit price lane bakes in its own job (root ADR-0333) and waits on the building lane's state there.
        self.assertIn("by-pnu-serving-bake parcel-zoning: waiting: the parcel lane it follows is held by another run",
                      parcel_held.stdout)
        self.assertNotIn("show-parcel-zoning-by-pnu-serving-state", [call["command"] for call in parcel_calls])        # The same exit from anything but the held lock is still a failure, in the job's own service too.
        self.log.unlink(missing_ok=True)
        result, _ = self.bake("all", FAKE_GOLD="101", FAKE_EXIT_COMMAND="show-parcel-by-pnu-serving-state",
                              FAKE_EXIT_CODE="75", FOUNDATION_JOB_SERVICE="1")
        self.assertEqual(result.returncode, 75, result.stdout)
        self.assertTrue(result.stdout.splitlines()[-1].startswith("foundation-job-outcome failed parcel"), result.stdout)
        self.assertEqual(job_specs.job_outcome(result.stdout), "failed")

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



def packs_state(**overrides):
    """The lane state's section_packs: floors re-baked alone into generation 2 after patch 3."""
    state = {"reflected_gold_iceberg_snapshot_id": "150", "document_schema_version": "doc.v2",
             "document_count": 100, "newest_patch": 3, "max_live_patches": 2, "cumulative_changes": 2,
             "sections": [{"name": "buildings", "generation": 1, "patch_floor": 0},
                          {"name": "floors", "generation": 2, "patch_floor": 3},
                          {"name": "units", "generation": 1, "patch_floor": 0},
                          {"name": "unit_prices", "generation": 1, "patch_floor": 0}],
             "generations_with_packs": [1, 2], "patches_with_packs": [1, 2, 3, 5]}
    return json.dumps({**state, **overrides})


class SectionPackBake(unittest.TestCase):
    """The building lane once its manifest names section packs (root ADR-0147)."""

    setUp = ByPnuServingBake.setUp
    exports = ByPnuServingBake.exports
    published = ByPnuServingBake.published
    PREFIX = "FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_"

    def bake(self, **env):
        # The objects reflect 101 and were baked with another schema: had the bake looked at
        # them, it would have chosen a full object bake against 101.
        return ByPnuServingBake.bake(self, "building", FAKE_PACKS=env.pop("FAKE_PACKS", packs_state()), **env)

    def test_packs_that_reflect_gold_are_nothing_to_do(self):
        result, calls = self.bake(FAKE_GOLD="150")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("served section packs (highest generation 2, 2 patches) already reflect Gold snapshot 150",
                      result.stdout)
        self.assertEqual([call["command"] for call in calls], ["show-building-by-pnu-serving-state"])

    def test_a_change_set_from_the_packs_is_baked_as_a_pack_patch(self):
        result, calls = self.bake(FAKE_DELTA_UPSERTS=json.dumps(PNUS[:2]),
                                  FAKE_DELTA_DELETES=json.dumps(["9999920000000000001"]))
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [delta] = [call for call in calls if call["command"] == "delta"]
        self.assertEqual(delta["options"]["--baseline-snapshot-id"], "150")
        [export] = self.exports(calls)
        env = export["env"]
        self.assertEqual(export["command"], "export-building-by-pnu-section-packs")
        # Above the newest served patch and every patch number holding a pack; no generation:
        # each section's patch goes under the generation it is served from.
        self.assertEqual(env[self.PREFIX + "TARGET_PATCH"], "6")
        for name in ("PACK_GENERATION", "TARGET_GENERATION", "FRESH_GENERATION", "SUMMARY_PATH", "PNU_PREFIX"):
            self.assertNotIn(self.PREFIX + name, env)
        self.assertEqual(open(env[self.PREFIX + "PNU_ALLOWLIST_PATH"]).read().split(), PNUS[:2])
        [publish] = self.published(calls)
        env = publish["env"]
        self.assertEqual(publish["command"], "publish-building-by-pnu-section-packs")
        self.assertEqual(env[self.PREFIX + "CONFIRM_PACK_PUBLISH"], "true")
        self.assertEqual(env[self.PREFIX + "EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID"], "202")
        summaries = pathlib.Path(env[self.PREFIX + "PACK_SUMMARY_DIR"])
        self.assertEqual([path.name for path in summaries.glob("*.json")], ["shard-all.json"])
        self.assertEqual(json.load(open(env[self.PREFIX + "CHANGE_SET_SUMMARY_PATH"]))["input"]["baseline_snapshot_id"],
                         "150")
        self.assertFalse(any(call["command"].endswith("-serving") or call["command"].endswith("-manifest")
                             for call in calls), "an object command ran on a lane serving packs")
        summary = json.loads((summaries.parent / "run-summary.json").read_text())
        self.assertEqual((summary["lane_serves"], summary["mode"], summary["target_patch"]), ("packs", "patch", 6))
        # What the run read, wrote and changed (root ADR-0314 §4): its one shard read all 4 data
        # files, wrote a 100-byte pack per document and tombstone, and changed 3 keys.
        self.assertEqual((summary["data_files"], summary["bytes_read"], summary["shards_reading_every_data_file"],
                          summary["bytes_written"], summary["keys_changed"]), (4, 4096, 1, 300, 3))
        self.assertFalse((self.state_root / "building/in-progress-packs.json").exists())

    def test_an_empty_change_set_publishes_the_packs_reflect_alone(self):
        result, calls = self.bake()
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(self.exports(calls), [])
        [publish] = self.published(calls)
        self.assertEqual(publish["command"], "publish-building-by-pnu-section-packs")
        self.assertNotIn(self.PREFIX + "PACK_SUMMARY_DIR", publish["env"])
        self.assertIn(self.PREFIX + "CHANGE_SET_SUMMARY_PATH", publish["env"])

    def test_a_pack_schema_change_bakes_a_new_generation_of_every_section(self):
        result, calls = self.bake(FAKE_PACKS=packs_state(document_schema_version="doc.v1",
                                                         generations_with_packs=[1, 2, 4]))
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual([call for call in calls if call["command"] == "delta"], [])
        exports = self.exports(calls)
        self.assertTrue(exports)
        for call in exports:
            self.assertEqual(call["command"], "export-building-by-pnu-section-packs")
            self.assertEqual(call["env"][self.PREFIX + "PACK_GENERATION"], "5")
            self.assertNotIn(self.PREFIX + "TARGET_PATCH", call["env"])
        # The shard plan split on the pack export's words, as it does for objects.
        self.assertTrue(any(call["env"].get(self.PREFIX + "PNU_PREFIX") == "9999910" for call in exports))
        [publish] = self.published(calls)
        self.assertEqual(publish["command"], "publish-building-by-pnu-section-packs")
        self.assertNotIn(self.PREFIX + "CHANGE_SET_SUMMARY_PATH", publish["env"])
        self.assertNotIn(self.PREFIX + "PACK_EXPECTED_DOCUMENT_COUNT", publish["env"])

    def test_a_version_that_expands_the_served_ones_is_patched_over_them(self):
        # Root ADR-0320: v2 only adds optional fields to v1, so the v1 documents stay served and
        # the change set alone decides what is re-rendered.
        result, calls = self.bake(FAKE_PACKS=packs_state(document_schema_version="doc.v1",
                                                         served_document_schema_versions=["doc.v1"]),
                                  FAKE_READABLE='["doc.v2", "doc.v1"]',
                                  FAKE_DELTA_UPSERTS=json.dumps(PNUS[:2]))
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("only adds optional fields to the served doc.v1", result.stdout)
        [delta] = [call for call in calls if call["command"] == "delta"]
        self.assertEqual(delta["options"]["--baseline-snapshot-id"], "150")
        [export] = self.exports(calls)
        self.assertEqual(export["env"][self.PREFIX + "TARGET_PATCH"], "6")
        self.assertNotIn(self.PREFIX + "PACK_GENERATION", export["env"])
        [publish] = self.published(calls)
        summary = json.loads((pathlib.Path(publish["env"][self.PREFIX + "PACK_SUMMARY_DIR"]).parent
                              / "run-summary.json").read_text())
        self.assertEqual((summary["mode"], summary["document_schema_expansion"]), ("patch", "doc.v1 -> doc.v2"))

    def test_an_expanding_version_still_compacts_at_the_patch_limit(self):
        result, calls = self.bake(FAKE_PACKS=packs_state(document_schema_version="doc.v1", max_live_patches=7,
                                                         served_document_schema_versions=["doc.v1", "doc.v2"]),
                                  FAKE_READABLE='["doc.v2", "doc.v1"]')
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("full — the base carries 7 patches", result.stdout)
        self.assertEqual([call for call in calls if call["command"] == "delta"], [])

    def test_a_served_version_the_export_does_not_expand_bakes_a_new_generation(self):
        # A patch of v1 sits over a v0 base, and v2 expands only v1: the v0 documents would stay.
        result, calls = self.bake(FAKE_PACKS=packs_state(document_schema_version="doc.v0",
                                                         served_document_schema_versions=["doc.v0", "doc.v1"],
                                                         generations_with_packs=[1, 2, 4]),
                                  FAKE_READABLE='["doc.v2", "doc.v1"]')
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("full — the export bakes doc.v2 documents, the base holds doc.v0,doc.v1", result.stdout)
        self.assertEqual([call for call in calls if call["command"] == "delta"], [])
        for call in self.exports(calls):
            self.assertEqual(call["env"][self.PREFIX + "PACK_GENERATION"], "5")

    def test_an_incomplete_pack_patch_is_not_published_and_the_rerun_resumes_it(self):
        change = {"FAKE_DELTA_UPSERTS": json.dumps(PNUS[:2]),
                  "FAKE_DELTA_DELETES": json.dumps(["9999920000000000001"])}
        result, calls = self.bake(FAKE_LOSE_TOMBSTONE="1", **change)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("incomplete patch: shards wrote 2 of 2 upserts and 0 of 1 tombstones", result.stderr)
        self.assertEqual(self.published(calls), [])
        self.assertEqual(json.loads((self.state_root / "building/in-progress-packs.json").read_text())["target"], 6)
        self.log.unlink()
        for path in (self.state_root / "building/runs").glob("*/summaries/shard-*.json"):
            path.unlink()
        # The bucket now lists patch 6; the rerun of the same snapshot resumes it all the same.
        result, calls = self.bake(FAKE_PACKS=packs_state(patches_with_packs=[1, 2, 3, 5, 6]), **change)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [export] = self.exports(calls)
        self.assertEqual(export["env"][self.PREFIX + "TARGET_PATCH"], "6")

    def test_a_pack_target_holding_another_snapshot_is_never_resumed(self):
        change = {"FAKE_DELTA_UPSERTS": json.dumps(PNUS[:2])}
        result, calls = self.bake(FAKE_FOREIGN_PACKS="1", **change)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("already holds packs of another Gold snapshot", result.stdout)
        self.assertEqual(len(self.exports(calls)), 1, "a refusal a retry cannot change was retried")
        self.assertEqual(self.published(calls), [])
        self.assertFalse((self.state_root / "building/in-progress-packs.json").exists())

    def test_a_withheld_document_is_published_around_and_fails_the_run_after(self):
        # Root ADR-0325 §4. 2026-10-28: one building document past what one request may copy
        # refused the whole lane six times and nothing was published for 585 hours. Now the lane
        # publishes every other document, and the run fails after it so the failure notice names it.
        change = {"FAKE_DELTA_UPSERTS": json.dumps(PNUS[:2]), "FAKE_WITHHELD": json.dumps([PNUS[0]])}
        result, calls = self.bake(**change)
        self.assertEqual(result.returncode, 1, result.stderr + result.stdout)
        [publish] = self.published(calls)
        self.assertEqual(publish["command"], "publish-building-by-pnu-section-packs")
        self.assertIn(f"WITHHELD (root ADR-0325 §4): 1 ({PNUS[0]}) documents answer only that they are withheld",
                      result.stdout)
        # Under the registered job the lane ends 0, so a lane that follows it still bakes, and the
        # run fails at its end, naming the lane.
        for path in self.state_root.glob("*/in-progress*.json"):
            path.unlink()
        self.log.unlink()
        result, calls = ByPnuServingBake.bake(self, "all", FAKE_PACKS=packs_state(), FAKE_GOLD="203", **change)
        self.assertEqual(result.returncode, 1, result.stderr + result.stdout)
        self.assertIn("publish-building-by-pnu-section-packs", [call["command"] for call in self.published(calls)])
        last = result.stdout.splitlines()[-1]
        self.assertTrue(last.startswith("foundation-job-outcome failed"), last)
        self.assertIn("building-withheld", last)

    def test_the_job_declares_the_capability_the_first_pack_publish_requires(self):
        contract = json.loads((job_specs.PLATFORM_ROOT / "config/r2-connections.contract.json").read_text(encoding="utf-8"))
        wanted = contract["building_by_pnu_gateway"]["section_packs"]["scheduled_bake"]
        jobs = json.loads(job_specs.JOBS.read_text(encoding="utf-8"))["jobs"]
        [job] = [job for job in jobs if job["id"] == wanted["job"]]
        self.assertIn(wanted["capability"], job["capabilities"])
        self.assertIn("gold-building-panel-to-building-by-pnu-section-packs", job["pipeline_graph_edges"])
        # The claim is the script's: it bakes and publishes packs.
        script = (OPS / "by-pnu-serving-bake.sh").read_text(encoding="utf-8")
        for command in ("export-${UNIT}-by-pnu-section-packs", "publish-${UNIT}-by-pnu-section-packs"):
            self.assertIn(command, script)


class ParcelSectionPackBake(unittest.TestCase):
    """The parcel lane once its manifest names section packs (root ADR-0147 §8, ADR-0160): the same
    run as the building lane's, with the parcel commands, and no verified re-base."""

    setUp = ByPnuServingBake.setUp
    exports = ByPnuServingBake.exports
    published = ByPnuServingBake.published
    PREFIX = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_"

    def bake(self, **env):
        state = packs_state(sections=[{"name": "documents", "generation": 1, "patch_floor": 0}])
        return ByPnuServingBake.bake(self, "parcel", FAKE_PACKS=env.pop("FAKE_PACKS", state), **env)

    def test_a_change_set_from_the_parcel_packs_is_baked_as_a_parcel_pack_patch(self):
        result, calls = self.bake(FAKE_DELTA_UPSERTS=json.dumps(PNUS[:2]))
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [export] = self.exports(calls)
        self.assertEqual(export["command"], "export-parcel-by-pnu-section-packs")
        self.assertIn(self.PREFIX + "TARGET_PATCH", export["env"])
        [publish] = self.published(calls)
        self.assertEqual(publish["command"], "publish-parcel-by-pnu-section-packs")
        self.assertFalse(any(call["command"].endswith("-serving") or call["command"].endswith("-manifest")
                             for call in calls), "an object command ran on a lane serving packs")

    def test_a_verified_rebase_of_a_lane_serving_packs_is_refused(self):
        result, calls = self.bake(FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE="true",
                                  FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE_REASON="drill")
        self.assertEqual(result.returncode, 64, result.stderr + result.stdout)
        self.assertIn("serves section packs; a verified re-base reads served objects", result.stdout)
        self.assertEqual(self.exports(calls), [])
        self.assertEqual(self.published(calls), [])

    def test_the_job_declares_the_parcel_capability(self):
        contract = json.loads((job_specs.PLATFORM_ROOT / "config/r2-connections.contract.json").read_text(encoding="utf-8"))
        wanted = contract["parcel_by_pnu_gateway"]["section_packs"]["scheduled_bake"]
        jobs = json.loads(job_specs.JOBS.read_text(encoding="utf-8"))["jobs"]
        [job] = [job for job in jobs if job["id"] == wanted["job"]]
        self.assertIn(wanted["capability"], job["capabilities"])
        self.assertIn("gold-parcel-panel-to-parcel-by-pnu-section-packs", job["pipeline_graph_edges"])


if __name__ == "__main__":
    unittest.main()
