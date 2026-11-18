"""The parcel tile bake's Gold half and its rehearsal (scripts/ops/parcel-tile-bake.sh, root ADR-0253).

A fake docker stands in for `docker compose ... spark-submit`: the matching gate refuses (exit 1 with a
verdict that did not pass) or crashes (exit 1, no verdict), and the served Gold writes its summary. The
release layout is the host's (root ADR-0134); its tile-bake contract points the work disk at a temporary
directory. Edition ids come from the real source contract.
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

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
RELEASE_ID = "c" * 40
CONTAINER_ROOT = "/workspace/target/lakehouse"

FAKE_DOCKER = r'''#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
if args[-1:] == ["lakehouse-target-init"]:  # the write check, its own run (lakehouse_spark_run)
    sys.exit(0)
assert args[0] == "compose" and "spark-submit" in args, args
submit = args[args.index("spark-submit"):]
script = next(a for a in submit if a.endswith(".py"))
job_args = submit[submit.index(script) + 1:]
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps({"job": pathlib.Path(script).name, "args": job_args}) + "\n")
host = lambda path: pathlib.Path(path.replace(os.environ["FAKE_CONTAINER_ROOT"], os.environ["FAKE_HOST_ROOT"], 1))
summary = host(job_args[job_args.index("--summary-output") + 1])
if script.endswith("parcel_matching_gate.py"):
    if os.environ["FAKE_GATE"] == "crash":
        sys.exit(1)
    if os.environ["FAKE_GATE"] == "no-registry":
        summary.write_text(json.dumps({"passed": False, "registry": "wait", "inputs_absent": ["silver.parcel_registry"]}))
        sys.exit(3)
    summary.write_text(json.dumps({"passed": os.environ["FAKE_GATE"] == "pass"}))
    sys.exit(0 if os.environ["FAKE_GATE"] == "pass" else 1)
host(job_args[job_args.index("--output-dir") + 1]).mkdir(parents=True)
summary.write_text("{}")
'''


@unittest.skipIf(sys.platform == "win32", "runs the job's bash with a fake docker, as the host does")
class ParcelTileBakeGoldHalf(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="parcel-tile-bake-")
        self.addCleanup(temp.cleanup)
        self.root = pathlib.Path(temp.name)
        base = self.root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("parcel-tile-bake.sh", "admitted-writer-runtime.sh", "database-url.sh", "job-journal.sh",
                     "held-lock.sh"):
            shutil.copy(PLATFORM / "scripts/ops" / name, release / "scripts/ops" / name)
            (release / "scripts/ops" / name).chmod(0o755)
        shutil.copytree(PLATFORM / "infra/lakehouse/spark/jobs", release / "infra/lakehouse/spark/jobs",
                        ignore=shutil.ignore_patterns("__pycache__"))
        shutil.copytree(PLATFORM / "infra/lakehouse/contracts", release / "infra/lakehouse/contracts")
        self.work_root = self.root / "tile-bake"
        bake = json.loads((PLATFORM / "config/tile-bake-containers.contract.json").read_text(encoding="utf-8"))
        bake["work_disk"]["production_root"] = str(self.work_root)
        (release / "config").mkdir()
        (release / "config/tile-bake-containers.contract.json").write_text(json.dumps(bake))
        (base / "current").symlink_to(pathlib.Path("releases") / RELEASE_ID)
        artifacts = base / "artifacts" / RELEASE_ID
        artifacts.mkdir(parents=True)
        publisher = artifacts / "foundation-outbox-publisher"
        publisher.write_text("#!/bin/sh\n[ -z \"$FAKE_PUBLISHER_FAIL\" ] || exit 1\necho lakehouse-tile-bake-rehearsal synthetic\nexit 0\n")
        publisher.chmod(0o555)
        (artifacts / "build.json").write_text(json.dumps({
            "source": RELEASE_ID, "publisher_image": "sha256:" + "a" * 64, "tippecanoe_image": "sha256:" + "d" * 64,
            "files": {"foundation-outbox-publisher": hashlib.sha256(publisher.read_bytes()).hexdigest(),
                      "jars/fixture.jar": "0" * 64}}))
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        (bin_dir / "docker").write_text(FAKE_DOCKER)
        (bin_dir / "docker").chmod(0o755)
        self.log = self.root / "calls.jsonl"
        self.state = self.root / "state"
        self.script = base / "current/scripts/ops/parcel-tile-bake.sh"
        self.env = {
            "PATH": f"{bin_dir}:{os.environ['PATH']}", "FAKE_LOG": str(self.log),
            "FAKE_CONTAINER_ROOT": CONTAINER_ROOT, "FAKE_HOST_ROOT": str(self.work_root / "lakehouse"),
            "FOUNDATION_PARCEL_TILE_BAKE_STATE_ROOT": str(self.state),
            "DATABASE_URL": "postgres://synthetic", "INVOCATION_ID": "e" * 32,
        }
        sys.path.insert(0, str(PLATFORM / "infra/lakehouse/spark/jobs"))
        self.addCleanup(sys.path.remove, str(PLATFORM / "infra/lakehouse/spark/jobs"))
        import vworld_parcel_editions as editions  # noqa: PLC0415
        contract = editions.load()
        self.served = editions.snapshot_id(contract, editions.served(contract))
        self.rehearsed = sorted(contract["editions"])[0]
        self.newest = editions.snapshot_id(contract, self.rehearsed)
        self.served_name = editions.served(contract)
        self.candidate = min(name for name in contract["editions"] if name > self.served_name)
        self.readiness = self.root / "readiness.json"
        self.env["FOUNDATION_PARCEL_EDITION_READINESS_PATH"] = str(self.readiness)
        # The real ledger's only entry is the bootstrap edition, which the half never bakes (root ADR-0490 §4);
        # the served path is exercised on a copy whose served edition came through a gated promotion.
        promoted = json.loads(json.dumps(contract))
        promoted["promotions"][0]["readiness"] = {"checked_at_utc": "2099-01-01T00:00:00Z", "contract_sha256": "0" * 64,
                                                  "stages": {stage: "synthetic" for stage in editions.PROMOTION_STAGES}}
        editions.validate(promoted)
        self.promoted = self.root / "promoted-contract.json"
        self.promoted.write_text(json.dumps(promoted, ensure_ascii=False), encoding="utf-8")

    def run_half(self, mode, gate, bootstrap=False):
        env = {**self.env, "FAKE_GATE": gate}
        if not bootstrap:
            env["VWORLD_PARCEL_SOURCE_CONTRACT"] = str(self.promoted)
        result = subprocess.run(["bash", str(self.script), mode], env=env, capture_output=True, text=True, timeout=120)
        calls = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        return result, calls

    def test_a_rehearsal_records_the_gates_refusal_and_builds_the_newest_editions_gold(self):
        result, calls = self.run_half(f"rehearsal-gold-{self.rehearsed}", "refuse")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual([c["job"] for c in calls], ["parcel_matching_gate.py", "parcel_boundary_served_gold.py"])
        for call in calls:
            self.assertIn("--allow-non-served-edition", call["args"], call["job"])
        self.assertEqual(calls[0]["args"][calls[0]["args"].index("--snapshot-id") + 1], self.newest)
        self.assertEqual((self.state / "rehearsal-ready").read_text().split()[0], self.newest)
        self.assertFalse((self.state / "ready").exists(), "a rehearsal never readies the job's bake")
        self.assertIn("foundation-job-outcome unchanged", result.stdout)

    def test_a_gate_that_crashed_stops_a_rehearsal_before_the_gold(self):
        # 2026-10-15: the gate refused its arguments with exit 1 and no verdict, and was read as a refusal.
        result, calls = self.run_half(f"rehearsal-gold-{self.rehearsed}", "crash")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual([c["job"] for c in calls], ["parcel_matching_gate.py"])
        self.assertFalse((self.state / "rehearsal-ready").exists())

    def test_the_jobs_gold_half_stops_at_a_refusal_and_reads_only_the_served_edition(self):
        result, calls = self.run_half("gold", "refuse")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual([c["job"] for c in calls], ["parcel_matching_gate.py"])
        self.assertNotIn("--allow-non-served-edition", calls[0]["args"])
        self.assertEqual(calls[0]["args"][calls[0]["args"].index("--snapshot-id") + 1], self.served)

    def test_the_jobs_gold_half_waits_while_the_gates_registry_does_not_exist(self):
        # Root ADR-0265: the gate exits 3 before it reads anything; nothing is built or readied.
        result, calls = self.run_half("gold", "no-registry")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual([c["job"] for c in calls], ["parcel_matching_gate.py"])
        self.assertFalse((self.state / "ready").exists())
        self.assertEqual(result.stdout.strip().splitlines()[-1], "foundation-job-outcome unchanged")
        self.assertIn("silver.parcel_registry", (self.state / "journal.log").read_text())

    def test_a_rehearsal_waits_for_the_registry_like_the_job(self):
        result, calls = self.run_half(f"rehearsal-gold-{self.rehearsed}", "no-registry")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual([c["job"] for c in calls], ["parcel_matching_gate.py"])
        self.assertFalse((self.state / "rehearsal-ready").exists())

    def test_a_rehearsal_of_an_edition_the_contract_does_not_hold_is_refused(self):
        result, calls = self.run_half("rehearsal-gold-209912", "refuse")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(calls, [])

    def test_the_jobs_gold_half_readies_the_bake_when_the_gate_passes(self):
        result, calls = self.run_half("gold", "pass")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual([c["job"] for c in calls], ["parcel_matching_gate.py", "parcel_boundary_served_gold.py"])
        self.assertEqual((self.state / "ready").read_text().split()[0], self.served)
        self.assertEqual(result.stdout.strip().splitlines()[-1], "foundation-job-outcome changed")


    # Root ADR-0490 §4: 2026-10-15..11-11 the one rehearsal a person ran failed and nobody ran another,
    # so 202609 never became ready. The job's halves now rehearse the judgement's candidate themselves.
    def wait_for_rehearsal(self):
        self.readiness.write_text(json.dumps({"editions": [
            {"edition": self.served_name, "first_failing_stage": None},
            {"edition": self.candidate, "first_failing_stage": "rehearsal"}]}), encoding="utf-8")

    def test_the_bootstrap_edition_is_never_baked_and_with_no_candidate_nothing_runs(self):
        result, calls = self.run_half("gold", "pass", bootstrap=True)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(calls, [])
        self.assertIn("served by the release from before the lakehouse bake", (self.state / "journal.log").read_text())
        self.assertEqual(result.stdout.strip().splitlines()[-1], "foundation-job-outcome unchanged")

    def test_the_gold_half_rehearses_the_candidate_once_and_hands_it_to_the_bake(self):
        self.wait_for_rehearsal()
        result, calls = self.run_half("gold", "pass", bootstrap=True)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual([c["job"] for c in calls], ["parcel_matching_gate.py", "parcel_boundary_served_gold.py"])
        candidate = calls[0]["args"][calls[0]["args"].index("--snapshot-id") + 1]
        self.assertTrue(candidate.endswith(self.candidate), candidate)
        for call in calls:
            self.assertIn("--allow-non-served-edition", call["args"], call["job"])
        self.assertEqual((self.state / "rehearsal-ready").read_text().split()[0], candidate)
        self.assertFalse((self.state / "ready").exists(), "a rehearsal never readies the served bake")
        self.assertEqual(result.stdout.strip().splitlines()[-1], "foundation-job-outcome changed")
        # Again the same day: tried, not tried again until REHEARSAL_RETRY_DAYS pass.
        again, calls_after = self.run_half("gold", "pass", bootstrap=True)
        self.assertEqual(again.returncode, 0, again.stderr)
        self.assertEqual(len(calls_after), len(calls))
        self.assertIn("again after 7 days", (self.state / "journal.log").read_text())

    def test_the_bake_half_bakes_the_rehearsal_when_no_served_gold_waits(self):
        self.wait_for_rehearsal()
        self.run_half("gold", "pass", bootstrap=True)
        result, _ = self.run_half("bake", "pass", bootstrap=True)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        journal = (self.state / "journal.log").read_text()
        self.assertIn("rehearsal baked", journal)
        self.assertFalse((self.state / "baked-edition").exists(), "a rehearsal bakes nothing served")
        self.assertFalse((self.state / "rehearsal-ready").exists(), "a baked rehearsal Gold is used up")
        # A rehearsal bake that failed keeps its Gold and is not baked again from it.
        self.run_half("gold", "pass", bootstrap=True)
        self.assertFalse((self.state / "rehearsal-ready").exists(), "tried today: no second rehearsal Gold")

    def test_a_failed_rehearsal_bake_is_not_baked_again_from_the_same_gold(self):
        self.wait_for_rehearsal()
        self.run_half("gold", "pass", bootstrap=True)
        self.env["FAKE_PUBLISHER_FAIL"] = "1"
        failed, _ = self.run_half("bake", "pass", bootstrap=True)
        self.assertNotEqual(failed.returncode, 0)
        self.assertTrue((self.state / "rehearsal-ready").exists())
        again, _ = self.run_half("bake", "pass", bootstrap=True)
        self.assertEqual(again.returncode, 0, again.stderr)
        self.assertIn("failed; the next rehearsal makes another", (self.state / "journal.log").read_text())


if __name__ == "__main__":
    unittest.main()
