"""The scheduled panel Gold rebuild (scripts/ops/gold-panel-rebuild.sh) against a fake docker.

The fake stands in for `docker compose ... run spark spark-submit <job>` and runs the real
decisions: for gold_rebuild.py it calls gold_rebuild.plan on a synthetic catalog and writes the
plan and pins where the container would; for a producer it applies gold_rebuild's real row-loss
refusal and records a commit in the synthetic catalog only when the producer would write. The
release layout is the host's (root ADR-0134), as in test_by_pnu_serving_bake.py. Snapshot ids are
synthetic.
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

PLATFORM = job_specs.PLATFORM_ROOT
RELEASE_ID = "e" * 40
INVOCATION = "c" * 32

FAKE_DOCKER = r'''#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
if args[:1] == ["rm"]:
    sys.exit(0)
assert args[0] == "compose" and "spark" in args, args
jobs_dir = pathlib.Path(os.environ["FAKE_JOBS_DIR"])
sys.path.insert(0, str(jobs_dir))
import gold_rebuild
state = pathlib.Path(os.environ["FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT"])
def host(path):
    return state / pathlib.Path(path).relative_to("/workspace/target/lakehouse")
submit = args[args.index("spark-submit"):]
job = pathlib.Path(next(a for a in submit if a.startswith("/workspace/infra/lakehouse/spark/jobs/"))).name
job_args = submit[submit.index("/workspace/infra/lakehouse/spark/jobs/" + job) + 1:]
def flag(name):
    return job_args[job_args.index(name) + 1] if name in job_args else None
catalog_path = pathlib.Path(os.environ["FAKE_CATALOG"])
catalog = json.loads(catalog_path.read_text())
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps({"job": job, "args": job_args, "submit": submit, "project": args[args.index("-p") + 1]}) + "\n")
if job == "gold_rebuild.py":
    parsed = gold_rebuild.parse_args(job_args)
    name = parsed.gold_table
    entry = gold_rebuild.load_contract(jobs_dir.parent.parent / "contracts/gold-panel-rebuild.contract.json")["tables"][name]
    fixture = catalog[name]
    inputs = (tuple(fixture["required"]), fixture.get("optional", {}), fixture["required"][0])
    try:
        decision = gold_rebuild.plan(name, entry, inputs, fixture["gold"], fixture["silver"],
                                     unconditional=parsed.unconditional_reason, time_fallback=parsed.time_fallback,
                                     measuring=parsed.measuring)
    except gold_rebuild.PlanError as error:
        sys.exit(f"gold_rebuild.PlanError: {error}")
    for path, value in ((flag("--pins-output"), decision["source_snapshots"]), (flag("--plan-output"), decision)):
        host(path).parent.mkdir(parents=True, exist_ok=True)
        host(path).write_text(json.dumps(value))
    sys.exit(0)
name = {"parcel_panel_silver_to_gold.py": "gold.parcel_panel", "building_panel_silver_to_gold.py": "gold.building_panel"}[job]
fixture = catalog[name]
if fixture.get("crash"):
    sys.exit("Error: the producer crashed")
rows = fixture["new_rows"]
minimum = flag("--minimum-count")
gold_rebuild.assert_minimum_row_count(rows, None if minimum is None else int(minimum))
host(flag("--summary-output")).write_text(json.dumps({"row_count": rows}))
if "--validate-only" not in job_args:
    pins = json.loads(host(flag("--source-snapshots-path")).read_text())
    fixture.setdefault("commits", []).append({"rows": rows, "pins": pins})
    catalog_path.write_text(json.dumps(catalog))
'''


def snap(snapshot_id, parent, at, added=1):
    return {"snapshot_id": snapshot_id, "parent_id": parent, "committed_at": at, "operation": "append",
            "summary": {"added-records": str(added)}}


def gold(pins, rows):
    summary = {"total-records": str(rows)}
    if pins is not None:
        summary["foundation.source-iceberg-snapshots"] = json.dumps(pins)
    return {"head": "90", "row_count": rows, "published_at_utc": "2026-01-10T00:00:00Z", "snapshots": [{
        "snapshot_id": "90", "parent_id": None, "committed_at": "2026-01-10T01:00:00Z", "operation": "overwrite",
        "summary": summary}]}


def table_fixture(changed, new_rows=1000, rows=1000, pinned=True):
    snapshots = [snap("11", None, "2026-01-01T00:00:00Z")]
    if changed:
        snapshots.append(snap("12", "11", "2026-01-20T00:00:00Z"))
    return {"required": ["silver.a", "silver.b"],
            "gold": gold({"silver.a": "11", "silver.b": "21"} if pinned else None, rows),
            "silver": {"silver.a": {"head": snapshots[-1]["snapshot_id"], "snapshots": snapshots},
                       "silver.b": {"head": "21", "snapshots": [snap("21", None, "2026-01-01T00:00:00Z")]}},
            "new_rows": new_rows}


class GoldPanelRebuild(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="gold-rebuild-")
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        base = self.root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("gold-panel-rebuild.sh", "admitted-writer-runtime.sh"):
            (release / "scripts/ops" / name).write_bytes((PLATFORM / "scripts/ops" / name).read_bytes())
            (release / "scripts/ops" / name).chmod(0o755)
        (release / "infra/lakehouse/contracts").mkdir(parents=True)
        contract = "infra/lakehouse/contracts/gold-panel-rebuild.contract.json"
        (release / contract).write_bytes((PLATFORM / contract).read_bytes())
        (release / "orchestration").mkdir()
        self.jobs = release / "orchestration/jobs.v1.json"
        self.jobs.write_bytes(job_specs.JOBS.read_bytes())
        (base / "current").symlink_to(pathlib.Path("releases") / RELEASE_ID)
        artifacts = base / "artifacts" / RELEASE_ID
        artifacts.mkdir(parents=True)
        publisher = artifacts / "foundation-outbox-publisher"
        publisher.write_text('#!/bin/sh\nprintf "publisher %s\\n" "$*"\n')
        publisher.chmod(0o555)
        (artifacts / "build.json").write_text(json.dumps({
            "source": RELEASE_ID, "publisher_image": "sha256:" + "a" * 64, "tippecanoe_image": "sha256:" + "d" * 64,
            "files": {"foundation-outbox-publisher": hashlib.sha256(publisher.read_bytes()).hexdigest(),
                      "jars/fixture.jar": "0" * 64}}))
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        (bin_dir / "docker").write_text(FAKE_DOCKER)
        (bin_dir / "docker").chmod(0o755)
        self.script = base / "current/scripts/ops/gold-panel-rebuild.sh"
        self.catalog = self.root / "catalog.json"
        self.log = self.root / "calls.jsonl"
        self.env = {
            "INVOCATION_ID": INVOCATION,
            "PATH": f"{bin_dir}:{os.environ['PATH']}", "FAKE_LOG": str(self.log), "FAKE_CATALOG": str(self.catalog),
            "FAKE_JOBS_DIR": str(PLATFORM / "infra/lakehouse/spark/jobs"),
            "FOUNDATION_GOLD_REBUILD_STATE_ROOT": str(self.root / "state"),
            "FOUNDATION_GOLD_REBUILD_SCRATCH": str(self.root / "scratch"),
        }

    def rebuild(self, parcel, building, *args):
        self.catalog.write_text(json.dumps({"gold.parcel_panel": parcel, "gold.building_panel": building}))
        result = subprocess.run(["bash", str(self.script), *args], env=self.env,
                                capture_output=True, text=True, timeout=120)
        calls = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        return result, calls, json.loads(self.catalog.read_text())

    def producer_calls(self, calls):
        return [call for call in calls if call["job"] != "gold_rebuild.py"]

    def test_no_newer_silver_is_nothing_to_do(self):
        result, calls, catalog = self.rebuild(table_fixture(False), table_fixture(False), "all")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(result.stdout.count("nothing to do"), 2, result.stdout)
        self.assertEqual([call["job"] for call in calls], ["gold_rebuild.py", "gold_rebuild.py"])
        self.assertFalse(any("commits" in table for table in catalog.values()))

    def test_newer_silver_rebuilds_with_the_pins_and_the_row_floor(self):
        result, calls, catalog = self.rebuild(table_fixture(False), table_fixture(True, new_rows=1005), "all")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [producer] = self.producer_calls(calls)
        self.assertEqual(producer["job"], "building_panel_silver_to_gold.py")
        args = producer["args"]
        self.assertEqual(args[args.index("--iceberg-snapshot-id") + 1], "12")
        self.assertEqual(args[args.index("--minimum-count") + 1], "990")
        self.assertIn("--allow-non-smoke-overwrite", args)
        self.assertFalse(any("--measuring" in call["args"] for call in calls))
        self.assertNotIn("--validate-only", args)
        # Sized by the contract, in the compose `spark` service the memory guard counts.
        submit = producer["submit"]
        self.assertEqual(submit[submit.index("--driver-memory") + 1], "16g")
        self.assertEqual(catalog["gold.building_panel"]["commits"],
                         [{"rows": 1005, "pins": {"silver.a": "12", "silver.b": "21"}}])
        self.assertNotIn("commits", catalog["gold.parcel_panel"])
        self.assertIn("committed gold.building_panel: 1005 rows (floor 990)", result.stdout)

    def test_row_loss_beyond_the_tolerance_is_refused_and_nothing_is_committed(self):
        result, calls, catalog = self.rebuild(table_fixture(False), table_fixture(True, new_rows=989), "building")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("fewer than the 990 the previous snapshot allows", result.stderr)
        self.assertIn("nothing was committed", result.stdout)
        self.assertNotIn("commits", catalog["gold.building_panel"])

    def test_a_dry_run_runs_the_whole_transform_and_commits_nothing(self):
        result, calls, catalog = self.rebuild(table_fixture(True), table_fixture(False), "parcel", "--dry-run")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [producer] = self.producer_calls(calls)
        self.assertIn("--validate-only", producer["args"])
        # Only a dry run may read an input the contract lists as unmeasured.
        self.assertIn("--measuring", calls[0]["args"])
        self.assertIn("dry run passed: 1000 rows (floor 990)", result.stdout)
        self.assertNotIn("commits", catalog["gold.parcel_panel"])

    def test_a_failed_table_does_not_stop_the_other_but_fails_the_run(self):
        parcel = {**table_fixture(True), "crash": True}
        result, calls, catalog = self.rebuild(parcel, table_fixture(True), "all")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual([call["job"] for call in self.producer_calls(calls)],
                         ["parcel_panel_silver_to_gold.py", "building_panel_silver_to_gold.py"])
        self.assertEqual(len(catalog["gold.building_panel"]["commits"]), 1)

    def set_enabled(self, enabled):
        listing = json.loads(self.jobs.read_text(encoding="utf-8"))
        next(job for job in listing["jobs"] if job["id"] == "gold_panel_rebuild")["enabled"] = enabled
        self.jobs.write_text(json.dumps(listing), encoding="utf-8")

    def test_spark_runs_in_this_invocations_own_compose_project(self):
        result, calls, _ = self.rebuild(table_fixture(True), table_fixture(True), "all")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(len(calls), 4)
        self.assertEqual({call["project"] for call in calls}, {"foundation-gold-rebuild-" + INVOCATION})
        # Outside systemd the run names a fresh project and says how to clean it up.
        del self.env["INVOCATION_ID"]
        self.log.unlink()
        result, calls, _ = self.rebuild(table_fixture(False), table_fixture(False), "all")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [project] = {call["project"] for call in calls}
        self.assertRegex(project, r"^foundation-gold-rebuild-[0-9a-f]{32}$")
        self.assertIn("INVOCATION_ID=" + project.removeprefix("foundation-gold-rebuild-") + " ", result.stderr)
        self.env["INVOCATION_ID"] = "../other"
        result, calls, _ = self.rebuild(table_fixture(True), table_fixture(True), "parcel")
        self.assertEqual(result.returncode, 64)

    def test_cleanup_has_the_release_publisher_stop_this_invocations_containers(self):
        result = subprocess.run(["bash", str(self.script), "cleanup"], env=self.env,
                                capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "publisher stop-gold-panel-rebuild\n")
        for args, env in ((["cleanup", "extra"], self.env),
                          (["cleanup"], {k: v for k, v in self.env.items() if k != "INVOCATION_ID"})):
            refused = subprocess.run(["bash", str(self.script), *args], env=env,
                                     capture_output=True, text=True, timeout=60)
            self.assertEqual(refused.returncode, 64, refused.stderr)
            self.assertEqual(refused.stdout, "")

    def test_an_enabled_job_refuses_a_gold_without_pins_and_the_first_run_may_use_its_publish_time(self):
        unpinned = table_fixture(True, pinned=False)
        self.set_enabled(True)
        result, calls, catalog = self.rebuild(unpinned, table_fixture(False), "parcel")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("records no source pins", result.stderr)
        self.assertIn("could not plan", result.stdout)
        self.assertIn("--no-time-fallback", calls[0]["args"])
        self.assertEqual(self.producer_calls(calls), [])
        self.assertNotIn("commits", catalog["gold.parcel_panel"])
        # A job list that cannot be read counts as enabled.
        self.jobs.unlink()
        self.log.unlink()
        result, calls, _ = self.rebuild(unpinned, table_fixture(False), "parcel")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--no-time-fallback", calls[0]["args"])
        # Before the job is on (the supervised first run), the publish time decides.
        self.jobs.write_bytes(job_specs.JOBS.read_bytes())
        self.set_enabled(False)
        self.log.unlink()
        result, calls, catalog = self.rebuild(unpinned, table_fixture(False), "parcel")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertNotIn("--no-time-fallback", calls[0]["args"])
        self.assertEqual(len(catalog["gold.parcel_panel"]["commits"]), 1)

    def test_an_unconditional_run_rebuilds_both_with_its_reason_and_the_row_floor(self):
        self.set_enabled(True)
        result, calls, catalog = self.rebuild(table_fixture(False, pinned=False), table_fixture(False, new_rows=989),
                                              "all", "--unconditional", "first supervised run")
        self.assertNotEqual(result.returncode, 0, "the building row floor still refuses")
        self.assertIn("rebuilding gold.parcel_panel: unconditional rebuild: first supervised run", result.stdout)
        self.assertEqual([call["job"] for call in self.producer_calls(calls)],
                         ["parcel_panel_silver_to_gold.py", "building_panel_silver_to_gold.py"])
        self.assertEqual(catalog["gold.parcel_panel"]["commits"],
                         [{"rows": 1000, "pins": {"silver.a": "11", "silver.b": "21"}}])
        self.assertNotIn("commits", catalog["gold.building_panel"])
        for args in (("parcel", "--unconditional"), ("parcel", "--unconditional", "--dry-run")):
            with self.subTest(args=args):
                self.log.unlink(missing_ok=True)
                result, calls, _ = self.rebuild(table_fixture(True), table_fixture(True), *args)
                self.assertEqual(result.returncode, 64)
                self.assertEqual(calls, [])

    def test_an_unknown_unit_or_option_is_refused(self):
        for args in (("everything",), ("parcel", "--force")):
            with self.subTest(args=args):
                result, calls, _ = self.rebuild(table_fixture(True), table_fixture(True), *args)
                self.assertEqual(result.returncode, 64)
                self.assertEqual(calls, [])


class Registration(unittest.TestCase):
    def test_the_job_is_alone_in_the_spark_pool_and_runs_before_the_bake(self):
        listing = json.loads(job_specs.JOBS.read_text(encoding="utf-8"))
        jobs = {job["id"]: job for job in listing["jobs"]}
        job = jobs["gold_panel_rebuild"]
        # Switched on after its supervised first run (2026-10-07, runbook gold-panel-rebuild.md 4).
        self.assertTrue(job["enabled"])
        self.assertNotIn("disabled_reason", job)
        self.assertEqual((job["pool"], job["pool_slots"], job["retries"]), ("spark", 3, 0))
        bake = jobs["by_pnu_serving_bake"]
        # Waiting together, Airflow starts the heavier: the Gold first, then the bake.
        self.assertGreater(job["priority_weight"], bake["priority_weight"])
        self.assertLess(int(job["schedule"].split()[1]), int(bake["schedule"].split()[1]))
        unit = (job_specs.SYSTEMD / job["systemd_service"]).read_text(encoding="utf-8")
        self.assertIn("gold-panel-rebuild.sh all", unit)
        # A timeout kills the client, not the daemon's Spark container: ExecStopPost removes it.
        self.assertRegex(unit, r"(?m)^ExecStopPost=/opt/foundation-platform/current/scripts/ops/gold-panel-rebuild.sh cleanup$")
        # systemd stops it before Airflow would (test_job_specs checks every job); a longer
        # timeout pushes the hourly folds past their starvation bound.
        self.assertIn("TimeoutStartSec=160m", unit)
        self.assertEqual(job["timeout_minutes"], 165)
        stretched = json.loads(job_specs.JOBS.read_text(encoding="utf-8"))
        next(j for j in stretched["jobs"] if j["id"] == "gold_panel_rebuild")["timeout_minutes"] = 166
        self.assertTrue(any(problem.startswith("map_edit_fold_admin may wait 1201 minutes")
                            for problem in job_specs.pool_starvation(stretched)))

    def test_the_job_carries_out_every_silver_edge_into_the_panel_golds(self):
        graph = json.loads(job_specs.GRAPH.read_text(encoding="utf-8"))
        contract = json.loads((PLATFORM / "infra/lakehouse/contracts/gold-panel-rebuild.contract.json").read_text(encoding="utf-8"))
        golds = {node["id"] for node in graph["nodes"] if node.get("table_name") in contract["tables"]}
        expected = {edge["id"] for edge in graph["edges"] if edge["to"] in golds and edge["status"] == "implemented"}
        job = next(job for job in json.loads(job_specs.JOBS.read_text(encoding="utf-8"))["jobs"]
                   if job["id"] == "gold_panel_rebuild")
        self.assertEqual(set(job["pipeline_graph_edges"]), expected)


if __name__ == "__main__":
    unittest.main()
