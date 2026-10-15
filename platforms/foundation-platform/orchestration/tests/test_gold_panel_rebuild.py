"""The scheduled panel Gold rebuild (scripts/ops/gold-panel-rebuild.sh) against a fake docker.

The fake stands in for `docker compose ... run spark spark-submit <job>` and runs the real
decisions: for gold_rebuild.py it calls gold_rebuild.plan on a synthetic catalog and writes the
plan and pins where the container would; for a producer it applies gold_rebuild's real row-loss
refusal and records a commit in the synthetic catalog only when the producer would write. The
release layout is the host's (root ADR-0134), as in test_by_pnu_serving_bake.py. Snapshot ids are
synthetic.
"""

import fcntl
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

PLATFORM = job_specs.PLATFORM_ROOT
sys.path.insert(0, str(PLATFORM / "infra/lakehouse/spark/jobs"))
import by_pnu_kinds  # noqa: E402

# The kind tables `all` rebuilds after the panels, one per kind line (root ADR-0193).
KIND_TABLES = [kind.gold_table for kind in by_pnu_kinds.kinds().values()]
KIND_PLANS = ["gold_rebuild.py"] * len(KIND_TABLES)
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
    contract = os.environ.get("FAKE_CONTRACT") or jobs_dir.parent.parent / "contracts/gold-panel-rebuild.contract.json"
    entry = gold_rebuild.load_contract(pathlib.Path(contract))["tables"][name]
    fixture = catalog[name]
    inputs = (tuple(fixture["required"]), fixture.get("optional", {}), fixture["required"][0])
    follows, pinned = entry.get("pins_from"), None
    if follows:
        # The followed table's current Gold. Each fixture keeps its own Silver history unless it says it
        # shares the followed table's (as production does): then a commit this run made is followed too.
        other = catalog[follows["gold_table"]]
        pinned = other["gold"]
        if fixture.get("shares_silver"):
            fixture["silver"]["silver.a"] = other["silver"]["silver.a"]
        if fixture.get("shares_silver") and other.get("commits"):
            summary = {"total-records": "1", gold_rebuild.SOURCE_SNAPSHOTS_PROPERTY: json.dumps(other["commits"][-1]["pins"])}
            pinned = {**pinned, "head": "91", "snapshots": [*pinned["snapshots"], {
                "snapshot_id": "91", "parent_id": pinned["head"], "committed_at": "2026-01-21T00:00:00Z",
                "operation": "overwrite", "summary": summary}]}
        # The fixtures name their inputs silver.a and silver.b: the pinned one is silver.a, or every one
        # for a kind table, which checks parity with its host panel (root ADR-0193 §6-2).
        entry = {**entry, "pins_from": {**follows, "inputs": "all" if follows["inputs"] == "all" else ["silver.a"]}}
    try:
        decision = gold_rebuild.plan(name, entry, inputs, fixture["gold"], fixture["silver"],
                                     unconditional=parsed.unconditional_reason, time_fallback=parsed.time_fallback,
                                     measuring=parsed.measuring, pinned_gold=pinned)
    except gold_rebuild.PlanError as error:
        sys.exit(f"gold_rebuild.PlanError: {error}")
    for path, value in ((flag("--pins-output"), decision["source_snapshots"]), (flag("--plan-output"), decision)):
        host(path).parent.mkdir(parents=True, exist_ok=True)
        host(path).write_text(json.dumps(value))
    sys.exit(0)
if job == "unit_official_price.py":
    unit_price = catalog.get("unit_price", {})
    if unit_price.get("crash"):
        sys.exit("Error: the unit price join failed")
    host(flag("--summary-output")).write_text(json.dumps({"vintage": "209909", "appended": unit_price.get("appended", False)}))
    sys.exit(0)
contract = os.environ.get("FAKE_CONTRACT") or jobs_dir.parent.parent / "contracts/gold-panel-rebuild.contract.json"
name, = [n for n, t in gold_rebuild.load_contract(pathlib.Path(contract))["tables"].items()
         if t["producer"] + ".py" == job and t.get("lane") in (None, flag("--lane"))]
fixture = catalog[name]
if fixture.get("crash"):
    sys.exit("Error: the producer crashed")
rows = fixture["new_rows"]
minimum = flag("--minimum-count")
gold_rebuild.assert_minimum_row_count(rows, None if minimum is None else int(minimum))
host(flag("--summary-output")).write_text(json.dumps({"row_count": rows, **fixture.get("summary", {})}))
if "--validate-only" not in job_args:
    pins = json.loads(host(flag("--source-snapshots-path")).read_text())
    commit = {"rows": rows, "pins": pins}
    if flag("--incremental-from-snapshots"):
        commit["previous_pins"] = json.loads(host(flag("--incremental-from-snapshots")).read_text())
    fixture.setdefault("commits", []).append(commit)
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
        # The script reads the table list through the release's planner, which reads the kind lines.
        for part in ("infra/lakehouse/contracts", "infra/lakehouse/spark/jobs"):
            shutil.copytree(PLATFORM / part, release / part, ignore=shutil.ignore_patterns("__pycache__"))
        (release / "config").mkdir()
        (release / "config/r2-connections.contract.json").write_bytes(
            (PLATFORM / "config/r2-connections.contract.json").read_bytes())
        contract = "infra/lakehouse/contracts/gold-panel-rebuild.contract.json"
        self.contract = release / contract
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

    def rebuild(self, parcel, building, *args, unit_price=None, unit_price_panel=None, extra=None):
        # The unit price panel (root ADR-0190) has nothing to do unless a test says otherwise.
        # So has every kind table (root ADR-0193), which follows its host panel's pins.
        self.catalog.write_text(json.dumps({"gold.parcel_panel": parcel, "gold.building_panel": building,
                                            "gold.unit_price": unit_price_panel or table_fixture(False),
                                            **{name: table_fixture(False) for name in KIND_TABLES},
                                            "unit_price": unit_price or {}, **(extra or {})}))
        result = subprocess.run(["bash", str(self.script), *args], env=self.env,
                                capture_output=True, text=True, timeout=120)
        calls = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        return result, calls, json.loads(self.catalog.read_text())

    def producer_calls(self, calls):
        return [call for call in calls if call["job"] not in ("gold_rebuild.py", "unit_official_price.py")]

    def test_the_unit_price_panel_first_joins_the_unit_prices(self):
        # Root ADR-0182, ADR-0190: before the unit price plan, never for the parcel or building panel
        # nor in a dry run.
        result, calls, _ = self.rebuild(table_fixture(False), table_fixture(False), "all", unit_price={"appended": True})
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual([call["job"] for call in calls], ["gold_rebuild.py", "gold_rebuild.py",
                                                           "unit_official_price.py", "gold_rebuild.py"])
        self.assertIn("unit prices: vintage 209909 appended", result.stdout)
        self.log.unlink()
        result, calls, _ = self.rebuild(table_fixture(False), table_fixture(False), "unit-price", "--dry-run",
                                        unit_price_panel=table_fixture(True))
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertNotIn("unit_official_price.py", [call["job"] for call in calls])
        # A table run on its own always joins.
        self.log.unlink()
        result, calls, _ = self.rebuild(table_fixture(False), table_fixture(False), "unit-price")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual([call["job"] for call in calls], ["unit_official_price.py", "gold_rebuild.py"])

    def test_a_failed_unit_price_join_refuses_the_unit_price_panel(self):
        result, calls, catalog = self.rebuild(table_fixture(False), table_fixture(False), "unit-price",
                                              unit_price={"crash": True}, unit_price_panel=table_fixture(True))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("could not derive silver.unit_official_price", result.stdout + result.stderr)
        self.assertEqual(self.producer_calls(calls), [])
        self.assertNotIn("commits", catalog["gold.unit_price"])

    def test_the_unit_price_panel_places_units_from_the_registers_the_building_panel_was_built_from(self):
        # Root ADR-0190: on its own the price panel waits for the building panel's registers...
        shared = {**table_fixture(True), "shares_silver": True}
        result, calls, catalog = self.rebuild(table_fixture(False), table_fixture(True), "unit-price",
                                              unit_price_panel=shared)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("nothing to do", result.stdout)
        self.assertNotIn("commits", catalog["gold.unit_price"])
        # ...and in `all`, rebuilt after the building panel, it takes the pins that one committed.
        self.log.unlink()
        result, calls, catalog = self.rebuild(table_fixture(False), table_fixture(True), "all",
                                              unit_price_panel=shared)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(catalog["gold.unit_price"]["commits"][0]["pins"],
                         catalog["gold.building_panel"]["commits"][0]["pins"])

    def test_all_rebuilds_every_table_of_the_contract_in_its_order(self):
        # A table the contract gains is rebuilt by the scheduled job without an edit to the script.
        contract = json.loads(self.contract.read_text(encoding="utf-8"))
        extra = dict(contract["tables"]["gold.parcel_panel"], unit="extra")
        contract["tables"] = {"gold.extra_panel": extra, **contract["tables"]}
        self.contract.write_text(json.dumps(contract), encoding="utf-8")
        self.env["FAKE_CONTRACT"] = str(self.contract)
        result, calls, _ = self.rebuild(table_fixture(False), table_fixture(False), "all",
                                        extra={"gold.extra_panel": table_fixture(False)})
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        planned = [call["args"][call["args"].index("--gold-table") + 1] for call in calls if call["job"] == "gold_rebuild.py"]
        self.assertEqual(planned, list(contract["tables"]))
        result, calls, _ = self.rebuild(table_fixture(False), table_fixture(False), "extra",
                                        extra={"gold.extra_panel": table_fixture(False)})
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)

    def test_no_newer_silver_is_nothing_to_do(self):
        result, calls, catalog = self.rebuild(table_fixture(False), table_fixture(False), "all")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(result.stdout.count("nothing to do"), 3, result.stdout)
        self.assertEqual([call["job"] for call in calls], ["gold_rebuild.py", "gold_rebuild.py",
                                                           "unit_official_price.py", "gold_rebuild.py"])
        self.assertFalse(any("commits" in table for table in catalog.values() if isinstance(table, dict)))
        # Nothing committed: the bake is not started (root ADR-0171).
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome unchanged")
        self.assertEqual(job_specs.job_outcome(result.stdout), "unchanged")

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
        # The recorded pin 11 is still kept, so the producer merges only the changed PNUs (root ADR-0180),
        # comparing against the pins the current Gold was built from, under the contract's bounds.
        self.assertEqual(catalog["gold.building_panel"]["commits"],
                         [{"rows": 1005, "pins": {"silver.a": "12", "silver.b": "21"},
                           "previous_pins": {"silver.a": "11", "silver.b": "21"}}])
        self.assertEqual(args[args.index("--max-changed-key-fraction") + 1], "0.3")
        self.assertEqual(args[args.index("--parity-sample-keys") + 1], "20000")
        self.assertIn("incremental: only the PNUs whose inputs changed", result.stdout)
        self.assertNotIn("commits", catalog["gold.parcel_panel"])
        self.assertIn("committed gold.building_panel: 1005 rows (floor 990)", result.stdout)
        # One table committed: the run changed its outputs and starts the bake (root ADR-0171).
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome changed")

    def test_kinds_rebuilds_every_kind_table_and_only_those(self):
        # Root ADR-0193 §9: the kind job's own run; `all` (the panel job) leaves the kinds to it.
        result, calls, _ = self.rebuild(table_fixture(False), table_fixture(False), "kinds")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        planned = [call["args"][call["args"].index("--gold-table") + 1] for call in calls if call["job"] == "gold_rebuild.py"]
        self.assertEqual(planned, KIND_TABLES)
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome unchanged")

    def test_a_kind_follows_its_host_panel_and_checks_parity_with_the_snapshot_it_committed(self):
        # Root ADR-0193 §3, §6-2: the kind runs on the pins of the panel the panel job committed, and the
        # one producer, told the kind's lane, checks it against that panel snapshot.
        self.assertIn("gold.parcel_zoning", KIND_TABLES)
        committed = {**table_fixture(True), "commits": [{"rows": 1004, "pins": {"silver.a": "12", "silver.b": "21"}}]}
        zoning = {**table_fixture(True), "shares_silver": True}
        result, calls, catalog = self.rebuild(committed, table_fixture(False), "kinds",
                                              extra={"gold.parcel_zoning": zoning})
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual([call["job"] for call in self.producer_calls(calls)], ["by_pnu_kind_silver_to_gold.py"])
        args = self.producer_calls(calls)[0]["args"]
        self.assertEqual(args[args.index("--lane") + 1], "parcel-zoning")
        self.assertEqual(args[args.index("--parity-panel-snapshot-id") + 1], "91")
        self.assertEqual(catalog["gold.parcel_zoning"]["commits"][0]["pins"], {"silver.a": "12", "silver.b": "21"})
        # The line root ADR-0193 §7 measures each table against, and the outcome that starts the bake.
        self.assertRegex(result.stdout, r"(?m)^gold-kind gold\.parcel_zoning mode=full seconds=\d+ rows=1000 changed_keys=-$")
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome changed")

    def test_a_pin_the_silver_no_longer_keeps_is_a_full_rebuild_with_its_reason(self):
        building = table_fixture(True, new_rows=1005)
        building["silver"]["silver.a"]["snapshots"] = building["silver"]["silver.a"]["snapshots"][1:]
        result, calls, catalog = self.rebuild(table_fixture(False), building, "building")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [producer] = self.producer_calls(calls)
        self.assertNotIn("--incremental-from-snapshots", producer["args"])
        self.assertIn("full rebuild: silver.a: snapshot 11 the Gold was built from is no longer kept", result.stdout)
        self.assertNotIn("previous_pins", catalog["gold.building_panel"]["commits"][0])

    def test_an_incremental_run_that_changed_no_row_does_not_start_the_bake(self):
        building = table_fixture(True)
        building["summary"] = {"rebuild": {"mode": "incremental", "merged": False}}
        result, _, catalog = self.rebuild(table_fixture(False), building, "all")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(len(catalog["gold.building_panel"]["commits"]), 1, "the new pins are recorded")
        self.assertIn("recorded the new pins on gold.building_panel: no PNU's row changed", result.stdout)
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome unchanged")

    def test_a_dry_run_of_both_tables_changes_nothing(self):
        result, _, catalog = self.rebuild(table_fixture(True), table_fixture(True), "all", "--dry-run")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertFalse(any("commits" in table for table in catalog.values()))
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome unchanged")

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
        # The building table did change; the failed run records no events (its next start will).
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome changed")

    def set_enabled(self, enabled):
        listing = json.loads(self.jobs.read_text(encoding="utf-8"))
        next(job for job in listing["jobs"] if job["id"] == "gold_panel_rebuild")["enabled"] = enabled
        self.jobs.write_text(json.dumps(listing), encoding="utf-8")

    def test_spark_runs_in_this_invocations_own_compose_project(self):
        result, calls, _ = self.rebuild(table_fixture(True), table_fixture(True), "all")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(len(calls), 6)  # three plans, the unit price join, two producers
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
                         ["parcel_panel_silver_to_gold.py", "building_panel_silver_to_gold.py",
                          "unit_price_panel_silver_to_gold.py"])
        self.assertEqual(catalog["gold.parcel_panel"]["commits"],
                         [{"rows": 1000, "pins": {"silver.a": "11", "silver.b": "21"}}])
        self.assertNotIn("commits", catalog["gold.building_panel"])
        for args in (("parcel", "--unconditional"), ("parcel", "--unconditional", "--dry-run")):
            with self.subTest(args=args):
                self.log.unlink(missing_ok=True)
                result, calls, _ = self.rebuild(table_fixture(True), table_fixture(True), *args)
                self.assertEqual(result.returncode, 64)
                self.assertEqual(calls, [])

    def test_one_rebuild_at_a_time_whoever_started_it(self):
        # The scheduled unit and the operator's unconditional copy (root ADR-0166) share this lock;
        # `all` holds it for both tables, so its own per-table runs are not refused.
        state = self.root / "state"
        state.mkdir()
        with open(state / "rebuild.lock", "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result, calls, _ = self.rebuild(table_fixture(True), table_fixture(True), "all")
        self.assertEqual(result.returncode, 75, result.stderr + result.stdout)
        self.assertIn("another rebuild holds", result.stderr)
        self.assertEqual(calls, [])
        result, calls, _ = self.rebuild(table_fixture(True), table_fixture(True), "all")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(len(self.producer_calls(calls)), 2)

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
        # Started by its Silver inputs, and the bake by it (root ADR-0171); on the fallback
        # schedules too, the Gold comes first.
        self.assertEqual((job["started_by"], bake["started_by"]), ("inputs", "inputs"))
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
        # And the unit price join it runs before the building panel (root ADR-0182).
        expected |= {edge["id"] for edge in graph["edges"] if edge["to"] == "silver-unit-official-price"}
        job = next(job for job in json.loads(job_specs.JOBS.read_text(encoding="utf-8"))["jobs"]
                   if job["id"] == "gold_panel_rebuild")
        self.assertEqual(set(job["pipeline_graph_edges"]), expected)

    def test_every_kind_line_is_wired_into_the_data_map_and_both_jobs(self):
        # Root ADR-0193 §9: a kind line is the only hand-written list. What it needs beyond the line is
        # named here, and a missing piece fails with the line's name and the piece.
        graph = json.loads(job_specs.GRAPH.read_text(encoding="utf-8"))
        jobs = {job["id"]: job for job in json.loads(job_specs.JOBS.read_text(encoding="utf-8"))["jobs"]}
        nodes = {node["id"]: node for node in graph["nodes"]}
        edges = {edge["id"]: edge for edge in graph["edges"]}
        node_id = lambda table: table.replace(".", "-").replace("_", "-")  # noqa: E731
        missing = []
        for lane, kind in by_pnu_kinds.kinds().items():
            gold, surface = node_id(kind.gold_table), f"{lane}-by-pnu-section-packs"
            fed, served = f"{node_id(kind.host_panel)}-to-{gold}", f"{gold}-to-{surface}"
            wanted = [(gold in nodes and nodes[gold].get("table_name") == kind.gold_table, f"graph node {gold}"),
                      (surface in nodes, f"graph node {surface}"),
                      (fed in edges, f"graph edge {fed}"), (served in edges, f"graph edge {served}"),
                      (fed in jobs["gold_kind_rebuild"]["pipeline_graph_edges"], f"gold_kind_rebuild carries {fed}"),
                      (served in jobs["by_pnu_serving_bake"]["pipeline_graph_edges"], f"by_pnu_serving_bake carries {served}")]
            missing += [f"{lane}: {what}" for ok, what in wanted if not ok]
        self.assertEqual(missing, [])
        kind_golds = {node_id(kind.gold_table) for kind in by_pnu_kinds.kinds().values()}
        # The kind job carries exactly the edges into the kind tables, and is started by the panels.
        self.assertEqual(set(jobs["gold_kind_rebuild"]["pipeline_graph_edges"]),
                         {edge["id"] for edge in graph["edges"] if edge["to"] in kind_golds})

    def test_the_kind_job_runs_the_kinds_after_the_panels_in_the_spark_pool(self):
        job = next(job for job in json.loads(job_specs.JOBS.read_text(encoding="utf-8"))["jobs"]
                   if job["id"] == "gold_kind_rebuild")
        self.assertEqual((job["started_by"], job["pool"], job["pool_slots"], job["retries"]), ("inputs", "spark", 3, 0))
        unit = (job_specs.SYSTEMD / job["systemd_service"]).read_text(encoding="utf-8")
        self.assertIn("gold-panel-rebuild.sh kinds", unit)
        self.assertRegex(unit, r"(?m)^ExecStopPost=/opt/foundation-platform/current/scripts/ops/gold-panel-rebuild.sh cleanup$")
        self.assertIn("TimeoutStartSec=160m", unit)
        # It shares the panel rebuild's state directory, where the one-rebuild-at-a-time lock lives.
        self.assertIn("StateDirectory=foundation-gold-panel-rebuild", unit)

    def test_every_lane_serves_a_gold_this_job_rebuilds_and_no_two_lanes_share_one(self):
        # A lane names its Gold once (r2-connections.contract.json by_pnu_lanes, root ADR-0193 §3);
        # this job's contract is where that table gets built.
        # A kind line's Gold is derived from its name (root ADR-0193 §9), and so is its rebuild entry.
        import gold_rebuild
        lanes = json.loads((PLATFORM / "config/r2-connections.contract.json").read_text(encoding="utf-8"))["by_pnu_lanes"]
        contract = gold_rebuild.load_contract()
        kinds = by_pnu_kinds.kinds()
        served = [kinds[name].gold_table if name in kinds else lane["gold_table"] for name, lane in lanes.items()]
        self.assertTrue(served)
        self.assertEqual(sorted(set(served) - set(contract["tables"])), [])
        self.assertEqual(len(served), len(set(served)))


if __name__ == "__main__":
    unittest.main()
