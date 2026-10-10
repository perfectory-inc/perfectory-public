"""The Silver refresh's unit, script, release and lane wiring (root ADR-0169).

What a run decides (the ledger's newest complete release, the table's own record of it) is the
publisher's and is tested there (`remote_lakehouse_job/silver_refresh/tests.rs`). This holds the
pieces around it together: the unit runs the script for its instance and cleans up after it, the
lanes' state lives in one place on the data disk, every lane contract is a pipeline-graph edge the
refresh carries out, and the script refuses a malformed call before it touches anything.
"""

import json
import pathlib
import re
import shutil
import subprocess
import sys
import unittest

ORCHESTRATION = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ORCHESTRATION / "dags"))

import job_specs  # noqa: E402

PLATFORM = job_specs.PLATFORM_ROOT
UNIT = PLATFORM / "infra/systemd/foundation-silver-refresh@.service"
SCRIPT = PLATFORM / "scripts/ops/silver-refresh.sh"
RELEASE = PLATFORM / "scripts/deploy/foundation-release.sh"
CONTRACTS = PLATFORM / "infra/lakehouse/contracts"
GRAPH = PLATFORM / "docs/catalog/pipeline-graph.v1.json"
SCRIPT_VIA = "platforms/foundation-platform/scripts/ops/silver-refresh.sh"


def unit_lines(key):
    return re.findall(rf"^{key}=(.*)$", UNIT.read_text(encoding="utf-8"), re.MULTILINE)


def lane_contracts():
    for path in sorted(CONTRACTS.glob("*.json")):
        document = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(document, dict) and "silver_refresh" in document:
            yield path.name, document


class TheUnit(unittest.TestCase):
    def test_it_runs_the_script_for_its_instance_and_cleans_up_after_every_exit(self):
        self.assertEqual(unit_lines("ExecStart"), ["/opt/foundation-platform/current/scripts/ops/silver-refresh.sh %i"])
        self.assertEqual(unit_lines("ExecStopPost"), ["/opt/foundation-platform/current/scripts/ops/silver-refresh.sh cleanup"])
        self.assertEqual(unit_lines("OnFailure"), ["foundation-unit-failed@%n.service"])
        self.assertEqual(unit_lines("Type"), ["oneshot"])

    def test_the_export_it_runs_is_memory_bounded(self):
        self.assertRegex(" ".join(unit_lines("MemoryMax")), r"^[1-9][0-9]*G$")

    def test_the_lanes_state_is_one_path_on_the_data_disk(self):
        # The script's default, the unit's only writable path and the directory the release makes
        # are one fact in three places; a drift is a unit systemd refuses to start, or a run that
        # writes where the unit cannot.
        script = re.search(r"SILVER_REFRESH_STATE_ROOT:-([^}]+)\}", SCRIPT.read_text(encoding="utf-8")).group(1)
        self.assertTrue(script.startswith("/data/"), "the root disk is small")
        self.assertEqual(unit_lines("ReadWritePaths"), [script])
        release = RELEASE.read_text(encoding="utf-8")
        self.assertRegex(release, rf"install -d -o foundation-platform -g foundation-platform -m 2770 {re.escape(script)}\n")

    def test_the_release_admits_the_unit_because_its_lanes_are_jobs(self):
        # foundation-release.sh installs the release admission of every listed job's service,
        # switched on or not (root ADR-0171 registered the lanes; before, it named the template).
        release = RELEASE.read_text(encoding="utf-8")
        self.assertIn('install_release_admission "${service}"', release)
        services = [job["systemd_service"] for job in json.loads(job_specs.JOBS.read_text(encoding="utf-8"))["jobs"]]
        lanes = [service for service in services if service.startswith("foundation-silver-refresh@")]
        self.assertEqual(len(lanes), len(list(lane_contracts())))
        self.assertEqual({job_specs.service_unit_file(service) for service in lanes}, {UNIT})


class TheLanes(unittest.TestCase):
    def test_every_lane_contract_is_a_scheduled_job_started_by_the_sweep(self):
        # One job per lane contract (root ADR-0171); the lane names are the publisher's, not a list
        # here: the job's service instance and its one edge both follow from the contract's table.
        graph = json.loads(GRAPH.read_text(encoding="utf-8"))
        tables = {node["id"]: node.get("table_name") for node in graph["nodes"]}
        edges = {edge["id"]: edge for edge in graph["edges"]}
        jobs = [job for job in json.loads(job_specs.JOBS.read_text(encoding="utf-8"))["jobs"]
                if job["systemd_service"].startswith("foundation-silver-refresh@")]
        written = sorted(tables[edges[edge]["to"]] for job in jobs for edge in job["pipeline_graph_edges"])
        self.assertEqual(written, sorted(document["silver_refresh"]["table"] for _, document in lane_contracts()))
        specs = {spec.job_id: spec for spec in job_specs.load_specs()}
        for job in jobs:
            with self.subTest(job["id"]):
                self.assertEqual(specs[job["id"]].started_by, "inputs")
                self.assertEqual(specs[job["id"]].producers, ("source_sweep",))

    def test_every_lane_contract_is_a_graph_edge_the_refresh_carries_out(self):
        graph = json.loads(GRAPH.read_text(encoding="utf-8"))
        tables = {node["id"]: node.get("table_name") for node in graph["nodes"]}
        found = list(lane_contracts())
        # The five hub lanes of root ADR-0169 step 2 and the seven VWorld land lanes of step 3.
        self.assertEqual(sorted(name.split("-")[0] for name, _ in found), ["hub"] * 5 + ["vworld"] * 7)
        for name, document in found:
            with self.subTest(contract=name):
                lane = document["silver_refresh"]
                # A hub lane reads roles of one ledger month; a land lane one source by its members.
                origin, slugs = (("source-building-hub-bulk", set(lane["roles"].values())) if "roles" in lane
                                 else ("source-vworld-dataset", {lane["source"]}))
                edges = [edge for edge in graph["edges"] if tables.get(edge["to"]) == lane["table"]
                         and edge["from"] == origin]
                self.assertEqual(len(edges), 1, f"{lane['table']} has one source edge")
                via = edges[0]["via"]
                self.assertIn(lane["export"], via)
                self.assertIn(SCRIPT_VIA, via)
                self.assertNotIn("platforms/foundation-platform/scripts/load/land-use-batch-load.sh", via)
                self.assertLessEqual(set(edges[0].get("source_slugs", [])), slugs)
                self.assertTrue(edges[0].get("source_slugs"), "the edge names the slugs it reads")

    def test_no_lane_contract_names_its_release(self):
        for name, document in lane_contracts():
            with self.subTest(contract=name):
                self.assertFalse({"selected_vintage", "objects", "granularity_counts"} & set(document))
                self.assertFalse({"selected_vintage", "objects"} & set(document["silver_refresh"]))


@unittest.skipIf(shutil.which("bash") is None, "needs bash")
class TheScript(unittest.TestCase):
    def run_script(self, *args, env=None):
        return subprocess.run([shutil.which("bash"), str(SCRIPT), *args], capture_output=True, text=True,
                              timeout=60, env=env, check=False)

    def test_a_malformed_call_is_refused_before_anything_runs(self):
        for args in ([], ["building_register_titles"], ["Building-Register-Titles"], ["a", "b"],
                     ["building-register-titles", "--force"], ["cleanup", "extra"]):
            with self.subTest(args=args):
                result = self.run_script(*args)
                self.assertEqual(result.returncode, 64, result.stderr)

    def test_cleanup_outside_systemd_names_what_it_needs(self):
        result = self.run_script("cleanup", env={"PATH": "/usr/bin:/bin"})
        self.assertEqual(result.returncode, 64)
        self.assertIn("INVOCATION_ID", result.stderr)

    def test_a_run_outside_an_installed_release_is_refused(self):
        # Past the arguments the script binds itself to the admitted release (root ADR-0134 §3);
        # this checkout is not one, so the run stops there (65) and never reaches the publisher.
        for args in (["building-register-titles"], ["building-register-titles", "--plan"]):
            with self.subTest(args=args):
                result = self.run_script(*args)
                self.assertEqual(result.returncode, 65, result.stderr)
                self.assertIn("not inside an installed release", result.stderr)


FAKE_PUBLISHER = r"""#!/usr/bin/env python3
import os, sys
assert sys.argv[1:] == ["run-silver-refresh"], sys.argv
lane = os.environ["FOUNDATION_PLATFORM_SILVER_REFRESH_LANE"]
scenario = os.environ["FAKE_SCENARIO"]
print(f"silver-refresh lane={lane} release=209907 identity=x: exporting", flush=True)
if scenario == "fails":
    sys.exit(3)
outcome = {"changed": "changed reason=exported_and_loaded", "unchanged": "unchanged reason=already_loaded"}.get(scenario)
if os.environ["FOUNDATION_PLATFORM_SILVER_REFRESH_PLAN"] == "1":
    print(f"silver-refresh-plan lane={lane} outcome=would_change reason=x release=209907 identity=x rows=0")
elif outcome:
    print(f"silver-refresh-outcome lane={lane} outcome={outcome} release=209907 identity=x rows=7")
"""


@unittest.skipIf(shutil.which("bash") is None or shutil.which("flock") is None, "needs bash and flock")
class TheJobOutcome(unittest.TestCase):
    """A lane's run ends with the line every scheduled job ends with (root ADR-0171 §3)."""

    RELEASE_ID = "e" * 40

    def setUp(self):
        import hashlib
        import os
        import tempfile

        self.temp = tempfile.TemporaryDirectory(prefix="silver-refresh-")
        self.addCleanup(self.temp.cleanup)
        root = pathlib.Path(self.temp.name)
        base = root / "opt/foundation-platform"
        release = base / "releases" / self.RELEASE_ID
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("silver-refresh.sh", "admitted-writer-runtime.sh"):
            (release / "scripts/ops" / name).write_bytes((PLATFORM / "scripts/ops" / name).read_bytes())
            (release / "scripts/ops" / name).chmod(0o755)
        (base / "current").symlink_to(pathlib.Path("releases") / self.RELEASE_ID)
        artifacts = base / "artifacts" / self.RELEASE_ID
        artifacts.mkdir(parents=True)
        publisher = artifacts / "foundation-outbox-publisher"
        publisher.write_text(FAKE_PUBLISHER)
        publisher.chmod(0o555)
        (artifacts / "build.json").write_text(json.dumps({
            "source": self.RELEASE_ID, "publisher_image": "sha256:" + "a" * 64, "tippecanoe_image": "sha256:" + "d" * 64,
            "files": {"foundation-outbox-publisher": hashlib.sha256(publisher.read_bytes()).hexdigest(),
                      "jars/fixture.jar": "0" * 64}}))
        self.script = base / "current/scripts/ops/silver-refresh.sh"
        self.env = {"PATH": os.environ["PATH"], "INVOCATION_ID": "a" * 32, "DATABASE_URL": "postgres://fixture/fixture",
                    "FOUNDATION_PLATFORM_SILVER_REFRESH_STATE_ROOT": str(root / "state")}
        (root / "state").mkdir()

    def run_lane(self, scenario, *args):
        return subprocess.run(["bash", str(self.script), "building-register-titles", *args],
                              env={**self.env, "FAKE_SCENARIO": scenario}, capture_output=True, text=True,
                              timeout=60, check=False)

    def test_changed_and_unchanged_end_the_run(self):
        for scenario in ("changed", "unchanged"):
            with self.subTest(scenario):
                result = self.run_lane(scenario)
                self.assertEqual(result.returncode, 0, result.stderr)
                lines = result.stdout.splitlines()
                self.assertTrue(lines[0].startswith("silver-refresh lane=building-register-titles"), "passed through")
                self.assertTrue(lines[-2].startswith(f"silver-refresh-outcome lane=building-register-titles outcome={scenario} "))
                self.assertEqual(lines[-1], f"foundation-job-outcome {scenario}")
                self.assertEqual(job_specs.job_outcome(result.stdout), scenario)

    def test_a_failed_run_keeps_its_status_and_states_no_outcome(self):
        result = self.run_lane("fails")
        self.assertEqual(result.returncode, 3)
        self.assertNotIn("foundation-job-outcome", result.stdout)

    def test_a_plan_is_not_a_run(self):
        result = self.run_lane("changed", "--plan")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("foundation-job-outcome", result.stdout)
        self.assertIn("outcome=would_change", result.stdout)


if __name__ == "__main__":
    unittest.main()
