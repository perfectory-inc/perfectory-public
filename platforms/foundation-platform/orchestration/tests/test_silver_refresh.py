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
        self.assertIn("install_release_admission foundation-silver-refresh@.service", release)


class TheLanes(unittest.TestCase):
    def test_every_lane_contract_is_a_graph_edge_the_refresh_carries_out(self):
        graph = json.loads(GRAPH.read_text(encoding="utf-8"))
        tables = {node["id"]: node.get("table_name") for node in graph["nodes"]}
        found = list(lane_contracts())
        self.assertEqual(len(found), 5, "the hub lanes of root ADR-0169 step 2")
        for name, document in found:
            with self.subTest(contract=name):
                lane = document["silver_refresh"]
                edges = [edge for edge in graph["edges"] if tables.get(edge["to"]) == lane["table"]
                         and edge["from"] == "source-building-hub-bulk"]
                self.assertEqual(len(edges), 1, f"{lane['table']} has one hub edge")
                via = edges[0]["via"]
                self.assertIn(lane["export"], via)
                self.assertIn(SCRIPT_VIA, via)
                self.assertNotIn("platforms/foundation-platform/scripts/load/land-use-batch-load.sh", via)
                self.assertLessEqual(set(edges[0].get("source_slugs", [])), set(lane["roles"].values()))

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


if __name__ == "__main__":
    unittest.main()
