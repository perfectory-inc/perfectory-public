"""What a release changes and which jobs write it (scripts/deploy/release_changes.py, root ADR-0270 deploy D3)."""

import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts/deploy/release_changes.py"

CONTRACTS = {"silver.a": {"columns": [{"name": "k"}]}, "silver.b": {"columns": [{"name": "k"}]}}
GRAPH = {
    "nodes": [{"id": "src"}, {"id": "a", "table_name": "silver.a"}, {"id": "b", "table_name": "silver.b"}],
    "edges": [{"id": "src-to-a", "from": "src", "to": "a"}, {"id": "src-to-b", "from": "src", "to": "b"}],
}
STEPS = {"state_directory": "/var/lib/perfectory/deploy-steps", "steps": {"lakehouse-migrate": {
    "inputs": ["infra/lakehouse/contracts", "infra/lakehouse/spark/jobs"],
    "per_table": {"infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json": "contracts"}}}}
JOBS = {"jobs": [{"id": "write_a", "pipeline_graph_edges": ["src-to-a"]},
                 {"id": "write_b", "pipeline_graph_edges": ["src-to-b"]},
                 {"id": "maintenance", "pipeline_graph_edges": []}]}


class ReleaseChanges(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="release-changes-")
        self.addCleanup(temp.cleanup)
        self.running, self.new = pathlib.Path(temp.name) / "running", pathlib.Path(temp.name) / "new"
        for root in (self.running, self.new):
            self.write(root, "infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json", {"contracts": CONTRACTS})
            self.write(root, "orchestration/jobs.v1.json", JOBS)
            self.write(root, "docs/catalog/pipeline-graph.v1.json", GRAPH)
            self.write(root, "config/deploy-steps.contract.json", STEPS)
            self.write(root, "infra/lakehouse/spark/jobs/migrate.py", "program")
            (root / "migrations").mkdir()
            (root / "migrations/0001_first.sql").write_text("select 1;\n")

    @staticmethod
    def write(root, relative, value):
        (root / relative).parent.mkdir(parents=True, exist_ok=True)
        (root / relative).write_text(json.dumps(value), encoding="utf-8")

    def run_script(self, running=None, new=None):
        result = subprocess.run([sys.executable, "-I", "-B", str(SCRIPT), str(running or self.running), str(new or self.new)],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.splitlines()

    def test_a_changed_table_names_the_jobs_that_write_it_and_those_the_graph_cannot_place(self):
        changed = {**CONTRACTS, "silver.a": {"columns": [{"name": "k"}, {"name": "added"}]}}
        self.write(self.new, "infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json", {"contracts": changed})
        self.assertEqual(self.run_script(), [
            "lakehouse tables this release changes: silver.a",
            "postgres migrations this release adds: none",
            "jobs that write them: write_a",
            "jobs whose writes the pipeline graph does not name: maintenance",
            "jobs the deploy holds until the release is live: maintenance, write_a",
        ])

    def test_an_added_or_removed_table_is_a_change(self):
        self.write(self.new, "infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json",
                   {"contracts": {"silver.a": CONTRACTS["silver.a"], "silver.c": {"columns": []}}})
        self.assertEqual(self.run_script()[0], "lakehouse tables this release changes: silver.b, silver.c")

    def test_a_postgres_migration_makes_every_job_a_writer(self):
        (self.new / "migrations/0002_second.sql").write_text("select 2;\n")
        self.assertEqual(self.run_script(), [
            "lakehouse tables this release changes: none",
            "postgres migrations this release adds: 0002_second.sql",
            "jobs that write them (a postgres migration: every job): write_a, write_b",
            "jobs whose writes the pipeline graph does not name: maintenance",
            "jobs the deploy holds until the release is live: every job",
        ])

    def test_a_release_that_changes_nothing_names_no_job(self):
        self.assertEqual(self.run_script(), ["lakehouse tables this release changes: none",
                                             "postgres migrations this release adds: none",
                                             "jobs the deploy holds until the release is live: none"])

    def test_the_real_tree_reads(self):
        # The committed inputs have the shape this reads: the release against itself changes nothing.
        self.assertEqual(self.run_script(PLATFORM, PLATFORM), ["lakehouse tables this release changes: none",
                                                                "postgres migrations this release adds: none",
                                                                "jobs the deploy holds until the release is live: none"])
        copy = self.new
        shutil.rmtree(copy)
        steps = json.loads((PLATFORM / "config/deploy-steps.contract.json").read_text(encoding="utf-8"))
        for relative in ("orchestration/jobs.v1.json", "docs/catalog/pipeline-graph.v1.json",
                         "config/deploy-steps.contract.json", "migrations",
                         *steps["steps"]["lakehouse-migrate"]["inputs"]):
            (copy / relative).parent.mkdir(parents=True, exist_ok=True)
            (shutil.copytree if (PLATFORM / relative).is_dir() else shutil.copy)(PLATFORM / relative, copy / relative)
        contracts = json.loads((copy / "infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json").read_text(encoding="utf-8"))
        contracts["contracts"]["silver.building_register_floors"]["columns"].append(
            {"name": "added", "logical_type": "string", "required": False})
        (copy / "infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json").write_text(json.dumps(contracts))
        lines = self.run_script(PLATFORM, copy)
        self.assertEqual(lines[0], "lakehouse tables this release changes: silver.building_register_floors")
        self.assertIn("building_register_floor", lines[2].split(": ", 1)[1].split(", "))

    def test_the_hold_file_names_the_held_jobs_one_per_line_or_every_job(self):
        hold = self.running.parent / "hold"

        def held():
            subprocess.run([sys.executable, "-I", "-B", str(SCRIPT), str(self.running), str(self.new),
                            "--hold-file", str(hold)], check=True, capture_output=True, timeout=30)
            return hold.read_text(encoding="utf-8")

        self.assertEqual(held(), "", "nothing changes: nothing is held")
        changed = {**CONTRACTS, "silver.b": {"columns": []}}
        self.write(self.new, "infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json", {"contracts": changed})
        self.assertEqual(held(), "maintenance\nwrite_b\n")
        (self.new / "migrations/0002_second.sql").write_text("select 2;\n")
        self.assertEqual(held(), "*\n", "a postgres migration holds every job")

    # Root ADR-0308 review (2026-10-29): the hold read one file of the migration's inputs, so a change
    # to its program or a backfill ran the migration over tables whose writers were not held.

    def test_any_other_input_of_the_migration_holds_the_writers_of_every_table(self):
        (self.new / "infra/lakehouse/spark/jobs/migrate.py").write_text("changed")
        lines = self.run_script()
        self.assertTrue(lines[0].startswith("lakehouse tables this release changes: every table (2): "
                                            "infra/lakehouse/spark/jobs/migrate.py changed"), lines[0])
        self.assertEqual(lines[-1], "jobs the deploy holds until the release is live: maintenance, write_a, write_b")

    def test_a_new_input_file_is_a_change_and_a_readme_is_not(self):
        (self.new / "infra/lakehouse/contracts/README.md").write_text("prose")
        self.assertEqual(self.run_script()[0], "lakehouse tables this release changes: none")
        self.write(self.new, "infra/lakehouse/contracts/lakehouse-known-drift.json", {"tables": []})
        self.assertIn("every table", self.run_script()[0])

    def host_run(self, host):
        result = subprocess.run([sys.executable, "-I", "-B", str(SCRIPT), str(self.running), str(self.new), "--host"],
                                env={**os.environ, "FOUNDATION_DEPLOY_STEPS_HOST_ROOT": str(host)},
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.splitlines()

    def test_on_the_host_the_running_release_is_the_baseline_only_once_it_was_migrated_here(self):
        host = self.running.parent / "host"
        (host / "var/lib/perfectory").mkdir(parents=True)
        (host / "var/lib/perfectory").chmod(0o755)
        lines = self.host_run(host)
        self.assertIn("every table (2): the running release is not what this host last migrated", lines[0])
        sys.path.insert(0, str(SCRIPT.parent))
        import deploy_steps
        deploy_steps.ran(self.running, "lakehouse-migrate", host)
        self.assertEqual(self.host_run(host)[0], "lakehouse tables this release changes: none")

    def test_two_trees_are_its_arguments(self):
        result = subprocess.run([sys.executable, "-I", "-B", str(SCRIPT), str(self.new)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 64)


if __name__ == "__main__":
    unittest.main()
