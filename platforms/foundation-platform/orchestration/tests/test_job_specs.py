"""The job list builds the DAGs the scheduler runs (root ADR-0122); these pin what it may say."""

import copy
import json
import pathlib
import re
import sys
import unittest

ORCHESTRATION = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ORCHESTRATION / "dags"))

import job_specs  # noqa: E402


def real_inputs():
    return (
        json.loads(job_specs.JOBS.read_text(encoding="utf-8")),
        json.loads(job_specs.GRAPH.read_text(encoding="utf-8")),
    )


class TheRealJobList(unittest.TestCase):
    def setUp(self):
        self.specs = job_specs.load_specs()

    def test_every_job_builds_a_dag_with_lineage(self):
        self.assertTrue(self.specs, "the job list builds no DAG")
        for spec in self.specs:
            with self.subTest(spec.dag_id):
                self.assertTrue(spec.inputs and spec.outputs, "a job must report what it reads and writes")

    def test_a_job_runs_in_exactly_one_place(self):
        # ADR-0122 §4: an enabled job's systemd timer no longer ships; a job still on systemd keeps
        # its timer. Otherwise it would run twice, or nowhere.
        timers = {path.name for path in job_specs.SYSTEMD.glob("*.timer")}
        for spec in self.specs:
            with self.subTest(spec.dag_id):
                self.assertEqual(
                    spec.systemd_timer in timers,
                    not spec.enabled,
                    "enabled in Airflow and still a systemd timer, or neither",
                )

    def test_a_shipped_timer_starts_the_service_its_job_names(self):
        for spec in self.specs:
            timer = job_specs.SYSTEMD / spec.systemd_timer
            if not timer.is_file():
                continue
            with self.subTest(spec.dag_id):
                units = re.findall(r"^Unit=(\S+)$", timer.read_text(encoding="utf-8"), flags=re.MULTILINE)
                self.assertEqual(units, [spec.systemd_service])

    def test_the_timeout_outlasts_the_service_own_limit(self):
        # Airflow stopping a job systemd would still let finish turns a slow success into a failure.
        for spec in self.specs:
            unit = job_specs.service_unit_file(spec.systemd_service).read_text(encoding="utf-8")
            limit = re.search(r"^TimeoutStartSec=(\d+)(m|h)$", unit, flags=re.MULTILINE)
            with self.subTest(spec.dag_id):
                self.assertIsNotNone(limit, "the service states its own time limit")
                minutes = int(limit.group(1)) * (60 if limit.group(2) == "h" else 1)
                self.assertGreater(spec.timeout_minutes, minutes)

    def test_dag_ids_are_unique(self):
        ids = [spec.dag_id for spec in self.specs]
        self.assertEqual(len(ids), len(set(ids)))


class WhatTheJobListMayNotSay(unittest.TestCase):
    def refused(self, mutate):
        jobs, graph = real_inputs()
        jobs = copy.deepcopy(jobs)
        mutate(jobs)
        with self.assertRaises(job_specs.JobListError):
            job_specs.load_specs(jobs, graph)

    def test_an_edge_the_pipeline_graph_does_not_have(self):
        self.refused(lambda jobs: jobs["jobs"][0]["pipeline_graph_edges"].append("no-such-edge"))

    def test_a_job_that_names_no_edge(self):
        self.refused(lambda jobs: jobs["jobs"][0].update(pipeline_graph_edges=[]))

    def test_a_pool_the_scheduler_does_not_have(self):
        self.refused(lambda jobs: jobs["jobs"][0].update(pool="big"))

    def test_a_service_the_release_does_not_ship(self):
        self.refused(lambda jobs: jobs["jobs"][0].update(systemd_service="foundation-no-such-job.service"))

    def test_a_service_name_outside_the_platform(self):
        self.refused(lambda jobs: jobs["jobs"][0].update(systemd_service="ssh.service"))

    def test_two_jobs_on_one_service(self):
        self.refused(lambda jobs: jobs["jobs"][1].update(systemd_service=jobs["jobs"][0]["systemd_service"]))

    def test_an_enabled_flag_that_is_not_a_boolean(self):
        self.refused(lambda jobs: jobs["jobs"][0].update(enabled="yes"))

    def test_a_duplicate_job_id(self):
        self.refused(lambda jobs: jobs["jobs"].append(copy.deepcopy(jobs["jobs"][0])))


if __name__ == "__main__":
    unittest.main()
