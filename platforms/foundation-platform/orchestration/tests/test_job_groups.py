"""The groups jobs move to Dagster in (root ADR-0583 §1–§2): each a switch of its own, downstream first. The
switched groups are a prefix of job_specs.DAGSTER_GROUPS, and a group is never half switched; read from the
real job list, so each switch PR changes only the definitions."""

import copy
import json
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "dags"))

import job_specs  # noqa: E402


def real_inputs():
    return (json.loads(job_specs.JOBS.read_text(encoding="utf-8")), json.loads(job_specs.GRAPH.read_text(encoding="utf-8")))


def switched_groups(jobs):
    """The groups whose switched-on jobs Dagster decides (all of them or none, §2)."""
    return {job["dagster_group"] for job in jobs["jobs"] if job["enabled"] and job.get("dagster_stage") == "dagster"}


class TheGroups(unittest.TestCase):
    def test_the_switched_groups_are_the_first_in_their_order_and_every_later_one_is_compared(self):
        # Root ADR-0583 §1: the groups move downstream first. The switched ones are a prefix of
        # DAGSTER_GROUPS with every switched-on job at `dagster`; every later group's are still decided by
        # Airflow and compared (§4 retires the comparison later).
        stages = {}
        for job in real_inputs()[0]["jobs"]:
            if job["enabled"]:
                stages.setdefault(job["dagster_group"], set()).add(job.get("dagster_stage", "airflow"))
        switched = [group for group in job_specs.DAGSTER_GROUPS if stages.get(group) == {"dagster"}]
        self.assertEqual(switched, list(job_specs.DAGSTER_GROUPS[:len(switched)]))
        self.assertTrue(switched)
        for group in job_specs.DAGSTER_GROUPS[len(switched):]:
            with self.subTest(group=group):
                self.assertLessEqual(stages.get(group, set()), {"airflow", *job_specs.COMPARED})

    def test_a_group_moves_in_one_switch(self):
        # Root ADR-0583 §2: in a group every switched-on job is at `dagster` or none is; the stages Airflow
        # decides (airflow, shadow, proxied) are one side. The last group is half moved here, to the side it is not on.
        jobs = copy.deepcopy(real_inputs()[0])
        self.assertEqual(job_specs.dagster_groups(jobs), [])
        group = job_specs.DAGSTER_GROUPS[-1]
        members = [job for job in jobs["jobs"] if job["dagster_group"] == group and job["enabled"]]
        other = "shadow" if group in switched_groups(jobs) else "dagster"
        members[0]["dagster_stage"] = other
        self.assertEqual(len(job_specs.dagster_groups(jobs)), 1)
        self.assertIn(f"group {group}", job_specs.dagster_groups(jobs)[0])
        for job in members:
            job["dagster_stage"] = other
        self.assertEqual(job_specs.dagster_groups(jobs), [])


if __name__ == "__main__":
    unittest.main()
