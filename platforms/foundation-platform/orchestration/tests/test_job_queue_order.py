"""The order Dagster's queue starts job runs in (job_specs.dagster_priority, root ADR-0435, ADR-0605): a job's
priority class and nothing else, so inside a class the queue is first in, first out (Dagster's own order
among equal priorities), and a run that follows a failed run of its job queues in the `default` class at most.

2026-11-23..24: a per-job weight inside the class kept nine Gold rebuilds behind a tile bake that failed four
times, two hours each, for 15 to 17 hours on the spark pool's one slot."""

import dataclasses
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "dags"))

import job_specs  # noqa: E402


class DagstersQueueOrder(unittest.TestCase):
    def setUp(self):
        self.specs = job_specs.load_specs()

    def test_a_runs_priority_is_its_class_so_a_class_is_first_in_first_out(self):
        # No job has a place of its own inside its class: two jobs of one class carry one priority, and
        # Dagster then starts the one queued first.
        by_class = {}
        for spec in self.specs:
            self.assertEqual(job_specs.dagster_priority(spec), spec.priority, spec.job_id)
            self.assertEqual(spec.dagster_tags()[job_specs.PRIORITY_TAG], str(spec.priority))
            by_class.setdefault(spec.priority, set()).add(spec.priority_weight)
        self.assertTrue(any(len(weights) > 1 for weights in by_class.values()),
                        "the list still holds jobs of one class with different weights; the queue ignores them")

    def test_a_run_after_a_failure_queues_in_the_default_class_at_most(self):
        above = [spec for spec in self.specs if spec.priority > spec.default_priority]
        below = [spec for spec in self.specs if spec.priority < spec.default_priority]
        self.assertTrue(above, "no job of a class above default: this test proves nothing")
        for spec in above:
            self.assertEqual(job_specs.dagster_priority(spec, after_failure=True), spec.default_priority, spec.job_id)
            self.assertEqual(spec.dagster_tags("triggered", after_failure=True)[job_specs.PRIORITY_TAG],
                             str(spec.default_priority))
        # A failure never raises a job: a backfill stays below the default class.
        for spec in below:
            self.assertEqual(job_specs.dagster_priority(spec, after_failure=True), spec.priority, spec.job_id)
        ordinary = next(spec for spec in self.specs if spec.priority == spec.default_priority)
        self.assertEqual(job_specs.dagster_priority(ordinary, after_failure=True), ordinary.priority)

    def test_the_default_class_is_the_lists_own(self):
        spec = dataclasses.replace(self.specs[0], priority=1000, default_priority=7)
        self.assertEqual(job_specs.dagster_priority(spec, after_failure=True), 7)


if __name__ == "__main__":
    unittest.main()
