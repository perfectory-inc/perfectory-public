"""The order Dagster's queue starts job runs in (job_specs.dagster_priority, root ADR-0435, ADR-0558): a job's
priority class first, then its queue weight, so inside a class a job that writes what another reads starts
first, as Airflow orders its pool's tasks (queue_weights). 2026-11-20 the Gold group moved to Dagster (root
ADR-0583), whose queue had read the class alone."""

import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "dags"))

import job_specs  # noqa: E402


class DagstersQueueOrder(unittest.TestCase):
    def test_dagsters_queue_starts_a_writer_before_what_reads_it_and_a_class_before_its_weight(self):
        # Root ADR-0558 as Airflow kept it (queue_weights), now in `dagster/priority` (dagster_priority).
        specs = {spec.job_id: spec for spec in job_specs.load_specs()}
        pairs = 0
        for reader in specs.values():
            for writer in reader.producers:
                writer = specs[writer]
                if writer.pool == reader.pool and writer.priority == reader.priority and writer.job_id != reader.job_id:
                    pairs += 1
                    self.assertGreaterEqual(job_specs.dagster_priority(writer), job_specs.dagster_priority(reader),
                                            (writer.job_id, reader.job_id))
        self.assertGreater(pairs, 0)
        by_class = {}
        for spec in specs.values():
            by_class.setdefault(spec.priority, []).append(job_specs.dagster_priority(spec))
        for lower, higher in zip(sorted(by_class), sorted(by_class)[1:], strict=False):
            self.assertLess(max(by_class[lower]), min(by_class[higher]))


if __name__ == "__main__":
    unittest.main()
