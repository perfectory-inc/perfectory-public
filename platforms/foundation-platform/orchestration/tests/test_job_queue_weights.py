"""A job that writes what another job of its pool reads is queued ahead of it (root ADR-0558).

Moved out of test_job_specs.py, which reached the 1,500-line limit, when the industrial land sale
notice lane (root ADR-0603) added its jobs to the circles below.
"""

import json
import pathlib
import sys
import unittest

ORCHESTRATION = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ORCHESTRATION / "dags"))

import job_specs  # noqa: E402


class AProducerOutranksItsConsumers(unittest.TestCase):
    """Root ADR-0558. 2026-11-11 12:15Z..: data_go_kr_file_data (weight 1) waited over 16 hours in the
    spark pool's queue while gold_parcel_nearby_rebuild (weight 2), which reads the Silver tables only
    it loads, ran ahead of it and failed on tables that did not exist yet."""

    def test_no_job_of_the_real_list_waits_behind_a_job_that_reads_what_it_writes(self):
        jobs = json.loads(job_specs.JOBS.read_text(encoding="utf-8"))
        graph = json.loads(job_specs.GRAPH.read_text(encoding="utf-8"))
        feeders = job_specs.producers(jobs, graph)
        by_id = {entry["id"]: entry for entry in jobs["jobs"]}
        weights = job_specs.queue_weights(jobs, feeders)
        circles = [{"map_edit_fold_admin", "map_edit_fold_complex"},  # they share one ledger
                   # The collector asks the read records which notices are settled; the Silver load reads what it wrote.
                   {"industrial_complex_notice_collect", "industrial_complex_notice_silver"},
                   # The same, with one read record for the sale notices of four sources (root ADR-0603):
                   # each collector reads only its own source's reads, but the graph names one table.
                   {"industrial_land_sale_notice_factoryon_collect", "industrial_land_sale_notice_ilis_collect",
                    "industrial_land_sale_notice_kicox_collect", "industrial_land_sale_notice_lh_collect",
                    "industrial_land_sale_notice_silver"}]
        pairs = [(writer, reader) for reader, writers in feeders.items() for writer in writers
                 if by_id[writer]["pool"] == by_id[reader]["pool"]
                 and not any({writer, reader} <= circle for circle in circles)]
        self.assertGreater(len(pairs), 100)
        self.assertEqual([pair for pair in pairs if weights[pair[0]] <= weights[pair[1]]], [])
        self.assertEqual(weights["map_edit_fold_admin"], weights["map_edit_fold_complex"])
        # The incident's pair no longer shares a pool (the file data load left Spark, root ADR-0559): the
        # rule above holds for every pair that does. What Airflow is handed:
        specs = {spec.job_id: spec.priority_weight for spec in job_specs.load_specs()}
        self.assertEqual(specs, weights)

    def test_a_weight_is_the_jobs_own_tier_until_a_reader_in_its_pool_raises_it(self):
        def listed(job_id, weight, pool="spark"):
            return {"id": job_id, "pool": pool, "priority_weight": weight}

        jobs = {"jobs": [listed("load", 1), listed("gold", 20), listed("bake", 10), listed("alone", 40),
                         listed("elsewhere", 90, "default_pool"), listed("fold_a", 50), listed("fold_b", 5),
                         listed("after_folds", 70)]}
        feeders = {"load": [], "gold": ["load"], "bake": ["gold"], "alone": [], "elsewhere": ["load", "alone"],
                   "fold_a": ["fold_b"], "fold_b": ["fold_a"], "after_folds": ["fold_b"]}
        self.assertEqual(job_specs.queue_weights(jobs, feeders),
                         {"load": 21, "gold": 20, "bake": 10, "alone": 40, "elsewhere": 90,
                          "fold_a": 71, "fold_b": 71, "after_folds": 70})



class AnInstanceReadsItsDropIns(unittest.TestCase):
    def test_an_instance_drop_in_sets_its_time_limit_over_its_template(self):
        # Root ADR-0574 §8: the template states its longest instance's limit, a shorter one its drop-in.
        template = job_specs.service_unit_text("foundation-industrial-land-sale-notice-collect@factoryon.service")
        hourly = job_specs.service_unit_text("foundation-industrial-land-sale-notice-collect@ilis.service")
        self.assertIn("\nTimeoutStartSec=4h\n", template)
        self.assertIn("\nTimeoutStartSec=40m\n", hourly)
        self.assertNotIn("TimeoutStartSec=4h", hourly)
        self.assertIn("industrial-land-sale-notice-collect.sh ilis\n", hourly)


if __name__ == "__main__":
    unittest.main()
