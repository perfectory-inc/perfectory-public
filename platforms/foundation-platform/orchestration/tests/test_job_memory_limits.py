"""A job states its unit's memory beside its time limit, and an instance of a template gets its own (root ADR-0574 §8).

What real incident does failing this prevent? 2026-11-22: foundation-gold-versions@parcel-real-transaction
(W81e) needed other limits than the template's other instances, and the 33 silver-refresh@ lanes all held
16G because they share one template (land-use-plan peaked at 1 GB, unit-areas at 15.4 GB): one MemoryMax
per unit file could not say it, and the memory lived in a second place, apart from the job.
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import unittest

REPOSITORY = pathlib.Path(__file__).resolve().parents[4]


def load(name: str, relative: str):
    spec = importlib.util.spec_from_file_location(name, REPOSITORY / relative)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


declared = load("declared_definitions_for_job_memory", "scripts/catalog/declared_definitions.py")

TEMPLATE = "foundation-x@.service"


def job(job_id: str, service: str, memory_max: str | None, **more) -> dict:
    item = {"id": job_id, "systemd_service": service, "time_limit_minutes": 60}
    if memory_max is not None:
        item.update({"memory_max": memory_max, "memory_measured": f"fixture {job_id}"})
    item.update(more)
    return item


def documents(jobs: list[dict], platform_units: dict | None = None) -> dict:
    return {
        declared.JOBS: {"jobs": jobs},
        declared.MEMORY: {"systemd_units": {"unit_files": dict(platform_units or {})}},
    }


class TheJobStatesItsUnitsMemory(unittest.TestCase):
    def test_a_template_holds_its_largest_instance_and_a_smaller_one_gets_its_own(self):
        built = documents([job("big", "foundation-x@big.service", "8G"),
                           job("small", "foundation-x@small.service", "2G", memory_measured_peak="1G"),
                           job("plain", "foundation-y.service", "512M")])
        declared.job_memory(built)
        section = built[declared.MEMORY]["systemd_units"]
        self.assertEqual(section["unit_files"][TEMPLATE],
                         {"class": "batch", "memory_max": "8G", "measured": "fixture big"})
        self.assertEqual(section["unit_files"]["foundation-y.service"]["memory_max"], "512M")
        self.assertEqual(section["instances"], {"foundation-x@small.service": {
            "class": "batch", "memory_max": "2G", "measured": "fixture small", "measured_peak": "1G"}})

    def test_instances_at_the_templates_cap_need_no_entry_of_their_own(self):
        built = documents([job("a", "foundation-x@a.service", "8G"), job("b", "foundation-x@b.service", "8G")])
        declared.job_memory(built)
        self.assertEqual(built[declared.MEMORY]["systemd_units"]["instances"], {})

    def test_a_job_unit_the_platform_also_declares_is_refused(self):
        built = documents([job("a", "foundation-y.service", "512M")],
                          {"foundation-y.service": {"class": "batch", "memory_max": "1G", "measured": "x"}})
        with self.assertRaisesRegex(declared.DefinitionError, "its memory is the job's"):
            declared.job_memory(built)

    def test_a_job_without_its_memory_is_refused(self):
        with self.assertRaisesRegex(declared.DefinitionError, "memory_max and memory_measured"):
            declared.job_memory(documents([job("a", "foundation-y.service", None)]))


    def test_jobs_that_run_one_service_must_state_one_memory(self):
        # EGM 2026-11-22: two jobs on one instance left the smaller cap, whichever came last.
        built = documents([job("big", "foundation-x@big.service", "8G"),
                           job("one", "foundation-x@shared.service", "2G"),
                           job("two", "foundation-x@shared.service", "4G")])
        with self.assertRaisesRegex(declared.DefinitionError, "run it and state different memory"):
            declared.job_memory(built)


if __name__ == "__main__":
    unittest.main()
