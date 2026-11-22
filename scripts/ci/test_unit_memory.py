"""scripts/ci/unit_memory.py renders, checks and removes an instance's memory drop-in (root ADR-0574 §8).

Run by scripts/guard/every-container-has-a-memory-cap-self-test.sh, the guard unit_memory.py belongs to.

What real incident does failing this prevent? 2026-11-22: the 33 silver-refresh@ lanes all held 16G
because they share one template (land-use-plan peaked at 1 GB, unit-areas at 15.4 GB), and
foundation-gold-versions@parcel-real-transaction (W81e) could not state its own limits: one MemoryMax
per unit file could not say it.
"""

from __future__ import annotations

import copy
import json
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import unit_memory  # noqa: E402

TEMPLATE = "foundation-x@.service"
UNIT = """[Unit]
Description=fixture

[Service]
Type=oneshot
ExecStart=/bin/true
"""


def job(job_id: str, service: str, memory_max: str, **more) -> dict:
    item = {"id": job_id, "systemd_service": service, "time_limit_minutes": 60,
            "memory_max": memory_max, "memory_measured": f"fixture {job_id}"}
    item.update(more)
    return item


class EveryJobsMemoryIsChecked(unittest.TestCase):
    def test_a_job_at_its_templates_cap_is_held_to_its_own_measured_peak(self):
        # EGM 2026-11-22: jobs that tie at the template's cap share one entry, so only that job's
        # measured peak met the 1.5 x rule; the 33 silver-refresh@ lanes at 16G are this case.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            (root / "units").mkdir()
            (root / "units" / TEMPLATE).write_text(UNIT, encoding="utf-8")
            (root / "jobs.v1.json").write_text(json.dumps({"jobs": [
                job("a", "foundation-x@a.service", "8G", memory_measured_peak="1G"),
                job("b", "foundation-x@b.service", "8G", memory_measured_peak="7G")]}), encoding="utf-8")
            contract = {"systemd_units": {
                "units": "units", "jobs": "jobs.v1.json", "memory_max_over_peak": 1.5, "memory_high_of_max": 0.9,
                "floor": "128M", "classes": {"batch": {"oom_score_adjust": 500}},
                "unit_files": {TEMPLATE: {"class": "batch", "memory_max": "8G", "measured": "fixture a",
                                          "measured_peak": "1G"}},
                "instances": {}}}
            unit_memory.write(root, contract)
            found = unit_memory.problems(root, contract)
            self.assertEqual(len(found), 1, found)
            self.assertIn("job b: memory_max 8G is under 1.5 x the measured peak 7G", found[0])


class AnInstanceGetsItsDropIn(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.directory.name)
        units = self.root / "units"
        units.mkdir()
        (units / TEMPLATE).write_text(UNIT, encoding="utf-8")
        (self.root / "jobs.v1.json").write_text(json.dumps({"jobs": [
            job("big", "foundation-x@big.service", "8G"), job("small", "foundation-x@small.service", "2G")]}),
            encoding="utf-8")
        self.contract = {"systemd_units": {
            "units": "units", "jobs": "jobs.v1.json", "memory_max_over_peak": 1.5, "memory_high_of_max": 0.9,
            "floor": "128M", "classes": {"batch": {"oom_score_adjust": 500}},
            "unit_files": {TEMPLATE: {"class": "batch", "memory_max": "8G", "measured": "fixture big"}},
            "instances": {"foundation-x@small.service": {"class": "batch", "memory_max": "2G", "measured": "fixture small"}},
        }}
        self.dropin = units / "foundation-x@small.service.d" / unit_memory.DROPIN

    def tearDown(self):
        self.directory.cleanup()

    def test_the_drop_in_is_written_checked_and_removed_with_its_instance(self):
        missing = unit_memory.problems(self.root, self.contract)
        self.assertTrue(any("foundation-x@small.service.d/50-memory.conf: not what" in problem for problem in missing), missing)
        unit_memory.write(self.root, self.contract)
        self.assertEqual(unit_memory.problems(self.root, self.contract), [])
        self.assertEqual(self.dropin.read_text(encoding="utf-8"),
                         f"{unit_memory.MARK}\n[Service]\nMemoryHigh=1843M\nMemoryMax=2G\n")
        self.assertIn("MemoryMax=8G", (self.root / "units" / TEMPLATE).read_text(encoding="utf-8"))
        gone = copy.deepcopy(self.contract)
        gone["systemd_units"]["instances"] = {}
        self.assertTrue(any("no job's instance holds less" in problem for problem in unit_memory.problems(self.root, gone)))
        unit_memory.write(self.root, gone)
        self.assertFalse(self.dropin.parent.exists())
        self.assertEqual(unit_memory.problems(self.root, gone), [])

    def test_an_instance_that_holds_its_templates_cap_or_more_is_refused(self):
        unit_memory.write(self.root, self.contract)
        larger = copy.deepcopy(self.contract)
        larger["systemd_units"]["instances"]["foundation-x@small.service"]["memory_max"] = "8G"
        self.assertTrue(any("holds no less than its template" in problem
                            for problem in unit_memory.problems(self.root, larger)))


if __name__ == "__main__":
    unittest.main()
