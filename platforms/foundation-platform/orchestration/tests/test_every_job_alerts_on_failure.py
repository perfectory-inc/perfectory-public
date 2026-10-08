"""Every scheduled job tells Slack when it fails, by one mechanism.

Until 2026-10-08 only three units carried `OnFailure=foundation-unit-failed@%n.service`; the Gold
rebuild, the by-PNU bake, data quality and six other jobs failed with nobody told (a red run in
Airflow, which nobody watches), while four scripts posted their own "failed at line N" from an ERR
trap. A job's unit is now what reports its failure, so the list of jobs in
orchestration/jobs.v1.json is the list that alerts.
"""

import json
import pathlib
import re
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
JOBS = json.loads((PLATFORM / "orchestration" / "jobs.v1.json").read_text(encoding="utf-8"))["jobs"]
NOTICE = "OnFailure=foundation-unit-failed@%n.service"


def unit_file(service: str) -> pathlib.Path:
    # An instance (`foundation-map-edit-fold@admin.service`) is its template's file.
    return PLATFORM / "infra" / "systemd" / re.sub(r"@[^.]+\.service$", "@.service", service)


def unit_section(text: str) -> str:
    return text.split("[Service]", 1)[0]


class EveryJobAlertsOnFailure(unittest.TestCase):
    def test_every_job_unit_reports_its_failure(self):
        missing = [
            job["id"]
            for job in JOBS
            if NOTICE not in unit_section(unit_file(job["systemd_service"]).read_text(encoding="utf-8")).splitlines()
        ]
        self.assertEqual(missing, [], f"these jobs fail silently; add {NOTICE} to their [Unit]")

    def test_no_job_script_posts_its_own_generic_failure(self):
        # The unit already says it failed; a script's own "failed at line N" says it twice.
        ops = PLATFORM / "scripts" / "ops"
        posting = [
            script.name
            for script in sorted(ops.glob("*.sh"))
            if re.search(r"notify_slack \"🔴[^\"]*(실패|FAILED)[^\"]*(line|줄) \$", script.read_text(encoding="utf-8"))
        ]
        self.assertEqual(posting, [])


if __name__ == "__main__":
    unittest.main()
