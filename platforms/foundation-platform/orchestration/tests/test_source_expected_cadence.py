"""Every collected endpoint declares its provider's release cadence (root ADR-0443).

2026-11-07 the readiness sweep found the three ingestion alerts silent (their query named a column
the schema lacks) and that, with series, one 24-hour line would page about 85 of 105 healthy
sources: most providers publish monthly or yearly and Bronze is written only when a release lands.
foundation-api renders these declarations as the bounds the rules compare; a collected endpoint with
none would be watched by nothing, so it is refused here.
"""

import json
import pathlib
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
CATALOG = json.loads((PLATFORM / "docs/catalog/public-source-endpoint-catalog.v1.json").read_text(encoding="utf-8"))
FILE_DATA = json.loads((PLATFORM / "infra/lakehouse/contracts/data-go-kr-file-data.contract.json").read_text(encoding="utf-8"))
ONBID = json.loads((PLATFORM / "infra/lakehouse/contracts/data-go-kr-onbid.contract.json").read_text(encoding="utf-8"))
JOBS = json.loads((PLATFORM / "orchestration/jobs.v1.json").read_text(encoding="utf-8"))["jobs"]
RELEASES = {"continuous", "daily", "monthly", "bimonthly", "quarterly", "yearly", "irregular"}


def collected():
    return [e for e in CATALOG["endpoints"] if not e.get("collection_decision")]


def every_hours(value):
    """Every every_hours the onbid contract names, wherever it nests them."""
    if isinstance(value, dict):
        return ([value["every_hours"]] if "every_hours" in value else []) + [h for v in value.values() for h in every_hours(v)]
    if isinstance(value, list):
        return [h for v in value for h in every_hours(v)]
    return []


class ExpectedCadenceTest(unittest.TestCase):
    def test_every_collected_endpoint_declares_a_cadence_and_no_other_does(self):
        self.assertIn("collected_endpoint_declares_expected_cadence", CATALOG["rules"])
        for endpoint in CATALOG["endpoints"]:
            with self.subTest(endpoint=endpoint["endpoint_slug"]):
                cadence = endpoint.get("expected_cadence")
                if endpoint.get("collection_decision"):
                    self.assertIsNone(cadence, "an endpoint that is not collected has no cadence to watch")
                    continue
                self.assertIsNotNone(cadence)
                self.assertIn(cadence["release"], RELEASES)
                self.assertTrue(cadence["basis_ko"].strip(), "a bound names the evidence it came from")
                age = cadence["max_age_hours"]
                if cadence["release"] == "irregular":
                    self.assertIsNone(age, "a provider with no schedule gives no age bound")
                else:
                    self.assertIsInstance(age, int)
                    self.assertGreater(age, 0)
                run = cadence.get("max_run_minutes")
                self.assertTrue(run is None or (isinstance(run, int) and run > 0))

    def test_the_file_data_cadence_is_the_contracts_published_word(self):
        for endpoint in collected():
            if endpoint["group"] != "data_go_kr_file_data":
                continue
            with self.subTest(endpoint=endpoint["endpoint_slug"]):
                word = FILE_DATA["files"][endpoint["dataset_slug"]]["published"]
                self.assertEqual(endpoint["expected_cadence"]["release"], "irregular" if word == "irregularly" else word)

    def test_an_onbid_bound_outlasts_two_of_its_slowest_collection_intervals(self):
        hours = every_hours(ONBID)
        self.assertTrue(hours, "the onbid contract names each operation's every_hours")
        for endpoint in collected():
            if endpoint["group"] == "onbid_open_api":
                with self.subTest(endpoint=endpoint["endpoint_slug"]):
                    self.assertGreaterEqual(endpoint["expected_cadence"]["max_age_hours"], 2 * max(hours))

    def test_a_run_bound_is_within_the_collecting_jobs_time_limit(self):
        court = next(job for job in JOBS if job["id"] == "court_auction_collect")
        for endpoint in collected():
            if endpoint["group"] == "court_auction":
                self.assertEqual(endpoint["expected_cadence"]["max_run_minutes"], court["time_limit_minutes"])


if __name__ == "__main__":
    unittest.main()
