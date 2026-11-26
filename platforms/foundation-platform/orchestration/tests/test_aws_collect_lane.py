"""A collector on AWS asks the vendor through its own alias and runs as the host's unit did (root ADR-0623).

orchestration/dagster/aws/collect_lane.py with the vendor, Secrets Manager and the publisher replaced: a lane
carries no time number; it starts daily at its source's full-walk hour; its run carries its own role, under the
run path, and the platform's ceiling; the run asks the vendor through the lane's alias with its run id, `pin`
first, then `credentials`; the publisher reads the pin with no catalog token; an inert vendor leaves the run
doing nothing; and the outcome line decides. No real value of any secret appears here.
"""

import io
import json
import pathlib
import sys
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "orchestration" / "dagster" / "aws"))
import collect_lane  # noqa: E402

JOB = "industrial_land_sale_notice_ilis_collect"
PIN = {"account_id": "synthetic-account", "bucket": "foundation-platform-lakehouse-prod",
       "tables": {"silver.industrial_land_sale_notice_read": {"snapshot_id": 42, "snapshot_timestamp_ms": 1,
                                                              "manifest_list_location": "s3://b/t/m.avro",
                                                              "metadata_location": "s3://b/t/1.metadata.json"}}}
CREDENTIALS = {"account_id": "synthetic-account", "bucket": "foundation-platform-lakehouse-prod",
               "expiration": "2026-11-26T09:00:00Z",
               "writer": {"access_key_id": "wk", "secret_access_key": "ws", "session_token": "wt", "expiration": "x"},
               "reader": {"access_key_id": "rk", "secret_access_key": "rs", "session_token": "rt", "expiration": "x"}}


class Vendor:
    """A Lambda client whose function answers per call (or fails)."""

    def __init__(self, answers, error=None):
        self.answers, self.error, self.asked = answers, error, []

    def invoke(self, FunctionName, InvocationType, Payload):  # noqa: N803 (boto3's names)
        body = json.loads(Payload)
        self.asked.append((FunctionName, InvocationType, body))
        response = {"Payload": io.BytesIO(json.dumps(self.answers[body["call"]]).encode())}
        if self.error:
            response["FunctionError"] = self.error
        return response


class Secrets:
    def get_secret_value(self, SecretId):  # noqa: N803 (boto3's name)
        assert SecretId == collect_lane.contract()["database"]["foundation"]["runtime_secret"], SecretId
        return {"SecretString": json.dumps({"host": "db.internal", "port": 5432, "dbname": "foundation",
                                            "username": "foundation_api", "password": "p@ss/word"})}


class TheLaneCarriesNoTimeNumber(unittest.TestCase):
    def setUp(self):
        self.platform = collect_lane.contract()
        self.lane = self.platform["collect"]["lanes"][JOB]

    def test_no_lane_states_a_time(self):
        for job_id, lane in self.platform["collect"]["lanes"].items():
            for name in lane:
                self.assertFalse(any(word in name for word in ("minute", "second", "hour", "schedule", "cron", "ttl")),
                                 f"{job_id}.{name}")

    def test_it_starts_daily_at_its_source_s_full_walk_hour(self):
        hour = collect_lane.source_contract(self.lane)["full_walk_hour_utc"]
        self.assertEqual(collect_lane.start_cron(self.lane), f"0 {hour} * * *")

    def test_every_lane_is_a_collect_job_of_the_job_list_with_a_landed_bound(self):
        for job_id in self.platform["collect"]["lanes"]:
            spec = collect_lane.job(job_id)
            self.assertEqual(spec["dagster_group"], "collect", job_id)
            self.assertTrue(spec["expected_outcome"]["landed_within_hours"], job_id)

    def test_its_run_carries_its_own_role_under_the_run_path_and_the_platform_ceiling(self):
        tags = collect_lane.run_tags(self.platform, JOB, self.lane)
        role = json.loads(tags["ecs/task_overrides"])["taskRoleArn"]
        self.assertEqual(role, f"arn:aws:iam::{self.platform['account']['id']}:role/perfectory-platform/run/"
                               f"perfectory-platform-{self.lane['role']}")
        self.assertEqual(tags["dagster/max_runtime"], "86400")
        self.assertEqual((tags[collect_lane.KIND_TAG], tags[collect_lane.LANE_TAG]), ("collect", JOB))
        size = self.platform["collect"]["size_classes"][self.lane["size"]]
        self.assertEqual((tags["ecs/cpu"], tags["ecs/memory"]), (str(size["cpu"]), str(size["memory"])))


class TheRunAsksTheVendor(unittest.TestCase):
    def setUp(self):
        self.lane = collect_lane.contract()["collect"]["lanes"][JOB]

    def test_through_its_own_alias_with_its_run_id_pin_first(self):
        vendor = Vendor({"pin": PIN, "credentials": CREDENTIALS})
        collect_lane.vended(vendor, "perfectory-platform-r2-vend", JOB, "pin", "run-1")
        collect_lane.vended(vendor, "perfectory-platform-r2-vend", JOB, "credentials", "run-1")
        self.assertEqual([(name, body["call"], body["run_id"], body["lane"]) for name, _, body in vendor.asked],
                         [(f"perfectory-platform-r2-vend:{JOB}", "pin", "run-1", JOB),
                          (f"perfectory-platform-r2-vend:{JOB}", "credentials", "run-1", JOB)])

    def test_a_refusal_or_a_failed_vendor_fails_the_run(self):
        with self.assertRaises(collect_lane.RunFailed):
            collect_lane.vended(Vendor({"pin": {"refused": "invoked without a lane's alias"}}), "f", JOB, "pin", "r")
        with self.assertRaises(collect_lane.RunFailed):
            collect_lane.vended(Vendor({"pin": {"errorType": "KeyError"}}, error="Unhandled"), "f", JOB, "pin", "r")

    def test_the_publisher_reads_the_pin_with_no_catalog_token(self):
        env = collect_lane.environment(self.lane, PIN, CREDENTIALS, "postgres://u:p@h/foundation", pathlib.Path("/tmp/x"))
        self.assertEqual(env["FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER"], "vended")
        self.assertEqual(json.loads(env["FOUNDATION_PLATFORM_LAKEHOUSE_VENDED_TABLES"]), PIN["tables"])
        self.assertFalse([name for name in env if "CATALOG" in name and "TOKEN" in name])
        self.assertEqual((env["FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SESSION_TOKEN"],
                          env["FOUNDATION_PLATFORM_LAKEHOUSE_READER_SESSION_TOKEN"]), ("wt", "rt"))
        self.assertEqual(env[self.lane["output_dir_env"]], str(pathlib.Path("/tmp/x")))
        without_reader = collect_lane.environment(self.lane, PIN, {**CREDENTIALS, "reader": None}, "u", pathlib.Path("/x"))
        self.assertNotIn("FOUNDATION_PLATFORM_LAKEHOUSE_READER_ACCESS_KEY_ID", without_reader)

    def test_the_database_url_is_the_runtime_role_over_tls(self):
        self.assertEqual(collect_lane.database_url(Secrets(), collect_lane.contract()),
                         "postgres://foundation_api:p%40ss%2Fword@db.internal:5432/foundation?sslmode=require")

    def test_an_inert_vendor_leaves_the_run_doing_nothing_and_asks_no_credentials(self):
        said, vendor = [], Vendor({"pin": {"inert": "r2-mint-token"}})
        self.assertEqual(collect_lane.collect(JOB, "run-1", Secrets(), vendor, said.append, program="never-run"),
                         ("waiting", {}, "r2-mint-token"))
        self.assertEqual([body["call"] for _, _, body in vendor.asked], ["pin"])
        self.assertIn("inert", said[0])


SCRIPT = """import sys
for line in sys.argv[2:]:
    print(line)
sys.exit(int(sys.argv[1]))
"""


class TheOutcomeLineDecides(unittest.TestCase):
    def run_with(self, status, *lines):
        with tempfile.TemporaryDirectory() as work:
            script = pathlib.Path(work) / "publisher.py"
            script.write_text(SCRIPT, encoding="utf-8")
            original = collect_lane.subprocess.Popen

            def popen(args, **kwargs):
                return original([*args, str(status), *lines], **kwargs)

            collect_lane.subprocess.Popen = popen
            try:
                return collect_lane.run({"command": str(script)}, {}, lambda line: None, sys.executable)
            finally:
                collect_lane.subprocess.Popen = original

    def test_changed_and_the_quiet_words_succeed(self):
        for word in ("changed", "unchanged", "waiting", "skipped"):
            lines = self.run_with(0, 'industrial-land-sale-notice-summary-json {"read": 3}', f"foundation-job-outcome {word} why")
            self.assertEqual(collect_lane.outcome(lines), word)
        self.assertEqual(collect_lane.summary(lines, "industrial-land-sale-notice-summary-json "), {"read": 3})

    def test_no_outcome_line_counts_as_changed(self):
        self.assertEqual(collect_lane.outcome(self.run_with(0, "done")), "changed")

    def test_failed_or_a_non_zero_exit_fails_the_run(self):
        with self.assertRaises(collect_lane.RunFailed):
            self.run_with(0, "foundation-job-outcome failed stalled")
        with self.assertRaises(collect_lane.RunFailed):
            self.run_with(3, "foundation-job-outcome changed")


if __name__ == "__main__":
    unittest.main()
