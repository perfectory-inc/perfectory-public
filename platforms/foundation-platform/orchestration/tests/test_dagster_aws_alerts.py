"""A failed run and an asset that went stale reach the alarm topic as names and ids only (root ADR-0616 §6).

orchestration/dagster/aws/alerts.py against a real Dagster instance (in memory): a job that fails, a freshness
state change to FAIL and one to PASS. The topic is stood in for; what would be published is read back. The
message is AWS Chatbot's custom notification and carries no error text: the run's failure here says
`a secret-looking answer`, and nothing of it is sent. Skips where Dagster is not installed; the observer's
image runs it (scripts/verify/foundation-dagster-observe.sh).
"""

import importlib.util
import json
import os
import pathlib
import unittest
from unittest import mock

AWS = pathlib.Path(__file__).resolve().parents[1] / "dagster" / "aws"

try:
    import dagster  # noqa: F401
except ImportError:  # pragma: no cover - the default runner has no Dagster
    dagster = None


class Topic:
    def __init__(self):
        self.messages = []

    def publish(self, TopicArn, Message):  # noqa: N803 - boto3's argument names
        self.messages.append((TopicArn, json.loads(Message)))


@unittest.skipIf(dagster is None, "Dagster is not installed here")
class AlarmsCarryNamesOnly(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location("aws_alerts", AWS / "alerts.py")
        cls.alerts = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.alerts)

    def setUp(self):
        from dagster import DagsterInstance

        self.instance = DagsterInstance.ephemeral()
        self.addCleanup(self.instance.dispose)
        self.topic = Topic()
        patcher = mock.patch.dict(os.environ, {self.alerts.TOPIC_ENVIRONMENT: "arn:aws:sns:ap-northeast-2:000000000000:alarms"})
        patcher.start()
        self.addCleanup(patcher.stop)
        client = mock.patch.object(self.alerts, "sns", return_value=self.topic)
        client.start()
        self.addCleanup(client.stop)

    def test_a_failed_run_sends_its_job_and_run_id_and_no_error_text(self):
        from dagster import DagsterEventType, build_run_status_sensor_context, job, op

        @op
        def collect():
            raise RuntimeError("a secret-looking answer")

        @job
        def collector():
            collect()

        result = collector.execute_in_process(instance=self.instance, raise_on_error=False)
        failure = next(event for event in result.all_events if event.event_type == DagsterEventType.RUN_FAILURE)
        context = build_run_status_sensor_context(sensor_name="alarm_on_run_failure", dagster_event=failure,
                                                  dagster_instance=self.instance, dagster_run=result.dagster_run)
        self.alerts.alarm_on_run_failure(context.for_run_failure())
        [(topic, message)] = self.topic.messages
        self.assertEqual(topic, "arn:aws:sns:ap-northeast-2:000000000000:alarms")
        self.assertEqual((message["version"], message["source"]), ("1.0", "custom"))
        text = json.dumps(message)
        self.assertIn("collector", text)
        self.assertIn(result.run_id, text)
        self.assertNotIn("secret-looking", text)

    def change(self, key, new, previous):
        from dagster._core.definitions.freshness import FreshnessStateChange

        self.instance.report_runless_asset_event(FreshnessStateChange(
            key=key, new_state=new, previous_state=previous, state_change_timestamp=0.0))

    def tick(self, repository, cursor=None):
        from dagster import build_sensor_context

        context = build_sensor_context(instance=self.instance, cursor=cursor, repository_def=repository)
        self.alerts.alarm_on_freshness_failure(context)
        return context.cursor

    def test_an_asset_that_went_fail_is_sent_once_and_one_that_passes_is_not(self):
        from dagster import AssetKey, AssetSpec, Definitions
        from dagster._core.definitions.freshness import FreshnessState

        stale, fresh = AssetKey(["source_read", "x"]), AssetKey(["source_read", "y"])
        repository = Definitions(assets=[AssetSpec(stale), AssetSpec(fresh)],
                                 sensors=[self.alerts.alarm_on_freshness_failure]).get_repository_def()
        self.change(stale, FreshnessState.FAIL, FreshnessState.PASS)
        self.change(fresh, FreshnessState.PASS, FreshnessState.UNKNOWN)
        cursor = self.tick(repository)
        [(_, message)] = self.topic.messages
        self.assertIn("source_read/x", message["content"]["description"])
        self.tick(repository, cursor)
        self.assertEqual(len(self.topic.messages), 1, "a state already looked at is not sent again")

    def test_the_location_loads_both_sensors(self):
        names = {sensor.name for sensor in self.alerts.definitions().sensors}
        self.assertEqual(names, {"alarm_on_run_failure", "alarm_on_freshness_failure"})


if __name__ == "__main__":
    unittest.main()
