"""What on AWS reaches the alarm channel from Dagster itself (root ADR-0616 §6, W107).

Two built-in sensors of this code location publish to the account's one alarm topic (infra/aws/dagster.tf),
which AWS Chatbot posts to one Slack channel:

- a run that failed: its job, its run id and this location (`run_failure_sensor`);
- an asset of this location whose freshness state became FAIL: its key (the latest freshness state Dagster's
  freshness daemon keeps for an asset with a `FreshnessPolicy`, `get_freshness_state_records`; the cursor
  holds, per asset, the time of the last state already looked at, so a FAIL is sent once).

The topic's rule (coordinator 2026-11-26, infra/aws/chatbot.tf): Chatbot may store and process a
notification in a Region outside ap-northeast-2, so a message carries names and ids only, never an error
text, a log line or a row. A run's failure message can quote the source's answer or a run's last log lines
(dagster-aws quotes them), so it is not sent: the run id leads to it in Dagster.
The message is Chatbot's custom notification (docs: "Custom notifications", schema version 1.0).
"""

import json
import os

from dagster import (AssetKey, DefaultSensorStatus, Definitions, RunFailureSensorContext, SensorEvaluationContext,
                     SkipReason, run_failure_sensor, sensor)

TOPIC_ENVIRONMENT = "DAGSTER_ALARM_TOPIC_ARN"
LOCATION = "foundation-aws"


def notification(title, description, keywords):
    """Chatbot's custom notification: version 1.0, source custom, markdown title and description."""
    return {"version": "1.0", "source": "custom",
            "content": {"textType": "client-markdown", "title": title, "description": description, "keywords": keywords}}


def sns():
    """The SNS client of the task's own role (the code server's, which may publish to the topic only)."""
    import boto3

    return boto3.client("sns")


def publish(message):
    """One notification to the alarm topic. No topic configured (a location run outside AWS): nothing is sent."""
    topic = os.environ.get(TOPIC_ENVIRONMENT)
    if not topic:
        return False
    sns().publish(TopicArn=topic, Message=json.dumps(message, ensure_ascii=False))
    return True


def run_failed(job_name, run_id):
    return notification(":rotating_light: Dagster run failed",
                        f"job `{job_name}` · run `{run_id}` · location `{LOCATION}`",
                        ["dagster", "run-failure", job_name])


def freshness_failed(asset_key):
    key = "/".join(asset_key.path)
    return notification(":hourglass: Asset is not fresh",
                        f"asset `{key}` is FAIL · location `{LOCATION}`",
                        ["dagster", "freshness", key])


@run_failure_sensor(name="alarm_on_run_failure", default_status=DefaultSensorStatus.RUNNING,
                    monitor_all_code_locations=True)
def alarm_on_run_failure(context: RunFailureSensorContext):
    publish(run_failed(context.dagster_run.job_name, context.dagster_run.run_id))


@sensor(name="alarm_on_freshness_failure", default_status=DefaultSensorStatus.RUNNING, minimum_interval_seconds=60)
def alarm_on_freshness_failure(context: SensorEvaluationContext):
    seen = json.loads(context.cursor) if context.cursor else {}
    keys = list(context.repository_def.asset_graph.get_all_asset_keys())
    records = context.instance.get_freshness_state_records(keys) if keys else {}
    sent = 0
    for key, record in sorted(records.items(), key=lambda item: item[0].to_user_string()):
        name, at = key.to_user_string(), record.updated_at.timestamp()
        if at <= seen.get(name, float("-inf")):
            continue
        seen[name] = at
        if record.freshness_state.value == "FAIL":
            sent += publish(freshness_failed(AssetKey(key.path)))
    context.update_cursor(json.dumps(seen, sort_keys=True))
    if not sent:
        return SkipReason(f"{len(keys)} asset(s), no freshness state newly FAIL")
    return None


def definitions():
    return Definitions(sensors=[alarm_on_run_failure, alarm_on_freshness_failure])
