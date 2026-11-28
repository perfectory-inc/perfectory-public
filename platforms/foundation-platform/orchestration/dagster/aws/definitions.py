"""The AWS code location (root ADR-0616 A1-A3; ADR-0623): Dagster on ECS, and the collectors that have moved in.

The ai-server location (../definitions.py) observes Airflow and starts host units through the forced command;
none of that is ported (root ADR-0616 §0), so it is never loaded here. Every collector here is made by one factory
from its lane in the contract (collect.lanes); collect_lane.py runs it:

- it starts by its condition, never by a plain schedule: the tick of the beat its source contract declares, kept
  until it is handled, and only when no run of it is in progress (ADR-0623 §1);
- its own automation sensor carries its run's tags: the lane's role and size class, and the kind and lane tags
  the run queue limits by (ADR-0616 §2-9);
- a run that read its source to the end reports the check `collected` passed, and materializes the lane's asset
  only when it wrote something new (ADR-0616 §6: "nothing new" is a passed check, not a materialization); a lane
  that asked nothing because it is not ready (vendor inert, a source key not placed, no state to start from)
  reports `ready` failed and names what it waits for (ADR-0623 §4);
- a collector of a source that drops what ended is held to its reads (root ADR-0614): the same `source_read/<job>`
  asset and bound as on the host (source_read_checks), here with a FreshnessPolicy the daemon evaluates, so no
  schedule starts a task to check it; alerts.py reports a FAIL.
"""

import datetime
import pathlib
import sys

from dagster import (AssetCheckResult, AssetCheckSeverity, AssetCheckSpec, AssetKey, AssetSpec, AutomationCondition,
                     AutomationConditionSensorDefinition, DefaultSensorStatus, Definitions, Failure, FreshnessPolicy,
                     MaterializeResult, multi_asset)

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))
import alerts  # noqa: E402
import collect_lane  # noqa: E402
import source_read_checks  # noqa: E402

GROUP = "collect"
READY = "ready"
COLLECTED = "collected"


def start_condition(lane):
    """At its source's beat, kept until it is handled, and only when no run of the lane is in progress."""
    cron, timezone = collect_lane.beat(lane)
    return (AutomationCondition.cron_tick_passed(cron, cron_timezone=timezone).since_last_handled()
            & ~AutomationCondition.in_progress())


def asset_key(job_id):
    return AssetKey(["collect", job_id])


def lane_definitions(job_id, lane, platform, job_list):
    """The asset and the automation sensor of collector `job_id`."""
    key = asset_key(job_id)

    @multi_asset(name=job_id,
                 specs=[AssetSpec(key, group_name=GROUP, skippable=True, description=collect_lane.description(job_id, lane, job_list),
                                  kinds={"bronze"}, automation_condition=start_condition(lane),
                                  metadata={"handover": lane["handover"], "command": lane["command"]})],
                 check_specs=[AssetCheckSpec(READY, asset=key, description=(
                                  "The lane had what it needs: the vendor's token, its source keys, a state to start "
                                  "from (ADR-0623 §4).")),
                              AssetCheckSpec(COLLECTED, asset=key, description=(
                                  "The run read its source to the end (changed or unchanged) and saved its state."))])
    def collector(context):
        import boto3

        secrets = boto3.client("secretsmanager", region_name=platform["region"])
        vendor = boto3.client("lambda", region_name=platform["region"])
        try:
            result = collect_lane.collect(job_id, context.run_id, secrets, vendor, context.log.info)
        except collect_lane.NotReady as waiting:
            context.log.warning(f"{job_id}: not ready: {waiting}")
            yield AssetCheckResult(check_name=READY, asset_key=key, passed=False, severity=AssetCheckSeverity.ERROR,
                                   metadata={"waits for": str(waiting)})
            # Dagster wants every declared check of the step: this run read nothing, which the readiness says.
            yield AssetCheckResult(check_name=COLLECTED, asset_key=key, passed=False, severity=AssetCheckSeverity.WARN,
                                   metadata={"not run": str(waiting)})
            return
        except collect_lane.RunFailed as error:
            yield AssetCheckResult(check_name=READY, asset_key=key, passed=True)
            yield AssetCheckResult(check_name=COLLECTED, asset_key=key, passed=False, severity=AssetCheckSeverity.ERROR,
                                   metadata={"failed": str(error)})
            raise Failure(str(error)) from None
        yield AssetCheckResult(check_name=READY, asset_key=key, passed=True)
        word = result["outcome"]
        read = source_read_checks.read(job_id, word)
        if read is not None:
            context.log_event(read)
        metadata = {name: value for name, value in result.items() if value is not None}
        yield AssetCheckResult(check_name=COLLECTED, asset_key=key, passed=word in source_read_checks.READ
                               or word in ("waiting", "skipped"), metadata=metadata)
        context.log.info(f"{job_id}: {word}")
        if word == collect_lane.MATERIALIZES:
            yield MaterializeResult(asset_key=key, metadata=metadata)

    sensor = AutomationConditionSensorDefinition(
        f"{job_id}_automation", target=[key], run_tags=collect_lane.run_tags(platform, job_id, lane),
        default_status=DefaultSensorStatus.RUNNING,
        description=f"Starts {job_id} by its condition (ADR-0623 §1) with its run's tags.")
    return collector, sensor


def read_assets(job_ids, job_list):
    """`source_read/<job>` of each lane whose source drops what ended, at the job's landed bound."""
    held = {"jobs": [entry for entry in job_list["jobs"] if entry["id"] in job_ids]}
    return [spec.replace_attributes(freshness_policy=FreshnessPolicy.time_window(
                fail_window=datetime.timedelta(hours=spec.metadata["landed_within_hours"])))
            for spec in source_read_checks.assets(held)]


def definitions():
    platform = collect_lane.contract()
    job_list = collect_lane.jobs()
    lanes = collect_lane.lanes_here(platform, job_list)
    assets, sensors = [], []
    for job_id, lane in sorted(lanes.items()):
        asset, sensor = lane_definitions(job_id, lane, platform, job_list)
        assets.append(asset)
        sensors.append(sensor)
    collectors = Definitions(assets=[*assets, *read_assets(set(lanes), job_list)], sensors=sensors)
    return Definitions.merge(collectors, alerts.definitions())


defs = definitions()
