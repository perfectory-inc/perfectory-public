"""The AWS code location (root ADR-0616 A1, A2; ADR-0623): Dagster on ECS, and the collectors that have moved in.

The ai-server location (../definitions.py) observes Airflow and starts host units through the forced
command; none of that is ported (root ADR-0616 §0), so it is never loaded here. Each collector moves in as
one asset of this location, one at a time (§7's hand-over rule), as collect_lane.py runs it:

- it starts by its condition, never by a plain schedule: the daily tick at its source's full-walk hour,
  kept until it is handled, and only when no run of it is in progress (ADR-0623 §1);
- its own automation sensor carries its run's tags: the lane's role and size class, the platform's run
  ceiling, and the kind and lane tags the run queue limits by;
- its health is Dagster's own: the read asset `source_read/<job>` it materializes when it read its source
  to the end, held to the job's landed bound by a FreshnessPolicy the daemon evaluates (no schedule, no
  run), and a check that fails while the credential vendor is inert (ADR-0623 §4).
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
import collect_lane  # noqa: E402
import source_read_checks  # noqa: E402

GROUP = "collect"
VENDOR_CHECK = "vendor_placed"


def start_condition(lane):
    """Once a day, kept until it is handled, and only when no run of the lane is in progress."""
    return (AutomationCondition.cron_tick_passed(collect_lane.start_cron(lane), cron_timezone="UTC").since_last_handled()
            & ~AutomationCondition.in_progress())


def lane_definitions(job_id, lane, platform):
    """The asset and the automation sensor of collector `job_id`."""
    key = AssetKey(lane["asset_key"])
    spec = collect_lane.job(job_id)

    @multi_asset(name=job_id,
                 specs=[AssetSpec(key, group_name=GROUP, skippable=True, description=spec["description"],
                                  automation_condition=start_condition(lane))],
                 check_specs=[AssetCheckSpec(VENDOR_CHECK, asset=key,
                                             description="The credential vendor holds its tokens (ADR-0623 §4).")])
    def collector(context):
        import boto3

        secrets = boto3.client("secretsmanager", region_name=platform["region"])
        vendor = boto3.client("lambda", region_name=platform["region"])
        try:
            word, summary, inert = collect_lane.collect(job_id, context.run_id, secrets, vendor, context.log.info)
        except collect_lane.RunFailed as error:
            raise Failure(str(error)) from None
        yield AssetCheckResult(check_name=VENDOR_CHECK, asset_key=key, passed=inert is None,
                               severity=AssetCheckSeverity.ERROR,
                               metadata={} if inert is None else {"not placed": inert})
        read = source_read_checks.read(job_id, word)
        if read is not None:
            context.log_event(read)
        context.log.info(f"{job_id}: {word}")
        if word == collect_lane.MATERIALIZES:
            yield MaterializeResult(asset_key=key, metadata={"summary": summary})

    sensor = AutomationConditionSensorDefinition(
        f"{job_id}_automation", target=[key], run_tags=collect_lane.run_tags(platform, job_id, lane),
        default_status=DefaultSensorStatus.RUNNING,
        description=f"Starts {job_id} by its condition (ADR-0623 §1) with its run's tags.")
    return collector, sensor


def read_assets(job_ids):
    """`source_read/<job>` of each lane, held to its landed bound by Dagster's own FreshnessPolicy."""
    bounds = source_read_checks.bounds(collect_lane.jobs())
    return [AssetSpec(source_read_checks.read_key(job_id), group_name=source_read_checks.GROUP, kinds={"source"},
                      description=f"{job_id} read its source to the end (root ADR-0614, ADR-0623 §4)",
                      metadata={"job": job_id, "landed_within_hours": bounds[job_id]},
                      freshness_policy=FreshnessPolicy.time_window(fail_window=datetime.timedelta(hours=bounds[job_id])))
            for job_id in sorted(job_ids) if job_id in bounds]


def definitions():
    platform = collect_lane.contract()
    lanes = platform["collect"]["lanes"]
    assets, sensors = [], []
    for job_id, lane in lanes.items():
        asset, sensor = lane_definitions(job_id, lane, platform)
        assets.append(asset)
        sensors.append(sensor)
    return Definitions(assets=[*assets, *read_assets(lanes)], sensors=sensors)


defs = definitions()
