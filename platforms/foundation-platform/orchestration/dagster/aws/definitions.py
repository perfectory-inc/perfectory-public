"""The AWS code location (root ADR-0616 A1-A3; ADR-0623): Dagster on ECS, and the collectors that have moved in.

The ai-server location (../definitions.py) observes Airflow and starts host units through the forced command;
none of that is ported (root ADR-0616 §0), so it is never loaded here. Every collector here is made by one factory
from its lane in the contract (collect.lanes); collect_lane.py runs it:

- it starts by its condition, never by a plain schedule: the tick of the beat its source contract declares, kept
  until it is handled, and only when no run of it is in progress (ADR-0623 §1); a load (`started_by: inputs`, a
  Silver or Gold lane) is eager() on the lanes here that produce its inputs (§1-2), its assets are the tables it
  writes (`iceberg/<namespace>/<table>`, root ADR-0626 §1), and a collector that has never changed anything does
  not hold it back;
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
LOADED = "loaded"


def start_condition(lane):
    """A collector: at its source's beat, kept until it is handled, and only when no run of the lane is in progress.
    A load: eager() (ADR-0623 §1-2) without its wait for missing inputs, since a collector that only ever found
    nothing new never materializes, and that must not stop the load of the others' objects."""
    if collect_lane.started_by_inputs(lane):
        # Dagster 1.13.27's own form (docs: customizing eager, "allow_missing_upstreams").
        return (AutomationCondition.eager().without(~AutomationCondition.any_deps_missing())
                .with_label("eager_allow_missing"))
    cron, timezone = collect_lane.beat(lane)
    return (AutomationCondition.cron_tick_passed(cron, cron_timezone=timezone).since_last_handled()
            & ~AutomationCondition.in_progress())


def asset_key(job_id):
    return AssetKey(["collect", job_id])


def table_key(table):
    namespace, name = table.split(".", 1)
    return AssetKey(["iceberg", namespace, name])


def lane_keys(job_id, lane, declaration):
    """The lane's assets: a collector's own key; a load's tables (its write grants in the Lakekeeper declaration)."""
    if not collect_lane.started_by_inputs(lane):
        return [asset_key(job_id)]
    tables = declaration["lanes"].get(job_id, {}).get("write", [])
    if not tables:
        raise ValueError(f"{job_id}: a load lane writes no table of the Lakekeeper catalog (lakekeeper-declaration)")
    return [table_key(table) for table in tables]


def lane_definitions(job_id, lane, platform, job_list, declaration=None):
    """The assets and the automation sensor of lane `job_id`."""
    load = collect_lane.started_by_inputs(lane)
    keys = lane_keys(job_id, lane, declaration or {"lanes": {}})
    key = keys[0]
    group = collect_lane.kind(job_id, lane, job_list)
    deps = [asset_key(other) for other in collect_lane.producers(job_id, platform, job_list)] if load else []
    done = LOADED if load else COLLECTED

    @multi_asset(name=job_id,
                 specs=[AssetSpec(each, group_name=group, skippable=True, deps=deps,
                                  description=collect_lane.description(job_id, lane, job_list),
                                  kinds={"iceberg"} if load else {"bronze"}, automation_condition=start_condition(lane),
                                  metadata={"handover": lane["handover"], "command": lane["command"]})
                        for each in keys],
                 check_specs=[AssetCheckSpec(READY, asset=key, description=(
                                  "The lane had what it needs: the vendor's token, its source keys, its catalog "
                                  "client, a state to start from (ADR-0623 §4).")),
                              AssetCheckSpec(done, asset=key, description=(
                                  "The run loaded what its inputs owed (changed or unchanged)." if load else
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
            yield AssetCheckResult(check_name=done, asset_key=key, passed=False, severity=AssetCheckSeverity.WARN,
                                   metadata={"not run": str(waiting)})
            return
        except collect_lane.RunFailed as error:
            yield AssetCheckResult(check_name=READY, asset_key=key, passed=True)
            yield AssetCheckResult(check_name=done, asset_key=key, passed=False, severity=AssetCheckSeverity.ERROR,
                                   metadata={"failed": str(error)})
            raise Failure(str(error)) from None
        yield AssetCheckResult(check_name=READY, asset_key=key, passed=True)
        word = result["outcome"]
        read = None if load else source_read_checks.read(job_id, word)
        if read is not None:
            context.log_event(read)
        metadata = {name: value for name, value in result.items() if value is not None}
        yield AssetCheckResult(check_name=done, asset_key=key, passed=word in source_read_checks.READ
                               or word in ("waiting", "skipped"), metadata=metadata)
        context.log.info(f"{job_id}: {word}")
        if word == collect_lane.MATERIALIZES:
            for each in keys:
                yield MaterializeResult(asset_key=each, metadata=metadata)

    sensor = AutomationConditionSensorDefinition(
        f"{job_id}_automation", target=keys, run_tags=collect_lane.run_tags(platform, job_id, lane, job_list),
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
    declaration = collect_lane.lakekeeper_declaration()
    assets, sensors = [], []
    for job_id, lane in sorted(lanes.items()):
        asset, sensor = lane_definitions(job_id, lane, platform, job_list, declaration)
        assets.append(asset)
        sensors.append(sensor)
    collectors = Definitions(assets=[*assets, *read_assets(set(lanes), job_list)], sensors=sensors)
    return Definitions.merge(collectors, alerts.definitions())


defs = definitions()
