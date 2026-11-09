#!/usr/bin/env python3
"""The asset reconciler: which assets are owed a run, by state rather than by event (root ADR-0333 §7).

    asset_reconcile.py            one tick (the job asset_reconcile, every five minutes)

On 2026-10-28 at 21:38 the 공매 Gold rebuild committed and recorded asset event 3912, and the bake that
serves it was never queued: Airflow drops an event for a paused DAG and a deploy held the bake paused
(root ADR-0337). Nothing measured it; the screen would have waited 13 hours for the 10:45 fallback.
An event is a fast path that can be lost. This compares state, as a Kubernetes controller or Dagster's
declarative automation does: for each job of the reconciler's `reconciles` (orchestration/jobs.v1.json)
it makes one asset (one per lane for a bake that serves several, by_pnu_serving_bake), and

- the versions the asset's inputs are at now: a Gold input's `main` head, read from the catalog over
  REST (iceberg_metadata.py); for an input the Gold contract pins to another Gold's build
  (gold-panel-rebuild.contract.json `pins_from`) the snapshot that Gold was built from, as the
  planner pins it (gold_rebuild.plan);
- the versions its last run read: a Gold's source pins, recorded on its own commit; a by-PNU lane's
  Gold snapshot its served packs reflect, which by-pnu-serving-bake.sh appends to the lane's
  reads.jsonl whenever a run leaves the lane serving one.

An input moved when a snapshot after the read one changed rows (gold_rebuild.input_change_since_pin:
a compaction or an empty commit is no change). An asset with a moved input is owed a run, unless it
runs now, or it is a lane whose documents follow another lane (documents_follow_lane) and its last run
is waiting for that lane, which has not moved since. Each owed asset is printed

    foundation-asset-owed <job id> <key>

once per job, with a key of the versions its owed assets owe (a lane's read snapshot and, after a
failed run, its failures in a row, started again after a backoff: lane_verdict), and the reconciler's DAG starts the job once per key unless a run
of it is queued or running (orchestration/dags/foundation_jobs.py): a repeated tick or a lost event
never starts it twice, and the start waits for the job's pool like any other.

It writes, for node-exporter's textfile collector (/var/lib/foundation-jobs/metrics/asset-reconcile.prom):
foundation_asset_lag_seconds (the newest unread input change less the read version's commit time, 0
when caught up), foundation_asset_unscheduled_since_timestamp_seconds (while an owed asset is not
running, not waiting and its pool has room: since the later of its first unread change and the last
tick that saw it running or its pool full) and foundation_asset_reconcile_timestamp_seconds; and
beside it docker-networks.prom, the host's bridge networks against what Docker's address pools hold,
which every job's Compose project draws from (bridge_networks), and docker-containers.prom, each running
container's cgroup id with its name (container_names), which cAdvisor cannot see without the engine's
socket and the FoundationContainerMemoryNearCap rule looks up (root ADR-0385), and dagster.prom,
whether Dagster's code location loads and when the dual run last compared (dagster_state; the
FoundationDagster*/FoundationDualRun* rules, root ADR-0431). The
FoundationAsset* rules turn them into asset_lag_seconds and asset_unscheduled_seconds and alert. The
idle-since times are kept under the job's state path between ticks. It changes no data: its outcome
is `unchanged`, or `failed` when no asset could be read.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# The unit runs this file itself: no bytecode beside the release (2026-10 the deploy's imports did).
sys.dont_write_bytecode = True
PLATFORM = Path(__file__).resolve().parents[2]
for relative in ("orchestration/dags", "infra/lakehouse/spark/jobs", "scripts/deploy", "scripts/ops"):
    sys.path.insert(0, str(PLATFORM / relative))

import by_pnu_kinds  # noqa: E402
import gold_rebuild  # noqa: E402
import iceberg_metadata  # noqa: E402
import job_specs  # noqa: E402
import job_units  # noqa: E402
import lane_follow_gate  # noqa: E402

RECONCILER = "asset_reconcile"
STATE_FILE = "idle.json"
METRICS_NAME = "asset-reconcile.prom"
# The scheduled jobs' textfile directory node-exporter reads (jobs.v1.json state_directories, root ADR-0330 §4).
METRICS_DIR = "/var/lib/foundation-jobs/metrics"
# An operator's pause of a job (start-scheduled-job.sh `switches`, root ADR-0282 §3): one file per job id.
SWITCHES_DIR = "/var/lib/foundation-jobs/switches"
READS = "reads.jsonl"
# The by-PNU serving surface of a lane is this node of the pipeline graph (`<lane>-by-pnu-section-packs`).
PACKS_SUFFIX = "-by-pnu-section-packs"
# A failed lane is started again after this, doubling per failure in a row up to the cap (lane_verdict).
FAILURE_BACKOFF_BASE = 600.0
FAILURE_BACKOFF_CAP = 4 * 3600.0
# A Gold run stopped at its unit's time limit is started again after this, whatever its attempt: a
# whole build resumes from the shards it staged (root ADR-0440), as a cut-over's gold step does (ADR-0435 b).
TIME_LIMIT_RETRY = 3600.0
# A start its pool refused is asked again after this, doubling per refusal up to the cap (queue_backoff).
QUEUE_RETRY_BASE = 300.0
QUEUE_RETRY_CAP = 3600.0
# A scheduled slot with no start after it is owed one this long after the slot: Airflow starts a job
# within seconds of its slot, and a start waiting in Airflow's own pool is a queued DAG run, which
# `select` already skips (root ADR-0467).
MISSED_START_GRACE = 900.0


@dataclass(frozen=True)
class Asset:
    job: str
    uri: str
    service: str
    gold: str | None = None  # the Gold table a Gold job makes
    inputs: tuple = ()  # its input tables (pipeline graph), the pinning Gold excluded
    pins_from: dict | None = None
    lane: str | None = None  # the lane a bake job serves
    lane_table: str | None = None
    follows: str | None = None  # the lane whose documents this lane's follow


@dataclass
class Verdict:
    owed: bool = False
    versions: dict = field(default_factory=dict)
    lag: float = 0.0
    owed_since: float | None = None
    waiting: str | None = None
    note: str = ""


def panel_assets(job_id: str, job: dict, service: str, golds: list, nodes: dict, edges: dict,
                 contract: dict) -> list[Asset]:
    """Each Gold table a job that makes several builds (gold_panel_rebuild: the parcel and the building
    panel), its own asset (root ADR-0333 §7, stage 2): its inputs are the tables the job's edges feed it from, less
    what the job makes itself (the unit price join it builds the building panel with) and the inputs
    the contract holds back from a real rebuild (`unmeasured_inputs`), so the asset is owed exactly when
    an input its planner pins moved."""
    made = {nodes[edges[edge]["to"]].get("table_name") for edge in job["pipeline_graph_edges"]}
    found = []
    for name in golds:
        node = nodes[name]
        gold = node["table_name"]
        entry = contract["tables"][gold]
        held_back = set(entry.get("unmeasured_inputs", {}))
        # A data kind (gold_kind_rebuild) is fed from its host panel and pinned to what it was built from.
        pins_from = entry.get("pins_from")
        pinning = {pins_from["gold_table"]} if pins_from else set()
        tables = {nodes[edges[edge]["from"]].get("table_name") for edge in job["pipeline_graph_edges"]
                  if edges[edge]["to"] == name} - {None} - made - held_back - pinning
        uri = job_specs.asset_uri((job_specs.platform_of(node), job_specs.name_of(node)))
        found.append(Asset(job_id, uri, service, gold=gold, inputs=tuple(sorted(tables)), pins_from=pins_from))
    return found


def assets(jobs: dict, graph: dict, contract: dict) -> list[Asset]:
    """The assets of every job the reconciler reconciles, from the job list, the graph and the contracts:
    a Gold job's one table, or each lane a by-PNU bake serves (by_pnu_serving_bake serves several, one
    after the other in one unit; each lane records its own reads, so each is its own asset)."""
    listing = {job["id"]: job for job in jobs["jobs"]}
    nodes = {node["id"]: node for node in graph["nodes"]}
    edges = {edge["id"]: edge for edge in graph["edges"]}
    carried = job_specs.job_nodes(jobs, graph)
    found = []
    for job_id in listing[RECONCILER]["reconciles"]:
        inputs, outputs = carried[job_id]
        service = listing[job_id]["systemd_service"]
        if len(outputs) == 1 and nodes[outputs[0]].get("table_name"):
            node = nodes[outputs[0]]
            uri = job_specs.asset_uri((job_specs.platform_of(node), job_specs.name_of(node)))
            tables = [nodes[name]["table_name"] for name in inputs if nodes[name].get("table_name")]
            entry = contract["tables"][node["table_name"]]
            pins_from = entry.get("pins_from")
            pinning = pins_from["gold_table"] if pins_from else None
            found.append(Asset(job_id, uri, service, gold=node["table_name"],
                               inputs=tuple(sorted(t for t in tables if t != pinning)), pins_from=pins_from))
            continue
        golds = [name for name in outputs if nodes[name].get("table_name") in contract["tables"]]
        if golds:
            found.extend(panel_assets(job_id, listing[job_id], service, golds, nodes, edges, contract))
            continue
        # The Gold tables each lane is fed from; a lane's object serving node and its packs are one lane.
        feeds: dict[str, set] = {}
        for edge_id in listing[job_id]["pipeline_graph_edges"]:
            edge = edges[edge_id]
            lane = job_specs.by_pnu_lane_of(edge["to"])
            if lane is None:
                raise ValueError(f"{job_id}: makes {edge['to']}, neither a Gold table nor a by-PNU lane's serving")
            if nodes[edge["from"]].get("table_name"):
                feeds.setdefault(lane, set()).add(nodes[edge["from"]]["table_name"])
        # The parcel and building lanes are the hosts; every other lane is a contract `by_pnu_lanes` lane.
        contract_lanes = json.loads(by_pnu_kinds.CONTRACT_PATH.read_text(encoding="utf-8"))["by_pnu_lanes"]
        for lane in sorted(feeds):
            if len(feeds[lane]) != 1:
                raise ValueError(f"{job_id}: the {lane} lane is fed from {sorted(feeds[lane])}, not one Gold table")
            (table,) = feeds[lane]
            node = nodes[f"{lane}{PACKS_SUFFIX}"]
            uri = job_specs.asset_uri((job_specs.platform_of(node), job_specs.name_of(node)))
            follows = None
            if lane in contract_lanes:
                fields = by_pnu_kinds.lane_fields(lane)
                if table != fields["gold_table"]:
                    raise ValueError(f"{job_id}: the graph feeds {lane} from {table}, the contract from {fields['gold_table']}")
                # The lane whose served documents this lane's must match (the unit price lane's building
                # lane), as the bake's own gate reads it; only across jobs: inside one bake the lanes run in order.
                follows = lane_follow_gate.followed_lane(lane) or None
                if follows and job_specs.writer_of(f"{follows}{PACKS_SUFFIX}", jobs, graph) == job_id:
                    follows = None
            found.append(Asset(job_id, uri, service, lane=lane, lane_table=table, follows=follows))
    return found


def changes_after(history: dict, target: str, read: str) -> list[dict]:
    """The snapshots from `target` back to `read` (excluded) that changed rows, newest first."""
    changed = []
    for snapshot in gold_rebuild.ancestry(history["snapshots"], target):
        if snapshot["snapshot_id"] == read:
            break
        if gold_rebuild.changes_row_data(snapshot):
            changed.append(snapshot)
    return changed


def moved(history: dict, target: str, read: str | None) -> tuple[bool, float | None, float | None]:
    """(whether `history` moved from `read` to `target`, the newest change's time, the oldest's)."""
    if read == target:
        return False, None, None
    if read is not None and gold_rebuild.input_change_since_pin(history["snapshots"], target, read) is None:
        return False, None, None
    changed = changes_after(history, target, read) if any(
        s["snapshot_id"] == target for s in history["snapshots"]) else []
    times = [iceberg_metadata.committed_epoch(s) for s in changed]
    return True, (max(times) if times else None), (min(times) if times else None)


def read_time(history: dict, snapshot: str | None) -> float | None:
    found = next((s for s in history["snapshots"] if s["snapshot_id"] == snapshot), None)
    return None if found is None else iceberg_metadata.committed_epoch(found)


def gold_verdict(asset: Asset, table) -> Verdict:
    """Owed when an input moved past the source pins the Gold's head records (gold_rebuild.plan's rule)."""
    gold = table(asset.gold)
    recorded = gold_rebuild.recorded_pins(gold["snapshots"], gold["head"]) if gold["head"] else None
    targets = {name: table(name)["head"] for name in asset.inputs}
    histories = {name: table(name) for name in asset.inputs}
    if asset.pins_from:
        host = table(asset.pins_from["gold_table"])
        followed = gold_rebuild.recorded_pins(host["snapshots"], host["head"]) if host["head"] else None
        names = asset.pins_from["inputs"]
        if names == "all":
            # A kind reads a subset of its host's inputs, all pinned to the host's (gold_rebuild.plan):
            # the ones its own Gold records. shortcut: an input its producer gains shows only after its next run.
            if followed is None:
                return Verdict(note=f"{asset.pins_from['gold_table']} records no source pins")
            names = sorted(set(recorded or ()) & set(followed))
        for name in names:
            if followed is None or name not in followed:
                return Verdict(note=f"{asset.pins_from['gold_table']} records no pin of {name}")
            targets[name] = followed[name]
            histories.setdefault(name, table(name))
    verdict = Verdict(versions=targets)
    if recorded is None:
        verdict.owed, verdict.note = True, f"{asset.gold} records no source pins"
        return verdict
    newest, oldest, lags = [], [], []
    for name, target in targets.items():
        if target is None:
            continue
        went, new, old = moved(histories[name], target, recorded.get(name))
        if went:
            verdict.owed = True
            if new is not None:
                newest.append(new)
                oldest.append(old)
                base = read_time(histories[name], recorded.get(name))
                lags.append(new - (base if base is not None else old))
    verdict.lag = max(lags, default=0.0)
    verdict.owed_since = min(oldest, default=None)
    return verdict


def failure_backoff(failures: int) -> float:
    """Seconds after its n-th failure in a row before a lane is started again: doubling from ten
    minutes to four hours, as a Kubernetes workqueue's exponential failure rate limiter requeues."""
    return min(FAILURE_BACKOFF_BASE * 2 ** (failures - 1), FAILURE_BACKOFF_CAP)


def gold_attempt(asset: Asset, verdict: Verdict, busy: bool, kept: dict, now: float, timed_out: bool = False) -> dict:
    """The versions a Gold asset's start is keyed by: its owed versions, and once a start of them has
    ended without catching the Gold up, the attempt it is (root ADR-0333 §7, as a lane's failure is).

    A start is keyed by the versions it owes, so it runs once per set of them. A run that ended
    without a commit (the rebuild lock held by a cut-over, `waiting`; a timeout) left the same
    versions owed under a key already used, and nothing started it again until an input moved
    (2026-11-07 08:45Z the panel rebuild ended `waiting` behind a lane cut-over's rebuild). The asset
    still owed and not running is started again after the lanes' backoff (ten minutes doubling to
    four hours, `failure_backoff`), counted from when its job last ran, under a new key; after a run its
    time limit stopped, an hour (`TIME_LIMIT_RETRY`): 2026-11-07 the parcel panel's lock waits and
    timeouts had doubled it to four hours, so a sharded build that resumes sat idle between runs."""
    if not asset.gold or not verdict.owed:
        for name in ("attempt", "attempt_of", "attempt_at"):
            kept.pop(name, None)
        return verdict.versions
    owed = versions_key({asset.uri: verdict.versions})
    if kept.get("attempt_of") != owed:
        kept.update(attempt_of=owed, attempt=0, attempt_at=now)
    elif busy:
        kept["attempt_at"] = now
    elif now - kept["attempt_at"] >= (TIME_LIMIT_RETRY if timed_out else failure_backoff(kept["attempt"] + 1)):
        kept.update(attempt=kept["attempt"] + 1, attempt_at=now)
    return {**verdict.versions, "attempt": kept["attempt"]} if kept["attempt"] else verdict.versions


def lane_verdict(asset: Asset, table, reads, now: float, published=lambda lane: True) -> Verdict:
    """Owed when the lane's Gold moved past the snapshot its last run left served.

    The versions owed name the snapshot read too, so a run that moved the lane but not to the head (a
    full bake finished at its ADR-0310 pin) is owed the next start; and after a failed run the number
    of failures in a row, so a failure is owed a new start once its backoff has passed, where the same
    key would have started it once only (2026-10-29: the unit price bake failed at 07:19Z and 07:40Z,
    and every tick owed the key already started)."""
    gold = table(asset.lane_table)
    last = reads(asset.lane)
    read = None if last is None else last.get("read_snapshot_id")
    versions = {asset.lane_table: gold["head"], "read": read}
    failures = (last or {}).get("consecutive_failures", 0) if (last or {}).get("outcome") == "failed" else 0
    if failures:
        versions["failures"] = failures
    followed = None
    if asset.follows:
        followed = (reads(asset.follows) or {}).get("read_snapshot_id")
        versions[f"lane:{asset.follows}"] = followed
    verdict = Verdict(versions=versions)
    if gold["head"] is None:
        return verdict
    if last is None:
        # A lane no run has published yet is the auto-promotion's (root ADR-0435): its bake would only say
        # `not cut over yet` and record no read, so the lane would be owed every tick, and an owed asset of
        # a `publish` job holds back every lane's first publish (by-pnu-auto-promote.sh, root ADR-0468).
        if not published(asset.lane):
            verdict.waiting = f"the {asset.lane} lane is not cut over yet; its first publish is the auto-promotion's"
            return verdict
        verdict.owed, verdict.note = True, f"the {asset.lane} lane has recorded no read yet"
        return verdict
    went, new, old = moved(gold, gold["head"], read)
    if not went:
        return verdict
    verdict.owed = True
    if new is not None:
        base = read_time(gold, read)
        verdict.lag = new - (base if base is not None else old)
        verdict.owed_since = old
    if failures:
        retry_at = datetime.fromisoformat(last["at"].replace("Z", "+00:00")).timestamp() + failure_backoff(failures)
        if now < retry_at:
            verdict.waiting = f"failed {failures} times in a row, last at {last['at']}; started again from {retry_at:.0f}"
        return verdict
    waiting = last.get("waiting_on")
    # It waited for the lane it follows at this very Gold, and that lane serves what it served then.
    if (last.get("outcome") == "waiting" and isinstance(waiting, dict) and waiting.get("lane") == asset.follows
            and last.get("gold_snapshot_id") == gold["head"]
            and (waiting.get("read_snapshot_id") or "-") == (followed or "-")):
        verdict.waiting = f"waits for the {asset.follows} lane to serve what gold {gold['head']} was built from"
    return verdict


def lane_behind_since(asset: Asset, table, reads) -> float | None:
    """When the oldest input change the lane does not serve yet was committed, the lane's data time
    (root ADR-0444): its Gold's row changes after the snapshot it serves, and each Silver or reference
    table's row changes after the pin of the Gold it serves (the Gold head's pins when that snapshot
    is no longer kept). 0.0 when the lane serves every change there is; None when it cannot be told (no
    read yet, no pins, a change without a kept commit time), and the alert then holds the served
    manifest's age instead.

    2026-10-17..11-07 the zoning, price, characteristics and land-rights lanes served packs that
    reflected their Gold, built from the newest Silver there was, and their monitors failed hourly on
    a 498-hour-old manifest; the parcel lane at 202 hours looked the same and was really behind."""
    gold = table(asset.lane_table)
    read = (reads(asset.lane) or {}).get("read_snapshot_id")
    if gold["head"] is None or read is None:
        return None
    changes = []
    went, _, oldest = moved(gold, gold["head"], read)
    if went:
        changes.append(oldest)
    # The head's rows are the served ones when no row changed since (a run that only recorded new pins,
    # 2026-11-08 12:32Z gold.building_panel): its pins are what the lane serves.
    served = read if went and any(s["snapshot_id"] == read for s in gold["snapshots"]) else gold["head"]
    pins = gold_rebuild.recorded_pins(gold["snapshots"], served)
    if pins is None:
        return None
    for name, pin in sorted(pins.items()):
        source = table(name)
        if source["head"] is None:
            continue
        went, _, oldest = moved(source, source["head"], pin)
        if went:
            changes.append(oldest)
    if None in changes:
        return None
    return min(changes, default=0.0)


def tick(now: float, found: list[Asset], table, reads, running, pool_full, state: dict, timed_out=lambda service: False,
         published=lambda lane: True):
    """(owed lines, metric lines, new state, errors): one pass over the assets. Pure but for its callables."""
    owed_by_job, metrics, errors, next_state = {}, [], [], {}
    for asset in found:
        labels = f'asset="{asset.uri}",job="{asset.job}"'
        kept = dict(state.get(asset.uri, {}))
        try:
            verdict = gold_verdict(asset, table) if asset.gold else lane_verdict(asset, table, reads, now, published)
            busy = running(asset.service)
            full = "" if busy else pool_full(asset.job)
        except Exception as error:  # noqa: BLE001 -- one asset that cannot be read does not stop the others
            errors.append(f"{asset.job}: {type(error).__name__}: {error}")
            metrics.append(f"foundation_asset_reconcile_error{{{labels}}} 1")
            if "unscheduled_since" in kept:
                metrics.append(f"foundation_asset_unscheduled_since_timestamp_seconds{{{labels}}} {kept['unscheduled_since']:.0f}")
            next_state[asset.uri] = kept
            continue
        metrics.append(f"foundation_asset_reconcile_error{{{labels}}} 0")
        metrics.append(f"foundation_asset_lag_seconds{{{labels}}} {max(verdict.lag, 0.0):.0f}")
        metrics.append(f"foundation_asset_owed{{{labels}}} {int(verdict.owed)}")
        if asset.lane:
            try:
                since = lane_behind_since(asset, table, reads)
            except Exception as error:  # noqa: BLE001 -- unknown data time; the owed verdict above stands
                print(f"asset_reconcile: could not date the {asset.lane} lane's inputs: {type(error).__name__}: {error}",
                      file=sys.stderr)
                since = None
            if since is not None:
                behind = max(now - since, 0.0) if since else 0.0
                metrics.append(f'foundation_by_pnu_lane_behind_seconds{{lane="{asset.lane}"}} {behind:.0f}')
        if busy or full:
            kept["busy_at"] = now
        versions = gold_attempt(asset, verdict, busy, kept, now, not busy and asset.gold is not None
                                and timed_out(asset.service))
        if verdict.owed and not busy and not verdict.waiting:
            owed_by_job.setdefault(asset.job, {})[asset.uri] = versions
        if verdict.owed and not busy and not full and not verdict.waiting:
            since = kept.get("unscheduled_since") or max(verdict.owed_since or now, kept.get("busy_at", 0.0))
            kept["unscheduled_since"] = min(since, now)
            metrics.append(f"foundation_asset_unscheduled_since_timestamp_seconds{{{labels}}} {kept['unscheduled_since']:.0f}")
        else:
            kept.pop("unscheduled_since", None)
        why = verdict.waiting or verdict.note
        print(f"asset {asset.uri} ({asset.job}): {'owed' if verdict.owed else 'current'}"
              f"{', running' if busy else ''}{', pool full' if full else ''}"
              f", lag {verdict.lag:.0f}s{f'; {why}' if why else ''}")
        next_state[asset.uri] = kept
    metrics.append(f"foundation_asset_reconcile_timestamp_seconds {now:.0f}")
    # One start per job: a job serving several lanes owes one run for all its owed lanes' versions.
    owed_lines = [f"foundation-asset-owed {job} {versions_key(owed)}" for job, owed in owed_by_job.items()]
    return owed_lines, metrics, next_state, errors


def queue_backoff(attempts: int) -> float:
    """Seconds after its n-th refusal before a queued start is asked again: doubling from five minutes
    to an hour (root ADR-0462), as a Kubernetes workqueue requeues; the pool having room comes first."""
    return min(QUEUE_RETRY_BASE * 2 ** (attempts - 1), QUEUE_RETRY_CAP)


def queued_starts(now: float, jobs: dict, queued: dict, last_started: dict, running, pool_full):
    """(owed lines, metric lines): each start a pool refused (job_units.py queue, root ADR-0462), owed a
    new start once its backoff has passed and its pool has room, keyed by its refusals so each retry
    starts once; and how long each job waits in the queue against its class's budget.

    Until 2026-11-09 a refused start was dropped: the ten first Silver loads refused at 07:00Z behind a
    lane cut-over's claim had their next try at the next day's 07:00Z."""
    listing = {job["id"]: job for job in jobs["jobs"]}
    owed, metrics = [], []
    for job_id, record in sorted(queued.items()):
        job = listing.get(job_id)
        if job is None or not job["enabled"]:
            continue  # no longer a job that runs: nothing starts it, its next start takes the record off
        labels = f'job="{job_id}",pool="{job["pool"]}",priority_class="{job.get("priority_class", "default")}"'
        budget = job_units.priority_class(jobs, job).get("queue_wait_budget_minutes")
        metrics.append(f"foundation_job_queue_wait_seconds{{{labels}}} {max(now - record['since'], 0):.0f}")
        metrics.append(f"foundation_job_queue_refusals{{{labels}}} {record.get('attempts', 1)}")
        if budget is not None:
            metrics.append(f"foundation_job_queue_wait_budget_seconds{{{labels}}} {budget * 60}")
        busy = running(job["systemd_service"])
        retry_at = record.get("last_at", record["since"]) + queue_backoff(record.get("attempts", 1))
        full = "" if busy or now < retry_at else pool_full(job_id)
        if not busy and now >= retry_at and not full:
            key = hashlib.sha256(json.dumps({"queued": job_id, "since": record["since"],
                                             "attempts": record.get("attempts", 1)}).encode()).hexdigest()[:16]
            owed.append(f"foundation-asset-owed {job_id} {key}")
        print(f"queued start {job_id}: waiting {now - record['since']:.0f}s, refused {record.get('attempts', 1)} times"
              f"{', running' if busy else ''}{f', next try from {retry_at:.0f}' if now < retry_at else ''}"
              f"{f'; {full}' if full else ''}")
    for job_id, started in sorted(last_started.items()):
        job = listing.get(job_id)
        if job is None:
            continue
        labels = f'job="{job_id}",pool="{job["pool"]}",priority_class="{job.get("priority_class", "default")}"'
        metrics.append(f"foundation_job_queue_last_wait_seconds{{{labels}}} {started['waited_seconds']}")
    return owed, metrics


def missed_starts(now: float, jobs: dict, reconciler: dict, starts: dict | None, queued: dict, paused, running):
    """(owed lines, metric lines): each job the reconciler starts again (job_specs.requeued) whose newest
    scheduled slot, at least MISSED_START_GRACE old and after it was switched on, no start followed, owed
    one start keyed by that slot. Level-triggered on the record of starts rather than on a refusal being
    queued, as Kubernetes' CronJob controller starts the newest schedule it missed and systemd's
    Persistent= timers catch up a missed one: whatever kept the start away (a refusal from before the
    queue existed, a queue write that failed, Airflow down) leaves the same trace, a slot with no start.

    2026-11-09 11:40Z: data_go_kr_file_data's first run was refused by the spark pool 3 hours before the
    start queue (root ADR-0462) was deployed, so nothing held the refusal and its next try was the next
    day's 11:40Z. A job in the queue is the queue's (queued_starts); a takes_turns job's deferral is its
    rule's (root ADR-0138); a paused job starts nothing (root ADR-0282 §3). None until the deploy has
    seeded the records (job_units.last_starts): no slot is judged without them."""
    if starts is None:
        print("missed starts: the start records are not seeded yet; no slot is judged")
        return [], []
    listing = {job["id"]: job for job in jobs["jobs"]}
    owed, metrics = [], []
    for job_id in job_specs.requeued(jobs, reconciler):
        job = listing[job_id]
        if job.get("takes_turns") or job_id in queued or paused(job_id):
            continue
        slot = job_specs.previous_slot(job["schedule"], now - MISSED_START_GRACE)
        since = job_specs._instant(job.get("enabled_since"))
        if since is None or slot < since.timestamp() or starts.get(job_id, 0) >= slot:
            continue
        labels = f'job="{job_id}",pool="{job["pool"]}",priority_class="{job.get("priority_class", "default")}"'
        metrics.append(f"foundation_job_missed_start_seconds{{{labels}}} {now - slot:.0f}")
        if any(running(service) for service in filter(None, (job["systemd_service"], job.get("check_service")))):
            print(f"missed start {job_id}: no start after its {slot:.0f} slot, but it runs")
            continue
        key = hashlib.sha256(json.dumps({"missed": job_id, "slot": slot}).encode()).hexdigest()[:16]
        owed.append(f"foundation-asset-owed {job_id} {key}")
        last = starts.get(job_id)
        print(f"missed start {job_id}: no start after its {slot:.0f} slot"
              f" (last {'never' if last is None else f'at {last}'}); started once for it")
    return owed, metrics


def versions_key(owed: dict) -> str:
    """The key of a job's owed versions: one asset's versions alone, several by asset."""
    keyed = next(iter(owed.values())) if len(owed) == 1 else owed
    return hashlib.sha256(json.dumps(keyed, sort_keys=True).encode()).hexdigest()[:16]


HELP = """# HELP foundation_asset_lag_seconds Newest unread input change less the read version's commit time (root ADR-0333).
# TYPE foundation_asset_lag_seconds gauge
# HELP foundation_asset_owed Whether an input of the asset moved past what its last run read.
# TYPE foundation_asset_owed gauge
# HELP foundation_asset_unscheduled_since_timestamp_seconds Since when the owed asset has had room in its pool and no run.
# TYPE foundation_asset_unscheduled_since_timestamp_seconds gauge
# HELP foundation_asset_reconcile_error Whether the reconciler could not read the asset this tick.
# TYPE foundation_asset_reconcile_error gauge
# HELP foundation_by_pnu_lane_behind_seconds How long the lane has served data behind its inputs: since the oldest Gold, Silver or reference change it does not serve (root ADR-0444); 0 when it serves every change.
# TYPE foundation_by_pnu_lane_behind_seconds gauge
# HELP foundation_job_queue_wait_seconds How long a job's start has waited in the start queue since its pool first refused it (root ADR-0462).
# TYPE foundation_job_queue_wait_seconds gauge
# HELP foundation_job_queue_refusals How many times its pool refused the queued start.
# TYPE foundation_job_queue_refusals gauge
# HELP foundation_job_queue_wait_budget_seconds How long the job's priority class may wait in the start queue before it alerts.
# TYPE foundation_job_queue_wait_budget_seconds gauge
# HELP foundation_job_queue_last_wait_seconds How long the job's last start that had been queued waited.
# TYPE foundation_job_queue_last_wait_seconds gauge
# HELP foundation_job_missed_start_seconds How long ago the job's newest scheduled slot passed with no start after it (root ADR-0467); absent once one starts.
# TYPE foundation_job_missed_start_seconds gauge
# HELP foundation_asset_reconcile_timestamp_seconds When the asset reconciler last finished a tick.
# TYPE foundation_asset_reconcile_timestamp_seconds gauge
"""


# How many bridge networks Docker's local-scope default address pools hold when the daemon names
# none (moby libnetwork ipamutils): three single /16s, twelve /16s out of three /14s and sixteen /20s
# out of one /16, the default bridge among them. Counted, not listed: the public repository refuses
# private-network literals.
DEFAULT_POOL_NETWORKS = 3 + 3 * 4 + 16
NETWORKS_METRICS_NAME = "docker-networks.prom"
NETWORKS_HELP = """# HELP foundation_docker_bridge_networks The host's Docker bridge networks, each holding one subnet of the address pools.
# TYPE foundation_docker_bridge_networks gauge
# HELP foundation_docker_address_pool_networks How many bridge networks the daemon's address pools hold.
# TYPE foundation_docker_address_pool_networks gauge
"""


def bridge_networks(run) -> str:
    """The textfile of the host's bridge networks against what the address pools hold. On 2026-10-29
    23 of 31 were taken, 10 of them left by per-run Compose projects whose cleanup failed; at 31
    every job's next Compose project fails to start ("could not find an available, non-overlapping
    IPv4 address pool"). `run` runs a docker command and returns its output."""
    taken = len(run(["docker", "network", "ls", "--quiet", "--filter", "driver=bridge"]).split())
    pools = json.loads(run(["docker", "info", "--format", "{{json .DefaultAddressPools}}"]) or "null")
    held = (sum(2 ** (pool["Size"] - ipaddress.ip_network(pool["Base"]).prefixlen) for pool in pools)
            if pools else DEFAULT_POOL_NETWORKS)
    return (NETWORKS_HELP + f"foundation_docker_bridge_networks {taken}\n"
            f"foundation_docker_address_pool_networks {held}\n")


CONTAINERS_METRICS_NAME = "docker-containers.prom"
CONTAINERS_HELP = """# HELP foundation_docker_container_info A running container's name by the cgroup cAdvisor reads it from.
# TYPE foundation_docker_container_info gauge
"""


def container_names(run) -> str:
    """The textfile naming each running container's cgroup, as cAdvisor labels it (`id`) under the
    systemd cgroup driver. cAdvisor runs without the engine's socket (container-runtime-policy), so
    it knows a container only by that id; the alert on a container near its cap names it from here.
    Docker container names are [a-zA-Z0-9][a-zA-Z0-9_.-]+, so they need no escaping."""
    lines = []
    for line in run(["docker", "ps", "--no-trunc", "--format", "{{.ID}} {{.Names}}"]).splitlines():
        identifier, name = line.split()
        lines.append(f'foundation_docker_container_info{{id="/system.slice/docker-{identifier}.scope",name="{name}"}} 1\n')
    return CONTAINERS_HELP + "".join(sorted(lines))


UNITS_METRICS_NAME = "foundation-units.prom"
# Since when each failing unit has failed, kept across ticks beside the reconciler's own state.
UNITS_STATE_FILE = "failing-units.json"
UNITS_HELP = """# HELP foundation_unit_failed_since_seconds When a failed host unit entered its failed state, with the owner and runbook its unit file names (root ADR-0445).
# TYPE foundation_unit_failed_since_seconds gauge
# HELP foundation_units_failed How many host units are failed now.
# TYPE foundation_units_failed gauge
"""
RELEASE_PREFIX = "file:///opt/foundation-platform/current/"


def label(value: str) -> str:
    """A Prometheus label value: systemd escapes instance names with backslashes (`\\x2d`)."""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def failed_units(failed, show, tracked: dict, read_text=lambda path: Path(path).read_text(encoding="utf-8")):
    """The textfile of every failed foundation unit, since when it has been failing, and who owns it,
    with the failing-since table to keep for the next tick (root ADR-0445).

    2026-11-01..07: the identity restore drill, the court auction collector and seven others stayed
    failed for days. Each had posted one Slack notice when it failed and nothing after, and a weekly
    or retired unit never runs again to post another. This is the state, not the event: the
    FoundationUnitFailedLong rule calls the owner while a unit keeps failing. A unit is failing from
    the first tick that saw it failed until one sees it neither failed nor running, so an hourly
    unit that fails every run stays one failure, not a new one each hour. The owner and runbook are
    the unit file's X-Foundation-Owner= and Documentation=; a unit whose file is gone (retired by a
    release) has neither, and FoundationUnitFailedUnowned says so. `failed` and `show` are
    job_units.failed and job_units.show, the host's one way to ask systemd."""
    names = sorted(entry["unit"] for entry in failed())
    shown = show(sorted(set(names) | set(tracked)))
    failing = {}
    for name, first in tracked.items():
        # Still failed, or running again (a failing unit between two runs); only a success (or a
        # reset) ends it.
        if job_units.failed_or_running(shown.get(name, {})):
            failing[name] = first
    lines = []
    for name in names:
        unit = shown.get(name, {})
        failing[name] = min(failing.get(name, job_units.since(unit)), job_units.since(unit))
        owner = ""
        if unit.get("FragmentPath"):
            try:
                owner = next((line.split("=", 1)[1].strip() for line in read_text(unit["FragmentPath"]).splitlines()
                              if line.startswith("X-Foundation-Owner=")), "")
            except OSError:
                pass
        runbook = (unit.get("Documentation") or "").split(" ")[0].removeprefix(RELEASE_PREFIX)
        lines.append(f'foundation_unit_failed_since_seconds{{unit="{label(name)}",owner="{label(owner)}",'
                     f'runbook="{label(runbook)}"}} {failing[name]}\n')
    return UNITS_HELP + "".join(lines) + f"foundation_units_failed {len(names)}\n", failing


DAGSTER_METRICS_NAME = "dagster.prom"
# The dual run's check and the DAG asset it sits on (orchestration/dagster/dual_run.py CHECK_NAME,
# dag_key); the host has no Dagster to import them from, so a test reads them from that file.
DUAL_RUN_CHECK = "dual_run_matches_airflow"
DUAL_RUN_KEY_PREFIX = ("foundation_airflow", "dag")
DAGSTER_HELP = """# HELP foundation_dagster_observing Whether Dagster observes Airflow (jobs.v1.json observers.dagster.enabled, root ADR-0333 stage 0).
# TYPE foundation_dagster_observing gauge
# HELP foundation_dagster_answered Whether Dagster's GraphQL answered this tick.
# TYPE foundation_dagster_answered gauge
# HELP foundation_dagster_code_location_loaded Whether a Dagster code location loads (0: a load error, or still loading).
# TYPE foundation_dagster_code_location_loaded gauge
# HELP foundation_dual_run_jobs How many jobs are at dagster_stage shadow (root ADR-0431 §2).
# TYPE foundation_dual_run_jobs gauge
# HELP foundation_dual_run_last_comparison_timestamp_seconds When a job's dual-run check last compared, and so appended to the ledger (root ADR-0458).
# TYPE foundation_dual_run_last_comparison_timestamp_seconds gauge
"""


def dagster_state(jobs: dict, dag_ids: dict, post) -> str:
    """The textfile of whether Dagster's code location loads and when each job in the dual run last
    compared (root ADR-0431, ADR-0458). `dag_ids` is job id -> DAG id of the jobs at `shadow`; `post`
    sends one GraphQL query to Dagster and returns its `data`, raising OSError or ValueError when
    Dagster does not answer.

    From 2026-11-06 21:16Z to 11-09 the code location failed every load (6,222 times in the daemon's
    log), so no dual-run check ran and the ledger got no line, while Dagster's UI answered and the
    deploy said "observing". A comparison is read from the instance's event log, not the location, so
    the time of the last one is known while the location is down. A check's evaluation is reported
    only after its ledger line is written (dual_run.append), so an evaluation is a ledger line."""
    observing = bool(jobs.get("observers", {}).get("dagster", {}).get("enabled"))
    text = DAGSTER_HELP + f"foundation_dagster_observing {int(observing)}\n"
    if not observing:
        return text
    text += f"foundation_dual_run_jobs {len(dag_ids)}\n"
    names = sorted(dag_ids)
    checks = "".join(
        f' j{index}: assetCheckExecutions(assetKey: {{path: {json.dumps([*DUAL_RUN_KEY_PREFIX, dag_ids[job]])}}},'
        f' checkName: "{DUAL_RUN_CHECK}", limit: 1) {{ evaluation {{ timestamp }} }}'
        for index, job in enumerate(names))
    query = ("{ workspaceOrError { __typename ... on Workspace { locationEntries { name locationOrLoadError"
             " { __typename } } } }" + checks + " }")
    try:
        data = post(query)
        entries = data["workspaceOrError"]["locationEntries"]
    except (OSError, ValueError, KeyError, TypeError):
        return text + "foundation_dagster_answered 0\n"
    text += "foundation_dagster_answered 1\n"
    for entry in sorted(entries, key=lambda entry: entry["name"]):
        loaded = (entry.get("locationOrLoadError") or {}).get("__typename") == "RepositoryLocation"
        text += f'foundation_dagster_code_location_loaded{{location="{label(entry["name"])}"}} {int(loaded)}\n'
    for index, job in enumerate(names):
        found = data.get(f"j{index}") or []
        evaluation = (found[0] if found else {}).get("evaluation")
        if evaluation:
            text += f'foundation_dual_run_last_comparison_timestamp_seconds{{job="{label(job)}"}} {evaluation["timestamp"]}\n'
    return text


def dagster_post(url: str):
    """`post` for dagster_state: one GraphQL query to Dagster's webserver (jobs.v1.json observers.dagster.ui)."""
    import urllib.request  # noqa: PLC0415

    def post(query: str) -> dict:
        request = urllib.request.Request(f"{url}/graphql", data=json.dumps({"query": query}).encode(), method="POST",
                                         headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read())["data"]
    return post


def write_replacing(path: Path, text: str) -> None:
    """Replaced whole by a rename, so a scrape or the next tick never reads half of it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".next")
    temporary.write_text(text, encoding="utf-8")
    temporary.chmod(0o644)
    temporary.replace(path)


def absent_is_empty(read, name: str) -> dict:
    """`read(name)`, or a history with no snapshot when the catalog has no such table (HTTP 404): a
    Gold a new job has not built yet is owed its first build, and its lane nothing, instead of a
    reconcile error every tick until then (2026-10-30, the court auction Gold, root ADR-0353)."""
    import urllib.error  # noqa: PLC0415

    try:
        return read(name)
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
        return {"head": None, "snapshots": []}


def last_read(bake_root: Path):
    def read(lane: str):
        path = bake_root / lane / READS
        try:
            lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        except FileNotFoundError:
            return None
        if not lines:
            return None
        records = [json.loads(line) for line in lines]
        last = records[-1]
        # The failures in a row at the end, for the failed lane's backoff (lane_verdict).
        failures = 0
        for record in reversed(records):
            if record.get("outcome") != "failed":
                break
            failures += 1
        return {**last, "consecutive_failures": failures}
    return read


def cut_over(bake_root: Path):
    """Whether a lane's cut-over has published it (a `publish` step that passed in the cut-over runner's
    evidence, by-pnu-lane-cutover.sh): the lanes the auto-promotion has not published yet record no read."""
    lanes: set | None = None

    def published(lane: str) -> bool:
        nonlocal lanes
        if lanes is None:
            lanes = set()
            for path in sorted((bake_root / "cutover").glob("*.jsonl")):
                for line in path.read_text(encoding="utf-8").splitlines():
                    try:
                        step = json.loads(line)
                    except ValueError:
                        continue
                    if step.get("step") == "publish" and step.get("outcome") == "passed":
                        lanes.add(step.get("lane"))
        return lane in lanes
    return published


def main() -> int:
    jobs = json.loads(job_specs.JOBS.read_text(encoding="utf-8"))
    graph = json.loads(job_specs.GRAPH.read_text(encoding="utf-8"))
    found = assets(jobs, graph, gold_rebuild.load_contract())
    services = {service for job in jobs["jobs"] for service in (job["systemd_service"], job.get("check_service")) if service}
    state_dir = Path(os.environ.get("FOUNDATION_ASSET_RECONCILE_STATE_DIR")
                     or job_specs.job_state_root(RECONCILER, jobs))
    metrics_file = Path(os.environ.get("FOUNDATION_ASSET_RECONCILE_METRICS_DIR") or METRICS_DIR) / METRICS_NAME
    bake_root = Path(os.environ.get("FOUNDATION_BY_PNU_BAKE_STATE_ROOT")
                     or job_specs.job_state_root("by_pnu_serving_bake", jobs))
    try:
        state = json.loads((state_dir / STATE_FILE).read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        state = {}
    catalog = iceberg_metadata.Catalog("FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_READER_TOKEN", "foundation-asset-reconcile")
    cache: dict = {}

    def table(name):
        if name not in cache:
            cache[name] = absent_is_empty(catalog.table, name)
        return cache[name]

    def running(service):
        assert service in services
        return bool(job_units.running([service]))

    def pool_full(job_id):
        return job_units.pool_full(job_specs.JOBS, job_id)

    def timed_out(service):
        assert service in services
        try:
            shown = subprocess.run(["systemctl", "show", "-p", "Result", "--value", service],
                                   capture_output=True, text=True, timeout=30)
        except (OSError, subprocess.SubprocessError):
            return False  # unknown: the doubling backoff stands
        return shown.stdout.strip() == "timeout"

    now = time.time()
    owed, metrics, state, errors = tick(now, found, table, last_read(bake_root), running, pool_full, state,
                                        timed_out, cut_over(bake_root))
    try:
        queued_owed, queued_metrics = queued_starts(now, jobs, job_units.queued(), job_units.last_started(),
                                                    running, pool_full)
    except (OSError, ValueError, KeyError) as error:
        errors.append(f"the start queue: {type(error).__name__}: {error}")
        queued_owed, queued_metrics = [], []
    owed = [line for line in owed if line.split()[1] not in {queued.split()[1] for queued in queued_owed}] + queued_owed
    metrics += queued_metrics
    switches = Path(os.environ.get("FOUNDATION_JOB_SWITCHES_DIR") or SWITCHES_DIR)
    try:
        missed_owed, missed_metrics = missed_starts(
            now, jobs, next(job for job in jobs["jobs"] if job["id"] == RECONCILER), job_units.last_starts(),
            job_units.queued(), lambda job_id: (switches / job_id).exists(), running)
    except (OSError, ValueError, KeyError, job_specs.JobListError) as error:
        errors.append(f"the start records: {type(error).__name__}: {error}")
        missed_owed, missed_metrics = [], []
    owed += [line for line in missed_owed if line.split()[1] not in {held.split()[1] for held in owed}]
    metrics += missed_metrics
    write_replacing(state_dir / STATE_FILE, json.dumps(state, sort_keys=True) + "\n")
    write_replacing(metrics_file, HELP + "\n".join(metrics) + "\n")
    # The pools every job's Compose project draws from; a probe that fails stops nothing, and the
    # FoundationDockerNetworksUnread rule says the series went stale.
    def run(command):
        return subprocess.run(command, check=True, capture_output=True, text=True, timeout=30).stdout

    try:
        write_replacing(metrics_file.with_name(NETWORKS_METRICS_NAME), bridge_networks(run))
    except (OSError, subprocess.SubprocessError, ValueError, KeyError) as error:
        print(f"asset_reconcile: could not count the Docker bridge networks: {error}", file=sys.stderr)
    try:
        write_replacing(metrics_file.with_name(CONTAINERS_METRICS_NAME), container_names(run))
    except (OSError, subprocess.SubprocessError, ValueError) as error:
        print(f"asset_reconcile: could not list the running containers: {error}", file=sys.stderr)
    try:
        try:
            tracked = json.loads((state_dir / UNITS_STATE_FILE).read_text(encoding="utf-8"))
        except (FileNotFoundError, ValueError):
            tracked = {}
        text, tracked = failed_units(job_units.failed, job_units.show, tracked)
        write_replacing(metrics_file.with_name(UNITS_METRICS_NAME), text)
        write_replacing(state_dir / UNITS_STATE_FILE, json.dumps(tracked, sort_keys=True) + "\n")
    except (OSError, subprocess.SubprocessError, ValueError, KeyError) as error:
        print(f"asset_reconcile: could not list the failed units: {error}", file=sys.stderr)
    try:
        dag_ids = {spec.job_id: spec.dag_id for spec in job_specs.load_specs(jobs, graph) if spec.dagster_stage == "shadow"}
        ui = jobs.get("observers", {}).get("dagster", {}).get("ui", "")
        write_replacing(metrics_file.with_name(DAGSTER_METRICS_NAME), dagster_state(jobs, dag_ids, dagster_post(ui)))
    except (OSError, ValueError, job_specs.JobListError) as error:
        print(f"asset_reconcile: could not ask Dagster: {error}", file=sys.stderr)
    for line in owed:
        print(line)
    for error in errors:
        print(f"asset_reconcile: could not read {error}", file=sys.stderr)
    if errors and len(errors) == len(found):
        print(f"foundation-job-outcome failed no asset could be read ({errors[0]})")
        return 1
    print("foundation-job-outcome unchanged")
    return 0


if __name__ == "__main__":
    sys.exit(main())
