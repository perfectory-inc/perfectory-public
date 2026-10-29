#!/usr/bin/env python3
"""The asset reconciler: which assets are owed a run, by state rather than by event (root ADR-0333 §7).

    asset_reconcile.py            one tick (the job asset_reconcile, every five minutes)

On 2026-10-28 at 21:38 the 공매 Gold rebuild committed and recorded asset event 3912, and the bake that
serves it was never queued: Airflow drops an event for a paused DAG and a deploy held the bake paused
(root ADR-0337). Nothing measured it; the screen would have waited 13 hours for the 10:45 fallback.
An event is a fast path that can be lost. This compares state, as a Kubernetes controller or Dagster's
declarative automation does: for each job of the reconciler's `reconciles` (orchestration/jobs.v1.json)
it makes one asset, and

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

with a key of the versions it owes, and the reconciler's DAG starts the job once per key unless a run
of it is queued or running (orchestration/dags/foundation_jobs.py): a repeated tick or a lost event
never starts it twice, and the start waits for the job's pool like any other.

It writes, for node-exporter's textfile collector (/var/lib/foundation-jobs/metrics/asset-reconcile.prom):
foundation_asset_lag_seconds (the newest unread input change less the read version's commit time, 0
when caught up), foundation_asset_unscheduled_since_timestamp_seconds (while an owed asset is not
running, not waiting and its pool has room: since the later of its first unread change and the last
tick that saw it running or its pool full) and foundation_asset_reconcile_timestamp_seconds. The
FoundationAsset* rules turn them into asset_lag_seconds and asset_unscheduled_seconds and alert. The
idle-since times are kept under the job's state path between ticks. It changes no data: its outcome
is `unchanged`, or `failed` when no asset could be read.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from dataclasses import dataclass, field
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
READS = "reads.jsonl"
# The by-PNU serving surface of a lane is this node of the pipeline graph (`<lane>-by-pnu-section-packs`).
PACKS_SUFFIX = "-by-pnu-section-packs"


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

    def key(self) -> str:
        return hashlib.sha256(json.dumps(self.versions, sort_keys=True).encode()).hexdigest()[:16]


def assets(jobs: dict, graph: dict, contract: dict) -> list[Asset]:
    """The asset of every job the reconciler reconciles, from the job list, the graph and the contracts."""
    listing = {job["id"]: job for job in jobs["jobs"]}
    nodes = {node["id"]: node for node in graph["nodes"]}
    carried = job_specs.job_nodes(jobs, graph)
    found = []
    for job_id in listing[RECONCILER]["reconciles"]:
        inputs, (output,) = carried[job_id]
        node = nodes[output]
        uri = job_specs.asset_uri((job_specs.platform_of(node), job_specs.name_of(node)))
        service = listing[job_id]["systemd_service"]
        tables = [nodes[name]["table_name"] for name in inputs if nodes[name].get("table_name")]
        if node.get("table_name"):
            entry = contract["tables"][node["table_name"]]
            pins_from = entry.get("pins_from")
            pinning = pins_from["gold_table"] if pins_from else None
            found.append(Asset(job_id, uri, service, gold=node["table_name"],
                               inputs=tuple(sorted(t for t in tables if t != pinning)), pins_from=pins_from))
        elif output.endswith(PACKS_SUFFIX):
            lane = output.removesuffix(PACKS_SUFFIX)
            fields = by_pnu_kinds.lane_fields(lane)
            (table,) = tables
            if table != fields["gold_table"]:
                raise ValueError(f"{job_id}: the graph feeds {output} from {table}, the contract from {fields['gold_table']}")
            # The lane whose served documents this lane's must match (the unit price lane's building
            # lane), as the bake's own gate reads it; only across jobs: inside one bake the lanes run in order.
            follows = lane_follow_gate.followed_lane(lane) or None
            if follows and job_specs.writer_of(f"{follows}{PACKS_SUFFIX}", jobs, graph) == job_id:
                follows = None
            found.append(Asset(job_id, uri, service, lane=lane, lane_table=table, follows=follows))
        else:
            raise ValueError(f"{job_id}: makes {output}, neither a Gold table nor a by-PNU lane's packs")
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
        for name in asset.pins_from["inputs"]:
            if followed is None or name not in followed:
                return Verdict(note=f"{asset.pins_from['gold_table']} records no pin of {name}")
            targets[name] = followed[name]
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


def lane_verdict(asset: Asset, table, reads) -> Verdict:
    """Owed when the lane's Gold moved past the snapshot its last run left served."""
    gold = table(asset.lane_table)
    last = reads(asset.lane)
    read = None if last is None else last.get("read_snapshot_id")
    versions = {asset.lane_table: gold["head"]}
    followed = None
    if asset.follows:
        followed = (reads(asset.follows) or {}).get("read_snapshot_id")
        versions[f"lane:{asset.follows}"] = followed
    verdict = Verdict(versions=versions)
    if gold["head"] is None:
        return verdict
    if last is None:
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
    waiting = last.get("waiting_on")
    # It waited for the lane it follows at this very Gold, and that lane serves what it served then.
    if (last.get("outcome") == "waiting" and isinstance(waiting, dict) and waiting.get("lane") == asset.follows
            and last.get("gold_snapshot_id") == gold["head"]
            and (waiting.get("read_snapshot_id") or "-") == (followed or "-")):
        verdict.waiting = f"waits for the {asset.follows} lane to serve what gold {gold['head']} was built from"
    return verdict


def tick(now: float, found: list[Asset], table, reads, running, pool_full, state: dict):
    """(owed lines, metric lines, new state, errors): one pass over the assets. Pure but for its callables."""
    owed_lines, metrics, errors, next_state = [], [], [], {}
    for asset in found:
        labels = f'asset="{asset.uri}",job="{asset.job}"'
        kept = dict(state.get(asset.uri, {}))
        try:
            verdict = gold_verdict(asset, table) if asset.gold else lane_verdict(asset, table, reads)
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
        if busy or full:
            kept["busy_at"] = now
        if verdict.owed and not busy and not verdict.waiting:
            owed_lines.append(f"foundation-asset-owed {asset.job} {verdict.key()}")
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
    return owed_lines, metrics, next_state, errors


HELP = """# HELP foundation_asset_lag_seconds Newest unread input change less the read version's commit time (root ADR-0333).
# TYPE foundation_asset_lag_seconds gauge
# HELP foundation_asset_owed Whether an input of the asset moved past what its last run read.
# TYPE foundation_asset_owed gauge
# HELP foundation_asset_unscheduled_since_timestamp_seconds Since when the owed asset has had room in its pool and no run.
# TYPE foundation_asset_unscheduled_since_timestamp_seconds gauge
# HELP foundation_asset_reconcile_error Whether the reconciler could not read the asset this tick.
# TYPE foundation_asset_reconcile_error gauge
# HELP foundation_asset_reconcile_timestamp_seconds When the asset reconciler last finished a tick.
# TYPE foundation_asset_reconcile_timestamp_seconds gauge
"""


def write_replacing(path: Path, text: str) -> None:
    """Replaced whole by a rename, so a scrape or the next tick never reads half of it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".next")
    temporary.write_text(text, encoding="utf-8")
    temporary.chmod(0o644)
    temporary.replace(path)


def last_read(bake_root: Path):
    def read(lane: str):
        path = bake_root / lane / READS
        try:
            lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        except FileNotFoundError:
            return None
        return json.loads(lines[-1]) if lines else None
    return read


def main() -> int:
    jobs = json.loads(job_specs.JOBS.read_text(encoding="utf-8"))
    graph = json.loads(job_specs.GRAPH.read_text(encoding="utf-8"))
    found = assets(jobs, graph, gold_rebuild.load_contract())
    services = {job["systemd_service"] for job in jobs["jobs"]}
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
            cache[name] = catalog.table(name)
        return cache[name]

    def running(service):
        assert service in services
        return bool(job_units.running([service]))

    def pool_full(job_id):
        return job_units.pool_full(job_specs.JOBS, job_id)

    owed, metrics, state, errors = tick(time.time(), found, table, last_read(bake_root), running, pool_full, state)
    write_replacing(state_dir / STATE_FILE, json.dumps(state, sort_keys=True) + "\n")
    write_replacing(metrics_file, HELP + "\n".join(metrics) + "\n")
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
