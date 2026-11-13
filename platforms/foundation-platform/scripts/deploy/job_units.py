#!/usr/bin/env python3
"""The host's one answer to "which jobs are registered" and "is this unit running" (root ADR-0184).

    job_units.py services <jobs.v1.json>       every registered job's systemd service, one per line
    job_units.py running <unit|pattern>...     each matching service that runs, "<unit> <ActiveState>"
    job_units.py deferred-dir                  where the runner records a job a release build deferred
    job_units.py deferred                      the job ids recorded there, one per line
    job_units.py pool-full <jobs.v1.json> <job id|service>
                                               why the job's pool has no room for it now, or the host no
                                               memory for what it claims (root ADR-0563), or nothing
    job_units.py claim <jobs.v1.json> <claimant> <its unit>
                                               queue the claimant's claim on its pool's slots, which counts
                                               while that unit runs (root ADR-0435)
    job_units.py release <claimant>            take the claimant's claim off the queue
    job_units.py queue <job id> <reason>       record a start its pool refused, to be started again (root ADR-0462)
    job_units.py dequeue <job id>              take a job that starts now off the start queue, and say how
                                               long it waited
    job_units.py preempted <jobs.v1.json> <job id>
                                               why a running job of a class that yields at a step boundary
                                               must yield its slots now, or nothing (root ADR-0435)
    job_units.py leftover-failed <jobs.v1.json>
                                               each failed unit no declaration runs again, whose failed
                                               state the deploy clears (root ADR-0445)
    job_units.py unretried-failed                  each failed unit that declares Restart= but was never
                                               restarted, which the deploy starts once (root ADR-0445 §5)

A unit runs unless its ActiveState is `inactive` or `failed`. A oneshot job is `activating` for its
whole run and never `active`, so `systemctl is-active` calls a running job stopped; that let a Gold
rebuild start beside a running one (2026-10-10). Patterns are systemctl's globs; a unit systemd has
not loaded is not running.

Release admission imports this file from the control checkout; the deploy, the autodeploy, the
operator script and the scheduler's dispatcher run it. Nothing else asks systemd whether a job runs.

Exit 0 when systemd answered, whether or not anything runs; 2 when it could not be asked, which no
caller may read as "nothing runs".
"""

from __future__ import annotations

import fnmatch
import json
import os
import re
import subprocess
import sys
import time

# Every ActiveState that is not one of these is a unit at work: activating, active, deactivating,
# reloading, refreshing, maintenance, or one systemd adds later.
STOPPED_STATES = frozenset({"inactive", "failed"})
# Where the scheduler's runner (start-scheduled-job.sh) records a job it did not start because a
# release build was running, one file per job id; the deploy starts each such job once it gives the
# schedule back, and the runner removes the file when the job starts (root ADR-0260).
DEFERRED_DIR = "/var/lib/foundation-scheduler/deferred"
JOB_ID = re.compile(r"[a-z][a-z0-9_]*")
# The host's capacity queue (root ADR-0435): one claim file per claimant of jobs.v1.json
# `capacity_claimants`, written by root (the claimants run as root) and read by every start.
CLAIMS_DIR = "/var/lib/foundation-run-now/capacity"
CLAIMANT = re.compile(r"[a-z][a-z0-9_]*")
# The host's start queue (root ADR-0462): one record per job whose start its pool refused, written by the
# scheduler's runner and read by the asset reconciler, which starts the job again once its pool has room.
# A pool-full refusal used to end the start with nothing recorded, so a daily job had one try a day
# (2026-11-09 07:00Z: ten first Silver loads refused behind a lane cut-over's claim, none started again).
QUEUE_DIR = "/var/lib/foundation-jobs/queue"
# The last start of each queued job, kept beside its record for the queue wait metric; every start is
# also appended to the history.
STARTED_SUFFIX = ".started.json"
QUEUE_HISTORY = "history.jsonl"
# When each job last started (start-scheduled-job.sh writes it, one file per job id holding the epoch
# second; a check that found nothing to do counts as its start, root ADR-0305). Beside the queue, so the
# asset reconciler (foundation-platform) reads what the takes_turns rule reads (root ADR-0467): it was
# under the scheduler's 0750 home, which only the scheduler could read, and a refusal made before the
# queue existed (2026-11-09 11:40Z, data_go_kr_file_data's first run) had no record anything could see.
STARTS_DIR = "/var/lib/foundation-jobs/queue/starts"
# Written by the deploy once it has copied the start records of the home's old directory over, so a
# job with no record here is one that never started, not one whose record was not copied yet.
STARTS_SEEDED = ".seeded"
# Every outcome the scheduler's runner said for a job (root ADR-0480): one appended line per run, check
# answer or start it did not make, beside the queue, read by the asset reconciler and the readiness
# report. A run used to leave its outcome only in its own journal and a start the runner refused left
# it nowhere, so "succeeded and produced nothing" was never a state anything kept (2026-11-09: the
# data_go_kr file data's first start skipped, 2026-11-06..: the shape ledger check waiting for a chain
# never provisioned).
OUTCOMES = "outcomes.jsonl"
# The outcome words of root ADR-0260 §1, and `skipped` (paused, or an operator's run-now not used).
OUTCOME_WORDS = ("changed", "unchanged", "waiting", "failed", "skipped")
# Where the kernel says how much memory a new workload can take without swapping (MemAvailable): what
# a job's memory claim is admitted against (root ADR-0563).
MEMINFO = "/proc/meminfo"
SIZE = re.compile(r"(\d+)([kmg])")


def deferred_dir() -> str:
    return os.environ.get("FOUNDATION_SCHEDULED_DEFERRED_DIR") or DEFERRED_DIR


def deferred() -> list[str]:
    """The ids of the jobs deferred while a release built and not started since."""
    try:
        names = os.listdir(deferred_dir())
    except FileNotFoundError:
        return []
    return sorted(name for name in names if JOB_ID.fullmatch(name))


def is_running(active_state: str) -> bool:
    return active_state not in STOPPED_STATES


def registered_services(jobs_file) -> list[str]:
    """Every registered job's service, a disabled one too: an operator starts a disabled lane for its
    supervised run (2026-10-11: a deploy that waited only for the enabled ones met one running)."""
    with open(jobs_file, encoding="utf-8") as handle:
        jobs = json.load(handle)["jobs"]
    # A job's check service (root ADR-0277) runs the release's publisher too: admitted and waited for alike.
    checks = {job["check_service"] for job in jobs if job.get("check_service")}
    return sorted({job["systemd_service"] for job in jobs} | checks)


def running(patterns, systemctl: str = "systemctl", env=None) -> dict[str, str]:
    """{unit: ActiveState} of the services matching `patterns` that run. Raises when systemd cannot answer."""
    if not patterns:
        return {}
    listing = subprocess.run([systemctl, "list-units", "--all", "--full", "--plain", "--no-legend",
                              "--type=service", *patterns],
                             check=True, capture_output=True, text=True, env=env).stdout
    result = {}
    for line in listing.splitlines():
        fields = line.split()
        # Some systemd versions mark a failed unit with a glyph before its name.
        if fields and "." not in fields[0]:
            fields = fields[1:]
        if len(fields) >= 3 and is_running(fields[2]):
            result[fields[0]] = fields[2]
    return result


def template(service: str) -> str | None:
    """`foundation-x@` for an instance `foundation-x@<name>.service`, else None."""
    return service[:service.index("@") + 1] if "@" in service else None


def failed(systemctl: str = "systemctl") -> list[dict]:
    """The failed foundation units as systemd lists them: {"unit", "load", "active", ...}."""
    return json.loads(subprocess.run([systemctl, "list-units", "--all", "--state=failed", "--output=json",
                                      "--no-pager", "foundation-*.service"],
                                     check=True, capture_output=True, text=True).stdout or "[]")


def show(units, systemctl: str = "systemctl") -> dict[str, dict]:
    """{unit: {property: value}} of what a failed unit's report needs; timestamps as `@<epoch>`."""
    if not units:
        return {}
    output = subprocess.run([systemctl, "show", "--timestamp=unix", "-p", "Id", "-p", "ActiveState",
                             "-p", "StateChangeTimestamp", "-p", "FragmentPath", "-p", "Documentation",
                             "-p", "Restart", "-p", "NRestarts",
                             "--", *units], check=True, capture_output=True, text=True).stdout
    shown = {}
    for block in output.strip().split("\n\n"):
        unit = dict(line.split("=", 1) for line in block.splitlines() if "=" in line)
        if unit.get("Id"):
            shown[unit["Id"]] = unit
    return shown


def failed_or_running(unit: dict) -> bool:
    """Whether a unit `show` reported is failed or at work; a unit it did not report is neither."""
    state = unit.get("ActiveState", "inactive")
    return state == "failed" or is_running(state)


def since(unit: dict) -> int:
    """When the unit entered its state, epoch seconds (0 when systemd has no time for it)."""
    return int((unit.get("StateChangeTimestamp") or "").removeprefix("@") or 0)


# How long an operator's own instance of a job's template (a rehearsal) keeps its failed state.
LEFTOVER_AFTER_SECONDS = 24 * 3600


def leftover_failed(jobs_file, systemctl: str = "systemctl", now=None) -> list[str]:
    """The failed foundation units that nothing declared runs again (root ADR-0445): a unit whose
    file a release removed (systemd keeps its failed state after the file is gone; the 공매 bake
    retired 11-05 sat failed and not-found), and an instance of a job's template that no job names,
    a day after it failed (the parcel tile bake's operator rehearsals, failed 10-15/16 and still
    failed three weeks later). A failed state is then always a declared unit that is broken now."""
    with open(jobs_file, encoding="utf-8") as handle:
        services = {job["systemd_service"] for job in json.load(handle)["jobs"]}
    templates = {template(service) for service in services} - {None}
    listed = failed(systemctl)
    gone = {entry["unit"] for entry in listed if entry.get("load") == "not-found"}
    instances = {entry["unit"] for entry in listed
                 if entry["unit"] not in gone and template(entry["unit"]) in templates and entry["unit"] not in services}
    now = time.time() if now is None else now
    shown = show(sorted(instances), systemctl)
    old = {unit for unit in instances if now - since(shown.get(unit, {})) >= LEFTOVER_AFTER_SECONDS}
    return sorted(gone | old)


def unretried_failed(systemctl: str = "systemctl") -> list[str]:
    """The failed foundation units that declare a retry (`Restart=`) and never had one (`NRestarts=0`).

    A failure before its unit's retry policy was installed is never retried: systemd retries only the
    failures it sees under that policy. 2026-11-08: the backup cut by the 03:43Z OOM and the two drills
    that failed at 04:54Z/05:30Z were still failed after the 18:01Z release gave them retries, and their
    next timers were 11-09 and 11-15. A failure retried under the policy has `NRestarts` above 0, and
    the unit's `StartLimitBurst` counts these starts too, so a deploy cannot start one without end."""
    listed = [entry["unit"] for entry in failed(systemctl) if entry.get("load") == "loaded"]
    shown = show(listed, systemctl)
    return sorted(unit for unit in listed
                  if shown.get(unit, {}).get("Restart", "no") != "no" and shown[unit].get("NRestarts") == "0")


def claims_dir() -> str:
    return os.environ.get("FOUNDATION_CAPACITY_CLAIMS_DIR") or CLAIMS_DIR


def queue_dir() -> str:
    return os.environ.get("FOUNDATION_START_QUEUE_DIR") or QUEUE_DIR


def starts_dir() -> str:
    return os.environ.get("FOUNDATION_SCHEDULED_STARTS_DIR") or STARTS_DIR


def last_starts() -> dict[str, int] | None:
    """{job id: when it last started, epoch seconds}, or None until the deploy has seeded the records
    (STARTS_SEEDED): before that a job with no record may only have one the deploy has not copied yet."""
    if not os.path.exists(os.path.join(starts_dir(), STARTS_SEEDED)):
        return None
    found = {}
    for name in sorted(os.listdir(starts_dir())):
        if not JOB_ID.fullmatch(name):
            continue  # the marker, or a partial write (`.next`)
        try:
            with open(os.path.join(starts_dir(), name), encoding="utf-8") as handle:
                found[name] = int(handle.read().strip())
        except (OSError, ValueError):
            continue
    return found


def _replace_json(path: str, record: dict) -> None:
    with open(f"{path}.next", "w", encoding="utf-8") as handle:
        json.dump(record, handle, sort_keys=True)
    os.chmod(f"{path}.next", 0o644)
    os.replace(f"{path}.next", path)


def _queue_files(suffix: str) -> dict[str, dict]:
    try:
        names = sorted(os.listdir(queue_dir()))
    except FileNotFoundError:
        return {}
    found = {}
    for name in names:
        job_id = name.removesuffix(suffix)
        if not name.endswith(suffix) or not JOB_ID.fullmatch(job_id):
            continue  # another kind of file, or a partial write (`.next`)
        try:
            with open(os.path.join(queue_dir(), name), encoding="utf-8") as handle:
                found[job_id] = json.load(handle)
        except (OSError, ValueError):
            continue
    return found


def queued() -> dict[str, dict]:
    """{job id: its record} of every start a pool refused and no start of the job has taken off since."""
    return _queue_files(".json")


def last_started() -> dict[str, dict]:
    """{job id: the record of its last start that had waited in the queue}."""
    return _queue_files(STARTED_SUFFIX)


def queue(job_id, reason, now) -> dict:
    """Records that `job_id`'s start was refused by its pool: since the first refusal not followed by a
    start, how many times, and the last reason. The record is what the reconciler starts again."""
    if not JOB_ID.fullmatch(job_id):
        raise KeyError(f"{job_id} is not a job id")
    os.makedirs(queue_dir(), mode=0o755, exist_ok=True)
    earlier = queued().get(job_id) or {}
    record = {"since": int(earlier.get("since", now)), "attempts": int(earlier.get("attempts", 0)) + 1,
              "last_at": int(now), "reason": str(reason)[:500]}
    _replace_json(os.path.join(queue_dir(), f"{job_id}.json"), record)
    return record


def dequeue(job_id, now) -> int | None:
    """Takes `job_id` off the queue as it starts; the seconds it waited since its first refusal, or None
    when it was not queued. Its start is kept as its last one and appended to the history."""
    if not JOB_ID.fullmatch(job_id):
        raise KeyError(f"{job_id} is not a job id")
    record = queued().get(job_id)
    if record is None:
        return None
    waited = max(int(now) - int(record["since"]), 0)
    started = {"job": job_id, "since": int(record["since"]), "started_at": int(now), "waited_seconds": waited,
               "attempts": int(record.get("attempts", 1))}
    _replace_json(os.path.join(queue_dir(), f"{job_id}{STARTED_SUFFIX}"), started)
    with open(os.path.join(queue_dir(), QUEUE_HISTORY), "a", encoding="utf-8") as handle:
        handle.write(json.dumps(started, sort_keys=True) + "\n")
    try:
        os.unlink(os.path.join(queue_dir(), f"{job_id}.json"))
    except FileNotFoundError:
        pass
    return waited


def outcomes_path() -> str:
    return os.path.join(queue_dir(), OUTCOMES)


def failed_code_version(job_id: str) -> str | None:
    """The code version of `job_id` in this file's own release (orchestration/dags/job_code_version.py,
    root ADR-0499): the runner runs the job from the tree it runs from. None when it cannot be made."""
    dags = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "orchestration", "dags")
    if dags not in sys.path:
        sys.path.insert(0, dags)
    try:
        import job_code_version  # noqa: PLC0415 - only for a failed run's line

        return job_code_version.version(job_code_version.job_of(job_id))
    except (ImportError, OSError, ValueError, KeyError) as error:
        print(f"job_units: no code version for {job_id}: {error}", file=sys.stderr)
        return None


def record_outcome(job_id, line, now, via="run", invocation="", code_version=failed_code_version) -> dict:
    """Appends what a run of `job_id` said, its `foundation-job-outcome <word> [reason]` line, or a
    check's `foundation-job-check <answer>` (via `check`). One line, never rewritten (append-only).

    A run that failed carries its job's code version (root ADR-0499): the asset reconciler starts it
    once more when a later release changes that version, where it would wait for its next slot."""
    if not JOB_ID.fullmatch(job_id):
        raise KeyError(f"{job_id} is not a job id")
    words = str(line).strip().split(" ", 2)
    if via == "check":
        if words[:1] != ["foundation-job-check"] or len(words) < 2:
            raise ValueError(f"not a check answer: {line!r}")
        # A check that found nothing to do was the job's turn and its outputs hold its inputs (root ADR-0305).
        outcome = "unchanged" if words[1] == "unchanged" else "checked"
    elif words[:1] == ["foundation-job-outcome"] and len(words) >= 2 and words[1] in OUTCOME_WORDS:
        outcome = words[1]
    else:
        raise ValueError(f"not an outcome line: {line!r}")
    record = {"job": job_id, "at": int(now), "outcome": outcome, "via": via,
              "reason": (words[2] if len(words) > 2 else "")[:300]}
    if invocation:
        record["invocation"] = str(invocation)
    if via == "run" and outcome == "failed" and (version := code_version(job_id)):
        record["code_version"] = version
    os.makedirs(queue_dir(), mode=0o755, exist_ok=True)
    new = not os.path.exists(outcomes_path())
    with open(outcomes_path(), "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
    if new:
        os.chmod(outcomes_path(), 0o644)  # the reconciler (foundation-platform) reads it
    return record


def outcomes_after(offset=0) -> tuple[list[dict], int]:
    """The outcome records appended after byte `offset`, and the offset to read from next. A ledger
    shorter than `offset` (replaced) is read from its start; a line still being written is left for
    the next read."""
    try:
        with open(outcomes_path(), "rb") as handle:
            size = os.fstat(handle.fileno()).st_size
            if size < offset:
                offset = 0
            handle.seek(offset)
            data = handle.read()
    except FileNotFoundError:
        return [], 0
    complete = data[: data.rfind(b"\n") + 1]
    records = []
    for raw in complete.splitlines():
        try:
            record = json.loads(raw)
        except ValueError:
            continue
        if isinstance(record, dict) and JOB_ID.fullmatch(str(record.get("job", ""))):
            records.append(record)
    return records, offset + len(complete)


def priority_class(listing, entry) -> dict:
    """A job's or a claimant's priority class (`default` when it names none); a list that declares no
    classes has one, of value 0 (job_specs.py refuses a class the list does not declare)."""
    return (listing.get("priority_classes") or {}).get(entry.get("priority_class", "default"), {"value": 0})


def priority(listing, entry) -> int:
    return priority_class(listing, entry)["value"]


def aged(listing, entry, since, now) -> float:
    """The priority of what has waited since `since`: its class's value, raised by the list's
    queue_aging for every hour it waited, up to its cap (root ADR-0462; Slurm's multifactor age factor),
    so a start a higher class keeps refusing outranks it in the end and nothing starves."""
    aging = listing.get("queue_aging") or {}
    waited_hours = max(now - since, 0) / 3600 if since is not None else 0.0
    return priority(listing, entry) + min(waited_hours * aging.get("points_per_hour", 0), aging.get("max_points", 0))


def live_claims(listing, running_units=running, now=None) -> list[dict]:
    """The queued claims whose unit runs, each with its claimant's pool, slots and priority.

    A claim names the systemd unit its claimant runs in; once that unit stops (it finished, failed or
    was killed) the claim holds nothing, so no crash leaves the pool reserved. Its priority is aged
    from when it was queued (`aged`)."""
    now = time.time() if now is None else now
    declared = listing.get("capacity_claimants") or {}
    try:
        names = sorted(os.listdir(claims_dir()))
    except FileNotFoundError:
        return []
    claims = []
    for name in names:
        claimant = name.removesuffix(".json")
        if not name.endswith(".json") or claimant not in declared:
            continue  # a partial write, or no claimant of this list
        try:
            with open(os.path.join(claims_dir(), name), encoding="utf-8") as handle:
                record = json.load(handle)
        except (OSError, ValueError):
            continue
        unit = record.get("unit") or ""
        if not fnmatch.fnmatchcase(unit, declared[claimant]["unit"]) or not running_units([unit]):
            continue
        claims.append({**record, "claimant": claimant, "pool": declared[claimant]["pool"],
                       "slots": declared[claimant]["pool_slots"],
                       "priority": aged(listing, declared[claimant], record.get("since"), now),
                       "priority_class": declared[claimant]["priority_class"]})
    return claims


def claim(jobs_file, claimant, unit, now) -> None:
    """Queues `claimant`'s claim for the unit it runs in, replacing its earlier one."""
    with open(jobs_file, encoding="utf-8") as handle:
        declared = json.load(handle).get("capacity_claimants") or {}
    if not CLAIMANT.fullmatch(claimant) or claimant not in declared:
        raise KeyError(f"{claimant} is not a capacity claimant of {jobs_file}")
    if not fnmatch.fnmatchcase(unit, declared[claimant]["unit"]):
        raise KeyError(f"{unit} is not a unit {claimant} runs in ({declared[claimant]['unit']})")
    os.makedirs(claims_dir(), mode=0o755, exist_ok=True)
    path = os.path.join(claims_dir(), f"{claimant}.json")
    with open(f"{path}.next", "w", encoding="utf-8") as handle:
        json.dump({"unit": unit, "since": int(now)}, handle)
    os.chmod(f"{path}.next", 0o644)
    os.replace(f"{path}.next", path)


def release(claimant) -> None:
    if not CLAIMANT.fullmatch(claimant):
        raise KeyError(f"{claimant} is not a claimant name")
    try:
        os.unlink(os.path.join(claims_dir(), f"{claimant}.json"))
    except FileNotFoundError:
        pass


def _waiting_claims(listing, pool, rank, running_units, *, equal, since=None, now=None) -> list[dict]:
    """The live claims on `pool` above `rank`, or (`equal`) at it too when queued no later than `since`
    (a start that has not waited is queued after every claim): a claim queued first is served first
    within its priority, as in any priority queue."""
    return [found for found in live_claims(listing, running_units, now)
            if found["pool"] == pool and (found["priority"] > rank or equal and found["priority"] == rank
                                          and (since is None or (found.get("since") or 0) <= since))]


def meminfo_path() -> str:
    return os.environ.get("FOUNDATION_HOST_MEMINFO") or MEMINFO


def size_bytes(text: str) -> int:
    """`6g`, `512m` or `64k` (the units the memory contracts use) as bytes."""
    match = SIZE.fullmatch(str(text).strip().lower())
    if match is None:
        raise ValueError(f"{text!r} is not a size like 6g")
    return int(match.group(1)) << {"k": 10, "m": 20, "g": 30}[match.group(2)]


def available_memory() -> int:
    """The host's MemAvailable, bytes: the kernel's own estimate of what a new workload can take."""
    with open(meminfo_path(), encoding="ascii") as handle:
        for line in handle:
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) << 10
    raise ValueError(f"{meminfo_path()} states no MemAvailable")


def unit_memory(units, systemctl: str = "systemctl") -> dict[str, tuple[int, int]]:
    """{unit: (its MemoryMax, what it holds now)} in bytes; a unit with no cap, or one systemd does not
    know, is a ValueError: a claim on memory nobody bounded cannot be sized."""
    output = subprocess.run([systemctl, "show", "-p", "Id", "-p", "MemoryMax", "-p", "MemoryCurrent", "--", *units],
                            check=True, capture_output=True, text=True).stdout
    shown = {}
    for block in output.strip().split("\n\n"):
        unit = dict(line.split("=", 1) for line in block.splitlines() if "=" in line)
        if unit.get("Id"):
            shown[unit["Id"]] = unit
    sized = {}
    for name in units:
        cap = shown.get(name, {}).get("MemoryMax", "")
        if not cap.isdigit():
            raise ValueError(f"{name} states no MemoryMax a memory claim can be sized by ({cap or 'not loaded'})")
        held = shown[name].get("MemoryCurrent", "")
        sized[name] = (int(cap), int(held) if held.isdigit() else 0)
    return sized


def memory_short(listing, job, named, unit_sizes=unit_memory, available=available_memory) -> str:
    """Why the host has no memory now for what `job` claims (`memory_claim`, root ADR-0563), or "".

    The claim is the MemoryMax of the units the job names: memory its run takes that no pool slot
    stands for (the local model's weights, held by the model runtime unit while the job runs). It is
    admitted against what the host has now, not against the caps of what could run: what a claimed
    unit already holds is counted as taken, and `memory_claims.keep_available` stays free after it. A
    start this refuses is queued and aged like one its pool refused (root ADR-0462). 2026-11-13: the
    fact job claimed every spark slot to keep the 24G model off Spark, the pool was never empty, and
    it waited 24,600 s with 30 GiB or more available for 95% of the week."""
    claim = job.get("memory_claim")
    if not claim:
        return ""
    gib = 1 << 30
    try:
        keep = size_bytes((listing.get("memory_claims") or {})["keep_available"])
        sizes = unit_sizes(claim["units"])
        free = available()
    except (KeyError, ValueError) as error:
        return f"the memory claim of {named} cannot be sized: {error.args[0] if isinstance(error, KeyError) else error}"
    need = sum(max(cap - held, 0) for cap, held in sizes.values())
    if free - need >= keep:
        return ""
    return (f"the host has no memory for {named}'s claim: {', '.join(sorted(sizes))} may take {need / gib:.1f} GiB more,"
            f" {free / gib:.1f} GiB is available and {keep / gib:.1f} GiB stays free")


def pool_patterns(listing, pool) -> list[str]:
    """The service names and templates (`name@*.service`) of every job of `pool`: what holds its slots."""
    members = [job for job in listing["jobs"] if job["pool"] == pool]
    return sorted({job["systemd_service"] for job in members}
                  | {f"{template(job['systemd_service'])}*.service" for job in members if template(job["systemd_service"])})


def claim_work(listing, claimant) -> list[str]:
    """The units whose run is the work a claim of `claimant` holds its pool for: every job of the pool
    (what the claim waits for) and the claimant's own `work_units` (its Spark steps' units)."""
    declared = (listing.get("capacity_claimants") or {})[claimant]
    return sorted(set(pool_patterns(listing, declared["pool"])) | set(declared.get("work_units") or []))


def pool_full(jobs_file, unit, running_units=running) -> str:
    """Why `unit` (a job id, its service, or another instance of a templated job's service) does not
    fit in its pool beside what runs now, or "".

    Every running service of the pool holds slots, whoever started it: the scheduler, an operator, or
    a hand `systemctl start` (2026-10-18: an operator's one-off took the spark pool's three slots,
    Airflow did not know, and FLOOR ran beside it at fourteen times its usual time). A disabled job
    counts too; an operator starts those. An instance of a templated job that is not a job itself (a
    parcel tile rehearsal) holds as many slots as the most any of that template's jobs takes. A pool
    the job list gives no slots is not bounded here.

    A job that fits its pool and states a `memory_claim` must also find the host's memory for it
    (`memory_short`, root ADR-0563): the same answer, so the same queue.
    """
    with open(jobs_file, encoding="utf-8") as handle:
        listing = json.load(handle)
    jobs = listing["jobs"]

    def slots_of(service):
        exact = [job for job in jobs if job["systemd_service"] == service]
        alike = exact or [job for job in jobs if template(service) and template(job["systemd_service"]) == template(service)]
        return max(alike, key=lambda job: job.get("pool_slots", 1)) if alike else None

    named = next((job["systemd_service"] for job in jobs if job["id"] == unit), unit)
    mine = slots_of(named)
    if mine is None:
        raise KeyError(f"{unit} is not a job, or an instance of one, in {jobs_file}")
    pool = listing.get("pools", {}).get(mine["pool"])
    if pool is None:
        return memory_short(listing, mine, named)
    held = sorted(service for service in running_units(pool_patterns(listing, mine["pool"])) if service != named)
    taken = {service: slots_of(service).get("pool_slots", 1) for service in held}
    need = mine.get("pool_slots", 1)
    # A claim waiting for this pool keeps its slots free of any start of a lower class, and of a
    # later one of its own (root ADR-0435): on 2026-11-06 a backfill that started first held the pool
    # while the cut-over waited for it. A start its pool refused before has aged in the queue since its
    # first refusal (root ADR-0462), so in the end it outranks a claim that keeps being queued anew.
    now = time.time()
    job_id = next((job["id"] for job in jobs if job["systemd_service"] == named), None)
    since = (queued().get(job_id) or {}).get("since") if job_id else None
    above = _waiting_claims(listing, mine["pool"], aged(listing, mine, since, now), running_units, equal=True,
                            since=since, now=now)
    reserved = sum(found["slots"] for found in above)
    if sum(taken.values()) + need + reserved <= pool["slots"]:
        return memory_short(listing, mine, named)
    if sum(taken.values()) + need <= pool["slots"]:
        waiting = ", ".join(f"{found['claimant']} ({found['priority_class']}, {found['slots']} slots, waiting since"
                            f" {found.get('since')})" for found in above)
        return f"the {mine['pool']} pool is kept for claims queued before {named}: {waiting}"
    holders = ", ".join(f"{service} holds {count}" for service, count in taken.items())
    return f"the {mine['pool']} pool has no room for {named} ({need} of {pool['slots']} slots): {holders}"


def preempted(jobs_file, job_id, running_units=running) -> str:
    """Why the running job `job_id`, of a class that yields at a step boundary, must end at its next
    one, or "": a live claim of a higher class waits in its pool and what runs leaves it no room
    (root ADR-0435; Kubernetes preempts a lower PriorityClass, here the job stops itself between
    committed steps, so nothing it did is lost)."""
    with open(jobs_file, encoding="utf-8") as handle:
        listing = json.load(handle)
    job = next((job for job in listing["jobs"] if job["id"] == job_id), None)
    if job is None:
        raise KeyError(f"{job_id} is not a job in {jobs_file}")
    if not priority_class(listing, job).get("yields_at_step_boundary"):
        return ""
    pool = (listing.get("pools") or {}).get(job["pool"])
    if pool is None:
        return ""
    above = _waiting_claims(listing, job["pool"], priority(listing, job), running_units, equal=False)
    if not above:
        return ""
    members = [entry for entry in listing["jobs"] if entry["pool"] == job["pool"]]
    held = sum(next((entry.get("pool_slots", 1) for entry in members if entry["systemd_service"] == service), 1)
               for service in running_units(pool_patterns(listing, job["pool"])))
    if held + sum(found["slots"] for found in above) <= pool["slots"]:
        return ""
    names = ", ".join(f"{found['claimant']} ({found['priority_class']})" for found in above)
    return f"yielded the {job['pool']} pool to {names}, which waits for its slots"


def main(argv) -> int:
    if len(argv) == 2 and argv[0] == "services":
        print("\n".join(registered_services(argv[1])))
        return 0
    if len(argv) >= 2 and argv[0] == "running":
        try:
            units = running(argv[1:])
        except (OSError, subprocess.CalledProcessError) as error:
            print(f"job_units: systemd cannot report {' '.join(argv[1:])}: {error}", file=sys.stderr)
            return 2
        for unit, state in sorted(units.items()):
            print(unit, state)
        return 0
    if len(argv) == 3 and argv[0] == "pool-full":
        try:
            print(pool_full(argv[1], argv[2]))
        except (OSError, subprocess.CalledProcessError) as error:
            print(f"job_units: systemd cannot report the pool of {argv[2]}: {error}", file=sys.stderr)
            return 2
        except KeyError as error:
            print(f"job_units: {error.args[0]}", file=sys.stderr)
            return 64
        return 0
    if len(argv) == 4 and argv[0] == "claim":
        try:
            claim(argv[1], argv[2], argv[3], time.time())
        except (OSError, ValueError, KeyError) as error:
            print(f"job_units: {error.args[0] if isinstance(error, KeyError) else error}", file=sys.stderr)
            return 64
        return 0
    if len(argv) == 3 and argv[0] == "queue":
        try:
            record = queue(argv[1], argv[2], time.time())
        except (OSError, KeyError) as error:
            print(f"job_units: {error.args[0] if isinstance(error, KeyError) else error}", file=sys.stderr)
            return 64
        print(f"queued since {record['since']}, refused {record['attempts']} times")
        return 0
    if len(argv) == 2 and argv[0] == "dequeue":
        try:
            waited = dequeue(argv[1], time.time())
        except (OSError, KeyError) as error:
            print(f"job_units: {error.args[0] if isinstance(error, KeyError) else error}", file=sys.stderr)
            return 64
        print("" if waited is None else waited)
        return 0
    if len(argv) == 2 and argv[0] == "release":
        try:
            release(argv[1])
        except (OSError, KeyError) as error:
            print(f"job_units: {error.args[0] if isinstance(error, KeyError) else error}", file=sys.stderr)
            return 64
        return 0
    if len(argv) == 3 and argv[0] == "preempted":
        try:
            print(preempted(argv[1], argv[2]))
        except (OSError, subprocess.CalledProcessError) as error:
            print(f"job_units: systemd cannot report the pool of {argv[2]}: {error}", file=sys.stderr)
            return 2
        except KeyError as error:
            print(f"job_units: {error.args[0]}", file=sys.stderr)
            return 64
        return 0
    if len(argv) == 2 and argv[0] == "leftover-failed":
        try:
            print("\n".join(leftover_failed(argv[1])))
        except (OSError, subprocess.CalledProcessError, ValueError) as error:
            print(f"job_units: systemd cannot report the failed units: {error}", file=sys.stderr)
            return 2
        return 0
    if argv == ["unretried-failed"]:
        try:
            print("\n".join(unretried_failed()))
        except (OSError, subprocess.CalledProcessError, ValueError) as error:
            print(f"job_units: systemd cannot report the failed units: {error}", file=sys.stderr)
            return 2
        return 0
    if len(argv) in (3, 4, 5) and argv[0] == "record-outcome":
        try:
            record_outcome(argv[1], argv[2], time.time(), *argv[3:])
        except (OSError, ValueError, KeyError) as error:
            print(f"job_units: {error.args[0] if isinstance(error, KeyError) else error}", file=sys.stderr)
            return 64
        return 0
    if argv == ["deferred-dir"]:
        print(deferred_dir())
        return 0
    if argv == ["deferred"]:
        print("\n".join(deferred()))
        return 0
    print("usage: job_units.py services <jobs.v1.json> | running <unit|pattern>... | deferred-dir | deferred"
          " | pool-full <jobs.v1.json> <job id|service> | claim <jobs.v1.json> <claimant> <unit>"
          " | release <claimant> | queue <job id> <reason> | dequeue <job id> | preempted <jobs.v1.json> <job id> | leftover-failed <jobs.v1.json>"
          " | unretried-failed | record-outcome <job id> <outcome line> [run|check] [invocation]",
          file=sys.stderr)
    return 64


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
