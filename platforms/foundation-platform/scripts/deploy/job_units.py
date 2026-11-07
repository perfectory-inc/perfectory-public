#!/usr/bin/env python3
"""The host's one answer to "which jobs are registered" and "is this unit running" (root ADR-0184).

    job_units.py services <jobs.v1.json>       every registered job's systemd service, one per line
    job_units.py running <unit|pattern>...     each matching service that runs, "<unit> <ActiveState>"
    job_units.py deferred-dir                  where the runner records a job a release build deferred
    job_units.py deferred                      the job ids recorded there, one per line
    job_units.py pool-full <jobs.v1.json> <job id|service>
                                               why the job's pool has no room for it now, or nothing
    job_units.py claim <jobs.v1.json> <claimant> <its unit>
                                               queue the claimant's claim on its pool's slots, which counts
                                               while that unit runs (root ADR-0435)
    job_units.py release <claimant>            take the claimant's claim off the queue
    job_units.py preempted <jobs.v1.json> <job id>
                                               why a running job of a class that yields at a step boundary
                                               must yield its slots now, or nothing (root ADR-0435)

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


def claims_dir() -> str:
    return os.environ.get("FOUNDATION_CAPACITY_CLAIMS_DIR") or CLAIMS_DIR


def priority_class(listing, entry) -> dict:
    """A job's or a claimant's priority class (`default` when it names none); a list that declares no
    classes has one, of value 0 (job_specs.py refuses a class the list does not declare)."""
    return (listing.get("priority_classes") or {}).get(entry.get("priority_class", "default"), {"value": 0})


def priority(listing, entry) -> int:
    return priority_class(listing, entry)["value"]


def live_claims(listing, running_units=running) -> list[dict]:
    """The queued claims whose unit runs, each with its claimant's pool, slots and priority.

    A claim names the systemd unit its claimant runs in; once that unit stops (it finished, failed or
    was killed) the claim holds nothing, so no crash leaves the pool reserved."""
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
                       "priority": priority(listing, declared[claimant]),
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


def _waiting_claims(listing, pool, rank, running_units, *, equal) -> list[dict]:
    """The live claims on `pool` above `rank`, or (`equal`) at it too: a claim queued first is served
    first within its class, as in any priority queue."""
    return [found for found in live_claims(listing, running_units)
            if found["pool"] == pool and (found["priority"] > rank or equal and found["priority"] == rank)]


def pool_full(jobs_file, unit, running_units=running) -> str:
    """Why `unit` (a job id, its service, or another instance of a templated job's service) does not
    fit in its pool beside what runs now, or "".

    Every running service of the pool holds slots, whoever started it: the scheduler, an operator, or
    a hand `systemctl start` (2026-10-18: an operator's one-off took the spark pool's three slots,
    Airflow did not know, and FLOOR ran beside it at fourteen times its usual time). A disabled job
    counts too; an operator starts those. An instance of a templated job that is not a job itself (a
    parcel tile rehearsal) holds as many slots as the most any of that template's jobs takes. A pool
    the job list gives no slots is not bounded here.
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
        return ""
    members = [job for job in jobs if job["pool"] == mine["pool"]]
    patterns = sorted({job["systemd_service"] for job in members}
                      | {f"{template(job['systemd_service'])}*.service" for job in members if template(job["systemd_service"])})
    held = sorted(service for service in running_units(patterns) if service != named)
    taken = {service: slots_of(service).get("pool_slots", 1) for service in held}
    need = mine.get("pool_slots", 1)
    # A claim waiting for this pool keeps its slots free of any start of a lower class, and of a
    # later one of its own (root ADR-0435): on 2026-11-06 a backfill that started first held the pool
    # while the cut-over waited for it.
    above = _waiting_claims(listing, mine["pool"], priority(listing, mine), running_units, equal=True)
    reserved = sum(found["slots"] for found in above)
    if sum(taken.values()) + need + reserved <= pool["slots"]:
        return ""
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
    patterns = sorted({entry["systemd_service"] for entry in members}
                      | {f"{template(entry['systemd_service'])}*.service" for entry in members
                         if template(entry["systemd_service"])})
    held = sum(next((entry.get("pool_slots", 1) for entry in members if entry["systemd_service"] == service), 1)
               for service in running_units(patterns))
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
    if argv == ["deferred-dir"]:
        print(deferred_dir())
        return 0
    if argv == ["deferred"]:
        print("\n".join(deferred()))
        return 0
    print("usage: job_units.py services <jobs.v1.json> | running <unit|pattern>... | deferred-dir | deferred"
          " | pool-full <jobs.v1.json> <job id|service> | claim <jobs.v1.json> <claimant> <unit>"
          " | release <claimant> | preempted <jobs.v1.json> <job id>",
          file=sys.stderr)
    return 64


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
