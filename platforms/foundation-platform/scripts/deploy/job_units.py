#!/usr/bin/env python3
"""The host's one answer to "which jobs are registered" and "is this unit running" (root ADR-0184).

    job_units.py services <jobs.v1.json>       every registered job's systemd service, one per line
    job_units.py running <unit|pattern>...     each matching service that runs, "<unit> <ActiveState>"
    job_units.py deferred-dir                  where the runner records a job a release build deferred
    job_units.py deferred                      the job ids recorded there, one per line
    job_units.py pool-full <jobs.v1.json> <job id|service>
                                               why the job's pool has no room for it now, or nothing

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

import json
import os
import re
import subprocess
import sys

# Every ActiveState that is not one of these is a unit at work: activating, active, deactivating,
# reloading, refreshing, maintenance, or one systemd adds later.
STOPPED_STATES = frozenset({"inactive", "failed"})
# Where the scheduler's runner (start-scheduled-job.sh) records a job it did not start because a
# release build was running, one file per job id; the deploy starts each such job once it gives the
# schedule back, and the runner removes the file when the job starts (root ADR-0260).
DEFERRED_DIR = "/var/lib/foundation-scheduler/deferred"
JOB_ID = re.compile(r"[a-z][a-z0-9_]*")


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
    if sum(taken.values()) + need <= pool["slots"]:
        return ""
    holders = ", ".join(f"{service} holds {count}" for service, count in taken.items())
    return f"the {mine['pool']} pool has no room for {named} ({need} of {pool['slots']} slots): {holders}"


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
    if argv == ["deferred-dir"]:
        print(deferred_dir())
        return 0
    if argv == ["deferred"]:
        print("\n".join(deferred()))
        return 0
    print("usage: job_units.py services <jobs.v1.json> | running <unit|pattern>... | deferred-dir | deferred"
          " | pool-full <jobs.v1.json> <job id|service>",
          file=sys.stderr)
    return 64


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
