#!/usr/bin/env python3
"""The host's one answer to "which jobs are registered" and "is this unit running" (root ADR-0184).

    job_units.py services <jobs.v1.json>       every registered job's systemd service, one per line
    job_units.py running <unit|pattern>...     each matching service that runs, "<unit> <ActiveState>"

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
import subprocess
import sys

# Every ActiveState that is not one of these is a unit at work: activating, active, deactivating,
# reloading, refreshing, maintenance, or one systemd adds later.
STOPPED_STATES = frozenset({"inactive", "failed"})


def is_running(active_state: str) -> bool:
    return active_state not in STOPPED_STATES


def registered_services(jobs_file) -> list[str]:
    """Every registered job's service, a disabled one too: an operator starts a disabled lane for its
    supervised run (2026-10-11: a deploy that waited only for the enabled ones met one running)."""
    with open(jobs_file, encoding="utf-8") as handle:
        return sorted({job["systemd_service"] for job in json.load(handle)["jobs"]})


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
    print("usage: job_units.py services <jobs.v1.json> | running <unit|pattern>...", file=sys.stderr)
    return 64


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
