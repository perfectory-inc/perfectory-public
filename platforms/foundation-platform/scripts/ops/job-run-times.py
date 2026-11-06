#!/usr/bin/env python3
"""Measure how long each scheduled job's runs took, from systemd's own journal (root ADR-0386).

The numbers a job's `per_run_cap.measured_run_seconds` states in orchestration/jobs.v1.json come
from here, so anyone can measure them again the same way. Read-only: it reads the journal the
caller pipes in and prints one JSON object per job; it writes nothing.

  journalctl _PID=1 --since 2026-10-16 -o json \\
      --output-fields=UNIT,MESSAGE_ID,MESSAGE,__REALTIME_TIMESTAMP \\
    | python3 scripts/ops/job-run-times.py [--from <job>=<UTC time> ...]

A run is systemd's start of the job's unit to the first event that ends it (finished,
deactivated, failed); a run whose end says `timeout` counts as a timeout. Every run counts, failed
ones too: a run that failed early is still a run the limit held. `--from` starts one job's window
later, at the merge of the change that fixed its runs, which the job's entry then names.
"""

from __future__ import annotations

import argparse
import datetime
import io
import json
import math
import sys
from pathlib import Path

JOBS = Path(__file__).resolve().parents[2] / "orchestration" / "jobs.v1.json"
STARTING = "7d4958e842da4a758f6c1cdc7b36dcc5"  # systemd: a unit's start job began
FINISHED = "39f53479d3a045ac8e11786248231fbf"  # systemd: a unit's start job ended
FAILED = "d9b373ed55a64feb8242e02dbe79a49c"  # systemd: a unit entered the failed state


def utc(seconds: float) -> str:
    return datetime.datetime.fromtimestamp(seconds, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def runs(lines, units):
    """Each unit's runs as (start seconds, duration seconds, timed out), from journal JSON lines."""
    started, out = {}, {unit: [] for unit in units}
    for line in lines:
        try:
            event = json.loads(line)
        except ValueError:
            continue
        unit = event.get("UNIT")
        if unit not in out:
            continue
        at = int(event["__REALTIME_TIMESTAMP"]) / 1e6
        message = event.get("MESSAGE") or ""
        if event.get("MESSAGE_ID") == STARTING:
            started[unit] = at
        elif unit in started and (event.get("MESSAGE_ID") in (FINISHED, FAILED)
                                  or "Failed with result" in message
                                  or "Deactivated successfully" in message):
            start = started.pop(unit)
            out[unit].append((start, at - start, "timeout" in message))
    return out


def measured(unit_runs, since: float, until: str):
    """The `measured_run_seconds` block for one job's runs that started at or after `since`."""
    kept = [run for run in unit_runs if run[0] >= since]
    if not kept:
        return None
    seconds = sorted(round(run[1]) for run in kept)

    def rank(share: float) -> int:  # nearest rank
        return seconds[math.ceil(len(seconds) * share) - 1]

    return {"runs": len(seconds), "p50": rank(0.5), "p90": rank(0.9), "max": seconds[-1],
            "timeouts": sum(1 for run in kept if run[2]),
            "from": utc(min(run[0] for run in kept)), "to": until}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--from", dest="starts", action="append", default=[], metavar="JOB=UTC")
    args = parser.parse_args(argv)
    starts = {}
    for item in args.starts:
        job, _, at = item.partition("=")
        starts[job] = datetime.datetime.fromisoformat(at.replace("Z", "+00:00")).timestamp()
    jobs = json.loads(JOBS.read_text(encoding="utf-8"))["jobs"]
    by_unit = runs(io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="replace"), {job["systemd_service"] for job in jobs})
    until = utc(datetime.datetime.now(datetime.timezone.utc).timestamp())
    for job in jobs:
        block = measured(by_unit[job["systemd_service"]], starts.get(job["id"], 0), until)
        print(json.dumps({"job": job["id"], "measured_run_seconds": block}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
