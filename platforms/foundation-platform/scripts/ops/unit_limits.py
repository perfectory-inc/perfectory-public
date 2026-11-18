#!/usr/bin/env python3
"""Every foundation unit's limits against what its runs measured, from systemd's own journal (root ADR-0569).

A limit is set from measurement and a unit near it says so before it fails. This reads, never
writes: systemd's job-start, failure, out-of-memory and resource lines (`Consumed …, <n> memory
peak`, the cgroup v2 `memory.peak` systemd logs when a unit stops) and the limits the units declare
(`systemctl show`). It prints the table the decision is reviewed against (`--table`, Markdown;
`--json`), and the asset reconciler writes its textfile (`textfile()`, every five minutes) for the
FoundationUnitNearItsLimit rule.

  python3 scripts/ops/unit_limits.py [--since -14d] [--table | --json]   # on the host

It runs `journalctl` (systemd's own lines, six message kinds) and `systemctl show` itself;
`--journal FILE` and `--declared FILE` read saved output instead (tests, a copy taken elsewhere).
`--prometheus URL` adds the Docker containers: their measured peak and cap from cAdvisor, and the
kernel's OOM kills inside each (`journalctl -k`), so a standing service killed at its cap shows too.

A run is a unit's job start to its resource line (systemd writes it when the unit stops), or to
its failure or deactivation when no resource line follows (a unit killed at its cap writes none). A
transient Docker scope (a container a unit started) has no job start, so its run begins at its
`Started` line; it is counted against the foundation unit whose run holds it, when exactly one does.
Every run counts, failed ones too.

What real incident does this prevent? 2026-11-13..18: onbid-silver-load's Spark died at its memory
cap 32 times and rt-molit-silver-load 23 of 92 runs, court-auction-collect ran at 96% of its 2 GiB,
and parcel-tile-bake held a 1.6 GB index in a 1 GiB unit until its 115-minute limit; nothing said a
unit was near its limit before it failed, and no table showed which limits were measured.
"""

from __future__ import annotations

import argparse
import io
import json
import math
import os
import re
import subprocess
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass, field

STARTING = "7d4958e842da4a758f6c1cdc7b36dcc5"  # a unit's start job began
STARTED = "39f53479d3a045ac8e11786248231fbf"  # a unit's start job ended (a scope's run begins here)
FAILED = "d9b373ed55a64feb8242e02dbe79a49c"  # a unit entered the failed state, UNIT_RESULT says why
OUT_OF_MEMORY = "fe6faa94e7774663a0da52717891d8ef"  # the kernel killed a process of this unit
RESOURCES = "ae8f7b866b0347b9af31fe1c80b127c0"  # Consumed …, MEMORY_PEAK: the unit stopped
DEACTIVATED = "7ad2d189f7e94e70a38c781354912448"  # the unit stopped successfully
MESSAGE_IDS = (STARTING, STARTED, FAILED, OUT_OF_MEMORY, RESOURCES, DEACTIVATED)
PREFIX = "foundation-"
SCOPE = "docker-"
# A run at or above this share of a limit is near it (root ADR-0569 §1).
NEAR = 0.8
# Consecutive newest runs near a limit before the rule raises it (root ADR-0569 §4).
NEAR_RUNS = 3
DURATION = re.compile(r"([0-9]+(?:\.[0-9]+)?)(us|ms|min|s|h|d|w)?")
JOURNAL_FIELDS = "UNIT,MESSAGE_ID,MESSAGE,MEMORY_PEAK,UNIT_RESULT,__REALTIME_TIMESTAMP"
SHOWN = ("Id", "MemoryMax", "MemoryHigh", "TimeoutStartUSec", "RuntimeMaxUSec", "Type")


@dataclass
class Run:
    start: float
    end: float | None = None
    peak: int | None = None
    result: str = "success"
    oom: bool = False
    containers: list = field(default_factory=list)  # (peak bytes, oom) of the Docker scopes it held

    @property
    def seconds(self) -> float | None:
        return None if self.end is None else self.end - self.start

    @property
    def outcome(self) -> str:
        """`oom-kill` when the unit or a container it started was killed at its cap, else systemd's result."""
        if self.oom or self.result == "oom-kill" or any(oom for _, oom in self.containers):
            return "oom-kill"
        return self.result


def runs(lines) -> dict[str, list[Run]]:
    """Each foundation unit's and Docker scope's runs, oldest first, from journal JSON lines."""
    open_runs: dict[str, Run] = {}
    out: dict[str, list[Run]] = {}
    for line in lines:
        try:
            event = json.loads(line)
        except ValueError:
            continue
        unit = event.get("UNIT") or ""
        scope = unit.startswith(SCOPE) and unit.endswith(".scope")
        if not (unit.startswith(PREFIX) or scope):
            continue
        at = int(event["__REALTIME_TIMESTAMP"]) / 1e6
        kind = event.get("MESSAGE_ID")
        message = event.get("MESSAGE") or ""
        if kind == STARTING or (scope and kind == STARTED):
            previous = open_runs.pop(unit, None)
            if previous is not None:
                out.setdefault(unit, []).append(previous)  # stopped with no resource line, or never: no peak
            open_runs[unit] = Run(start=at)
            continue
        current = open_runs.get(unit)
        if current is None:
            continue  # its start is before the window
        if kind == FAILED or "Failed with result" in message:
            quoted = message.split("'")
            current.result = event.get("UNIT_RESULT") or (quoted[1] if len(quoted) > 2 else "failed")
            current.end = at  # a unit killed at its cap logs no resource line: the failure ends its run
        elif kind == DEACTIVATED:
            current.end = at
        elif kind == OUT_OF_MEMORY:
            current.oom = True
        elif kind == RESOURCES:
            current.end = at
            peak = event.get("MEMORY_PEAK")
            current.peak = int(peak) if peak and peak.isdigit() else None
            out.setdefault(unit, []).append(open_runs.pop(unit))
    return out


def attribute_scopes(by_unit: dict[str, list[Run]]) -> dict[str, list[Run]]:
    """The foundation units' runs with each container counted against the one run that held it.

    A scope held by no run, or by two (two jobs of one pool at once), is left out: guessing would
    put one job's container on another's line."""
    units = {name: unit_runs for name, unit_runs in by_unit.items() if name.startswith(PREFIX)}
    spans = [(run.start, run.end, run) for unit_runs in units.values() for run in unit_runs if run.end is not None]
    for name, scope_runs in by_unit.items():
        if not name.startswith(SCOPE):
            continue
        for scope in scope_runs:
            if scope.end is None:
                continue
            holders = [run for start, end, run in spans if start <= scope.start and scope.end <= end]
            if len(holders) == 1:
                holders[0].containers.append((scope.peak or 0, scope.oom or scope.result == "oom-kill"))
    return units


def limit_bytes(raw: str | None) -> int | None:
    """systemd's MemoryMax/MemoryHigh: bytes, or None for `infinity`."""
    return int(raw) if raw and raw.isdigit() else None


def limit_seconds(raw: str | None) -> float | None:
    """systemd's TimeoutStartUSec/RuntimeMaxUSec as `show` prints them (`1h 55min`, `infinity`)."""
    if not raw or raw in ("infinity", "0"):
        return None
    units = {"us": 1e-6, "ms": 1e-3, "s": 1, "min": 60, "h": 3600, "d": 86400, "w": 604800}
    total = 0.0
    for number, unit in DURATION.findall(raw):
        total += float(number) * units[unit or "s"]
    return total or None


def declared(text: str) -> dict[str, dict]:
    """{unit: properties} from `systemctl show -p Id -p MemoryMax …` output."""
    shown = {}
    for block in text.strip().split("\n\n"):
        unit = dict(line.split("=", 1) for line in block.splitlines() if "=" in line)
        if unit.get("Id"):
            shown[unit["Id"]] = unit
    return shown


def show(units, systemctl: str = "systemctl") -> dict[str, dict]:
    if not units:
        return {}
    arguments = [systemctl, "show"] + [f"-p{name}" for name in SHOWN] + ["--", *units]
    return declared(subprocess.run(arguments, check=True, capture_output=True, text=True).stdout)


def rank(values: list[float], share: float) -> float:
    """Nearest-rank percentile, as job-run-times.py measures (root ADR-0386)."""
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * share) - 1)]


def time_limit(unit: dict) -> float | None:
    """The run's time limit: a oneshot's start timeout, else its runtime maximum."""
    start = limit_seconds(unit.get("TimeoutStartUSec"))
    runtime = limit_seconds(unit.get("RuntimeMaxUSec"))
    if unit.get("Type") == "oneshot":
        return start or runtime
    return runtime


def near_streak(ratios: list[float | None]) -> int:
    """How many of the newest runs, back from the last, were at or above NEAR."""
    streak = 0
    for ratio in reversed(ratios):
        if ratio is None or ratio < NEAR:
            break
        streak += 1
    return streak


def row(name: str, unit_runs: list[Run], unit: dict) -> dict:
    """One unit's line: declared limits, measured peak and run time, the failures, and its state."""
    memory_max = limit_bytes(unit.get("MemoryMax"))
    seconds_limit = time_limit(unit)
    finished = [run for run in unit_runs if run.end is not None]
    peaks = [run.peak for run in finished if run.peak is not None]
    times = [run.seconds for run in finished]
    container_peaks = [peak for run in finished for peak, _ in run.containers]
    outcomes = [run.outcome for run in finished]
    memory_ratios = [run.peak / memory_max if memory_max and run.peak is not None else None for run in finished]
    time_ratios = [run.seconds / seconds_limit if seconds_limit else None for run in finished]
    line = {
        "unit": name,
        "runs": len(finished),
        "memory_max": memory_max,
        "memory_high": limit_bytes(unit.get("MemoryHigh")),
        "time_limit_seconds": seconds_limit,
        "peak_p95": rank(peaks, 0.95) if peaks else None,
        "peak_max": max(peaks) if peaks else None,
        "container_peak_max": max(container_peaks) if container_peaks else None,
        "seconds_p50": round(rank(times, 0.5)) if times else None,
        "seconds_p95": round(rank(times, 0.95)) if times else None,
        "seconds_max": round(max(times)) if times else None,
        "oom": outcomes.count("oom-kill"),
        "timeout": outcomes.count("timeout"),
        "failed_other": sum(1 for outcome in outcomes if outcome not in ("success", "oom-kill", "timeout")),
        "near_memory_runs": near_streak(memory_ratios),
        "near_time_runs": near_streak(time_ratios),
        "last_run": finished[-1].end if finished else None,
    }
    line["memory_share"] = line["peak_max"] / memory_max if memory_max and line["peak_max"] else None
    line["time_share"] = line["seconds_max"] / seconds_limit if seconds_limit and line["seconds_max"] else None
    line["state"] = state(line)
    return line


def state(line: dict) -> str:
    """failing: a run died at a limit in the window; near: a run reached NEAR of one; else fits."""
    if line["oom"] or line["timeout"]:
        return "failing"
    if max(line["memory_share"] or 0, line["time_share"] or 0) >= NEAR:
        return "near"
    if not line["runs"]:
        return "unmeasured"
    return "fits"


def mib(value) -> str:
    return "—" if value is None else f"{value / (1 << 20):,.0f}M"


def duration(value) -> str:
    if value is None:
        return "—"
    return f"{value / 60:,.0f}m" if value >= 600 else f"{value:,.0f}s"


def share(value) -> str:
    return "—" if value is None else f"{value:.0%}"


def markdown(rows: list[dict]) -> str:
    head = ("| unit | runs | MemoryMax | peak p95 | peak max | % | container peak | time limit | p50 | p95 | max | % "
            "| oom | timeout | other failed | state |\n|" + "---|" * 16 + "\n")
    body = "".join(
        f"| {r['unit'].removeprefix(PREFIX).removesuffix('.service')} | {r['runs']} | {mib(r['memory_max'])} "
        f"| {mib(r['peak_p95'])} | {mib(r['peak_max'])} | {share(r['memory_share'])} | {mib(r['container_peak_max'])} "
        f"| {duration(r['time_limit_seconds'])} | {duration(r['seconds_p50'])} | {duration(r['seconds_p95'])} "
        f"| {duration(r['seconds_max'])} | {share(r['time_share'])} | {r['oom']} | {r['timeout']} | {r['failed_other']} "
        f"| {r['state']} |\n"
        for r in rows)
    return head + body


TEXTFILE_HELP = """# HELP foundation_unit_runs_near_limit How many of a unit's newest runs, back from the last, reached 80% of its memory or time limit (root ADR-0569 §4).
# TYPE foundation_unit_runs_near_limit gauge
# HELP foundation_unit_limit_share The newest finished run's measured peak memory or run time as a share of the unit's limit.
# TYPE foundation_unit_limit_share gauge
# HELP foundation_unit_limits_measured_seconds When the reconciler last measured the units' runs against their limits.
# TYPE foundation_unit_limits_measured_seconds gauge
"""


def label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def textfile(rows: list[dict], by_unit_runs: dict[str, list[Run]], now: float) -> str:
    """The series the FoundationUnitNearItsLimit rule reads: per unit and limit, the near streak and the
    newest run's share. A unit with no limit of that kind writes nothing for it."""
    lines = []
    for r in rows:
        unit = label(r["unit"])
        finished = [run for run in by_unit_runs.get(r["unit"], []) if run.end is not None]
        last = finished[-1] if finished else None
        if r["memory_max"]:
            lines.append(f'foundation_unit_runs_near_limit{{unit="{unit}",limit="memory"}} {r["near_memory_runs"]}\n')
            if last is not None and last.peak is not None:
                lines.append(f'foundation_unit_limit_share{{unit="{unit}",limit="memory"}} {last.peak / r["memory_max"]:.4f}\n')
        if r["time_limit_seconds"]:
            lines.append(f'foundation_unit_runs_near_limit{{unit="{unit}",limit="time"}} {r["near_time_runs"]}\n')
            if last is not None:
                lines.append(f'foundation_unit_limit_share{{unit="{unit}",limit="time"}} {last.seconds / r["time_limit_seconds"]:.4f}\n')
    return TEXTFILE_HELP + "".join(sorted(lines)) + f"foundation_unit_limits_measured_seconds {int(now)}\n"


METRICS_NAME = "unit-limits.prom"
# The reconciler measures at most this often: 14 days of systemd's lines take seconds to read and
# the reconciler's own unit has a three-minute limit.
EVERY_SECONDS = 1800


def due(path, now: float) -> bool:
    """Whether the textfile at `path` is missing or older than EVERY_SECONDS."""
    try:
        return now - os.stat(path).st_mtime >= EVERY_SECONDS
    except FileNotFoundError:
        return True


def listed_units(systemctl: str = "systemctl") -> set[str]:
    listed = subprocess.run([systemctl, "list-units", "--all", "--plain", "--no-legend", f"{PREFIX}*.service"],
                            check=True, capture_output=True, text=True).stdout
    return {line.split()[0] for line in listed.splitlines() if line.strip()}


def measure(now: float, since: str = "-14d") -> str:
    """The textfile, from the host's journal and systemd, read-only."""
    by_unit = attribute_scopes(runs(journal(since)))
    shown = show(sorted(listed_units() | set(by_unit)))
    names = sorted(set(by_unit) | {name for name in shown if name.endswith(".service")})
    rows = [row(name, by_unit.get(name, []), shown.get(name, {})) for name in names]
    return textfile(rows, by_unit, now)


def journal(since: str = "-14d", journalctl: str = "journalctl") -> list[str]:
    """The journal lines this reads: systemd's own (PID 1), its six message kinds only."""
    arguments = [journalctl, "_PID=1", f"--since={since}", "-o", "json", f"--output-fields={JOURNAL_FIELDS}"]
    arguments += [f"MESSAGE_ID={kind}" for kind in MESSAGE_IDS]
    return subprocess.run(arguments, check=True, capture_output=True, text=True).stdout.splitlines()


OOM_CGROUP = re.compile(r"oom_memcg=/system\.slice/docker-([0-9a-f]{64})\.scope")


def container_kills(kernel_lines) -> dict[str, int]:
    """{container id: OOM kills} from the kernel's `oom-kill:… oom_memcg=…docker-<id>.scope` lines."""
    kills: dict[str, int] = {}
    for line in kernel_lines:
        match = OOM_CGROUP.search(line)
        if match and "oom-kill:" in line:
            kills[match.group(1)] = kills.get(match.group(1), 0) + 1
    return kills


def container_rows(running: dict[str, str], peaks: dict[str, float], caps: dict[str, float],
                   kills: dict[str, int]) -> list[dict]:
    """One line per running container: its cap, the most it held in the window (cAdvisor working
    set, what the kernel cannot reclaim), and the OOM kills inside it. `running` is {id: name}."""
    rows = []
    for identifier, name in sorted(running.items(), key=lambda item: item[1]):
        cap = caps.get(identifier) or None
        peak = peaks.get(identifier)
        line = {"container": name, "memory_max": cap, "peak_max": peak, "oom": kills.get(identifier, 0),
                "memory_share": peak / cap if cap and peak else None}
        line["state"] = ("failing" if line["oom"] else "near" if (line["memory_share"] or 0) >= NEAR
                         else "fits" if peak is not None else "unmeasured")
        rows.append(line)
    return rows


def prometheus_vector(url: str, expression: str) -> dict[str, float]:
    query = urllib.parse.urlencode({"query": expression})
    with urllib.request.urlopen(f"{url.rstrip('/')}/api/v1/query?{query}", timeout=60) as response:
        result = json.load(response)["data"]["result"]
    return {item["metric"]["id"].removeprefix("/system.slice/docker-").removesuffix(".scope"): float(item["value"][1])
            for item in result}


def containers(url: str, since: str) -> list[dict]:
    """The host's running containers measured over `since` (`-14d`): read-only Docker, journal and Prometheus."""
    days = since.lstrip("-")
    listed = subprocess.run(["docker", "ps", "--no-trunc", "--format", "{{.ID}} {{.Names}}"],
                            check=True, capture_output=True, text=True).stdout
    running = dict(line.split(maxsplit=1) for line in listed.splitlines() if line.strip())
    scope = '{id=~"/system.slice/docker-.+"}'
    peaks = prometheus_vector(url, f"max_over_time(container_memory_working_set_bytes{scope}[{days}])")
    caps = prometheus_vector(url, f"container_spec_memory_limit_bytes{scope}")
    kernel = subprocess.run(["journalctl", "-k", f"--since={since}", "-o", "cat", "--grep=oom-kill:"],
                            capture_output=True, text=True).stdout.splitlines()
    return container_rows(running, peaks, caps, container_kills(kernel))


def container_markdown(rows: list[dict]) -> str:
    head = "| container | cap | working-set peak | % | oom kills | state |\n|" + "---|" * 6 + "\n"
    return head + "".join(f"| {r['container']} | {mib(r['memory_max'])} | {mib(r['peak_max'])} | {share(r['memory_share'])} "
                          f"| {r['oom']} | {r['state']} |\n" for r in rows)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--journal", help="saved journal JSON lines to read instead of running journalctl ('-': stdin)")
    parser.add_argument("--declared", help="saved `systemctl show` output to read instead of asking systemctl")
    parser.add_argument("--prometheus", help="the host's Prometheus, to add the Docker containers")
    parser.add_argument("--since", default="-14d", help="the window (journalctl --since; also the Prometheus range)")
    form = parser.add_mutually_exclusive_group()
    form.add_argument("--table", action="store_true", help="Markdown table (default)")
    form.add_argument("--json", action="store_true", help="one JSON object per unit or container")
    args = parser.parse_args(argv)
    if args.journal == "-":
        lines = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="replace").readlines()
    elif args.journal:
        with open(args.journal, encoding="utf-8", errors="replace") as handle:
            lines = handle.readlines()
    else:
        lines = journal(args.since)
    by_unit = attribute_scopes(runs(lines))
    if args.declared:
        with open(args.declared, encoding="utf-8") as handle:
            shown = declared(handle.read())
    else:
        shown = show(sorted(listed_units() | set(by_unit)))
    names = sorted(set(by_unit) | {name for name in shown if name.endswith(".service")})
    rows = [row(name, by_unit.get(name, []), shown.get(name, {})) for name in names]
    boxes = containers(args.prometheus, args.since) if args.prometheus else []
    if args.json:
        for r in rows + boxes:
            print(json.dumps(r))
    else:
        print(markdown(rows), end="")
        if boxes:
            print("\n" + container_markdown(boxes), end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
