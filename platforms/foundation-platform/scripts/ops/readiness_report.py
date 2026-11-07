#!/usr/bin/env python3
"""The whole system's production readiness, once a day, from the definitions (root ADR-0442).

Every row comes from a definition the repository already keeps, never from a list written here:

- every enabled job of orchestration/jobs.v1.json: its unit's (and its check unit's) last success and last failure from
  systemd's own journal (the events scripts/ops/job-run-times.py reads), the last error line of its
  last failed run, and whether the last success is inside the budget its schedule declares (one
  schedule period plus its time limit); the operations map's problem for it (failed, or blocked by a
  failed input: foundation-api's foundation_scheduled_job_problem, root ADR-0195);
- every by-PNU lane of config/by-pnu-lane-index.v1.json: the served manifest's age against its
  freshness budget, how long it has been behind its inputs (asset-reconcile.prom, root ADR-0444: a
  lane serving every change there is is not stale however old its manifest) and the hourly serving
  monitor's verdict (the monitor reads the lane's sample PNUs from the live hostname, root ADR-0306),
  the asset reconciler's lag of its packs behind their Gold (asset-reconcile.prom, root ADR-0333), and
  its bake and Gold rebuild jobs' rows;
- every Bronze source foundation-api reports (foundation_platform_ingestion_run_last_finished_at_seconds);
- the host: memory and swap, the data disks, failed units, and timers that never fired.

Read-only: it asks systemd, the journal, the metrics textfiles and foundation-api's /metrics, and
writes readiness.json and readiness.md under its state directory (FOUNDATION_READINESS_REPORT_DIR)
and readiness-report.prom for node-exporter. `--print` writes nothing and prints the table.
Rows are sorted by what a user sees first: a served lane past its budget, then what feeds a lane,
then Silver and collection, then operations, then the host.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

sys.dont_write_bytecode = True
PLATFORM = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "scripts" / "deploy"))

import job_units  # noqa: E402
JOBS = PLATFORM / "orchestration" / "jobs.v1.json"
LANES = PLATFORM / "config" / "by-pnu-lane-index.v1.json"
METRICS_DIR = Path(os.environ.get("FOUNDATION_READINESS_METRICS_DIR", "/var/lib/foundation-jobs/metrics"))
OUT_DIR = Path(os.environ.get("FOUNDATION_READINESS_REPORT_DIR", "/var/lib/foundation-platform/readiness"))
API_METRICS = os.environ.get("FOUNDATION_READINESS_API_METRICS_URL", "http://127.0.0.1:18080/metrics")
DISKS = ("/", "/data")
JOURNAL_DAYS = 14
# Impact order: what users see first.
IMPACT = {"serving": 1, "feeds-serving": 2, "silver": 3, "collection": 4, "operations": 5, "host": 6}
SAMPLE = re.compile(r'^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})? (?P<value>\S+)$')
LABEL = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')
ERROR_LINE = re.compile(r"error|refus|denied|timed out|timeout|failed|panic|Traceback|exceeded", re.IGNORECASE)


def samples(text: str) -> list[tuple[str, dict, float]]:
    """Prometheus text exposition lines as (name, labels, value)."""
    out = []
    for line in text.splitlines():
        found = SAMPLE.match(line.strip())
        if found and not line.startswith("#"):
            try:
                out.append((found["name"], dict(LABEL.findall(found["labels"] or "")), float(found["value"])))
            except ValueError:
                continue
    return out


def cron_period_seconds(schedule: str) -> int | None:
    """The longest gap between fires of a `<minutes> <hour|*> * * *` schedule, or None for any other form."""
    fields = schedule.split()
    if len(fields) != 5 or fields[2:] != ["*", "*", "*"]:
        return None
    try:
        minutes = [int(m) for m in fields[0].split(",")]
        hours = list(range(24)) if fields[1] == "*" else [int(h) for h in fields[1].split(",")]
    except ValueError:
        return None
    fires = sorted(h * 60 + m for h in hours for m in minutes)
    gaps = [b - a for a, b in zip(fires, fires[1:])] + [fires[0] + 1440 - fires[-1]]
    return max(gaps) * 60


def unit_runs(events: list[dict]) -> dict:
    """The last success and last failure, as (UTC seconds, systemd's words), from the unit's PID-1 journal events."""
    last = {"success": None, "failure": None, "start": None}
    for event in events:
        at = int(event["__REALTIME_TIMESTAMP"]) / 1e6
        message = event.get("MESSAGE") or ""
        if message.startswith("Starting ") or message.startswith("Started "):
            last["start"] = (at, message)
        elif "Deactivated successfully" in message or message.startswith("Finished "):
            last["success"] = (at, message)
        elif "Failed with result" in message or message.startswith("Failed to start"):
            last["failure"] = (at, message)
    return last


def job_row(job: dict, runs: dict, reason: str | None, problems: list[dict], now: float) -> dict:
    period = cron_period_seconds(job["schedule"])
    budget = None if period is None else period + 60 * int(job.get("time_limit_minutes") or 0)
    success = runs["success"][0] if runs["success"] else None
    failure = runs["failure"][0] if runs["failure"] else None
    failing = failure is not None and (success is None or failure > success)
    stale = budget is not None and (success is None or now - success > budget)
    state = "ok"
    if failing:
        state = "failing"
    elif stale:
        state = "stale"
    if any(p["kind"] == "blocked" for p in problems) and state == "ok":
        state = "blocked"
    return {
        "kind": "job", "id": job["id"], "unit": job["systemd_service"], "impact": impact_of(job["id"]),
        "state": state, "last_success_utc": iso(success), "last_failure_utc": iso(failure),
        "budget_hours": None if budget is None else round(budget / 3600, 1),
        "success_age_hours": None if success is None else round((now - success) / 3600, 1),
        "result": result_of(runs["failure"][1]) if failing else None,
        "reason": reason if failing else None,
        "problems": sorted({f'{p["kind"]}: {p["summary"]}' for p in problems}),
    }


def result_of(message: str) -> str | None:
    """systemd's word for how a failed run ended: exit-code, timeout, signal, oom-kill."""
    found = re.search(r"Failed with result '([\w-]+)'", message)
    return found[1] if found else None


def impact_of(job_id: str) -> str:
    if job_id.startswith("by_pnu_") or "tile" in job_id or job_id.endswith("_boundary_release"):
        return "serving"
    if job_id.startswith("gold_"):
        return "feeds-serving"
    if job_id.startswith("silver_") or job_id.endswith("_silver") or job_id in ("building_register_floor",):
        return "silver"
    if "collect" in job_id or job_id in ("source_sweep", "registered_factory", "legal_dong_code_changes",
                                          "vworld_parcel_edition", "parcel_number_change_history", "data_go_kr_file_data"):
        return "collection"
    return "operations"


def lane_row(lane: dict, metrics: dict, monitor_result: str, jobs_by_id: dict) -> dict:
    name = lane["lane"]
    age = metrics.get(("foundation_by_pnu_served_manifest_age_seconds", name))
    budget = metrics.get(("foundation_by_pnu_serving_freshness_budget_seconds", name))
    lags = [metrics.get(("foundation_asset_lag_seconds", f"perfectory://{node}")) for node in lane["serving_nodes"]]
    lag = max((value for value in lags if value is not None), default=None)
    behind = metrics.get(("foundation_by_pnu_lane_behind_seconds", name))
    feeding = [jobs_by_id[j["id"]] for j in lane["bake_jobs"] + lane.get("rebuild_jobs", []) if j["id"] in jobs_by_id]
    live = all(job.get("enabled") for job in feeding) if feeding else False
    if age is None:
        state = "not-served" if not live else "unmeasured"
    elif budget is not None and (behind > budget if behind is not None else age > budget):
        state = "stale"
    elif monitor_result not in ("success", ""):
        state = "monitor-failing"
    else:
        state = "ok"
    return {
        "kind": "lane", "id": name, "unit": f"foundation-by-pnu-serving-monitor@{name}.service", "impact": "serving",
        "state": state, "served_age_hours": None if age is None else round(age / 3600, 1),
        "budget_hours": None if budget is None else round(budget / 3600, 1),
        "behind_hours": None if behind is None else round(behind / 3600, 1),
        "monitor": monitor_result or None, "asset_lag_hours": None if lag is None else round(lag / 3600, 1),
        "gold_tables": lane["gold_tables"], "feeding_jobs": [j["id"] for j in feeding],
        "feeding_enabled": live,
    }


def source_rows(api: list[tuple[str, dict, float]], now: float) -> list[dict]:
    rows = []
    for name, labels, value in api:
        if name == "foundation_platform_ingestion_run_last_finished_at_seconds":
            rows.append({"kind": "source", "id": labels.get("source") or labels.get("source_slug", "?"),
                         "impact": "collection", "state": "failing" if labels.get("status") == "failed" else "ok",
                         "last_finished_utc": iso(value), "age_hours": round((now - value) / 3600, 1)})
    return rows


def host_rows(meminfo: str, disks: dict, failed_units: list[str], never_fired: list[str], memory: list[dict]) -> list[dict]:
    info = {k: int(v.split()[0]) * 1024 for k, v in (line.split(":", 1) for line in meminfo.splitlines() if ":" in line)}
    rows = [{"kind": "host", "id": "memory", "impact": "host",
             "state": "low" if info.get("MemAvailable", 0) < 0.1 * info.get("MemTotal", 1) else "ok",
             "available_gib": round(info.get("MemAvailable", 0) / 2**30, 1), "total_gib": round(info.get("MemTotal", 0) / 2**30, 1),
             "swap_used_gib": round((info.get("SwapTotal", 0) - info.get("SwapFree", 0)) / 2**30, 1)}]
    for path, (total, used) in disks.items():
        rows.append({"kind": "host", "id": f"disk {path}", "impact": "host", "state": "low" if used / total > 0.85 else "ok",
                     "used_ratio": round(used / total, 2), "free_gib": round((total - used) / 2**30)})
    rows.append({"kind": "host", "id": "failed units", "impact": "host", "state": "failing" if failed_units else "ok",
                 "count": len(failed_units), "units": failed_units})
    rows.append({"kind": "host", "id": "timers never fired", "impact": "host", "state": "stale" if never_fired else "ok",
                 "count": len(never_fired), "units": never_fired})
    near = [m for m in memory if m["max"] and m["peak"] / m["max"] > 0.9]
    rows.append({"kind": "host", "id": "units near their MemoryMax", "impact": "host", "state": "low" if near else "ok",
                 "count": len(near), "units": [f'{m["unit"]} {m["peak"] / 2**20:.0f}/{m["max"] / 2**20:.0f} MiB' for m in near]})
    return rows


def blocking(row: dict) -> bool:
    return row["state"] not in ("ok", "disabled")


def sort_key(row: dict) -> tuple:
    return (not blocking(row), IMPACT[row["impact"]], row["kind"], row["id"])


def iso(seconds: float | None) -> str | None:
    return None if seconds is None else time.strftime("%Y-%m-%dT%H:%MZ", time.gmtime(seconds))


def markdown(rows: list[dict], checked_at: float) -> str:
    lines = [f"readiness {iso(checked_at)} — blockers {sum(blocking(r) for r in rows)} of {len(rows)} rows",
             "", "| 영향 | 종류 | 대상 | 상태 | 근거 |", "| --- | --- | --- | --- | --- |"]
    for row in sorted(rows, key=sort_key):
        if not blocking(row):
            continue
        evidence = {k: v for k, v in row.items() if k not in ("kind", "id", "impact", "state") and v not in (None, [], "")}
        lines.append(f'| {row["impact"]} | {row["kind"]} | `{row["id"]}` | {row["state"]} | '
                     f'{json.dumps(evidence, ensure_ascii=False)[:400].replace("|", "/")} |')
    return "\n".join(lines) + "\n"


def run(command: list[str]) -> str:
    return subprocess.run(command, capture_output=True, text=True, timeout=120).stdout


def journal_events(unit: str) -> list[dict]:
    text = run(["journalctl", "_PID=1", f"UNIT={unit}", "--since", f"-{JOURNAL_DAYS}d", "-o", "json",
                "--output-fields=MESSAGE,__REALTIME_TIMESTAMP", "--no-pager"])
    return [json.loads(line) for line in text.splitlines() if line.startswith("{")]


def failure_reason(unit: str, before: float) -> str | None:
    text = run(["journalctl", "-u", unit, "--until", f"@{int(before) + 1}", "-n", "300", "-o", "cat", "--no-pager"])
    lines = [line.strip() for line in text.splitlines() if ERROR_LINE.search(line) and "Failed with result" not in line
             and "Main process exited" not in line and "Failed to start" not in line
             # A timed-out run's cleanup races Docker's own --rm; the run's reason is the line before.
             and "removal of container" not in line]
    return lines[-1][:240] if lines else None


def main(argv: list[str]) -> int:
    now = time.time()
    jobs = json.loads(JOBS.read_text(encoding="utf-8"))["jobs"]
    by_id = {job["id"]: job for job in jobs}
    try:
        api = samples(urllib.request.urlopen(API_METRICS, timeout=30).read().decode("utf-8"))
    except OSError as error:
        print(f"readiness_report: foundation-api metrics unread ({error})", file=sys.stderr)
        api = []
    problems: dict[str, list[dict]] = {}
    for name, labels, value in api:
        if name == "foundation_scheduled_job_problem" and value == 1:
            problems.setdefault(labels.get("job_id", ""), []).append(labels)
    rows = []
    for job in jobs:
        if not job.get("enabled"):
            continue
        # A job with a check service is fresh when its check said there is nothing to do (root ADR-0277).
        units = [job["systemd_service"]] + ([job["check_service"]] if job.get("check_service") else [])
        events = {unit: journal_events(unit) for unit in units}
        runs = unit_runs(sorted((e for unit in units for e in events[unit]), key=lambda e: int(e["__REALTIME_TIMESTAMP"])))
        failed_unit = next((unit for unit in units if unit_runs(events[unit])["failure"] == runs["failure"]), units[0])
        reason = failure_reason(failed_unit, runs["failure"][0]) if runs["failure"] else None
        rows.append(job_row(job, runs, reason, problems.get(job["id"], []), now))
    textfiles = []
    for path in sorted(METRICS_DIR.glob("*.prom")):
        try:
            textfiles += samples(path.read_text(encoding="utf-8"))
        except OSError:
            continue
    metrics = {(name, labels.get("lane") or labels.get("asset")): value for name, labels, value in textfiles}
    for lane in json.loads(LANES.read_text(encoding="utf-8"))["lanes"]:
        result = run(["systemctl", "show", "-p", "Result", "--value", f'foundation-by-pnu-serving-monitor@{lane["lane"]}.service']).strip()
        rows.append(lane_row(lane, metrics, result, by_id))
    rows += source_rows(api, now)
    failed = run(["systemctl", "--failed", "--plain", "--no-legend", "--no-pager"]).split()
    failed_units = sorted({word for word in failed if word.endswith((".service", ".timer", ".path"))})
    never_fired = [timer for timer in run(["systemctl", "list-timers", "--all", "--plain", "--no-legend", "--no-pager",
                                           "foundation-*"]).split() if timer.endswith(".timer")
                   and run(["systemctl", "show", "-p", "LastTriggerUSec", "--value", timer]).strip() in ("", "n/a", "0")]
    memory = []
    for unit in job_units.running(["foundation-*"]):
        if unit.endswith(".service"):
            shown = dict(line.split("=", 1) for line in run(["systemctl", "show", "-p", "MemoryPeak", "-p", "MemoryMax", unit]).splitlines() if "=" in line)
            peak, cap = shown.get("MemoryPeak", ""), shown.get("MemoryMax", "")
            if peak.isdigit() and cap.isdigit():
                memory.append({"unit": unit, "peak": int(peak), "max": int(cap)})
    disks = {path: (usage.total, usage.used) for path in DISKS if Path(path).exists() for usage in [shutil.disk_usage(path)]}
    rows += host_rows(Path("/proc/meminfo").read_text(encoding="utf-8"), disks, failed_units, never_fired, memory)
    rows.sort(key=sort_key)
    table = markdown(rows, now)
    if "--print" in argv:
        print(table, end="")
        print(json.dumps(rows, ensure_ascii=False), file=sys.stderr)
        return 0
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, text in (("readiness.json", json.dumps({"checked_at": iso(now), "rows": rows}, ensure_ascii=False, indent=1) + "\n"),
                       ("readiness.md", table)):
        temporary = OUT_DIR / f".{name}.tmp"
        temporary.write_text(text, encoding="utf-8")
        temporary.replace(OUT_DIR / name)
    counts: dict[str, int] = {}
    for row in rows:
        if blocking(row):
            counts[row["impact"]] = counts.get(row["impact"], 0) + 1
    prom = ["# HELP foundation_readiness_blockers Rows of the daily readiness report that are not ok, by impact (root ADR-0442).",
            "# TYPE foundation_readiness_blockers gauge"]
    prom += [f'foundation_readiness_blockers{{impact="{impact}"}} {counts.get(impact, 0)}' for impact in IMPACT]
    prom += ["# HELP foundation_readiness_report_timestamp_seconds When the readiness report last ran.",
             "# TYPE foundation_readiness_report_timestamp_seconds gauge", f"foundation_readiness_report_timestamp_seconds {now:.0f}"]
    temporary = METRICS_DIR / ".readiness-report.prom.tmp"
    temporary.write_text("\n".join(prom) + "\n", encoding="utf-8")
    temporary.replace(METRICS_DIR / "readiness-report.prom")
    print(table, end="")
    print("foundation-job-outcome unchanged")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
