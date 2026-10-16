#!/usr/bin/env python3
"""The operations map's two service checks (root ADR-0209), written to one JSON file:

- edge: how Korean visitors reach the live catalog, buildings and tiles hostnames. Every live
  hostname of those three gateways in config/r2-connections.contract.json (the public hostname and
  its aliases; nothing is listed here) is asked for `/cdn-cgi/trace` SAMPLES times, each on a new
  connection as a first visit would be: p50/p95 of the full answer time and the Cloudflare colo it
  entered at. Cloudflare answers that path at the edge, so no Worker runs and nothing is billed; it
  measures the entry path (where a SEA entry costs seconds, 2026-10-12), not the Worker's or R2's
  time, which the by-PNU serving monitor holds. cloudflare.com's own colo is the reference: when
  it says ICN and ours say SEA, the plan's routing is why, not our code.
- ci: the weekly pull request CI wall-time trend of root ADR-0203, computed by that ADR's own
  scripts/ci/ci_time_budget.py from the control checkout the deploy keeps (it is outside the
  platform's release tree). GitHub's runs API is read anonymously, and the host's 60 requests an
  hour are shared with the autodeploy (scripts/deploy/foundation-autodeploy.sh), so each UTC day's
  runs are fetched once, after the day is over, while the hour's budget stays above the
  autodeploy's share, and kept beside the file; a week is reported once all its days are kept.
  The first fill of CI_WEEKS weeks takes some hours; after that it is a few requests a day.

Each part records its own error. Exits 1 only when no hostname could be measured: the unit's
OnFailure reports that to Slack. foundation-api serves the file at /data-catalog/v1/service-checks.
"""

from __future__ import annotations

import importlib.util
import json
import math
import os
import pathlib
import sys
import time
import urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
CONTRACT = PLATFORM / "config" / "r2-connections.contract.json"
GATEWAYS = ("parcel_by_pnu_gateway", "building_by_pnu_gateway", "vector_tile_gateway")
REFERENCE_HOST = "cloudflare.com"
SAMPLES = 10
TIMEOUT_SECONDS = 15
CI_WEEKS = 8
# A day's pull request runs are fetched once that day is this far past: CI finishes well within it.
RUN_SETTLE = timedelta(hours=3)
# GitHub's anonymous budget is 60 requests an hour per host, and the autodeploy asks up to 24 of them
# (two every five minutes while a commit waits). A busy day is about 1,600 runs (2026-10-12), 100 to a request, split
# in halves past the API's 1,000-result cap.
API_RESERVE = 30
REQUESTS_PER_DAY = 20
OUT = os.environ.get("FOUNDATION_SERVICE_CHECKS_PATH", "/var/lib/foundation-platform/service-checks/service-checks.json")
CONTROL_ROOT = pathlib.Path(os.environ.get("FOUNDATION_SERVICE_CHECKS_CONTROL_ROOT", "/opt/perfectory-control/current"))


def hostnames(contract: pathlib.Path = CONTRACT) -> list[str]:
    data = json.loads(contract.read_text(encoding="utf-8"))
    names = []
    for gateway in GATEWAYS:
        for name in [data[gateway]["public_hostname"], *data[gateway].get("public_hostname_aliases", [])]:
            if name not in names:
                names.append(name)
    return names


def percentile(values: list[float], fraction: float) -> float:
    """Nearest-rank, as ci_time_budget.percentile: every reported value was observed."""
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def colo_of(trace: str) -> str | None:
    for line in trace.splitlines():
        key, _, value = line.partition("=")
        if key == "colo" and value.strip():
            return value.strip()
    return None


def trace(host: str) -> tuple[float, str | None]:
    """One answer of https://<host>/cdn-cgi/trace: (milliseconds, colo)."""
    started = time.perf_counter()
    request = urllib.request.Request(f"https://{host}/cdn-cgi/trace", headers={"User-Agent": "foundation-service-checks"})
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        body = response.read().decode("utf-8", "replace")
    return (time.perf_counter() - started) * 1000, colo_of(body)


def measure_host(host: str, ask=trace, samples: int = SAMPLES) -> dict:
    times, colos, errors = [], Counter(), []
    for _ in range(samples):
        try:
            took, colo = ask(host)
        except Exception as error:  # a refused or timed-out sample is counted, not fatal
            errors.append(f"{type(error).__name__}: {error}"[:200])
            continue
        times.append(took)
        if colo:
            colos[colo] += 1
    return {
        "hostname": host,
        "colo": colos.most_common(1)[0][0] if colos else None,
        "p50_ms": round(percentile(times, 0.5)) if times else None,
        "p95_ms": round(percentile(times, 0.95)) if times else None,
        "samples": len(times),
        "failed_samples": len(errors),
        "error": errors[-1] if not times else None,
    }


def measure_edge(hosts: list[str], ask=trace, now: datetime | None = None) -> dict:
    reference = measure_host(REFERENCE_HOST, ask, samples=1)
    return {
        "checked_at": (now or datetime.now(timezone.utc)).isoformat(),
        "reference_colo": reference["colo"],
        "hosts": [measure_host(host, ask) for host in hosts],
    }


def load_ci_budget_module(root: pathlib.Path = CONTROL_ROOT):
    path = root / "scripts" / "ci" / "ci_time_budget.py"
    spec = importlib.util.spec_from_file_location("ci_time_budget", path)
    if spec is None or spec.loader is None or not path.is_file():
        raise FileNotFoundError(f"{path} is not there")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def remaining_requests() -> int:
    """GitHub's anonymous core budget left this hour; asking it is not counted against it."""
    request = urllib.request.Request("https://api.github.com/rate_limit", headers={"Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        return int(json.load(response)["resources"]["core"]["remaining"])


def ci_days(ci, now: datetime) -> tuple[list[datetime], list[datetime]]:
    """The trend's full weeks and the UTC days whose runs it needs: those weeks and the Monday
    after (an attempt begun late on Sunday finishes then), each only once it is RUN_SETTLE old so
    its runs have completed."""
    current = ci.week_start(now)
    weeks = [current - timedelta(weeks=n) for n in range(CI_WEEKS, 0, -1)]
    days = [weeks[0] + timedelta(days=n) for n in range(CI_WEEKS * 7 + 1)]
    return weeks, [day for day in days if day + timedelta(days=1) + RUN_SETTLE <= now]


def measure_ci(store: pathlib.Path, now: datetime, ci, repository: str, budget: float, remaining=remaining_requests) -> dict:
    """Fetch the days not stored yet, newest first, while the hour's budget stays above
    API_RESERVE (the autodeploy's share), then compute the weeks whose days are all stored."""
    store.mkdir(parents=True, exist_ok=True)
    weeks, days = ci_days(ci, now)
    wanted = {f"{day:%Y-%m-%d}.json" for day in days}
    for old in store.glob("*.json"):
        if old.name not in wanted and old.name < min(wanted, default=""):
            old.unlink()  # a cache of the runs API, recomputable, not a record
    error = None
    for day in sorted(days, reverse=True):
        path = store / f"{day:%Y-%m-%d}.json"
        if path.exists():
            continue
        try:
            if remaining() < API_RESERVE + REQUESTS_PER_DAY:
                break  # the next hour's run carries on
            runs = ci.fetch_runs(repository, day, day + timedelta(days=1), None)
        except Exception as failure:  # the stored days still give a trend; the next run retries
            error = f"{type(failure).__name__}: {failure}"[:300]
            break
        temporary = path.with_name(path.name + ".tmp")
        temporary.write_text(json.dumps(runs), encoding="utf-8")
        os.replace(temporary, path)
    stored = {path.stem for path in store.glob("*.json")}
    runs = [run for path in sorted(store.glob("*.json")) for run in json.loads(path.read_text(encoding="utf-8"))]
    complete = [
        week for week in weeks if all(f"{week + timedelta(days=n):%Y-%m-%d}" in stored for n in range(8))
    ]
    trend = [{k: v for k, v in row.items() if k != "attempts"} for row in ci.weekly_trend(ci.group_attempts(runs), complete)]
    return {
        "attempted_at": now.isoformat(),
        "budget_minutes": budget,
        "weeks": trend,
        "pending_days": len([day for day in days if f"{day:%Y-%m-%d}" not in stored]),
        "error": error,
    }


def measure_ci_from_control(store: pathlib.Path, now: datetime, root: pathlib.Path = CONTROL_ROOT) -> dict:
    try:
        ci = load_ci_budget_module(root)
        repository = json.loads((root / "tools" / "github" / "repository-identity.json").read_text(encoding="utf-8"))["full_name"]
        budget = ci.load_budget(root / "tools" / "ci" / "ci-time-budget.contract.json")
    except Exception as error:
        return {"attempted_at": now.isoformat(), "budget_minutes": None, "weeks": [], "pending_days": None, "error": f"{type(error).__name__}: {error}"[:300]}
    return measure_ci(store, now, ci, repository, budget)


def write_atomically(path: pathlib.Path, document: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.chmod(temporary, 0o644)  # foundation-api reads it from its own container user
    os.replace(temporary, path)


def main() -> int:
    out = pathlib.Path(OUT)
    now = datetime.now(timezone.utc)
    edge = measure_edge(hostnames(), now=now)
    ci = measure_ci_from_control(out.parent / "ci-runs", now)
    write_atomically(out, {"schema_version": 1, "checked_at": now.isoformat(), "edge": edge, "ci": ci})
    for host in edge["hosts"]:
        print(f"{host['hostname']}: colo={host['colo']} p50={host['p50_ms']}ms p95={host['p95_ms']}ms samples={host['samples']}")
    print(f"reference {REFERENCE_HOST}: colo={edge['reference_colo']}; ci error={ci.get('error')}")
    if not any(host["samples"] for host in edge["hosts"]):
        print("service-checks: no hostname answered", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
