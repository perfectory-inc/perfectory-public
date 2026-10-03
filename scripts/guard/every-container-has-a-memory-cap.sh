#!/usr/bin/env bash
# Every compose service states a memory cap, and the caps of what runs on the shared host fit in it.
#
# What real incident does failing this prevent? On 2026-10-01 Trino had no container cap and its
# image sized the heap from the whole host: it held 27.7GB of ai-server's 62GB while idle, on the
# machine that also runs the Spark loads and the data catalog. Sixteen more containers had no cap,
# so one leak in any of them could take the database and every timer down with it (root ADR-0118
# §6).
#
# The caps live in compose, with required parameters bound to their numeric source contracts.
# tools/host-memory-budget.contract.json says
# which compose projects run on the host, with which profiles, and what the caps do not cover;
# this guard reads both and adds them up. A compose file the contract does not place is a failure,
# so a new stack cannot reach the host without a budget decision.
#
# Budget arithmetic: services that stay up count at their cap; one-shot jobs (restart "no", or a
# dependency another service waits on to complete) run one at a time during a deploy or a timer,
# so only the largest counts.
#
# Some one-shot containers are started by code itself, not by compose: the lakehouse
# tile bake's GDAL and tippecanoe (root ADR-0133 §3). Their caps live in the contract the code
# reads; `one_shot_contracts` names that contract and where its containers are, and this guard
# counts each as a one-shot job. A container there without a `memory_limit` is a failure.
#
# Scheduled jobs that share an Airflow pool can run together as far as its slots allow (root
# ADR-0138). `scheduled_jobs` names the job list, the systemd units and, for every job in a declared
# pool, what it runs: compose services, contract containers, or its unit's MemoryMax (a unit that
# should have one and does not is a failure). A job holds the largest of its sources at a time; a
# pool holds the sum over every set of its jobs whose slots fit; the pools together are the
# scheduled load, which counts instead of the largest one-shot job when it is larger. The release
# build refuses to start while a registered job runs, and manual loads run with the DAGs paused,
# so neither ever adds to it.
#
# Two more limits share this inventory. Every service names the file's log cap (`logging: *log-cap`,
# anchored once per file): on 2026-10-02 one uncapped json-file log reached 32GB and filled the
# root disk under the production database twice. And every service that stays up on the host
# restarts: the data catalog's Kafka broker stopped on that full disk, stayed stopped, and the
# catalog server logged its reconnect attempts at 5.7GB an hour until the disk filled again.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
name="every-container-has-a-memory-cap"
command -v python3 >/dev/null 2>&1 || {
  echo "FAIL ${name}: python3 is required" >&2
  exit 1
}

python3 - "$root" "$name" <<'PY'
from __future__ import annotations

import itertools
import json
import os
import pathlib
import re
import sys

ROOT = pathlib.Path(sys.argv[1]).resolve()
NAME = sys.argv[2]
CONTRACT = "tools/host-memory-budget.contract.json"
COMPOSE_NAME = re.compile(r"(?:docker-)?compose(?:[.-][a-z0-9_.-]+)?\.ya?ml")
SIZE = re.compile(r"([0-9]+)([kmg])")
PARAMETER_NAME = re.compile(r"[A-Z_][A-Z0-9_]*")
REQUIRED_MEMORY = re.compile(r"\$\{([A-Z_][A-Z0-9_]*):\?[^${}\r\n]+\}([kmg])")
UNIT = {"k": 1 << 10, "m": 1 << 20, "g": 1 << 30}
LOG_CAP_REFERENCE = "*log-cap"
LOG_CAP_ANCHOR = re.compile(
    r'^x-log-cap: &log-cap\n  driver: json-file\n  options:\n    max-size: "[0-9]+m"\n    max-file: "[0-9]+"$',
    re.M,
)
RESTARTS = {"unless-stopped", "always", "on-failure"}
SKIP_DIRS = {".git", "node_modules", "target", ".next", ".venv", "dist"}

errors: list[str] = []


def size_of(raw: str) -> int | None:
    match = SIZE.fullmatch(raw.strip().strip("\"'").lower())
    return int(match.group(1)) * UNIT[match.group(2)] if match else None


def memory_parameters(contract: dict) -> dict[str, int]:
    """Resolve declared JSON object members, without evaluating environment or templates."""
    bindings = contract.get("memory_parameters", {})
    if not isinstance(bindings, dict):
        errors.append(f"{CONTRACT}: memory_parameters must be an object")
        return {}
    values: dict[str, int] = {}
    for name, binding in bindings.items():
        try:
            if not PARAMETER_NAME.fullmatch(name):
                raise ValueError("invalid parameter name")
            if not isinstance(binding, dict) or set(binding) != {"contract", "json_path"}:
                raise ValueError("binding must contain only contract and json_path")
            relative = binding["contract"]
            if not isinstance(relative, str) or not relative or "\\" in relative or ":" in relative:
                raise ValueError("contract must be a repository-relative POSIX path")
            path = pathlib.PurePosixPath(relative)
            if path.is_absolute() or ".." in path.parts:
                raise ValueError("contract path must not escape the repository")
            source = (ROOT / path).resolve(strict=True)
            if not source.is_relative_to(ROOT) or not source.is_file():
                raise ValueError("contract must resolve to a file inside the repository")
            keys = binding["json_path"]
            if not isinstance(keys, list) or not keys or any(not isinstance(key, str) or not key for key in keys):
                raise ValueError("json_path must be a nonempty array of object member names")
            value = json.loads(source.read_text(encoding="utf-8"))
            for key in keys:
                if not isinstance(value, dict) or key not in value:
                    raise ValueError(f"json_path member {key!r} is missing")
                value = value[key]
            if type(value) is not int or value <= 0:
                raise ValueError("memory value must be a positive integer, not a boolean")
            values[name] = value
        except (OSError, ValueError, RuntimeError) as error:
            errors.append(f"{CONTRACT}: memory_parameters[{name!r}]: {error}")
    return values


def memory_cap(raw: str, parameters: dict[str, int]) -> int | None:
    literal = size_of(raw)
    if literal is not None:
        return literal
    match = REQUIRED_MEMORY.fullmatch(raw.strip().strip("\"'"))
    if match is None or match.group(1) not in parameters:
        return None
    return parameters[match.group(1)] * UNIT[match.group(2)]


def gib(value: int) -> str:
    return f"{value / (1 << 30):.2f}g"


def compose_files() -> list[str]:
    found = []
    for directory, subdirs, files in os.walk(ROOT):
        subdirs[:] = [d for d in subdirs if d not in SKIP_DIRS and not d.startswith(".")]
        for file in files:
            if COMPOSE_NAME.fullmatch(file):
                found.append(pathlib.Path(directory, file).relative_to(ROOT).as_posix())
    return sorted(found)


def parse(relative: str) -> tuple[list[str], dict[str, dict]]:
    """Reads the narrow compose shape this repository writes: two-space indentation."""
    text = (ROOT / relative).read_text(encoding="utf-8")
    includes: list[str] = []
    services: dict[str, dict] = {}
    section = None
    service = None
    key = None
    dependency = None
    for line in text.split("\n"):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        top = re.match(r"^([A-Za-z0-9_.-]+):", line)
        if top:
            section, service, key = top.group(1), None, None
            continue
        if section == "include":
            match = re.match(r"^  - path:\s*(\S+)", line)
            if match:
                included = (pathlib.PurePosixPath(relative).parent / match.group(1)).as_posix()
                includes.append(os.path.normpath(included).replace(os.sep, "/"))
            continue
        if section != "services":
            continue
        match = re.match(r"^  ([A-Za-z0-9_.-]+):\s*$", line)
        if match:
            service = match.group(1)
            services[service] = {"profiles": [], "waits_on_completion_of": [], "file": relative}
            key = None
            continue
        if service is None:
            continue
        entry = services[service]
        match = re.match(r"^    ([a-z_]+):\s*(.*)$", line)
        if match:
            key, value = match.group(1), match.group(2).strip()
            if key in ("mem_limit", "restart", "image", "logging"):
                entry[key] = value.strip("\"'")
                if key == "logging":
                    entry["logging_file"] = relative
            continue
        if key == "profiles":
            match = re.match(r"^      - \s*(\S+)", line)
            if match:
                entry["profiles"].append(match.group(1).strip("\"'"))
        elif key == "depends_on":
            match = re.match(r"^      ([A-Za-z0-9_.-]+):\s*$", line)
            if match:
                dependency = match.group(1)
            match = re.match(r"^        condition:\s*(\S+)", line)
            if match and match.group(1) == "service_completed_successfully" and dependency:
                entry["waits_on_completion_of"].append(dependency)
    return includes, services


def project_services(files: list[str], seen: set[str]) -> dict[str, dict]:
    """Merges a project's files the way compose does: includes first, later files override."""
    merged: dict[str, dict] = {}
    for relative in files:
        if not (ROOT / relative).is_file():
            errors.append(f"{CONTRACT}: names {relative}, which does not exist")
            continue
        seen.add(relative)
        includes, services = parse(relative)
        for included, service in project_services(includes, seen).items():
            merged.setdefault(included, service)
        for service_name, service in services.items():
            current = merged.setdefault(service_name, {"profiles": [], "waits_on_completion_of": []})
            for field in ("mem_limit", "restart", "image", "logging", "logging_file"):
                if field in service:
                    current[field] = service[field]
            # Errors point at the definition that names the image, where the cap belongs.
            if "image" in service or "file" not in current:
                current["file"] = service["file"]
            if service["profiles"]:
                current["profiles"] = service["profiles"]
            current["waits_on_completion_of"] += service["waits_on_completion_of"]
    return merged


try:
    contract = json.loads((ROOT / CONTRACT).read_text(encoding="utf-8"))
except (OSError, ValueError) as error:
    print(f"FAIL {NAME}: cannot read {CONTRACT}: {error}", file=sys.stderr)
    sys.exit(1)
if contract.get("schema_version") != 1:
    print(f"FAIL {NAME}: {CONTRACT} schema_version is not the 1 this guard reads", file=sys.stderr)
    sys.exit(1)

parameters = memory_parameters(contract)
host = contract["host"]
physical = size_of(host["physical_memory"])
reserved = size_of(host["host_reserved"])
if physical is None or reserved is None:
    print(f"FAIL {NAME}: {CONTRACT} host sizes must look like 62g or 512m", file=sys.stderr)
    sys.exit(1)

outside = [entry["path_prefix"] for entry in contract.get("outside_scope", [])]
placed: set[str] = set()
report: list[str] = []
standing_total = 0
largest_job = (0, "")

def contract_one_shots(contract: dict) -> list[tuple[int, str]]:
    """The caps of containers code starts itself, read from the contract that code reads."""
    entries = contract.get("one_shot_contracts", [])
    if not isinstance(entries, list):
        errors.append(f"{CONTRACT}: one_shot_contracts must be an array")
        return []
    jobs: list[tuple[int, str]] = []
    for entry in entries:
        try:
            if not isinstance(entry, dict) or set(entry) != {"contract", "containers", "why"}:
                raise ValueError("an entry names only contract, containers and why")
            relative = entry["contract"]
            if not isinstance(relative, str) or not relative or "\\" in relative:
                raise ValueError("contract must be a repository-relative POSIX path")
            path = pathlib.PurePosixPath(relative)
            if path.is_absolute() or ".." in path.parts:
                raise ValueError("contract path must not escape the repository")
            source = (ROOT / path).resolve(strict=True)
            if not source.is_relative_to(ROOT) or not source.is_file():
                raise ValueError("contract must resolve to a file inside the repository")
            containers = json.loads(source.read_text(encoding="utf-8"))
            keys = entry["containers"]
            if not isinstance(keys, list) or not keys or any(not isinstance(key, str) or not key for key in keys):
                raise ValueError("containers must be a nonempty array of object member names")
            for key in keys:
                if not isinstance(containers, dict) or key not in containers:
                    raise ValueError(f"containers member {key!r} is missing")
                containers = containers[key]
            if not isinstance(containers, dict) or not containers:
                raise ValueError("containers must name a nonempty object of containers")
            for name, container in sorted(containers.items()):
                raw = container.get("memory_limit") if isinstance(container, dict) else None
                cap = size_of(raw) if isinstance(raw, str) else None
                if cap is None:
                    errors.append(f"{relative}: container {name} states no memory_limit like 512m or 8g")
                    continue
                jobs.append((cap, f"{relative}#{name}"))
        except (OSError, ValueError, RuntimeError) as error:
            errors.append(f"{CONTRACT}: one_shot_contracts: {error}")
    return jobs


for cap, label in contract_one_shots(contract):
    if cap > largest_job[0]:
        largest_job = (cap, label)

project_caps: dict[str, dict[str, int]] = {}

groups = [(project, True) for project in contract["projects"]]
groups += [({"name": "not on " + host["name"], "files": entry["files"], "profiles": None}, False)
           for entry in contract.get("not_on_host", [])]

for project, on_host in groups:
    services = project_services(project["files"], placed)
    if on_host:
        project_caps[project["name"]] = {
            service_name: cap
            for service_name, service in services.items()
            if (cap := memory_cap(service.get("mem_limit", ""), parameters)) is not None
        }
    jobs = {dependency for service in services.values() for dependency in service["waits_on_completion_of"]}
    for service_name, service in sorted(services.items()):
        where = service.get("file", project["files"][0])
        if service.get("logging") != LOG_CAP_REFERENCE:
            errors.append(f"{where}: service {service_name} does not name the file's log cap (logging: {LOG_CAP_REFERENCE})")
        elif not LOG_CAP_ANCHOR.search((ROOT / service["logging_file"]).read_text(encoding="utf-8")):
            errors.append(f"{service['logging_file']}: names {LOG_CAP_REFERENCE} but defines no x-log-cap anchor with max-size and max-file")
        cap = memory_cap(service.get("mem_limit", ""), parameters)
        if cap is None:
            where = service.get("file", project["files"][0])
            errors.append(
                f"{where}: service {service_name} states no valid literal or contract-bound required mem_limit"
                f" (project {project['name']})"
            )
            continue
        if not on_host:
            continue
        active = not service["profiles"] or bool(set(service["profiles"]) & set(project["profiles"]))
        if not active:
            continue
        if service.get("restart") == "no" or service_name in jobs:
            if cap > largest_job[0]:
                largest_job = (cap, f"{project['name']}/{service_name}")
            continue
        if service.get("restart") not in RESTARTS:
            errors.append(f"{where}: service {service_name} stays up on {host['name']} but does not restart (restart: unless-stopped)")
        standing_total += cap
        report.append(f"  {gib(cap):>8}  {project['name']}/{service_name}")

for relative in compose_files():
    if relative in placed or any(relative.startswith(prefix) for prefix in outside):
        continue
    errors.append(
        f"{relative}: a compose file {CONTRACT} does not place; add it to a host project, "
        "to not_on_host, or to outside_scope with the reason"
    )

UNIT_MEMORY = re.compile(r"^MemoryMax=([0-9]+)([KMG])$", re.M)


def repository_file(relative: str, what: str) -> pathlib.Path:
    if not isinstance(relative, str) or not relative or "\\" in relative:
        raise ValueError(f"{what} must be a repository-relative POSIX path")
    path = pathlib.PurePosixPath(relative)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{what} must not escape the repository")
    return ROOT / path


def unit_memory_max(units: pathlib.Path, service: str) -> int:
    """MemoryMax of the unit file a job's service is installed from (a template for name@x)."""
    match = re.fullmatch(r"([a-z0-9-]+)(@[a-z0-9-]+)?\.service", service)
    if not match:
        raise ValueError(f"{service!r} is not a service name")
    unit = units / (match.group(1) + ("@" if match.group(2) else "") + ".service")
    found = UNIT_MEMORY.findall(unit.read_text(encoding="utf-8")) if unit.is_file() else None
    if not found:
        raise ValueError(f"{unit.relative_to(ROOT).as_posix()} states no MemoryMax=<n>K|M|G")
    if len(found) > 1:
        raise ValueError(f"{unit.relative_to(ROOT).as_posix()} states MemoryMax more than once")
    number, suffix = found[0]
    return int(number) * UNIT[suffix.lower()]


def contract_containers_cap(entry: dict) -> int:
    containers = json.loads(repository_file(entry["contract"], "contract").read_text(encoding="utf-8"))
    for key in entry["containers"]:
        containers = containers[key]
    caps = [size_of(container.get("memory_limit", "")) for container in containers.values()]
    if not caps or None in caps:
        raise ValueError(f"{entry['contract']}: every container needs a memory_limit")
    return max(caps)


def scheduled_load(section: dict) -> tuple[int, str]:
    """The most memory the pooled scheduled jobs can hold at once, and which jobs that is."""
    jobs_list = json.loads(repository_file(section["jobs"], "jobs").read_text(encoding="utf-8"))
    units = repository_file(section["units"], "units")
    memory = section["memory"]
    if not isinstance(memory, dict):
        raise ValueError("memory must map job ids to their memory sources")
    pools = jobs_list.get("pools", {})
    jobs = {job["id"]: job for job in jobs_list["jobs"]}
    for job_id in memory:
        if job_id not in jobs or jobs[job_id]["pool"] not in pools:
            errors.append(f"{CONTRACT}: scheduled_jobs.memory names {job_id!r}, which is not a job in a declared pool")
    peaks: dict[str, int] = {}
    for job_id, job in jobs.items():
        if job["pool"] not in pools:
            continue  # default_pool jobs run the publisher natively: host_reserved covers them
        sources = memory.get(job_id)
        if not isinstance(sources, list) or not sources:
            errors.append(f"{CONTRACT}: scheduled job {job_id!r} in pool {job['pool']!r} names no memory sources")
            continue
        caps = []
        for source in sources:
            try:
                if source == {"unit": "MemoryMax"}:
                    caps.append(unit_memory_max(units, job["systemd_service"]))
                elif isinstance(source, dict) and set(source) == {"project", "service"}:
                    cap = project_caps.get(source["project"], {}).get(source["service"])
                    if cap is None:
                        raise ValueError(f"no capped service {source['service']!r} in host project {source['project']!r}")
                    caps.append(cap)
                elif isinstance(source, dict) and set(source) == {"contract", "containers"}:
                    caps.append(contract_containers_cap(source))
                else:
                    raise ValueError(f"unknown memory source {source!r}")
            except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
                errors.append(f"{CONTRACT}: scheduled job {job_id!r}: {error}")
        if caps:
            peaks[job_id] = max(caps)
    worst_total, worst_label = 0, []
    for pool, spec in pools.items():
        members = [job for job in jobs.values() if job["pool"] == pool and job["id"] in peaks]
        worst, worst_set = 0, ()
        for size in range(1, len(members) + 1):
            for group in itertools.combinations(members, size):
                if sum(job.get("pool_slots", 1) for job in group) > spec["slots"]:
                    continue
                held = sum(peaks[job["id"]] for job in group)
                if held > worst:
                    worst, worst_set = held, group
        worst_total += worst
        if worst_set:
            worst_label.append(f"{pool}: " + " + ".join(f"{job['id']} {gib(peaks[job['id']])}" for job in worst_set))
    return worst_total, "; ".join(worst_label)


scheduled = (0, "")
if "scheduled_jobs" in contract:
    try:
        scheduled = scheduled_load(contract["scheduled_jobs"])
    except (OSError, ValueError, KeyError, TypeError) as error:
        errors.append(f"{CONTRACT}: scheduled_jobs: {error}")

heaviest = max(largest_job[0], scheduled[0])
total = standing_total + heaviest + reserved
summary = [
    f"  {gib(standing_total):>8}  services that stay up",
    f"  {gib(largest_job[0]):>8}  largest one-shot job ({largest_job[1] or 'none'})",
    f"  {gib(scheduled[0]):>8}  scheduled jobs at once ({scheduled[1] or 'none'}); the larger of these two counts",
    f"  {gib(reserved):>8}  host_reserved",
    f"  {gib(total):>8}  of {gib(physical)} on {host['name']}",
]
if total > physical:
    errors.append(
        f"{host['name']}: caps add up to {gib(total)}, over the {gib(physical)} the host has"
    )

if errors:
    for error in dict.fromkeys(errors):
        print(f"FAIL {NAME}: {error}", file=sys.stderr)
    print("\n".join(report + summary), file=sys.stderr)
    sys.exit(1)
if os.environ.get("PERFECTORY_MEMORY_BUDGET_VERBOSE"):
    print("\n".join(report + summary))
print(f"OK {NAME}: {gib(total)} of {gib(physical)} on {host['name']}")
PY
