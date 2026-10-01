#!/usr/bin/env bash
# Every compose service states a memory cap, and the caps of what runs on the shared host fit in it.
#
# What real incident does failing this prevent? On 2026-10-01 Trino had no container cap and its
# image sized the heap from the whole host: it held 27.7GB of ai-server's 62GB while idle, on the
# machine that also runs the Spark loads and the data catalog. Sixteen more containers had no cap,
# so one leak in any of them could take the database and every timer down with it (root ADR-0118
# §6).
#
# The caps live in the compose files and nowhere else. tools/host-memory-budget.contract.json says
# which compose projects run on the host, with which profiles, and what the caps do not cover;
# this guard reads both and adds them up. A compose file the contract does not place is a failure,
# so a new stack cannot reach the host without a budget decision.
#
# Budget arithmetic: services that stay up count at their cap; one-shot jobs (restart "no", or a
# dependency another service waits on to complete) run one at a time during a deploy or a timer,
# so only the largest counts.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
name="every-container-has-a-memory-cap"
command -v python3 >/dev/null 2>&1 || {
  echo "FAIL ${name}: python3 is required" >&2
  exit 1
}

python3 - "$root" "$name" <<'PY'
from __future__ import annotations

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
UNIT = {"k": 1 << 10, "m": 1 << 20, "g": 1 << 30}
SKIP_DIRS = {".git", "node_modules", "target", ".next", ".venv", "dist"}

errors: list[str] = []


def size_of(raw: str) -> int | None:
    match = SIZE.fullmatch(raw.strip().strip("\"'").lower())
    return int(match.group(1)) * UNIT[match.group(2)] if match else None


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
            if key in ("mem_limit", "restart", "image"):
                entry[key] = value.strip("\"'")
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
            for field in ("mem_limit", "restart", "image"):
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

groups = [(project, True) for project in contract["projects"]]
groups += [({"name": "not on " + host["name"], "files": entry["files"], "profiles": None}, False)
           for entry in contract.get("not_on_host", [])]

for project, on_host in groups:
    services = project_services(project["files"], placed)
    jobs = {dependency for service in services.values() for dependency in service["waits_on_completion_of"]}
    for service_name, service in sorted(services.items()):
        cap = size_of(service.get("mem_limit", ""))
        if cap is None:
            where = service.get("file", project["files"][0])
            errors.append(
                f"{where}: service {service_name} states no mem_limit like 512m or 4g"
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
        standing_total += cap
        report.append(f"  {gib(cap):>8}  {project['name']}/{service_name}")

for relative in compose_files():
    if relative in placed or any(relative.startswith(prefix) for prefix in outside):
        continue
    errors.append(
        f"{relative}: a compose file {CONTRACT} does not place; add it to a host project, "
        "to not_on_host, or to outside_scope with the reason"
    )

total = standing_total + largest_job[0] + reserved
summary = [
    f"  {gib(standing_total):>8}  services that stay up",
    f"  {gib(largest_job[0]):>8}  largest one-shot job ({largest_job[1] or 'none'})",
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
