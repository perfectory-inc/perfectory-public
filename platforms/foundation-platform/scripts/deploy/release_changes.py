#!/usr/bin/env python3
"""What a release changes in the stores the jobs write, and which jobs write it (root ADR-0270 §3, deploy D3).

    release_changes.py <running release> <new release> [--hold-file <path>] [--host]

Both are a Foundation Platform tree (the running one is /opt/foundation-platform/current). Prints:

- the lakehouse tables the deploy's lakehouse migration may change, decided from that step's own
  inputs in config/deploy-steps.contract.json (root ADR-0308, one list for both): where only an
  input it reads table by table (`per_table`) changed, the tables whose entry differs (added,
  removed or changed); where any other input changed (its program, a backfill, compose, the
  engine pin), every lakehouse table;
- the Postgres migrations the new release has and the running one does not;
- the jobs that write those tables, by each job's `pipeline_graph_edges` and the graph's
  `table_name` of where an edge goes (orchestration/jobs.v1.json, docs/catalog/pipeline-graph.v1.json
  of the new release);
- the jobs the graph names no table for, which a caller must count as writers of every table.

With --host, the running release is the baseline only when this host's record says its migration
ran against it (deploy_steps.py should-run answers skip for it): a migration that failed, a catalog
restore, a record removed by hand or production drift found by shape_ledger_check make every
lakehouse table changed, since the migration will run over all of them.

A Postgres migration is not mapped to tables yet, so it makes every job a writer.

With --hold-file, the jobs the deploy holds until the new release is live are written there, one id
per line: the writers and the jobs the graph cannot place when anything changes, nothing when
nothing does, and `*` (every job) for a Postgres migration (deploy D5).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import deploy_steps  # noqa: E402
from worker_autodeploy import input_digests  # noqa: E402

CONTRACTS = "infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json"
MIGRATIONS = "migrations"
JOBS = "orchestration/jobs.v1.json"
GRAPH = "docs/catalog/pipeline-graph.v1.json"
STEP = "lakehouse-migrate"


def read(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def input_files(platform: Path, inputs: list[str]) -> dict[str, bytes]:
    """Every file of the inputs by path, with its digest: the files the step's fingerprint reads."""
    return dict(input_digests(platform, inputs, missing_ok=True))


def keyed(path: Path, key: str) -> dict:
    try:
        return read(path)[key]
    except FileNotFoundError:
        return {}


def changed_tables(running: Path, new: Path, host: Path | None) -> tuple[list[str], str]:
    """(the tables the migration may change, why every table when it is every table)."""
    entry = deploy_steps.step_of(new, STEP)
    every = sorted(read(new / CONTRACTS)["contracts"].keys() | keyed(running / CONTRACTS, "contracts").keys())
    if host is not None:
        try:
            run, reason = deploy_steps.should_run(running, STEP, host)
        except Exception as error:  # noqa: BLE001 -- no answer is no baseline
            run, reason = True, f"{type(error).__name__}: {error}"
        if run:
            return every, f"the running release is not what this host last migrated ({reason})"
    before, after = input_files(running, entry["inputs"]), input_files(new, entry["inputs"])
    differ = sorted(path for path in before.keys() | after.keys() if before.get(path) != after.get(path))
    per_table = entry.get("per_table", {})
    other = [path for path in differ if path not in per_table]
    if other:
        return every, f"{', '.join(other[:3])}{', ...' if len(other) > 3 else ''} changed, which the migration runs"
    tables: set[str] = set()
    for path in differ:
        old, now = keyed(running / path, per_table[path]), keyed(new / path, per_table[path])
        tables |= {name for name in old.keys() | now.keys() if old.get(name) != now.get(name)}
    return sorted(tables), ""


def added_migrations(running: Path, new: Path) -> list[str]:
    def names(root: Path) -> set[str]:
        folder = root / MIGRATIONS
        return {path.name for path in folder.glob("*.sql")} if folder.is_dir() else set()

    return sorted(names(new) - names(running))


def writers(new: Path, tables: list[str], every_table: bool) -> tuple[list[str], list[str]]:
    """(the jobs that write one of `tables`, the jobs the graph names no table for)."""
    graph = read(new / GRAPH)
    nodes = {node["id"]: node for node in graph["nodes"]}
    edges = {edge["id"]: edge for edge in graph["edges"]}
    writing, unknown = [], []
    for job in read(new / JOBS)["jobs"]:
        written = {nodes[edges[edge]["to"]].get("table_name") for edge in job.get("pipeline_graph_edges", [])
                   if edge in edges and edges[edge]["to"] in nodes} - {None}
        if not written:
            unknown.append(job["id"])
        elif every_table or written & set(tables):
            writing.append(job["id"])
    return sorted(writing), sorted(unknown)


def main(argv: list[str]) -> int:
    hold_file, host = None, None
    if "--host" in argv:
        argv = [arg for arg in argv if arg != "--host"]
        host = Path(os.environ.get("FOUNDATION_DEPLOY_STEPS_HOST_ROOT") or "/")
    if len(argv) == 4 and argv[2] == "--hold-file":
        hold_file, argv = Path(argv[3]), argv[:2]
    if len(argv) != 2:
        print("usage: release_changes.py <running release> <new release> [--hold-file <path>] [--host]", file=sys.stderr)
        return 64
    running, new = Path(argv[0]), Path(argv[1])
    tables, every = changed_tables(running, new, host)
    migrations = added_migrations(running, new)
    writing, unknown = writers(new, tables, every_table=bool(migrations))
    if every:
        print(f"lakehouse tables this release changes: every table ({len(tables)}): {every}")
    else:
        print(f"lakehouse tables this release changes: {', '.join(tables) or 'none'}")
    print(f"postgres migrations this release adds: {', '.join(migrations) or 'none'}")
    if tables or migrations:
        why = " (a postgres migration: every job)" if migrations else ""
        print(f"jobs that write them{why}: {', '.join(writing) or 'none'}")
        print(f"jobs whose writes the pipeline graph does not name: {', '.join(unknown) or 'none'}")
    held = ["*"] if migrations else sorted(set(writing) | set(unknown)) if tables else []
    print(f"jobs the deploy holds until the release is live: {'every job' if held == ['*'] else ', '.join(held) or 'none'}")
    if hold_file is not None:
        hold_file.write_text("".join(f"{job}\n" for job in held), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
