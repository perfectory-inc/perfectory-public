#!/usr/bin/env python3
"""What a release changes in the stores the jobs write, and which jobs write it (root ADR-0270 §3, deploy D3).

    release_changes.py <running release> <new release> [--hold-file <path>]

Both are a Foundation Platform tree (the running one is /opt/foundation-platform/current). Prints:

- the lakehouse tables whose contract entry differs (added, removed or changed): the deploy's
  `migrate` step brings those tables to the new contract;
- the Postgres migrations the new release has and the running one does not;
- the jobs that write those tables, by each job's `pipeline_graph_edges` and the graph's
  `table_name` of where an edge goes (orchestration/jobs.v1.json, docs/catalog/pipeline-graph.v1.json
  of the new release);
- the jobs the graph names no table for, which a caller must count as writers of every table.

A Postgres migration is not mapped to tables yet, so it makes every job a writer.

With --hold-file, the jobs the deploy holds until the new release is live are written there, one id
per line: the writers and the jobs the graph cannot place when anything changes, nothing when
nothing does, and `*` (every job) for a Postgres migration (deploy D5). The deploy gives every other
job its schedule back once the release is built.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

CONTRACTS = "infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json"
MIGRATIONS = "migrations"
JOBS = "orchestration/jobs.v1.json"
GRAPH = "docs/catalog/pipeline-graph.v1.json"


def read(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def changed_tables(running: Path, new: Path) -> list[str]:
    before = read(running / CONTRACTS)["contracts"]
    after = read(new / CONTRACTS)["contracts"]
    return sorted(name for name in before.keys() | after.keys() if before.get(name) != after.get(name))


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
    hold_file = None
    if len(argv) == 4 and argv[2] == "--hold-file":
        hold_file, argv = Path(argv[3]), argv[:2]
    if len(argv) != 2:
        print("usage: release_changes.py <running release> <new release> [--hold-file <path>]", file=sys.stderr)
        return 64
    running, new = Path(argv[0]), Path(argv[1])
    tables = changed_tables(running, new)
    migrations = added_migrations(running, new)
    writing, unknown = writers(new, tables, every_table=bool(migrations))
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
