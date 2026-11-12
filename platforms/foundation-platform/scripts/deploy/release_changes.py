#!/usr/bin/env python3
"""What a release changes in the stores the jobs write, and which jobs write it (root ADR-0270 §3, deploy D3).

    release_changes.py <running release> <new release> [--hold-file <path>] [--host]

Both are a Foundation Platform tree (the running one is /opt/foundation-platform/current). Prints:

- the lakehouse tables the deploy's lakehouse migration may change, decided from that step's own
  inputs in config/deploy-steps.contract.json (root ADR-0308, one list for both): where only an
  input it reads table by table (`per_table`) changed, the tables whose entry differs (added,
  removed or changed); where a file of a directory its `closure` names changed, what the
  migration's own code reaches decides (root ADR-0437): the program's call graph and the contracts
  it names (gold_code_version.parts, the analysis a Gold commit's code version is) changed, every
  table; a backfill's function changed, its table; neither (another job's script, a source
  contract the migration never reads), nothing; where any other input changed (compose, the
  runner, its unit), every lakehouse table;
- the Postgres migrations the new release has and the running one does not;
- the jobs that write those tables, by each job's `pipeline_graph_edges` and the graph's
  `table_name` of where an edge goes (orchestration/jobs.v1.json, docs/catalog/pipeline-graph.v1.json
  of the new release);
- the jobs the graph names no table for, which a caller must count as writers of every table.
  A job whose every edge lands in a source group (Bronze objects and their ledger) writes no
  lakehouse table and is neither (root ADR-0557).

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

import ast

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


def under(path: str, directories: list[str]) -> bool:
    return any(path == directory or path.startswith(directory + "/") for directory in directories)


def backfills(tree: Path, closure: dict) -> dict[str, tuple[str, str]]:
    """{table: (module, function)} of the program's backfill registry, read from its syntax tree: a
    registry `{(table, column): (module, function)}` (lakehouse_schema_migrate.BACKFILLS)."""
    path = tree / closure["directories"][0] / f"{closure['program']}.py"
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        targets = [node.target] if isinstance(node, ast.AnnAssign) else getattr(node, "targets", [])
        if any(isinstance(t, ast.Name) and t.id == closure["backfills"] for t in targets) and node.value:
            return {table: tuple(function) for (table, _), function in ast.literal_eval(node.value).items()}
    raise ValueError(f"{path} has no {closure['backfills']} registry")


def analyzer(tree: Path, closure: dict):
    """The new release's gold_code_version: one analysis reads both trees, so a change to the analysis
    itself is not mistaken for a change to the code it reads."""
    import importlib.util  # noqa: PLC0415

    path = tree / closure["directories"][0] / "gold_code_version.py"
    spec = importlib.util.spec_from_file_location("release_changes_gold_code_version", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def code_parts(analysis, tree: Path, closure: dict, module: str, start: str | None, skip: set[str]) -> dict[str, str]:
    """What `module` (its script, or its `start` function) reaches in `tree`, less the `skip` files,
    which the hold reads table by table."""
    found = analysis.parts(module, {"plumbing": {}, "plumbing_symbols": {}, "scoped": {}}, "", (),
                           jobs=tree / closure["directories"][0], contract_dirs=(tree / closure["directories"][1],),
                           root=tree, start=start)
    return {part: digest for part, digest in found.items() if part.split("::")[0] not in skip}


def closure_change(running: Path, new: Path, closure: dict, per_table: dict) -> tuple[bool, list[str], set[str]]:
    """(whether the program's code changed, the parts that did, the tables whose backfill changed)."""
    analysis, skip = analyzer(new, closure), set(per_table)
    before = code_parts(analysis, running, closure, closure["program"], None, skip)
    after = code_parts(analysis, new, closure, closure["program"], None, skip)
    moved = sorted(part for part in before.keys() | after.keys() if before.get(part) != after.get(part))
    if moved:
        return True, moved, set()
    old, now = backfills(running, closure), backfills(new, closure)
    tables = {table for table in old.keys() | now.keys()
              if old.get(table) != now.get(table)
              or code_parts(analysis, running, closure, *old[table], skip)
              != code_parts(analysis, new, closure, *now[table], skip)}
    return False, [], tables


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
    closure = entry.get("closure")
    other = [path for path in differ if path not in per_table
             and not (closure and under(path, closure["directories"]))]
    if other:
        return every, f"{', '.join(other[:3])}{', ...' if len(other) > 3 else ''} changed, which the migration runs"
    tables: set[str] = set()
    if closure and any(path not in per_table for path in differ):
        whole, moved, backfilled = closure_change(running, new, closure, per_table)
        if whole:
            return every, f"{', '.join(moved[:3])}{', ...' if len(moved) > 3 else ''} changed, which the migration runs"
        tables |= backfilled
    for path in [path for path in differ if path in per_table]:
        old, now = keyed(running / path, per_table[path]), keyed(new / path, per_table[path])
        tables |= {name for name in old.keys() | now.keys() if old.get(name) != now.get(name)}
    return sorted(tables), ""


def added_migrations(running: Path, new: Path) -> list[str]:
    def names(root: Path) -> set[str]:
        folder = root / MIGRATIONS
        return {path.name for path in folder.glob("*.sql")} if folder.is_dir() else set()

    return sorted(names(new) - names(running))


def writers(new: Path, tables: list[str], every_table: bool) -> tuple[list[str], list[str]]:
    """(the jobs whose asset is made from one of `tables`, the jobs the graph names no table for).

    A job's asset is the table it writes, or, for a job that writes no table (a by-PNU bake, whose
    asset is a lane's packs), the tables its asset is made from: the deploy holds only the assets the
    release changes (root ADR-0333 §5, narrowing root ADR-0308). Before, a bake was a job the graph
    could not place and every table change held it (2026-10-28: the public-auction bake, held paused
    by a release that changed no table it reads, missed its Gold's event 3912). The asset reconciler
    (`reconciles`) reads catalog metadata and writes nothing, so nothing a release changes holds it.
    """
    graph = read(new / GRAPH)
    nodes = {node["id"]: node for node in graph["nodes"]}
    edges = {edge["id"]: edge for edge in graph["edges"]}
    writing, unknown = [], []
    for job in read(new / JOBS)["jobs"]:
        carried = [edges[edge] for edge in job.get("pipeline_graph_edges", []) if edge in edges]
        written = {nodes[edge["to"]].get("table_name") for edge in carried if edge["to"] in nodes} - {None}
        made_from = {nodes[edge["from"]].get("table_name") for edge in carried if edge["from"] in nodes} - {None}
        asset = written or made_from
        if job.get("reconciles") and not carried:
            continue
        # A job whose every edge lands in a source group writes Bronze objects and their ledger, no
        # lakehouse table: a table change does not hold it, a Postgres migration does (root ADR-0557).
        if not every_table and carried and all(nodes.get(edge["to"], {}).get("type") == "source_group" for edge in carried):
            continue
        if not asset:
            unknown.append(job["id"])
        elif every_table or asset & set(tables):
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
