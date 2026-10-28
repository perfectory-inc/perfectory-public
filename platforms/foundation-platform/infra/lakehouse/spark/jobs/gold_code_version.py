"""The code version a Gold commit was built by (root ADR-0321, the ADR-0191 §5 `build-source-sha256`).

A Gold producer's rows are a function of its Silver pins and its code. The pins are recorded on every
commit (gold_rebuild.SOURCE_SNAPSHOTS_PROPERTY); this records the code beside them, so the planner
rebuilds when either changes, as a Bazel action key covers both its inputs and its command.

The code is the producer script and every module of this directory it imports, transitively (any
`import x` / `from x import y` naming a `x.py` here, at any depth, so a deferred import counts),
plus every JSON contract those modules name as a string constant and that exists under
`infra/lakehouse/contracts/` or `config/`. Python is hashed as its syntax tree without docstrings,
so a comment, docstring or formatting change is not a code change; JSON as its canonical form.

The rebuild contract's `code_version` (contracts/gold-panel-rebuild.contract.json) narrows it:
- `plumbing_modules`: modules that cannot change a row (the planner, the Spark session settings),
  with the reason; they and what only they import are left out.
- `scoped_contracts`: a registry many tables share (every table's columns, every by-PNU lane) counts
  only at the entries this table reads: the parts it `keep`s, and under each `entries` path the
  entries its selector picks: `tables` (the Gold table and its inputs) or `lanes` (the by-PNU lanes
  whose Gold or host panel is the table). An edit to another table's entry is not this table's code.

The value is JSON `{"sha256": <of the whole>, "files": {<path>: <sha256 prefix>}}`: the planner
names the files that changed.
"""
from __future__ import annotations

import ast
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Iterable

PROPERTY = "foundation.build-source-sha256"
JOBS = Path(__file__).resolve().parent
LAKEHOUSE = JOBS.parents[1]
PLATFORM = JOBS.parents[3]
CONTRACT_DIRS = (LAKEHOUSE / "contracts", PLATFORM / "config")
FILE_DIGITS = 16
SELECTORS = ("tables", "lanes")


def _without_docstrings(tree: ast.AST) -> ast.AST:
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
                    and isinstance(body[0].value.value, str):
                node.body = body[1:] or [ast.Pass()]
    return tree


def _imports(tree: ast.AST) -> set[str]:
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names.add(node.module.split(".")[0])
    return names


def _json_names(tree: ast.AST) -> set[str]:
    return {node.value for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value.endswith(".json")
            and "/" not in node.value and "{" not in node.value}


def settings(contract: dict[str, Any], jobs: Path = JOBS) -> dict[str, Any]:
    """The rebuild contract's `code_version`, checked: {"plumbing": {...}, "scoped": {...}}."""
    declared = contract.get("code_version") or {}
    plumbing = declared.get("plumbing_modules", {})
    if not (isinstance(plumbing, dict) and all(isinstance(w, str) and w.strip() for w in plumbing.values())):
        raise ValueError("code_version.plumbing_modules must map each module to why it cannot change a row")
    missing = sorted(name for name in plumbing if not (jobs / f"{name}.py").is_file())
    if missing:
        raise ValueError(f"code_version.plumbing_modules names modules that do not exist: {missing}")
    scoped = declared.get("scoped_contracts", {})
    for name, rule in scoped.items():
        entries = rule.get("entries", {}) if isinstance(rule, dict) else None
        if not (isinstance(entries, dict) and set(entries.values()) <= set(SELECTORS)
                and isinstance(rule.get("keep", []), list) and str(rule.get("reason", "")).strip()):
            raise ValueError(f"code_version.scoped_contracts.{name} needs `entries` (path -> one of {SELECTORS}), "
                             "an optional `keep` list and its reason")
    return {"plumbing": plumbing, "scoped": scoped}


def _at(document: Any, path: str) -> Any:
    for part in path.split("."):
        document = document.get(part) if isinstance(document, dict) else None
    return document


def _picks(selector: str, key: str, entry: Any, table: str, tables: set[str]) -> bool:
    if selector == "tables":
        return key in tables
    from by_pnu_kinds import HOST_PANELS
    entry = entry if isinstance(entry, dict) else {}
    return table in {entry.get("gold_table"), "gold." + key.replace("-", "_"), HOST_PANELS.get(entry.get("host"))}


def _scoped(document: Any, rule: dict[str, Any], table: str, tables: set[str]) -> dict[str, Any]:
    kept = {path: _at(document, path) for path in rule.get("keep", [])}
    for path, selector in rule["entries"].items():
        found = _at(document, path) or {}
        kept[path] = {k: v for k, v in sorted(found.items()) if _picks(selector, k, v, table, tables)}
    return kept


def files(producer: str, config: dict[str, Any], table: str, inputs: Iterable[str], jobs: Path = JOBS,
          contract_dirs: tuple[Path, ...] = CONTRACT_DIRS, root: Path = PLATFORM) -> dict[str, str]:
    """{path relative to the platform: sha256} of the code `producer` builds `table` from `inputs` with."""
    plumbing, scoped = config["plumbing"], config["scoped"]
    tables = {table, *inputs}
    found: dict[str, str] = {}
    queue, seen, contracts = [producer], set(), set()
    while queue:
        name = queue.pop()
        if name in seen or (name in plumbing and name != producer):
            continue
        seen.add(name)
        path = jobs / f"{name}.py"
        if not path.is_file():
            continue
        tree = _without_docstrings(ast.parse(path.read_text(encoding="utf-8")))
        found[path.relative_to(root).as_posix()] = hashlib.sha256(ast.dump(tree).encode()).hexdigest()
        queue.extend(_imports(tree))
        contracts |= _json_names(tree)
    for name in sorted(contracts):
        for directory in contract_dirs:
            path = directory / name
            if path.is_file():
                document = json.loads(path.read_text(encoding="utf-8"))
                if name in scoped:
                    document = _scoped(document, scoped[name], table, tables)
                canonical = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
                found[path.relative_to(root).as_posix()] = hashlib.sha256(canonical.encode()).hexdigest()
    return found


def value(producer: str, config: dict[str, Any], table: str, inputs: Iterable[str], **paths: Any) -> str:
    hashed = files(producer, config, table, inputs, **paths)
    whole = hashlib.sha256(json.dumps(hashed, sort_keys=True).encode()).hexdigest()
    return json.dumps({"sha256": whole, "files": {k: v[:FILE_DIGITS] for k, v in sorted(hashed.items())}},
                      sort_keys=True, separators=(",", ":"))


def changed_files(recorded: str, current: str) -> list[str]:
    """The files whose hash differs (or that one side lacks) between two recorded values."""
    old, new = json.loads(recorded)["files"], json.loads(current)["files"]
    return sorted(name for name in set(old) | set(new) if old.get(name) != new.get(name))


def same(recorded: str, current: str) -> bool:
    return json.loads(recorded)["sha256"] == json.loads(current)["sha256"]


def table_name(table: str) -> str:
    """`gold.t` of a table name as a producer writes it (`` `r2`.`gold`.`t` ``, `r2.gold.t`, `gold.t`)."""
    return ".".join(table.replace("`", "").split(".")[-2:])


def of_running_producer(table: str, inputs: Iterable[str]) -> str | None:
    """The code version of the producer this process runs (`spark <producer>.py`) building `table`
    from `inputs`, or None when the entry point is not a producer of this directory (a test runner)."""
    main = getattr(sys.modules.get("__main__"), "__file__", None)
    if main is None or Path(main).resolve().parent != JOBS:
        return None
    from gold_rebuild import CONTRACT_PATH
    config = settings(json.loads(CONTRACT_PATH.read_text(encoding="utf-8")))
    return value(Path(main).stem, config, table_name(table), inputs)
