"""The code version a Gold commit was built by (root ADR-0321, the ADR-0191 §5 `build-source-sha256`).

A Gold producer's rows are a function of its Silver pins and its code. The pins are recorded on every
commit (gold_rebuild.SOURCE_SNAPSHOTS_PROPERTY); this records the code beside them, so the planner
rebuilds when either changes, as a Bazel action key covers both its inputs and its command.

The code is the call graph the producer script runs, at the level of top-level symbols (dbt's
`state:modified.body` per model, Bazel's per-action inputs), not whole modules:

- It starts at what `spark <producer>.py` executes: the script's top-level statements (its
  `if __name__ == "__main__":` block calls `main`).
- A symbol is a top-level function, class (with all its methods: a method call through an instance
  is not resolvable statically, so a reached class counts whole) or assignment of a module in this
  directory. From each reached symbol it follows every name it uses that resolves to another
  symbol: of its own module, of a module it imports (`import x` then `x.f`, or `from x import f`),
  at any depth, deferred imports included. A module used as a value (`getattr(x, name)`) counts
  whole. A module's other top-level statements (a loop filling a registry) count whenever any of
  its symbols is reached; another module's `if __name__ == "__main__":` block does not.
- A name bound to a module outside this directory (`F` for `pyspark.sql.functions`) counts as the
  binding itself, so pointing it elsewhere is a change.
- Every JSON contract a reached symbol names as a string constant, under `infra/lakehouse/contracts/`
  or `config/`, counts too.

A symbol is hashed as its syntax tree without docstrings and without function argument and return
annotations, so a comment, docstring, annotation or formatting change is not a code change, and
neither is any change to a function the producer never reaches (another producer's, a CLI helper's);
JSON is hashed as its canonical form.

The rebuild contract's `code_version` (contracts/gold-panel-rebuild.contract.json) narrows it:
- `plumbing_modules`: modules that cannot change a row (the planner, the Spark session settings),
  with the reason; nothing of theirs is followed.
- `scoped_contracts`: a registry many tables share (every table's columns, every by-PNU lane) counts
  only at the entries this table reads: the parts it `keep`s, and under each `entries` path the
  entries its selector picks: `tables` (the Gold table and its inputs) or `lanes` (the by-PNU lanes
  whose Gold or host panel is the table). An edit to another table's entry is not this table's code.

The value is JSON `{"sha256": <of the whole>, "parts": {<path>[::<symbol>]: <sha256 prefix>}}`: the
planner names the symbols and contracts that changed.
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
PART_DIGITS = 16
SELECTORS = ("tables", "lanes")
# The part a module's top-level statements other than its symbols are hashed under.
MODULE_STATEMENTS = "<module>"
WHOLE = "*"


def _normalized(tree: ast.AST) -> ast.AST:
    """`tree` without docstrings and without function argument and return annotations."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
                    and isinstance(body[0].value.value, str):
                node.body = body[1:] or [ast.Pass()]
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            node.returns = None
        if isinstance(node, ast.arg):
            node.annotation = None
    return tree


def _is_main_block(node: ast.stmt) -> bool:
    test = getattr(node, "test", None)
    return (isinstance(node, ast.If) and isinstance(test, ast.Compare) and isinstance(test.left, ast.Name)
            and test.left.id == "__name__")


class Module:
    """One module of the jobs directory: its symbols, import bindings and other statements."""

    def __init__(self, name: str, path: Path) -> None:
        self.name, self.path = name, path
        tree = _normalized(ast.parse(path.read_text(encoding="utf-8")))
        self.symbols: dict[str, list[ast.AST]] = {}
        self.bindings: dict[str, tuple[str, str | None]] = {}  # alias -> (module, attribute or None)
        self.statements: list[ast.stmt] = []
        self.main_block: list[ast.stmt] = []
        for node in tree.body:
            self._bind_imports(node)
            names = self._defines(node)
            if names:
                for name_ in names:
                    self.symbols.setdefault(name_, []).append(node)
            elif _is_main_block(node):
                self.main_block.append(node)
            elif not isinstance(node, (ast.Import, ast.ImportFrom)):
                self.statements.append(node)
        # Imports inside functions (deferred) bind for the whole module: a name resolves to them too.
        for node in ast.walk(tree):
            self._bind_imports(node)

    def _bind_imports(self, node: ast.AST) -> None:
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.asname:
                    self.bindings.setdefault(alias.asname, (alias.name, None))
                else:
                    top = alias.name.split(".")[0]
                    self.bindings.setdefault(top, (top, None))
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            for alias in node.names:
                if alias.name == "*":
                    self.bindings.setdefault(f"*{node.module}", (node.module, WHOLE))
                else:
                    self.bindings.setdefault(alias.asname or alias.name, (node.module, alias.name))

    @staticmethod
    def _defines(node: ast.stmt) -> list[str]:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            return [node.name]
        targets = node.targets if isinstance(node, ast.Assign) else \
            [node.target] if isinstance(node, (ast.AnnAssign, ast.AugAssign)) else []
        names = [n.id for t in targets for n in ast.walk(t) if isinstance(n, ast.Name)]
        # Only a plain binding names a symbol; `D[k] = v` is a statement of the module.
        return names if targets and all(isinstance(t, (ast.Name, ast.Tuple, ast.List)) for t in targets) else []

    def references(self, nodes: list[ast.AST]) -> tuple[set[tuple[str, str]], set[str]]:
        """((module, symbol) the nodes use, JSON names they mention). `symbol` is WHOLE for a module
        used as a value; the module is this one for a name it defines."""
        used: set[tuple[str, str]] = set()
        contracts: set[str] = set()
        module_aliases = {alias for alias, (_, attribute) in self.bindings.items() if attribute is None}

        def visit(node: ast.AST) -> None:
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) \
                    and node.value.id in module_aliases and node.value.id not in self.symbols:
                used.add((self.bindings[node.value.id][0], node.attr))
                return
            if isinstance(node, ast.Name):
                if node.id in self.symbols:
                    used.add((self.name, node.id))
                elif node.id in self.bindings:
                    module, attribute = self.bindings[node.id]
                    used.add((module, attribute or WHOLE))
                    used.add((self.name, f"import:{node.id}"))
            elif isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value.endswith(".json") \
                    and "/" not in node.value and "{" not in node.value:
                contracts.add(node.value)
            for child in ast.iter_child_nodes(node):
                visit(child)

        for node in nodes:
            visit(node)
        for alias, (module, attribute) in self.bindings.items():
            if attribute == WHOLE:
                used.add((module, WHOLE))
        return used, contracts


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


def _digest(nodes: list[ast.AST]) -> str:
    return hashlib.sha256("\n".join(ast.dump(node) for node in nodes).encode()).hexdigest()


def parts(producer: str, config: dict[str, Any], table: str, inputs: Iterable[str], jobs: Path = JOBS,
          contract_dirs: tuple[Path, ...] = CONTRACT_DIRS, root: Path = PLATFORM) -> dict[str, str]:
    """{part: sha256} of the code `producer` builds `table` from `inputs` with: each reached symbol as
    `<path relative to the platform>::<symbol>`, each contract as its path."""
    plumbing, scoped = config["plumbing"], config["scoped"]
    modules: dict[str, Module | None] = {}

    def module(name: str) -> Module | None:
        if name not in modules:
            path = jobs / f"{name}.py"
            reachable = path.is_file() and (name == producer or name not in plumbing)
            modules[name] = Module(name, path) if reachable else None
        return modules[name]

    found: dict[str, str] = {}
    contracts: set[str] = set()
    queue: list[tuple[str, str]] = [(producer, MODULE_STATEMENTS)]
    seen: set[tuple[str, str]] = set()
    while queue:
        name, symbol = queue.pop()
        if (name, symbol) in seen:
            continue
        seen.add((name, symbol))
        source = module(name)
        if source is None:
            continue
        where = source.path.relative_to(root).as_posix()
        if symbol == WHOLE:
            queue.extend((name, s) for s in source.symbols)
            queue.append((name, MODULE_STATEMENTS))
            continue
        if symbol == MODULE_STATEMENTS:
            nodes = source.statements + (source.main_block if name == producer else [])
        elif symbol.startswith("import:"):
            alias = symbol.split(":", 1)[1]
            found[f"{where}::{symbol}"] = hashlib.sha256(repr(source.bindings[alias]).encode()).hexdigest()
            continue
        elif symbol in source.symbols:
            nodes = source.symbols[symbol]
            # Any symbol of a module brings the statements that run when it is imported.
            queue.append((name, MODULE_STATEMENTS))
        elif symbol in source.bindings:
            # A name the module re-exports from its own import.
            module_name, attribute = source.bindings[symbol]
            queue.append((module_name, attribute or WHOLE))
            continue
        else:
            continue
        if nodes:
            found[f"{where}::{symbol}"] = _digest(nodes)
        used, named = source.references(nodes)
        queue.extend(used)
        contracts |= named
    for name in sorted(contracts):
        for directory in contract_dirs:
            path = directory / name
            if path.is_file():
                document = json.loads(path.read_text(encoding="utf-8"))
                if name in scoped:
                    document = _scoped(document, scoped[name], table, {table, *inputs})
                canonical = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
                found[path.relative_to(root).as_posix()] = hashlib.sha256(canonical.encode()).hexdigest()
    return found


def value(producer: str, config: dict[str, Any], table: str, inputs: Iterable[str], **paths: Any) -> str:
    hashed = parts(producer, config, table, inputs, **paths)
    whole = hashlib.sha256(json.dumps(hashed, sort_keys=True).encode()).hexdigest()
    return json.dumps({"sha256": whole, "parts": {k: v[:PART_DIGITS] for k, v in sorted(hashed.items())}},
                      sort_keys=True, separators=(",", ":"))


def changed_parts(recorded: str, current: str) -> list[str]:
    """The symbols and contracts whose hash differs (or that one side lacks) between two values."""
    old, new = json.loads(recorded).get("parts", {}), json.loads(current).get("parts", {})
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
