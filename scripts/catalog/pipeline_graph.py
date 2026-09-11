"""Pipeline graph contract and reconciliation against its three independent owners."""

from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
FOUNDATION = Path("platforms/foundation-platform")
GRAPH = FOUNDATION / "docs/catalog/pipeline-graph.v1.json"
EXAMPLE = GRAPH.with_name("pipeline-graph.v1.example.json")
ENDPOINTS = FOUNDATION / "docs/catalog/public-source-endpoint-catalog.v1.json"
CONTRACTS = FOUNDATION / "infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json"
SCHEMA_VERSION = 2
DATA_RELATION = "feeds"
NODE_TYPES = {"source_group", "silver_table", "gold_table", "serving_group", "serving_surface"}


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def load(root: Path, path: Path) -> dict:
    return load_json(root / path)


def migration_tables(root: Path) -> set[str]:
    """Read all migrations; refuse lifecycle changes this bounded scanner cannot reconcile."""
    files = sorted((root / FOUNDATION / "migrations").glob("*.sql"))
    if not files:
        raise ValueError("no migrations found")
    tables = set()
    # Ignore comments, strings and function bodies: dynamic SQL is not a table declaration.
    ignored = re.compile(r"--[^\n]*|/\*.*?\*/|'(?:''|[^'])*'|\$(\w*)\$.*?\$\1\$", re.S)
    ident = r'"?([a-z_][a-z_0-9]*)"?'
    qualified = rf'"?(catalog|serving_postgis)"?\s*\.\s*{ident}'
    create = re.compile(rf"\bCREATE\s+(?:UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?{qualified}", re.I)
    rename = re.compile(rf"\bALTER\s+TABLE\s+{qualified}\s+RENAME\s+TO\s+{ident}\s*;", re.I)
    lifecycle = re.compile(rf"\b(?:DROP\s+(?:TABLE|SCHEMA)\s+(?:IF\s+EXISTS\s+)?\"?(?:catalog|serving_postgis)\b|ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?{qualified}[^;]*\b(?:RENAME\s+TO|SET\s+SCHEMA)\b)", re.I)
    for path in files:
        sql = ignored.sub(" ", path.read_text(encoding="utf-8-sig"))
        if lifecycle.search(rename.sub(" ", sql)):
            raise ValueError(f"unsupported serving table lifecycle in {path.name}; update reconciliation")
        # Replay declarations in order: a preserved table can be renamed and its old name reused.
        events = sorted([*create.finditer(sql), *rename.finditer(sql)], key=lambda event: event.start())
        for event in events:
            schema, table, *target = (part.lower() for part in event.groups())
            name = f"{schema}.{table}"
            if target:
                renamed = f"{schema}.{target[0]}"
                if name not in tables or renamed in tables:
                    raise ValueError(f"invalid serving table rename in {path.name}: {name} -> {renamed}")
                tables.remove(name)
                tables.add(renamed)
            else:
                tables.add(name)
    if not tables:
        raise ValueError("no catalog/serving_postgis CREATE TABLE declarations found")
    return tables


def reconcile(root: Path, graph: dict | None = None) -> dict[str, int]:
    graph = load_json(root / GRAPH) if graph is None else graph
    endpoint_doc = load_json(root / ENDPOINTS)
    endpoints = endpoint_doc["endpoints"]
    contracts = load_json(root / CONTRACTS)["contracts"]
    expected_groups = {e["group"] for e in endpoints}
    expected_contracts = {c["table_name"] for c in contracts.values()}
    expected_serving = migration_tables(root)
    problems = []

    def compare(label: str, actual: list[str], expected: set[str]) -> None:
        counts = Counter(actual)
        duplicates = sorted(k for k, count in counts.items() if count > 1)
        missing, extra = sorted(expected - counts.keys()), sorted(counts.keys() - expected)
        if duplicates or missing or extra:
            problems.append(f"{label}: missing={missing}, extra={extra}, duplicates={duplicates}")

    if graph.get("schema_version") != SCHEMA_VERSION:
        problems.append(f"schema_version must be {SCHEMA_VERSION}")
    nodes, edges = graph["nodes"], graph["edges"]
    compare("node IDs", [n["id"] for n in nodes], {n["id"] for n in nodes})
    compare("edge IDs", [e["id"] for e in edges], {e["id"] for e in edges})
    compare("source groups", [n.get("endpoint_catalog_group", "") for n in nodes if n["type"] == "source_group"], expected_groups)
    compare("lakehouse tables", [n.get("table_name", "") for n in nodes if n["type"] in {"silver_table", "gold_table"}], expected_contracts)
    compare("serving tables", [t for n in nodes if n["type"] == "serving_group" for t in n["tables"]], expected_serving)
    by_id = {n["id"]: n for n in nodes}
    if any("silver" in e for e in endpoints):
        problems.append("endpoint silver duplicates the graph edges")
    main = (root / FOUNDATION / "services/foundation-outbox-publisher/src/main.rs").read_text(encoding="utf-8")
    commands = set(re.findall(r'Some\("([^"\n]+)"\)', main))
    bindings = []
    for node in nodes:
        kind = node["type"]
        if kind not in NODE_TYPES:
            problems.append(f"unknown node type: {node['id']}")
        if not all(node.get(key) for key in ("title", "description", "status", "owner")):
            problems.append(f"missing node metadata: {node['id']}")
        if kind in {"silver_table", "gold_table"} and not node.get("table_name", "").startswith(kind.removesuffix("_table") + "."):
            problems.append(f"wrong table type: {node['id']}")
        if kind in {"silver_table", "gold_table"} and node.get("runtime_bindings") != [{"kind": "lakehouse_contract", "value": node.get("table_name")}]:
            problems.append(f"lakehouse runtime binding disagrees with table: {node['id']}")
        if kind == "source_group":
            if node.get("unmapped_status") != "collected_only":
                problems.append(f"source group needs collected_only fallback: {node['id']}")
            if any(key in node for key in ("endpoints", "dataset_slugs", "source_slugs")):
                problems.append(f"source group copies endpoint inventory: {node['id']}")
        if kind == "serving_group" and not node.get("tables"):
            problems.append(f"empty serving tables: {node['id']}")
        bindings.extend((b["kind"], b["value"]) for b in node.get("runtime_bindings", []))
    if len(bindings) != len(set(bindings)):
        problems.append("duplicate runtime binding")
    for edge in edges:
        if edge["from"] not in by_id or edge["to"] not in by_id:
            problems.append(f"edge endpoints: {edge['id']}")
            continue
        via = edge.get("via")
        if not isinstance(via, list) or not via or any(not isinstance(step, str) or not step.strip() for step in via):
            problems.append(f"edge via: {edge['id']}")
        else:
            for step in via:
                if step not in commands and not (root / step).is_file():
                    problems.append(f"unknown via {step!r}: {edge['id']}")
        source = by_id[edge["from"]]
        selected = edge.get("source_slugs")
        if source["type"] == "source_group" and edge.get("relation") == DATA_RELATION:
            allowed = {e["bronze"]["source_slug"] for e in endpoints if e["group"] == source["endpoint_catalog_group"]}
            if not isinstance(selected, list) or not selected or not set(selected) <= allowed:
                problems.append(f"edge source selector: {edge['id']}")
        elif selected is not None:
            problems.append(f"endpoint selector outside source data edge: {edge['id']}")
        if edge.get("status") == "implemented" and any(by_id[edge[key]]["status"] in {"contract_only", "collected_only"} for key in ("from", "to")):
            problems.append(f"invented lane for unconnected dataset: {edge['id']}")
    if problems:
        raise ValueError("\n".join(problems))
    return {"source_groups": len(expected_groups), "lakehouse_tables": len(expected_contracts),
            "serving_tables": len(expected_serving), "nodes": len(nodes), "edges": len(edges)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path, nargs="?", default=ROOT)
    args = parser.parse_args()
    try:
        sizes = reconcile(args.root)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"FAIL pipeline-graph-covers-every-dataset: {error}", file=sys.stderr)
        return 1
    print("OK pipeline-graph-covers-every-dataset " + " ".join(f"{key}={value}" for key, value in sizes.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
