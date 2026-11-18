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
# ADR-0288 §1: a parcel source (a silver table carrying `pnu`, fed from a source) reaches the current
# pnu only through the one place-identity resolver, never by copying the source code. These are the
# via steps that count as that resolver (address_pnu is 경매's path, converging under ADR-0288 §3).
RESOLVER_VIA = {
    "platforms/foundation-platform/infra/lakehouse/spark/jobs/place_identity_resolver.py",
    "platforms/foundation-platform/infra/lakehouse/spark/jobs/address_pnu.py",
}
# ADR-0483 (ADR-0288 §5): the stable place id is the join key of every parcel source. A PNU-keyed Gold
# that a resolver-placed Silver feeds carries `place_id`, read from the place-id bridge by a feeds edge.
# 2026-11-11 (read-only): no Gold carried one, so a join across a 법정동 code change fell back to the PNU.
PLACE_ID_COLUMN = "place_id"
PLACE_ID_BRIDGE = "gold.place_id_bridge"
# Root ADR-0400: why an endpoint is not collected. A deliberate verdict is a decision not to collect;
# a waiting verdict is a collection blocked on someone outside the pipeline (the owner's application).
COLLECTION_VERDICTS = {
    "api_duplicate_of_bulk": "deliberate",
    "file_covers_the_api": "deliberate",
    "provider_does_not_publish": "deliberate",
    "waiting_on_owner_application": "waiting",
}
# Lanes whose endpoints exist only to be refused: each must name its verdict and decision.
REFUSED_LANES = {"disabled_api_duplicate", "provider_inventory_missing"}
# Lanes the daily source sweep collects whether or not a Silver lane reads them (ADR-0168, ADR-0359).
SWEPT_LANES = {"bulk_file", "provider_dataset_file"}
# The four classes of root ADR-0400, in the order the map shows them.
COLLECTION_CLASSES = ("served", "collected", "not_collected", "deliberate")
# intelligence_table: a table only the intelligence platform writes and foundation only reads (root ADR-0477 §1).
NODE_TYPES = {"source_group", "silver_table", "gold_table", "reference_table", "intelligence_table", "serving_group",
              "serving_surface"}
TABLE_TYPES = {"silver_table", "gold_table", "reference_table", "intelligence_table"}


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def load(root: Path, path: Path) -> dict:
    return load_json(root / path)


def collection_classes(graph: dict, endpoints: list[dict]) -> dict[str, str]:
    """Root ADR-0400: each endpoint's class by its bronze source slug, from the two SSOTs only.

    served: a live source edge selects it and a live data path from there reaches a serving surface.
    collected: a live source edge selects it, or the daily sweep collects its lane, but no surface.
    deliberate: its collection_decision is a deliberate verdict. not_collected: everything else,
    including a waiting verdict and a source edge that is switched off.
    """
    nodes = {n["id"]: n for n in graph["nodes"]}
    live = [e for e in graph["edges"] if e["status"] in {"implemented", "partial"}
            and e["relation"] in {"feeds", "joins", "serves"}]
    after: dict[str, list[str]] = {}
    for edge in live:
        after.setdefault(edge["from"], []).append(edge["to"])

    def reaches_surface(start: str) -> bool:
        seen, pending = set(), [start]
        while pending:
            current = pending.pop()
            if current in seen:
                continue
            seen.add(current)
            if nodes[current]["type"] == "serving_surface":
                return True
            pending.extend(after.get(current, []))
        return False

    classes = {}
    for endpoint in endpoints:
        slug = endpoint["bronze"]["source_slug"]
        decision = endpoint.get("collection_decision")
        if decision and COLLECTION_VERDICTS.get(decision.get("verdict")) == "deliberate":
            classes[slug] = "deliberate"
            continue
        starts = [e["to"] for e in live if slug in (e.get("source_slugs") or [])
                  and nodes[e["from"]]["type"] == "source_group"]
        if any(reaches_surface(start) for start in starts):
            classes[slug] = "served"
        elif decision:
            classes[slug] = "not_collected"
        elif starts or endpoint["source_acquisition_lane"] in SWEPT_LANES:
            classes[slug] = "collected"
        else:
            classes[slug] = "not_collected"
    return classes


def family_files(root: Path) -> list[Path]:
    """The HUB family files (root ADR-0551): each entry is a lane whose export the publisher resolves."""
    return sorted((root / FOUNDATION / "infra/lakehouse/contracts").glob("hub-lane-family-*.v1.json"))


def reconcile_inputs(root: Path) -> set[Path]:
    """Every file reconcile reads, relative to root: a self-test copies exactly these into its tree."""
    graph = load_json(root / GRAPH)
    paths = {GRAPH, ENDPOINTS, CONTRACTS, FOUNDATION / "services/foundation-outbox-publisher/src/main.rs"}
    paths.update(p.relative_to(root) for p in (root / FOUNDATION / "migrations").glob("*.sql"))
    paths.update(p.relative_to(root) for p in family_files(root))
    paths.update(Path(step) for e in graph["edges"] for step in e.get("via", []) if (root / step).is_file())
    # Root ADR-0400: a collection_decision names a decision record that must exist.
    paths.update(Path(e["collection_decision"]["decision"]) for e in load_json(root / ENDPOINTS)["endpoints"]
                 if "collection_decision" in e and (root / e["collection_decision"]["decision"]).is_file())
    return paths


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
    drop = re.compile(rf"\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?{qualified}\s*;", re.I)
    # DROP TABLE and RENAME are handled by the ordered replay below; refuse only the
    # lifecycle changes this bounded scanner still cannot reconcile (DROP SCHEMA, SET SCHEMA).
    lifecycle = re.compile(rf"\b(?:DROP\s+SCHEMA\s+(?:IF\s+EXISTS\s+)?\"?(?:catalog|serving_postgis)\b|ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?{qualified}[^;]*\bSET\s+SCHEMA\b)", re.I)
    for path in files:
        sql = ignored.sub(" ", path.read_text(encoding="utf-8-sig"))
        if lifecycle.search(drop.sub(" ", rename.sub(" ", sql))):
            raise ValueError(f"unsupported serving table lifecycle in {path.name}; update reconciliation")
        # Replay declarations in order: a preserved table can be renamed and its old name
        # reused, and a superseded serving copy can be dropped (ADR-0108).
        events = sorted(
            [
                *(("create", event) for event in create.finditer(sql)),
                *(("rename", event) for event in rename.finditer(sql)),
                *(("drop", event) for event in drop.finditer(sql)),
            ],
            key=lambda tagged: tagged[1].start(),
        )
        for kind, event in events:
            schema, table, *target = (part.lower() for part in event.groups())
            name = f"{schema}.{table}"
            if kind == "rename":
                renamed = f"{schema}.{target[0]}"
                if name not in tables or renamed in tables:
                    raise ValueError(f"invalid serving table rename in {path.name}: {name} -> {renamed}")
                tables.remove(name)
                tables.add(renamed)
            elif kind == "drop":
                tables.discard(name)
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
    columns_by_table = {c["table_name"]: {col["name"] for col in c.get("columns", [])} for c in contracts.values()}
    # ADR-0288 §1: EVERY silver table that carries a `pnu` is a parcel source and must reach the
    # current pnu through a resolver, never by copying a provider code (the 공매 bypass). The earlier
    # "has a `pnu_basis`/`*_pnu_raw` column" test was an evadable heuristic — a verbatim copy with no
    # such column slipped through — so the invariant is now the `pnu` column itself. Its source→silver
    # feeds edge must either list a resolver in `via` (RESOLVER_VIA), or carry an explicit, non-empty
    # `pnu_resolution` on the edge: the reasoned exemption for a pnu composed by the Rust hub/VWorld
    # export (standard_pnu_from_hub_register_codes, ADR-0145) or that is itself the canonical cadastral
    # PNU. Fail-closed: a new pnu-bearing source with neither is rejected, so a bypass cannot be silent.
    pnu_silver = {name for name, cols in columns_by_table.items() if "pnu" in cols and name.startswith("silver.")}
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
    compare("lakehouse tables", [n.get("table_name", "") for n in nodes if n["type"] in TABLE_TYPES], expected_contracts)
    compare("serving tables", [t for n in nodes if n["type"] == "serving_group" for t in n["tables"]], expected_serving)
    by_id = {n["id"]: n for n in nodes}
    if any("silver" in e for e in endpoints):
        problems.append("endpoint silver duplicates the graph edges")
    # Root ADR-0400. The map once said "승인 후 수집 가능" for 23 endpoints no collector read
    # (ADR-0355 §6): a not-collected endpoint now names why, and a named reason must stay true.
    for endpoint in endpoints:
        slug = endpoint["bronze"]["source_slug"]
        decision = endpoint.get("collection_decision")
        if endpoint["source_acquisition_lane"] in REFUSED_LANES and not decision:
            problems.append(f"refused endpoint names no collection_decision: {slug}")
        if decision is None:
            continue
        verdict = decision.get("verdict") if isinstance(decision, dict) else None
        if verdict not in COLLECTION_VERDICTS:
            problems.append(f"unknown collection_decision verdict: {slug}")
            continue
        adr = decision.get("decision", "")
        if not (isinstance(adr, str) and adr.startswith("docs/adr/") and (root / adr).is_file()):
            problems.append(f"collection_decision names no decision record: {slug}")
        if not str(decision.get("reason_ko", "")).strip():
            problems.append(f"collection_decision gives no reason: {slug}")
        if any(slug in (e.get("source_slugs") or []) and e["status"] in {"implemented", "partial"}
               for e in graph["edges"]):
            problems.append(f"collected endpoint still says it is not collected: {slug}")
    main = (root / FOUNDATION / "services/foundation-outbox-publisher/src/main.rs").read_text(encoding="utf-8")
    commands = set(re.findall(r'Some\("([^"\n]+)"\)', main))
    # A HUB family lane's export is no literal there: the publisher resolves `export-<lane>-silver-handoff`
    # through the family file entry (root ADR-0551, hub_family_silver_export::lane_id_of_export).
    for family in family_files(root):
        lanes = json.loads(family.read_text(encoding="utf-8"))["lanes"]
        commands |= {f"export-{lane}-silver-handoff" for lane in lanes}
    bindings = []
    for node in nodes:
        kind = node["type"]
        if kind not in NODE_TYPES:
            problems.append(f"unknown node type: {node['id']}")
        if not all(node.get(key) for key in ("title", "description", "status", "owner")):
            problems.append(f"missing node metadata: {node['id']}")
        if kind in TABLE_TYPES and not node.get("table_name", "").startswith(kind.removesuffix("_table") + "."):
            problems.append(f"wrong table type: {node['id']}")
        if kind in TABLE_TYPES and node.get("runtime_bindings") != [{"kind": "lakehouse_contract", "value": node.get("table_name")}]:
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
        target = by_id[edge["to"]]
        selected = edge.get("source_slugs")
        resolution = edge.get("pnu_resolution")
        parcel_source = (source["type"] == "source_group" and edge.get("relation") == DATA_RELATION
                         and target.get("table_name") in pnu_silver)
        if parcel_source:
            resolved_by_via = isinstance(via, list) and bool(set(via) & RESOLVER_VIA)
            declared = isinstance(resolution, str) and bool(resolution.strip())
            if not resolved_by_via and not declared:
                problems.append(f"parcel source edge skips the place-identity resolver: {edge['id']}")
            if resolution is not None and not declared:
                problems.append(f"empty pnu_resolution: {edge['id']}")
        elif resolution is not None:
            problems.append(f"pnu_resolution on a non-parcel-source edge: {edge['id']}")
        if source["type"] == "source_group" and edge.get("relation") == DATA_RELATION:
            allowed = {e["bronze"]["source_slug"] for e in endpoints if e["group"] == source["endpoint_catalog_group"]}
            if not isinstance(selected, list) or not selected or not set(selected) <= allowed:
                problems.append(f"edge source selector: {edge['id']}")
        elif selected is not None:
            problems.append(f"endpoint selector outside source data edge: {edge['id']}")
        if edge.get("status") == "implemented" and any(by_id[edge[key]]["status"] in {"contract_only", "collected_only"} for key in ("from", "to")):
            problems.append(f"invented lane for unconnected dataset: {edge['id']}")
    # Connectivity, after dbt_project_evaluator's Root Models / Unused Sources: a lakehouse
    # dataset must be produced and consumed, an active source must be consumed. contract_only
    # is the one sanctioned island; its required description states why no lane exists.
    # reference tables need a producer like any dataset but are exempt from the consumer
    # rule as a kind: the ingest-time resolver reads them cross-cuttingly (ADR-0103 ③),
    # not through a dataset edge, so no consumer edge is their normal state, not a defect.
    produced = {e["to"] for e in edges}
    consumed = {e["from"] for e in edges}
    for node in nodes:
        if node.get("status") == "contract_only":
            continue
        if node["type"] in TABLE_TYPES:
            if node["id"] not in produced:
                problems.append(f"dataset has no producer edge: {node['id']}")
            if node["type"] != "reference_table" and node["id"] not in consumed:
                problems.append(f"dataset has no consumer edge: {node['id']}")
        elif node["type"] == "source_group" and node.get("status") not in {"disabled", "planned", "collection_available"} and node["id"] not in consumed:
            problems.append(f"source group has no consumer edge: {node['id']}")
    problems.extend(place_id_problems(nodes, edges, columns_by_table))
    if problems:
        raise ValueError("\n".join(problems))
    return {"source_groups": len(expected_groups), "lakehouse_tables": len(expected_contracts),
            "serving_tables": len(expected_serving), "nodes": len(nodes), "edges": len(edges)}


def place_id_problems(nodes: list[dict], edges: list[dict], columns_by_table: dict[str, set[str]]) -> list[str]:
    """ADR-0483: every PNU-keyed Gold a resolver-placed parcel source feeds carries `place_id` and reads it
    from the place-id bridge. A parcel source is a Silver whose source edge goes through the resolver
    (RESOLVER_VIA), the set ADR-0288 §1 holds; a Gold without `pnu` (a region's statistics) is keyed by
    something else and is not held here."""
    by_id = {n["id"]: n for n in nodes}
    live = [e for e in edges if e.get("relation") == DATA_RELATION and e["from"] in by_id and e["to"] in by_id]
    placed = {e["to"] for e in live if by_id[e["from"]]["type"] == "source_group"
              and isinstance(e.get("via"), list) and set(e["via"]) & RESOLVER_VIA}
    bridge = {n["id"] for n in nodes if n.get("table_name") == PLACE_ID_BRIDGE}
    problems = []
    for gold_id in sorted({e["to"] for e in live if e["from"] in placed and by_id[e["to"]]["type"] == "gold_table"}):
        table = by_id[gold_id].get("table_name", "")
        columns = columns_by_table.get(table, set())
        if "pnu" not in columns:
            continue
        if PLACE_ID_COLUMN not in columns:
            problems.append(f"parcel source Gold carries no {PLACE_ID_COLUMN}: {table}")
        if not any(e["from"] in bridge and e["to"] == gold_id for e in live):
            problems.append(f"parcel source Gold does not read {PLACE_ID_BRIDGE}: {table}")
    return problems


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
