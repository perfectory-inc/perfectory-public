"""Fault injection for ADR-0086: omissions, duplicates, owner growth and dangling edges."""

from copy import deepcopy
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

from pipeline_graph import CONTRACTS, ENDPOINTS, FOUNDATION, GRAPH, ROOT, load_json


def write_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="")


def main() -> None:
    baseline = load_json(ROOT / GRAPH)
    with tempfile.TemporaryDirectory(prefix="pipeline-graph-self-test-") as temporary:
        root = Path(temporary)
        paths = {GRAPH, ENDPOINTS, CONTRACTS,
                 FOUNDATION / "services/foundation-outbox-publisher/src/main.rs"}
        paths.update(p.relative_to(ROOT) for p in (ROOT / FOUNDATION / "migrations").glob("*.sql"))
        paths.update(Path(step) for e in baseline["edges"] for step in e.get("via", []) if (ROOT / step).is_file())
        for path in paths:
            (root / path).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / path, root / path)

        def check(label: str, diagnostic: str | None = None) -> None:
            result = subprocess.run([sys.executable, str(ROOT / "scripts/catalog/pipeline_graph.py"), str(root)],
                                    capture_output=True, text=True, encoding="utf-8")
            if diagnostic is None:
                assert result.returncode == 0, result.stdout + result.stderr
                print(result.stdout.strip())
            else:
                assert result.returncode == 1 and diagnostic in result.stderr, (label, result.returncode, result.stdout, result.stderr)
                print(f"REJECTED {label}: exit=1 ({diagnostic})")

        check("baseline")
        cases = [
            ("(a) missing source group", "source groups", lambda g: g["nodes"].remove(next(n for n in g["nodes"] if n["type"] == "source_group"))),
            ("(b) missing Silver table", "lakehouse tables", lambda g: g["nodes"].remove(next(n for n in g["nodes"] if n["type"] == "silver_table"))),
            ("(b) missing Gold table", "lakehouse tables", lambda g: g["nodes"].remove(next(n for n in g["nodes"] if n["type"] == "gold_table"))),
            ("(c) missing serving table", "serving tables", lambda g: next(n for n in g["nodes"] if n["type"] == "serving_group")["tables"].pop()),
            ("(c) duplicate serving table", "duplicates=", lambda g: next(n for n in g["nodes"] if n["type"] == "serving_group")["tables"].append(next(n for n in g["nodes"] if n["type"] == "serving_group")["tables"][0])),
            ("(d) dangling edge", "edge endpoints", lambda g: g["edges"][0].update(to="does-not-exist")),
            ("missing executable", "edge via", lambda g: g["edges"][0].pop("via")),
            ("invented executable", "unknown via", lambda g: g["edges"][0].update(via=["invented-command"])),
            ("wrong source selector", "edge source selector", lambda g: next(e for e in g["edges"] if "source_slugs" in e).update(source_slugs=["nonexistent-source"])),
            ("(a) extra source group", "source groups", lambda g: next(n for n in g["nodes"] if n["type"] == "source_group").update(endpoint_catalog_group="nonexistent-group")),
            ("(a) duplicate source group", "source groups", lambda g: g["nodes"].append({**next(n for n in g["nodes"] if n["type"] == "source_group"), "id": "extra-group"})),
            ("(b) extra lakehouse table", "lakehouse tables", lambda g: next(n for n in g["nodes"] if n["type"] == "silver_table").update(table_name="silver.nonexistent_table")),
            ("(b) duplicate lakehouse table", "lakehouse tables", lambda g: g["nodes"].append({**next(n for n in g["nodes"] if n["type"] == "gold_table"), "id": "extra-gold"})),
            ("(b) wrong table layer", "wrong table type", lambda g: next(n for n in g["nodes"] if n["type"] == "gold_table").update(type="silver_table")),
            ("(c) extra serving table", "serving tables", lambda g: next(n for n in g["nodes"] if n["type"] == "serving_group")["tables"].append("catalog.nonexistent_table")),
            ("(d) dangling source", "edge endpoints", lambda g: g["edges"][0].update({"from": "does-not-exist"})),
            ("unknown node type", "unknown node type", lambda g: g["nodes"][0].update(type="invented_type")),
            ("mismatched table binding", "runtime binding disagrees", lambda g: next(n for n in g["nodes"] if n["type"] == "silver_table")["runtime_bindings"][0].update(value="silver.wrong_table")),
            ("invented contract-only lane", "invented lane", lambda g: next(n for n in g["nodes"] if n["id"] == g["edges"][0]["to"]).update(status="contract_only")),
            ("duplicate node ID", "node IDs", lambda g: g["nodes"].append(deepcopy(g["nodes"][0]))),
            ("old schema", "schema_version", lambda g: g.update(schema_version="foundation-platform.pipeline_graph.v1")),
        ]
        for label, diagnostic, mutate in cases:
            graph = deepcopy(baseline)
            mutate(graph)
            write_json(root / GRAPH, graph)
            check(label, diagnostic)
        write_json(root / GRAPH, baseline)

        endpoints = load_json(root / ENDPOINTS)
        added = deepcopy(endpoints["endpoints"][0])
        added.update(group="new_owner_group", endpoint_slug="new-endpoint")
        endpoints["endpoints"].append(added)
        write_json(root / ENDPOINTS, endpoints)
        check("(a) new endpoint owner group", "source groups")
        shutil.copyfile(ROOT / ENDPOINTS, root / ENDPOINTS)
        contracts = load_json(root / CONTRACTS)
        contracts["contracts"]["silver.new_owner_table"] = {"table_name": "silver.new_owner_table"}
        write_json(root / CONTRACTS, contracts)
        check("(b) new contract owner table", "lakehouse tables")
        shutil.copyfile(ROOT / CONTRACTS, root / CONTRACTS)
        migration = root / FOUNDATION / "migrations/99990101000000_graph_self_test.sql"
        migration.write_text('CREATE\nUNLOGGED TABLE IF NOT EXISTS catalog.new_owner_table (id integer);\n', encoding="utf-8", newline="")
        check("(c) new migration owner table", "serving tables")
        migration.write_text('-- CREATE TABLE catalog.comment_only (id integer);\n/* CREATE TABLE catalog.comment_too (id integer); */\n', encoding="utf-8", newline="")
        check("comments are not tables")
        migration.write_text('DROP TABLE catalog.parcel;\n', encoding="utf-8", newline="")
        check("unsupported table lifecycle", "unsupported serving table lifecycle")
        migration.write_text('', encoding="utf-8", newline="")

        # Exercise rendering through the CLI, including both derived artifacts.
        (root / "docs").mkdir(exist_ok=True)
        renderer = ROOT / "scripts/catalog/render-pipeline-map.py"
        def render_check(*args: str) -> subprocess.CompletedProcess:
            return subprocess.run([sys.executable, str(renderer), "--root", str(root), *args],
                                  capture_output=True, text=True, encoding="utf-8")
        result = render_check()
        assert result.returncode == 0, result.stderr
        assert render_check("--check").returncode == 0
        output = root / "docs/data-pipeline-map.md"
        clean = output.read_bytes()
        assert b"\r" not in clean
        for content, label in [(clean + b"stale\n", "stale document"),
                               (clean.replace(b"\n", b"\r\n"), "CRLF document")]:
            output.write_bytes(content)
            result = render_check("--check")
            assert result.returncode == 1 and "stale pipeline map artifact" in result.stderr
            print(f"REJECTED {label}: exit=1 (stale pipeline map artifact)")
        output.write_bytes(clean)
        example = root / GRAPH.with_name("pipeline-graph.v1.example.json")
        example.write_bytes(b"{}\n")
        assert render_check("--check").returncode == 1
        print("REJECTED stale API example: exit=1 (stale pipeline map artifact)")
    # The published API schema and the registry migrate together.
    schemas = load_json(ROOT / FOUNDATION / "docs/openapi/pipeline-graph.v1.json")["components"]["schemas"]
    assert baseline["schema_version"] in schemas["PipelineGraphResponse"]["properties"]["schema_version"]["enum"]
    assert {n["type"] for n in baseline["nodes"]} == set(schemas["PipelineGraphNode"]["properties"]["type"]["enum"])
    for node in baseline["nodes"]:
        assert set(schemas["PipelineGraphNode"]["required"]) <= node.keys(), node["id"]
    for edge in baseline["edges"]:
        assert set(schemas["PipelineGraphEdge"]["required"]) <= edge.keys(), edge["id"]
    print("OK pipeline-graph-covers-every-dataset-self-test")


if __name__ == "__main__":
    main()
