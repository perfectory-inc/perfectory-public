"""Fault injection for the connectivity rules: producer-less and consumer-less nodes are rejected.

Modeled on dbt_project_evaluator's Root Models / Unused Sources. Each case plants a
disconnection in a copy of the graph and proves the guard exits 1 naming it.
"""

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
    with tempfile.TemporaryDirectory(prefix="pipeline-graph-connectivity-") as temporary:
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

        def dataset(graph: dict, connected: str) -> dict:
            key = {"producer": "to", "consumer": "from"}[connected]
            return next(n for n in graph["nodes"]
                        if n["type"] in {"silver_table", "gold_table"} and n["status"] != "contract_only"
                        and any(e[key] == n["id"] for e in graph["edges"]))

        def sever(graph: dict, node: dict, key: str) -> None:
            graph["edges"] = [e for e in graph["edges"] if e[key] != node["id"]]

        # The baseline must hold both islands: contract_only is the only sanctioned disconnection.
        islands = [n for n in baseline["nodes"] if n["status"] == "contract_only"]
        assert {n["table_name"] for n in islands} == {"silver.complex_parcel_memberships", "gold.complex_spatial_locator"}, islands
        cases = [
            ("producer-less dataset", "dataset has no producer edge",
             lambda g: sever(g, dataset(g, "producer"), "to")),
            ("consumer-less dataset", "dataset has no consumer edge",
             lambda g: sever(g, dataset(g, "consumer"), "from")),
            ("consumer-less active source", "source group has no consumer edge",
             lambda g: next(n for n in g["nodes"] if n["type"] == "source_group"
                            and not any(e["from"] == n["id"] for e in g["edges"])).update(status="partial")),
            # Reference tables are consumer-exempt by kind, never producer-exempt.
            ("producer-less reference table", "dataset has no producer edge",
             lambda g: sever(g, next(n for n in g["nodes"] if n["type"] == "reference_table"), "to")),
            ("contract_only exemption is status-bound", "dataset has no producer edge",
             lambda g: next(n for n in g["nodes"] if n["status"] == "contract_only").update(status="implemented")),
        ]
        for label, diagnostic, mutate in cases:
            graph = deepcopy(baseline)
            mutate(graph)
            write_json(root / GRAPH, graph)
            check(label, diagnostic)
        write_json(root / GRAPH, baseline)
        # Green baseline proves both structural exemptions: the contract_only islands and the
        # consumer-less reference tables pass while everything else stays connected.
        check("baseline with contract_only islands and consumer-exempt reference tables")
    print("OK pipeline-graph-connectivity-self-test")


if __name__ == "__main__":
    main()
