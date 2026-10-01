"""Interim seed: sends Foundation's declared pipeline graph to the data catalog (DataHub).

Stands in until data contracts are registered directly (root ADR-0117 §2, §3); delete it then.

- Nodes are named by dataset_names.py, the rule the Airflow jobs share. A lakehouse table's title,
  description and declared status go in the editable description, which ingestion never writes;
  every other node's properties are written directly.
- One job per produced node ("build <node>") carries the declared edges as an OpenLineage event.
  This is the DECLARED plan, not an observed run; the job says so.
- An earlier seed named lakehouse tables on `perfectory`; those duplicates are soft-deleted
  (status.removed), never hard-deleted.

Standard library only; runs where it can reach Foundation and GMS (see datahub-runtime.sh).
"""

import json
import os
import sys
import urllib.error
import urllib.request
import uuid
from collections import defaultdict
from datetime import datetime, timezone

from dataset_names import DECLARED, LAKEHOUSE, name_of, platform_of

GRAPH = os.environ.get(
    "FOUNDATION_PIPELINE_GRAPH_URL", "http://127.0.0.1:18080/catalog/v1/pipeline-graph"
)
GMS = os.environ.get("DATAHUB_GMS_URL", "http://127.0.0.1:18095")
LINEAGE = f"{GMS}/openapi/openlineage/api/v1/lineage"
INGEST = f"{GMS}/aspects?action=ingestProposal"
PRODUCER = "https://github.com/perfectory-inc/perfectory-public/pipeline-graph-seed"
SCHEMA = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"


def urn_of(platform, name):
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},PROD)"


def description_of(node):
    text = f'{node.get("title", "")} — {node.get("description", "")}'.strip(" —")
    return f'{text}\n\n선언 상태: {node.get("status", "")} · 종류: {node.get("type", "")}'


def post(url, body, headers=None):
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", **(headers or {})},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.status < 300


def propose(urn, aspect_name, value):
    return post(
        INGEST,
        {
            "proposal": {
                "entityType": "dataset",
                "entityUrn": urn,
                "changeType": "UPSERT",
                "aspectName": aspect_name,
                "aspect": {"contentType": "application/json", "value": json.dumps(value)},
            }
        },
        {"X-RestLi-Protocol-Version": "2.0.0"},
    )


def lineage_event(target, sources, nodes, now):
    def dataset(node):
        return {"namespace": platform_of(node), "name": name_of(node)}

    return {
        "eventType": "COMPLETE",
        "eventTime": now,
        "producer": PRODUCER,
        "schemaURL": SCHEMA,
        "run": {
            "runId": str(uuid.uuid4()),
            # DataHub names the orchestrator from this facet; without it the event is refused.
            "facets": {
                "processing_engine": {
                    "_producer": PRODUCER,
                    "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json",
                    "name": "perfectory-pipeline-graph",
                    "version": "1",
                }
            },
        },
        "job": {
            "namespace": DECLARED,
            "name": f"build {target}",
            "facets": {
                "documentation": {
                    "_producer": PRODUCER,
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DocumentationJobFacet.json",
                    "description": "선언된 흐름(pipeline-graph.v1.json). 실제 실행 기록이 아니다.",
                }
            },
        },
        "inputs": [dataset(nodes[s]) for s in sorted(set(sources))],
        "outputs": [dataset(nodes[target])],
    }


def main():
    graph = json.load(urllib.request.urlopen(GRAPH, timeout=30))
    nodes = {n["id"]: n for n in graph["nodes"]}
    feeds = defaultdict(list)
    for edge in graph["edges"]:
        if edge["from"] in nodes and edge["to"] in nodes:
            feeds[edge["to"]].append(edge["from"])
    now = datetime.now(timezone.utc).isoformat()
    counts = defaultdict(int)

    def attempt(kind, call):
        try:
            counts[kind] += bool(call())
        except urllib.error.HTTPError as error:
            counts["failed"] += 1
            print(kind, error.code, error.read()[:300], file=sys.stderr)

    for target, sources in sorted(feeds.items()):
        attempt("jobs", lambda: post(LINEAGE, lineage_event(target, sources, nodes, now)))

    for node in nodes.values():
        name, platform = name_of(node), platform_of(node)
        if platform == LAKEHOUSE:
            attempt(
                "lakehouse_described",
                lambda: propose(
                    urn_of(LAKEHOUSE, name),
                    "editableDatasetProperties",
                    {"description": description_of(node)},
                ),
            )
            attempt(
                "duplicates_removed",
                lambda: propose(urn_of(DECLARED, name), "status", {"removed": True}),
            )
        else:
            attempt(
                "declared_described",
                lambda: propose(
                    urn_of(DECLARED, name),
                    "datasetProperties",
                    {
                        "name": name,
                        "description": description_of(node),
                        "customProperties": {
                            "title": node.get("title", ""),
                            "node_type": str(node.get("type", "")),
                            "declared_status": str(node.get("status", "")),
                            "pipeline_graph_id": node["id"],
                        },
                    },
                ),
            )

    print(
        f"nodes={len(nodes)} edges={len(graph['edges'])} "
        + " ".join(f"{key}={value}" for key, value in sorted(counts.items()))
    )
    return 1 if counts["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
