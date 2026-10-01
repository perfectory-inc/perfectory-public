"""Interim seed: sends Foundation's declared pipeline graph to DataHub as OpenLineage events.

Stands in until data contracts are registered directly (root ADR-0117 §2, §3); delete it then.

One job per produced node ("build <node>"), whose inputs are the nodes that feed it. Each node's
title, type and declared status go in its documentation facet. This is the DECLARED plan, not an
observed run; the job says so. Standard library only; runs on ai-server.
"""

import json
import os
import sys
import urllib.request
import uuid
from collections import defaultdict
from datetime import datetime, timezone

GRAPH = os.environ.get("FOUNDATION_PIPELINE_GRAPH_URL", "http://127.0.0.1:18080/catalog/v1/pipeline-graph")
GMS = os.environ.get("DATAHUB_GMS_URL", "http://127.0.0.1:18095")
LINEAGE = f"{GMS}/openapi/openlineage/api/v1/lineage"
INGEST = f"{GMS}/aspects?action=ingestProposal"
NAMESPACE = "perfectory"
PRODUCER = "https://github.com/perfectory-inc/perfectory-public/pipeline-graph-seed"
SCHEMA = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"


def dataset(node):
    name = node.get("table_name") or node["id"]
    doc = f'{node.get("title", "")} — 종류 {node.get("type")}, 선언 상태 {node.get("status")}. {node.get("description", "")}'
    return {
        "namespace": NAMESPACE,
        "name": name,
        "facets": {
            "documentation": {
                "_producer": PRODUCER,
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DocumentationDatasetFacet.json",
                "description": doc.strip(),
            }
        },
    }


def main():
    graph = json.load(urllib.request.urlopen(GRAPH, timeout=30))
    nodes = {n["id"]: n for n in graph["nodes"]}
    feeds = defaultdict(list)
    for edge in graph["edges"]:
        if edge["from"] in nodes and edge["to"] in nodes:
            feeds[edge["to"]].append(edge["from"])
    now = datetime.now(timezone.utc).isoformat()
    sent = failed = 0
    for target, sources in sorted(feeds.items()):
        event = {
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
                "namespace": NAMESPACE,
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
        request = urllib.request.Request(
            LINEAGE, data=json.dumps(event).encode(), headers={"Content-Type": "application/json"}, method="POST"
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                sent += response.status < 300
        except urllib.error.HTTPError as error:
            failed += 1
            print(target, error.code, error.read()[:300], file=sys.stderr)
    # DataHub's OpenLineage converter keeps job documentation but drops a dataset's, so each
    # dataset's title, description and declared status are written as its properties directly.
    described = 0
    for node in nodes.values():
        name = node.get("table_name") or node["id"]
        urn = f"urn:li:dataset:(urn:li:dataPlatform:{NAMESPACE},{name},PROD)"
        properties = {
            "name": name,
            "description": f'{node.get("title", "")} — {node.get("description", "")}'.strip(" —"),
            "customProperties": {
                "title": node.get("title", ""),
                "node_type": str(node.get("type", "")),
                "declared_status": str(node.get("status", "")),
                "pipeline_graph_id": node["id"],
            },
        }
        proposal = {
            "proposal": {
                "entityType": "dataset",
                "entityUrn": urn,
                "changeType": "UPSERT",
                "aspectName": "datasetProperties",
                "aspect": {"contentType": "application/json", "value": json.dumps(properties)},
            }
        }
        request = urllib.request.Request(
            INGEST,
            data=json.dumps(proposal).encode(),
            headers={"Content-Type": "application/json", "X-RestLi-Protocol-Version": "2.0.0"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                described += response.status < 300
        except urllib.error.HTTPError as error:
            failed += 1
            print(name, error.code, error.read()[:300], file=sys.stderr)
    print(
        f"nodes={len(nodes)} edges={len(graph['edges'])} jobs_sent={sent} "
        f"datasets_described={described} failed={failed}"
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
