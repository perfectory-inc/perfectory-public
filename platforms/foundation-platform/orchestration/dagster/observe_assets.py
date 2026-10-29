"""Dagster's assets, generated from the two files Airflow's DAGs come from (root ADR-0333 §2, stage 0).

Nothing here is written by hand: every pipeline-graph node (docs/catalog/pipeline-graph.v1.json) is
an asset, its edges are the asset's deps, and the jobs in orchestration/jobs.v1.json that write it
are mapped to it through dagster-airlift. The mapping is the task that records the job's output
asset events (`job_specs.PUBLISH_TASK_ID`): Airflow records them exactly when that task succeeds, so
Dagster's materialization is the same fact as Airflow's asset event, read from the same job spec
(`job_specs.load_specs`, which foundation_jobs.py builds the DAGs from). The two orchestrators
cannot drift: a job or edge changes both or neither.

In this stage Dagster only observes (airlift's "observe" step): Airflow runs every job.
"""

import json
import pathlib
import sys

from dagster import AssetKey, AssetSpec
from dagster_airlift.core import assets_with_multiple_task_mappings

DAGS = pathlib.Path(__file__).resolve().parents[1] / "dags"
if str(DAGS) not in sys.path:
    sys.path.insert(0, str(DAGS))

import job_specs  # noqa: E402  (beside foundation_jobs.py; it adds infra/datahub to the path)
from dataset_names import name_of, platform_of  # noqa: E402


def asset_key(dataset):
    """The Dagster key of a pipeline-graph dataset: its catalog platform, then its name's dotted parts.

    `iceberg://gold.unit_price` is `iceberg/gold/unit_price`: the identity Airflow's asset and the
    data catalog already use (job_specs.asset_uri), split where the catalog's name is.
    """
    namespace, name = dataset
    return AssetKey([namespace, *name.split(".")])


def contracts():
    """jobs.v1.json and pipeline-graph.v1.json, as job_specs reads them."""
    return (json.loads(job_specs.JOBS.read_text(encoding="utf-8")),
            json.loads(job_specs.GRAPH.read_text(encoding="utf-8")))


def node_keys(graph):
    """Pipeline-graph node id -> its asset key; two nodes that would share a key are refused."""
    keys = {node["id"]: asset_key((platform_of(node), name_of(node))) for node in graph["nodes"]}
    if len(set(keys.values())) != len(keys):
        raise job_specs.JobListError("two pipeline-graph nodes name the same dataset; Dagster needs one key each")
    return keys


def writers(jobs, graph):
    """Pipeline-graph node id -> the DAG ids whose publish task records its events, in job-list order."""
    specs = {spec.job_id: spec for spec in job_specs.load_specs(jobs, graph)}
    found = {}
    for job_id, (_, outputs) in job_specs.job_nodes(jobs, graph).items():
        for node_id in outputs:
            found.setdefault(node_id, []).append(specs[job_id].dag_id)
    return found


def _reaches(edges, start, goal):
    seen, todo = set(), [start]
    while todo:
        node = todo.pop()
        if node == goal:
            return True
        if node not in seen:
            seen.add(node)
            todo.extend(edge["to"] for edge in edges if edge["from"] == node)
    return False


def feedback_edges(graph):
    """The edges by which a serving surface feeds what it is made from: staff decisions coming back
    (the lineage stewardship loop, root ADR-0118 §2). An asset graph has no cycles, so these are the
    asset's metadata, not its deps; any other cycle is refused, so a new one is a decision, not a hang
    (dagster-airlift walks deps recursively and never returns on a cycle)."""
    types = {node["id"]: node["type"] for node in graph["nodes"]}
    feedback = {edge["id"] for edge in graph["edges"]
                if types[edge["from"]].startswith("serving") and _reaches(graph["edges"], edge["to"], edge["from"])}
    rest = [edge for edge in graph["edges"] if edge["id"] not in feedback]
    looping = sorted(edge["id"] for edge in rest if _reaches(rest, edge["to"], edge["from"]))
    if looping:
        raise job_specs.JobListError(f"pipeline-graph edges form a cycle that no serving surface closes: {looping}")
    return feedback


def observed_assets(jobs=None, graph=None):
    """One AssetSpec per pipeline-graph node; the nodes a job writes are mapped to its publish task."""
    if jobs is None or graph is None:
        jobs, graph = contracts()
    keys = node_keys(graph)
    feedback = feedback_edges(graph)
    deps, fed_back = {}, {}
    for edge in graph["edges"]:
        (fed_back if edge["id"] in feedback else deps).setdefault(edge["to"], set()).add(edge["from"])
    written = writers(jobs, graph)
    assets = []
    for node in graph["nodes"]:
        dataset = (platform_of(node), name_of(node))
        spec = AssetSpec(
            keys[node["id"]],
            deps=[keys[source] for source in sorted(deps.get(node["id"], ()))],
            description=node.get("title"),
            group_name=node["type"],
            kinds={dataset[0]},
            metadata={"pipeline_graph_node": node["id"], "dataset": job_specs.asset_uri(dataset),
                      **({"fed_back_by": sorted(fed_back[node["id"]])} if node["id"] in fed_back else {})},
        )
        if node["id"] in written:
            assets.extend(assets_with_multiple_task_mappings(
                assets=[spec],
                task_handles=[{"dag_id": dag_id, "task_id": job_specs.PUBLISH_TASK_ID} for dag_id in written[node["id"]]],
            ))
        else:
            assets.append(spec)
    return assets


def dag_ids(jobs=None, graph=None):
    """The DAGs Dagster reads: one per job, as foundation_jobs.py names them."""
    if jobs is None or graph is None:
        jobs, graph = contracts()
    return {spec.dag_id for spec in job_specs.load_specs(jobs, graph)}
