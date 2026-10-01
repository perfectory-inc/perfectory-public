"""How a pipeline-graph node is named in the data catalog (root ADR-0117 §3).

One rule for every producer of lineage: the declared-graph seed and the Airflow jobs (root
ADR-0122) both name a node through this module, so a run and the plan land on the same entity.

- A lakehouse table (silver./gold./reference.) is on the `iceberg` platform, the entity the
  lakehouse ingestion fills with columns and snapshots.
- Every other node (sources, serving tables, surfaces) is on the `perfectory` platform.
"""

DECLARED = "perfectory"
LAKEHOUSE = "iceberg"
LAKEHOUSE_NAMESPACES = ("silver.", "gold.", "reference.")


def name_of(node):
    return node.get("table_name") or node["id"]


def platform_of(node):
    return LAKEHOUSE if name_of(node).startswith(LAKEHOUSE_NAMESPACES) else DECLARED
