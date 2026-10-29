"""A lakehouse table's snapshot history from the catalog's REST API: no Spark, no data file.

The history comes back in the shape the Gold planner reads from Spark's `<table>.snapshots`
(infra/lakehouse/spark/jobs/gold_rebuild.py `_table_state`): {"head": id or None, "snapshots": [{
"snapshot_id", "parent_id", "committed_at", "operation", "summary"}]}, ids as strings. So the asset
reconciler (asset_reconcile.py) and the unit price bake's follow gate (lane_follow_gate.py) decide
with the planner's own functions (`recorded_pins`, `input_change_since_pin`), never a second copy.

The catalog settings are the REST URI, the warehouse and a token: the asset reconciler reads with the
operations map's read-only token (operations-map.env, as the shape ledger does, root ADR-0259); the
bake's gate with the writer token its unit already holds (map-edit-fold.env). One `config` call per
process gives the warehouse prefix.
"""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone


def history(metadata: dict) -> dict:
    """The planner's {"head", "snapshots"} of an Iceberg table-metadata document."""
    refs = metadata.get("refs") or {}
    head = refs["main"]["snapshot-id"] if "main" in refs else metadata.get("current-snapshot-id")
    snapshots = []
    for snapshot in metadata.get("snapshots") or []:
        summary = dict(snapshot.get("summary") or {})
        parent = snapshot.get("parent-snapshot-id")
        snapshots.append({
            "snapshot_id": str(snapshot["snapshot-id"]),
            "parent_id": None if parent is None else str(parent),
            "committed_at": datetime.fromtimestamp(snapshot["timestamp-ms"] / 1000, timezone.utc)
                            .strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z",
            "operation": summary.get("operation"),
            "summary": summary,
        })
    return {"head": None if head in (None, -1) else str(head), "snapshots": snapshots}


def committed_epoch(snapshot: dict) -> float:
    """A snapshot's commit time as seconds since the epoch."""
    return datetime.fromisoformat(snapshot["committed_at"].replace("Z", "+00:00")).timestamp()


class Catalog:
    """Reads table histories from the Iceberg REST catalog the lakehouse jobs commit through."""

    def __init__(self, token: str, user_agent: str, environ=os.environ):
        """`token`: the variable that holds the catalog token, the read-only one where the unit has it."""
        self.uri = environ["FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI"].rstrip("/")
        self.warehouse = environ["FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE"]
        self.token = environ[token]
        # Cloudflare refuses urllib's default User-Agent with 403 (lineage-queue-check.py).
        self.user_agent = user_agent
        self.prefix = None

    def _get(self, url: str) -> dict:
        request = urllib.request.Request(url, headers={"Authorization": f"Bearer {self.token}",
                                                       "User-Agent": self.user_agent})
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response)

    def table(self, name: str) -> dict:
        """The {"head", "snapshots"} of `namespace.table`."""
        if self.prefix is None:
            config = self._get(f"{self.uri}/v1/config?warehouse={urllib.parse.quote(self.warehouse)}")
            self.prefix = config["overrides"]["prefix"]
        namespace, table = name.split(".", 1)
        return history(self._get(f"{self.uri}/v1/{self.prefix}/namespaces/{namespace}/tables/{table}")["metadata"])
