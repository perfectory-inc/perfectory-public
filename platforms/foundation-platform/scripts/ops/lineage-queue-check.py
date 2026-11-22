#!/usr/bin/env python3
"""Is the review queue already built from the lineage's current snapshot? (root ADR-0277's shape)

The stewardship cycle asks before it starts the queue's Spark run. Two catalog reads over REST, no
Spark: `silver.parcel_lineage`'s current snapshot id, and the one the queue's current Gold commit
recorded (`foundation.lineage-snapshot-id`, written by lineage_review_queue_to_gold.py). Prints
`unchanged <snapshot id>` when they are the same, otherwise `would_change`, and on any error
`would_change (<reason>)`: a check that cannot tell lets the run happen. Always exits 0.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.parse
import urllib.request

PROPERTY = "foundation.lineage-snapshot-id"
LINEAGE = ("silver", "parcel_lineage")
QUEUE = ("gold", "lineage_review_queue")


def verdict(lineage: dict, queue: dict) -> str:
    """`unchanged <id>` when the queue's current commit recorded the lineage's current snapshot."""
    current = lineage.get("current-snapshot-id")
    commits = {snapshot["snapshot-id"]: snapshot for snapshot in queue.get("snapshots", [])}
    recorded = commits.get(queue.get("current-snapshot-id"), {}).get("summary", {}).get(PROPERTY)
    if current in (None, -1) or recorded != str(current):
        return "would_change"
    return f"unchanged {current}"


def table_metadata(namespace: str, name: str) -> dict:
    uri = os.environ["FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI"].rstrip("/")
    warehouse = os.environ["FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE"]
    token = os.environ["FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN"]

    def get(url: str) -> dict:
        # Cloudflare refuses urllib's default User-Agent with 403.
        request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}",
                                                       "User-Agent": "foundation-lineage-queue-check"})
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response)

    prefix = get(f"{uri}/v1/config?warehouse={urllib.parse.quote(warehouse)}")["overrides"]["prefix"]
    return get(f"{uri}/v1/{prefix}/namespaces/{namespace}/tables/{name}")["metadata"]


def main() -> int:
    try:
        print(verdict(table_metadata(*LINEAGE), table_metadata(*QUEUE)))
    except (OSError, KeyError, ValueError) as error:
        print(f"would_change ({type(error).__name__}: {error})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
