"""Once-read physical selections and ownership evidence for one lineage derivation."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from lakehouse_snapshot_pins import read_pinned_iceberg, read_snapshot_document, snapshot_id

PHYSICAL_TABLE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*")


@dataclass(frozen=True)
class OwnershipInput:
    rows: dict[str, dict[str, Any]]
    sha256: str | None


@dataclass(frozen=True)
class LineageInputs:
    sources: dict[str, dict[str, str]]
    ownership_old: OwnershipInput
    ownership_new: OwnershipInput


def input_roles(args: Any) -> tuple[str, ...]:
    roles = ("boundaries_from", "boundaries_to", "code_changes", "history")
    if bool(args.building_from_snapshot_id) != bool(args.building_to_snapshot_id):
        raise ValueError("--building-from-snapshot-id and --building-to-snapshot-id go together")
    if args.building_from_snapshot_id:
        roles += ("buildings_from", "buildings_to")
    return roles


def read_ownership(path: str | None) -> OwnershipInput:
    if not path:
        return OwnershipInput({}, None)
    payload = Path(path).read_bytes()
    rows = {}
    for line in payload.decode("utf-8").splitlines():
        if line.strip():
            row = json.loads(line)
            rows[row["pnu"]] = row
    return OwnershipInput(rows, hashlib.sha256(payload).hexdigest())


def load_lineage_inputs(args: Any) -> LineageInputs:
    """Freeze file contents before Spark; later reads and provenance reuse this selection."""
    roles = input_roles(args)
    if not args.source_snapshots_path:
        raise ValueError("--source-snapshots-path is required for every lineage input")
    document = read_snapshot_document(args.source_snapshots_path)
    if not isinstance(document, dict) or set(document) != set(roles):
        raise ValueError("--source-snapshots-path must name every consumed lineage input exactly once")
    sources = {}
    for role in roles:
        binding = document[role]
        if not isinstance(binding, dict) or set(binding) != {"table", "snapshot_id"}:
            raise ValueError(f"{role} must contain exactly table and snapshot_id")
        table = binding["table"]
        if not isinstance(table, str) or PHYSICAL_TABLE.fullmatch(table) is None:
            raise ValueError(f"{role} table must be namespace.table using plain SQL identifiers")
        sources[role] = {"table": table.lower(), "snapshot_id": snapshot_id(binding["snapshot_id"])}
    return LineageInputs(sources, read_ownership(args.ownership_old_jsonl), read_ownership(args.ownership_new_jsonl))


def physical_inputs(catalog: str, inputs: LineageInputs) -> dict[str, dict[str, str]]:
    return {
        role: {"table": f"{catalog.lower()}.{binding['table']}", "snapshot_id": binding["snapshot_id"]}
        for role, binding in inputs.sources.items()
    }


def bind_input_views(spark: Any, catalog: str, inputs: LineageInputs) -> dict[str, str]:
    """Bind each role once; SQL readers see only these explicitly pinned relations."""
    pins = {role: binding["snapshot_id"] for role, binding in inputs.sources.items()}
    views = {}
    for role, binding in physical_inputs(catalog, inputs).items():
        view = f"_parcel_lineage_{role}"
        read_pinned_iceberg(spark, binding["table"], role, pins).createOrReplaceTempView(view)
        views[role] = f"`{view}`"
    return views
