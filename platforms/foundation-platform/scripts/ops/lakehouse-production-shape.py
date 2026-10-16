#!/usr/bin/env python3
"""Capture the production lakehouse's table shapes into the committed shape ledger (root ADR-0259).

Read-only: the REST catalog for every table of every namespace a contract names, and Trino's
`$files` metadata table for the column sets its live data files hold. Writes, per table, whether its
current snapshot holds rows, its columns now, and every other set of columns a live data file was
written with. No row value leaves the catalog, so the ledger is safe for the public repository.

Usage (the catalog settings are the ones `lakehouse-migrate` runs with; `.env.local` on a laptop;
Trino is the server's, published on its loopback, reached through `ssh -L 18081:127.0.0.1:18081`):
  FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI=... FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE=... \
  FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_READER_TOKEN=... [FOUNDATION_PLATFORM_TRINO_URL=http://127.0.0.1:18081] \
  python3 scripts/ops/lakehouse-production-shape.py
"""

from __future__ import annotations

import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

AREA = Path(__file__).resolve().parents[2]
CONTRACTS = AREA / "infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json"
LEDGER = AREA / "infra/lakehouse/contracts/lakehouse-production-shape.json"
SCHEMA_VERSION = "foundation-platform.lakehouse_production_shape.v1"

# Iceberg primitive types in the contracts' logical vocabulary (platform_contracts.spark_sql_type).
# Anything else is kept as `iceberg:<type>`, which no contract type equals, so the gate refuses it.
PRIMITIVES = {"string": "string", "binary": "binary", "boolean": "boolean", "int": "int", "long": "long",
              "double": "double", "date": "date", "timestamptz": "timestamp"}


def logical_type(iceberg_type: object) -> str:
    if isinstance(iceberg_type, str):
        if iceberg_type.startswith("decimal("):
            return iceberg_type.replace(" ", "")
        return PRIMITIVES.get(iceberg_type, f"iceberg:{iceberg_type}")
    if isinstance(iceberg_type, dict) and iceberg_type.get("type") == "list":
        return f"array<{logical_type(iceberg_type['element'])}>"
    return f"iceberg:{json.dumps(iceberg_type, sort_keys=True)}"


def nested_ids(iceberg_type: object) -> list[int]:
    """The field ids inside a nested type: a list's element, a map's key and value, a struct's fields."""
    if not isinstance(iceberg_type, dict):
        return []
    children = {"list": [("element-id", "element")], "map": [("key-id", "key"), ("value-id", "value")]}.get(
        iceberg_type.get("type"), [])
    found = []
    for id_key, type_key in children:
        found += [iceberg_type[id_key], *nested_ids(iceberg_type[type_key])]
    for field in iceberg_type.get("fields", []) if iceberg_type.get("type") == "struct" else []:
        found += [field["id"], *nested_ids(field["type"])]
    return found


def table_shape(metadata: dict, file_shapes: list[list[int]]) -> dict:
    """A table's shape by Iceberg field id: every field it ever had, the current schema's columns in
    their order, and every other set of columns a live data file holds (`file_shapes`)."""
    current = metadata.get("current-snapshot-id")
    snapshots = {snapshot["snapshot-id"]: snapshot for snapshot in metadata.get("snapshots", [])}
    # `snapshots[-1]` is not the current snapshot; only current-snapshot-id names it.
    rows = int(snapshots[current]["summary"].get("total-records", "0")) if current in snapshots else 0
    schemas = sorted(metadata["schemas"], key=lambda schema: schema["schema-id"])
    fields: dict[int, list] = {}
    for schema in schemas:
        for field in schema["fields"]:
            entry = [field["id"], field["name"], logical_type(field["type"]), not field["required"]]
            if fields.setdefault(field["id"], entry) != entry:
                # A renamed or retyped field: name it rather than record a shape that never existed.
                raise ValueError(f"field {field['id']} changed from {fields[field['id']]} to {entry}")
    by_id = {schema["schema-id"]: schema for schema in schemas}
    columns = [field["id"] for field in by_id[metadata["current-schema-id"]]["fields"]]
    # The live files, not the schemas: a backfill that rewrote every file leaves no file of the
    # schema before it, and a schema no file was written under is no shape a reader meets.
    # A file records sizes by leaf: a list column by its element's id. Name each by its column.
    column_of = {field["id"]: field["id"] for schema in schemas for field in schema["fields"]}
    for schema in schemas:
        for field in schema["fields"]:
            column_of.update(dict.fromkeys(nested_ids(field["type"]), field["id"]))
    unknown = sorted({leaf for ids in file_shapes for leaf in ids} - set(column_of))
    if unknown:
        raise ValueError(f"live data files hold field ids no schema names: {unknown}")
    shapes = {tuple(sorted({column_of[leaf] for leaf in ids})) for ids in file_shapes}
    earlier = sorted(list(ids) for ids in shapes if list(ids) != sorted(columns))
    return {"has_rows": rows > 0, "fields": sorted(fields.values()),
            "columns": columns, "earlier": earlier}


def trino_rows(url: str, sql: str) -> list[list]:
    """One statement over Trino's client protocol (POST /v1/statement, then follow nextUri)."""
    headers = {"X-Trino-User": "lakehouse-production-shape", "User-Agent": "foundation-lakehouse-production-shape"}
    request = urllib.request.Request(f"{url}/v1/statement", data=sql.encode(), headers=headers, method="POST")
    rows: list[list] = []
    while request is not None:
        with urllib.request.urlopen(request, timeout=120) as response:
            page = json.load(response)
        if "error" in page:
            raise RuntimeError(f"trino: {page['error'].get('message')}")
        rows += page.get("data", [])
        request = urllib.request.Request(page["nextUri"], headers=headers) if page.get("nextUri") else None
    return rows


def live_file_shapes(trino_url: str, table_name: str) -> list[list[int]]:
    """The distinct sets of field ids the table's live data files hold: a data file's manifest entry
    records a size for every column the file was written with."""
    namespace, name = table_name.split(".", 1)
    rows = trino_rows(trino_url, f'SELECT array_sort(map_keys(column_sizes)) FROM r2.{namespace}."{name}$files" '
                                 "WHERE content = 0 GROUP BY 1")
    if any(row[0] is None for row in rows):
        raise ValueError(f"{table_name}: a live data file records no column sizes")
    return [row[0] for row in rows]


def render(ledger: dict) -> str:
    """JSON with one line per field and per column list, so a refresh diffs by the field it changed."""
    def line(value: object) -> str:
        return json.dumps(value, ensure_ascii=False)

    def block(indent: str, items: list[str], close: str) -> str:
        return "\n".join(items).join(["[\n", f"\n{indent}{close}"]) if items else "[]"

    out = ["{"] + [f" {line(key)}: {line(value)}," for key, value in ledger.items() if key != "tables"]
    tables = []
    for name, table in ledger["tables"].items():
        body = [
            f"   \"has_rows\": {line(table['has_rows'])}",
            "   \"fields\": " + block("   ", [f"    {line(field)}," for field in table["fields"]], "]"),
            f"   \"columns\": {line(table['columns'])}",
            "   \"earlier\": " + block("   ", [f"    {line(ids)}," for ids in table["earlier"]], "]"),
        ]
        tables.append(f"  {line(name)}: {{\n" + ",\n".join(body) + "\n  }")
    rendered = "\n".join(out + [" \"tables\": {", ",\n".join(tables), " }", "}"]) + "\n"
    # Drop the trailing comma each list item carries; JSON allows none before a closing bracket.
    rendered = rendered.replace(",\n   ]", "\n   ]")
    if json.loads(rendered) != ledger:
        raise AssertionError("the rendered ledger does not read back as the captured one")
    return rendered


def capture() -> dict:
    """The ledger of the production lakehouse now, from the catalog settings in the environment."""
    uri = os.environ["FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI"].rstrip("/")
    warehouse = os.environ["FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE"]
    token = os.environ["FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_READER_TOKEN"]
    trino_url = os.environ.get("FOUNDATION_PLATFORM_TRINO_URL", "http://127.0.0.1:18081").rstrip("/")

    def get(url: str) -> dict:
        request = urllib.request.Request(url, headers={
            # Cloudflare refuses urllib's default User-Agent with 403.
            "Authorization": f"Bearer {token}", "User-Agent": "foundation-lakehouse-production-shape",
        })
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response)

    # The catalog's path prefix comes from /v1/config; without it every table reads as absent.
    prefix = get(f"{uri}/v1/config?warehouse={urllib.parse.quote(warehouse)}")["overrides"]["prefix"]
    contracts = json.loads(CONTRACTS.read_text(encoding="utf-8"))["contracts"]
    # Every table of every namespace a contract names, not only the contract tables: a pull request
    # that adds a contract for a table production already holds is gated by that table's shape.
    tables = {}
    for namespace in sorted({table_name.split(".", 1)[0] for table_name in contracts}):
        names, page = [], ""
        while True:
            listing = get(f"{uri}/v1/{prefix}/namespaces/{namespace}/tables" + (f"?pageToken={page}" if page else ""))
            names += [identifier["name"] for identifier in listing["identifiers"]]
            page = listing.get("next-page-token") or ""
            if not page:
                break
        for name in sorted(names):
            metadata = get(f"{uri}/v1/{prefix}/namespaces/{namespace}/tables/{name}")["metadata"]
            table_name = f"{namespace}.{name}"
            # A table no write has committed to has no snapshot, so no file (Trino refuses its $files).
            written = metadata.get("current-snapshot-id") not in (None, -1)
            files = live_file_shapes(trino_url, table_name) if written else []
            tables[table_name] = table_shape(metadata, files)
    return {
        "schema_version": SCHEMA_VERSION,
        "decision": "docs/adr/0259-ci-migrates-the-production-table-shape.md",
        "captured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "captured_by": "platforms/foundation-platform/scripts/ops/lakehouse-production-shape.py",
        "tables": tables,
    }


def main() -> int:
    # --output <path>: write elsewhere than the tree's ledger (the host's check writes to its state).
    output = Path(sys.argv[2]) if sys.argv[1:2] == ["--output"] else LEDGER
    ledger = capture()
    output.write_text(render(ledger), encoding="utf-8", newline="\n")
    print(f"lakehouse-production-shape: {len(ledger['tables'])} tables written to {output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
