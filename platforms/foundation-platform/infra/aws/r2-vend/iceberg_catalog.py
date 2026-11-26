"""The current snapshot and the location of one Iceberg table, from R2 Data Catalog's REST API (the
protocol crates/lakehouse/lakehouse-infrastructure/src/iceberg_rest_catalog.rs speaks: the config's
prefix, then `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}`).

Only the vending function holds the catalog token; it resolves a lane's tables here and returns their
snapshots, so no run asks the catalog (the `vended` provider, root ADR-0616 §2-8). Errors name the HTTP
status, never the token.
"""

import json
import urllib.error
import urllib.parse
import urllib.request


class CatalogError(RuntimeError):
    """The catalog refused, or answered in a shape this module does not know."""


def http(token, url):
    """(HTTP status, JSON answer) of one GET."""
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, {}


def catalog_uri(account, bucket):
    """R2 Data Catalog's documented address for a bucket, and its warehouse."""
    return f"https://catalog.cloudflarestorage.com/{account}/{bucket}", f"{account}_{bucket}"


def prefix(token, uri, warehouse, get=http):
    status, answer = get(token, f"{uri}/v1/config?{urllib.parse.urlencode({'warehouse': warehouse})}")
    if status != 200:
        raise CatalogError(f"the catalog's config answered HTTP {status}")
    for section in ("overrides", "defaults"):
        value = (answer.get(section) or {}).get("prefix")
        if value:
            return value
    return warehouse


def current(token, account, bucket, table_name, get=http, catalog_prefix=None):
    """{location, snapshot}: the table's storage location (None when the table does not exist) and its
    current snapshot as {snapshot_id, snapshot_timestamp_ms, manifest_list_location, metadata_location}
    (None when it has none)."""
    namespace, _, table = table_name.rpartition(".")
    if not namespace or not table:
        raise CatalogError(f"{table_name!r} is not <namespace>.<table>")
    uri, warehouse = catalog_uri(account, bucket)
    catalog_prefix = catalog_prefix or prefix(token, uri, warehouse, get)
    path = "/".join(urllib.parse.quote(part, safe="") for part in
                    (catalog_prefix, "namespaces", "\x1f".join(namespace.split(".")), "tables", table))
    status, answer = get(token, f"{uri}/v1/{path}")
    if status == 404:
        return {"location": None, "snapshot": None}
    if status != 200:
        raise CatalogError(f"the catalog answered HTTP {status} for {table_name}")
    metadata = answer.get("metadata") or {}
    location = metadata.get("location")
    if not location or not answer.get("metadata-location"):
        raise CatalogError(f"the catalog gave no location or metadata location for {table_name}")
    current_id = metadata.get("current-snapshot-id")
    current_id = int(current_id) if current_id not in (None, "", -1, "-1") else None
    if current_id is None:
        return {"location": location, "snapshot": None}
    snapshot = next((s for s in metadata.get("snapshots") or [] if int(s.get("snapshot-id", -1)) == current_id), None)
    if not snapshot or not snapshot.get("manifest-list") or snapshot.get("timestamp-ms") is None:
        raise CatalogError(f"the catalog omitted current snapshot {current_id} of {table_name} or its manifest list")
    return {"location": location, "snapshot": {
        "snapshot_id": current_id,
        "snapshot_timestamp_ms": int(snapshot["timestamp-ms"]),
        "manifest_list_location": snapshot["manifest-list"],
        "metadata_location": answer.get("metadata-location"),
    }}
