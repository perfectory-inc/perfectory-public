# ADR 0036 - Static Vector Tile Runtime Contract
| Field | Value |
|---|---|
| Date | 2026-05-12 |
| Status | Accepted |
| Owner | Foundation Platform |
| Consumer | Gongzzang |
| Upstream SSOT | `../../../../platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md` |

## Decision

Foundation Platform owns public and reference vector-tile acquisition, build,
storage, lineage, publication, and rollback. Gongzzang only consumes the
published contract and has no vector-tile ETL or R2 write path.

The browser resolves the manifest from exactly one of these locations:

1. `NEXT_PUBLIC_TILES_MANIFEST_URL` for an explicitly configured public manifest.
2. `NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL/catalog/v1/vector-tiles/manifest`
   otherwise.

The `/v1` segment versions the HTTP compatibility contract. It is not a lakehouse
data version and does not appear in R2 artifact paths.

## Contract

```json
{
  "schema_version": 1,
  "current_version": "0196e7e0-3c20-7000-8000-000000000042",
  "previous_version": "0196e7e0-3c20-7000-8000-000000000041",
  "tiles_url_template": "https://static.example.com/{object_key_prefix}/{z}/{x}/{y}.pbf",
  "published_at": "2026-05-12T00:00:00Z",
  "artifacts": {
    "parcel_anchor": {
      "source_layer": "parcel_anchor",
      "tile_min_zoom": 12,
      "tile_max_zoom": 12,
      "render_min_zoom": 12,
      "render_max_zoom": 22,
      "tilejson_object_key": "gold/vector-tiles/artifacts/0196e7e0-3c20-7000-8000-000000000042/parcel_anchor.json",
      "object_key_prefix": "gold/vector-tiles/artifacts/0196e7e0-3c20-7000-8000-000000000042/parcel_anchor/",
      "flat_tile_count": 2119,
      "flat_tile_total_bytes": 2318455415,
      "lineage": {
        "source_record_id": "0196e7e0-3c20-7000-8000-000000000101",
        "manifest_file_asset_id": "0196e7e0-3c20-7000-8000-000000000102",
        "tilejson_file_asset_id": "0196e7e0-3c20-7000-8000-000000000103",
        "source_file_asset_ids": ["0196e7e0-3c20-7000-8000-000000000104"]
      }
    }
  }
}
```

The fields have separate jobs:

- `schema_version` is wire-contract metadata.
- `current_version` and `previous_version` are immutable artifact identifiers
  stored as metadata; they are not R2 paths.
- `tilejson_object_key` and `object_key_prefix` are physical R2 addresses.
- checksums, lineage, timestamps, active state, and rollback history remain
  Foundation Catalog metadata.

Gongzzang must not derive data dates, schema versions, lineage, or active state
by parsing an object key.

## Runtime Rules

- Validate `current_version` and `previous_version` as UUIDs.
- Materialize tile URLs only from `tiles_url_template` and
  `artifacts[layer].object_key_prefix`.
- Treat an absent required layer as a typed runtime error.
- Never synthesize missing lineage or artifact metadata.
- Never write, promote, or roll back Foundation artifacts.
- Keep API/event contract versions such as `/catalog/v1` and `.published.v1`.
- Reject semantic data versions such as `gold/v1/`, `version=...`, or
  `manifest.v2.json` in physical artifact paths.

## Rejected Options

### Gongzzang-owned vector-tile ETL

Rejected because it creates a second owner for shared spatial facts and breaks
the Foundation Platform boundary.

### Deriving active state from an R2 directory

Rejected because R2 is object storage, not the Catalog SSOT. The active pointer,
lineage, and rollback state belong to Foundation Catalog metadata.

### Naver internal tiles as canonical data

Rejected because they are a map SDK implementation detail and do not provide
Gongzzang-owned PNU identity or Foundation lineage guarantees.

## Verification

- `apps/web/tests/unit/map/vector-tile-manifest.test.ts`
- `apps/web/tests/unit/foundation-platform-event-contract.test.ts`
- `docs/architecture/foundation-platform-boundary.v1.json`
