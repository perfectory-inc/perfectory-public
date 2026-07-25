# ADR 0036 - Foundation Vector Tile Runtime Contract

| Field | Value |
|---|---|
| Date | 2026-05-12 |
| Last amended | 2026-07-24 |
| Status | Accepted |
| Owner | Foundation Platform |
| Consumer | Gongzzang |
| Upstream SSOT | [`Foundation ADR 0004`](../../../../platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md) |

## Decision

Foundation Platform owns public/reference vector-tile acquisition, canonical data, build, storage,
lineage, publication, rollback, and the active runtime manifest. Gongzzang validates and consumes
that published contract. It has no Foundation vector-tile ETL, R2 write, promotion, or rollback
path.

The browser resolves the schema-v2 runtime manifest from
`NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL/catalog/v1/vector-tiles/runtime-manifest`. The existing
`NEXT_PUBLIC_TILES_MANIFEST_URL` keeps its schema-v1 meaning and is never repurposed for v2.

During the bounded first migration slice, the existing schema-v1 manifest location remains a
separate input for only `parcel_anchor_aggregate` and `parcel_anchor`: it resolves through
`NEXT_PUBLIC_TILES_MANIFEST_URL` when explicitly configured, otherwise through
`NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL/catalog/v1/vector-tiles/manifest`. The v2 endpoint
supplies `parcels`. Gongzzang must ignore the v1 `parcels` artifact once v2 `parcels` is active, so
this temporary dual-manifest read never creates two sources for one publication unit.

The `/v1` segment versions the HTTP compatibility contract. It is not an Iceberg revision,
manifest schema version, or R2 path component.

For each Foundation publication unit and serving generation, the browser registers exactly one
complete Martin vector source:

```text
DynamicPostgis XOR StaticPmtiles
```

It never combines a static base with a dynamic overlay, tombstones, or client-side feature
suppression for the same unit. The Gongzzang-owned listing marker and `filter_hash`/delta paths are
separate and unchanged.

## Exact schema dispatch

`schema_version` is an exact discriminator:

- `1` selects the legacy flat-object DTO.
- `2` selects the single-source publication DTO.
- Every other value is rejected.

The consumer must not use a permissive `schema_version >= 1` parser or silently treat v2 fields as
v1 fields.

## Legacy v1

V1 remains readable during a bounded migration. Its bytes and meanings do not change:

- `current_version` and `previous_version` are UUID metadata.
- `tiles_url_template` contains `{object_key_prefix}`, `{z}`, `{x}`, and `{y}`.
- `artifacts[layer].object_key_prefix` is a physical flat-MVT object prefix.
- `flat_tile_count` and `flat_tile_total_bytes` describe real individual tile objects.
- `source_layer`, zooms, UUID lineage, and non-empty tile statistics are required.

V1 fields are never reused for a PMTiles object or Martin route. New v1 production publication stops
after the v2 producer/consumer cutover. During the first v2 slice, the frozen v1 document still
contains its parcel artifact plus the two anchor artifacts byte-for-byte; the v2-aware runtime
registers only the two anchor sources from that document and does not register its parcel artifact.

## V2 contract

V2 has top-level:

```text
schema_version = 2
current_version                 # immutable manifest UUID / ETag identity
manifest_generation            # global JavaScript-safe poll token only
refresh_after_seconds
published_at
publication_units
```

Every publication unit has:

```text
data_revision                   # UUID for the exact logical feature set
serving_generation              # JavaScript-safe integer
active_release_id               # immutable release UUID
canonical_iceberg_snapshot_id   # positive base-10 decimal string, never JSON number
source                          # closed tagged union
layers                          # non-empty MVT layer metadata
lineage
```

Real Iceberg snapshot IDs can exceed JavaScript's safe integer range. The consumer therefore
accepts `canonical_iceberg_snapshot_id` only as a positive decimal string.
`manifest_generation` and every `serving_generation` must be in
`1..=9007199254740991`.
`refresh_after_seconds` must equal `4`.

The `source` union has exactly two variants:

- `dynamic_postgis`: stable explicit `martin_source_id`, generation-addressed
  `tiles_url_template`, `postgis_projection_revision`, and `cache_policy`.
- `static_pmtiles`: immutable release-addressed `martin_source_id` and `tiles_url_template`,
  plus PMTiles object key, file-asset UUID, SHA-256, and byte size.

Each unit's URL is already a complete Martin template containing `{z}`, `{x}`, and `{y}`. Gongzzang
does not append an extension or perform object-key substitution for v2. Every layer declares stable
`source_layer`, tile/render zooms, and canonical
lowercase `feature_id_property`. The current parcel identity is `pnu`; proof-only uppercase `PNU`
is not a second production identity.

Production v2 tile URLs must be absolute HTTPS. The parser permits absolute HTTP only when the host
is a loopback literal or `localhost`, solely for the checked-in Docker proof; the Foundation
production publish gate rejects every HTTP URL, including loopback.

The first v2 unit is only `parcels`. The current marker runtime still requires both anchor layers,
which remain independent legacy v1 sources during this slice. A parcel source transition must not
replace those sources. `complex`, `parcel_anchor_aggregate`, `parcel_anchor`, `admin`, and
`buildings` migrate to v2 only after their own producer and consumer parity is proven.

## Active-map refresh

While the map is mounted and visible, Gongzzang:

1. polls the Catalog manifest with one non-overlapping `ETag` / `If-None-Match` request every four
   seconds while visible;
2. parses the complete response before applying any change;
3. treats `manifest_generation` only as a signal that some unit may have changed;
4. diffs per-unit `serving_generation`;
5. replaces only changed Mapbox vector sources while preserving their style-layer order,
   interaction handlers, zooms, and feature identity; and
6. keeps the currently registered source descriptor if the new manifest or source is
   invalid/unready.

One unit's old and new sources must never remain registered together. An already-open map must load
the selected generation within the Foundation freshness SLO.

That retention guarantee is exact for immutable static URLs. A dynamic generation value only busts
caches around one stable Martin/PostGIS source; it is not a historical snapshot selector. If the
client rejects a malformed manifest while that projection advances, its retained dynamic URL may
return the latest committed complete geometry. Gongzzang must not present this as rollback.

Each mounted visible map therefore contributes at most `0.25` manifest requests/second. The client
uses a randomized initial phase to avoid a synchronized herd, aborts on hide/unmount, and applies
bounded exponential backoff after errors. The endpoint is an explicit anonymous public contract in
Foundation's traffic/auth registry and Gongzzang's outbound allow policy; no service credential is
placed in browser JavaScript.

## Runtime rules

Gongzzang must:

- validate all UUIDs, decimal snapshot strings, generation ranges, tagged-union fields, zooms, and
  required layer sets;
- materialize the two legacy anchor URLs only with v1 rules, ignore v1 `parcels` when v2 `parcels`
  is active, and use v2 source URLs directly;
- treat v2 `parcels` plus v1 `parcel_anchor_aggregate` and `parcel_anchor` as core for the current
  map during the bounded migration;
- use manifest lineage for diagnostics and support evidence; and
- fetch selected-object details through the owning API rather than treating MVT as canonical data.

Gongzzang must not:

- write, promote, or roll back Foundation artifacts;
- synthesize missing units, sources, layers, lineage, or identity;
- derive active state from R2 object listing or parse business meaning from an object key;
- use `manifest_generation` as a source selector;
- compose static/dynamic Foundation features for one unit; or
- move listing price, status, exposure, `filter_hash`, or marker-delta semantics into this manifest.

## Rejected options

- Gongzzang-owned vector-tile ETL or R2 publication.
- Naver internal tiles as canonical data.
- Permissive schema parsing or semantic reuse of v1 flat-object fields.
- Direct browser PMTiles/custom transport as the production default.
- Static base plus dynamic overlay/tombstone filtering for a Foundation unit.
- Retargeting Foundation parcel-anchor units when only the parcel polygon unit changes.

## Verification

- `apps/web/tests/unit/map/vector-tile-manifest.test.ts`
- `apps/web/tests/unit/map/listing-map-runtime.test.ts`
- `apps/web/tests/unit/foundation-platform-event-contract.test.ts`
- `docs/architecture/foundation-platform-boundary.v1.json`
- `cargo xtask verify gongzzang`

The upstream field-level contract and publication rules remain
[Foundation ADR 0004](../../../../platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md).
