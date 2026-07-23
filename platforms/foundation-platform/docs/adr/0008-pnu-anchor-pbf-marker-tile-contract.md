# ADR 0008 - PNU Anchor PBF Marker Tile Contract

| Field | Value |
|---|---|
| Date | 2026-05-22 |
| Status | Accepted |
| Related | [`gongzzang ADR 0037`](../../../../products/gongzzang/docs/adr/0037-pnu-anchor-pbf-marker-tiles.md) |
| Scope | `foundation-platform` Catalog, parcel marker anchors, map marker tile serving, `gongzzang` map runtime |

## Context

Gongzzang map runtime needs to render many parcel-attached map features such as listings,
real transaction prices, official land prices, auctions, and future parcel indicators. The runtime
must not depend on ad-hoc bounding-box JSON endpoints because that path creates the same failure
mode repeatedly:

- a wide viewport can request too much data at once;
- `ORDER BY`, deduplication, and count queries can happen before a safe tile boundary;
- marker records may be silently truncated by a per-request limit;
- marker coordinates can drift away from the parcel identity if coordinates are stored per product;
- the frontend must reconcile too many object-specific API shapes.

The platform architecture already decided two relevant rules:

- static parcel and administrative geometry is served as flat `.pbf` vector tiles through the
  foundation-platform-owned manifest contract in ADR 0004;
- parcel-attached business identity is PNU-first in Gongzzang ADR 0018.

This ADR extends those rules to marker positions and marker tile responses.

This ADR does not transfer Gongzzang listing ownership into foundation-platform. Listings are Gongzzang
market-domain product data. foundation-platform owns parcel anchors and public/reference spatial layers;
product services own product semantics and may serve product marker PBF tiles using the same
PNU-anchor contract.

## Decision

All launch map marker traffic for parcel-attached objects must use **PNU anchor backed PBF vector
tiles**.

Contract constants:

```text
marker_tile_response_format = MVT_PBF
marker_position_source = PNU_ANCHOR
bbox_marker_runtime_forbidden = true
dropped_marker_success_forbidden = true
launch_runtime_source = R2_CDN_VECTOR_TILE_MANIFEST
runtime_manifest_endpoint = /catalog/v1/vector-tiles/manifest
db_reference_endpoint_launch_forbidden = true
db_reference_endpoint_scope = diagnostics_bounded_proof_admin
aggregate_anchor_max_zoom = 11
exact_anchor_min_zoom = 12
```

PBF is the serving projection. It is not the source of truth for location.

For foundation-platform-owned static/reference marker layers, the launch hot path is the active vector
tile manifest consumed from `/catalog/v1/vector-tiles/manifest`, then R2/CDN tile artifacts
materialized from that manifest. The database-backed `/map/v1/marker-tiles/...` endpoint is a
reference path for diagnostics, bounded regional proof, and admin verification; it must not be used
as the production launch runtime for national traffic.

Low zooms must not repeat every individual PNU anchor. Static/reference parcel anchors use aggregate
artifacts through z11 and exact PNU anchor artifacts from z12 upward.

The source of truth for marker position is a foundation-platform Catalog anchor derived from parcel
geometry and identified by PNU. A product such as Gongzzang may decide what a marker means, how it
is styled, and what details panel opens, but it must not own the canonical parcel marker position.

## Anchor Registry

foundation-platform Catalog owns a logical `parcel_marker_anchor` registry.

Minimum anchor fields:

| Field | Meaning |
|---|---|
| `pnu` | Parcel identity. This is the primary lookup key. |
| `anchor_lng` | Longitude in EPSG:4326. |
| `anchor_lat` | Latitude in EPSG:4326. |
| `algorithm` | Anchor algorithm name. `official_label_point` is preferred when a source provides it; otherwise `polylabel`. |
| `algorithm_version` | Stable version string for reproducible anchor generation. |
| `source_geometry_version` | Parcel geometry build/version that produced the anchor. |
| `source_geometry_checksum_sha256` | Checksum or build checksum for the source geometry input. |
| `computed_at_utc` | UTC timestamp of anchor computation. |

The storage name may change during implementation, but those semantics must remain intact.

The anchor must be inside the parcel polygon when source geometry permits it. If a parcel geometry is
invalid or missing, foundation-platform must emit an explicit lineage/error state instead of inventing a
coordinate.

## Marker Tile Contract

Marker tiles are addressed by tile coordinate and filter identity, not by arbitrary viewport bounds.

Recommended public read shape:

```text
GET /map/v1/marker-tiles/{layer}/{z}/{x}/{y}.pbf?filter_hash={hash}
```

`layer` is a stable marker layer name such as `parcel_anchor`, `real_transaction_price`,
`official_land_price`, `auction`, or product-owned layers such as Gongzzang `listing`.
`filter_hash` is the identity of a validated filter contract, not a free-form SQL expression.

foundation-platform serves layers it owns, such as `parcel_anchor` and public/reference data layers.
Gongzzang listing marker tiles are served by Gongzzang unless a later ADR explicitly creates a
neutral projection boundary that does not store or interpret listing business semantics.

The PBF tile contains point features whose geometry is the resolved PNU anchor. Each feature must
contain only the minimum rendering and lookup properties required for the map:

| Property | Meaning |
|---|---|
| `id` | Product-owned object id or aggregate id. |
| `pnu` | Parcel identity used to resolve the anchor. |
| `kind` | Stable marker kind for style selection. |
| `count` | Aggregate count when the feature represents multiple objects. |
| `rank` | Optional deterministic display rank for label collision. |
| `detail_ref` | Opaque lookup reference for detail API fetch. |

Large labels and rich marker cards are presentation. They are not data completeness. If visual space
is insufficient, the renderer must degrade labels to small dots or aggregate symbols. A successful
tile response must not silently drop eligible records just because there is no visual room.

## Completeness Rule

Tile responses may aggregate, but they must not lie.

Allowed:

- point feature for every eligible record;
- deterministic aggregation where `count` and `detail_ref` preserve drill-down;
- zoom-dependent simplification only when the simplified feature represents the full underlying set;
- separate detail fetch by `id`, `pnu`, or `detail_ref`.

Forbidden:

- `LIMIT N` as a success-path data cap for a tile without an explicit "truncated" failure state;
- dropping lower-ranked markers and returning HTTP 200 as if the tile is complete;
- deriving marker coordinates from product-owned `latitude`/`longitude` columns for parcel-attached
  objects;
- public launch map marker requests based on `bbox`, `bounds`, `south/west/north/east`, or raw
  coordinate envelopes.

If a tile cannot be represented within configured budgets, the service must return a structured
budget error or an aggregate that truthfully represents the underlying records.

## Relationship To Static Parcel Tiles

Static parcel polygons and dynamic marker points are separate tile layers with one location model.

- Parcel polygon PBF: immutable or slowly changing static geometry layer from ADR 0004.
- Marker point PBF: dynamic or semi-static marker layer generated by the owning service from
  business data joined to foundation-platform PNU anchors.

Both layers use PNU as the join key. The marker point PBF must not duplicate the parcel polygon as
its own location source.

## JSON Use

JSON marker endpoints are allowed only for admin diagnostics, contract tests, and detail fetches.
They are not the launch map marker rendering path.

The launch map may fetch details as JSON after a user selects a feature, but the map-wide marker
surface is PBF/MVT.

## Consequences

Positive:

- viewport size no longer controls backend result size directly;
- marker position has a single owner and is reproducible from parcel geometry lineage;
- Gongzzang, Dawneer, and future products can share the same anchor semantics;
- map rendering can degrade from labels to dots without data loss;
- the API contract aligns with CDN/cache-friendly tile addressing.

Cost:

- foundation-platform must own an anchor generation and lineage pipeline;
- product marker data must join through PNU before tile encoding, without foundation-platform owning
  product semantics such as listing price, status, exposure, search filters, or detail payloads;
- filter hashing and tile budget errors need a strict contract before production exposure;
- current bbox JSON map paths in Gongzzang become transitional and must not be treated as launch
  architecture.

## Implementation Sequence

1. Define the anchor registry schema and anchor generation algorithm contract.
2. Build anchor generation from parcel geometry with checksum and version lineage.
3. Define the marker tile response schema and filter hash contract.
4. Add one low-risk marker layer first, preferably read-only real transaction or official land
   price points.
5. Move Gongzzang listing markers from bbox JSON to Gongzzang-owned PBF marker tiles that consume
   foundation-platform anchors by PNU.
6. Add CI guardrails that reject new launch map marker paths using bbox/bounds.
7. Deprecate and remove legacy listing coordinate marker paths after the PBF runtime is verified.

DB migrations for the anchor registry require explicit migration approval before generation.

## Revisit Triggers

- parcel geometry source changes enough to require a new anchor algorithm version;
- a product needs non-parcel-attached freehand coordinates as a first-class business object;
- a marker layer cannot meet tile budget without truthful aggregation;
- another service attempts to own parcel marker coordinates outside foundation-platform.

## References

- [ADR 0004 - Static Vector Tile Runtime Contract](./0004-static-vector-tile-runtime-contract.md)
- [ADR 0006 - Lakehouse Table Format and Serving Architecture](./0006-lakehouse-table-format-and-serving-architecture.md)
- [Gongzzang ADR 0018 - PNU-first identity](../../../../products/gongzzang/docs/adr/0018-pnu-first-identity-no-coordinates.md)
- [Gongzzang ADR 0036 - Static vector tile runtime contract](../../../../products/gongzzang/docs/adr/0036-static-vector-tile-runtime-contract.md)
