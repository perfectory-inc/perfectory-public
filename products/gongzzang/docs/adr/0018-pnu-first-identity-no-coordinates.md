# ADR-0018: Listing Identity Is PNU-First

| | |
|---|---|
| Date | 2026-05-06 |
| Status | Accepted, hardened 2026-05-22 |
| Decision Owner | Product/Engineering |
| Context | ADR 0016 base-layer tiles, ADR 0037 PNU-anchor PBF marker tiles |

## Context

Gongzzang listings are parcel-attached objects. A listing belongs to a parcel identified by PNU.
Coordinates from GPS, geocoding, or user clicks are estimates and can disagree with cadastral data.

For a high-integrity industrial real-estate platform, listing location must not have two owners.

## Decision

`listing.parcel_pnu` is the listing location identity. Listing rows do not own a product coordinate.

Map marker placement is resolved through foundation-platform parcel marker anchors and PBF marker tiles.
Listing card/detail APIs may expose business data, but not a separate marker coordinate.

## Current Code State

The legacy product coordinate path has been removed from launch listing flows:

- `Listing` aggregate no longer has a product coordinate field.
- `POST /listings`, `PATCH /listings/:id`, and `GET /listings/:id` no longer accept or expose a product coordinate.
- `PgListingRepository` no longer reads/writes listing product coordinates.
- `migrations/10001_core_tables.sql` no longer creates a listing product coordinate column or index.
- PNU-anchor marker contract guardrails reject reintroducing listing-card coordinates, viewport bounds marker queries, or product-coordinate storage paths.

The launch schema is clean from the baseline migration because Gongzzang has not launched and does
not need backward-compatible local schema history for this coordinate path.

## Consequences

Positive:

- One location owner: foundation-platform catalog anchors.
- No conflict between listing rows and parcel geometry.
- Listing search/card APIs stay business-data focused.
- Marker rendering can scale through PBF tiles without dropped eligible records.

Tradeoffs:

- Building-level or arbitrary-point products need a separate ADR and a different identity model.
- Existing local development databases created before this ADR hardening must be recreated from the
  migration chain.

## Reconsideration Triggers

- Gongzzang adds a non-parcel-attached product that truly needs arbitrary coordinates.
- Foundation Platform cannot provide marker anchors for a required parcel class.
- Building-footprint-level placement becomes a launch requirement.

## References

- [ADR 0037: PNU Anchor PBF Marker Tiles](./0037-pnu-anchor-pbf-marker-tiles.md)
- [Foundation Platform ADR 0008](../../../../platforms/foundation-platform/docs/adr/0008-pnu-anchor-pbf-marker-tile-contract.md)
- [Core baseline migration](../../migrations/20260719000102_core_tables.sql)
