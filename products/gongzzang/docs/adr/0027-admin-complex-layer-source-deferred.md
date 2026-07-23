# ADR 0027 - No Substitute Data For Static Map Layers

| Field | Value |
|---|---|
| Date | 2026-05-08 |
| Status | Superseded operationally by [ADR 0034](./0034-catalog-ownership-handover-to-foundation-platform.md), [ADR 0036](./0036-static-vector-tile-runtime-contract.md), and [ADR 0048](./0048-horizontal-platform-redefinition.md) |

## Decision

A static map layer must remain unpublished until its owner can provide a
source-specific dataset, schema, validation policy, and manifest entry. Data
from another layer must never be relabeled to make an incomplete layer appear
available.

The old Gongzzang ETL activation switch is no longer current. Foundation
Platform owns parcel, administrative, industrial-complex, building, and other
public/reference spatial layers. Its published vector-tile manifest is the
only runtime statement that a layer is available.

## Runtime Contract

- Gongzzang registers only layers present in the validated manifest contract.
- Each manifest layer retains its own `source_layer`, zoom range, and artifact
  identity.
- Missing layers are unavailable; they are not synthesized from parcel data or
  enabled by a product-side deployment switch.

## Consequences

- Adding an administrative, industrial-complex, or building layer is a
  Foundation publication change followed by contract consumption in
  Gongzzang.
- Workflow filenames and former ETL crate names are historical implementation
  details and are not part of this decision.

## Enforcement

- [ADR 0036](./0036-static-vector-tile-runtime-contract.md) owns the consumer
  manifest contract.
- `docs/architecture/foundation-platform-boundary.v1.json` records the owner
  boundary and forbidden product-side implementations.
