# ADR 0031 - Historical Data And Staff Boundary

| Field | Value |
|---|---|
| Date | 2026-05-11 |
| Status | Superseded by [ADR 0048](./0048-horizontal-platform-redefinition.md) |
| Related | [ADR 0049](./0049-identity-platform-contract-design.md) |

## Historical Decision

The original shared implementation separated canonical Catalog data from staff
identity and authorization. That separation proved the ownership boundary, but
the original decision still placed both capabilities inside one deployable.

ADR 0048 supersedes that topology with two horizontal platforms:

- Foundation Platform owns Catalog, collection, lakehouse, lineage, and
  canonical public/reference data.
- Identity Platform owns staff identity, service identity, authentication, and
  authorization policy.

## Preserved Invariants

- Cross-platform direct database access is forbidden.
- Foundation stores only stable identity-principal references needed for audit;
  it does not own staff account lifecycle or authorization policy.
- Identity does not own Catalog entities or canonical data writes.
- Gongzzang product users and product sessions remain Gongzzang-owned.
- Integration uses published APIs, events, signed claims, or immutable
  artifacts.

## Current Sources Of Truth

- [ADR 0048](./0048-horizontal-platform-redefinition.md) defines platform
  ownership.
- [ADR 0049](./0049-identity-platform-contract-design.md) defines the Identity
  contract boundary.
- `docs/architecture/foundation-platform-boundary.v1.json` defines the
  machine-readable Gongzzang/Foundation boundary.

This file is retained only to preserve the decision lineage. It is not an
implementation guide.
