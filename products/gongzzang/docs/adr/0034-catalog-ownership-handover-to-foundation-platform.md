# ADR 0034 - Catalog Ownership Handover To Foundation Platform

| Field | Value |
|---|---|
| Date | 2026-05-11 |
| Status | Completed; ownership reaffirmed by [ADR 0048](./0048-horizontal-platform-redefinition.md) |
| Boundary SSOT | `docs/architecture/foundation-platform-boundary.v1.json` |

## Decision

Foundation Platform is the sole owner of canonical industrial-complex, parcel,
building, manufacturer, public/reference spatial, collection, and lakehouse
data. Gongzzang consumes those facts only through published Foundation APIs,
events, and immutable artifacts.

## Completed Extraction

The following implementation categories are absent from the Gongzzang runtime
workspace:

- canonical Catalog domain crates;
- V-World and data.go.kr source clients;
- raw capture and collection-control runtimes;
- public/reference vector-tile ETL;
- Catalog API-drift monitoring;
- Foundation-owned collection and raw-data database tables.

Gongzzang permanently owns listings, listing media, auctions, product users,
product search, and product-facing marker semantics. It may keep local read
models derived from Foundation artifacts, but those read models are not
canonical coordinate or Catalog sources.

## Fresh-Schema Rule

This project has not launched. The Gongzzang migration chain therefore creates
only the final product-owned schema. It does not create retired Foundation-owned
tables and then drop or rename them through compatibility migrations.

## Enforcement

- `docs/architecture/foundation-platform-boundary.v1.json` records ownership and
  forbidden dependencies.
- `scripts/lefthook/foundation-ownership-boundary.sh` rejects reintroduction of
  Foundation internals into active Gongzzang code.
- `tests/migrations/test_v001_full.sh` proves that a fresh database contains the
  product tables and excludes Foundation-owned tables.

## Current Cross-Repo Sources

- [ADR 0048](./0048-horizontal-platform-redefinition.md)
- `../../../../platforms/foundation-platform/docs/adr/0021-adopt-horizontal-platform-redefinition.md`
