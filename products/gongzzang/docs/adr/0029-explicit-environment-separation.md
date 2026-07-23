# ADR 0029 - Explicit Environment And Secret Separation

| Field | Value |
|---|---|
| Date | 2026-05-11 |
| Status | Accepted invariant; legacy compatibility removed by [ADR 0035](./0035-legacy-r2-removal-and-atomic-namespace.md), Foundation-owned ETL implementation moved by [ADR 0034](./0034-catalog-ownership-handover-to-foundation-platform.md) |

## Decision

Any process that can mutate external state must receive an explicit, typed
environment. It must fail closed when the environment is absent or invalid.
Credentials are scoped to that environment and are loaded atomically: a
partial credential set is an error, and credentials from another environment
must never be used as a fallback.

This is an ownership-independent safety invariant. Each platform defines its
own environment type and secret namespace; it must not infer the target from
which credentials happen to be present.

## Gongzzang Boundary

Gongzzang no longer owns the public-data ETL or its object-storage credentials.
The Gongzzang environment example therefore exposes only product-owned settings
and published Foundation integration contracts. Generic ETL and raw-data R2
credentials are forbidden at this boundary.

## Consequences

- Local, staging, and production mutations cannot share an ambiguous secret
  namespace.
- Compatibility aliases that weaken the invariant are prohibited; ADR 0035
  records their removal.
- Concrete workflow names and secret values remain deployment details outside
  this public architecture contract.

## Enforcement

- `docs/architecture/foundation-platform-boundary.v1.json` defines the allowed
  Gongzzang environment surface.
- Environment parsers and configuration tests must reject missing, invalid, or
  partial mutation configuration.
- Secret scanners and repository policy prevent credentials from becoming
  source-controlled defaults.
