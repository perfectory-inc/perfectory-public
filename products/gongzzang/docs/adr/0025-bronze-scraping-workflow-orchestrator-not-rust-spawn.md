# ADR 0025 - Bronze Producer Isolation

| Field | Value |
|---|---|
| Date | 2026-05-08 |
| Status | Superseded operationally by [ADR 0034](./0034-catalog-ownership-handover-to-foundation-platform.md) and [ADR 0048](./0048-horizontal-platform-redefinition.md) |
| Amends | [ADR 0022](./0022-bronze-scraping-isolated-python-service.md) |

## Decision

The durable decision is an isolation boundary, not a particular CI workflow:

- a product runtime must not spawn or import a provider-specific acquisition
  runtime;
- heterogeneous acquisition and transformation stages exchange immutable
  objects and explicit manifests;
- retry, resource sizing, and failure ownership are isolated by stage;
- a promotion step may consume only artifacts that passed the producer's
  validation contract.

The historical Gongzzang-owned Bronze workflow and its implementation paths are
no longer current. Foundation Platform now owns public-data acquisition,
Bronze/Silver/Gold processing, and public/reference vector-tile publication.
Gongzzang consumes only its published HTTP, event, and immutable-artifact
contracts.

## Consequences

- Gongzzang code must not regain provider scrapers or subprocess adapters for
  Foundation-owned acquisition.
- Foundation Platform may choose its own orchestrator, provided the stage and
  artifact boundaries above remain explicit.
- Workflow filenames and runner layouts are deployment details, so they are not
  architecture contracts and are intentionally absent from this ADR.

## Enforcement

- [ADR 0034](./0034-catalog-ownership-handover-to-foundation-platform.md)
  defines the current owner boundary.
- `docs/architecture/foundation-platform-boundary.v1.json` and the repository
  boundary guard prevent Gongzzang from reintroducing Foundation internals.
