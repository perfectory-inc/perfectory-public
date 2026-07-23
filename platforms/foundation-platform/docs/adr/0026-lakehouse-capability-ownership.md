# ADR 0026: Lakehouse Capability Owns Materialization and Publication

- Status: Accepted
- Date: 2026-07-16
- Supersedes: Lakehouse ownership implied by the Catalog umbrella
- Related: Foundation ADR 0021, Gongzzang ADR 0048

## Context

The Foundation Platform is already the physical owner of Catalog, Collection,
Lakehouse, Normalization, and Spatial capabilities. Collection has been extracted
from the former Catalog umbrella, but Lakehouse behavior is still distributed
through `catalog-domain`, `catalog-application`, `catalog-infrastructure`,
`outbox-publisher`, and the two Foundation services.

This distribution has concrete defects:

- Registry asset, active-version, and artifact writes are not one transaction.
- Gold pointer reads and publication are mixed into Catalog repository and unit-of-work ports.
- Lakehouse errors are represented as `CatalogError`.
- Domain objects manufacture shared wire events directly.
- Silver materialization, lineage validation, and quality policy have no single owner.
- Moving files layer by layer would leave intermediate commits that do not compile.

## Decision

### Capability packages

Lakehouse behavior moves to these packages:

```text
crates/lakehouse/lakehouse-domain
crates/lakehouse/lakehouse-application
crates/lakehouse/lakehouse-infrastructure
```

Package direction is:

```text
lakehouse-domain
        ^
        |
lakehouse-application ---> catalog-domain
        ^                   collection-domain
        |
lakehouse-infrastructure
        ^
        |
Foundation service composition roots
```

Catalog and Collection must not depend on a Lakehouse package.

### Vertical-slice cutover

The migration moves complete behavior slices. A slice includes its domain contract,
application port and use case, infrastructure adapter, composition-root wiring, and
compatibility tests. Every committed slice must compile and pass its focused tests.
There is no committed red architecture-test phase and no compatibility re-export.

### Transaction ownership

`LakehouseRegistryUnitOfWork` owns namespace validation, asset upsert, active-version
transition, and artifact insertion as one PostgreSQL transaction.

`LakehousePublicationUnitOfWork` owns the Gold pointer, its source record and file
assets, and the corresponding outbox event as one PostgreSQL transaction. Existing
SQL, row locking, optimistic-version behavior, and event bytes are preserved.

Until a separately approved physical-schema migration, Lakehouse infrastructure is
authorized to read canonical Catalog tables and to write only these legacy-schema
records for Lakehouse transactions:

- `catalog.source_record`
- `catalog.file_asset`
- `catalog.industrial_complex_gold_pointer`
- `catalog.lakehouse_*`
- `catalog.outbox_event`

This is physical co-location, not Catalog capability ownership.

### Domain events and wire contracts

Lakehouse domain objects produce Lakehouse-owned domain event data. They do not
import a shared protocol event union. Infrastructure or service adapters map domain
events to the existing `foundation-shared-kernel::events::catalog_v1` wire DTOs.
Exact JSON compatibility tests preserve existing consumers while avoiding a package
dependency cycle.

### Error boundary

Lakehouse packages use `LakehouseError`. HTTP adapters map it to the existing public
400, 409, and opaque 500 behavior. Outbound HTTP adapters map transport failures to
`LakehouseError`; Lakehouse code never manufactures `CatalogError`.

## Consequences

### Positive

- Lakehouse ownership is explicit and follows the same capability/layer convention
  as Collection, Identity, and Intelligence packages.
- Registry and Gold publication gain testable rollback guarantees.
- Catalog becomes smaller without changing API routes, event names, object keys, or DB data.
- Future Lakehouse workers and Kafka adapters receive one stable application boundary.

### Cost

- Service composition roots temporarily inject Catalog and Lakehouse adapters together.
- Lakehouse infrastructure temporarily touches selected tables under the legacy
  `catalog` PostgreSQL schema.
- Compatibility tests are required because Rust ownership changes while public wire
  contracts remain unchanged.

## Explicit Non-Goals

This decision does not:

- move or rename PostgreSQL schemas or tables;
- split service deployables;
- add Kafka, Kubernetes, Temporal, or another orchestrator;
- extract Normalization or Spatial capability code;
- change HTTP routes, JSON fields, event names, R2 keys, CLI commands, or persisted values.

Those changes require separate decisions and verification.

## Verification

Completion requires:

1. no Lakehouse/Iceberg/Silver/Gold implementation remains under `catalog-*`;
2. no Catalog or Collection package depends on `lakehouse-*`;
3. Registry rollback tests prove no active version exists without its artifact;
4. Gold failure-injection and concurrency tests prove atomic publication;
5. exact HTTP/OpenAPI/event/lineage compatibility tests remain green;
6. focused, workspace, clippy, formatting, and supply-chain gates pass.
