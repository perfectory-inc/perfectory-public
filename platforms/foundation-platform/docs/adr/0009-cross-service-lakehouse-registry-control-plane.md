# ADR 0009 - Cross-Service Lakehouse Registry Control Plane

| Field | Value |
|---|---|
| Date | 2026-06-05 |
| Status | Accepted |
| Scope | `foundation-platform` Lakehouse Registry bounded context, cross-service R2 bucket ownership, data asset discovery |
| Related ADRs | [ADR 0002](./0002-r2-primary-object-storage.md), [ADR 0005](./0005-object-lake-layout-and-indexing.md), [ADR 0006](./0006-lakehouse-table-format-and-serving-architecture.md), [ADR 0007](./0007-netflix-style-lakehouse-compute-architecture.md), [ADR 0008](./0008-pnu-anchor-pbf-marker-tile-contract.md) |

## Context

`foundation-platform`, `gongzzang`, and `dawneer` need independent ownership of their own data bodies, but
the company still needs one governed way to discover, authorize, promote, audit, and roll back
lakehouse assets.

A legacy or single-owner R2 bucket may have root-level medallion prefixes such as:

```text
bronze/
silver-handoff/
gold/
__r2_data_catalog/
```

That layout is acceptable only as a single-owner bucket namespace. It is not a clean cross-service
namespace because the object key alone cannot answer:

- which service owns the data;
- which service may write it;
- which version is active;
- which Bronze inputs produced a Silver or Gold artifact;
- whether a consumer is allowed to read it.

Adding a new top-level service above `foundation-platform` would create another control plane before the
existing `foundation-platform` control plane is fully hardened. That would increase operational surface
area without improving ownership clarity.

## Decision

The `Lakehouse Registry` is a Foundation Platform bounded context for data assets. It is not an
organization-wide identity or application control plane.

```text
foundation-platform
├─ Catalog
│  └─ parcel, building, industrial complex, PNU anchor, public/reference layers
└─ Lakehouse Registry
   └─ storage namespace, data asset, version, lineage, quality, and access registry
```

The Lakehouse Registry manages metadata for lakehouse assets across service-owned R2 buckets. It does
not make `foundation-platform` the owner of every service's business data.

```text
Data owner:
  foundation-platform owns Catalog/common/public spatial data.
  gongzzang owns listings, listing media, Onbid sale data, court auction data, and market data.
  dawneer owns Dawneer workbench/product-specific data.

Registry owner:
  foundation-platform owns the registry records, policy checks, active pointers, lineage, and discovery API.
```

## Physical Storage Model

For production, use service-owned buckets per environment. Each bucket may use the standard
medallion root layout because the provisioned bucket binding supplies the owner boundary. Names
below are logical placeholders, not claims about active Cloudflare resources.

```text
<foundation-platform-bucket>/
├─ bronze/
├─ silver/
├─ gold/
└─ __r2_data_catalog/

<gongzzang-bucket>/
├─ bronze/
├─ silver/
├─ gold/
└─ __r2_data_catalog/

<dawneer-bucket>/
├─ bronze/
├─ silver/
├─ gold/
└─ __r2_data_catalog/
```

If a single physical bucket is ever required, service ownership must be the first meaningful prefix:

```text
<environment>/foundation-platform/bronze/
<environment>/gongzzang/bronze/
<environment>/dawneer/bronze/
```

New cross-service data must not be written into an unowned root such as:

```text
bronze/source=...
gold/...
```

unless the bucket is explicitly a single-owner bucket.

## Registry Responsibilities

The Lakehouse Registry records:

- storage namespaces: provider, account, bucket, environment, owner service, allowed root prefix;
- data assets: stable qualified names such as `foundation_platform.gold.parcel_marker_anchor` or
  `gongzzang.bronze.onbid_sale`;
- dataset versions: immutable version id, schema version, table format, active/previous/retired state;
- object artifacts: object keys, byte size, checksum, row count, content type, retention class;
- ingestion runs: source, request fingerprint, rate policy, result state, written objects;
- lineage edges: Bronze object set -> Silver table snapshot -> Gold artifact;
- quality checks: row count, null rates, schema compatibility, spatial validity, checksum verification;
- access policies: which service may read, write, promote, or consume each asset;
- consumer bindings: which app/API/event contract consumes which active version.

The data body remains in R2/Iceberg. PostgreSQL stores the control-plane metadata, not bulk payloads.

## API Boundary

Consumers must not infer object keys. They ask `foundation-platform` for the active asset or register their
own service-owned artifacts.

Initial API shapes:

```text
POST /internal/lakehouse/namespaces
POST /internal/lakehouse/assets
POST /internal/lakehouse/ingestion-runs
POST /internal/lakehouse/artifacts
POST /internal/lakehouse/lineage
POST /internal/lakehouse/promotions
GET  /internal/lakehouse/assets/{qualified_name}/active
GET  /internal/lakehouse/assets/{qualified_name}/versions/{version}
GET  /internal/lakehouse/assets/{qualified_name}/lineage
```

Public/product APIs may expose narrowed read-only contracts, but write/promotion endpoints remain
internal and service-authenticated.

## R2 Data Catalog and Iceberg

Cloudflare R2 Data Catalog remains a provider for Iceberg table metadata. It is not the business
ownership SSOT.

```text
R2 bucket / Iceberg table metadata = table storage/catalog provider
foundation-platform Lakehouse Registry = ownership, discovery, active version, lineage, quality, policy SSOT
```

Each service-owned bucket can enable R2 Data Catalog when that bucket needs Iceberg tables. Raw
unstructured artifacts may remain plain R2 objects but must still be registered when they participate
in a governed pipeline.

## Legacy Bucket Interpretation

A legacy bucket with root-level `bronze/`, `gold/`, or `silver-handoff/` prefixes must be assigned to
exactly one owner namespace before any new writes. Existing prefixes do not prove ownership, active
status, or migration completion. Product-owned assets must not be added to a root that is assigned to
Foundation Platform.

## Provisioning Contract

This ADR does not record whether an account, bucket, prefix, storage class, or region is currently
provisioned. A namespace becomes active only after all of the following are verified:

1. infrastructure-as-code or an approved provisioning record binds environment, account, bucket,
   owner service, and allowed prefix;
2. the Lakehouse Registry contains the matching namespace record;
3. credentials are scoped to the owner and permitted roots;
4. a bounded write/read/reconciliation check succeeds;
5. the evidence and resource identifiers are stored in the private operations evidence system under
   [root ADR 0007](../../../../docs/adr/0007-public-code-private-operations-boundary.md).

Retired or smoke-only resources may be removed only after inventory proves that no current registry
record, lineage edge, consumer binding, or pipeline references them.

## Forbidden

- A product service guessing R2 keys for canonical data.
- A product service writing to a `foundation-platform` owned bucket/root.
- `foundation-platform` treating Gongzzang-owned business data as Catalog-owned facts.
- New multi-service buckets whose first partition is only `bronze/`, `silver/`, or `gold/`.
- Storing raw public API payload bodies in PostgreSQL JSONB as the primary Bronze store.
- Promotion by object existence alone, without registry state, checksum, quality evidence, and lineage.

## Migration Path

1. Freeze each discovered legacy root-level layout until its owner and consumers are classified.
2. Provision service-owned namespace bindings for `foundation-platform`, `gongzzang`, and `dawneer`
   per environment.
3. Implement `Lakehouse Registry` schema and internal APIs in `foundation-platform`.
4. Inventory and register validated Foundation Platform Bronze/Silver/Gold objects as existing assets.
5. Point new Gongzzang-owned pipelines, including Onbid and court auction, to the Gongzzang-owned
   lakehouse namespace.
6. Register Gongzzang artifacts in foundation-platform Lakehouse Registry after write verification.
7. Add boundary checks so new object keys and env variables cannot use root-level shared medallion
   prefixes without an owner namespace.
8. Only after inventory, lineage, and consumer-binding verification, migrate or retire legacy
   root-level objects.

## Consequences

Positive:

- `foundation-platform` remains the platform control plane; no premature fourth service is introduced.
- Service data ownership stays clear while discovery and governance are centralized.
- Bucket IAM, lifecycle, retention, and blast radius can be service-specific.
- R2 Data Catalog/Iceberg remains replaceable because business logic depends on registry contracts.
- Later extraction to a separate `data-platform` service is possible because the bounded context is
  isolated.

Cost:

- More buckets and registry records must be provisioned and audited.
- Services must register artifacts after writing them; direct object-key conventions are insufficient.
- Any legacy root-level bucket requires classification before cleanup.

## Exit Criteria

- `foundation-platform` has a `Lakehouse Registry` bounded context design and implementation plan.
- Every new governed object belongs to exactly one service-owned storage namespace.
- Registry API can resolve active assets without consumers knowing raw R2 keys.
- Boundary checks reject new root-level multi-service `bronze/`, `silver/`, or `gold/` writes.
- Discovered Foundation Platform R2 objects are inventoried before any deletion or migration.
