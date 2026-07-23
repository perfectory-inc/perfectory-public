# ADR 0025 - Bronze Catalog recovery evidence sealing

Status: Accepted
Date: 2026-07-14
Owner: foundation-platform
Related: [ADR 0016](./0016-bronze-commit-protocol.md),
[ADR 0019](./0019-bronze-readable-object-lake-postgres-catalog-ssot.md)

## Context

Catalog recovery verifies existing Bronze bytes in R2 and reconstructs missing Postgres metadata.
Recovery sets can be large, so running a complete dry-run and then APPLY would read and hash the same
bytes twice. A local-only recovery manifest can also disappear after a successful apply, leaving the
Catalog mutation without durable evidence of why it was allowed.

R2 and Postgres cannot participate in one transaction. Recovery therefore needs durable immutable
input evidence, exact object identity checks, and source-scoped atomic Catalog mutation without a
second full-byte validation pass.

## Decision

1. APPLY is source-scoped. One explicit source is fully verified before its Catalog transaction is
   committed. A partial candidate limit is forbidden in APPLY mode.
2. Immediately before APPLY, the endpoint catalog, provider inventory, R2 inventory, and rewritten
   recovery manifest are sealed to the Foundation Platform R2 bucket as immutable, content-addressed
   control evidence:

   ```text
   control/evidence/bronze-catalog-recovery/{kind}/sha256={sha256}.json
   ```

3. Evidence writes use `CreateOnly`. An existing key is reusable only when its stored SHA-256
   metadata matches. A hash mismatch or unsupported evidence URI fails before Catalog mutation.
4. The sealed R2 manifest URI and SHA-256 are recorded on the recovery ingestion run and every
   recovered Bronze object. Each object also records the ETag and last-modified value observed during
   the live R2 verification.
5. Full object bytes are read and hashed once in the APPLY verification phase. The verified source is
   then committed atomically to Postgres; a second full dry-run is not required.
6. Manual manifest URI override is forbidden in APPLY mode. Dry-run may still use a local manifest.
7. `control/` is a control-plane evidence namespace, not a fourth medallion data layer. Bronze,
   Silver, and Gold remain unchanged.
8. Object verification is bounded-concurrent at the service adapter, with a default concurrency of
   32 and an accepted range of 1 through 64. The application use case remains runtime-independent,
   preserves manifest order, and performs no Catalog mutation until every result validates.

## Failure semantics

- Changed or missing local evidence: fail before R2 object verification.
- Existing evidence with matching checksum metadata: idempotent reuse.
- Existing evidence with different or missing checksum metadata: fail loudly.
- Any Bronze object size, ETag, last-modified, or checksum mismatch: fail before Catalog mutation.
- Catalog transaction failure: no partial source metadata is committed; sealed evidence remains
  available for diagnosis and retry.

## Consequences

- Large recovery sources require one full-byte verification pass instead of two.
- Independent R2 object reads can overlap without introducing partial Catalog commits or unbounded
  memory. With 16 MiB R2 range windows, the default upper bound is approximately 512 MiB of active
  range bodies plus SDK overhead.
- Recovery decisions remain reproducible after local build artifacts are deleted.
- R2 inventory audit explicitly retains valid recovery evidence and does not classify it as unknown
  or disposable smoke data.
- The evidence key compiler and audit classifier share one R2 layout SSOT.
- This adds only small JSON evidence objects. It does not add a database migration, Kafka,
  Kubernetes, Temporal, or another orchestration framework.

## Non-goals

- Recovery evidence is not a generic evidence framework.
- It does not replace the Bronze Catalog, provider inventory, or ingestion-run audit records.
- It does not authorize deletion of unresolved or legacy objects.
- It does not change the physical Bronze object layout.
