# ADR 0027: Normalization Capability Owns Semantic Decisions and Proposal Governance

- Status: Accepted
- Date: 2026-07-17
- Supersedes: Normalization ownership implied by the Catalog umbrella
- Related: Foundation ADR 0021, Foundation ADR 0026, Gongzzang ADR 0048

## Context

Foundation Platform already owned deterministic normalization, semantic metadata,
entity-impact detection, AI proposal intake, human review, application, and rollback.
At the time of this decision, those behaviors were implemented inside `catalog-domain`,
`catalog-application`, and `catalog-infrastructure` even though Catalog's permanent
responsibility is canonical entities and their commands.

The current placement has concrete defects:

- Lakehouse materialization imports normalization rules from `catalog-domain`.
- Normalization lifecycle errors are represented as `CatalogError`.
- Proposal commands and their unit-of-work port are mixed into Catalog ports.
- One Catalog infrastructure file owns proposal persistence, review persistence,
  target-specific validation, canonical mutation SQL, outbox writes, and rollback.
- Semantic metadata and entity-impact ownership are not visible from package names.
- A future worker or Kafka adapter has no single Normalization application boundary.

## Decision

### Capability packages

Normalization behavior moves to these packages:

```text
crates/normalization/normalization-domain
crates/normalization/normalization-application
crates/normalization/normalization-infrastructure
```

`normalization-domain` owns deterministic floor/unit rules, entity-context
resolution, semantic metadata, entity-impact mapping, proposal identity and
lifecycle types, and `NormalizationError`.

`normalization-application` owns proposal submit/review/apply/rollback use cases,
commands, receipts, and the `NormalizationUnitOfWork` port.

`normalization-infrastructure` owns PostgreSQL proposal/review/application ledger
persistence and the transaction that coordinates an approved canonical change.

### Dependency direction

```text
normalization-domain
        ^
        |
normalization-application
        ^
        |
normalization-infrastructure ---> catalog-application
        |                         catalog-infrastructure
        |
Foundation service composition roots

lakehouse-application ----------> normalization-domain
```

Catalog and Collection packages must not depend on a Normalization package.
Lakehouse may depend on Normalization's pure domain rules while materializing
Silver rows. Normalization infrastructure may call a transaction-scoped Catalog
infrastructure collaborator because canonical Catalog SQL remains Catalog-owned.
Every request and result crossing that boundary is Catalog-owned (`ComplexId`,
`ComplexMutation`, and a Catalog mutation receipt). Catalog never imports a
Normalization command or type.

### Canonical apply transaction

AI remains a proposal producer. Human review remains mandatory. Apply and rollback
continue to run as one Foundation PostgreSQL transaction:

1. lock the proposal or prior application;
2. validate lifecycle state and expected canonical version;
3. call the Catalog-owned transaction collaborator for canonical mutation;
4. write the Catalog outbox event when canonical state changes;
5. write the Normalization application/rollback ledger;
6. update proposal status;
7. commit once.

The Catalog collaborator accepts an existing SQLx transaction and never commits it.
Catalog SQL, row mapping, optimistic-version checks, and Catalog event construction
remain in `catalog-infrastructure`. Normalization infrastructure never duplicates
canonical Catalog mutation SQL.

Atomicity tests must fail after the Catalog collaborator has successfully changed
the canonical row and inserted its outbox event. A failure while inserting the
Normalization application ledger or updating proposal status must roll back the
canonical row, outbox event, Normalization ledger, and proposal status together.

Building-register unit overrides remain Normalization ledger records because they
do not directly mutate a canonical Catalog aggregate in the current slice.

Building-register-unit applications form one rooted, acyclic, unbranched predecessor
chain per target. Immutable lineage follows the historical tail while active override
state is tracked independently. Active state is the deepest application in that chain
that has not been rolled back. Rolling back an ancestor does not resurrect an older
value while a deeper descendant remains active. Transaction-start timestamps and UUID
order are audit metadata and must not select state. Reader and writer share one graph
query; missing links, extra roots, branches, malformed snapshot envelopes, and cycles
fail loudly.

Industrial-complex rollback is a compensating inverse patch. It restores only fields
changed by the selected application. Applications and compensating rows form a
version-ordered LIFO stack. The selected application must be the active stack top, and
adjacent rows without a version gap must hand off an identical canonical snapshot.
After B is compensated, A may be compensated against the latest validated ledger head;
an external Catalog mutation, non-LIFO request, malformed ledger handoff, or ABA change
returns a state conflict before canonical mutation. A proposed patch equal to the locked
canonical state is rejected before it can create a version, outbox event, ledger row, or
status transition.

### Error boundary

Normalization packages use `NormalizationError`. They do not manufacture
`CatalogError`. Catalog errors returned by the transaction collaborator are mapped
at the Normalization infrastructure boundary while preserving current HTTP status
and response behavior. The one intentional hardening is that an internal submit
failure keeps its existing HTTP status and error code but returns an opaque message;
database and provider details are never returned to the caller.

### Physical database namespace

This capability extraction does not rename existing PostgreSQL tables. Until a
separately approved physical-schema migration, Normalization infrastructure is
authorized to write only these legacy-namespace records:

- `catalog.normalization_proposal`
- `catalog.normalization_proposal_review`
- `catalog.normalization_application`

It may coordinate Catalog-owned `catalog.industrial_complex` and
`catalog.outbox_event` changes only through the Catalog transaction collaborator.
Physical co-location does not imply Catalog capability ownership.

Active building-register unit override reads are also Normalization-owned. Service
workers obtain application id plus the opaque `after_snapshot` through a
Normalization application read port implemented by Normalization infrastructure;
they do not query `catalog.normalization_application` directly.

### Compatibility

The extraction preserves existing HTTP paths, request/response JSON shape, existing
Catalog OpenAPI,
proposal keys, status wire values, PostgreSQL rows, event bytes, Silver/Parquet
output, and authorization requirements. Validation and conflict messages remain
unchanged for existing outcomes. Newly detected stale-state conflicts use the
existing HTTP 409 error shape, and newly detected no-op mutations use the existing
invalid-input shape. Internal submit failures retain their status and error code
while their message is deliberately redacted. Normalization routes now have their own
generated OpenAPI document backed by public transport DTOs; they were never represented
by the static Catalog document. No compatibility re-export remains under `catalog-*`
after cutover.

## Consequences

### Positive

- Package names reveal the real owner of normalization behavior.
- Catalog returns to canonical entity and command ownership.
- Lakehouse consumes deterministic rules from their actual owner.
- Proposal governance has one stable boundary for Intelligence, HTTP, and future
  event adapters.
- Canonical apply remains atomic without duplicating Catalog persistence logic.

### Cost

- Normalization infrastructure temporarily writes tables under the legacy
  `catalog` PostgreSQL schema.
- The Foundation API composition root injects Catalog and Normalization adapters.
- Exact compatibility tests are required while Rust ownership changes.

## Explicit Non-Goals

This decision does not:

- rename or move PostgreSQL schemas or tables;
- change normalization rules, accepted values, or entity-resolution policy;
- run Qwen or automatically approve a proposal;
- change Bronze, Silver, Gold, Parquet, Iceberg, R2, or dbt contracts;
- add Kafka, Kubernetes, Temporal, Dagster, or another orchestrator;
- extract Spatial capability or split service deployables;
- change public HTTP paths, JSON fields, permissions, or event names.

## Verification

Completion requires:

1. no Normalization implementation or forwarding alias remains under `catalog-*`;
2. Catalog and Collection packages have no dependency on `normalization-*`;
3. Lakehouse imports deterministic rules only from `normalization-domain`;
4. apply and rollback failure-injection tests prove one-transaction behavior;
5. concurrency tests prove chain-based active state and conflict-safe compensation;
6. exact HTTP, generated Normalization OpenAPI, proposal-key, status, and Silver-output
   tests remain green;
7. focused, workspace, clippy, formatting, and supply-chain gates pass.

---

> 2026-07-20 개정 각주: crate rename 반영 — 본문의 `normalization-{domain,application,infrastructure}` 는
> 현재 `crates/normalization/foundation-normalization-{domain,application,infrastructure}` 이다
> (전역 유일 패키지명 규칙, 루트 ADR-0001). 결정 내용 자체는 변경 없음.
